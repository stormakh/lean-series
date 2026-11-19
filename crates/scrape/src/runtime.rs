//! The scrape scheduler: one scheduler thread with a timer heap and an elastic pool of worker threads.
//!
//! A scrape loop is a state machine ([`Task`]): the scheduler pops it when it is due and hands it to a worker,
//! which runs one step (one scrape, or one staleness step) and schedules the next step itself. A scrape never runs
//! on the scheduler thread. Workers are started on demand when every worker is busy (slow targets block their
//! worker in blocking HTTP I/O), and extra workers exit after a minute without work. With nothing due, every
//! thread sleeps without a timeout.
//!
//! Time comes from a [`Clock`]. The real clock uses the monotonic clock for scheduling and the wall clock for
//! sample timestamps. A [`VirtualClock`] makes time move only when a test advances it (Go `testing/synctest`).

use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};

/// A source of time.
pub trait Clock: Send + Sync {
    /// The wall clock time (Go `time.Now()` without the monotonic reading).
    fn wall(&self) -> SystemTime;
    /// The monotonic time since an arbitrary start; scheduling uses it.
    fn mono(&self) -> Duration;
    /// Whether time only moves when advanced explicitly.
    fn is_virtual(&self) -> bool {
        false
    }
}

/// The system clock.
pub struct RealClock {
    start: Instant,
}

impl RealClock {
    pub fn new() -> RealClock {
        RealClock { start: Instant::now() }
    }
}

impl Default for RealClock {
    fn default() -> Self {
        RealClock::new()
    }
}

impl Clock for RealClock {
    fn wall(&self) -> SystemTime {
        SystemTime::now()
    }
    fn mono(&self) -> Duration {
        self.start.elapsed()
    }
}

/// A clock that only moves when [`Runtime::sleep`] advances it.
pub struct VirtualClock {
    wall0: SystemTime,
    now: Mutex<Duration>,
}

impl VirtualClock {
    /// A virtual clock that starts at `wall0` (Go synctest starts at 2000-01-01 00:00:00 UTC).
    pub fn new(wall0: SystemTime) -> VirtualClock {
        VirtualClock { wall0, now: Mutex::new(Duration::ZERO) }
    }
    fn set(&self, d: Duration) {
        let mut n = self.now.lock().unwrap();
        if d > *n {
            *n = d;
        }
    }
}

impl Clock for VirtualClock {
    fn wall(&self) -> SystemTime {
        self.wall0 + *self.now.lock().unwrap()
    }
    fn mono(&self) -> Duration {
        *self.now.lock().unwrap()
    }
    fn is_virtual(&self) -> bool {
        true
    }
}

/// A unit of scheduled work. `generation` is the generation given to [`Runtime::schedule_at`]; tasks ignore entries
/// with an outdated generation.
pub trait Task: Send + Sync {
    fn run_task(self: Arc<Self>, rt: &Arc<Runtime>, generation: u64);
}

struct Entry {
    due: Duration,
    seq: u64,
    generation: u64,
    task: Arc<dyn Task>,
}

impl PartialEq for Entry {
    fn eq(&self, o: &Entry) -> bool {
        self.due == o.due && self.seq == o.seq
    }
}
impl Eq for Entry {}
impl PartialOrd for Entry {
    fn partial_cmp(&self, o: &Entry) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Entry {
    fn cmp(&self, o: &Entry) -> std::cmp::Ordering {
        (self.due, self.seq).cmp(&(o.due, o.seq))
    }
}

#[derive(Default)]
struct Sched {
    heap: BinaryHeap<Reverse<Entry>>,
    started: bool,
}

#[derive(Default)]
struct Work {
    queue: VecDeque<(Arc<dyn Task>, u64)>,
    idle: usize,
    total: usize,
    running: usize,
}

/// The scheduler and its workers.
pub struct Runtime {
    clock: Arc<dyn Clock>,
    sched: Mutex<Sched>,
    sched_cv: Condvar,
    work: Mutex<Work>,
    work_cv: Condvar,
    idle_cv: Condvar,
    seq: AtomicU64,
    min_workers: usize,
    max_workers: usize,
    virtual_clock: Option<Arc<VirtualClock>>,
}

const WORKER_IDLE_EXIT: Duration = Duration::from_secs(60);
const WORKER_STACK: usize = 1 << 20;

impl Runtime {
    /// A runtime with `clock`. Workers start on demand, up to `max_workers`.
    pub fn new(clock: Arc<dyn Clock>, min_workers: usize, max_workers: usize) -> Arc<Runtime> {
        Runtime::build(clock, None, min_workers, max_workers)
    }

    fn build(clock: Arc<dyn Clock>, virtual_clock: Option<Arc<VirtualClock>>, min_workers: usize, max_workers: usize) -> Arc<Runtime> {
        Arc::new(Runtime {
            virtual_clock,
            clock,
            sched: Mutex::new(Sched::default()),
            sched_cv: Condvar::new(),
            work: Mutex::new(Work::default()),
            work_cv: Condvar::new(),
            idle_cv: Condvar::new(),
            seq: AtomicU64::new(0),
            min_workers: min_workers.max(1),
            max_workers: max_workers.max(min_workers.max(1)),
        })
    }

    /// The process-wide runtime with the real clock.
    pub fn global() -> Arc<Runtime> {
        static G: OnceLock<Arc<Runtime>> = OnceLock::new();
        G.get_or_init(|| {
            let n = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
            Runtime::new(Arc::new(RealClock::new()), n, 4096)
        })
        .clone()
    }

    /// A runtime with a virtual clock (for tests that use Go's `synctest`).
    pub fn new_virtual(wall0: SystemTime) -> Arc<Runtime> {
        let v = Arc::new(VirtualClock::new(wall0));
        Runtime::build(v.clone(), Some(v), 2, 4096)
    }

    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    /// The monotonic now of the clock.
    pub fn now(&self) -> Duration {
        self.clock.mono()
    }

    /// The wall clock now.
    pub fn wall(&self) -> SystemTime {
        self.clock.wall()
    }

    /// Schedules `task` to run at the monotonic time `due` with generation `generation`.
    pub fn schedule_at(self: &Arc<Self>, due: Duration, task: Arc<dyn Task>, generation: u64) {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let mut s = self.sched.lock().unwrap();
        if !s.started {
            s.started = true;
            let rt = self.clone();
            std::thread::Builder::new()
                .name("scrape-scheduler".into())
                .spawn(move || rt.scheduler())
                .expect("start scrape scheduler");
        }
        let wake = s.heap.peek().is_none_or(|Reverse(e)| due < e.due);
        s.heap.push(Reverse(Entry { due, seq, generation, task }));
        drop(s);
        if wake {
            self.sched_cv.notify_one();
        }
    }

    /// Runs `task` on a worker now.
    pub fn spawn(self: &Arc<Self>, task: Arc<dyn Task>, generation: u64) {
        let mut w = self.work.lock().unwrap();
        w.queue.push_back((task, generation));
        if w.idle > w.queue.len() - 1 {
            drop(w);
            self.work_cv.notify_one();
            return;
        }
        if w.total < self.max_workers {
            w.total += 1;
            drop(w);
            let rt = self.clone();
            std::thread::Builder::new()
                .name("scrape-worker".into())
                .stack_size(WORKER_STACK)
                .spawn(move || rt.worker())
                .expect("start scrape worker");
            return;
        }
        drop(w);
        self.work_cv.notify_one();
    }

    fn scheduler(self: Arc<Self>) {
        let mut s = self.sched.lock().unwrap();
        loop {
            let now = self.clock.mono();
            let mut due = Vec::new();
            while let Some(Reverse(e)) = s.heap.peek() {
                if e.due > now {
                    break;
                }
                let Reverse(e) = s.heap.pop().unwrap();
                due.push(e);
            }
            if !due.is_empty() {
                drop(s);
                for e in due {
                    self.spawn(e.task, e.generation);
                }
                s = self.sched.lock().unwrap();
                continue;
            }
            match s.heap.peek() {
                Some(Reverse(e)) if !self.clock.is_virtual() => {
                    let d = e.due - now;
                    s = self.sched_cv.wait_timeout(s, d).unwrap().0;
                }
                _ => {
                    // Nothing due: sleep until a push (or, with a virtual clock, until time moves).
                    self.idle_cv.notify_all();
                    s = self.sched_cv.wait(s).unwrap();
                }
            }
        }
    }

    fn worker(self: Arc<Self>) {
        let mut w = self.work.lock().unwrap();
        loop {
            if let Some((t, generation)) = w.queue.pop_front() {
                w.running += 1;
                drop(w);
                t.run_task(&self, generation);
                w = self.work.lock().unwrap();
                w.running -= 1;
                if w.running == 0 && w.queue.is_empty() {
                    self.idle_cv.notify_all();
                }
                continue;
            }
            w.idle += 1;
            let (g, to) = self.work_cv.wait_timeout(w, WORKER_IDLE_EXIT).unwrap();
            w = g;
            w.idle -= 1;
            if to.timed_out() && w.queue.is_empty() && w.total > self.min_workers {
                w.total -= 1;
                return;
            }
        }
    }

    /// Runs `f` on a worker at the monotonic time `due` (a one-shot timer on the runtime clock).
    pub fn schedule_fn_at(self: &Arc<Self>, due: Duration, f: Box<dyn FnOnce() + Send>) {
        self.schedule_at(due, Arc::new(FnTask(Mutex::new(Some(f)))), 0);
    }

    /// Marks the runtime busy while the guard lives, for work outside the workers that [`Runtime::wait_idle`]
    /// must wait for (Go synctest: a goroutine that is not durably blocked).
    pub fn busy_guard(self: &Arc<Self>) -> BusyGuard {
        self.work.lock().unwrap().running += 1;
        BusyGuard(self.clone())
    }

    /// Number of worker threads now (for tests and metrics).
    pub fn workers(&self) -> usize {
        self.work.lock().unwrap().total
    }

    fn busy(&self) -> bool {
        let w = self.work.lock().unwrap();
        if w.running > 0 || !w.queue.is_empty() {
            return true;
        }
        drop(w);
        let now = self.clock.mono();
        let s = self.sched.lock().unwrap();
        s.heap.peek().is_some_and(|Reverse(e)| e.due <= now)
    }

    /// Waits until no task runs or is due (Go `synctest.Wait` for the scrape threads).
    pub fn wait_idle(&self) {
        // Tasks may start other tasks or threads that schedule tasks; a few quiet checks in a row make this
        // robust against such hand-offs.
        let mut quiet = 0;
        while quiet < 3 {
            if self.busy() {
                quiet = 0;
                let w = self.work.lock().unwrap();
                let _ = self.idle_cv.wait_timeout(w, Duration::from_millis(5)).unwrap();
            } else {
                quiet += 1;
                std::thread::sleep(Duration::from_millis(2));
            }
        }
    }

    /// Advances a virtual clock by `d`, running every task that becomes due on the way, in time order
    /// (Go `time.Sleep` inside a synctest bubble). With the real clock it sleeps.
    pub fn sleep(&self, d: Duration) {
        if !self.clock.is_virtual() {
            std::thread::sleep(d);
            return;
        }
        let target = self.clock.mono() + d;
        loop {
            self.wait_idle();
            let next = {
                let s = self.sched.lock().unwrap();
                s.heap.peek().map(|Reverse(e)| e.due)
            };
            match next {
                Some(n) if n <= target => {
                    self.set_virtual(n);
                }
                _ => break,
            }
        }
        self.set_virtual(target);
        self.wait_idle();
    }

    fn set_virtual(&self, t: Duration) {
        if let Some(v) = &self.virtual_clock {
            v.set(t);
        }
        let _g = self.sched.lock().unwrap();
        self.sched_cv.notify_all();
    }
}

struct FnTask(Mutex<Option<Box<dyn FnOnce() + Send>>>);

impl Task for FnTask {
    fn run_task(self: Arc<Self>, _rt: &Arc<Runtime>, _generation: u64) {
        let f = self.0.lock().unwrap().take();
        if let Some(f) = f {
            f();
        }
    }
}

/// See [`Runtime::busy_guard`].
pub struct BusyGuard(Arc<Runtime>);

impl Drop for BusyGuard {
    fn drop(&mut self) {
        let mut w = self.0.work.lock().unwrap();
        w.running -= 1;
        if w.running == 0 && w.queue.is_empty() {
            self.0.idle_cv.notify_all();
        }
    }
}
