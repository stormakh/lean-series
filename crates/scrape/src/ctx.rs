//! A cancellation context for the scrape threads (the part of Go's `context.Context` that scraping needs):
//! cancellation that propagates to children, an optional deadline, blocking waits, and wake callbacks that the
//! scheduler uses to resume a loop as soon as its context is cancelled. A child removes its registration from the
//! parent when it is dropped, so long-lived parents do not grow.

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use storage::{ERR_CANCELED, ERR_DEADLINE_EXCEEDED, Error};

const NOT_DONE: u8 = 0;
const CANCELED: u8 = 1;
const DEADLINE: u8 = 2;

/// A callback run once when the context is cancelled.
pub type Waker = Arc<dyn Fn() + Send + Sync>;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

struct Inner {
    id: u64,
    parent: Option<Ctx>,
    deadline: Option<Instant>,
    state: AtomicU8,
    wakers: Mutex<Vec<(u64, Waker)>>,
    cv: Condvar,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(p) = &self.parent {
            p.remove_waker(self.id);
        }
    }
}

/// A cancellation scope. Clones share the scope.
#[derive(Clone)]
pub struct Ctx(Arc<Inner>);

impl std::fmt::Debug for Ctx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ctx(done={})", self.is_done())
    }
}

impl Default for Ctx {
    fn default() -> Self {
        Ctx::background()
    }
}

impl Ctx {
    fn new(parent: Option<Ctx>, deadline: Option<Instant>) -> Ctx {
        let c = Ctx(Arc::new(Inner {
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            parent: parent.clone(),
            deadline,
            state: AtomicU8::new(NOT_DONE),
            wakers: Mutex::new(Vec::new()),
            cv: Condvar::new(),
        }));
        if let Some(p) = parent {
            let weak: Weak<Inner> = Arc::downgrade(&c.0);
            p.add_waker_with_id(
                c.0.id,
                Arc::new(move || {
                    if let Some(i) = weak.upgrade() {
                        Ctx(i).cancel_with(CANCELED);
                    }
                }),
            );
        }
        c
    }

    /// A context that is only done when cancelled (Go `context.Background()` with a cancel function).
    pub fn background() -> Ctx {
        Ctx::new(None, None)
    }

    /// A child that is done when it or this context is done (Go `context.WithCancel`).
    pub fn child(&self) -> Ctx {
        Ctx::new(Some(self.clone()), None)
    }

    /// A child with a deadline `d` from now (Go `context.WithTimeout`).
    pub fn with_timeout(&self, d: Duration) -> Ctx {
        Ctx::new(Some(self.clone()), Some(Instant::now() + d))
    }

    /// The deadline of this context or the closest one of its parents.
    pub fn deadline(&self) -> Option<Instant> {
        let mut d = self.0.deadline;
        let mut p = self.0.parent.clone();
        while let Some(c) = p {
            if let Some(x) = c.0.deadline {
                d = Some(d.map_or(x, |y| y.min(x)));
            }
            p = c.0.parent.clone();
        }
        d
    }

    fn cancel_with(&self, how: u8) {
        if self.0.state.compare_exchange(NOT_DONE, how, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return;
        }
        let wakers = std::mem::take(&mut *self.0.wakers.lock().unwrap_or_else(|e| e.into_inner()));
        self.0.cv.notify_all();
        for (_, w) in wakers {
            w();
        }
    }

    /// Cancels the context and its children. Idempotent.
    pub fn cancel(&self) {
        self.cancel_with(CANCELED);
    }

    fn state(&self) -> u8 {
        let mut c = self;
        loop {
            let s = c.0.state.load(Ordering::SeqCst);
            if s != NOT_DONE {
                return s;
            }
            if let Some(d) = c.0.deadline
                && Instant::now() >= d
            {
                return DEADLINE;
            }
            match &c.0.parent {
                Some(p) => c = p,
                None => return NOT_DONE,
            }
        }
    }

    /// Whether the context is done (Go `ctx.Err() != nil`).
    pub fn is_done(&self) -> bool {
        self.state() != NOT_DONE
    }

    /// Go `ctx.Err()`: `context canceled` or `context deadline exceeded`.
    pub fn err(&self) -> Option<Error> {
        match self.state() {
            CANCELED => Some(ERR_CANCELED),
            DEADLINE => Some(ERR_DEADLINE_EXCEEDED),
            _ => None,
        }
    }

    /// Blocks until the context is done (Go `<-ctx.Done()`).
    pub fn wait(&self) {
        while !self.wait_timeout(Duration::from_secs(3600)) {}
    }

    /// Waits for `d` or until done; true when done (Go `select { case <-ctx.Done(): case <-time.After(d): }`).
    pub fn wait_timeout(&self, d: Duration) -> bool {
        let end = Instant::now() + d;
        let mut g = self.0.wakers.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if self.is_done() {
                return true;
            }
            let now = Instant::now();
            if now >= end {
                return false;
            }
            let mut until = end;
            if let Some(dl) = self.deadline() {
                until = until.min(dl.max(now));
            }
            // Parents wake this context through its waker registration (cancel takes this lock after setting the
            // state, so the wakeup is not lost); deadlines of parents bound `until`.
            let wait = (until - now).max(Duration::from_micros(1));
            g = self.0.cv.wait_timeout(g, wait).unwrap_or_else(|e| e.into_inner()).0;
        }
    }

    fn add_waker_with_id(&self, id: u64, w: Waker) {
        let mut g = self.0.wakers.lock().unwrap_or_else(|e| e.into_inner());
        if self.0.state.load(Ordering::SeqCst) != NOT_DONE {
            drop(g);
            w();
            return;
        }
        g.push((id, w));
    }

    /// Registers `w` to run once when this context is cancelled (at once if it already is). Returns an id for
    /// [`Ctx::remove_waker`].
    pub fn add_waker(&self, w: Waker) -> u64 {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        self.add_waker_with_id(id, w);
        id
    }

    /// Removes a waker registration.
    pub fn remove_waker(&self, id: u64) {
        let mut g = self.0.wakers.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(i) = g.iter().position(|(x, _)| *x == id) {
            g.swap_remove(i);
        }
    }

    /// Whether two contexts are the same scope.
    pub fn same(&self, o: &Ctx) -> bool {
        Arc::ptr_eq(&self.0, &o.0)
    }
}
