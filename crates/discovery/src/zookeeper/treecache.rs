//! Port of `util/treecache`: keeps the data of all nodes under a ZooKeeper path cached, updated from watch
//! events, and sends an event for every node whose data changed (with the data) or that was deleted (no
//! data).
//!
//! Go runs one goroutine per watched node that waits for its data watch, its children watch or its `done`
//! signal and forwards the first of them to the cache loop. Here the watch callbacks of a node share one
//! registration that forwards the first event to the loop and is disarmed by `done`, with the same rules
//! (an event that comes before the registration is armed waits for it; a `done` with no armed registration
//! disarms the next one).
//!
//! Differs from Go: the output events are not blocking sends (Go's unbuffered channel can block the cache
//! loop, and `Stop` with it, when the reader is gone); the retry after a failure is a deadline of the loop
//! instead of a timer goroutine.
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use super::zk::{self, Conn, Event, EventType};
use crate::log::Logger;
use crate::metrics::{Collector, CounterVec, GaugeVec, Opts};

/// Go `failureCounter` (registered on the default registry in Go's `init`).
pub fn failure_counter() -> &'static Arc<CounterVec> {
    static C: OnceLock<Arc<CounterVec>> = OnceLock::new();
    C.get_or_init(|| CounterVec::new_counter(Opts::new("prometheus_treecache_zookeeper_failures_total", "The total number of ZooKeeper failures.")))
}

/// Go `numWatchers`.
pub fn num_watchers() -> &'static Arc<GaugeVec> {
    static G: OnceLock<Arc<GaugeVec>> = OnceLock::new();
    G.get_or_init(|| GaugeVec::new_gauge(Opts::new("prometheus_treecache_watcher_goroutines", "The current number of watcher goroutines.")))
}

/// The two process-wide treecache metrics, for the registry that serves `/metrics`.
pub fn treecache_metrics() -> Vec<Arc<dyn Collector>> {
    vec![failure_counter().clone(), num_watchers().clone()]
}

/// Go `ZookeeperTreeCacheEvent`: `data` is None when the node was deleted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZookeeperTreeCacheEvent {
    pub path: String,
    pub data: Option<Vec<u8>>,
}

/// Where the cache sends its events (Go `chan ZookeeperTreeCacheEvent`).
pub type EventSink = Box<dyn Fn(ZookeeperTreeCacheEvent) + Send>;

enum LoopMsg {
    Zk(Event),
    Stop,
}

#[derive(Default)]
struct RegSt {
    armed: bool,
    pending: Option<Event>,
    finished: bool,
}

/// The watches set by one `recursive_node_update` of a node (Go: one watcher goroutine).
struct Reg {
    st: Mutex<RegSt>,
    tx: mpsc::Sender<LoopMsg>,
}

impl Reg {
    fn lock(&self) -> std::sync::MutexGuard<'_, RegSt> {
        self.st.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn on_event(&self, ev: Event) {
        let mut s = self.lock();
        if s.finished {
            return;
        }
        if !s.armed {
            if s.pending.is_none() {
                s.pending = Some(ev);
            }
            return;
        }
        s.finished = true;
        num_watchers().get().dec();
        let _ = self.tx.send(LoopMsg::Zk(ev));
    }

    /// Go `wg.Go(func() {...})`: starts forwarding. `done` is a pending done signal of the node.
    fn arm(&self, done: bool) {
        let mut s = self.lock();
        s.armed = true;
        num_watchers().get().inc();
        if done {
            s.finished = true;
            num_watchers().get().dec();
            return;
        }
        if let Some(ev) = s.pending.take() {
            s.finished = true;
            num_watchers().get().dec();
            let _ = self.tx.send(LoopMsg::Zk(ev));
        }
    }

    /// The done signal: stops a forwarding registration. False if it was not forwarding.
    fn stop(&self) -> bool {
        let mut s = self.lock();
        if s.armed && !s.finished {
            s.finished = true;
            num_watchers().get().dec();
            return true;
        }
        false
    }

    fn is_finished(&self) -> bool {
        self.lock().finished
    }
}

/// Go `zookeeperTreeCacheNode`.
#[derive(Default)]
struct Node {
    data: Option<Vec<u8>>,
    stopped: bool,
    /// Go `done` holds a value no watcher goroutine took yet.
    done_buffered: bool,
    regs: Vec<Arc<Reg>>,
    children: HashMap<String, Node>,
}

impl Node {
    /// Go `node.done <- struct{}{}`: wakes one forwarding registration, or stays for the next one.
    fn signal_done(&mut self) {
        self.regs.retain(|r| !r.is_finished());
        if !self.regs.iter().any(|r| r.stop()) {
            self.done_buffered = true;
        }
    }

    fn arm(&mut self, reg: Arc<Reg>) {
        self.regs.retain(|r| !r.is_finished());
        let done = std::mem::take(&mut self.done_buffered);
        reg.arm(done);
        if !done {
            self.regs.push(reg);
        }
    }
}

struct Cx {
    conn: Conn,
    events: EventSink,
    tx: mpsc::Sender<LoopMsg>,
    logger: Logger,
}

impl Cx {
    fn emit(&self, path: &str, data: Option<Vec<u8>>) {
        (self.events)(ZookeeperTreeCacheEvent { path: path.to_string(), data });
    }
}

/// Go `ZookeeperTreeCache`.
pub struct ZookeeperTreeCache {
    tx: mpsc::Sender<LoopMsg>,
    thread: Option<JoinHandle<()>>,
}

/// Go `NewZookeeperTreeCache`: starts the cache loop for `path`.
pub fn new_zookeeper_tree_cache(conn: Conn, path: &str, events: EventSink, logger: Logger) -> ZookeeperTreeCache {
    let (tx, rx) = mpsc::channel();
    let cx = Cx { conn, events, tx: tx.clone(), logger };
    let prefix = path.to_string();
    let thread = std::thread::Builder::new().name("zk-treecache".to_string()).spawn(move || run_loop(cx, prefix, rx)).ok();
    ZookeeperTreeCache { tx, thread }
}

impl ZookeeperTreeCache {
    /// Go `Stop`: stops the loop and every watch forwarding; waits for the loop to end.
    pub fn stop(&mut self) {
        let _ = self.tx.send(LoopMsg::Stop);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl Drop for ZookeeperTreeCache {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Go `loop`.
fn run_loop(cx: Cx, prefix: String, rx: mpsc::Receiver<LoopMsg>) {
    let mut head = Node { stopped: true, ..Default::default() };
    let mut failure_mode = false;
    let mut retry_at: Option<Instant> = None;
    let failure = |failure_mode: &mut bool, retry_at: &mut Option<Instant>| {
        failure_counter().get().inc();
        *failure_mode = true;
        *retry_at = Some(Instant::now() + Duration::from_secs(10));
    };

    if let Err(e) = recursive_node_update(&cx, &prefix, &mut head, true) {
        cx.logger.error("Error during initial read of Zookeeper", &[("err", &e)]);
        failure(&mut failure_mode, &mut retry_at);
    }

    loop {
        let msg = match retry_at {
            Some(t) => match rx.recv_timeout(t.saturating_duration_since(Instant::now())) {
                Ok(m) => Some(m),
                Err(mpsc::RecvTimeoutError::Timeout) => None,
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            },
            None => match rx.recv() {
                Ok(m) => Some(m),
                Err(_) => return,
            },
        };
        match msg {
            Some(LoopMsg::Zk(ev)) => {
                cx.logger.debug("Received Zookeeper event", &[("event", &ev.to_string())]);
                if failure_mode {
                    continue;
                }
                if ev.type_ == EventType::NOT_WATCHING {
                    cx.logger.info("Lost connection to Zookeeper.", &[]);
                    failure(&mut failure_mode, &mut retry_at);
                    continue;
                }
                let rel = ev.path.strip_prefix(prefix.as_str()).unwrap_or(&ev.path);
                let parts: Vec<&str> = rel.split('/').collect();
                let mut node = &mut head;
                for part in &parts[1..] {
                    node = node.children.entry(part.to_string()).or_default();
                }
                let r = recursive_node_update(&cx, &ev.path, node, parts.len() <= 1);
                match r {
                    Err(e) => {
                        cx.logger.error("Error during processing of Zookeeper event", &[("err", &e)]);
                        failure(&mut failure_mode, &mut retry_at);
                    }
                    Ok(()) if head.data.is_none() => {
                        cx.logger.error("Error during processing of Zookeeper event", &[("err", "path no longer exists"), ("path", &prefix)]);
                        failure(&mut failure_mode, &mut retry_at);
                    }
                    Ok(()) => {}
                }
            }
            None => {
                retry_at = None;
                cx.logger.info("Attempting to resync state with Zookeeper", &[]);
                // Reset the root's children before traversing the path again.
                let mut previous = Node { children: std::mem::take(&mut head.children), ..Default::default() };
                match recursive_node_update(&cx, &prefix, &mut head, true) {
                    Err(e) => {
                        cx.logger.error("Error during Zookeeper resync", &[("err", &e)]);
                        // Revert to the previous state.
                        head.children = previous.children;
                        failure(&mut failure_mode, &mut retry_at);
                    }
                    Ok(()) => {
                        resync_state(&cx, &prefix, &head, &mut previous);
                        cx.logger.info("Zookeeper resync successful", &[]);
                        failure_mode = false;
                    }
                }
            }
            Some(LoopMsg::Stop) => {
                // Stop head as well.
                head.signal_done();
                recursive_stop(&mut head);
                return;
            }
        }
    }
}

/// Go `recursiveNodeUpdate`: reads the node's data and children with watches, recursing into new children.
fn recursive_node_update(cx: &Cx, path: &str, node: &mut Node, is_head: bool) -> Result<(), String> {
    let reg = Arc::new(Reg { st: Mutex::new(RegSt::default()), tx: cx.tx.clone() });
    let r1 = reg.clone();
    match cx.conn.get_w_fn(path, Box::new(move |ev| r1.on_event(ev))) {
        Err(zk::Error::NoNode) => {
            recursive_delete(cx, path, node);
            if is_head {
                return Err(format!("path {path} does not exist"));
            }
            return Ok(());
        }
        Err(e) => return Err(e.to_string()),
        Ok((data, _)) => {
            if node.data.as_deref() != Some(&data[..]) {
                node.data = Some(data.clone());
                cx.emit(path, Some(data));
            }
        }
    }

    let r2 = reg.clone();
    let children = match cx.conn.children_w_fn(path, Box::new(move |ev| r2.on_event(ev))) {
        Err(zk::Error::NoNode) => {
            recursive_delete(cx, path, node);
            return Ok(());
        }
        Err(e) => return Err(e.to_string()),
        Ok((c, _)) => c,
    };

    let mut current: HashSet<&str> = HashSet::new();
    for child in &children {
        current.insert(child);
        // New, or a previous watch on it triggered.
        if node.children.get(child).is_none_or(|c| c.stopped) {
            node.children.insert(child.clone(), Node::default());
            let c = node.children.get_mut(child).unwrap();
            recursive_node_update(cx, &format!("{path}/{child}"), c, false)?;
        }
    }

    // Remove nodes that no longer exist.
    let gone: Vec<String> = node.children.keys().filter(|n| !current.contains(n.as_str()) || node.data.is_none()).cloned().collect();
    for name in gone {
        if let Some(mut c) = node.children.remove(&name) {
            recursive_delete(cx, &format!("{path}/{name}"), &mut c);
        }
    }

    // Pass up zookeeper events, until the node is deleted.
    node.arm(reg);
    Ok(())
}

/// Go `resyncState`.
fn resync_state(cx: &Cx, path: &str, current: &Node, previous: &mut Node) {
    for (child, prev) in previous.children.iter_mut() {
        match current.children.get(child) {
            Some(cur) => resync_state(cx, &format!("{path}/{child}"), cur, prev),
            None => recursive_delete(cx, &format!("{path}/{child}"), prev),
        }
    }
}

/// Go `recursiveDelete`.
fn recursive_delete(cx: &Cx, path: &str, node: &mut Node) {
    if !node.stopped {
        node.signal_done();
        node.stopped = true;
    }
    if node.data.is_some() {
        cx.emit(path, None);
        node.data = None;
    }
    for (name, child) in node.children.iter_mut() {
        recursive_delete(cx, &format!("{path}/{name}"), child);
    }
}

/// Go `recursiveStop`.
fn recursive_stop(node: &mut Node) {
    if !node.stopped {
        node.signal_done();
        node.stopped = true;
    }
    for child in node.children.values_mut() {
        recursive_stop(child);
    }
}
