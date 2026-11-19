//! Port of `util/logging/dedupe.go`: drops a line when a line with the same message was written less than
//! `repeat` ago.
use super::Logger;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const GARBAGE_COLLECT_EVERY: Duration = Duration::from_secs(10);
const EXPIRE_ENTRIES_AFTER: Duration = Duration::from_secs(60);
const MAX_ENTRIES: usize = 1024;

pub(super) struct State {
    repeat: Duration,
    inner: Mutex<Inner>,
}

struct Inner {
    seen: HashMap<String, Instant>,
    last_gc: Instant,
}

impl State {
    /// True when the message may be written now. Old entries are removed every 10s while lines arrive (Go runs a
    /// ticker goroutine for this; here it happens on the logging call, so an idle logger never wakes up).
    pub(super) fn admit(&self, msg: &str) -> bool {
        let now = Instant::now();
        let mut g = match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        if now.duration_since(g.last_gc) >= GARBAGE_COLLECT_EVERY {
            g.seen.retain(|_, t| now.duration_since(*t) <= EXPIRE_ENTRIES_AFTER);
            g.last_gc = now;
        }
        if let Some(t) = g.seen.get(msg) {
            if now.duration_since(*t) < self.repeat {
                return false;
            }
        }
        if g.seen.len() < MAX_ENTRIES {
            g.seen.insert(msg.to_string(), now);
        }
        true
    }
}

/// Go `Dedupe`: a logger that writes to `next`, but repeats a message only every `repeat`. Handles made from it
/// with `with` share the same memory of seen messages. There is no background goroutine, so nothing to stop.
pub fn dedupe(next: &Logger, repeat: Duration) -> Logger {
    let mut l = next.clone();
    l.dedupe = Some(Arc::new(State { repeat, inner: Mutex::new(Inner { seen: HashMap::new(), last_gc: Instant::now() }) }));
    l
}
