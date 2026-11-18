//! A cancel token: the part of Go's `context.Context` that discoverers need (`ctx.Done()`), for blocking
//! threads. Cancelling wakes every thread that waits on the token, cancels its children and runs the hooks
//! that other blocking primitives (the update channel) register to wake their own waiters.
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

type Hook = Box<dyn FnOnce() + Send>;

struct State {
    cancelled: bool,
    hooks: Vec<Hook>,
}

struct Inner {
    state: Mutex<State>,
    cv: Condvar,
}

/// Cloning shares the token.
#[derive(Clone)]
pub struct CancelToken(Arc<Inner>);

impl Default for CancelToken {
    fn default() -> Self {
        CancelToken::new()
    }
}

impl std::fmt::Debug for CancelToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CancelToken(cancelled={})", self.is_cancelled())
    }
}

impl CancelToken {
    /// A token that is never cancelled unless `cancel` is called (Go `context.Background()` + `WithCancel`).
    pub fn new() -> CancelToken {
        CancelToken(Arc::new(Inner { state: Mutex::new(State { cancelled: false, hooks: Vec::new() }), cv: Condvar::new() }))
    }

    /// A new token that is cancelled with this one (Go `context.WithCancel(ctx)`).
    pub fn child(&self) -> CancelToken {
        let c = CancelToken::new();
        let weak: Weak<Inner> = Arc::downgrade(&c.0);
        self.on_cancel(Box::new(move || {
            if let Some(i) = weak.upgrade() {
                CancelToken(i).cancel();
            }
        }));
        c
    }

    /// Cancels the token. Idempotent.
    pub fn cancel(&self) {
        let hooks = {
            let mut s = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
            if s.cancelled {
                return;
            }
            s.cancelled = true;
            std::mem::take(&mut s.hooks)
        };
        self.0.cv.notify_all();
        for h in hooks {
            h();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.state.lock().unwrap_or_else(|e| e.into_inner()).cancelled
    }

    /// Runs `f` once when the token is cancelled (at once if it already is).
    pub fn on_cancel(&self, f: Hook) {
        let mut s = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        if s.cancelled {
            drop(s);
            f();
            return;
        }
        s.hooks.push(f);
    }

    /// Blocks until the token is cancelled (Go `<-ctx.Done()`).
    pub fn wait(&self) {
        let mut s = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        while !s.cancelled {
            s = self.0.cv.wait(s).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Sleeps for `d` or until cancelled. Returns true if the token was cancelled (Go
    /// `select { case <-ctx.Done(): case <-time.After(d): }`).
    pub fn wait_timeout(&self, d: Duration) -> bool {
        let deadline = Instant::now() + d;
        let mut s = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if s.cancelled {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            s = self.0.cv.wait_timeout(s, deadline - now).unwrap_or_else(|e| e.into_inner()).0;
        }
    }
}
