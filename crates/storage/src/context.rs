//! A cheap replacement for Go's `context.Context` that PromQL, storage and TSDB share: cancellation and an
//! optional deadline, checked by polling (no timers, no threads).
//!
//! A `Context` is a cheap clone (one `Arc`). A child made with [`Context::with_cancel`] or
//! [`Context::with_deadline`] is done when it or any parent is done; cancelling a child does not cancel the parent.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{Duration, Instant};

use crate::error::{Error, Sentinel};

static CANCELED: Sentinel = Sentinel::new("context canceled");
/// Go `context.Canceled`.
pub const ERR_CANCELED: Error = Error::Sentinel(&CANCELED);
static DEADLINE_EXCEEDED: Sentinel = Sentinel::new("context deadline exceeded");
/// Go `context.DeadlineExceeded`.
pub const ERR_DEADLINE_EXCEEDED: Error = Error::Sentinel(&DEADLINE_EXCEEDED);

const NOT_DONE: u8 = 0;
const DONE_CANCELED: u8 = 1;
const DONE_DEADLINE: u8 = 2;

struct Inner {
    state: AtomicU8,
    deadline: Option<Instant>,
    parent: Option<Arc<Inner>>,
    /// A value attached with [`Context::with_value`] (Go `context.WithValue`), keyed by its type.
    value: Option<Arc<dyn std::any::Any + Send + Sync>>,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner")
            .field("state", &self.state)
            .field("deadline", &self.deadline)
            .field("parent", &self.parent)
            .field("value", &self.value.is_some())
            .finish()
    }
}

impl Inner {
    fn done(&self) -> u8 {
        let s = self.state.load(Ordering::Relaxed);
        if s != NOT_DONE {
            return s;
        }
        if let Some(d) = self.deadline
            && Instant::now() >= d
        {
            // Remember it, so later checks are one atomic load.
            let _ = self.state.compare_exchange(
                NOT_DONE,
                DONE_DEADLINE,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            return self.state.load(Ordering::Relaxed);
        }
        match &self.parent {
            Some(p) => p.done(),
            None => NOT_DONE,
        }
    }
}

/// A cancellation and deadline scope (Go `context.Context`).
#[derive(Clone, Debug)]
pub struct Context {
    inner: Option<Arc<Inner>>,
}

/// Cancels the [`Context`] it was made with (Go `context.CancelFunc`). Clones cancel the same context.
#[derive(Clone, Debug)]
pub struct CancelToken {
    inner: Arc<Inner>,
}

impl CancelToken {
    /// Cancels the context and its children. Cancelling twice has no effect.
    pub fn cancel(&self) {
        let _ = self.inner.state.compare_exchange(
            NOT_DONE,
            DONE_CANCELED,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }
}

impl Default for Context {
    fn default() -> Context {
        Context::background()
    }
}

impl Context {
    /// A context that is never done (Go `context.Background()`).
    pub const fn background() -> Context {
        Context { inner: None }
    }

    /// A child context and the token that cancels it (Go `context.WithCancel`).
    pub fn with_cancel(&self) -> (Context, CancelToken) {
        self.child(None)
    }

    /// A child context that is done at `deadline` (Go `context.WithDeadline`). The earlier deadline wins.
    pub fn with_deadline(&self, deadline: Instant) -> (Context, CancelToken) {
        let d = match self.deadline() {
            Some(p) if p < deadline => p,
            _ => deadline,
        };
        self.child(Some(d))
    }

    /// A child context that is done after `timeout` (Go `context.WithTimeout`).
    pub fn with_timeout(&self, timeout: Duration) -> (Context, CancelToken) {
        self.with_deadline(Instant::now() + timeout)
    }

    fn child(&self, deadline: Option<Instant>) -> (Context, CancelToken) {
        let inner = Arc::new(Inner {
            state: AtomicU8::new(NOT_DONE),
            deadline,
            parent: self.inner.clone(),
            value: None,
        });
        (
            Context {
                inner: Some(inner.clone()),
            },
            CancelToken { inner },
        )
    }

    /// A child context that carries `v` (Go `context.WithValue` with the type of `v` as the key). It is done when
    /// the parent is done.
    pub fn with_value<T: std::any::Any + Send + Sync>(&self, v: T) -> Context {
        let inner = Arc::new(Inner {
            state: AtomicU8::new(NOT_DONE),
            deadline: None,
            parent: self.inner.clone(),
            value: Some(Arc::new(v)),
        });
        Context { inner: Some(inner) }
    }

    /// The nearest value of type `T` attached with [`Context::with_value`] (Go `ctx.Value(key)`).
    pub fn value<T: std::any::Any + Send + Sync>(&self) -> Option<&T> {
        let mut cur = self.inner.as_deref();
        while let Some(i) = cur {
            if let Some(v) = i.value.as_ref().and_then(|v| v.downcast_ref::<T>()) {
                return Some(v);
            }
            cur = i.parent.as_deref();
        }
        None
    }

    /// The deadline of this context or of its nearest parent with one.
    pub fn deadline(&self) -> Option<Instant> {
        let mut cur = self.inner.as_deref();
        while let Some(i) = cur {
            if i.deadline.is_some() {
                return i.deadline;
            }
            cur = i.parent.as_deref();
        }
        None
    }

    /// Whether the context is cancelled or past its deadline.
    #[inline]
    pub fn is_done(&self) -> bool {
        match &self.inner {
            None => false,
            Some(i) => i.done() != NOT_DONE,
        }
    }

    /// Go `ctx.Err()`: `None`, [`ERR_CANCELED`] or [`ERR_DEADLINE_EXCEEDED`].
    pub fn err(&self) -> Option<Error> {
        match &self.inner {
            None => None,
            Some(i) => match i.done() {
                DONE_CANCELED => Some(ERR_CANCELED),
                DONE_DEADLINE => Some(ERR_DEADLINE_EXCEEDED),
                _ => None,
            },
        }
    }
}
