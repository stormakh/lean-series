//! A minimal stand-in for Go's `context.Context` (only cancellation), used by index and postings helpers.
// TODO(storage): use storage::Context after merge.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// The error text of Go's `context.Canceled`.
pub const CANCELED: &str = "context canceled";

/// A cancellable context. Cheap to clone; all clones share the cancel flag.
#[derive(Clone, Debug, Default)]
pub struct Context {
    canceled: Option<Arc<AtomicBool>>,
}

impl Context {
    /// Go `context.Background()`: never canceled.
    pub fn background() -> Context {
        Context { canceled: None }
    }

    /// Go `context.WithCancel`: a context and its cancel handle.
    pub fn with_cancel() -> (Context, CancelFunc) {
        let flag = Arc::new(AtomicBool::new(false));
        (Context { canceled: Some(flag.clone()) }, CancelFunc(flag))
    }

    /// Go `ctx.Err()`: `Some(CANCELED)` once canceled.
    pub fn err(&self) -> Option<&'static str> {
        match &self.canceled {
            Some(f) if f.load(Ordering::Relaxed) => Some(CANCELED),
            _ => None,
        }
    }
}

/// Cancels the context it came from.
#[derive(Clone, Debug)]
pub struct CancelFunc(Arc<AtomicBool>);

impl CancelFunc {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Go `util.checkContextEveryNIterations`: check for cancellation every this many iterations.
pub const CHECK_CONTEXT_EVERY_N_ITERATIONS: usize = 100;
