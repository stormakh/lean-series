//! Port of `storage/remote/intern.go`: a reference counted string interner.
//!
//! Inspired / copied / modified from https://gitlab.com/cznic/strutil/blob/master/strutil.go,
//! which is MIT licensed, so:
//!
//! Copyright (c) 2014 The strutil Authors. All rights reserved.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use discovery::metrics::{CounterVec, MetricVec, Opts};

/// Go `noReferenceReleases` (`prometheus_remote_storage_string_interner_zero_reference_releases_total`), a
/// package-level counter (Go registers it with `promauto`; see `write::register_global_metrics`).
pub fn no_reference_releases() -> &'static Arc<CounterVec> {
    static C: OnceLock<Arc<CounterVec>> = OnceLock::new();
    C.get_or_init(|| {
        MetricVec::new_counter(Opts::new(
            "prometheus_remote_storage_string_interner_zero_reference_releases_total",
            "The number of times release has been called for strings that are not interned.",
        ))
    })
}

struct Entry {
    refs: AtomicI64,
    s: Arc<str>,
}

/// Go `pool`.
#[derive(Default)]
pub struct Pool {
    pool: RwLock<HashMap<Arc<str>, Arc<Entry>>>,
}

impl Pool {
    /// Go `newPool`.
    pub fn new() -> Pool {
        Pool::default()
    }

    /// Interns `s` and returns the shared string.
    pub fn intern(&self, s: &str) -> Arc<str> {
        if s.is_empty() {
            return Arc::from("");
        }
        {
            let p = self.pool.read().unwrap_or_else(|e| e.into_inner());
            if let Some(interned) = p.get(s) {
                // Increase the reference count while we're still holding the read lock,
                // This will prevent the release() from deleting the entry while we're increasing its ref count.
                interned.refs.fetch_add(1, Ordering::SeqCst);
                return interned.s.clone();
            }
        }
        let mut p = self.pool.write().unwrap_or_else(|e| e.into_inner());
        if let Some(interned) = p.get(s) {
            interned.refs.fetch_add(1, Ordering::SeqCst);
            return interned.s.clone();
        }
        let a: Arc<str> = Arc::from(s);
        p.insert(a.clone(), Arc::new(Entry { refs: AtomicI64::new(1), s: a.clone() }));
        a
    }

    /// Drops one reference of `s`, forgetting it at zero.
    pub fn release(&self, s: &str) {
        let interned = {
            let p = self.pool.read().unwrap_or_else(|e| e.into_inner());
            p.get(s).cloned()
        };
        let Some(interned) = interned else {
            no_reference_releases().get().inc();
            return;
        };
        let refs = interned.refs.fetch_sub(1, Ordering::SeqCst) - 1;
        if refs > 0 {
            return;
        }
        let mut p = self.pool.write().unwrap_or_else(|e| e.into_inner());
        if interned.refs.load(Ordering::SeqCst) != 0 {
            return;
        }
        p.remove(s);
    }

    /// The reference count of an interned string, `None` when it is not interned (Go reads `pool[s].refs`).
    pub fn refs(&self, s: &str) -> Option<i64> {
        self.pool.read().unwrap_or_else(|e| e.into_inner()).get(s).map(|e| e.refs.load(Ordering::SeqCst))
    }
}
