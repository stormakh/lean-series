//! Port of `util/zeropool`: a type-safe pool of items.
//!
//! Go needs this package to avoid an allocation per `sync.Pool.Put` of a non-pointer value. A Rust pool stores
//! values directly, so `put` and `get` do not allocate once the pool's storage has grown to its working size.
//! Go's `sync.Pool` drops pooled items at garbage collection; this pool keeps them until they are taken.

use std::sync::Mutex;

/// Go `Pool[T]`. The default value is valid: it returns `T::default()` when nothing is pooled.
pub struct Pool<T> {
    items: Mutex<Vec<T>>,
    item: Option<Box<dyn Fn() -> T + Send + Sync>>,
}

impl<T> Default for Pool<T> {
    fn default() -> Self {
        Pool { items: Mutex::new(Vec::new()), item: None }
    }
}

impl<T: Default> Pool<T> {
    /// Go `New`: a pool with the given function to create new items.
    pub fn new(item: impl Fn() -> T + Send + Sync + 'static) -> Pool<T> {
        Pool { items: Mutex::new(Vec::new()), item: Some(Box::new(item)) }
    }

    /// Go `Get`: an item from the pool, creating a new one if necessary. Safe for concurrent use.
    pub fn get(&self) -> T {
        if let Some(v) = self.items.lock().unwrap_or_else(|e| e.into_inner()).pop() {
            return v;
        }
        match &self.item {
            Some(f) => f(),
            // The zero value of the pool returns the zero value of T.
            None => T::default(),
        }
    }

    /// Go `Put`: adds an item to the pool.
    pub fn put(&self, item: T) {
        self.items.lock().unwrap_or_else(|e| e.into_inner()).push(item);
    }
}
