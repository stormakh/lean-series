//! Port of `util/pool`: a bucketed pool for variably sized slices.
//!
//! Go stores `any` and checks with reflection that a slice is put; here the item type implements [`Slice`].
//! Go's `sync.Pool` drops pooled items at garbage collection; this pool keeps them until they are taken.

use std::sync::Mutex;

/// A slice-like item: what Go's reflection reads (`Cap`) and does (`Slice(0, 0)`) on the put value.
pub trait Slice {
    /// Go `cap(s)`.
    fn capacity(&self) -> usize;
    /// Go `s[:0]`: empties the item and keeps its storage.
    fn truncate_to_empty(&mut self);
}

impl<T> Slice for Vec<T> {
    fn capacity(&self) -> usize {
        Vec::capacity(self)
    }
    fn truncate_to_empty(&mut self) {
        self.clear();
    }
}

/// Go `Pool`: a bucketed pool for variably sized slices.
pub struct Pool<T> {
    buckets: Vec<Mutex<Vec<T>>>,
    sizes: Vec<usize>,
    /// The function used to create an empty slice when none exist yet.
    make: Box<dyn Fn(usize) -> T + Send + Sync>,
}

impl<T: Slice> Pool<T> {
    /// Go `New`: a pool with size buckets for `min_size` to `max_size`, increasing by `factor`.
    pub fn new(min_size: i64, max_size: i64, factor: f64, make: impl Fn(usize) -> T + Send + Sync + 'static) -> Pool<T> {
        if min_size < 1 {
            panic!("invalid minimum pool size");
        }
        if max_size < 1 {
            panic!("invalid maximum pool size");
        }
        if factor < 1.0 {
            panic!("invalid factor");
        }
        let mut sizes = Vec::new();
        let mut s = min_size;
        while s <= max_size {
            sizes.push(s as usize);
            s = (s as f64 * factor) as i64;
        }
        Pool { buckets: sizes.iter().map(|_| Mutex::new(Vec::new())).collect(), sizes, make: Box::new(make) }
    }

    /// Go `Get`: a slice that fits the given size.
    pub fn get(&self, sz: i64) -> T {
        for (i, &bkt_size) in self.sizes.iter().enumerate() {
            if sz > bkt_size as i64 {
                continue;
            }
            let b = self.buckets[i].lock().unwrap_or_else(|e| e.into_inner()).pop();
            return b.unwrap_or_else(|| (self.make)(bkt_size));
        }
        (self.make)(sz as usize)
    }

    /// Go `Put`: adds a slice to the right bucket in the pool.
    pub fn put(&self, mut s: T) {
        for (i, &size) in self.sizes.iter().enumerate() {
            if s.capacity() > size {
                continue;
            }
            s.truncate_to_empty();
            self.buckets[i].lock().unwrap_or_else(|e| e.into_inner()).push(s);
            return;
        }
    }
}
