//! Port of `util/pool/pool_test.go` and `util/zeropool/pool_test.go` (both Go files are `pool_test.go` with a
//! `TestPool`, so each is a module here).

mod pool {
    use util::pool::Pool;

    fn make_func(size: usize) -> Vec<i64> {
        Vec::with_capacity(size)
    }

    #[test]
    fn test_pool() {
        let test_pool = Pool::new(1, 8, 2.0, make_func);
        let cases: &[(i64, usize)] = &[(-1, 1), (3, 4), (10, 10)];
        for &(size, expected_cap) in cases {
            let ret = test_pool.get(size);
            assert_eq!(expected_cap, ret.capacity(), "size {size}");
            test_pool.put(ret);
        }
    }
}

mod zeropool {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};

    use util::zeropool::Pool;

    #[test]
    fn test_pool() {
        // provides correct values
        {
            let pool = Pool::new(|| vec![0u8; 1024]);
            let mut item1 = pool.get();
            assert_eq!(item1.len(), 1024);
            let mut item2 = pool.get();
            assert_eq!(item2.len(), 1024);
            pool.put(item1);
            pool.put(item2);
            item1 = pool.get();
            assert_eq!(item1.len(), 1024);
            item2 = pool.get();
            assert_eq!(item2.len(), 1024);
        }

        // is not racy
        {
            let pool = Arc::new(Pool::new(|| vec![0u8; 1024]));
            const ITERATIONS: usize = 1_000_000;
            const CONCURRENCY: usize = u8::MAX as usize;
            let counter = Arc::new(AtomicI64::new(0));
            // Go fills a channel with one token per iteration; workers take tokens until it is empty.
            let tokens = Arc::new(AtomicI64::new(ITERATIONS as i64));
            let start = Arc::new(std::sync::Barrier::new(CONCURRENCY));
            let mut handles = Vec::new();
            for worker in 0..CONCURRENCY {
                let (pool, counter, tokens, start) = (pool.clone(), counter.clone(), tokens.clone(), start.clone());
                handles.push(std::thread::spawn(move || {
                    start.wait();
                    while tokens.fetch_sub(1, Ordering::AcqRel) > 0 {
                        let mut item = pool.get();
                        item[0] = worker as u8;
                        // Counts and also adds some delay to add raciness.
                        counter.fetch_add(1, Ordering::Relaxed);
                        if item[0] != worker as u8 {
                            panic!("wrong value");
                        }
                        pool.put(item);
                    }
                }));
            }
            for h in handles {
                h.join().expect("worker");
            }
            assert_eq!(counter.load(Ordering::Relaxed), ITERATIONS as i64);
        }

        // does not allocate
        // adapted: Go counts allocations with testing.AllocsPerRun (< 1 per Get/Put pair). Here the same buffer
        // must come back on every Get after the warm-up, which means no new item is made.
        {
            let pool = Pool::new(|| vec![0u8; 1024]);
            // Warm up, this will allocate one slice.
            let slice = pool.get();
            let ptr = slice.as_ptr();
            pool.put(slice);
            for _ in 0..1000 {
                let slice = pool.get();
                assert_eq!(slice.as_ptr(), ptr, "Should not allocate.");
                pool.put(slice);
            }
        }

        // zero value is valid
        {
            let pool: Pool<Vec<u8>> = Pool::default();
            let slice = pool.get();
            assert!(slice.is_empty());
            pool.put(slice);
            // adapted: as above, the pooled value comes back on every Get; for an empty Vec Rust never allocates.
            let slice = pool.get();
            assert_eq!(slice.capacity(), 0);
            pool.put(slice);
            for _ in 0..1000 {
                let slice = pool.get();
                assert_eq!(slice.capacity(), 0, "Should not allocate.");
                pool.put(slice);
            }
        }
    }
}
