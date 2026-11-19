//! Port of `tsdb/agent/series_test.go`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex, mpsc};
use std::time::Duration;

use agent::{MemSeries, StripeSeries};
use model::labels::{self, Labels};

/// Waits for `f` to finish in another thread, failing after 15 seconds (Go's `time.After(15 * time.Second)`).
fn within_15s(name: &str, f: impl FnOnce() + Send + 'static) {
    let (tx, rx) = mpsc::channel();
    let h = std::thread::spawn(move || {
        f();
        let _ = tx.send(());
    });
    match rx.recv_timeout(Duration::from_secs(15)) {
        Ok(()) => h.join().unwrap(),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            // The closure panicked: propagate.
            if let Err(e) = h.join() {
                std::panic::resume_unwind(e);
            }
        }
        Err(mpsc::RecvTimeoutError::Timeout) => panic!("{name}"),
    }
}

#[test]
fn test_no_deadlock() {
    const NUM_WORKERS: usize = 1000;
    within_15s("deadlock detected", || {
        let stripe_series = Arc::new(StripeSeries::new(3));
        let started = Arc::new(Barrier::new(2 * NUM_WORKERS));
        let mut hs = Vec::new();
        for _ in 0..NUM_WORKERS {
            let (ss, started) = (stripe_series.clone(), started.clone());
            hs.push(std::thread::spawn(move || {
                started.wait();
                let _ = ss.gc(i64::MAX, false);
            }));
        }
        for i in 0..NUM_WORKERS {
            let (ss, started) = (stripe_series.clone(), started.clone());
            hs.push(std::thread::spawn(move || {
                started.wait();
                let lset = labels::from_strings(&["id", &i.to_string()]);
                let series = MemSeries::new(i as u64, lset.clone(), 0);
                ss.set_unless_already_set(lset.hash(), series);
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
    });
}

fn labels_with_hash_collision() -> (Labels, Labels) {
    // These two series have the same XXHash; thanks to https://github.com/pstibrany/labels_hash_collisions
    let mut ls1 = labels::from_strings(&["__name__", "metric", "lbl", "HFnEaGl"]);
    let mut ls2 = labels::from_strings(&["__name__", "metric", "lbl", "RqcXatm"]);

    if ls1.hash() != ls2.hash() {
        // These ones are the same when using -tags slicelabels.
        ls1 = labels::from_strings(&["__name__", "metric", "lbl1", "value", "lbl2", "l6CQ5y"]);
        ls2 = labels::from_strings(&["__name__", "metric", "lbl1", "value", "lbl2", "v7uDlF"]);
    }
    if ls1.hash() != ls2.hash() {
        panic!("This code needs to be updated: find new labels with colliding hash values.");
    }
    (ls1, ls2)
}

/// Go `stripeSeriesWithCollidingSeries`: a stripeSeries with two series having the same, colliding, hash.
fn stripe_series_with_colliding_series() -> (StripeSeries, Arc<MemSeries>, Arc<MemSeries>) {
    let (lbls1, lbls2) = labels_with_hash_collision();
    let ms1 = MemSeries::new(1, lbls1.clone(), 0);
    let ms2 = MemSeries::new(2, lbls2, 0);
    let hash = lbls1.hash();
    let s = StripeSeries::new(1);
    s.set_unless_already_set(hash, ms1.clone());
    s.set_unless_already_set(hash, ms2.clone());
    (s, ms1, ms2)
}

#[test]
fn test_stripe_series_get() {
    let (s, ms1, ms2) = stripe_series_with_colliding_series();
    let hash = ms1.lset.hash();

    // Verify that we can get both of the series despite the hash collision.
    let got = s.get_by_hash(hash, &ms1.lset).unwrap();
    assert!(Arc::ptr_eq(&ms1, &got));
    let got = s.get_by_hash(hash, &ms2.lset).unwrap();
    assert!(Arc::ptr_eq(&ms2, &got));
}

#[test]
fn test_stripe_series_set_unless_already_set() {
    let lbl = labels::from_strings(&["__name__", "metric", "lbl", "HFnEaGl"]);
    let ss = StripeSeries::new(1);

    let (ms, created) = ss.set_unless_already_set(lbl.hash(), MemSeries::new(1, lbl.clone(), 0));
    assert!(created);
    assert_eq!(lbl, ms.lset);

    let (ms2, created) = ss.set_unless_already_set(lbl.hash(), MemSeries::new(2, lbl.clone(), 0));
    assert!(!created);
    // Go require.Equal on the pointers compares the pointed-to series (same ref, labels and fields).
    assert_eq!(ms.r#ref, ms2.r#ref);
    assert_eq!(ms.lset, ms2.lset);
    assert_eq!(ms.last_sample_timestamp(), ms2.last_sample_timestamp());
    assert_eq!(ms.metadata(), ms2.metadata());
}

/// Concurrent set_unless_already_set calls for the same label set produce exactly one canonical series: all
/// callers get the same pointer, the winning ref is reachable by id, and losing refs are unreachable.
#[test]
fn test_set_unless_already_set_concurrent_same_labels() {
    // size=1 forces all threads into the same hash bucket.
    let ss = Arc::new(StripeSeries::new(1));
    let lset = labels::from_strings(&["__name__", "test_metric"]);
    let hash = lset.hash();

    const N: usize = 100;
    let start = Arc::new(Barrier::new(N));
    let results: Arc<Mutex<Vec<Option<Arc<MemSeries>>>>> = Arc::new(Mutex::new(vec![None; N]));
    let mut hs = Vec::new();
    for i in 0..N {
        let (ss, start, results, lset) = (ss.clone(), start.clone(), results.clone(), lset.clone());
        hs.push(std::thread::spawn(move || {
            start.wait();
            let (s, _) = ss.set_unless_already_set(hash, MemSeries::new(i as u64 + 1, lset, 0));
            results.lock().unwrap()[i] = Some(s);
        }));
    }
    for h in hs {
        h.join().unwrap();
    }

    let results: Vec<Arc<MemSeries>> = results.lock().unwrap().iter().map(|r| r.clone().unwrap()).collect();
    let canonical = &results[0];
    for r in &results[1..] {
        assert!(Arc::ptr_eq(canonical, r));
    }
    assert!(Arc::ptr_eq(canonical, &ss.get_by_id(canonical.r#ref).unwrap()));
    for i in 0..N {
        let r = i as u64 + 1;
        if r != canonical.r#ref {
            assert!(ss.get_by_id(r).is_none());
        }
    }
}

/// Concurrent set_unless_already_set and gc do not deadlock, surviving series remain reachable, and
/// GC-eligible series are removed.
#[test]
fn test_set_unless_already_set_concurrent_gc() {
    within_15s("deadlock", || {
        let ss = Arc::new(StripeSeries::new(512));
        let survivors: Arc<Mutex<Vec<Arc<MemSeries>>>> = Arc::default();
        let eligible: Arc<Mutex<Vec<Arc<MemSeries>>>> = Arc::default();
        let start = Arc::new(Barrier::new(50));

        let done = Arc::new(AtomicBool::new(false));
        let gc = {
            let (ss, done) = (ss.clone(), done.clone());
            std::thread::spawn(move || {
                while !done.load(Ordering::SeqCst) {
                    ss.gc(1, false); // Removes series with lastTs < 1, i.e. lastTs == 0.
                }
            })
        };

        let mut hs = Vec::new();
        for w in 0..50usize {
            let (ss, start, survivors, eligible) = (ss.clone(), start.clone(), survivors.clone(), eligible.clone());
            hs.push(std::thread::spawn(move || {
                start.wait();
                for r in 0..20usize {
                    let lset = labels::from_strings(&["w", &w.to_string(), "r", &r.to_string()]);
                    // Odd r: survivor (lastTs = MaxInt64). Even r: GC-eligible (lastTs = 0, removed by gc(1)).
                    let last_ts = if r % 2 == 1 { i64::MAX } else { 0 };
                    let s = MemSeries::new((w * 20 + r + 1) as u64, lset.clone(), last_ts);
                    let (got, ok) = ss.set_unless_already_set(lset.hash(), s);
                    if ok {
                        if last_ts == i64::MAX {
                            survivors.lock().unwrap().push(got);
                        } else {
                            eligible.lock().unwrap().push(got);
                        }
                    }
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        done.store(true, Ordering::SeqCst);
        gc.join().unwrap();

        // Survivors must still be reachable by both ID and hash despite concurrent GC.
        for s in survivors.lock().unwrap().iter() {
            assert!(Arc::ptr_eq(s, &ss.get_by_id(s.r#ref).unwrap()));
            assert!(Arc::ptr_eq(s, &ss.get_by_hash(s.lset.hash(), &s.lset).unwrap()));
        }

        // A final synchronous GC pass removes all eligible series; they are unreachable by both lookups.
        ss.gc(1, false);
        for s in eligible.lock().unwrap().iter() {
            assert!(ss.get_by_id(s.r#ref).is_none());
            assert!(ss.get_by_hash(s.lset.hash(), &s.lset).is_none());
        }
    });
}

#[test]
fn test_stripe_series_gc() {
    let (s, ms1, ms2) = stripe_series_with_colliding_series();
    let hash = ms1.lset.hash();

    s.gc(1, false);

    // Verify that we can get neither ms1 nor ms2 after gc-ing the corresponding series.
    assert!(s.get_by_hash(hash, &ms1.lset).is_none());
    assert!(s.get_by_hash(hash, &ms2.lset).is_none());
}
