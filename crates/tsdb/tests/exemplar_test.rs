//! Port of `tsdb/exemplar_test.go`. The Go tests read unexported fields of the storage (`nextIndex`,
//! `exemplars[i]`, `index`); here the `debug_*` helpers of the storage give the same values.

use std::sync::{Arc, Barrier, LazyLock};

use model::exemplar::{Exemplar, EXEMPLAR_MAX_LABEL_SET_LENGTH};
use model::labels::{self, Labels, METRIC_NAME};
use model::matcher::{MatchType, Matcher};
use storage::{Context, ERR_DUPLICATE_EXEMPLAR, ERR_EXEMPLAR_LABEL_LENGTH, ERR_OUT_OF_ORDER_EXEMPLAR};
use tsdb::exemplar::{CircularExemplarStorage, ExemplarMetrics, ERR_INVALID_EXEMPLAR};

static E_METRICS: LazyLock<Arc<ExemplarMetrics>> = LazyLock::new(ExemplarMetrics::new);

fn metrics() -> Arc<ExemplarMetrics> {
    E_METRICS.clone()
}

fn verbose() -> bool {
    std::env::var_os("TEST_VERBOSE").is_some()
}

fn ex(l: &Labels, value: f64, ts: i64) -> Exemplar {
    Exemplar { labels: l.clone(), value, ts, has_ts: false }
}

fn eq_matcher(name: &str, value: &str) -> Matcher {
    Matcher::must_new(MatchType::Equal, name, value)
}

// Tests the same exemplar cases as AddExemplar, but specifically the ValidateExemplar function so it can be relied
// on externally.
#[test]
fn test_validate_exemplar() {
    let es = CircularExemplarStorage::new(2, metrics(), 0).unwrap();

    let l = labels::from_strings(&["service", "asdf"]);
    let e = Exemplar { labels: labels::from_strings(&["trace_id", "qwerty"]), value: 0.1, ts: 1, has_ts: false };

    es.validate_exemplar(&l, &e).unwrap();
    es.add_exemplar(&l, &e).unwrap();

    let e2 = Exemplar { labels: labels::from_strings(&["trace_id", "zxcvb"]), value: 0.1, ts: 2, has_ts: false };

    es.validate_exemplar(&l, &e2).unwrap();
    es.add_exemplar(&l, &e2).unwrap();

    assert_eq!(
        es.validate_exemplar(&l, &e2),
        Err(ERR_DUPLICATE_EXEMPLAR),
        "error is expected attempting to validate duplicate exemplar"
    );

    let mut e3 = e2.clone();
    e3.ts = 3;
    assert_eq!(
        es.validate_exemplar(&l, &e3),
        Err(ERR_DUPLICATE_EXEMPLAR),
        "error is expected when attempting to add duplicate exemplar, even with different timestamp"
    );

    e3.ts = 1;
    e3.value = 0.3;
    assert_eq!(es.validate_exemplar(&l, &e3), Err(ERR_OUT_OF_ORDER_EXEMPLAR));

    let e4 = Exemplar {
        labels: labels::from_strings(&["a", &"b".repeat(EXEMPLAR_MAX_LABEL_SET_LENGTH)]),
        value: 0.1,
        ts: 2,
        has_ts: false,
    };
    assert_eq!(Err(ERR_EXEMPLAR_LABEL_LENGTH), es.validate_exemplar(&l, &e4));

    let e5 = Exemplar {
        labels: labels::from_strings(&["trace_id", "aaaa", "trace_id", "bbbb"]),
        value: 0.1,
        ts: 4,
        has_ts: false,
    };
    let err = es.validate_exemplar(&l, &e5).unwrap_err();
    assert!(err.is(&ERR_INVALID_EXEMPLAR), "error is expected when the exemplar label names are not unique");
    let err = es.add_exemplar(&l, &e5).unwrap_err();
    assert!(err.is(&ERR_INVALID_EXEMPLAR), "error is expected when the exemplar label names are not unique");
}

#[test]
fn test_circular_exemplar_storage_add_exemplar() {
    let series1 = labels::from_strings(&["trace_id", "foo"]);
    let series2 = labels::from_strings(&["trace_id", "bar"]);

    let series1_matcher = vec![eq_matcher("trace_id", series1.get("trace_id"))];
    let series2_matcher = vec![eq_matcher("trace_id", series2.get("trace_id"))];

    struct Case {
        name: &'static str,
        size: i64,
        exemplars: Vec<Exemplar>,
        want_exemplars: Vec<Exemplar>,
        matcher: Vec<Matcher>,
        want_error: Option<storage::Error>,
    }
    let s1 = &series1;
    let s2 = &series2;
    let test_cases = vec![
        Case {
            name: "insert after newest",
            size: 3,
            exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 2)],
            matcher: series1_matcher.clone(),
            want_exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 2)],
            want_error: None,
        },
        Case {
            name: "insert before oldest",
            size: 3,
            exemplars: vec![ex(s1, 0.1, 2), ex(s1, 0.2, 1)],
            matcher: series1_matcher.clone(),
            want_exemplars: vec![ex(s1, 0.2, 1), ex(s1, 0.1, 2)],
            want_error: None,
        },
        Case {
            name: "insert in between",
            size: 3,
            exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 3), ex(s1, 0.3, 2)],
            matcher: series1_matcher.clone(),
            want_exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.3, 2), ex(s1, 0.2, 3)],
            want_error: None,
        },
        Case {
            name: "insert after newest with overflow",
            size: 3,
            exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 2), ex(s1, 0.3, 3), ex(s1, 0.4, 4)],
            matcher: series1_matcher.clone(),
            want_exemplars: vec![ex(s1, 0.2, 2), ex(s1, 0.3, 3), ex(s1, 0.4, 4)],
            want_error: None,
        },
        Case {
            name: "insert before oldest with overflow",
            size: 3,
            exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 2), ex(s1, 0.3, 3), ex(s1, 0.4, 0)],
            matcher: series1_matcher.clone(),
            want_exemplars: vec![ex(s1, 0.4, 0), ex(s1, 0.2, 2), ex(s1, 0.3, 3)],
            want_error: None,
        },
        Case {
            name: "insert between with overflow",
            size: 3,
            exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 3), ex(s1, 0.3, 4), ex(s1, 0.4, 2)],
            matcher: series1_matcher.clone(),
            want_exemplars: vec![ex(s1, 0.4, 2), ex(s1, 0.2, 3), ex(s1, 0.3, 4)],
            want_error: None,
        },
        Case {
            name: "out-of-order insert where evicted entry is insertion point",
            size: 3,
            exemplars: vec![
                ex(s1, 0.2, 2), // pos 0, linked list middle
                ex(s1, 0.1, 1), // pos 1, linked list oldest
                ex(s1, 0.5, 5), // pos 2, linked list newest
                ex(s1, 0.3, 3),
            ],
            matcher: series1_matcher.clone(),
            want_exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.3, 3), ex(s1, 0.5, 5)],
            want_error: None,
        },
        Case {
            name: "insert out of the OOO window",
            size: 3,
            exemplars: vec![ex(s1, 0.1, 200), ex(s1, 0.2, 1)],
            matcher: vec![],
            want_exemplars: vec![],
            want_error: Some(ERR_OUT_OF_ORDER_EXEMPLAR),
        },
        Case {
            name: "insert multiple series",
            size: 3,
            exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 3), ex(s2, 0.3, 4)],
            matcher: series1_matcher.clone(),
            want_exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 3)],
            want_error: None,
        },
        Case {
            name: "insert multiple series with overflow",
            size: 3,
            exemplars: vec![ex(s2, 0.1, 1), ex(s2, 0.2, 2), ex(s2, 0.3, 3), ex(s1, 0.4, 4)],
            matcher: series2_matcher.clone(),
            want_exemplars: vec![ex(s2, 0.2, 2), ex(s2, 0.3, 3)],
            want_error: None,
        },
        Case {
            name: "series1 overflows series2 out-of-order",
            size: 3,
            exemplars: vec![ex(s2, 0.1, 3), ex(s2, 0.2, 2), ex(s2, 0.3, 4), ex(s1, 0.4, 4), ex(s1, 0.5, 1)],
            matcher: series2_matcher.clone(),
            want_exemplars: vec![ex(s2, 0.3, 4)],
            want_error: None,
        },
        Case {
            name: "ignore duplicate exemplars",
            size: 3,
            exemplars: vec![ex(s1, 0.1, 3), ex(s1, 0.1, 3)],
            matcher: series1_matcher.clone(),
            want_exemplars: vec![ex(s1, 0.1, 3)],
            want_error: None,
        },
        Case {
            name: "ignore duplicate exemplars when buffer is full",
            size: 3,
            exemplars: vec![ex(s1, 0.1, 3), ex(s1, 0.2, 4), ex(s1, 0.3, 5), ex(s1, 0.3, 5)],
            matcher: series1_matcher.clone(),
            want_exemplars: vec![ex(s1, 0.1, 3), ex(s1, 0.2, 4), ex(s1, 0.3, 5)],
            want_error: None,
        },
        Case {
            name: "empty timestamps are valid",
            size: 3,
            exemplars: vec![ex(s1, 0.1, 0), ex(s1, 0.2, 0)],
            matcher: series1_matcher.clone(),
            want_exemplars: vec![ex(s1, 0.1, 0), ex(s1, 0.2, 0)],
            want_error: None,
        },
        Case {
            name: "exemplar label length exceeds maximum",
            size: 3,
            exemplars: vec![ex(&labels::from_strings(&["a", &"b".repeat(EXEMPLAR_MAX_LABEL_SET_LENGTH)]), 0.1, 2)],
            matcher: vec![],
            want_exemplars: vec![],
            want_error: Some(ERR_EXEMPLAR_LABEL_LENGTH),
        },
        Case {
            name: "native histograms",
            size: 6,
            exemplars: vec![
                ex(s1, 0.1, 1),
                ex(s1, 0.2, 2),
                ex(s1, 0.3, 3),
                ex(s1, 0.1, 1),
                ex(s1, 0.2, 2),
                ex(s1, 0.3, 3),
            ],
            matcher: series1_matcher.clone(),
            want_exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 2), ex(s1, 0.3, 3)],
            want_error: None,
        },
        Case {
            name: "evict only exemplar for series then re-add",
            size: 2,
            exemplars: vec![
                // series1 at index 0, series2 at index 1, then series1 evicts its own only exemplar.
                ex(s1, 0.1, 1),
                ex(s2, 0.2, 2),
                ex(s1, 0.3, 3),
            ],
            matcher: series1_matcher.clone(),
            want_exemplars: vec![ex(s1, 0.3, 3)],
            want_error: None,
        },
    ];

    for tc in test_cases {
        let es = CircularExemplarStorage::new(tc.size, metrics(), 100).unwrap();

        // Add exemplars and compare tc.want_error against the first exemplar failing.
        let mut add_error = Ok(());
        for (i, ex) in tc.exemplars.iter().enumerate() {
            add_error = es.add_exemplar(&ex.labels, ex);
            if add_error.is_err() {
                break;
            }
            if verbose() {
                println!("Buffer[{}]:\n{}", i, es.debug_circular_buffer());
            }
        }
        match &tc.want_error {
            None => assert!(add_error.is_ok(), "{}: {:?}", tc.name, add_error),
            Some(want) => {
                let err = add_error.as_ref().expect_err(tc.name);
                assert!(err.is(want), "{}: got {:?}, want {:?}", tc.name, err, want);
            }
        }
        if add_error.is_err() {
            continue;
        }

        // Ensure exemplars are returned correctly and in-order.
        let got_exemplars = es.select(0, 1000, std::slice::from_ref(&tc.matcher)).unwrap();
        if tc.want_exemplars.is_empty() {
            assert!(got_exemplars.is_empty(), "{}", tc.name);
        } else {
            assert_eq!(got_exemplars.len(), 1, "{}", tc.name);
            assert_eq!(tc.want_exemplars, got_exemplars[0].exemplars, "{}", tc.name);
        }
    }
}

#[test]
fn test_circular_exemplar_storage_resize() {
    let series1 = labels::from_strings(&["trace_id", "foo"]);
    let series2 = labels::from_strings(&["trace_id", "bar"]);
    let matcher1 = vec![Matcher::must_new(MatchType::Regexp, "trace_id", "(foo|bar)")];
    let s1 = &series1;
    let s2 = &series2;

    struct Case {
        name: &'static str,
        exemplars: Vec<Exemplar>,
        resize: i64,
        want_exemplars: Vec<Exemplar>,
        want_next_index: i32,
    }
    let test_cases = vec![
        Case {
            name: "in-order, grow",
            exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 2)],
            resize: 10,
            want_exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 2)],
            want_next_index: 3,
        },
        Case {
            name: "in-order, shrink",
            exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 2), ex(s1, 0.3, 3)],
            resize: 2,
            want_exemplars: vec![ex(s1, 0.2, 2), ex(s1, 0.3, 3)],
            want_next_index: 0,
        },
        Case {
            name: "out-of-order, shrink",
            exemplars: vec![ex(s1, 0.3, 3), ex(s1, 0.2, 2), ex(s1, 0.1, 1)],
            resize: 2,
            want_exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 2)],
            want_next_index: 0,
        },
        Case {
            name: "out-of-order, grow",
            exemplars: vec![ex(s1, 0.3, 3), ex(s1, 0.2, 2)],
            resize: 5,
            want_exemplars: vec![ex(s1, 0.2, 2), ex(s1, 0.3, 3)],
            want_next_index: 3,
        },
        Case {
            name: "duplicate timestamps",
            exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 1), ex(s1, 0.3, 2)],
            resize: 3,
            want_exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 1), ex(s1, 0.3, 2)],
            want_next_index: 0,
        },
        Case {
            name: "empty input, grow",
            exemplars: vec![],
            resize: 10,
            want_exemplars: vec![],
            want_next_index: 3,
        },
        Case {
            name: "empty input, shrink",
            exemplars: vec![],
            resize: 1,
            want_exemplars: vec![],
            want_next_index: 0,
        },
        Case {
            name: "shrink to zero",
            exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 2)],
            resize: 0,
            want_exemplars: vec![],
            want_next_index: 0,
        },
        Case {
            name: "multiple series, shrink",
            exemplars: vec![ex(s1, 0.1, 1), ex(s2, 1.1, 2), ex(s1, 0.2, 3), ex(s2, 1.2, 4)],
            resize: 2,
            want_exemplars: vec![ex(s1, 0.2, 3), ex(s2, 1.2, 4)],
            want_next_index: 0,
        },
        Case {
            name: "shrink to one",
            exemplars: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 2)],
            resize: 1,
            want_exemplars: vec![ex(s1, 0.2, 2)],
            want_next_index: 0,
        },
        Case {
            name: "shrink to two",
            exemplars: vec![ex(s1, 0.1, 1)],
            resize: 2,
            want_exemplars: vec![ex(s1, 0.1, 1)],
            want_next_index: 0,
        },
    ];

    for tc in test_cases {
        let es = CircularExemplarStorage::new(3, metrics(), 100).unwrap();

        for ex in &tc.exemplars {
            es.add_exemplar(&ex.labels, ex).unwrap();
        }

        // Resize the circular buffer.
        if verbose() {
            println!("Buffer[before-resize]:\n{}", es.debug_circular_buffer());
        }
        es.resize(tc.resize);
        if verbose() {
            println!("Buffer[after-resize]:\n{}", es.debug_circular_buffer());
        }

        // Ensure exemplars are returned correctly and in-order.
        let got_exemplars = es.select(0, 1000, std::slice::from_ref(&matcher1)).unwrap();
        let mut flat: Vec<Exemplar> = Vec::new();
        for group in got_exemplars {
            flat.extend(group.exemplars);
        }
        // Go sort.Slice (not stable); the expected values have distinct timestamps except "duplicate timestamps",
        // which comes from a single series already in order.
        flat.sort_by_key(|e| e.ts);
        assert_eq!(tc.want_exemplars, flat, "{}: exemplar mismatch", tc.name);
        assert_eq!(tc.want_next_index, es.debug_next_index(), "{}: next index mismatch", tc.name);
    }

    struct TwiceCase {
        name: &'static str,
        add_exemplars1: Vec<Exemplar>,
        resize1: i64,
        want_exemplars1: Vec<Exemplar>,
        resize2: i64,
        add_exemplars2: Vec<Exemplar>,
        want_exemplars2: Vec<Exemplar>,
    }
    let resize_twice_cases = vec![
        TwiceCase {
            name: "shrink then grow ordered",
            add_exemplars1: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 2), ex(s1, 0.3, 3), ex(s1, 0.4, 4)],
            resize1: 2,
            want_exemplars1: vec![ex(s1, 0.3, 3), ex(s1, 0.4, 4)],
            resize2: 5,
            add_exemplars2: vec![ex(s1, 0.5, 5), ex(s1, 0.6, 6), ex(s1, 0.7, 7)],
            want_exemplars2: vec![ex(s1, 0.3, 3), ex(s1, 0.4, 4), ex(s1, 0.5, 5), ex(s1, 0.6, 6), ex(s1, 0.7, 7)],
        },
        TwiceCase {
            name: "shrink then grow out-of-order",
            add_exemplars1: vec![ex(s1, 0.1, 1), ex(s1, 0.4, 4), ex(s1, 0.2, 2), ex(s1, 0.3, 3)],
            resize1: 2,
            // We delete in the order of ingestion, not temporally.
            want_exemplars1: vec![ex(s1, 0.2, 2), ex(s1, 0.3, 3)],
            resize2: 5,
            add_exemplars2: vec![ex(s1, 0.7, 7), ex(s1, 0.6, 6), ex(s1, 0.5, 5)],
            want_exemplars2: vec![ex(s1, 0.2, 2), ex(s1, 0.3, 3), ex(s1, 0.5, 5), ex(s1, 0.6, 6), ex(s1, 0.7, 7)],
        },
        TwiceCase {
            name: "grow then shrink ordered",
            add_exemplars1: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 2), ex(s1, 0.3, 3), ex(s1, 0.4, 4)],
            resize1: 5,
            want_exemplars1: vec![ex(s1, 0.2, 2), ex(s1, 0.3, 3), ex(s1, 0.4, 4)],
            resize2: 2,
            add_exemplars2: vec![ex(s1, 0.5, 5), ex(s1, 0.6, 6), ex(s1, 0.7, 7)],
            want_exemplars2: vec![ex(s1, 0.6, 6), ex(s1, 0.7, 7)],
        },
        TwiceCase {
            name: "grow then shrink out-of-order",
            add_exemplars1: vec![ex(s1, 0.1, 1), ex(s1, 0.4, 4), ex(s1, 0.2, 2), ex(s1, 0.3, 3)],
            resize1: 5,
            // We delete in the order of ingestion, not temporally.
            want_exemplars1: vec![ex(s1, 0.2, 2), ex(s1, 0.3, 3), ex(s1, 0.4, 4)],
            resize2: 2,
            add_exemplars2: vec![ex(s1, 0.7, 7), ex(s1, 0.5, 5), ex(s1, 0.6, 6)],
            want_exemplars2: vec![ex(s1, 0.5, 5), ex(s1, 0.6, 6)],
        },
        TwiceCase {
            name: "grow non-full buffer then add entries",
            add_exemplars1: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 2)],
            resize1: 10,
            want_exemplars1: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 2)],
            resize2: 10,
            add_exemplars2: vec![ex(s1, 0.3, 3), ex(s1, 0.4, 4)],
            want_exemplars2: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 2), ex(s1, 0.3, 3), ex(s1, 0.4, 4)],
        },
        TwiceCase {
            name: "shrink non-full buffer then add entries",
            add_exemplars1: vec![ex(s1, 0.1, 1)],
            resize1: 2,
            want_exemplars1: vec![ex(s1, 0.1, 1)],
            resize2: 2,
            add_exemplars2: vec![ex(s1, 0.2, 2)],
            want_exemplars2: vec![ex(s1, 0.1, 1), ex(s1, 0.2, 2)],
        },
    ];

    for tc in resize_twice_cases {
        let es = CircularExemplarStorage::new(3, metrics(), 100).unwrap();
        for ex in &tc.add_exemplars1 {
            es.add_exemplar(&ex.labels, ex).unwrap();
        }
        es.resize(tc.resize1);
        let got_exemplars = es.select(0, 1000, std::slice::from_ref(&matcher1)).unwrap();
        assert_eq!(got_exemplars.len(), 1, "{}", tc.name);
        assert_eq!(tc.want_exemplars1, got_exemplars[0].exemplars, "{}", tc.name);
        es.resize(tc.resize2);
        for ex in &tc.add_exemplars2 {
            es.add_exemplar(&ex.labels, ex).unwrap();
        }
        if verbose() {
            println!("Buffer[after-resize2]:\n{}", es.debug_circular_buffer());
        }
        let got_exemplars = es.select(0, 1000, std::slice::from_ref(&matcher1)).unwrap();
        assert_eq!(got_exemplars.len(), 1, "{}", tc.name);
        assert_eq!(tc.want_exemplars2, got_exemplars[0].exemplars, "{}", tc.name);
    }
}

#[test]
fn test_storage_overflow() {
    // Test that circular buffer index and assignment works properly, adding more exemplars than can be stored and
    // then querying for them.
    let es = CircularExemplarStorage::new(5, metrics(), 0).unwrap();

    let (l_name, l_value) = ("service", "asdf");
    let l = labels::from_strings(&[l_name, l_value]);

    let mut e_list = Vec::new();
    for i in 0..es.debug_len() + 1 {
        let e = Exemplar {
            labels: labels::from_strings(&["trace_id", "a"]),
            value: (i + 1) as f64 / 10.0,
            ts: 101 + i as i64,
            has_ts: false,
        };
        let _ = es.add_exemplar(&l, &e);
        e_list.push(e);
    }
    assert!(es.debug_exemplar_at(0).ts == 106, "exemplar was not stored correctly");

    let m = Matcher::new(MatchType::Equal, l_name, l_value).expect("error creating label matcher for exemplar query");
    let ret = es.select(100, 110, &[vec![m]]).unwrap();
    assert_eq!(ret.len(), 1, "select should have returned samples for a single series only");

    assert!(
        e_list[1..] == ret[0].exemplars[..],
        "select did not return expected exemplars\n\texpected: {:?}\n\tactual: {:?}\n",
        &e_list[1..],
        ret[0].exemplars
    );
}

#[test]
fn test_select_exemplar() {
    let es = CircularExemplarStorage::new(5, metrics(), 0).unwrap();

    let (l_name, l_value) = ("service", "asdf");
    let l = labels::from_strings(&[l_name, l_value]);
    let e = Exemplar { labels: labels::from_strings(&["trace_id", "querty"]), value: 0.1, ts: 12, has_ts: false };

    es.add_exemplar(&l, &e).expect("adding exemplar failed");
    assert!(es.debug_exemplar_at(0) == e, "exemplar was not stored correctly");

    let m = Matcher::new(MatchType::Equal, l_name, l_value).expect("error creating label matcher for exemplar query");
    let ret = es.select(0, 100, &[vec![m]]).unwrap();
    assert_eq!(ret.len(), 1, "select should have returned samples for a single series only");

    let expected_result = vec![e];
    assert!(
        expected_result == ret[0].exemplars,
        "select did not return expected exemplars\n\texpected: {:?}\n\tactual: {:?}\n",
        expected_result,
        ret[0].exemplars
    );
}

#[test]
fn test_select_exemplar_multi_series() {
    let es = CircularExemplarStorage::new(5, metrics(), 0).unwrap();

    let l1_name = "test_metric";
    let l1 = labels::from_strings(&[METRIC_NAME, l1_name, "service", "asdf"]);
    let l2_name = "test_metric2";
    let l2 = labels::from_strings(&[METRIC_NAME, l2_name, "service", "qwer"]);

    for i in 0..es.debug_len() {
        let e1 = Exemplar {
            labels: labels::from_strings(&["trace_id", "a"]),
            value: (i + 1) as f64 / 10.0,
            ts: 101 + i as i64,
            has_ts: false,
        };
        es.add_exemplar(&l1, &e1).unwrap();

        let e2 = Exemplar {
            labels: labels::from_strings(&["trace_id", "b"]),
            value: (i + 1) as f64 / 10.0,
            ts: 101 + i as i64,
            has_ts: false,
        };
        es.add_exemplar(&l2, &e2).unwrap();
    }

    let m = Matcher::new(MatchType::Equal, METRIC_NAME, l2_name).expect("error creating label matcher for exemplar query");
    let ret = es.select(100, 200, &[vec![m]]).unwrap();
    assert_eq!(ret.len(), 1, "select should have returned samples for a single series only");
    assert_eq!(ret[0].exemplars.len(), 3, "didn't get expected 8 exemplars, got {}", ret[0].exemplars.len());

    let m = Matcher::new(MatchType::Equal, METRIC_NAME, l1_name).expect("error creating label matcher for exemplar query");
    let ret = es.select(100, 200, &[vec![m]]).unwrap();
    assert_eq!(ret.len(), 1, "select should have returned samples for a single series only");
    assert_eq!(ret[0].exemplars.len(), 2, "didn't get expected 8 exemplars, got {}", ret[0].exemplars.len());
}

#[test]
fn test_select_exemplar_time_range() {
    let len_es: i64 = 5;
    let es = CircularExemplarStorage::new(len_es, metrics(), 0).unwrap();

    let (l_name, l_value) = ("service", "asdf");
    let l = labels::from_strings(&[l_name, l_value]);

    for i in 0..len_es {
        es.add_exemplar(
            &l,
            &Exemplar { labels: labels::from_strings(&["trace_id", &i.to_string()]), value: 0.1, ts: 101 + i, has_ts: false },
        )
        .unwrap();
        assert_eq!(es.debug_index_entry(&l).unwrap().1, i as i32, "exemplar was not stored correctly");
    }

    let m = Matcher::new(MatchType::Equal, l_name, l_value).expect("error creating label matcher for exemplar query");
    let ret = es.select(102, 104, &[vec![m]]).unwrap();
    assert_eq!(ret.len(), 1, "select should have returned samples for a single series only");
    assert_eq!(
        ret[0].exemplars.len(),
        3,
        "didn't get expected two exemplars {}, {:?}",
        ret[0].exemplars.len(),
        ret
    );
}

// Test to ensure that even though a series matches more than one matcher from the query that its exemplars are
// only included in the result a single time.
#[test]
fn test_select_exemplar_duplicate_series() {
    let es = CircularExemplarStorage::new(4, metrics(), 0).unwrap();

    let e = Exemplar { labels: labels::from_strings(&["trace_id", "qwerty"]), value: 0.1, ts: 12, has_ts: false };

    let (l_name0, l_value0) = ("service", "asdf");
    let (l_name1, l_value1) = ("cluster", "us-central1");
    let l = labels::from_strings(&[l_name0, l_value0, l_name1, l_value1]);

    // Lets just assume somehow the PromQL expression generated two separate lists of matchers, both of which can
    // select this particular series.
    let m = vec![
        vec![Matcher::must_new(MatchType::Equal, l_name0, l_value0)],
        vec![Matcher::must_new(MatchType::Equal, l_name1, l_value1)],
    ];

    es.add_exemplar(&l, &e).expect("adding exemplar failed");
    assert!(es.debug_exemplar_at(0) == e, "exemplar was not stored correctly");

    let ret = es.select(0, 100, &m).unwrap();
    assert_eq!(ret.len(), 1, "select should have returned samples for a single series only");
}

#[test]
fn test_index_overwrite() {
    let es = CircularExemplarStorage::new(2, metrics(), 0).unwrap();

    let l1 = labels::from_strings(&["service", "asdf"]);
    let l2 = labels::from_strings(&["service", "qwer"]);

    let e = |value: f64, ts: i64| Exemplar { labels: Labels::default(), value, ts, has_ts: false };
    es.add_exemplar(&l1, &e(1.0, 1)).unwrap();
    es.add_exemplar(&l2, &e(2.0, 2)).unwrap();
    es.add_exemplar(&l2, &e(3.0, 3)).unwrap();

    // Ensure index GC'ing is taking place, there should no longer be any index entry for series l1 since we just
    // wrote two exemplars for series l2.
    assert!(es.debug_index_entry(&l1).is_none());
    assert_eq!(Some((1, 0, l2.clone())), es.debug_index_entry(&l2));

    es.add_exemplar(&l1, &e(4.0, 4)).unwrap();

    let i = es.debug_index_entry(&l2);
    assert_eq!(Some((0, 0, l2.clone())), i);
}

#[test]
fn test_resize() {
    struct Case {
        name: &'static str,
        start_size: i64,
        new_count: i64,
        expected_series: Vec<i32>,
        not_expected_series: Vec<i32>,
        expected_migrated: i32,
    }
    let test_cases = vec![
        Case {
            name: "Grow",
            start_size: 100,
            new_count: 200,
            expected_series: vec![99, 98, 1, 0],
            not_expected_series: vec![100],
            expected_migrated: 100,
        },
        Case {
            name: "Shrink",
            start_size: 100,
            new_count: 50,
            expected_series: vec![99, 98, 50],
            not_expected_series: vec![49, 1, 0],
            expected_migrated: 50,
        },
        Case {
            name: "ShrinkToZero",
            start_size: 100,
            new_count: 0,
            expected_series: vec![],
            not_expected_series: vec![],
            expected_migrated: 0,
        },
        Case {
            name: "Negative",
            start_size: 100,
            new_count: -1,
            expected_series: vec![],
            not_expected_series: vec![],
            expected_migrated: 0,
        },
        Case {
            name: "NegativeToNegative",
            start_size: -1,
            new_count: -2,
            expected_series: vec![],
            not_expected_series: vec![],
            expected_migrated: 0,
        },
        Case {
            name: "GrowFromZero",
            start_size: 0,
            new_count: 10,
            expected_series: vec![],
            not_expected_series: vec![],
            expected_migrated: 0,
        },
    ];

    for tc in test_cases {
        let es = CircularExemplarStorage::new(tc.start_size, metrics(), 0).unwrap();

        for i in 0..tc.start_size.max(0) {
            es.add_exemplar(
                &labels::from_strings(&["service", &i.to_string()]),
                &Exemplar { labels: Labels::default(), value: i as f64, ts: i, has_ts: false },
            )
            .unwrap();
        }

        if verbose() {
            println!("Buffer[before-resize]:\n{}", es.debug_circular_buffer());
        }
        let resized = es.resize(tc.new_count);
        if verbose() {
            println!("Buffer[after-resize]:\n{}", es.debug_circular_buffer());
        }

        assert_eq!(tc.expected_migrated, resized, "{}", tc.name);

        let q = es.querier(&Context::background()).unwrap();

        for expected in &tc.expected_series {
            let matchers = vec![Matcher::must_new(MatchType::Equal, "service", &expected.to_string())];
            let ex = q.select(0, i64::MAX, &[matchers]).unwrap();
            assert!(!ex.is_empty(), "{}: series {}", tc.name, expected);
        }

        for not_expected in &tc.not_expected_series {
            let matchers = vec![Matcher::must_new(MatchType::Equal, "service", &not_expected.to_string())];
            let ex = q.select(0, i64::MAX, &[matchers]).unwrap();
            assert!(ex.is_empty(), "{}: series {}", tc.name, not_expected);
        }
    }
}

// Tries to provoke a data race between AddExemplar and Resize. (Go runs it with the race detector; here the lock
// makes a race a compile error, and the test checks that concurrent calls neither fail nor deadlock.)
#[test]
fn test_circular_exemplar_storage_concurrent_add_exemplar_resize() {
    let es = CircularExemplarStorage::new(0, metrics(), 0).unwrap();

    let l = labels::from_strings(&["service", "asdf"]);
    let e = Exemplar { labels: labels::from_strings(&["trace_id", "qwerty"]), value: 0.1, ts: 1, has_ts: false };

    let started = Arc::new(Barrier::new(2));

    let h = {
        let es = es.clone();
        let started = started.clone();
        std::thread::spawn(move || {
            started.wait();
            for _ in 0..100 {
                es.add_exemplar(&l, &e).unwrap();
            }
        })
    };

    for i in 0..100 {
        es.resize(i + 1);
        if i == 0 {
            started.wait();
        }
    }
    h.join().unwrap();
}
