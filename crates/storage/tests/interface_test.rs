//! Port of `storage/interface_test.go`.

use std::borrow::Cow;

use chunkenc::{ChunkIterator, ValueType};
use storage::*;

#[test]
fn test_mock_series() {
    let s = mock_series(
        vec![],
        vec![1, 2, 3],
        vec![1.0, 2.0, 3.0],
        &["__name__", "foo"],
    );
    let mut it = s.iterator(None);
    let mut ts = Vec::new();
    let mut vs = Vec::new();
    while it.next() == ValueType::Float {
        let (t, v) = it.at();
        ts.push(t);
        vs.push(v);
    }
    assert_eq!(ts, vec![1, 2, 3]);
    assert_eq!(vs, vec![1.0, 2.0, 3.0]);
}

#[test]
fn test_mock_series_with_st() {
    let s = mock_series(
        vec![0, 1, 2],
        vec![1, 2, 3],
        vec![1.0, 2.0, 3.0],
        &["__name__", "foo"],
    );
    let mut it = s.iterator(None);
    let mut ts = Vec::new();
    let mut vs = Vec::new();
    let mut st = Vec::new();
    while it.next() == ValueType::Float {
        let (t, v) = it.at();
        ts.push(t);
        vs.push(v);
        st.push(it.at_st());
    }
    assert_eq!(ts, vec![1, 2, 3]);
    assert_eq!(vs, vec![1.0, 2.0, 3.0]);
    assert_eq!(st, vec![0, 1, 2]);
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[test]
fn test_label_hints_apply_limit() {
    // Unordered, so a leading limit and a smallest limit differ.
    let input = strs(&["d", "b", "e", "a", "c"]);
    let lh = |limit, limit_smallest| {
        Some(LabelHints {
            limit,
            limit_smallest,
        })
    };
    let cases: Vec<(&str, Option<LabelHints>, Vec<String>, bool)> = vec![
        ("nil hints", None, input.clone(), false),
        ("no limit", lh(0, true), input.clone(), false),
        ("limit at count", lh(5, true), input.clone(), false),
        ("limit above count", lh(6, true), input.clone(), false),
        ("negative limit", lh(-1, true), input.clone(), false),
        (
            "negative limit without flag",
            lh(-1, false),
            input.clone(),
            false,
        ),
        ("leading limit", lh(3, false), strs(&["d", "b", "e"]), false),
        ("smallest limit", lh(3, true), strs(&["a", "b", "c"]), true),
        ("smallest limit of one", lh(1, true), strs(&["a"]), true),
    ];
    for (name, hints, want, want_allocated) in cases {
        let values = input.clone();
        let (got, allocated) = LabelHints::apply_limit(hints.as_ref(), &values);
        assert_eq!(got.as_ref(), want.as_slice(), "{}", name);
        assert_eq!(allocated, want_allocated, "{}", name);
        assert_eq!(
            values, input,
            "{}: ApplyLimit must not modify its input",
            name
        );
        // An allocated result is owned, so the caller may keep and modify it.
        assert_eq!(allocated, matches!(got, Cow::Owned(_)), "{}", name);
        if allocated {
            let mut got = got.into_owned();
            got[0] = "zzz".to_string();
            assert_eq!(
                values, input,
                "{}: an allocated result must not alias the input",
                name
            );
        }
    }
}

/// A small PCG-like generator for the shuffle (Go uses math/rand/v2 PCG(1, 2)).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let x = self.0;
        (x ^ (x >> 33)).wrapping_mul(0xff51afd7ed558ccd)
    }
}

/// Compares the bounded selection against a full sort over randomly ordered values, at limits either side of a
/// power of two so the heap is exercised at depth.
#[test]
fn test_label_hints_apply_limit_smallest() {
    const COUNT: usize = 500;
    let values: Vec<String> = (0..COUNT).map(|i| format!("value_{:04}", i)).collect();
    let mut shuffled = values.clone();
    // adapted: the shuffle order differs from Go's PCG(1, 2), which does not change the expected result.
    let mut rng = Rng(1 ^ (2 << 32));
    for i in (1..shuffled.len()).rev() {
        let j = (rng.next() % (i as u64 + 1)) as usize;
        shuffled.swap(i, j);
    }
    for limit in [1, 2, 3, 7, 63, 64, 65, COUNT - 1] {
        let hints = LabelHints {
            limit: limit as i64,
            limit_smallest: true,
        };
        let (got, allocated) = LabelHints::apply_limit(Some(&hints), &shuffled);
        assert!(allocated, "{}", limit);
        assert_eq!(got.as_ref(), &values[..limit], "{}", limit);
    }
}

#[test]
fn test_label_hints_apply_limit_edge_inputs() {
    // nil input.
    let hints = LabelHints {
        limit: 3,
        limit_smallest: true,
    };
    let (got, allocated) = LabelHints::apply_limit(Some(&hints), &[]);
    assert!(got.is_empty());
    assert!(!allocated);

    // empty input.
    let empty: Vec<String> = Vec::new();
    let (got, allocated) = LabelHints::apply_limit(Some(&hints), &empty);
    assert!(got.is_empty());
    assert!(!allocated);

    // Duplicates must not be collapsed, and must not displace a smaller value that appears later.
    let input = strs(&["b", "b", "c", "a", "b"]);
    let (got, allocated) = LabelHints::apply_limit(Some(&hints), &input);
    assert_eq!(got.as_ref(), strs(&["a", "b", "b"]).as_slice());
    assert!(allocated);
}

#[test]
fn test_label_hints_allows_early_stop() {
    let lh = |limit, limit_smallest| {
        Some(LabelHints {
            limit,
            limit_smallest,
        })
    };
    let cases = [
        ("nil hints", None, false),
        ("no limit", lh(0, false), false),
        ("negative limit", lh(-1, false), false),
        ("limit only", lh(1, false), true),
        ("limit with smallest", lh(1, true), false),
        ("smallest without limit", lh(0, true), false),
    ];
    for (name, hints, want) in cases {
        assert_eq!(
            LabelHints::allows_early_stop(hints.as_ref()),
            want,
            "{}",
            name
        );
    }
}
