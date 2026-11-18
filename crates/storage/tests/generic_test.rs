//! Port of `storage/generic_test.go` (benchmarks left out).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use storage::annotations::Annotations;
use storage::*;

/// Accepts values starting with prefix; the score is inversely proportional to the value length (shorter =
/// higher score).
struct PrefixScoreFilter {
    prefix: &'static str,
}

impl Filter for PrefixScoreFilter {
    fn accept(&self, v: &str) -> (bool, f64) {
        if !v.starts_with(self.prefix) {
            return (false, 0.0);
        }
        // Score in (0, 1], higher for shorter values.
        (true, 1.0 / (1 + v.len() - self.prefix.len()) as f64)
    }
}

/// Accepts every value with the same score.
struct ConstScoreFilter(f64);

impl Filter for ConstScoreFilter {
    fn accept(&self, _: &str) -> (bool, f64) {
        (true, self.0)
    }
}

/// Adapts a function to the Filter interface for testing (Go `FilterFunc`).
struct FilterFunc<F>(F);

impl<F: Fn(&str) -> (bool, f64) + Send + Sync> Filter for FilterFunc<F> {
    fn accept(&self, v: &str) -> (bool, f64) {
        (self.0)(v)
    }
}

fn hints(filter: Option<Arc<dyn Filter>>, order_by: storage::Ordering, limit: i64) -> SearchHints {
    SearchHints {
        filter,
        limit,
        order_by,
    }
}

fn sr(value: &str, score: f64) -> SearchResult {
    SearchResult {
        value: value.to_string(),
        score,
    }
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[test]
fn test_apply_search_hints() {
    use storage::Ordering::*;
    let values = strs(&["alpha", "beta", "gamma", "delta", "epsilon"]);

    // nil hints returns all with score 1.
    {
        let got = apply_search_hints(&values, None);
        assert_eq!(got.len(), values.len());
        for (i, r) in got.iter().enumerate() {
            assert_eq!(r.value, values[i]);
            assert_eq!(r.score, 1.0);
        }
    }

    // no filter ascending limit.
    {
        let got = apply_search_hints(&values, Some(&hints(None, ByValueAsc, 3)));
        assert_eq!(
            got,
            vec![sr("alpha", 1.0), sr("beta", 1.0), sr("gamma", 1.0)]
        );
    }

    // no filter descending limit walks input in reverse.
    {
        let got = apply_search_hints(&values, Some(&hints(None, ByValueDesc, 2)));
        assert_eq!(got, vec![sr("epsilon", 1.0), sr("delta", 1.0)]);
    }

    // filter ascending early exit at limit.
    {
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        let filter = FilterFunc(move |_: &str| {
            h.fetch_add(1, AtomicOrdering::Relaxed);
            (true, 1.0)
        });
        let got = apply_search_hints(&values, Some(&hints(Some(Arc::new(filter)), ByValueAsc, 2)));
        assert_eq!(got.len(), 2);
        // Early exit: the filter should not have been called for values past the second match.
        assert_eq!(hits.load(AtomicOrdering::Relaxed), 2);
    }

    // filter descending reverses then limits.
    {
        let got = apply_search_hints(
            &values,
            Some(&hints(
                Some(Arc::new(PrefixScoreFilter { prefix: "" })),
                ByValueDesc,
                2,
            )),
        );
        assert_eq!(got.len(), 2);
        // Input order is values[0..4]; reverse picks epsilon then delta.
        assert_eq!(got[0].value, "epsilon");
        assert_eq!(got[1].value, "delta");
    }

    // filter descending limit early exits without scanning all.
    {
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        let filter = FilterFunc(move |_: &str| {
            h.fetch_add(1, AtomicOrdering::Relaxed);
            (true, 1.0)
        });
        let got = apply_search_hints(
            &values,
            Some(&hints(Some(Arc::new(filter)), ByValueDesc, 2)),
        );
        assert_eq!(got, vec![sr("epsilon", 1.0), sr("delta", 1.0)]);
        assert_eq!(
            hits.load(AtomicOrdering::Relaxed),
            2,
            "must early-exit at limit for OrderByValueDesc"
        );
    }

    // score desc top-K returns best K.
    {
        // Score is inversely tied to length; top-K of size 3 picks the three shortest "x" values.
        let input: Vec<String> = (0..100)
            .map(|i| format!("x{:0width$}", i, width = i + 1))
            .collect();
        let got = apply_search_hints(
            &input,
            Some(&hints(
                Some(Arc::new(PrefixScoreFilter { prefix: "x" })),
                ByScoreDesc,
                3,
            )),
        );
        assert_eq!(got.len(), 3);
        // Scores are non-increasing.
        for i in 1..got.len() {
            assert!(got[i - 1].score >= got[i].score);
        }
        // Best score is the shortest value ("x0").
        assert_eq!(got[0].value, "x0");
    }

    // score desc tie-break on Value asc.
    {
        let got = apply_search_hints(
            &values,
            Some(&hints(
                Some(Arc::new(ConstScoreFilter(0.5))),
                ByScoreDesc,
                3,
            )),
        );
        assert_eq!(
            got,
            vec![sr("alpha", 0.5), sr("beta", 0.5), sr("delta", 0.5)]
        );
    }

    // score desc unlimited keeps all matches sorted.
    {
        let got = apply_search_hints(
            &values,
            Some(&hints(
                Some(Arc::new(PrefixScoreFilter { prefix: "" })),
                ByScoreDesc,
                0,
            )),
        );
        assert_eq!(got.len(), values.len());
        for i in 1..got.len() {
            if got[i - 1].score == got[i].score {
                assert!(got[i - 1].value < got[i].value);
                continue;
            }
            assert!(got[i - 1].score > got[i].score);
        }
    }

    // near-MaxInt limit does not panic top-K.
    {
        let got = apply_search_hints(
            &strs(&["alpha", "beta", "gamma"]),
            Some(&hints(
                Some(Arc::new(FilterFunc(|_: &str| (true, 0.5)))),
                ByScoreDesc,
                i64::MAX,
            )),
        );
        assert_eq!(got.len(), 3);
    }

    // near-MaxInt limit does not panic reverse early exit.
    {
        let got = apply_search_hints(
            &strs(&["alpha", "beta", "gamma"]),
            Some(&hints(
                Some(Arc::new(FilterFunc(|_: &str| (true, 1.0)))),
                ByValueDesc,
                i64::MAX,
            )),
        );
        assert_eq!(
            got,
            vec![sr("gamma", 1.0), sr("beta", 1.0), sr("alpha", 1.0)]
        );
    }

    // near-MaxInt limit does not panic linear ascending path.
    {
        let got = apply_search_hints(
            &strs(&["alpha", "beta", "gamma"]),
            Some(&hints(
                Some(Arc::new(FilterFunc(|_: &str| (true, 0.5)))),
                ByValueAsc,
                i64::MAX,
            )),
        );
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].value, "alpha");
        assert_eq!(got[2].value, "gamma");
    }

    // filter selectivity 1 percent caps allocation.
    {
        let input: Vec<String> = (0..10000).map(|i| format!("v{}", i)).collect();
        // Match values whose decimal representation ends in "00".
        let got = apply_search_hints(
            &input,
            Some(&hints(
                Some(Arc::new(FilterFunc(|v: &str| (v.ends_with("00"), 0.5)))),
                ByValueAsc,
                50,
            )),
        );
        assert!(got.len() <= 50);
    }
}

#[test]
fn test_new_search_result_set_from_slice_and_error() {
    // The warnings round-trip through warnings() and err() is hidden until the slice is exhausted.
    let mut warns = Annotations::new();
    warns.add(Error::msg("upstream warning"));

    let results = vec![sr("alpha", 1.0), sr("beta", 1.0)];
    let mut rs = new_search_result_set_from_slice_and_error(
        results.clone(),
        warns,
        Some(Error::msg("tail failure")),
    );

    let mut got = Vec::new();
    while rs.next() {
        got.push(rs.at().clone());
        // Warnings are observable from the start, before iteration ends.
        assert!(!rs.warnings().is_empty());
        // err() must remain nil while the slice is still producing values.
        assert!(rs.err().is_none());
    }
    assert_eq!(got, results);
    let err = rs.err().expect("an error");
    assert!(err.to_string().contains("tail failure"));

    // Warnings still reflect the upstream payload after iteration ended.
    let got_warnings = rs.warnings().as_errors();
    assert_eq!(got_warnings.len(), 1);
    assert!(got_warnings[0].to_string().contains("upstream warning"));

    assert!(rs.close().is_none());
}

#[test]
fn test_linear_result_cap() {
    let min = MIN_LINEAR_ALLOC_CAP as i64;
    // Boundary: limit smaller than the floor.
    assert_eq!(linear_result_cap(10000, 10), min);
    // Limit zero means unlimited; cap grows to len.
    assert_eq!(linear_result_cap(10000, 0), 10000);
    // Cap is capped by len(values).
    assert_eq!(linear_result_cap(50, 100), 50);
    // 2*limit dominates when above the floor and below len.
    assert_eq!(linear_result_cap(10000, 500), 1000);
    // Overflow fallback returns the small-allocation floor.
    assert_eq!(linear_result_cap(10000, i64::MAX), min);
}
