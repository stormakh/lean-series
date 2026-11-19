//! Port of `web/api/v1/search_filters_test.go`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use storage::Filter;
use web::api::search::{SearchFilter, build_search_filter};
use web::api::search_filters::*;

fn in_delta(want: f64, got: f64, delta: f64) -> bool {
    (want - got).abs() <= delta
}

#[test]
fn test_substring_filter() {
    // (name, query, value, want accepted, want score)
    let tests: &[(&str, &str, &str, bool, f64)] = &[
        ("exact match", "prometheus", "prometheus", true, 1.0),
        ("prefix match", "prom", "prometheus", true, 1.0),
        // idx=3, maxIdx=10-4=6 -> 1.0 - 0.9*3/6 = 0.55.
        ("substring match in middle", "meth", "prometheus", true, 0.55),
        // idx=6, maxIdx=10-4=6 -> 1.0 - 0.9 = 0.1.
        ("substring match at end", "heus", "prometheus", true, 0.1),
        ("case mismatch is not accepted", "prometheus", "Prometheus", false, 0.0),
        ("no match", "grafana", "prometheus", false, 0.0),
        ("empty query accepts all", "", "anything", true, 1.0),
        // 6 runes, query 1 rune, maxPos=5, pos=3 -> 1.0 - 0.9*3/5 = 0.46.
        ("multi-byte rune position", "本", "東京日本京都", true, 0.46),
        // idx=2, maxIdx=5-2=3 -> 1.0 - 0.9*2/3 = 0.4.
        ("ascii substring stays on fast path", "fo", "infor", true, 0.4),
        // runes=5, query=1 -> maxPos=4, pos=1 -> 0.775.
        ("ascii match with multi-byte suffix uses rune positions", "b", "abc日本", true, 0.775),
        // runes=6, query=1 -> maxPos=5, pos=2 -> 0.64.
        ("multi-byte prefix uses rune positions", "c", "日本cabc", true, 0.64),
    ];
    for &(name, query, value, want_accepted, want_score) in tests {
        let filter = SubstringFilter::new(query);
        let (accepted, score) = filter.accept(value);
        assert_eq!(want_accepted, accepted, "{}", name);
        assert!(in_delta(want_score, score, 0.01), "{}: score {}", name, score);
    }
}

#[test]
fn test_fuzzy_filter() {
    // (name, query, threshold, value, want accepted, min score)
    let tests: &[(&str, &str, f64, &str, bool, f64)] = &[
        ("exact match", "prometheus", 0.8, "prometheus", true, 1.0),
        ("close match above threshold", "prometheus", 0.8, "promethus", true, 0.8),
        ("distant match below threshold", "prometheus", 0.8, "grafana", false, 0.0),
        ("case mismatch is not accepted", "prometheus", 0.8, "PROMETHEUS", false, 0.0),
    ];
    for &(name, query, threshold, value, want_accepted, min_score) in tests {
        let filter = FuzzyFilter::new(query, threshold);
        let (accepted, score) = filter.accept(value);
        assert_eq!(want_accepted, accepted, "{}", name);
        if want_accepted {
            assert!(score >= min_score, "{}: score {}", name, score);
        }
    }
}

#[test]
fn test_fuzzy_filter_concurrency() {
    let values: Vec<String> = vec![
        "prometheus".to_string(),
        "promethus".to_string(),
        "promethius".to_string(),
        "prmetheus".to_string(),
        "prometeus".to_string(),
        format!("prometheus{}", "x".repeat(128)),
        format!("prométheus{}", "x".repeat(128)),
    ];
    let want_filter = FuzzyFilter::new("prometheus", 0.8);
    let want: Vec<(bool, f64)> = values.iter().map(|v| want_filter.accept(v)).collect();

    const WORKERS: usize = 10;
    let filter = Arc::new(FuzzyFilter::new("prometheus", 0.8));
    let values = Arc::new(values);
    let handles: Vec<_> = (0..WORKERS)
        .map(|_| {
            let filter = filter.clone();
            let values = values.clone();
            std::thread::spawn(move || values.iter().map(|v| filter.accept(v)).collect::<Vec<_>>())
        })
        .collect();
    for (worker, h) in handles.into_iter().enumerate() {
        let got = h.join().unwrap();
        for (i, value) in values.iter().enumerate() {
            assert_eq!(want[i], got[i], "worker {}, value {:?}", worker, value);
        }
    }
}

#[test]
fn test_subsequence_filter() {
    // (name, query, threshold, value, want accepted, want score; -1 means any positive value)
    let tests: &[(&str, &str, f64, &str, bool, f64)] = &[
        ("exact match", "prometheus", 1.0, "prometheus", true, 1.0),
        ("prefix match scores 1.0", "prom", 1.0, "prometheus", true, 1.0),
        ("subsequence match above zero threshold", "pms", 0.0, "prometheus", true, -1.0),
        ("non-subsequence rejected", "xyz", 0.0, "prometheus", false, 0.0),
        ("case mismatch rejected", "prom", 0.0, "PROMETHEUS", false, 0.0),
        ("below threshold rejected", "pms", 0.99, "prometheus", false, -1.0),
    ];
    for &(name, query, threshold, value, want_accepted, want_score) in tests {
        let filter = SubsequenceFilter::new(query, threshold);
        let (accepted, score) = filter.accept(value);
        assert_eq!(want_accepted, accepted, "{}", name);
        if want_score >= 0.0 {
            assert!(in_delta(want_score, score, 1e-9), "{}: score {}", name, score);
        }
    }
}

/// Records how many times `accept` is called per value.
struct CountingFilter {
    calls: Mutex<HashMap<String, usize>>,
    result: Mutex<HashMap<String, MemoEntry>>,
}

impl CountingFilter {
    fn new(result: HashMap<String, MemoEntry>) -> Arc<CountingFilter> {
        Arc::new(CountingFilter { calls: Mutex::new(HashMap::new()), result: Mutex::new(result) })
    }
    fn calls(&self, v: &str) -> usize {
        self.calls.lock().unwrap().get(v).copied().unwrap_or(0)
    }
}

impl Filter for CountingFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        *self.calls.lock().unwrap().entry(value.to_string()).or_default() += 1;
        let r = self.result.lock().unwrap().get(value).copied().unwrap_or_default();
        (r.accepted, r.score)
    }
}

#[test]
fn test_memoizing_filter() {
    let inner = CountingFilter::new(HashMap::from([
        ("prometheus".to_string(), MemoEntry { accepted: true, score: 1.0 }),
        ("grafana".to_string(), MemoEntry { accepted: false, score: 0.0 }),
    ]));
    let memo = MemoizingFilter::new(inner.clone());

    // First call computes.
    let (accepted, score) = memo.accept("prometheus");
    assert!(accepted);
    assert_eq!(1.0, score);

    // Repeat calls hit the cache.
    for _ in 0..5 {
        let (accepted, score) = memo.accept("prometheus");
        assert!(accepted);
        assert_eq!(1.0, score);
    }

    // Distinct values are computed once each.
    let (accepted, score) = memo.accept("grafana");
    assert!(!accepted);
    assert_eq!(0.0, score);
    memo.accept("grafana");

    assert_eq!(1, inner.calls("prometheus"));
    assert_eq!(1, inner.calls("grafana"));
}

#[test]
fn test_memoizing_filter_cache_cap() {
    let mut results = HashMap::with_capacity(MEMOIZING_FILTER_MAX_ENTRIES + 10);
    for i in 0..MEMOIZING_FILTER_MAX_ENTRIES + 10 {
        results.insert(i.to_string(), MemoEntry { accepted: true, score: 1.0 });
    }
    let inner = CountingFilter::new(results);
    let memo = MemoizingFilter::new(inner.clone());

    // Fill the cache to its cap.
    for i in 0..MEMOIZING_FILTER_MAX_ENTRIES {
        memo.accept(&i.to_string());
    }
    assert_eq!(MEMOIZING_FILTER_MAX_ENTRIES, memo.cache_len());

    // A value past the cap is computed by inner but not stored.
    let overflow = "overflow-value";
    inner.result.lock().unwrap().insert(overflow.to_string(), MemoEntry { accepted: true, score: 0.5 });
    memo.accept(overflow);
    memo.accept(overflow);
    assert_eq!(MEMOIZING_FILTER_MAX_ENTRIES, memo.cache_len());
    // Past the cap, every accept reaches the inner filter.
    assert_eq!(2, inner.calls(overflow));

    // Cached values are still served from the cache.
    memo.accept("0");
    assert_eq!(1, inner.calls("0"));
}

#[test]
fn test_memoizing_filter_byte_cap() {
    const VALUE_LEN: usize = 1024;
    let entries_to_overflow = MEMOIZING_FILTER_MAX_BYTES / VALUE_LEN + 2;
    assert!(entries_to_overflow < MEMOIZING_FILTER_MAX_ENTRIES, "test setup: byte cap must be reached before entry cap");

    let mut keys = Vec::with_capacity(entries_to_overflow);
    let mut results = HashMap::with_capacity(entries_to_overflow);
    for i in 0..entries_to_overflow {
        let prefix = format!("{:010}:", i);
        let key = format!("{}{}", prefix, "x".repeat(VALUE_LEN - prefix.len()));
        results.insert(key.clone(), MemoEntry { accepted: true, score: 1.0 });
        keys.push(key);
    }
    assert_eq!(VALUE_LEN, keys[0].len());

    let inner = CountingFilter::new(results);
    let memo = MemoizingFilter::new(inner.clone());
    for k in &keys {
        memo.accept(k);
    }

    assert!(memo.cache_len() < entries_to_overflow, "byte cap must limit cached entries below the inserted count");
    assert!(memo.cache_len() < MEMOIZING_FILTER_MAX_ENTRIES, "byte cap must be reached before the entry cap");
    assert!(memo.bytes() <= MEMOIZING_FILTER_MAX_BYTES, "cached bytes must not exceed the configured budget");

    // A value past the cap is recomputed by the inner filter each call.
    let overflow_key = &keys[entries_to_overflow - 1];
    let calls_before = inner.calls(overflow_key);
    assert!(
        !memo.cache_contains(overflow_key),
        "the last inserted key must be past the byte cap and therefore not cached"
    );
    memo.accept(overflow_key);
    memo.accept(overflow_key);
    assert_eq!(calls_before + 2, inner.calls(overflow_key), "values past the byte cap must reach the inner filter on every accept");
}

#[test]
fn test_build_search_filter_memo_only_for_expensive_chain() {
    // adapted: Go type-asserts `*memoizingFilter`; the Rust builder returns an enum that says whether it memoizes.
    let got = build_search_filter(&["prom".to_string()], 0, "jarowinkler", true);
    assert!(!matches!(got, Some(SearchFilter::Memoizing(_))), "substring-only chain should not be wrapped with memoizingFilter");
    assert!(got.is_some());

    let got = build_search_filter(&["prm".to_string()], 0, "subsequence", true);
    assert!(matches!(got, Some(SearchFilter::Memoizing(_))), "subsequence chain must be memoized");

    let got = build_search_filter(&["prom".to_string()], 80, "jarowinkler", true);
    assert!(matches!(got, Some(SearchFilter::Memoizing(_))), "jaro-winkler with fuzz threshold must be memoized");
}

#[test]
fn test_case_folding_filter() {
    let inner = Arc::new(SubstringFilter::new("prom"));
    let wrapped = CaseFoldingFilter::new(inner);

    let (accepted, score) = wrapped.accept("Prometheus");
    assert!(accepted);
    assert_eq!(1.0, score);

    let (accepted, _) = wrapped.accept("Grafana");
    assert!(!accepted);
}

#[test]
fn test_or_filter() {
    // (name, substring query, fuzzy query, fuzzy threshold, value, want accepted, min score)
    let tests: &[(&str, &str, &str, f64, &str, bool, f64)] = &[
        ("substring match only", "prom", "", 0.0, "prometheus", true, 1.0),
        ("substring rejects", "prom", "", 0.0, "node", false, 0.0),
        ("fuzzy fallback", "go_gor", "go_gor", 0.8, "go_goroutins", true, 0.8),
        ("no filters accept all", "", "", 0.0, "anything", true, 1.0),
    ];
    for &(name, sq, fq, ft, value, want_accepted, min_score) in tests {
        let filter = OrFilter {
            substring_filter: if sq.is_empty() { None } else { Some(SubstringFilter::new(sq)) },
            fuzzy_filter: if fq.is_empty() { None } else { Some(FuzzyFilter::new(fq, ft)) },
        };
        let (accepted, score) = filter.accept(value);
        assert_eq!(want_accepted, accepted, "{}", name);
        if want_accepted {
            assert!(score >= min_score, "{}: score {}", name, score);
        }
    }
}

#[test]
fn test_chain_filter() {
    // (query, threshold, is substring)
    type Spec = (&'static str, f64, bool);
    let tests: Vec<(&str, Vec<Spec>, &str, bool, f64)> = vec![
        ("both filters accept", vec![("prom", 0.0, true), ("prometheus", 0.8, false)], "prometheus", true, 1.0),
        ("first filter rejects", vec![("grafana", 0.0, true), ("prometheus", 0.8, false)], "prometheus", false, 0.0),
        ("second filter rejects", vec![("prom", 0.0, true), ("grafana", 0.9, false)], "prometheus", false, 0.0),
        ("empty chain accepts all", vec![], "anything", true, 1.0),
        ("max score wins for ranking", vec![("prom", 0.0, true), ("prometheus", 0.5, false)], "prometheus", true, 1.0),
    ];
    for (name, specs, value, want_accepted, want_score) in tests {
        let filters: Vec<Arc<dyn Filter>> = specs
            .iter()
            .map(|&(q, t, sub)| -> Arc<dyn Filter> {
                if sub { Arc::new(SubstringFilter::new(q)) } else { Arc::new(FuzzyFilter::new(q, t)) }
            })
            .collect();
        let chain = ChainFilter::new(filters);
        let (accepted, score) = chain.accept(value);
        assert_eq!(want_accepted, accepted, "{}", name);
        assert!(in_delta(want_score, score, 1e-9), "{}: score {}", name, score);
    }
}
