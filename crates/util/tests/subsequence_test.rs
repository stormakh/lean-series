//! Port of `util/strutil/subsequence_test.go` (the benchmarks are not ported).

use util::strutil::SubsequenceMatcher;

fn score(pattern: &str, text: &str) -> f64 {
    SubsequenceMatcher::new(pattern).score(text)
}

fn in_delta(expected: f64, actual: f64, delta: f64) -> bool {
    (expected - actual).abs() <= delta
}

#[test]
fn test_subsequence_score() {
    // (name, pattern, text, want_score, want_zero)
    let tests: &[(&str, &str, &str, f64, bool)] = &[
        ("empty pattern", "", "anything", 1.0, false),
        ("empty text", "abc", "", 0.0, true),
        ("exact match", "my awesome text", "my awesome text", 1.0, false),
        // Intervals [0,1], leading=0, trailing=13. raw = 4 - 13/30, normalized by 4.
        ("prefix match", "my", "my awesome text", 107.0 / 120.0 * 0.999, false),
        // Intervals [11,13], leading=11, trailing=1. raw = 9 - 11/15 - 1/30, normalized by 9.
        ("substring match", "tex", "my awesome text", 247.0 / 270.0 * 0.999, false),
        // Intervals [8,9] and [11,11], leading=8, inner gap=1, trailing=3. raw = 5 - 9/15 - 3/30, normalized by 9.
        ("fuzzy match picks best starting position", "met", "my awesome text", 43.0 / 90.0 * 0.999, false),
        // Match at [2,4], leading gap=2, trailing=0. raw = 9 - 2/5, normalized by 9.
        ("prefers later position with better consecutive run", "bac", "babac", 43.0 / 45.0 * 0.999, false),
        // Intervals [0,7], leading=0, trailing=1. raw = 64 - 1/18, normalized by 64 and scaled.
        ("longer prefix match stays below exact match", "handler1", "handler10", 1149849.0 / 1152000.0, false),
        ("pattern longer than text", "abcd", "abc", 0.0, true),
        ("pattern longer in runes than multi-byte text", "abc", "éé", 0.0, true),
        ("non-ASCII pattern with ASCII text", "é", "ab", 0.0, true),
        ("no subsequence match", "xyz", "abc", 0.0, true),
        ("unicode exact match", "éàü", "éàü", 1.0, false),
        // Intervals [0,1], leading=0, trailing=1. raw = 4 - 1/6, normalized by 4.
        ("unicode prefix match", "éà", "éàü", 23.0 / 24.0 * 0.999, false),
        ("unicode no match", "üé", "éàü", 0.0, true),
        ("unicode first char matches but pattern cannot complete", "éàx", "éàü", 0.0, true),
        // Intervals [0,0] and [2,2], leading=0, inner gap=1, trailing=0. Raw = 1 + 1 - 1/3, normalized by 4.
        ("unicode fuzzy match with gap between intervals", "éü", "éàü", 5.0 / 12.0 * 0.999, false),
        // Intervals [0,1], leading=0, trailing=1. raw = 4 - 1/6, normalized by 4.
        ("mixed ascii and unicode", "aé", "aéb", 23.0 / 24.0 * 0.999, false),
        // 'é' (U+00E9) and 'ã' (U+00E3) share the leading UTF-8 byte but must not be treated as equal.
        ("unicode chars sharing leading utf-8 byte do not match", "é", "ã", 0.0, true),
        ("single char exact match", "a", "a", 1.0, false),
        // 'o'(1),'a'(2) form one interval [1,2], leading gap=1, trailing=1. Raw = 2² - 1/4 - 1/8 = 29/8.
        ("consecutive match with leading gap", "oa", "goat", 29.0 / 32.0 * 0.999, false),
        // Greedy: intervals [0,1] and [3,4]. Raw = 2² + 2² - 1/5, normalized by 16.
        ("repeated chars use greedy match", "abaa", "abbaa", 39.0 / 80.0 * 0.999, false),
    ];
    for &(name, pattern, text, want_score, want_zero) in tests {
        let got = score(pattern, text);
        if want_zero {
            assert_eq!(0.0, got, "{name}");
            continue;
        }
        assert!(in_delta(want_score, got, 1e-9), "{name}: got {got}, want {want_score}");
    }
}

#[test]
fn test_subsequence_score_properties() {
    // Prefix match scores below 1.0; only exact match scores 1.0.
    // "pro" in "prometheus": intervals [0,2], trailing=7. raw = 9 - 7/20, normalized by 9.
    assert!(in_delta(173.0 / 180.0 * 0.999, score("pro", "prometheus"), 1e-9));

    // Exact match always scores 1.0.
    assert_eq!(1.0, score("prometheus", "prometheus"));

    // Score is always in [0, 1].
    let cases = [("abc", "xaxbxcx"), ("z", "aaaaaz"), ("ab", "ba"), ("met", "my awesome text")];
    for (p, t) in cases {
        let s = score(p, t);
        assert!((0.0..=1.0).contains(&s), "score {s} out of range for pattern={p:?} text={t:?}");
        assert!(!s.is_nan());
    }

    // Prefix scores higher than non-prefix substring.
    let prefix_score = score("abc", "abcdef");
    let suffix_score = score("abc", "defabc");
    assert!(prefix_score > suffix_score);

    // Consecutive chars score higher than scattered.
    let consecutive_score = score("abc", "xabcx");
    let scattered_score = score("abc", "xaxbxcx");
    assert!(consecutive_score > scattered_score);
}
