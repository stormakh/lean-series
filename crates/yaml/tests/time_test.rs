//! Ported from prometheus/common v0.72.1 model/time_test.go (the Duration tests).
use yaml::{Duration, parse_duration, parse_duration_allow_negative};

const MS: i64 = 1_000_000;
const SEC: i64 = 1000 * MS;
const MIN: i64 = 60 * SEC;
const HOUR: i64 = 60 * MIN;

#[test]
fn test_duration_milliseconds() {
    let d = Duration(2 * HOUR + 30 * MIN + 15 * SEC + 250 * MS);
    assert_eq!(d.millis(), 2 * 60 * 60 * 1000 + 30 * 60 * 1000 + 15 * 1000 + 250);
    assert_eq!(Duration(-1500 * MS).millis(), -1500);
}

struct Case {
    input: &'static str,
    out: i64,
    expected_string: &'static str,
    allowed_negative: bool,
}

fn c(input: &'static str, out: i64, expected_string: &'static str, allowed_negative: bool) -> Case {
    Case { input, out, expected_string, allowed_negative }
}

fn base_cases() -> Vec<Case> {
    vec![
        c("0", 0, "0s", false),
        c("0w", 0, "0s", false),
        c("0s", 0, "", false),
        c("324ms", 324 * MS, "", false),
        c("3s", 3 * SEC, "", false),
        c("5m", 5 * MIN, "", false),
        c("1h", HOUR, "", false),
        c("4d", 4 * 24 * HOUR, "", false),
        c("4d1h", 4 * 24 * HOUR + HOUR, "", false),
        c("14d", 14 * 24 * HOUR, "2w", false),
        c("3w", 3 * 7 * 24 * HOUR, "", false),
        c("3w2d1h", 3 * 7 * 24 * HOUR + 2 * 24 * HOUR + HOUR, "23d1h", false),
        c("10y", 10 * 365 * 24 * HOUR, "", false),
    ]
}

#[test]
fn test_parse_duration() {
    let mut negative = vec![
        c("-3s", -3 * SEC, "", true),
        c("-5m", -5 * MIN, "", true),
        c("-1h", -HOUR, "", true),
        c("-2d", -2 * 24 * HOUR, "", true),
        c("-1w", -7 * 24 * HOUR, "", true),
        c("-3w2d1h", -(3 * 7 * 24 * HOUR + 2 * 24 * HOUR + HOUR), "-23d1h", true),
        c("-10y", -10 * 365 * 24 * HOUR, "", true),
    ];
    for mut b in base_cases() {
        b.allowed_negative = true;
        negative.push(b);
    }
    let mut all = base_cases();
    all.extend(negative);
    for c in all {
        let d = if c.allowed_negative { parse_duration_allow_negative(c.input) } else { parse_duration(c.input) };
        let d = d.unwrap_or_else(|_| panic!("Unexpected error on input {:?}", c.input));
        assert_eq!(d.0, c.out, "input {:?}", c.input);
        let expected = if c.expected_string.is_empty() { c.input } else { c.expected_string };
        assert_eq!(d.go_string(), expected, "input {:?}", c.input);
    }
}

// adapted: UnmarshalText/MarshalText are the YAML scalar decode and encode of model.Duration here.
#[test]
fn test_duration_unmarshal_text() {
    for c in base_cases() {
        let d: Duration = yaml::unmarshal(c.input).unwrap_or_else(|e| panic!("Unexpected error on input {:?}: {e}", c.input));
        assert_eq!(d.0, c.out);
        let expected = if c.expected_string.is_empty() { c.input } else { c.expected_string };
        assert_eq!(yaml::marshal(&d).unwrap(), format!("{expected}\n"));
    }
}

#[test]
fn test_parse_bad_duration() {
    for c in ["1", "1y1m1d", "1.5d", "d", "294y", "200y10400w", "107675d", "2584200h", ""] {
        assert!(parse_duration(c).is_err(), "Expected error on input {c}");
    }
}
