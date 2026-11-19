//! Port of `promql/value_test.go`.

use model::labels::from_strings;
use promql::{Sample, Series, matrix_contains_same_labelset, vector_contains_same_labelset};

fn s(v: &str) -> Sample {
    Sample { metric: from_strings(&["lbl", v]), ..Default::default() }
}

fn ser(v: &str) -> Series {
    Series::new(from_strings(&["lbl", v]))
}

#[test]
fn test_vector_contains_same_labelset() {
    let cases: Vec<(&str, Vec<Sample>, bool)> = vec![
        ("empty vector", vec![], false),
        ("vector with one series", vec![s("a")], false),
        ("vector with two different series", vec![s("a"), s("b")], false),
        ("vector with two equal series", vec![s("a"), s("a")], true),
        ("vector with three series, two equal", vec![s("a"), s("b"), s("a")], true),
    ];
    for (name, v, expected) in cases {
        assert_eq!(expected, vector_contains_same_labelset(&v), "{}", name);
    }
}

#[test]
fn test_matrix_contains_same_labelset() {
    let cases: Vec<(&str, Vec<Series>, bool)> = vec![
        ("empty matrix", vec![], false),
        ("matrix with one series", vec![ser("a")], false),
        ("matrix with two different series", vec![ser("a"), ser("b")], false),
        ("matrix with two equal series", vec![ser("a"), ser("a")], true),
        ("matrix with three series, two equal", vec![ser("a"), ser("b"), ser("a")], true),
    ];
    for (name, m, expected) in cases {
        assert_eq!(expected, matrix_contains_same_labelset(&m), "{}", name);
    }
}
