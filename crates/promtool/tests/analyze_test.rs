//! Port of cmd/promtool/analyze_test.go.

use promtool::analyze::{Statistics, calc_classic_bucket_statistics, get_bucket_counts_at_time, sort_matrix};
use promtool::client_model::{LabelSet, Matrix, SamplePair, SampleStream};

fn stream(le: &str, values: &[(f64, i64)]) -> SampleStream {
    let mut metric = LabelSet::new();
    metric.insert("le".to_string(), le.to_string());
    SampleStream {
        metric,
        values: values.iter().map(|&(value, timestamp)| SamplePair { timestamp, value }).collect(),
        histograms: Vec::new(),
    }
}

/// Go `exampleMatrix` after `sortMatrix` (Go sorts it in `init`).
fn example_matrix() -> Matrix {
    let mut m = vec![
        stream("+Inf", &[(31.0, 100), (32.0, 200), (40.0, 300)]),
        stream("0.5", &[(10.0, 100), (11.0, 200), (11.0, 300)]),
        stream("10", &[(30.0, 100), (31.0, 200), (37.0, 300)]),
        stream("2", &[(25.0, 100), (26.0, 200), (27.0, 300)]),
    ];
    sort_matrix(&mut m);
    m
}

#[test]
fn test_get_bucket_counts_at_time() {
    let m = example_matrix();
    let len = m.len();
    for (time_idx, expected) in [(0, vec![10, 15, 5, 1]), (1, vec![11, 15, 5, 1]), (2, vec![11, 16, 10, 3])] {
        let res = get_bucket_counts_at_time(&m, len, time_idx).expect("no error");
        assert_eq!(expected, res, "exampleMatrix@{time_idx}");
    }
}

#[test]
fn test_calc_classic_bucket_statistics() {
    let cases = [(example_matrix(), Statistics { min_pop: 4, avg_pop: 4.0, max_pop: 4, total: 4 })];
    for (i, (mut matrix, expected)) in cases.into_iter().enumerate() {
        let res = calc_classic_bucket_statistics(&mut matrix).expect("no error");
        assert_eq!(expected, res, "case {i}");
    }
}
