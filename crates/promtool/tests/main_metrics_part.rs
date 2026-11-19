//! Part of the port of cmd/promtool/main_test.go (check metrics), to merge into tests/main_test.rs.

use promtool::check_metrics::{LINT_OPTION_ALL, LINT_OPTION_NONE, MetricStat, check_metrics, check_metrics_extended};
use promtool::{FAILURE_EXIT_CODE, Io, LINT_ERR_EXIT_CODE, SUCCESS_EXIT_CODE};

#[test]
fn test_check_metrics_extended() {
    let mut f =
        std::fs::File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/../../upstream/cmd/promtool/testdata/metrics-test.prom"))
            .unwrap();

    let (stats, total) = check_metrics_extended(&mut f).unwrap();
    assert_eq!(27, total);
    assert_eq!(
        vec![
            MetricStat {
                name: "prometheus_tsdb_compaction_chunk_size_bytes".to_string(),
                cardinality: 15,
                percentage: 15f64 / 27f64,
            },
            MetricStat { name: "go_gc_duration_seconds".to_string(), cardinality: 7, percentage: 7f64 / 27f64 },
            MetricStat {
                name: "net_conntrack_dialer_conn_attempted_total".to_string(),
                cardinality: 4,
                percentage: 4f64 / 27f64,
            },
            MetricStat { name: "go_info".to_string(), cardinality: 1, percentage: 1f64 / 27f64 },
        ],
        stats
    );
}

#[test]
fn test_check_metrics_lint_options() {
    const TEST_METRICS: &str = "
# HELP testMetric_CamelCase A test metric with camelCase
# TYPE testMetric_CamelCase gauge
testMetric_CamelCase{label=\"value1\"} 1
";

    struct Case {
        name: &'static str,
        lint: &'static str,
        extended: bool,
        want_err_code: i32,
        want_lint: bool,
        want_card: bool,
    }
    let tests = [
        Case {
            name: "default_all_with_extended",
            lint: LINT_OPTION_ALL,
            extended: true,
            want_err_code: LINT_ERR_EXIT_CODE,
            want_lint: true,
            want_card: true,
        },
        Case {
            name: "lint_none_with_extended",
            lint: LINT_OPTION_NONE,
            extended: true,
            want_err_code: SUCCESS_EXIT_CODE,
            want_lint: false,
            want_card: true,
        },
        Case {
            name: "both_disabled_fails",
            lint: LINT_OPTION_NONE,
            extended: false,
            want_err_code: FAILURE_EXIT_CODE,
            want_lint: false,
            want_card: false,
        },
    ];

    for tt in tests {
        // adapted: Go swaps os.Stdin/Stdout/Stderr for pipes; the Rust function takes the streams through Io.
        let mut stdin = TEST_METRICS.as_bytes();
        let mut out_buf = Vec::new();
        let mut err_buf = Vec::new();
        let code = {
            let mut io = Io { stdin: &mut stdin, stdout: &mut out_buf, stderr: &mut err_buf };
            check_metrics(tt.extended, tt.lint, &mut io)
        };
        let out = String::from_utf8(out_buf).unwrap();
        let err = String::from_utf8(err_buf).unwrap();

        assert_eq!(tt.want_err_code, code, "{}", tt.name);
        if tt.want_lint {
            assert!(err.contains("testMetric_CamelCase"), "{}", tt.name);
        } else {
            assert!(!err.contains("testMetric_CamelCase"), "{}", tt.name);
        }

        if tt.want_card {
            assert!(out.contains("Cardinality"), "{}", tt.name);
        } else {
            assert!(!out.contains("Cardinality"), "{}", tt.name);
        }
    }
}
