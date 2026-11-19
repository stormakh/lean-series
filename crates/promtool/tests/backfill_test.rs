//! Port of cmd/promtool/backfill_test.go.

mod pt_tsdb_common;

use std::collections::HashMap;
use std::time::Duration;

use chunkenc::{ChunkIterator, ValueType};
use model::labels::{self, Labels};
use model::matcher::{MatchType, Matcher};
use promtool::backfill::{backfill, get_compatible_block_duration};
use pt_tsdb_common::TempDir;
use storage::{Context, Querier};
use tsdb::db::DEFAULT_BLOCK_DURATION;

#[derive(Clone, Debug, PartialEq)]
struct BackfillSample {
    timestamp: i64,
    value: f64,
    labels: Labels,
}

fn sample(timestamp: i64, value: f64, ls: &[&str]) -> BackfillSample {
    BackfillSample { timestamp, value, labels: labels::from_strings(ls) }
}

fn sort_samples(samples: &mut [BackfillSample]) {
    // Go sort.Slice is not stable; the samples of the tests have no ties.
    samples.sort_by(|sx, sy| {
        if sx.timestamp != sy.timestamp {
            return sx.timestamp.cmp(&sy.timestamp);
        }
        sx.value.partial_cmp(&sy.value).unwrap_or(std::cmp::Ordering::Equal)
    });
}

fn query_all_series(q: &dyn Querier, _mint: i64, _maxt: i64) -> Vec<BackfillSample> {
    let mut ss = q.select(&Context::background(), false, None, &[Matcher::must_new(MatchType::Regexp, "", ".*")]);
    let mut samples = Vec::new();
    while ss.next() {
        let series = ss.at().clone();
        let mut it = series.iterator(None);
        assert!(it.err().is_none());
        while it.next() == ValueType::Float {
            let (ts, v) = it.at();
            samples.push(BackfillSample { timestamp: ts, value: v, labels: series.labels().clone() });
        }
    }
    samples
}

fn test_blocks(
    db: &tsdb::db::DB,
    expected_min_time: i64,
    expected_max_time: i64,
    expected_block_duration: i64,
    expected_samples: &mut [BackfillSample],
    expected_num_blocks: usize,
) {
    let blocks = db.blocks();
    assert_eq!(blocks.len(), expected_num_blocks, "did not create correct number of blocks");

    for (i, block) in blocks.iter().enumerate() {
        assert_eq!(
            block.min_time() / expected_block_duration,
            (block.max_time() - 1) / expected_block_duration,
            "block {} contains data outside of one aligned block duration",
            i
        );
    }

    let mut q = db.querier(i64::MIN, i64::MAX).expect("querier");

    let mut all_samples = query_all_series(&*q, expected_min_time, expected_max_time);
    sort_samples(&mut all_samples);
    sort_samples(expected_samples);
    assert_eq!(expected_samples.to_vec(), all_samples, "did not create correct samples");

    if !all_samples.is_empty() {
        assert_eq!(
            expected_min_time, all_samples[0].timestamp,
            "timestamp of first sample is not the expected minimum time"
        );
        assert_eq!(
            expected_max_time,
            all_samples[all_samples.len() - 1].timestamp,
            "timestamp of last sample is not the expected maximum time"
        );
    }
    assert!(q.close().is_none());
}

#[derive(Default)]
struct Expected {
    min_time: i64,
    max_time: i64,
    num_blocks: usize,
    block_duration: i64,
    samples: Vec<BackfillSample>,
}

struct Case {
    to_parse: &'static str,
    is_ok: bool,
    description: &'static str,
    max_samples_in_appender: usize,
    max_block_duration: Duration,
    labels: &'static [(&'static str, &'static str)],
    expected: Expected,
}

#[test]
fn test_backfill() {
    let tests = vec![
        Case {
            to_parse: r#"# EOF"#,
            is_ok: true,
            description: "Empty file.",
            max_samples_in_appender: 5000,
            max_block_duration: Duration::from_secs(0 * 3600),
            labels: &[],
            expected: Expected {
                min_time: i64::MAX,
                max_time: i64::MIN,
                num_blocks: 0,
                block_duration: DEFAULT_BLOCK_DURATION,
                samples: vec![
                ],
            },
        },
        Case {
            to_parse: r#"# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{code="200"} 1021 1565133713.989
http_requests_total{code="400"} 1 1565133713.990
# EOF
"#,
            is_ok: true,
            description: "Multiple samples with different timestamp for different series.",
            max_samples_in_appender: 5000,
            max_block_duration: Duration::from_secs(0 * 3600),
            labels: &[],
            expected: Expected {
                min_time: 1565133713989,
                max_time: 1565133713990,
                num_blocks: 1,
                block_duration: DEFAULT_BLOCK_DURATION,
                samples: vec![
                    sample(1565133713989, 1021.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(1565133713990, 1.0, &["__name__", "http_requests_total", "code", "400"]),
                ],
            },
        },
        Case {
            to_parse: r#"# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{code="200"} 1021 1565133713.989
http_requests_total{code="200"} 1022 1565392913.989
http_requests_total{code="200"} 1023 1565652113.989
# EOF
"#,
            is_ok: true,
            description: "Multiple samples separated by 3 days.",
            max_samples_in_appender: 5000,
            max_block_duration: Duration::from_secs(0 * 3600),
            labels: &[],
            expected: Expected {
                min_time: 1565133713989,
                max_time: 1565652113989,
                num_blocks: 3,
                block_duration: DEFAULT_BLOCK_DURATION,
                samples: vec![
                    sample(1565133713989, 1021.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(1565392913989, 1022.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(1565652113989, 1023.0, &["__name__", "http_requests_total", "code", "200"]),
                ],
            },
        },
        Case {
            to_parse: r#"# TYPE go info
go_info{version="go1.15.3"} 1 1565392913.989
# TYPE http_requests_total counter
http_requests_total{code="200"} 1021 1565133713.989
# EOF
"#,
            is_ok: true,
            description: "Unordered samples from multiple series, which end in different blocks.",
            max_samples_in_appender: 5000,
            max_block_duration: Duration::from_secs(0 * 3600),
            labels: &[],
            expected: Expected {
                min_time: 1565133713989,
                max_time: 1565392913989,
                num_blocks: 2,
                block_duration: DEFAULT_BLOCK_DURATION,
                samples: vec![
                    sample(1565133713989, 1021.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(1565392913989, 1.0, &["__name__", "go_info", "version", "go1.15.3"]),
                ],
            },
        },
        Case {
            to_parse: r#"# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{code="200"} 1021 1565133713.989
http_requests_total{code="200"} 1 1565133714.989
http_requests_total{code="400"} 2 1565133715.989
# EOF
"#,
            is_ok: true,
            description: "Multiple samples with different timestamp for the same series.",
            max_samples_in_appender: 5000,
            max_block_duration: Duration::from_secs(0 * 3600),
            labels: &[],
            expected: Expected {
                min_time: 1565133713989,
                max_time: 1565133715989,
                num_blocks: 1,
                block_duration: DEFAULT_BLOCK_DURATION,
                samples: vec![
                    sample(1565133713989, 1021.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(1565133714989, 1.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(1565133715989, 2.0, &["__name__", "http_requests_total", "code", "400"]),
                ],
            },
        },
        Case {
            to_parse: r#"# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{code="200"} 1021 1624463088.000
http_requests_total{code="200"} 1 1627055153.000
http_requests_total{code="400"} 2 1627056153.000
# EOF
"#,
            is_ok: true,
            description: "Long maximum block duration puts all data into one block.",
            max_samples_in_appender: 5000,
            max_block_duration: Duration::from_secs(2048 * 3600),
            labels: &[],
            expected: Expected {
                min_time: 1624463088000,
                max_time: 1627056153000,
                num_blocks: 1,
                block_duration: 1458 * 3600 * 1000,
                samples: vec![
                    sample(1624463088000, 1021.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(1627055153000, 1.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(1627056153000, 2.0, &["__name__", "http_requests_total", "code", "400"]),
                ],
            },
        },
        Case {
            to_parse: r#"# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{code="200"} 1 1624463088.000
http_requests_total{code="200"} 2 1629503088.000
http_requests_total{code="200"} 3 1629863088.000
# EOF
"#,
            is_ok: true,
            description: "Long maximum block duration puts all data into two blocks.",
            max_samples_in_appender: 5000,
            max_block_duration: Duration::from_secs(2048 * 3600),
            labels: &[],
            expected: Expected {
                min_time: 1624463088000,
                max_time: 1629863088000,
                num_blocks: 2,
                block_duration: 1458 * 3600 * 1000,
                samples: vec![
                    sample(1624463088000, 1.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(1629503088000, 2.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(1629863088000, 3.0, &["__name__", "http_requests_total", "code", "200"]),
                ],
            },
        },
        Case {
            to_parse: r#"# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{code="200"} 1 1624463088.000
http_requests_total{code="200"} 2 1765943088.000
http_requests_total{code="200"} 3 1768463088.000
# EOF
"#,
            is_ok: true,
            description: "Maximum block duration longer than longest possible duration, uses largest duration, puts all data into two blocks.",
            max_samples_in_appender: 5000,
            max_block_duration: Duration::from_secs(200000 * 3600),
            labels: &[],
            expected: Expected {
                min_time: 1624463088000,
                max_time: 1768463088000,
                num_blocks: 2,
                block_duration: 39366 * 3600 * 1000,
                samples: vec![
                    sample(1624463088000, 1.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(1765943088000, 2.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(1768463088000, 3.0, &["__name__", "http_requests_total", "code", "200"]),
                ],
            },
        },
        Case {
            to_parse: r#"# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{code="200"} 1021 1565133713.989
http_requests_total{code="200"} 1022 1565144513.989
http_requests_total{code="400"} 2 1565155313.989
http_requests_total{code="400"} 1 1565166113.989
# EOF
"#,
            is_ok: true,
            description: "Multiple samples that end up in different blocks.",
            max_samples_in_appender: 5000,
            max_block_duration: Duration::from_secs(0 * 3600),
            labels: &[],
            expected: Expected {
                min_time: 1565133713989,
                max_time: 1565166113989,
                num_blocks: 4,
                block_duration: DEFAULT_BLOCK_DURATION,
                samples: vec![
                    sample(1565133713989, 1021.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(1565144513989, 1022.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(1565155313989, 2.0, &["__name__", "http_requests_total", "code", "400"]),
                    sample(1565166113989, 1.0, &["__name__", "http_requests_total", "code", "400"]),
                ],
            },
        },
        Case {
            to_parse: r#"# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{code="200"} 1021 1565133713.989
http_requests_total{code="200"} 1022 1565133714
http_requests_total{code="200"} 1023 1565133716
http_requests_total{code="200"} 1022 1565144513.989
http_requests_total{code="400"} 2 1565155313.989
http_requests_total{code="400"} 3 1565155314
http_requests_total{code="400"} 1 1565166113.989
# EOF
"#,
            is_ok: true,
            description: "Number of samples are greater than the sample batch size.",
            max_samples_in_appender: 2,
            max_block_duration: Duration::from_secs(0 * 3600),
            labels: &[],
            expected: Expected {
                min_time: 1565133713989,
                max_time: 1565166113989,
                num_blocks: 4,
                block_duration: DEFAULT_BLOCK_DURATION,
                samples: vec![
                    sample(1565133713989, 1021.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(1565133714000, 1022.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(1565133716000, 1023.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(1565144513989, 1022.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(1565155313989, 2.0, &["__name__", "http_requests_total", "code", "400"]),
                    sample(1565155314000, 3.0, &["__name__", "http_requests_total", "code", "400"]),
                    sample(1565166113989, 1.0, &["__name__", "http_requests_total", "code", "400"]),
                ],
            },
        },
        Case {
            to_parse: r#"# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{code="200"} 1021 0
http_requests_total{code="200"} 1022 7199
http_requests_total{code="400"} 1023 0
http_requests_total{code="400"} 1024 7199
# EOF
"#,
            is_ok: true,
            description: "One series spanning 2h in same block should not cause problems to other series.",
            max_samples_in_appender: 1,
            max_block_duration: Duration::from_secs(0 * 3600),
            labels: &[],
            expected: Expected {
                min_time: 0,
                max_time: 7199000,
                num_blocks: 1,
                block_duration: DEFAULT_BLOCK_DURATION,
                samples: vec![
                    sample(0, 1021.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(7199000, 1022.0, &["__name__", "http_requests_total", "code", "200"]),
                    sample(0, 1023.0, &["__name__", "http_requests_total", "code", "400"]),
                    sample(7199000, 1024.0, &["__name__", "http_requests_total", "code", "400"]),
                ],
            },
        },
        Case {
            to_parse: r#"no_help_no_type{foo="bar"} 42 6900
# EOF
"#,
            is_ok: true,
            description: "Sample with no #HELP or #TYPE keyword.",
            max_samples_in_appender: 5000,
            max_block_duration: Duration::from_secs(0 * 3600),
            labels: &[],
            expected: Expected {
                min_time: 6900000,
                max_time: 6900000,
                num_blocks: 1,
                block_duration: DEFAULT_BLOCK_DURATION,
                samples: vec![
                    sample(6900000, 42.0, &["__name__", "no_help_no_type", "foo", "bar"]),
                ],
            },
        },
        Case {
            to_parse: r#"no_newline_after_eof 42 6900
# EOF"#,
            is_ok: true,
            description: "Sample without newline after # EOF.",
            max_samples_in_appender: 5000,
            max_block_duration: Duration::from_secs(0 * 3600),
            labels: &[],
            expected: Expected {
                min_time: 6900000,
                max_time: 6900000,
                num_blocks: 1,
                block_duration: DEFAULT_BLOCK_DURATION,
                samples: vec![
                    sample(6900000, 42.0, &["__name__", "no_newline_after_eof"]),
                ],
            },
        },
        Case {
            to_parse: r#"bare_metric 42.24 1001
# EOF
"#,
            is_ok: true,
            description: "Bare sample.",
            max_samples_in_appender: 5000,
            max_block_duration: Duration::from_secs(0 * 3600),
            labels: &[],
            expected: Expected {
                min_time: 1001000,
                max_time: 1001000,
                num_blocks: 1,
                block_duration: DEFAULT_BLOCK_DURATION,
                samples: vec![
                    sample(1001000, 42.24, &["__name__", "bare_metric"]),
                ],
            },
        },
        Case {
            to_parse: r#"# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total{code="200"} 1 1624463088.000
http_requests_total{code="200"} 2 1629503088.000
http_requests_total{code="200"} 3 1629863088.000
# EOF
"#,
            is_ok: true,
            description: "Sample with external labels.",
            max_samples_in_appender: 5000,
            max_block_duration: Duration::from_secs(2048 * 3600),
            labels: &[("cluster_id", "123"), ("org_id", "999")],
            expected: Expected {
                min_time: 1624463088000,
                max_time: 1629863088000,
                num_blocks: 2,
                block_duration: 1458 * 3600 * 1000,
                samples: vec![
                    sample(1624463088000, 1.0, &["__name__", "http_requests_total", "code", "200", "cluster_id", "123", "org_id", "999"]),
                    sample(1629503088000, 2.0, &["__name__", "http_requests_total", "code", "200", "cluster_id", "123", "org_id", "999"]),
                    sample(1629863088000, 3.0, &["__name__", "http_requests_total", "code", "200", "cluster_id", "123", "org_id", "999"]),
                ],
            },
        },
        Case {
            to_parse: r#"# HELP rpc_duration_seconds A summary of the RPC duration in seconds.
# TYPE rpc_duration_seconds summary
rpc_duration_seconds{quantile="0.01"} 3102
rpc_duration_seconds{quantile="0.05"} 3272
# EOF
"#,
            is_ok: false,
            description: "Does not have timestamp.",
            max_samples_in_appender: 0,
            max_block_duration: Duration::from_secs(0 * 3600),
            labels: &[],
            expected: Expected::default(),
        },
        Case {
            to_parse: r#"# HELP bad_metric This a bad metric
# TYPE bad_metric bad_type
bad_metric{type="has a bad type information"} 0.0 111
# EOF
"#,
            is_ok: false,
            description: "Has a bad type information.",
            max_samples_in_appender: 0,
            max_block_duration: Duration::from_secs(0 * 3600),
            labels: &[],
            expected: Expected::default(),
        },
        Case {
            to_parse: r#"# HELP no_nl This test has no newline so will fail
# TYPE no_nl gauge
no_nl{type="no newline"}
# EOF
"#,
            is_ok: false,
            description: "No newline.",
            max_samples_in_appender: 0,
            max_block_duration: Duration::from_secs(0 * 3600),
            labels: &[],
            expected: Expected::default(),
        },
        Case {
            to_parse: r#"# HELP no_eof This test has no EOF so will fail
# TYPE no_eof gauge
no_eof 1 1
"#,
            is_ok: false,
            description: "No EOF.",
            max_samples_in_appender: 0,
            max_block_duration: Duration::from_secs(0 * 3600),
            labels: &[],
            expected: Expected::default(),
        },
        Case {
            to_parse: r#"# HELP after_eof There is data after EOF.
# TYPE after_eof gauge
after_eof 1 1
# EOF
after_eof 1 2
"#,
            is_ok: false,
            description: "Data after EOF.",
            max_samples_in_appender: 0,
            max_block_duration: Duration::from_secs(0 * 3600),
            labels: &[],
            expected: Expected::default(),
        },
    ];
    // Go runs the cases in parallel; they are independent, so threads do the same here.
    std::thread::scope(|s| {
        let handles: Vec<_> = tests.into_iter().map(|test| s.spawn(move || run_case(test))).collect();
        for h in handles {
            h.join().expect("case panicked");
        }
    });
}

fn run_case(mut test: Case) {
    eprintln!("Test:{}", test.description);

    let output_dir = TempDir::new();

    let block_duration =
        Duration::from_millis(get_compatible_block_duration(test.max_block_duration.as_millis() as i64) as u64);
    let custom: HashMap<String, String> = test.labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    let mut out = Vec::new();
    let res = backfill(
        test.max_samples_in_appender,
        test.to_parse.as_bytes(),
        &output_dir.str(),
        false,
        false,
        block_duration,
        &custom,
        &mut out,
    );

    if !test.is_ok {
        assert!(res.is_err(), "{}", test.description);
        return;
    }

    if let Err(e) = res {
        panic!("{}: unexpected error: {}", test.description, e);
    }
    let mut options = tsdb::db::default_options();
    // maximum duration tests require a long retention
    options.retention_duration = 10 * 365 * 24 * 3600 * 1000;
    let db = tsdb::db::open(output_dir.path(), None, Some(options), None).expect("open");

    test_blocks(
        &db,
        test.expected.min_time,
        test.expected.max_time,
        test.expected.block_duration,
        &mut test.expected.samples,
        test.expected.num_blocks,
    );
    db.close().expect("close");
}
