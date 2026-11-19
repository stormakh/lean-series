//! Port of cmd/promtool/rules_test.go.

use std::path::{Path, PathBuf};
use std::time::Duration;

use chunkenc::{ChunkIterator, ValueType};
use model::labels::{self, ValidationScheme};
use model::matcher::{MatchType, Matcher};
use promtool::api::{QueryRangeApi, QueryResult, Range, Time};
use promtool::client_model::{LabelSet, Matrix, SamplePair, SampleStream, Value};
use promtool::rules_cmd::{new_rule_importer, RuleImporter, RuleImporterConfig};
use rules::log::Logger;
use storage::Context;

struct MockQueryRangeApi {
    samples: Matrix,
}

impl QueryRangeApi for MockQueryRangeApi {
    fn query_range(&self, _query: &str, _r: Range) -> QueryResult {
        Ok((Value::Matrix(self.samples.clone()), Vec::new(), Vec::new()))
    }
}

const DEFAULT_BLOCK_DURATION: Duration = Duration::from_millis(tsdb::db::DEFAULT_BLOCK_DURATION as u64);

/// Go `t.TempDir()` (under bench/tmp/pt-unittest; removed on drop).
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> TempDir {
        let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../bench/tmp/pt-unittest/tests");
        std::fs::create_dir_all(&base).unwrap();
        loop {
            let n = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
            let p = base.join(format!("t{n}"));
            if std::fs::create_dir(&p).is_ok() {
                return TempDir(p);
            }
        }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// `time.Date(2009, time.November, 10, 6, 34, 0, 0, time.UTC)`.
const START_UNIX: i64 = 1257834840;

fn metric(kv: &[(&str, &str)]) -> LabelSet {
    kv.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

/// TestBackfillRuleIntegration is an integration test that runs all the rule importer code to confirm the parts
/// work together.
#[test]
fn test_backfill_rule_integration() {
    const TEST_VALUE: f64 = 123.0;
    const TEST_VALUE2: f64 = 98.0;
    let start = Time::from_unix(START_UNIX, 0);
    // Go `model.Time(start.Add(-9 * time.Hour).Unix())`: seconds used as milliseconds.
    let test_time = start.sub_dur(Duration::from_secs(9 * 3600)).unix();
    let test_time2 = start.sub_dur(Duration::from_secs(8 * 3600)).unix();
    let twenty_four_hour_duration = Duration::from_secs(24 * 3600);

    struct Case {
        name: &'static str,
        runcount: usize,
        block_duration: Duration,
        expected_block_count: usize,
        expected_series_count: usize,
        expected_sample_count: usize,
        samples: Matrix,
    }
    let one = |m: LabelSet, values: Vec<SamplePair>| vec![SampleStream { metric: m, values, histograms: Vec::new() }];
    let sp = |timestamp, value| SamplePair { timestamp, value };
    let test_cases = vec![
        Case {
            name: "no samples",
            runcount: 1,
            block_duration: DEFAULT_BLOCK_DURATION,
            expected_block_count: 0,
            expected_series_count: 0,
            expected_sample_count: 0,
            samples: Vec::new(),
        },
        Case {
            name: "run importer once",
            runcount: 1,
            block_duration: DEFAULT_BLOCK_DURATION,
            expected_block_count: 8,
            expected_series_count: 4,
            expected_sample_count: 4,
            samples: one(metric(&[("name1", "val1")]), vec![sp(test_time, TEST_VALUE)]),
        },
        Case {
            name: "run importer with dup name label",
            runcount: 1,
            block_duration: DEFAULT_BLOCK_DURATION,
            expected_block_count: 8,
            expected_series_count: 4,
            expected_sample_count: 4,
            samples: one(metric(&[("__name__", "val1"), ("name1", "val1")]), vec![sp(test_time, TEST_VALUE)]),
        },
        Case {
            name: "one importer twice",
            runcount: 2,
            block_duration: DEFAULT_BLOCK_DURATION,
            expected_block_count: 8,
            expected_series_count: 4,
            expected_sample_count: 8,
            samples: one(
                metric(&[("name1", "val1")]),
                vec![sp(test_time, TEST_VALUE), sp(test_time2, TEST_VALUE2)],
            ),
        },
        Case {
            name: "run importer once with larger blocks",
            runcount: 1,
            block_duration: twenty_four_hour_duration,
            expected_block_count: 4,
            expected_series_count: 4,
            expected_sample_count: 4,
            samples: one(metric(&[("name1", "val1")]), vec![sp(test_time, TEST_VALUE)]),
        },
    ];
    // adapted: Go runs the subtests in parallel; here they run on threads.
    let handles: Vec<_> = test_cases
        .into_iter()
        .map(|tt| {
            std::thread::spawn(move || {
                let tmp_dir = TempDir::new();
                let ctx = Context::background();

                // Execute the test more than once to simulate running the rule importer twice with the same data.
                // We expect duplicate blocks with the same series are created when run more than once.
                for i in 0..tt.runcount {
                    let mut rule_importer = new_test_rule_importer(start, &tmp_dir.0, tt.samples.clone(), tt.block_duration);
                    let path1 = tmp_dir.0.join("test.file").to_str().unwrap().to_string();
                    create_single_rule_test_files(&path1).unwrap();
                    let path2 = tmp_dir.0.join("test2.file").to_str().unwrap().to_string();
                    create_multi_rule_test_files(&path2).unwrap();

                    // Confirm that the rule files were loaded in correctly.
                    let errs = rule_importer.load_groups(&[path1.clone(), path2.clone()]);
                    if let Some(err) = errs.first() {
                        panic!("{}: {err}", tt.name);
                    }
                    assert_eq!(rule_importer.groups.len(), 3, "{}", tt.name);
                    let group1 = rule_importer.groups.get(&format!("{path1};group0")).expect("group0");
                    let default_interval = Duration::from_secs(60);
                    assert_eq!(default_interval, group1.interval());
                    let g_rules = group1.all_rules();
                    assert_eq!(g_rules.len(), 1);
                    assert_eq!("rule1", g_rules[0].name());
                    assert_eq!("ruleExpr", g_rules[0].query().unwrap().to_string());
                    assert_eq!(1, g_rules[0].labels().len());

                    let group2 = rule_importer.groups.get(&format!("{path2};group2")).expect("group2");
                    assert_eq!(default_interval, group2.interval());
                    let g2_rules = group2.all_rules();
                    assert_eq!(g2_rules.len(), 2);
                    assert_eq!("grp2_rule1", g2_rules[0].name());
                    assert_eq!("grp2_rule1_expr", g2_rules[0].query().unwrap().to_string());
                    assert_eq!(0, g2_rules[0].labels().len());

                    // Backfill all recording rules then check the blocks to confirm the correct data was created.
                    let errs = rule_importer.import_all();
                    if let Some(err) = errs.first() {
                        panic!("{}: {err}", tt.name);
                    }

                    let opts = tsdb::db::default_options();
                    let db = tsdb::db::open(&tmp_dir.0, None, Some(opts), None).unwrap();

                    let blocks = db.blocks();
                    assert_eq!(blocks.len(), (i + 1) * tt.expected_block_count, "{}", tt.name);
                    drop(blocks);

                    let mut q = db.querier(i64::MIN, i64::MAX).unwrap();

                    {
                        let mut selected_series =
                            q.select(&ctx, false, None, &[Matcher::must_new(MatchType::Regexp, "", ".*")]);
                        let (mut series_count, mut samples_count) = (0, 0);
                        while selected_series.next() {
                            series_count += 1;
                            let series = selected_series.at().clone();
                            if series.labels().len() != 3 {
                                assert_eq!(2, series.labels().len());
                                let x = labels::from_strings(&["__name__", "grp2_rule1", "name1", "val1"]);
                                assert_eq!(&x, series.labels());
                            } else {
                                assert_eq!(3, series.labels().len());
                            }
                            let mut it = series.iterator(None);
                            while it.next() == ValueType::Float {
                                samples_count += 1;
                                let (ts, v) = it.at();
                                if v == TEST_VALUE {
                                    assert_eq!(test_time, ts);
                                } else {
                                    assert_eq!(test_time2, ts);
                                }
                            }
                            assert!(it.err().is_none());
                        }
                        assert!(selected_series.err().is_none());
                        assert_eq!(tt.expected_series_count, series_count, "{}", tt.name);
                        assert_eq!(tt.expected_sample_count, samples_count, "{}", tt.name);
                    }
                    assert!(q.close().is_none());
                    db.close().unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

fn new_test_rule_importer(start: Time, tmp_dir: &Path, test_samples: Matrix, block_duration: Duration) -> RuleImporter {
    let logger = Logger::nop();
    let cfg = RuleImporterConfig {
        output_dir: tmp_dir.to_str().unwrap().to_string(),
        start: start.sub_dur(Duration::from_secs(10 * 3600)),
        end: start.sub_dur(Duration::from_secs(7 * 3600)),
        eval_interval: Duration::from_secs(60),
        block_duration,
        name_validation_scheme: ValidationScheme::Utf8,
    };

    new_rule_importer(logger, cfg, Box::new(MockQueryRangeApi { samples: test_samples }))
}

fn create_single_rule_test_files(path: &str) -> std::io::Result<()> {
    let recording_rules = "groups:
- name: group0
  rules:
  - record: rule1
    expr:  ruleExpr
    labels:
        testlabel11: testlabelvalue11
";
    std::fs::write(path, recording_rules)
}

fn create_multi_rule_test_files(path: &str) -> std::io::Result<()> {
    let recording_rules = "groups:
- name: group1
  rules:
  - record: grp1_rule1
    expr: grp1_rule1_expr
    labels:
        testlabel11: testlabelvalue12
- name: group2
  rules:
  - record: grp2_rule1
    expr: grp2_rule1_expr
  - record: grp2_rule2
    expr: grp2_rule2_expr
    labels:
        testlabel11: testlabelvalue13
";
    std::fs::write(path, recording_rules)
}

/// TestBackfillLabels confirms that the labels in the rule file override the labels from the metrics
/// received from Prometheus Query API, including the __name__ label.
#[test]
fn test_backfill_labels() {
    let tmp_dir = TempDir::new();
    let ctx = Context::background();

    let start = Time::from_unix(START_UNIX, 0);
    let mock_api_samples = vec![SampleStream {
        metric: metric(&[("name1", "override-me"), ("__name__", "override-me-too")]),
        values: vec![SamplePair { timestamp: start.unix_milli(), value: 123.0 }],
        histograms: Vec::new(),
    }];
    let mut rule_importer = new_test_rule_importer(start, &tmp_dir.0, mock_api_samples, DEFAULT_BLOCK_DURATION);

    let path = tmp_dir.0.join("test.file").to_str().unwrap().to_string();
    let recording_rules = "groups:
- name: group0
  rules:
  - record: rulename
    expr:  ruleExpr
    labels:
        name1: value-from-rule
";
    std::fs::write(&path, recording_rules).unwrap();
    let errs = rule_importer.load_groups(&[path]);
    if let Some(err) = errs.first() {
        panic!("{err}");
    }

    let errs = rule_importer.import_all();
    if let Some(err) = errs.first() {
        panic!("{err}");
    }

    let opts = tsdb::db::default_options();
    let db = tsdb::db::open(&tmp_dir.0, None, Some(opts), None).unwrap();

    let mut q = db.querier(i64::MIN, i64::MAX).unwrap();

    // "correct-labels"
    {
        let mut selected_series = q.select(&ctx, false, None, &[Matcher::must_new(MatchType::Regexp, "", ".*")]);
        while selected_series.next() {
            let series = selected_series.at();
            let expected_labels = labels::from_strings(&["__name__", "rulename", "name1", "value-from-rule"]);
            assert_eq!(&expected_labels, series.labels());
        }
        assert!(selected_series.err().is_none());
    }
    assert!(q.close().is_none());
    db.close().unwrap();
}
