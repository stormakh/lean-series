//! Port of cmd/promtool/tsdb_test.go.
//!
//! Not ported: `TestFormatSeriesSetRejectsUnknownSampleType`. It feeds `chunkenc.ValueType(255)` to
//! `formatSeriesSet`; the Rust `chunkenc::ValueType` is a closed enum, so no iterator can return an unknown value
//! type and the `default` branch of Go's switch does not exist in Rust.

mod pt_tsdb_common;

use std::collections::HashMap;
use std::time::Duration;

use model::histogram::Histogram;
use model::labels;
use pt_tsdb_common::*;
use promtool::backfill::backfill;
use promtool::tsdb_cmd::{
    format_series_set, format_series_set_labels_to_json, format_series_set_open_metrics, generate_bucket,
};
use storage::{AppendV2Options, Context};

#[test]
fn test_generate_bucket() {
    struct Tc {
        min: i64,
        max: i64,
        start: i64,
        end: i64,
        step: i64,
    }
    let tcs = [Tc { min: 101, max: 141, start: 100, end: 150, step: 10 }];

    for tc in tcs {
        let (start, end, step) = generate_bucket(tc.min, tc.max);

        assert_eq!(tc.start, start);
        assert_eq!(tc.end, end);
        assert_eq!(tc.step, step);
    }
}

#[test]
fn test_tsdb_dump() {
    let storage = loaded_storage(
        r#"
		load 1m
			metric{foo="bar", baz="abc"} 1 2 3 4 5
			heavy_metric{foo="bar"} 5 4 3 2 1
			heavy_metric{foo="foo"} 5 4 3 2 1
	"#,
    );

    struct Tt {
        name: &'static str,
        mint: i64,
        maxt: i64,
        sandbox_dir_root: Option<TempDir>,
        match_: Vec<&'static str>,
        expected_dump: &'static str,
        expected_series: &'static str,
    }

    let tests = vec![
        Tt {
            name: "default match",
            mint: i64::MIN,
            maxt: i64::MAX,
            sandbox_dir_root: None,
            match_: vec!["{__name__=~'(?s:.*)'}"],
            expected_dump: "dump-test-1.prom",
            expected_series: "dump-series-1.prom",
        },
        Tt {
            name: "default match with sandbox dir root set",
            mint: i64::MIN,
            maxt: i64::MAX,
            sandbox_dir_root: Some(TempDir::new()),
            match_: vec!["{__name__=~'(?s:.*)'}"],
            expected_dump: "dump-test-1.prom",
            expected_series: "dump-series-1.prom",
        },
        Tt {
            name: "same matcher twice",
            mint: i64::MIN,
            maxt: i64::MAX,
            sandbox_dir_root: None,
            match_: vec!["{foo=~'.+'}", "{foo=~'.+'}"],
            expected_dump: "dump-test-1.prom",
            expected_series: "dump-series-1.prom",
        },
        Tt {
            name: "no duplication",
            mint: i64::MIN,
            maxt: i64::MAX,
            sandbox_dir_root: None,
            match_: vec!["{__name__=~'(?s:.*)'}", "{baz='abc'}"],
            expected_dump: "dump-test-1.prom",
            expected_series: "dump-series-1.prom",
        },
        Tt {
            name: "well merged",
            mint: i64::MIN,
            maxt: i64::MAX,
            sandbox_dir_root: None,
            match_: vec!["{__name__='heavy_metric'}", "{baz='abc'}"],
            expected_dump: "dump-test-1.prom",
            expected_series: "dump-series-1.prom",
        },
        Tt {
            name: "multi matchers",
            mint: i64::MIN,
            maxt: i64::MAX,
            sandbox_dir_root: None,
            match_: vec!["{__name__='heavy_metric',foo='foo'}", "{__name__='metric'}"],
            expected_dump: "dump-test-2.prom",
            expected_series: "dump-series-2.prom",
        },
        Tt {
            name: "with reduced mint and maxt",
            mint: 60000,
            maxt: 120000,
            sandbox_dir_root: None,
            match_: vec!["{__name__='metric'}"],
            expected_dump: "dump-test-3.prom",
            expected_series: "dump-series-3.prom",
        },
    ];
    for tt in &tests {
        let sandbox = tt.sandbox_dir_root.as_ref().map(|d| d.str()).unwrap_or_default();
        let dumped_metrics =
            get_dumped_samples(&storage.dir(), &sandbox, tt.mint, tt.maxt, &tt.match_, format_series_set);
        let expected_metrics = std::fs::read(testdata(tt.expected_dump)).expect(tt.name);
        let expected_metrics = normalize_new_line(expected_metrics);
        // Sort both, because Prometheus does not guarantee the output order.
        assert_eq!(
            sort_lines(&String::from_utf8(expected_metrics).unwrap()),
            sort_lines(&dumped_metrics),
            "{}",
            tt.name
        );

        let dumped_series = get_dumped_samples(
            &storage.dir(),
            &sandbox,
            tt.mint,
            tt.maxt,
            &tt.match_,
            format_series_set_labels_to_json,
        );
        let expected_series = std::fs::read(testdata(tt.expected_series)).expect(tt.name);
        let expected_series = normalize_new_line(expected_series);
        assert_eq!(
            sort_lines(&String::from_utf8(expected_series).unwrap()),
            sort_lines(&dumped_series),
            "{}",
            tt.name
        );
    }
}

#[test]
fn test_tsdb_dump_native_histogram() {
    let test_storage = loaded_storage(
        r#"
		load 1m
			mixed_hist{job="test"} 1 {{sum:2 count:2}} 3
	"#,
    );

    let mut app = test_storage.db.appender_v2(&Context::background());
    for i in 0..3i64 {
        let h = Histogram {
            count: (i + 1) as u64,
            sum: (i + 1) as f64,
            zero_count: (i + 1) as u64,
            zero_threshold: 0.001,
            ..Histogram::default()
        };
        app.append(
            0,
            &labels::from_strings(&[labels::METRIC_NAME, "integer_hist", "job", "test"]),
            0,
            i * 60_000,
            0.0,
            Some(&h),
            None,
            &AppendV2Options::default(),
        )
        .expect("append");
    }
    assert!(app.commit().is_none());

    let dumped_metrics = get_dumped_samples(
        &test_storage.dir(),
        "",
        i64::MIN,
        i64::MAX,
        &["{__name__=~'(?s:.*)'}"],
        format_series_set,
    );
    let expected = r#"
{__name__="integer_hist", job="test"} {count:1, sum:1, [-0.001,0.001]:1} 0
{__name__="integer_hist", job="test"} {count:2, sum:2, [-0.001,0.001]:2} 60000
{__name__="integer_hist", job="test"} {count:3, sum:3, [-0.001,0.001]:3} 120000
{__name__="mixed_hist", job="test"} 1 0
{__name__="mixed_hist", job="test"} {count:2, sum:2} 60000
{__name__="mixed_hist", job="test"} 3 120000
"#;
    assert_eq!(sort_lines(expected.trim()), sort_lines(dumped_metrics.trim()));
}

#[test]
fn test_tsdb_dump_open_metrics() {
    let storage = loaded_storage(
        r#"
		load 1m
			my_counter{foo="bar", baz="abc"} 1 2 3 4 5
			my_gauge{bar="foo", abc="baz"} 9 8 0 4 7
	"#,
    );

    struct Tt {
        name: &'static str,
        sandbox_dir_root: Option<TempDir>,
    }
    let tests = vec![
        Tt { name: "default match", sandbox_dir_root: None },
        Tt { name: "default match with sandbox dir root set", sandbox_dir_root: Some(TempDir::new()) },
    ];
    for tt in &tests {
        let expected_metrics = std::fs::read(testdata("dump-openmetrics-test.prom")).expect(tt.name);
        let expected_metrics = normalize_new_line(expected_metrics);
        let sandbox = tt.sandbox_dir_root.as_ref().map(|d| d.str()).unwrap_or_default();
        let dumped_metrics = get_dumped_samples(
            &storage.dir(),
            &sandbox,
            i64::MIN,
            i64::MAX,
            &["{__name__=~'(?s:.*)'}"],
            format_series_set_open_metrics,
        );
        assert_eq!(
            sort_lines(&String::from_utf8(expected_metrics).unwrap()),
            sort_lines(&dumped_metrics),
            "{}",
            tt.name
        );
    }
}

#[test]
fn test_tsdb_dump_open_metrics_round_trip() {
    let initial_metrics = std::fs::read(testdata("dump-openmetrics-roundtrip-test.prom")).expect("read");
    let initial_metrics = normalize_new_line(initial_metrics);

    let db_dir = TempDir::new();
    // Import samples from OM format
    let mut out = Vec::new();
    backfill(
        5000,
        &initial_metrics,
        &db_dir.str(),
        false,
        false,
        Duration::from_secs(2 * 3600),
        &HashMap::new(),
        &mut out,
    )
    .expect("backfill");
    let db = tsdb::db::open(db_dir.path(), None, Some(tsdb::db::default_options()), None).expect("open");

    // Dump the blocks into OM format
    let dumped_metrics = get_dumped_samples(
        &db_dir.str(),
        "",
        i64::MIN,
        i64::MAX,
        &["{__name__=~'(?s:.*)'}"],
        format_series_set_open_metrics,
    );

    // Should get back the initial metrics.
    assert_eq!(String::from_utf8(initial_metrics).unwrap(), dumped_metrics);
    db.close().expect("close");
}
