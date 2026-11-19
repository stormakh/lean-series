//! Port of `tsdb/db_append_v2_test.go`, lines 1..4075.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use chunkenc::{ChunkIterator, ValueType};
use model::histogram::{CounterResetHint, FloatHistogram, Histogram, Span};
use model::labels::{self, Labels};
use model::matcher::{MatchType, Matcher};
use storage::{AppendV2Options, AppenderV2, Context, Sample, SeriesArc};
use tsdb::db::{self, DB};
use tsdb_format::record;
use tsdb_format::tombstones::{self, Interval};
use tsdb_format::wlog;

use crate::common::{fs, new_test_db, query, with_dir, with_opts, TempDir, TestDb};

// ---------------------------------------------------------------------------------------------------------------
// Helpers.

fn ctx() -> Context {
    Context::background()
}

fn ls(kv: &[&str]) -> Labels {
    labels::from_strings(kv)
}

fn m(t: MatchType, n: &str, v: &str) -> Matcher {
    Matcher::must_new(t, n, v)
}

fn meq(n: &str, v: &str) -> Matcher {
    m(MatchType::Equal, n, v)
}

fn ao() -> AppendV2Options<'static> {
    AppendV2Options::default()
}

/// `app.Append(0, l, 0, t, v, nil, nil, storage.AOptions{})`.
fn app_f(app: &mut dyn AppenderV2, l: &Labels, t: i64, v: f64) -> Result<u64, storage::Error> {
    app.append(0, l, 0, t, v, None, None, &ao())
}

fn app_h(app: &mut dyn AppenderV2, l: &Labels, t: i64, h: Option<&Histogram>, fh: Option<&FloatHistogram>) -> Result<u64, storage::Error> {
    app.append(0, l, 0, t, 0.0, h, fh, &ao())
}

fn smap(entries: Vec<(&str, Vec<Sample>)>) -> BTreeMap<String, Vec<Sample>> {
    entries.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

fn rand_f64() -> f64 {
    crate::common::rand_f64()
}

/// Go `rand.Int63n(n)`.
fn rand_i63n(n: i64) -> i64 {
    let mut b = [0u8; 8];
    crypto::random_bytes(&mut b);
    (u64::from_le_bytes(b) % n as u64) as i64
}

fn querier(db: &DB, mint: i64, maxt: i64) -> Box<dyn storage::Querier> {
    db.querier(mint, maxt).expect("querier")
}

/// Expands the samples of a series (Go `storage.ExpandSamples(s.Iterator(nil), nil)`).
fn expand(s: &SeriesArc) -> Result<Vec<Sample>, storage::Error> {
    let mut it = s.iterator(None);
    storage::expand_samples(&mut it)
}

/// Go `boundedSamples` (head_test.go).
fn bounded_samples(full: &[Sample], mint: i64, maxt: i64) -> Vec<Sample> {
    let mut full = full;
    while let Some(f) = full.first() {
        if f.t >= mint {
            break;
        }
        full = &full[1..];
    }
    for (i, s) in full.iter().enumerate() {
        if s.t > maxt {
            return full[..i].to_vec();
        }
    }
    full.to_vec()
}

/// The comparison loop of the Go tests that compare a select result with a `newMockSeriesSet` of one list
/// series.
fn compare_with_list_series(res: &mut storage::BoxSeriesSet, exp_lbls: &Labels, exp_samples: Vec<Sample>, check_warnings: bool) {
    let expss_series: Vec<SeriesArc> = vec![storage::new_list_series(exp_lbls.clone(), exp_samples)];
    let mut i = 0usize;
    loop {
        let eok = i < expss_series.len();
        let rok = res.next();
        assert_eq!(eok, rok);
        if !eok {
            if check_warnings {
                assert!(res.warnings().is_empty());
            }
            break;
        }
        let sexp = &expss_series[i];
        i += 1;
        let sres = res.at().clone();
        assert_eq!(sexp.labels(), sres.labels());
        let (smpl_exp, err_exp) = split(expand(sexp));
        let (smpl_res, err_res) = split(expand(&sres));
        assert_eq!(err_exp, err_res);
        assert_eq!(smpl_exp, smpl_res);
    }
}

fn split<T: Default, E>(r: Result<T, E>) -> (T, Option<E>) {
    match r {
        Ok(v) => (v, None),
        Err(e) => (T::default(), Some(e)),
    }
}

fn sum_float_values(q: &dyn storage::Querier) -> f64 {
    let mut series_set = q.select(&ctx(), false, None, &[meq("foo", "bar")]);
    let mut sum = 0.0;
    while series_set.next() {
        let s = series_set.at().clone();
        let mut it = s.iterator(None);
        while it.next() == ValueType::Float {
            let (_, v) = it.at();
            sum += v;
        }
        assert!(it.err().is_none());
    }
    assert!(series_set.err().is_none());
    assert!(series_set.warnings().is_empty());
    sum
}

// ---------------------------------------------------------------------------------------------------------------
// Tests.

#[test]
fn test_data_available_only_after_commit_append_v2() {
    let db = new_test_db(vec![]);

    let mut app = db.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["foo", "bar"]), 0, 0.0).unwrap();

    let series_set = query(querier(&db, 0, 1), &[meq("foo", "bar")]);
    assert_eq!(BTreeMap::new(), series_set);

    assert!(app.commit().is_none());

    let series_set = query(querier(&db, 0, 1), &[meq("foo", "bar")]);
    assert_eq!(smap(vec![(r#"{foo="bar"}"#, vec![fs(0, 0.0)])]), series_set);
}

#[test]
fn test_no_panic_after_wal_corruption_append_v2() {
    let mut opts = db::default_options();
    opts.wal_segment_size = 32 * 1024;
    let db = new_test_db(vec![with_opts(opts)]);

    // Append until the first mmapped head chunk.
    let mut exp_samples = Vec::new();
    let mut maxt = 0i64;
    {
        // Appending 121 samples because on the 121st a new chunk will be created.
        for _ in 0..121 {
            let mut app = db.appender_v2(&ctx());
            let r = app_f(&mut *app, &ls(&["foo", "bar"]), maxt, 0.0);
            exp_samples.push(fs(maxt, 0.0));
            r.unwrap();
            assert!(app.commit().is_none());
            maxt += 1;
        }
        db.close().unwrap();
    }

    // Corrupt the WAL after the first sample of the series so that it has at least one sample and it is not
    // garbage collected. The repair deletes all WAL records after the corrupted record and these are read from the
    // mmapped chunk.
    {
        let wal_dir = db.dir().join("wal");
        let mut wal_files: Vec<_> = std::fs::read_dir(&wal_dir).unwrap().map(|e| e.unwrap().path()).collect();
        wal_files.sort();
        let f = std::fs::OpenOptions::new().read(true).write(true).open(&wal_files[0]).unwrap();
        let mut r = wlog::Reader::new(std::io::BufReader::new(f.try_clone().unwrap()));
        assert!(r.next(), "reading the series record");
        assert!(r.next(), "reading the first sample record");
        // Write an invalid record header to corrupt everything after the first wal sample.
        use std::os::unix::fs::FileExt;
        f.write_at(&[99], r.offset() as u64).unwrap();
    }

    // Query the data.
    {
        let db = new_test_db(vec![with_dir(db.dir())]);
        assert_eq!(1.0, db.head().metrics.wal_corruptions_total.get(), "WAL corruption count mismatch");

        let series_set = query(querier(&db, 0, maxt), &[meq("", "")]);
        // The last sample should be missing as it was after the WAL segment corruption.
        assert_eq!(smap(vec![(r#"{foo="bar"}"#, exp_samples[..exp_samples.len() - 1].to_vec())]), series_set);
    }
}

#[test]
fn test_data_not_available_after_rollback_append_v2() {
    let db = new_test_db(vec![]);

    let mut app = db.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["type", "float"]), 0, 0.0).unwrap();
    let h = Histogram { count: 42, sum: f64::NAN, ..Default::default() };
    app_h(&mut *app, &ls(&["type", "histogram"]), 0, Some(&h), None).unwrap();
    let fh = FloatHistogram { count: 42.0, sum: f64::NAN, ..Default::default() };
    app_h(&mut *app, &ls(&["type", "floathistogram"]), 0, None, Some(&fh)).unwrap();
    assert!(app.rollback().is_none());

    for typ in ["float", "histogram", "floathistogram"] {
        let series_set = query(querier(&db, 0, 1), &[meq("type", typ)]);
        assert_eq!(BTreeMap::new(), series_set);
    }

    let sr = wlog::new_segments_reader(db.head().wal().unwrap().dir()).unwrap();
    // Read records from WAL and check for expected count of series and samples.
    let mut r = wlog::Reader::new(sr);
    let mut dec = record::Decoder::new(tsdb::Logger::nop());
    let (mut wal_series_count, mut wal_samples_count, mut wal_histogram_count, mut wal_float_histogram_count, mut wal_exemplars_count) =
        (0, 0, 0, 0, 0);
    while r.next() {
        let rec = r.record();
        match dec.r#type(rec) {
            record::Type::Series => {
                let mut v = Vec::new();
                dec.series(rec, &mut v).unwrap();
                wal_series_count += v.len();
            }
            record::Type::Samples | record::Type::SamplesV2 => {
                let mut v = Vec::new();
                dec.samples(rec, &mut v).unwrap();
                wal_samples_count += v.len();
            }
            record::Type::Exemplars => {
                let mut v = Vec::new();
                dec.exemplars(rec, &mut v).unwrap();
                wal_exemplars_count += v.len();
            }
            record::Type::HistogramSamples | record::Type::CustomBucketsHistogramSamples | record::Type::HistogramSamplesV2 => {
                let mut v = Vec::new();
                dec.histogram_samples(rec, &mut v).unwrap();
                wal_histogram_count += v.len();
            }
            record::Type::FloatHistogramSamples
            | record::Type::CustomBucketsFloatHistogramSamples
            | record::Type::FloatHistogramSamplesV2 => {
                let mut v = Vec::new();
                dec.float_histogram_samples(rec, &mut v).unwrap();
                wal_float_histogram_count += v.len();
            }
            _ => {}
        }
    }

    // Check that only series get stored after calling Rollback.
    assert_eq!(3, wal_series_count, "series should have been written to WAL");
    assert_eq!(0, wal_samples_count, "samples should not have been written to WAL");
    assert_eq!(0, wal_exemplars_count, "exemplars should not have been written to WAL");
    assert_eq!(0, wal_histogram_count, "histograms should not have been written to WAL");
    assert_eq!(0, wal_float_histogram_count, "float histograms should not have been written to WAL");
}

#[test]
fn test_db_appender_v2_add_ref() {
    let db = new_test_db(vec![]);
    let empty = labels::empty_labels();

    let mut app1 = db.appender_v2(&ctx());
    let ref1 = app_f(&mut *app1, &ls(&["a", "b"]), 123, 0.0).unwrap();

    // Reference should already work before commit.
    let ref2 = app1.append(ref1, &empty, 0, 124, 1.0, None, None, &ao()).unwrap();
    assert_eq!(ref1, ref2);

    assert!(app1.commit().is_none());

    let mut app2 = db.appender_v2(&ctx());

    // First ref should already work in next transaction.
    let ref3 = app2.append(ref1, &empty, 0, 125, 0.0, None, None, &ao()).unwrap();
    assert_eq!(ref1, ref3);

    let ref4 = app2.append(ref1, &ls(&["a", "b"]), 0, 133, 1.0, None, None, &ao()).unwrap();
    assert_eq!(ref1, ref4);

    // Reference must be valid to add another sample.
    let ref5 = app2.append(ref2, &empty, 0, 143, 2.0, None, None, &ao()).unwrap();
    assert_eq!(ref1, ref5);

    // Missing labels & invalid refs should fail.
    let err = app2.append(9999999, &empty, 0, 1, 1.0, None, None, &ao()).unwrap_err();
    assert!(err.is(&tsdb::head::ERR_INVALID_SAMPLE), "{err}");

    assert!(app2.commit().is_none());

    let res = query(querier(&db, 0, 200), &[meq("a", "b")]);
    assert_eq!(
        smap(vec![(
            &ls(&["a", "b"]).to_string(),
            vec![fs(123, 0.0), fs(124, 1.0), fs(125, 0.0), fs(133, 1.0), fs(143, 2.0)]
        )]),
        res
    );
}

#[test]
fn test_db_appender_v2_empty_labels_ignored() {
    let db = new_test_db(vec![]);

    let mut app1 = db.appender_v2(&ctx());
    let ref1 = app_f(&mut *app1, &ls(&["a", "b"]), 123, 0.0).unwrap();

    // Add with empty label.
    let ref2 = app_f(&mut *app1, &ls(&["a", "b", "c", ""]), 124, 0.0).unwrap();

    // Should be the same series.
    assert_eq!(ref1, ref2);

    assert!(app1.commit().is_none());
}

#[test]
fn test_db_appender_v2_empty_labelset_causes_error() {
    let db = new_test_db(vec![]);

    let mut app = db.appender_v2(&ctx());
    let err = app_f(&mut *app, &labels::empty_labels(), 0, 0.0).unwrap_err();
    assert_eq!("empty labelset: invalid sample", err.to_string());
}

#[test]
fn test_delete_simple_append_v2() {
    const NUM_SAMPLES: i64 = 10;

    let iv = |mint, maxt| Interval { mint, maxt };
    let cases: Vec<(Vec<Interval>, Vec<i64>)> = vec![
        (vec![iv(0, 3)], vec![4, 5, 6, 7, 8, 9]),
        (vec![iv(1, 3)], vec![0, 4, 5, 6, 7, 8, 9]),
        (vec![iv(1, 3), iv(4, 7)], vec![0, 8, 9]),
        (vec![iv(1, 3), iv(4, 700)], vec![0]),
        // This case is to ensure that labels and symbols are deleted.
        (vec![iv(0, 9)], vec![]),
    ];

    for (intervals, remaint) in cases {
        let db = new_test_db(vec![]);

        let mut app = db.appender_v2(&ctx());
        let mut smpls = vec![0.0; NUM_SAMPLES as usize];
        for i in 0..NUM_SAMPLES {
            smpls[i as usize] = rand_f64();
            let _ = app_f(&mut *app, &ls(&["a", "b"]), i, smpls[i as usize]);
        }
        assert!(app.commit().is_none());

        // Delete the ranges.
        for r in &intervals {
            db.delete(&ctx(), r.mint, r.maxt, &[meq("a", "b")]).unwrap();
        }

        // Compare the result.
        let q = querier(&db, 0, NUM_SAMPLES);
        let mut res = q.select(&ctx(), false, None, &[meq("a", "b")]);

        let exp_samples: Vec<Sample> = remaint.iter().map(|&ts| fs(ts, smpls[ts as usize])).collect();
        compare_with_list_series(&mut res, &ls(&["a", "b"]), exp_samples, true);
    }
}

#[test]
fn test_amend_histogram_datapoint_causes_error_append_v2() {
    let db = new_test_db(vec![]);

    let mut app = db.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["a", "b"]), 0, 0.0).unwrap();
    assert!(app.commit().is_none());

    let mut app = db.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["a", "b"]), 0, 0.0).unwrap();
    let err = app_f(&mut *app, &ls(&["a", "b"]), 0, 1.0).unwrap_err();
    assert!(err.is(&storage::ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP), "{err}");
    assert!(app.rollback().is_none());

    let mut h = Histogram {
        schema: 3,
        count: 52,
        sum: 2.7,
        zero_threshold: 0.1,
        zero_count: 42,
        positive_spans: vec![Span { offset: 0, length: 4 }, Span { offset: 10, length: 3 }],
        positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
        ..Default::default()
    };
    let mut fh = h.to_float();

    let mut app = db.appender_v2(&ctx());
    app_h(&mut *app, &ls(&["a", "c"]), 0, Some(&h.copy()), None).unwrap();
    assert!(app.commit().is_none());

    let mut app = db.appender_v2(&ctx());
    app_h(&mut *app, &ls(&["a", "c"]), 0, Some(&h.copy()), None).unwrap();
    h.schema = 2;
    let err = app_h(&mut *app, &ls(&["a", "c"]), 0, Some(&h.copy()), None).unwrap_err();
    assert_eq!(storage::ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP, err);
    assert!(app.rollback().is_none());

    // Float histogram.
    let mut app = db.appender_v2(&ctx());
    app_h(&mut *app, &ls(&["a", "d"]), 0, None, Some(&fh.copy())).unwrap();
    assert!(app.commit().is_none());

    let mut app = db.appender_v2(&ctx());
    app_h(&mut *app, &ls(&["a", "d"]), 0, None, Some(&fh.copy())).unwrap();
    fh.schema = 2;
    let err = app_h(&mut *app, &ls(&["a", "d"]), 0, None, Some(&fh.copy())).unwrap_err();
    assert_eq!(storage::ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP, err);
    assert!(app.rollback().is_none());
}

#[test]
fn test_duplicate_na_n_datapoint_no_amend_error_append_v2() {
    let db = new_test_db(vec![]);

    let mut app = db.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["a", "b"]), 0, f64::NAN).unwrap();
    assert!(app.commit().is_none());

    let mut app = db.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["a", "b"]), 0, f64::NAN).unwrap();
}

#[test]
fn test_non_duplicate_na_n_datapoints_causes_amend_error_append_v2() {
    let db = new_test_db(vec![]);

    let mut app = db.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["a", "b"]), 0, f64::from_bits(0x7ff0000000000001)).unwrap();
    assert!(app.commit().is_none());

    let mut app = db.appender_v2(&ctx());
    let err = app_f(&mut *app, &ls(&["a", "b"]), 0, f64::from_bits(0x7ff0000000000002)).unwrap_err();
    assert!(err.is(&storage::ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP), "{err}");
}

#[test]
fn test_skipping_invalid_values_in_same_txn_append_v2() {
    let db = new_test_db(vec![]);

    // Append AmendedValue.
    let mut app = db.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["a", "b"]), 0, 1.0).unwrap();
    app_f(&mut *app, &ls(&["a", "b"]), 0, 2.0).unwrap();
    assert!(app.commit().is_none());

    // Make sure the right value is stored.
    let ss_map = query(querier(&db, 0, 10), &[meq("a", "b")]);
    assert_eq!(smap(vec![(&ls(&["a", "b"]).to_string(), vec![fs(0, 1.0)])]), ss_map);

    // Append Out of Order Value.
    let mut app = db.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["a", "b"]), 10, 3.0).unwrap();
    app_f(&mut *app, &ls(&["a", "b"]), 7, 5.0).unwrap();
    assert!(app.commit().is_none());

    let ss_map = query(querier(&db, 0, 10), &[meq("a", "b")]);
    assert_eq!(smap(vec![(&ls(&["a", "b"]).to_string(), vec![fs(0, 1.0), fs(10, 3.0)])]), ss_map);
}

#[test]
fn test_db_snapshot_append_v2() {
    let db = new_test_db(vec![]);

    // Append data.
    let mut app = db.appender_v2(&ctx());
    let mint = 1414141414000i64;
    for i in 0..1000 {
        app_f(&mut *app, &ls(&["foo", "bar"]), mint + i, 1.0).unwrap();
    }
    assert!(app.commit().is_none());

    // Create snapshot.
    let snap = TempDir::new();
    db.snapshot(snap.path(), true).unwrap();
    db.close().unwrap();

    // Reopen DB from snapshot.
    let db = new_test_db(vec![with_dir(snap.path())]);

    let mut q = querier(&db, mint, mint + 1000);
    let sum = sum_float_values(&*q);
    assert!(q.close().is_none());
    assert_eq!(1000.0, sum);
}

#[test]
fn test_db_snapshot_chunks_outside_of_compacted_range_append_v2() {
    let db = new_test_db(vec![]);

    let mut app = db.appender_v2(&ctx());
    let mint = 1414141414000i64;
    for i in 0..1000 {
        app_f(&mut *app, &ls(&["foo", "bar"]), mint + i, 1.0).unwrap();
    }
    assert!(app.commit().is_none());

    let snap = TempDir::new();

    // Hackingly introduce "race", by having lower max time then maxTime in last chunk.
    db.head().max_time.fetch_sub(10, Ordering::SeqCst);

    db.snapshot(snap.path(), true).unwrap();
    db.close().unwrap();

    // Reopen DB from snapshot.
    let db = new_test_db(vec![with_dir(snap.path())]);

    let mut q = querier(&db, mint, mint + 1000);
    let sum = sum_float_values(&*q);
    assert!(q.close().is_none());

    // Since we snapshotted with MaxTime - 10, so expect 10 less samples.
    assert_eq!(1000.0 - 10.0, sum);
}

#[test]
fn test_db_snapshot_with_delete_append_v2() {
    const NUM_SAMPLES: i64 = 10;

    let db = new_test_db(vec![]);

    let mut app = db.appender_v2(&ctx());
    let mut smpls = vec![0.0; NUM_SAMPLES as usize];
    for i in 0..NUM_SAMPLES {
        smpls[i as usize] = rand_f64();
        let _ = app_f(&mut *app, &ls(&["a", "b"]), i, smpls[i as usize]);
    }
    assert!(app.commit().is_none());

    let cases: Vec<(Vec<Interval>, Vec<i64>)> =
        vec![(vec![Interval { mint: 1, maxt: 3 }, Interval { mint: 4, maxt: 7 }], vec![0, 8, 9])];

    for (intervals, remaint) in cases {
        // Delete the ranges.
        for r in &intervals {
            db.delete(&ctx(), r.mint, r.maxt, &[meq("a", "b")]).unwrap();
        }

        // Create snapshot.
        let snap = TempDir::new();
        db.snapshot(snap.path(), true).unwrap();

        // Reopen DB from snapshot.
        let db = new_test_db(vec![with_dir(snap.path())]);

        // Compare the result.
        let mut q = querier(&db, 0, NUM_SAMPLES);
        {
            let mut res = q.select(&ctx(), false, None, &[meq("a", "b")]);

            let exp_samples: Vec<Sample> = remaint.iter().map(|&ts| fs(ts, smpls[ts as usize])).collect();
            if exp_samples.is_empty() {
                assert!(!res.next());
            } else {
                compare_with_list_series(&mut res, &ls(&["a", "b"]), exp_samples, true);
            }
        }
        assert!(q.close().is_none());
    }
}

#[test]
fn test_db_e2e_append_v2() {
    const NUM_DATAPOINTS: usize = 1000;
    const NUM_RANGES: usize = 1000;
    const TIME_INTERVAL: i64 = 3;

    // Create 8 series with 1000 data-points of different ranges and run queries.
    let lbls: Vec<Labels> = [
        ["b", "localhost:9090", "prometheus"],
        ["b", "127.0.0.1:9090", "prometheus"],
        ["b", "127.0.0.1:9090", "prom-k8s"],
        ["b", "localhost:9090", "prom-k8s"],
        ["c", "localhost:9090", "prometheus"],
        ["c", "127.0.0.1:9090", "prometheus"],
        ["c", "127.0.0.1:9090", "prom-k8s"],
        ["c", "localhost:9090", "prom-k8s"],
    ]
    .iter()
    .map(|[a, i, j]| ls(&["a", a, "instance", i, "job", j]))
    .collect();

    let mut series_map: BTreeMap<String, Vec<Sample>> = BTreeMap::new();
    for l in &lbls {
        series_map.insert(l.to_string(), Vec::new());
    }

    let db = new_test_db(vec![]);

    let mut app = db.appender_v2(&ctx());
    for lset in &lbls {
        let mut series = Vec::new();
        let mut ts = rand_i63n(300);
        for _ in 0..NUM_DATAPOINTS {
            let v = rand_f64();
            series.push(fs(ts, v));
            app_f(&mut *app, lset, ts, v).unwrap();
            ts += rand_i63n(TIME_INTERVAL) + 1;
        }
        series_map.insert(lset.to_string(), series);
    }
    assert!(app.commit().is_none());

    // Query each selector on 1000 random time-ranges.
    let queries: Vec<Vec<Matcher>> = vec![
        vec![meq("a", "b")],
        vec![meq("a", "b"), meq("job", "prom-k8s")],
        vec![meq("a", "c"), meq("instance", "localhost:9090"), meq("job", "prometheus")],
        // TODO: Add Regexp Matchers.
    ];

    for qry in &queries {
        let mut matched: Vec<Labels> =
            lbls.iter().filter(|l| qry.iter().all(|m| m.matches(l.get(&m.name)))).cloned().collect();
        matched.sort();

        for _ in 0..NUM_RANGES {
            let mint = rand_i63n(300);
            let maxt = mint + rand_i63n(TIME_INTERVAL * NUM_DATAPOINTS as i64);

            let mut expected = BTreeMap::new();
            // Build the mockSeriesSet.
            for m in &matched {
                let smpls = bounded_samples(&series_map[&m.to_string()], mint, maxt);
                if !smpls.is_empty() {
                    expected.insert(m.to_string(), smpls);
                }
            }

            let mut q = querier(&db, mint, maxt);
            {
                let mut ss = q.select(&ctx(), false, None, qry);
                let mut result = BTreeMap::new();
                while ss.next() {
                    let x = ss.at().clone();
                    let smpls = expand(&x).unwrap();
                    if !smpls.is_empty() {
                        result.insert(x.labels().to_string(), smpls);
                    }
                }
                assert!(ss.err().is_none());
                assert!(ss.warnings().is_empty());
                assert_eq!(expected, result);
            }
            q.close();
        }
    }
}

#[test]
fn test_wal_flushed_on_db_close_append_v2() {
    let db = new_test_db(vec![]);

    let lbls = ls(&["labelname", "labelvalue"]);

    let mut app = db.appender_v2(&ctx());
    app_f(&mut *app, &lbls, 0, 1.0).unwrap();
    assert!(app.commit().is_none());

    db.close().unwrap();

    let db = new_test_db(vec![with_dir(db.dir())]);

    let q = querier(&db, 0, 1);
    let (values, ws, err) = q.label_values(&ctx(), "labelname", None, &[]);
    assert!(err.is_none());
    assert!(ws.is_empty());
    assert_eq!(vec!["labelvalue".to_string()], values);
}

#[test]
fn test_wal_segment_size_options_append_v2() {
    fn files_of(db_dir: &Path) -> Vec<(String, u64)> {
        let mut files: Vec<(String, u64)> = std::fs::read_dir(db_dir.join("wal"))
            .unwrap()
            .map(|e| e.unwrap())
            .filter(|e| !e.file_type().unwrap().is_dir())
            .map(|e| (e.file_name().to_string_lossy().into_owned(), e.metadata().unwrap().len()))
            .collect();
        files.sort();
        files
    }
    let tests: Vec<(i64, fn(&Path, i64))> = vec![
        // Default Wal Size.
        (0, |db_dir, _| {
            let files = files_of(db_dir);
            let def = db::default_options().wal_segment_size as u64;
            // All the full segment files (all but the last) should match the segment size option.
            for (name, size) in &files[..files.len() - 1] {
                assert_eq!(def, *size, "WAL file size doesn't match WALSegmentSize option, filename: {name}");
            }
            let last = files.last().unwrap();
            assert!(def > last.1, "last WAL file size is not smaller than the WALSegmentSize option, filename: {}", last.0);
        }),
        // Custom Wal Size.
        (2 * 32 * 1024, |db_dir, segment_size| {
            let files = files_of(db_dir);
            assert!(!files.is_empty(), "current WALSegmentSize should result in more than a single WAL file.");
            // All the full segment files (all but the last) should match the segment size option.
            for (name, size) in &files[..files.len() - 1] {
                assert_eq!(segment_size as u64, *size, "WAL file size doesn't match WALSegmentSize option, filename: {name}");
            }
            let last = files.last().unwrap();
            assert!(
                segment_size as u64 > last.1,
                "last WAL file size is not smaller than the WALSegmentSize option, filename: {}",
                last.0
            );
        }),
        // Wal disabled.
        (-1, |db_dir, _| {
            // Check that WAL dir is not there.
            assert!(std::fs::metadata(db_dir.join("wal")).is_err());
            // Check that there is chunks dir.
            std::fs::metadata(tsdb::head::mmapped_chunks_dir(db_dir)).unwrap();
        }),
    ];
    for (segment_size, test_func) in tests {
        let mut opts = db::default_options();
        opts.wal_segment_size = segment_size;
        let db = new_test_db(vec![with_opts(opts)]);

        for i in 0..155i64 {
            let mut app = db.appender_v2(&ctx());
            let r = app_f(&mut *app, &ls(&[&format!("wal{i}"), "size"]), i, rand_f64()).unwrap();
            for j in 1..=78i64 {
                app.append(r, &labels::empty_labels(), 0, i + j, rand_f64(), None, None, &ao()).unwrap();
            }
            assert!(app.commit().is_none());
        }

        db.close().unwrap();
        test_func(db.dir(), segment_size);
    }
}

#[test]
fn test_wal_replay_race_on_samples_logged_before_series_append_v2() {
    const NUM_RUNS: usize = 1;
    const NUM_SAMPLES_BEFORE_SERIES_CREATION: usize = 1000;

    // We test both with few and many samples appended after series creation. If samples are < 120 then there's no
    // mmap-ed chunk, otherwise there's at least 1 mmap-ed chunk when replaying the WAL.
    for enable_st_storage in [false, true] {
        for num_samples_after_series_creation in [1usize, 1000] {
            for _run in 1..=NUM_RUNS {
                wal_replay_race_on_samples_logged_before_series(
                    NUM_SAMPLES_BEFORE_SERIES_CREATION,
                    num_samples_after_series_creation,
                    enable_st_storage,
                );
            }
        }
    }
}

fn wal_replay_race_on_samples_logged_before_series(
    num_samples_before_series_creation: usize,
    num_samples_after_series_creation: usize,
    enable_st_storage: bool,
) {
    const NUM_SERIES: usize = 1000;

    let db = new_test_db(vec![]);
    db.disable_compactions();

    for series_ref in 1..=NUM_SERIES {
        // Log samples before the series is logged to the WAL.
        let enc = record::Encoder { enable_st_storage };
        let samples: Vec<record::RefSample> = (0..num_samples_before_series_creation)
            .map(|ts| record::RefSample { r#ref: series_ref as u64, st: 0, t: ts as i64, v: ts as f64 })
            .collect();
        let mut buf = Vec::new();
        enc.samples(&samples, &mut buf);
        db.head().wal().unwrap().log(&buf).unwrap();

        // Add samples via appender so that they're logged after the series in the WAL.
        let mut app = db.appender_v2(&ctx());
        let lbls = ls(&["series_id", &series_ref.to_string()]);
        for ts in num_samples_before_series_creation..num_samples_before_series_creation + num_samples_after_series_creation {
            app_f(&mut *app, &lbls, ts as i64, ts as f64).unwrap();
        }
        assert!(app.commit().is_none());
    }

    db.close().unwrap();

    // Reopen the DB, replaying the WAL.
    let db = new_test_db(vec![with_dir(db.dir())]);

    // Query back chunks for all series.
    let q = db.chunk_querier(i64::MIN, i64::MAX).unwrap();
    let mut set = q.select(&ctx(), false, None, &[m(MatchType::Regexp, "series_id", ".+")]);
    let mut actual_series = 0;
    while set.next() {
        actual_series += 1;
        let mut actual_chunks = 0;
        let s = set.at().clone();
        let mut chunks_it = s.iterator(None);
        while chunks_it.next() {
            actual_chunks += 1;
        }
        assert!(chunks_it.err().is_none());

        // We expect 1 chunk every 120 samples after series creation.
        assert_eq!((num_samples_after_series_creation / 120) + 1, actual_chunks, "series: {}", s.labels());
    }
    assert!(set.err().is_none());
    assert_eq!(NUM_SERIES, actual_series);
}

#[test]
fn test_tombstone_clean_append_v2() {
    const NUM_SAMPLES: i64 = 10;

    let db = new_test_db(vec![]);

    let mut app = db.appender_v2(&ctx());
    let mut smpls = vec![0.0; NUM_SAMPLES as usize];
    for i in 0..NUM_SAMPLES {
        smpls[i as usize] = rand_f64();
        let _ = app_f(&mut *app, &ls(&["a", "b"]), i, smpls[i as usize]);
    }
    assert!(app.commit().is_none());

    let cases: Vec<(Vec<Interval>, Vec<i64>)> =
        vec![(vec![Interval { mint: 1, maxt: 3 }, Interval { mint: 4, maxt: 7 }], vec![0, 8, 9])];

    let mut keep = Vec::new();
    for (intervals, remaint) in cases {
        // Create snapshot.
        let snap = TempDir::new();
        db.snapshot(snap.path(), true).unwrap();
        db.close().unwrap();

        // Reopen DB from snapshot.
        let db = new_test_db(vec![with_dir(snap.path())]);

        for r in &intervals {
            db.delete(&ctx(), r.mint, r.maxt, &[meq("a", "b")]).unwrap();
        }

        // All of the setup for THIS line.
        db.clean_tombstones().unwrap();

        // Compare the result.
        let q = querier(&db, 0, NUM_SAMPLES);
        let mut res = q.select(&ctx(), false, None, &[meq("a", "b")]);

        let exp_samples: Vec<Sample> = remaint.iter().map(|&ts| fs(ts, smpls[ts as usize])).collect();
        if exp_samples.is_empty() {
            assert!(!res.next());
            continue;
        }
        compare_with_list_series(&mut res, &ls(&["a", "b"]), exp_samples, false);
        assert!(res.warnings().is_empty());

        for b in db.blocks() {
            // adapted: Go compares `b.tombstones` with `tombstones.NewMemTombstones()`; the block's tombstone
            // reader must be empty.
            let tr = tsdb::BlockReader::tombstones(&b).unwrap();
            assert_eq!(0, tr.total());
            let mut n = 0;
            tr.iter(&mut |_, _| {
                n += 1;
                Ok(())
            })
            .unwrap();
            assert_eq!(0, n);
        }
        drop(res);
        drop(q);
        keep.push((db, snap));
    }
}

#[test]
fn test_tombstone_clean_result_empty_block_append_v2() {
    let num_samples = 10i64;

    let db = new_test_db(vec![]);

    let mut app = db.appender_v2(&ctx());
    let mut smpls = vec![0.0; num_samples as usize];
    for i in 0..num_samples {
        smpls[i as usize] = rand_f64();
        let _ = app_f(&mut *app, &ls(&["a", "b"]), i, smpls[i as usize]);
    }
    assert!(app.commit().is_none());
    // Interval should cover the whole block.
    let intervals = [Interval { mint: 0, maxt: num_samples }];

    // Create snapshot.
    let snap = TempDir::new();
    db.snapshot(snap.path(), true).unwrap();
    db.close().unwrap();

    // Reopen DB from snapshot.
    let db = new_test_db(vec![with_dir(snap.path())]);

    // Create tombstones by deleting all samples.
    for r in &intervals {
        db.delete(&ctx(), r.mint, r.maxt, &[meq("a", "b")]).unwrap();
    }

    db.clean_tombstones().unwrap();

    // After cleaning tombstones that covers the entire block, no blocks should be left behind.
    let actual_block_dirs = db::block_dirs(db.dir()).unwrap();
    assert!(actual_block_dirs.is_empty());
}
