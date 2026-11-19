//! Port of `tsdb/db_append_v2_test.go`, lines 1..4075.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use chunkenc::{ChunkIterator, ValueType};
use model::histogram::{FloatHistogram, Histogram, Span};
use model::labels::{self, Labels};
use model::matcher::{MatchType, Matcher};
use storage::{AppendV2Options, AppenderV2, Context, Sample, SeriesArc};
use tsdb::db::{self, DB};
use tsdb_format::record;
use tsdb_format::tombstones::Interval;
use tsdb_format::wlog;

use crate::common::{create_block, fs, gen_series, new_test_db, query, with_dir, with_opts, with_rngs, TempDir, TestDb};

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

// ---------------------------------------------------------------------------------------------------------------
// Helpers of the second group of tests.

fn leveled(db: &DB) -> &Arc<tsdb::compact::LeveledCompactor> {
    db.leveled_compactor.as_ref().expect("leveled compactor")
}

fn compactions_ran(db: &DB) -> i64 {
    leveled(db).metrics().ran.get() as i64
}

/// Go `expandSeriesSet` (db_test.go).
#[allow(clippy::type_complexity)]
fn expand_series_set(
    ss: &mut storage::BoxSeriesSet,
) -> (Vec<Labels>, BTreeMap<String, Vec<Sample>>, storage::Annotations, Option<storage::Error>) {
    let mut result_labels = Vec::new();
    let mut result_samples = BTreeMap::new();
    while ss.next() {
        let series = ss.at().clone();
        let mut samples = Vec::new();
        let mut it = series.iterator(None);
        while it.next() == ValueType::Float {
            let (t, v) = it.at();
            samples.push(fs(t, v));
        }
        result_labels.push(series.labels().clone());
        result_samples.insert(series.labels().to_string(), samples);
    }
    (result_labels, result_samples, ss.warnings(), ss.err())
}

/// Go `queryAndExpandChunks` (db_test.go): closes the querier.
fn query_and_expand_chunks(mut q: storage::BoxChunkQuerier, matchers: &[Matcher]) -> BTreeMap<String, Vec<Vec<Sample>>> {
    let mut res = BTreeMap::new();
    {
        let mut ss = q.select(&ctx(), false, None, matchers);
        while ss.next() {
            let series = ss.at().clone();
            let mut samples = Vec::new();
            let mut it = series.iterator(None);
            while it.next() {
                let chk = it.at().chunk.clone().expect("chunk");
                let mut cit = chk.iterator(None);
                samples.push(storage::expand_samples(&mut cit).unwrap());
            }
            assert!(it.err().is_none());
            res.insert(series.labels().to_string(), samples);
        }
        assert!(ss.err().is_none());
        assert!(ss.warnings().is_empty());
    }
    assert!(q.close().is_none());
    res
}

/// Go `testutil.DirHash`: a hash of the files (contents, sizes, names and modification times) under `path`.
fn dir_hash(path: &Path) -> [u8; 32] {
    fn walk(p: &Path, h: &mut crypto::Sha256) {
        let md = std::fs::metadata(p).unwrap();
        if md.is_dir() {
            let mut entries: Vec<_> = std::fs::read_dir(p).unwrap().map(|e| e.unwrap().path()).collect();
            entries.sort();
            for e in entries {
                walk(&e, h);
            }
            return;
        }
        h.update(&std::fs::read(p).unwrap());
        h.update(md.len().to_string().as_bytes());
        h.update(p.file_name().unwrap().to_string_lossy().as_bytes());
        let mt = md.modified().unwrap().duration_since(std::time::UNIX_EPOCH).unwrap();
        h.update(&mt.as_secs().to_le_bytes());
        h.update(&mt.subsec_nanos().to_le_bytes());
    }
    let mut h = crypto::Sha256::new();
    walk(path, &mut h);
    h.finish()
}

/// Go `deleteNonBlocks` (db_test.go).
fn delete_non_blocks(db_dir: &Path) -> Result<(), String> {
    let is_block = |e: &std::fs::DirEntry| {
        tsdb::compact::is_block_dir(&e.file_name().to_string_lossy(), e.file_type().map(|t| t.is_dir()).unwrap_or(false))
    };
    for e in std::fs::read_dir(db_dir).map_err(|e| e.to_string())? {
        let e = e.map_err(|e| e.to_string())?;
        if !is_block(&e) {
            let p = e.path();
            if p.is_dir() {
                std::fs::remove_dir_all(&p).map_err(|e| e.to_string())?;
            } else {
                std::fs::remove_file(&p).map_err(|e| e.to_string())?;
            }
        }
    }
    for e in std::fs::read_dir(db_dir).map_err(|e| e.to_string())? {
        let e = e.map_err(|e| e.to_string())?;
        if !is_block(&e) {
            return Err(format!("root folder:{} still hase non block directory:{}", db_dir.display(), e.file_name().to_string_lossy()));
        }
    }
    Ok(())
}

/// Go `createHead` (block_test.go): a head with the WAL and the given series appended (through a V1 appender).
fn create_head(w: wlog::WL, series: &[SeriesArc], chunk_dir: &Path) -> Arc<tsdb::head::Head> {
    let mut opts = tsdb::head::HeadOptions::default();
    opts.chunk_dir_root = chunk_dir.to_path_buf();
    let head = tsdb::head::Head::new(Some(Arc::new(w)), None, opts, None).unwrap();
    let mut app = head.appender(&ctx());
    for s in series {
        let mut r = 0;
        let lset = s.labels();
        let samples = expand(s).unwrap();
        let mut last_typ = samples.first().map(sample_type);
        for smp in &samples {
            let typ = sample_type(smp);
            if Some(typ) != last_typ {
                // The behaviour of appender is undefined if samples of different types are appended to the same
                // series in a single Commit().
                assert!(app.commit().is_none());
                app = head.appender(&ctx());
            }
            r = match typ {
                ValueType::Float => app.append(r, lset, smp.t, smp.f),
                ValueType::Histogram => app.append_histogram(r, lset, smp.t, smp.h.as_deref(), None),
                _ => app.append_histogram(r, lset, smp.t, None, smp.fh.as_deref()),
            }
            .unwrap();
            last_typ = Some(typ);
        }
    }
    assert!(app.commit().is_none());
    head
}

fn sample_type(s: &Sample) -> ValueType {
    if s.h.is_some() {
        ValueType::Histogram
    } else if s.fh.is_some() {
        ValueType::FloatHistogram
    } else {
        ValueType::Float
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Tests (second group).

#[test]
fn test_size_retention_append_v2() {
    for enable_st_storage in [false, true] {
        let mut opts = db::default_options();
        opts.out_of_order_time_window = 100;
        let db = new_test_db(vec![with_opts(opts), with_rngs(&[100])]);

        let blocks = [(100i64, 200i64), (200, 300), (300, 400), (400, 500), (500, 600)];
        for (mint, maxt) in blocks {
            create_block(db.dir(), &gen_series(100, 10, mint, maxt));
        }

        let head_blocks = [(700i64, 800i64)];

        // Add some data to the WAL.
        let mut head_app = db.head().appender_v2(&ctx());
        let mut a_series = labels::empty_labels();
        for (mint, maxt) in head_blocks {
            let series = gen_series(100, 10, mint, maxt + 1);
            for s in &series {
                a_series = s.labels().clone();
                let mut it = s.iterator(None);
                while it.next() == ValueType::Float {
                    let (tim, v) = it.at();
                    app_f(&mut *head_app, s.labels(), tim, v).unwrap();
                }
                assert!(it.err().is_none());
            }
        }
        assert!(head_app.commit().is_none());
        db.head().mmap_head_chunks();

        let start = std::time::Instant::now();
        while !db.head().chunk_disk_mapper.is_queue_empty() {
            assert!(start.elapsed() < std::time::Duration::from_secs(2), "queue not empty");
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        let wal = db.head().wal().unwrap().clone();
        let cdm = &db.head().chunk_disk_mapper;

        // Test that registered size matches the actual disk size.
        db.reload_blocks().unwrap(); // Reload the db to register the new db size.
        assert_eq!(blocks.len(), db.blocks().len()); // Ensure all blocks are registered.
        let mut block_size = db.metrics.blocks_bytes.get() as i64; // Use the actual internal metrics.
        let mut wal_size = wal.size().unwrap();
        let mut cdm_size = cdm.size().unwrap();
        assert_ne!(0, cdm_size);
        // Expected size should take into account block size + WAL size + Head chunks size.
        let mut exp_size = block_size + wal_size + cdm_size;
        let mut act_size = tsdb_format::fileutil::dir_size(db.dir()).unwrap();
        assert_eq!(exp_size, act_size, "registered size doesn't match actual disk size");

        // Create a WAL checkpoint, and compare sizes.
        let (first, last) = wlog::segments(wal.dir()).unwrap();
        wlog::checkpoint(&wal, first, last - 1, &|_| false, 0, enable_st_storage, false).unwrap();
        block_size = db.metrics.blocks_bytes.get() as i64;
        wal_size = wal.size().unwrap();
        cdm_size = cdm.size().unwrap();
        assert_ne!(0, cdm_size);
        exp_size = block_size + wal_size + cdm_size;
        act_size = tsdb_format::fileutil::dir_size(db.dir()).unwrap();
        assert_eq!(exp_size, act_size, "registered size doesn't match actual disk size");

        // Truncate Chunk Disk Mapper and compare sizes.
        cdm.truncate(900).unwrap();
        cdm_size = cdm.size().unwrap();
        assert_ne!(0, cdm_size);
        exp_size = block_size + wal_size + cdm_size;
        act_size = tsdb_format::fileutil::dir_size(db.dir()).unwrap();
        assert_eq!(exp_size, act_size, "registered size doesn't match actual disk size");

        // Add some out of order samples to check the size of WBL.
        let mut head_app = db.head().appender_v2(&ctx());
        for ts in 750..800i64 {
            app_f(&mut *head_app, &a_series, ts, ts as f64).unwrap();
        }
        assert!(head_app.commit().is_none());

        wal_size = wal.size().unwrap();
        let wbl_size = db.head().wbl().unwrap().size().unwrap();
        assert_ne!(0, wbl_size);
        cdm_size = cdm.size().unwrap();
        exp_size = block_size + wal_size + wbl_size + cdm_size;
        act_size = tsdb_format::fileutil::dir_size(db.dir()).unwrap();
        assert_eq!(exp_size, act_size, "registered size doesn't match actual disk size");

        // Decrease the max bytes limit so that a delete is triggered.
        // Check total size, total count and check that the oldest block was deleted.
        let first_block_size = tsdb::BlockReader::size(&db.blocks()[0]);
        let size_limit = act_size - first_block_size;
        db.opts.write().unwrap().max_bytes = size_limit; // Set the new db size limit one block smaller that the actual size.
        db.reload_blocks().unwrap(); // Reload the db to register the new db size.

        let exp_blocks = &blocks[1..];
        let act_blocks = db.blocks();
        block_size = db.metrics.blocks_bytes.get() as i64;
        wal_size = wal.size().unwrap();
        cdm_size = cdm.size().unwrap();
        assert_ne!(0, cdm_size);
        // Expected size should take into account block size + WAL size + WBL size.
        exp_size = block_size + wal_size + wbl_size + cdm_size;
        let act_retention_count = db.metrics.size_retention_count.get() as i64;
        act_size = tsdb_format::fileutil::dir_size(db.dir()).unwrap();

        assert_eq!(1, act_retention_count, "metric retention count mismatch");
        assert_eq!(exp_size, act_size, "metric db size doesn't match actual disk size");
        assert!(exp_size <= size_limit, "actual size ({exp_size}) is expected to be less than or equal to limit ({size_limit})");
        assert_eq!(blocks.len() - 1, act_blocks.len(), "new block count should be decreased from:{} to:{}", blocks.len(), blocks.len() - 1);
        assert_eq!(exp_blocks[0].1, act_blocks[0].max_time(), "maxT mismatch of the first block");
        assert_eq!(exp_blocks[exp_blocks.len() - 1].1, act_blocks[act_blocks.len() - 1].max_time(), "maxT mismatch of the last block");
    }
}

#[test]
fn test_not_matcher_selects_labels_unset_series_append_v2() {
    let db = new_test_db(vec![]);

    let labelpairs = vec![ls(&["a", "abcd", "b", "abcde"]), ls(&["labelname", "labelvalue"])];

    let mut app = db.appender_v2(&ctx());
    for lbls in &labelpairs {
        app_f(&mut *app, lbls, 0, 1.0).unwrap();
    }
    assert!(app.commit().is_none());

    let cases: Vec<(Vec<Matcher>, Vec<Labels>)> = vec![
        (vec![m(MatchType::NotEqual, "lname", "lvalue")], labelpairs.clone()),
        (vec![meq("a", "abcd"), m(MatchType::NotEqual, "b", "abcde")], vec![]),
        (vec![meq("a", "abcd"), m(MatchType::NotEqual, "b", "abc")], vec![labelpairs[0].clone()]),
        (vec![m(MatchType::NotRegexp, "a", "abd.*")], labelpairs.clone()),
        (vec![m(MatchType::NotRegexp, "a", "abc.*")], labelpairs[1..].to_vec()),
        (vec![m(MatchType::NotRegexp, "c", "abd.*")], labelpairs.clone()),
        (vec![m(MatchType::NotRegexp, "labelname", "labelvalue")], labelpairs[..1].to_vec()),
    ];

    let mut q = querier(&db, 0, 10);
    for (selector, series) in cases {
        let mut ss = q.select(&ctx(), false, None, &selector);
        let (lres, _, ws, err) = expand_series_set(&mut ss);
        assert!(err.is_none());
        assert!(ws.is_empty());
        assert_eq!(series, lres);
    }
    assert!(q.close().is_none());
}

#[test]
fn test_chunk_at_block_boundary_append_v2() {
    let db = new_test_db(vec![]);

    let mut app = db.appender_v2(&ctx());

    let block_range = leveled(&db).ranges()[0];
    let label = ls(&["foo", "bar"]);

    for i in 0..3i64 {
        app_f(&mut *app, &label, i * block_range, 0.0).unwrap();
        app_f(&mut *app, &label, i * block_range + 1000, 0.0).unwrap();
    }
    assert!(app.commit().is_none());

    db.compact(&ctx()).unwrap();

    let mut builder = labels::ScratchBuilder::new(0);
    for block in db.blocks() {
        let r = tsdb::BlockReader::index(&block).unwrap();
        let meta = tsdb::BlockReader::meta(&block);

        let (k, v) = tsdb_format::index::all_postings_key();
        let mut p = r.postings(&ctx(), k, &[v]).unwrap();

        let mut chks = Vec::new();
        let mut chunk_count = 0;
        while p.next() {
            r.series(p.at(), &mut builder, &mut chks).unwrap();
            for c in &chks {
                assert!(
                    meta.min_time <= c.min_time && c.max_time <= meta.max_time,
                    "chunk spans beyond block boundaries: [block.MinTime={}, block.MaxTime={}]; [chunk.MinTime={}, chunk.MaxTime={}]",
                    meta.min_time,
                    meta.max_time,
                    c.min_time,
                    c.max_time
                );
                chunk_count += 1;
            }
        }
        assert_eq!(1, chunk_count, "expected 1 chunk in block {}, got {}", meta.ulid, chunk_count);
        drop(p);
        r.close().unwrap();
    }
}

#[test]
fn test_querier_with_boundary_chunks_append_v2() {
    let db = new_test_db(vec![]);

    let mut app = db.appender_v2(&ctx());

    let block_range = leveled(&db).ranges()[0];
    let label = ls(&["foo", "bar"]);

    for i in 0..5i64 {
        app_f(&mut *app, &label, i * block_range, 0.0).unwrap();
        app_f(&mut *app, &ls(&["blockID", &i.to_string()]), i * block_range, 0.0).unwrap();
    }
    assert!(app.commit().is_none());

    db.compact(&ctx()).unwrap();

    assert!(db.blocks().len() >= 3, "invalid test, less than three blocks in DB");

    let mut q = querier(&db, block_range, 2 * block_range);
    // The requested interval covers 2 blocks, so the querier's label values for blockID should give us 2 values,
    // one from each block.
    let (b, ws, err) = q.label_values(&ctx(), "blockID", None, &[]);
    assert!(err.is_none());
    assert!(ws.is_empty());
    assert_eq!(vec!["1".to_string(), "2".to_string()], b);
    q.close();
}

#[test]
fn test_initialize_head_timestamp_append_v2() {
    // clean
    {
        let db = new_test_db(vec![]);

        // Should be set to init values if no WAL or blocks exist so far.
        assert_eq!(i64::MAX, db.head().min_time());
        assert_eq!(i64::MIN, db.head().max_time());
        assert!(!db.head().initialized());

        // First added sample initializes the writable range.
        let mut app = db.appender_v2(&ctx());
        app_f(&mut *app, &ls(&["a", "b"]), 1000, 1.0).unwrap();

        assert_eq!(1000, db.head().min_time());
        assert_eq!(1000, db.head().max_time());
        assert!(db.head().initialized());
    }

    let log_records = |dir: &Path, enable_st_storage: bool| {
        std::fs::create_dir_all(dir.join("wal")).unwrap();
        let w = wlog::WL::new(dir.join("wal"), wlog::compression::Type::None).unwrap();

        let enc = record::Encoder { enable_st_storage };
        let mut series = Vec::new();
        enc.series(
            &[
                record::RefSeries { r#ref: 123, labels: ls(&["a", "1"]) },
                record::RefSeries { r#ref: 124, labels: ls(&["a", "2"]) },
            ],
            &mut series,
        );
        let mut samples = Vec::new();
        enc.samples(
            &[
                record::RefSample { r#ref: 123, st: 0, t: 5000, v: 1.0 },
                record::RefSample { r#ref: 124, st: 0, t: 15000, v: 1.0 },
            ],
            &mut samples,
        );
        w.log_batch(&[series, samples]).unwrap();
        w.close().unwrap();
    };

    for enable_st_storage in [false, true] {
        // wal-only
        let dir = TempDir::new();
        log_records(dir.path(), enable_st_storage);

        let db = new_test_db(vec![with_dir(dir.path())]);

        assert_eq!(5000, db.head().min_time());
        assert_eq!(15000, db.head().max_time());
        assert!(db.head().initialized());
    }
    // existing-block
    {
        let dir = TempDir::new();

        create_block(dir.path(), &gen_series(1, 1, 1000, 2000));

        let db = new_test_db(vec![with_dir(dir.path())]);

        assert_eq!(2000, db.head().min_time());
        assert_eq!(2000, db.head().max_time());
        assert!(db.head().initialized());
    }
    for enable_st_storage in [false, true] {
        // existing-block-and-wal
        let dir = TempDir::new();

        create_block(dir.path(), &gen_series(1, 1, 1000, 6000));
        log_records(dir.path(), enable_st_storage);

        let db = new_test_db(vec![with_dir(dir.path())]);

        assert_eq!(6000, db.head().min_time());
        assert_eq!(15000, db.head().max_time());
        assert!(db.head().initialized());
        // Check that old series has been GCed.
        assert_eq!(1, db.head().num_series());
    }
}

#[test]
fn test_no_empty_blocks_append_v2() {
    let db = new_test_db(vec![with_rngs(&[100])]);

    db.disable_compactions();

    let range_to_trigger_compaction = leveled(&db).ranges()[0] / 2 * 3 - 1;
    let default_label = ls(&["foo", "bar"]);
    let default_matcher = m(MatchType::Regexp, "", ".*");

    // Test no blocks after compact with empty head.
    {
        db.compact(&ctx()).unwrap();
        let act_blocks = db::block_dirs(db.dir()).unwrap();
        assert_eq!(db.blocks().len(), act_blocks.len());
        assert!(act_blocks.is_empty());
        assert_eq!(0, compactions_ran(&db), "no compaction should be triggered here");
    }

    // Test no blocks after deleting all samples from head.
    {
        let mut app = db.appender_v2(&ctx());
        app_f(&mut *app, &default_label, 1, 0.0).unwrap();
        app_f(&mut *app, &default_label, 2, 0.0).unwrap();
        app_f(&mut *app, &default_label, 3 + range_to_trigger_compaction, 0.0).unwrap();
        assert!(app.commit().is_none());
        db.delete(&ctx(), i64::MIN, i64::MAX, &[default_matcher.clone()]).unwrap();
        db.compact(&ctx()).unwrap();
        assert_eq!(1, compactions_ran(&db), "compaction should have been triggered here");

        let act_blocks = db::block_dirs(db.dir()).unwrap();
        assert_eq!(db.blocks().len(), act_blocks.len());
        assert!(act_blocks.is_empty());

        let mut app = db.appender_v2(&ctx());
        let err = app_f(&mut *app, &default_label, 1, 0.0).unwrap_err();
        assert_eq!(storage::ERR_OUT_OF_BOUNDS, err, "the head should be truncated so no samples in the past should be allowed");

        // Adding new blocks.
        let current_time = db.head().max_time();
        app_f(&mut *app, &default_label, current_time, 0.0).unwrap();
        app_f(&mut *app, &default_label, current_time + 1, 0.0).unwrap();
        app_f(&mut *app, &default_label, current_time + range_to_trigger_compaction, 0.0).unwrap();
        assert!(app.commit().is_none());

        db.compact(&ctx()).unwrap();
        assert_eq!(2, compactions_ran(&db), "compaction should have been triggered here");
        let act_blocks = db::block_dirs(db.dir()).unwrap();
        assert_eq!(db.blocks().len(), act_blocks.len());
        assert_eq!(1, act_blocks.len(), "No blocks created when compacting with >0 samples");
    }

    // When no new block is created from head, and there are some blocks on disk compaction should not run into
    // infinite loop (was seen during development).
    {
        let old_blocks = db.blocks();
        let mut app = db.appender_v2(&ctx());
        let current_time = db.head().max_time();
        app_f(&mut *app, &default_label, current_time, 0.0).unwrap();
        app_f(&mut *app, &default_label, current_time + 1, 0.0).unwrap();
        app_f(&mut *app, &default_label, current_time + range_to_trigger_compaction, 0.0).unwrap();
        assert!(app.commit().is_none());
        db.head().delete(&ctx(), i64::MIN, i64::MAX, &[default_matcher.clone()]).unwrap();
        db.compact(&ctx()).unwrap();
        assert_eq!(3, compactions_ran(&db), "compaction should have been triggered here");
        let new_blocks = db.blocks();
        assert_eq!(old_blocks.len(), new_blocks.len());
        for (a, b) in old_blocks.iter().zip(&new_blocks) {
            assert!(a.ptr_eq(b));
        }
    }

    // Test no blocks remaining after deleting all samples from disk.
    {
        let current_time = db.head().max_time();
        let r0 = leveled(&db).ranges()[0];
        let blocks = [(current_time, current_time + r0), (current_time + 100, current_time + 100 + r0)];
        for (mint, maxt) in blocks {
            create_block(db.dir(), &gen_series(2, 2, mint, maxt));
        }

        let old_blocks = db.blocks();
        db.reload_blocks().unwrap(); // Reload the db to register the new blocks.
        assert_eq!(blocks.len() + old_blocks.len(), db.blocks().len()); // Ensure all blocks are registered.
        db.delete(&ctx(), i64::MIN, i64::MAX, &[default_matcher.clone()]).unwrap();
        db.compact(&ctx()).unwrap();
        assert_eq!(
            5,
            compactions_ran(&db),
            "compaction should have been triggered here once for each block that have tombstones"
        );

        let act_blocks = db::block_dirs(db.dir()).unwrap();
        assert_eq!(db.blocks().len(), act_blocks.len());
        assert_eq!(1, act_blocks.len(), "All samples are deleted. Only the most recent block should remain after compaction.");
    }
}

#[test]
fn test_db_label_names_append_v2() {
    struct Case {
        sample_labels1: Vec<[&'static str; 2]>,
        sample_labels2: Vec<[&'static str; 2]>,
        exp1: Vec<&'static str>,
        exp2: Vec<&'static str>,
    }
    let tests = vec![
        Case {
            sample_labels1: vec![["name1", "1"], ["name3", "3"], ["name2", "2"]],
            sample_labels2: vec![["name4", "4"], ["name1", "1"]],
            exp1: vec!["name1", "name2", "name3"],
            exp2: vec!["name1", "name2", "name3", "name4"],
        },
        Case {
            sample_labels1: vec![["name2", "2"], ["name1", "1"], ["name2", "2"]],
            sample_labels2: vec![["name6", "6"], ["name0", "0"]],
            exp1: vec!["name1", "name2"],
            exp2: vec!["name0", "name1", "name2", "name6"],
        },
    ];

    let block_range = 1000i64;
    // Appends samples into the database.
    let append_samples = |db: &DB, mint: i64, maxt: i64, sample_labels: &[[&str; 2]]| {
        let mut app = db.appender_v2(&ctx());
        for i in mint..=maxt {
            for tuple in sample_labels {
                let label = ls(&[tuple[0], tuple[1]]);
                app_f(&mut *app, &label, i * block_range, 0.0).unwrap();
            }
        }
        assert!(app.commit().is_none());
    };
    for tst in tests {
        let db = new_test_db(vec![]);

        append_samples(&db, 0, 4, &tst.sample_labels1);

        // Testing head.
        let head_indexr = db.head().index();
        let label_names = tsdb::IndexReader::label_names(&head_indexr, &ctx(), &[]).unwrap();
        assert_eq!(tst.exp1, label_names);
        tsdb::IndexReader::close(&head_indexr).unwrap();

        // Testing disk.
        db.compact(&ctx()).unwrap();
        // All blocks have same label names, hence check them individually.
        // No need to aggregate and check.
        for b in db.blocks() {
            let block_indexr = tsdb::BlockReader::index(&b).unwrap();
            let label_names = block_indexr.label_names(&ctx(), &[]).unwrap();
            assert_eq!(tst.exp1, label_names);
            block_indexr.close().unwrap();
        }

        // Adding more samples to head with new label names so that we can test (head+disk).LabelNames(ctx) (the
        // union).
        append_samples(&db, 5, 9, &tst.sample_labels2);

        // Testing DB (union).
        let mut q = querier(&db, i64::MIN, i64::MAX);
        let (label_names, ws, err) = q.label_names(&ctx(), None, &[]);
        assert!(err.is_none());
        assert!(ws.is_empty());
        assert!(q.close().is_none());
        assert_eq!(tst.exp2, label_names);
    }
}

#[test]
fn test_correct_num_tombstones_append_v2() {
    let db = new_test_db(vec![]);

    let block_range = leveled(&db).ranges()[0];
    let (name, value) = ("foo", "bar");
    let default_label = ls(&[name, value]);
    let default_matcher = meq(name, value);

    let mut app = db.appender_v2(&ctx());
    for i in 0..3i64 {
        for j in 0..15i64 {
            app_f(&mut *app, &default_label, i * block_range + j, 0.0).unwrap();
        }
    }
    assert!(app.commit().is_none());

    db.compact(&ctx()).unwrap();
    assert_eq!(1, db.blocks().len());

    let num_tombstones = |db: &DB| tsdb::BlockReader::meta(&db.blocks()[0]).stats.num_tombstones;

    db.delete(&ctx(), 0, 1, &[default_matcher.clone()]).unwrap();
    assert_eq!(1, num_tombstones(&db));

    // {0, 1} and {2, 3} are merged to form 1 tombstone.
    db.delete(&ctx(), 2, 3, &[default_matcher.clone()]).unwrap();
    assert_eq!(1, num_tombstones(&db));

    db.delete(&ctx(), 5, 6, &[default_matcher.clone()]).unwrap();
    assert_eq!(2, num_tombstones(&db));

    db.delete(&ctx(), 9, 11, &[default_matcher.clone()]).unwrap();
    assert_eq!(3, num_tombstones(&db));
}

#[test]
fn test_block_ranges_append_v2() {
    let dir = TempDir::new();

    // Test that the compactor doesn't create overlapping blocks when a non standard block already exists.
    let first_block_maxt = 3i64;
    create_block(dir.path(), &gen_series(1, 1, 0, first_block_maxt));
    let open = || {
        let (o, r) = db::validate_opts(Some(db::default_options()), vec![10000]).unwrap();
        db::open_with_ranges(dir.path(), None, o, r, None).unwrap()
    };
    let db = open();
    db.disable_compactions();

    let range_to_trigger_compaction = leveled(&db).ranges()[0] / 2 * 3 + 1;

    let mut app = db.appender_v2(&ctx());
    let lbl = ls(&["a", "b"]);
    assert!(
        app_f(&mut *app, &lbl, first_block_maxt - 1, rand_f64()).is_err(),
        "appending a sample with a timestamp covered by a previous block shouldn't be possible"
    );
    app_f(&mut *app, &lbl, first_block_maxt + 1, rand_f64()).unwrap();
    app_f(&mut *app, &lbl, first_block_maxt + 2, rand_f64()).unwrap();
    let second_block_maxt = first_block_maxt + range_to_trigger_compaction;
    app_f(&mut *app, &lbl, second_block_maxt, rand_f64()).unwrap(); // Add samples to trigger a new compaction.
    assert!(app.commit().is_none());
    db.compact(&ctx()).unwrap();
    let blocks = db.blocks();
    assert_eq!(2, blocks.len(), "no new block after compaction");

    assert!(
        blocks[1].min_time() >= blocks[0].max_time(),
        "new block overlaps  old:{:?},new:{:?}",
        tsdb::BlockReader::meta(&blocks[0]),
        tsdb::BlockReader::meta(&blocks[1])
    );

    // Test that wal records are skipped when an existing block covers the same time ranges and compaction doesn't
    // create an overlapping block.
    let mut app = db.appender_v2(&ctx());
    for k in 1..=4 {
        app_f(&mut *app, &lbl, second_block_maxt + k, rand_f64()).unwrap();
    }
    assert!(app.commit().is_none());
    drop(blocks);
    db.close().unwrap();
    drop(db);

    let third_block_maxt = second_block_maxt + 2;
    create_block(dir.path(), &gen_series(1, 1, second_block_maxt + 1, third_block_maxt));

    let db = open();
    db.disable_compactions();

    assert_eq!(3, db.blocks().len(), "db doesn't include expected number of blocks");
    assert_eq!(db.blocks()[2].max_time(), third_block_maxt, "unexpected maxt of the last block");

    let mut app = db.appender_v2(&ctx());
    app_f(&mut *app, &lbl, third_block_maxt + range_to_trigger_compaction, rand_f64()).unwrap(); // Trigger a compaction.
    assert!(app.commit().is_none());
    db.compact(&ctx()).unwrap();
    let blocks = db.blocks();
    assert_eq!(4, blocks.len(), "no new block after compaction");

    assert!(
        blocks[3].min_time() >= blocks[2].max_time(),
        "new block overlaps  old:{:?},new:{:?}",
        tsdb::BlockReader::meta(&blocks[2]),
        tsdb::BlockReader::meta(&blocks[3])
    );
    drop(blocks);
    db.close().unwrap();
}

#[test]
fn test_db_read_only_append_v2() {
    let db_dir = TempDir::new();
    let match_all = meq("", "");

    // Bootstrap the db.
    {
        // Create three 2-sample blocks.
        let db_blocks = [(10i64, 12i64), (12, 14), (14, 16)];
        for (mint, maxt) in db_blocks {
            create_block(db_dir.path(), &gen_series(1, 1, mint, maxt));
        }

        // Add head to test DBReadOnly WAL reading capabilities.
        let w = wlog::WL::new(db_dir.path().join("wal"), wlog::compression::Type::Snappy).unwrap();
        let h = create_head(w, &gen_series(1, 1, 16, 18), db_dir.path());
        h.close().unwrap();
    }

    // Open a normal db to use for a comparison.
    let (exp_blocks, exp_series, exp_chunks, exp_db_hash) = {
        let db_writable = new_test_db(vec![with_dir(db_dir.path())]);
        db_writable.disable_compactions();

        let db_size_before_append = tsdb_format::fileutil::dir_size(db_writable.dir()).unwrap();
        let mut app = db_writable.appender_v2(&ctx());
        app_f(&mut *app, &ls(&["foo", "bar"]), db_writable.head().max_time() + 1, 0.0).unwrap();
        assert!(app.commit().is_none());

        let exp_blocks: Vec<tsdb::BlockMeta> = db_writable.blocks().iter().map(tsdb::BlockReader::meta).collect();
        let exp_db_size = tsdb_format::fileutil::dir_size(db_writable.dir()).unwrap();
        assert!(exp_db_size > db_size_before_append, "db size didn't increase after an append");

        let exp_series = query(querier(&db_writable, i64::MIN, i64::MAX), &[match_all.clone()]);
        let cq = db_writable.chunk_querier(i64::MIN, i64::MAX).unwrap();
        let exp_chunks = query_and_expand_chunks(cq, &[match_all.clone()]);

        db_writable.close().unwrap(); // Close here to allow getting the dir hash for windows.
        let exp_db_hash = dir_hash(db_writable.dir());
        (exp_blocks, exp_series, exp_chunks, exp_db_hash)
    };
    let exp_block = exp_blocks[0].clone();

    // Open a read only db and ensure that the API returns the same result as the normal DB.
    let mut db_read_only = db::DbReadOnly::open(db_dir.path(), None, None).unwrap();

    // blocks
    {
        let blocks = db_read_only.blocks().unwrap();
        assert_eq!(exp_blocks.len(), blocks.len());
        for (i, exp_block) in exp_blocks.iter().enumerate() {
            assert_eq!(*exp_block, tsdb::BlockReader::meta(&blocks[i]), "block meta mismatch");
        }
    }
    // block
    {
        let block_id = exp_block.ulid.to_string();
        let block = db_read_only.block(&block_id, None).unwrap();
        assert_eq!(exp_block, tsdb::BlockReader::meta(&block), "block meta mismatch");
    }
    // invalid block ID
    {
        let block_id = "01GTDVZZF52NSWB5SXQF0P2PGF";
        assert!(db_read_only.block(block_id, None).is_err());
    }
    // last block ID
    {
        let block_id = db_read_only.last_block_id().unwrap();
        assert_eq!(exp_blocks[2].ulid.to_string(), block_id);
    }
    // querier
    {
        // Open a read only db and ensure that the API returns the same result as the normal DB.
        let q = db_read_only.querier(i64::MIN, i64::MAX).unwrap();
        let read_only_series = query(q, &[match_all.clone()]);
        let read_only_db_hash = dir_hash(db_dir.path());

        assert_eq!(exp_series.len(), read_only_series.len(), "total series mismatch");
        assert_eq!(exp_series, read_only_series, "series mismatch");
        assert_eq!(exp_db_hash, read_only_db_hash, "after all read operations the db hash should remain the same");
    }
    // chunk querier
    {
        let cq = db_read_only.chunk_querier(i64::MIN, i64::MAX).unwrap();
        let read_only_series = query_and_expand_chunks(cq, &[match_all.clone()]);
        let read_only_db_hash = dir_hash(db_dir.path());

        assert_eq!(exp_chunks.len(), read_only_series.len(), "total series mismatch");
        assert_eq!(exp_chunks, read_only_series, "series chunks mismatch");
        assert_eq!(exp_db_hash, read_only_db_hash, "after all read operations the db hash should remain the same");
    }
    db_read_only.close().unwrap();
}

#[test]
fn test_db_read_only_flush_wal_append_v2() {
    let db_dir = TempDir::new();
    let maxt = 1000i64;

    // Bootstrap the db.
    {
        // Append data to the WAL.
        let db = new_test_db(vec![with_dir(db_dir.path())]);
        db.disable_compactions();
        let mut app = db.appender_v2(&ctx());
        for i in 0..maxt {
            app_f(&mut *app, &ls(&[crate::common::DEFAULT_LABEL_NAME, "flush"]), i, 1.0).unwrap();
        }
        assert!(app.commit().is_none());
        db.close().unwrap();
    }

    // Flush WAL.
    let mut db = db::DbReadOnly::open(db_dir.path(), None, None).unwrap();

    let flush = TempDir::new();
    db.flush_wal(flush.path()).unwrap();
    db.close().unwrap();

    // Reopen the DB from the flushed WAL block.
    let mut db = db::DbReadOnly::open(flush.path(), None, None).unwrap();
    let blocks = db.blocks().unwrap();
    assert_eq!(1, blocks.len());

    let mut q = db.querier(0, maxt - 1).unwrap();
    {
        // Sum the values.
        let mut series_set = q.select(&ctx(), false, None, &[meq(crate::common::DEFAULT_LABEL_NAME, "flush")]);
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
        assert_eq!(1000.0, sum);
    }
    assert!(q.close().is_none());
    drop(blocks);
    db.close().unwrap();
}

#[test]
fn test_db_read_only_querier_no_alteration_append_v2() {
    let count_chunks = |dir: &Path| std::fs::read_dir(tsdb::head::mmapped_chunks_dir(dir)).unwrap().count();

    let spin_up_querier_and_check = |dir: &Path, sandbox_dir: &Path, chunks_count: usize| {
        let db_dir_hash = dir_hash(dir);
        // Bootstrap a RO db from the same dir and set up a querier.
        let mut db_read_only = db::DbReadOnly::open(dir, Some(sandbox_dir), None).unwrap();
        assert_eq!(chunks_count, count_chunks(dir));
        let mut q = db_read_only.querier(i64::MIN, i64::MAX).unwrap();
        assert!(q.close().is_none());
        drop(q);
        db_read_only.close().unwrap();
        // The RO Head doesn't alter RW db chunks_head/.
        assert_eq!(chunks_count, count_chunks(dir));
        assert_eq!(dir_hash(dir), db_dir_hash);
    };

    // doesn't cut chunks while replaying WAL
    {
        let db = new_test_db(vec![]);

        // Append until the first mmapped head chunk.
        for i in 0..121i64 {
            let mut app = db.appender_v2(&ctx());
            app_f(&mut *app, &ls(&["foo", "bar"]), i, 0.0).unwrap();
            assert!(app.commit().is_none());
        }

        let sandbox = TempDir::new();
        spin_up_querier_and_check(db.dir(), sandbox.path(), 0);

        // The RW Head should have no problem cutting its own chunk, this also proves that a chunk needed to be cut.
        db.force_head_mmap();
        assert_eq!(1, count_chunks(db.dir()));
    }

    // doesn't truncate corrupted chunks
    {
        let db = new_test_db(vec![]);
        db.close().unwrap();

        // Simulate a corrupted chunk: without a header.
        std::fs::File::create(tsdb::head::mmapped_chunks_dir(db.dir()).join("000001")).unwrap();

        let sandbox = TempDir::new();
        spin_up_querier_and_check(db.dir(), sandbox.path(), 1);

        // The RW Head should have no problem truncating its corrupted file: this proves that the chunk needed to
        // be truncated.
        let db = new_test_db(vec![with_dir(db.dir())]);

        assert_eq!(0, count_chunks(db.dir()));
    }
}

#[test]
fn test_db_cannot_see_partial_commits_append_v2() {
    let db = new_test_db(vec![]);

    let stop = std::sync::atomic::AtomicBool::new(false);
    let (first_tx, first_rx) = std::sync::mpsc::channel::<()>();

    let inconsistencies = std::thread::scope(|s| {
        // Insert data in batches.
        let dbr = &db;
        let stop = &stop;
        s.spawn(move || {
            let mut iter = 0i64;
            loop {
                let mut app = dbr.appender_v2(&ctx());
                for j in 0..100 {
                    app_f(&mut *app, &ls(&["foo", "bar", "a", &j.to_string()]), iter, iter as f64).unwrap();
                }
                assert!(app.commit().is_none());

                if iter == 0 {
                    first_tx.send(()).unwrap();
                }
                iter += 1;

                if stop.load(Ordering::SeqCst) {
                    return;
                }
            }
        });

        first_rx.recv().unwrap();

        // This is a race condition, so do a few tests to tickle it. Usually most will fail.
        let mut inconsistencies = 0;
        for _ in 0..10 {
            let mut q = querier(&db, 0, 1000000);
            {
                let mut ss = q.select(&ctx(), false, None, &[meq("foo", "bar")]);
                let (_, series_set, ws, err) = expand_series_set(&mut ss);
                assert!(err.is_none());
                assert!(ws.is_empty());

                let mut values = std::collections::HashSet::new();
                for series in series_set.values() {
                    values.insert(series[series.len() - 1].f.to_bits());
                }
                if values.len() != 1 {
                    inconsistencies += 1;
                }
            }
            q.close();
        }
        stop.store(true, Ordering::SeqCst);
        inconsistencies
    });

    assert_eq!(0, inconsistencies, "Some queries saw inconsistent results.");
}

#[test]
fn test_db_query_doesnt_see_appends_after_creation_append_v2() {
    let db = new_test_db(vec![]);
    let mut querier_before_add = querier(&db, 0, 1000000);

    let mut app = db.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["foo", "bar"]), 0, 0.0).unwrap();

    let mut querier_after_add_but_before_commit = querier(&db, 0, 1000000);

    let check = |q: &dyn storage::Querier, exp: BTreeMap<String, Vec<Sample>>| {
        let mut ss = q.select(&ctx(), false, None, &[meq("foo", "bar")]);
        let (_, series_set, ws, err) = expand_series_set(&mut ss);
        assert!(err.is_none());
        assert!(ws.is_empty());
        assert_eq!(exp, series_set);
    };

    // None of the queriers should return anything after the Add but before the commit.
    check(&*querier_before_add, BTreeMap::new());
    check(&*querier_after_add_but_before_commit, BTreeMap::new());

    // This commit is after the queriers are created, so should not be returned.
    assert!(app.commit().is_none());

    // Nothing returned for querier created before the Add.
    check(&*querier_before_add, BTreeMap::new());

    // Series exists but has no samples for querier created after Add.
    check(&*querier_after_add_but_before_commit, smap(vec![(r#"{foo="bar"}"#, vec![])]));

    let mut querier_after_commit = querier(&db, 0, 1000000);

    // Samples are returned for querier created after Commit.
    check(&*querier_after_commit, smap(vec![(r#"{foo="bar"}"#, vec![fs(0, 0.0)])]));

    querier_after_commit.close();
    querier_after_add_but_before_commit.close();
    querier_before_add.close();
}

#[test]
fn test_compact_head_append_v2() {
    // Open a DB and append data to the WAL.
    let opts = || {
        let mut o = db::zero_options();
        o.retention_duration = 24 * 15 * 3600 * 1000;
        o.no_lockfile = true;
        o.min_block_duration = 2 * 3600 * 1000;
        o.max_block_duration = 2 * 3600 * 1000;
        o.wal_compression = wlog::compression::Type::Snappy;
        o
    };
    let db = new_test_db(vec![with_opts(opts())]);
    let mut app = db.appender_v2(&ctx());
    let mut exp_samples = Vec::new();
    let maxt = 100i64;
    for i in 0..maxt {
        let val = rand_f64();
        app_f(&mut *app, &ls(&["a", "b"]), i, val).unwrap();
        exp_samples.push(fs(i, val));
    }
    assert!(app.commit().is_none());

    // Compact the Head to create a new block.
    db.compact_head(&tsdb::head::RangeHead::new(db.head().clone(), 0, maxt - 1)).unwrap();
    db.close().unwrap();

    // Delete everything but the new block and reopen the db to query it to ensure it includes the head data.
    delete_non_blocks(db.dir()).unwrap();
    let db = new_test_db(vec![with_dir(db.dir()), with_opts(opts())]);
    assert_eq!(1, db.blocks().len());
    assert_eq!(maxt, db.head().min_time());
    let mut q = querier(&db, 0, maxt - 1);
    let mut act_samples = Vec::new();
    {
        let mut series_set = q.select(&ctx(), false, None, &[meq("a", "b")]);
        while series_set.next() {
            let s = series_set.at().clone();
            let mut it = s.iterator(None);
            while it.next() == ValueType::Float {
                let (time, val) = it.at();
                act_samples.push(fs(time, val));
            }
            assert!(it.err().is_none());
        }
        assert_eq!(exp_samples, act_samples);
        assert!(series_set.err().is_none());
    }
    assert!(q.close().is_none());
}

#[test]
fn test_compact_head_with_deletion_append_v2() {
    let db = new_test_db(vec![]);

    let mut app = db.appender_v2(&ctx());
    app_f(&mut *app, &ls(&["a", "b"]), 10, rand_f64()).unwrap();
    assert!(app.commit().is_none());

    db.delete(&ctx(), 0, 100, &[meq("a", "b")]).unwrap();

    // This recreates the bug.
    db.compact_head(&tsdb::head::RangeHead::new(db.head().clone(), 0, 100)).unwrap();
    db.close().unwrap();
}

// ---------------------------------------------------------------------------------------------------------------
// Tests (third group).

#[test]
fn test_one_checkpoint_per_compact_call_append_v2() {
    let block_range = 1000i64;
    let opts = || {
        let mut o = db::zero_options();
        o.retention_duration = block_range * 1000;
        o.no_lockfile = true;
        o.min_block_duration = block_range;
        o.max_block_duration = block_range;
        o
    };

    let db = new_test_db(vec![with_opts(opts())]);
    db.disable_compactions();

    // Case 1: Lot's of uncompacted data in Head.

    let lbls = ls(&["foo_d", "choco_bar"]);
    // Append samples spanning 59 block ranges.
    let mut app = db.appender_v2(&ctx());
    let wal = db.head().wal().unwrap().clone();
    for i in 0..60i64 {
        app_f(&mut *app, &lbls, block_range * i, rand_f64()).unwrap();
        app_f(&mut *app, &lbls, block_range * i + block_range / 2, rand_f64()).unwrap();
        // Rotate the WAL file so that there is >3 files for checkpoint to happen.
        wal.next_segment().unwrap();
    }
    assert!(app.commit().is_none());

    // Check the existing WAL files.
    let (first, last) = wlog::segments(wal.dir()).unwrap();
    assert_eq!(0, first);
    assert_eq!(60, last);

    assert_eq!(0.0, db.head().metrics.checkpoint_creation_total.get());
    db.compact(&ctx()).unwrap();
    assert_eq!(1.0, db.head().metrics.checkpoint_creation_total.get());

    // As the data spans for 59 blocks, 58 go to disk and 1 remains in Head.
    assert_eq!(58, db.blocks().len());
    // Though WAL was truncated only once, head should be truncated after each compaction.
    assert_eq!(58.0, db.head().metrics.head_truncate_total.get());

    // The compaction should have only truncated first 2/3 of WAL (while also rotating the files).
    let (first, last) = wlog::segments(wal.dir()).unwrap();
    assert_eq!(40, first);
    assert_eq!(61, last);

    // The first checkpoint would be for first 2/3rd of WAL, hence till 39. That should be the last checkpoint.
    let (_, cno) = wlog::last_checkpoint(wal.dir()).unwrap();
    assert_eq!(39, cno);

    // Case 2: Old blocks on disk.
    // The above blocks will act as old blocks.

    // Creating a block to cover the data in the Head so that Head will skip the data during replay and start
    // fresh.
    let blocks = db.blocks();
    let new_block_mint = blocks[blocks.len() - 1].max_time();
    let new_block_maxt = db.head().max_time() + 1;
    drop(blocks);
    drop(wal);
    db.close().unwrap();

    create_block(db.dir(), &gen_series(1, 1, new_block_mint, new_block_maxt));

    let db = new_test_db(vec![with_dir(db.dir()), with_opts(opts())]);
    db.disable_compactions();
    let wal = db.head().wal().unwrap().clone();

    // 1 block more.
    assert_eq!(59, db.blocks().len());
    // No series in Head because of this new block.
    assert_eq!(0, db.head().num_series());

    // Adding sample way into the future.
    let mut app = db.appender_v2(&ctx());
    app_f(&mut *app, &lbls, block_range * 120, rand_f64()).unwrap();
    assert!(app.commit().is_none());

    // The mint of head is the last block maxt, that means the gap between mint and maxt of Head is too large.
    // This will trigger many compactions.
    assert_eq!(new_block_maxt, db.head().min_time());

    // Another WAL file was rotated.
    let (first, last) = wlog::segments(wal.dir()).unwrap();
    assert_eq!(40, first);
    assert_eq!(62, last);

    assert_eq!(0.0, db.head().metrics.checkpoint_creation_total.get());
    db.compact(&ctx()).unwrap();
    assert_eq!(1.0, db.head().metrics.checkpoint_creation_total.get());

    // No new blocks should be created as there was not data in between the new samples and the blocks.
    assert_eq!(59, db.blocks().len());

    // The compaction should have only truncated first 2/3 of WAL (while also rotating the files).
    let (first, last) = wlog::segments(wal.dir()).unwrap();
    assert_eq!(55, first);
    assert_eq!(63, last);

    // The first checkpoint would be for first 2/3rd of WAL, hence till 54. That should be the last checkpoint.
    let (_, cno) = wlog::last_checkpoint(wal.dir()).unwrap();
    assert_eq!(54, cno);
}

/// The common setup of the `TestQuerierShouldNotFailIfOOOCompactionOccurs*` tests: a DB with in-order samples
/// and a single OOO sample. Returns the DB, the number of samples written and the interval.
fn ooo_compaction_querier_setup() -> (TestDb, usize, i64) {
    let mut opts = db::default_options();
    opts.out_of_order_time_window = 3 * crate::common::DEFAULT_BLOCK_DURATION;
    let db = new_test_db(vec![with_opts(opts)]);

    // Disable compactions so we can control it.
    db.disable_compactions();

    let metric = ls(&["__name__", "test_metric"]);
    let interval = 15 * 1000i64;
    let mut ts = 0i64;
    let mut samples_written = 0;

    // Capture the first timestamp - this will be the timestamp of the OOO sample we'll append below.
    let ooo_ts = ts;
    ts += interval;

    // Push samples after the OOO sample we'll write below.
    while ts < 10 * interval {
        let mut app = db.appender_v2(&ctx());
        app_f(&mut *app, &metric, ts, ts as f64).unwrap();
        assert!(app.commit().is_none());
        samples_written += 1;
        ts += interval;
    }

    // Push a single OOO sample.
    let mut app = db.appender_v2(&ctx());
    app_f(&mut *app, &metric, ooo_ts, ts as f64).unwrap();
    assert!(app.commit().is_none());
    samples_written += 1;

    (db, samples_written, interval)
}

/// Starts `CompactOOOHead` in a thread. Returns the completion flag and the result receiver.
fn start_ooo_compaction(
    db: &TestDb,
) -> (Arc<std::sync::atomic::AtomicBool>, std::sync::mpsc::Receiver<Result<(), storage::Error>>) {
    let complete = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let dbc = db.db.clone();
    let c = complete.clone();
    std::thread::spawn(move || {
        let r = dbc.compact_ooo_head(&ctx());
        tx.send(r).unwrap();
        c.store(true, Ordering::SeqCst);
    });
    (complete, rx)
}

fn eventually(f: impl Fn() -> bool, timeout: std::time::Duration, tick: std::time::Duration, msg: &str) {
    let start = std::time::Instant::now();
    loop {
        if f() {
            return;
        }
        assert!(start.elapsed() < timeout, "{msg}");
        std::thread::sleep(tick);
    }
}

fn select_test_metric(q: &dyn storage::ChunkQuerier, interval: i64) -> storage::BoxChunkSeriesSet {
    let hints = storage::SelectHints { start: 0, end: i64::MAX, step: interval, ..Default::default() };
    q.select(&ctx(), true, Some(&hints), &[meq("__name__", "test_metric")])
}

/// Collects the series of the set (Go collects their chunk iterators); exactly one is expected.
fn collect_one_series(series_set: &mut storage::BoxChunkSeriesSet) -> storage::ChunkSeriesArc {
    let mut series = Vec::new();
    while series_set.next() {
        series.push(series_set.at().clone());
    }
    assert!(series_set.err().is_none());
    assert_eq!(1, series.len());
    series.pop().unwrap()
}

fn count_samples(it: &mut storage::BoxMetaIterator<'_>) -> usize {
    let mut samples_read = 0;
    while it.next() {
        samples_read += it.at().chunk.as_ref().unwrap().num_samples();
    }
    assert!(it.err().is_none());
    samples_read
}

#[test]
fn test_querier_should_not_fail_if_ooo_compaction_occurs_after_retrieving_querier_append_v2() {
    let (db, samples_written, interval) = ooo_compaction_querier_setup();

    // Get a querier.
    let mut querier_created_before_compaction = db.chunk_querier(0, i64::MAX).unwrap();

    // Start OOO head compaction.
    let (compaction_complete, compaction_err) = start_ooo_compaction(&db);

    // Wait until CompactOOOHead has written the OOO block, reloaded, and published lastGarbageCollectedMmapRef
    // before creating the second querier below.
    eventually(
        || {
            if compaction_complete.load(Ordering::SeqCst) {
                return true;
            }
            db.mtx.read().unwrap().last_garbage_collected_mmap_ref.0 != 0
        },
        std::time::Duration::from_secs(60),
        std::time::Duration::from_millis(10),
        "CompactOOOHead did not reach the reader-wait phase",
    );
    if compaction_complete.load(Ordering::SeqCst) {
        compaction_err.recv().unwrap().unwrap();
    }
    // The compaction must still be waiting for querierCreatedBeforeCompaction to be closed. If it does not wait,
    // then the query will return incorrect results or fail.
    assert!(
        !compaction_complete.load(Ordering::SeqCst),
        "compaction completed before reading chunks or closing querier created before compaction"
    );

    // Get another querier. This one should only use the compacted blocks from disk and ignore the chunks that will
    // be garbage collected.
    let mut querier_created_after_compaction = db.chunk_querier(0, i64::MAX).unwrap();

    let test_querier = |q: &dyn storage::ChunkQuerier| {
        // Query back the series.
        let mut series_set = select_test_metric(q, interval);
        let series = collect_one_series(&mut series_set);
        let mut iterator = series.iterator(None);
        // Check that we can still successfully read all samples.
        assert_eq!(samples_written, count_samples(&mut iterator));
    };

    test_querier(&*querier_created_before_compaction);

    assert!(
        !compaction_complete.load(Ordering::SeqCst),
        "compaction completed before closing querier created before compaction"
    );
    assert!(querier_created_before_compaction.close().is_none());
    eventually(
        || compaction_complete.load(Ordering::SeqCst),
        std::time::Duration::from_secs(60),
        std::time::Duration::from_millis(10),
        "compaction should complete after querier created before compaction was closed, and not wait for querier created after compaction",
    );
    compaction_err.recv().unwrap().unwrap();
    assert_eq!(1.0, db.head().metrics.chunks_removed.get());

    // Use the querier created after compaction and confirm it returns the expected results (ie. from the disk
    // block created from OOO head and in-order head) without error.
    test_querier(&*querier_created_after_compaction);
    assert!(querier_created_after_compaction.close().is_none());
}

#[test]
fn test_querier_should_not_fail_if_ooo_compaction_occurs_after_selecting_append_v2() {
    let (db, samples_written, interval) = ooo_compaction_querier_setup();

    // Get a querier.
    let mut querier = db.chunk_querier(0, i64::MAX).unwrap();

    // Query back the series.
    let mut series_set = select_test_metric(&*querier, interval);

    // Start OOO head compaction.
    let (compaction_complete, compaction_err) = start_ooo_compaction(&db);

    // Give CompactOOOHead time to start work.
    // If it does not wait for the querier to be closed, then the query will return incorrect results or fail.
    std::thread::sleep(std::time::Duration::from_secs(1));
    assert!(!compaction_complete.load(Ordering::SeqCst), "compaction completed before reading chunks or closing querier");

    // Collect the iterator for the series.
    let series = collect_one_series(&mut series_set);
    let mut iterator = series.iterator(None);

    // Check that we can still successfully read all samples.
    assert_eq!(samples_written, count_samples(&mut iterator));

    assert!(!compaction_complete.load(Ordering::SeqCst), "compaction completed before closing querier");
    assert!(querier.close().is_none());
    eventually(
        || compaction_complete.load(Ordering::SeqCst),
        std::time::Duration::from_secs(60),
        std::time::Duration::from_millis(10),
        "compaction should complete after querier was closed",
    );
    compaction_err.recv().unwrap().unwrap();
    assert_eq!(1.0, db.head().metrics.chunks_removed.get());
}

#[test]
fn test_querier_should_not_fail_if_ooo_compaction_occurs_after_retrieving_iterators_append_v2() {
    let (db, samples_written, interval) = ooo_compaction_querier_setup();

    // Get a querier.
    let mut querier = db.chunk_querier(0, i64::MAX).unwrap();

    // Query back the series.
    let mut series_set = select_test_metric(&*querier, interval);

    // Collect the iterator for the series.
    let series = collect_one_series(&mut series_set);
    let mut iterator = series.iterator(None);

    // Start OOO head compaction.
    let (compaction_complete, compaction_err) = start_ooo_compaction(&db);

    // Give CompactOOOHead time to start work.
    // If it does not wait for the querier to be closed, then the query will return incorrect results or fail.
    std::thread::sleep(std::time::Duration::from_secs(1));
    assert!(!compaction_complete.load(Ordering::SeqCst), "compaction completed before reading chunks or closing querier");

    // Check that we can still successfully read all samples.
    assert_eq!(samples_written, count_samples(&mut iterator));

    assert!(!compaction_complete.load(Ordering::SeqCst), "compaction completed before closing querier");
    assert!(querier.close().is_none());
    eventually(
        || compaction_complete.load(Ordering::SeqCst),
        std::time::Duration::from_secs(60),
        std::time::Duration::from_millis(10),
        "compaction should complete after querier was closed",
    );
    compaction_err.recv().unwrap().unwrap();
    assert_eq!(1.0, db.head().metrics.chunks_removed.get());
}

/// A WAL/WBL record (Go `[]any` of record slices).
#[derive(Debug, PartialEq)]
enum WalRec {
    Series(Vec<record::RefSeries>),
    Samples(Vec<record::RefSample>),
    Markers(Vec<record::RefMmapMarker>),
    Histograms(Vec<record::RefHistogramSample>),
    FloatHistograms(Vec<record::RefFloatHistogramSample>),
}

#[test]
fn test_ooowal_write_append_v2() {
    let minutes = |m: i64| m * 60 * 1000;
    use storage::tsdbutil as tu;

    let s1 = ls(&["l", "v1"]);
    let s2 = ls(&["l", "v2"]);

    // A sample of the scenario's type: (series ref, minutes) -> one-sample record content.
    #[derive(Clone, Copy)]
    enum Kind {
        Float,
        Hist(fn(i64) -> Histogram),
        FloatHist(fn(i64) -> FloatHistogram),
    }

    // Builds a record of the scenario's sample type from (ref, mins) pairs.
    let recs = |kind: Kind, ss: &[(u64, i64)]| -> WalRec {
        match kind {
            Kind::Float => WalRec::Samples(
                ss.iter().map(|&(r, mins)| record::RefSample { r#ref: r, st: 0, t: minutes(mins), v: mins as f64 }).collect(),
            ),
            Kind::Hist(g) => WalRec::Histograms(
                ss.iter()
                    .map(|&(r, mins)| record::RefHistogramSample { r#ref: r, st: 0, t: minutes(mins), h: Arc::new(g(mins)) })
                    .collect(),
            ),
            Kind::FloatHist(g) => WalRec::FloatHistograms(
                ss.iter()
                    .map(|&(r, mins)| record::RefFloatHistogramSample { r#ref: r, st: 0, t: minutes(mins), fh: Arc::new(g(mins)) })
                    .collect(),
            ),
        }
    };
    let marker = |r: u64, mmap_ref: u64| {
        WalRec::Markers(vec![record::RefMmapMarker { r#ref: r, mmap_ref: record::ChunkDiskMapperRef(mmap_ref) }])
    };

    // The MmapRef in this are not hand calculated, and instead taken from the test run. What is important here
    // is the order of records, and that MmapRef increases for each record.
    let scenarios: Vec<(&str, Kind, [u64; 3])> = vec![
        ("float", Kind::Float, [58, 107, 156]),
        ("integer histogram", Kind::Hist(tu::generate_test_histogram), [89, 172, 257]),
        ("float histogram", Kind::FloatHist(tu::generate_test_float_histogram), [177, 348, 521]),
        ("custom buckets histogram", Kind::Hist(tu::generate_test_custom_buckets_histogram), [82, 160, 239]),
        (
            "custom buckets float histogram",
            Kind::FloatHist(tu::generate_test_custom_buckets_float_histogram),
            [134, 263, 393],
        ),
    ];

    for (name, kind, mrefs) in scenarios {
        let expected_ooo_records = vec![
            marker(1, 0),
            recs(kind, &[(1, 40)]),
            marker(2, 0),
            recs(kind, &[(2, 42)]),
            recs(kind, &[(2, 45), (1, 35)]),
            marker(1, 0x100000000 + 8), // 3rd sample, hence m-mapped.
            recs(kind, &[(1, 36), (1, 37)]),
            marker(1, 0x100000000 + mrefs[0]), // 3rd sample, hence m-mapped.
            recs(kind, &[(1, 50)]),             // Does not contain the in-order sample here.
            // Single commit but multiple OOO records.
            marker(2, 0x100000000 + mrefs[1]),
            recs(kind, &[(2, 50), (2, 51)]),
            marker(2, 0x100000000 + mrefs[2]),
            recs(kind, &[(2, 52), (2, 53)]),
        ];
        let expected_ino_records = vec![
            WalRec::Series(vec![
                record::RefSeries { r#ref: 1, labels: s1.clone() },
                record::RefSeries { r#ref: 2, labels: s2.clone() },
            ]),
            recs(kind, &[(1, 60), (2, 60)]),
            recs(kind, &[(1, 40)]),
            recs(kind, &[(2, 42)]),
            recs(kind, &[(2, 45), (1, 35), (1, 36), (1, 37)]),
            recs(kind, &[(1, 50), (2, 65)]), // Contains both in-order and ooo sample.
            recs(kind, &[(2, 50), (2, 51), (2, 52), (2, 53)]),
        ];

        let append_sample = |app: &mut dyn AppenderV2, l: &Labels, mins: i64| -> u64 {
            match kind {
                Kind::Float => app.append(0, l, 0, minutes(mins), mins as f64, None, None, &ao()).unwrap(),
                Kind::Hist(g) => app.append(0, l, 0, minutes(mins), 0.0, Some(&g(mins)), None, &ao()).unwrap(),
                Kind::FloatHist(g) => app.append(0, l, 0, minutes(mins), 0.0, None, Some(&g(mins)), &ao()).unwrap(),
            }
        };
        ooo_wal_write(name, append_sample, expected_ooo_records, expected_ino_records);
    }
}

fn ooo_wal_write(
    name: &str,
    append_sample: impl Fn(&mut dyn AppenderV2, &Labels, i64) -> u64,
    expected_ooo_records: Vec<WalRec>,
    expected_ino_records: Vec<WalRec>,
) {
    let mut opts = db::default_options();
    opts.out_of_order_cap_max = 2;
    opts.out_of_order_time_window = 30 * 60 * 1000;
    let db = new_test_db(vec![with_opts(opts)]);

    let (s1, s2) = (ls(&["l", "v1"]), ls(&["l", "v2"]));

    // Ingest sample at 1h.
    let mut app = db.appender_v2(&ctx());
    append_sample(&mut *app, &s1, 60);
    append_sample(&mut *app, &s2, 60);
    assert!(app.commit().is_none());

    // OOO for s1.
    let mut app = db.appender_v2(&ctx());
    append_sample(&mut *app, &s1, 40);
    assert!(app.commit().is_none());

    // OOO for s2.
    let mut app = db.appender_v2(&ctx());
    append_sample(&mut *app, &s2, 42);
    assert!(app.commit().is_none());

    // OOO for both s1 and s2 in the same commit.
    let mut app = db.appender_v2(&ctx());
    append_sample(&mut *app, &s2, 45);
    append_sample(&mut *app, &s1, 35);
    append_sample(&mut *app, &s1, 36); // m-maps.
    append_sample(&mut *app, &s1, 37);
    assert!(app.commit().is_none());

    // OOO for s1 but not for s2 in the same commit.
    let mut app = db.appender_v2(&ctx());
    append_sample(&mut *app, &s1, 50); // m-maps.
    append_sample(&mut *app, &s2, 65);
    assert!(app.commit().is_none());

    // Single commit has 2 times m-mapping and more samples after m-map.
    let mut app = db.appender_v2(&ctx());
    append_sample(&mut *app, &s2, 50); // m-maps.
    append_sample(&mut *app, &s2, 51);
    append_sample(&mut *app, &s2, 52); // m-maps.
    append_sample(&mut *app, &s2, 53);
    assert!(app.commit().is_none());

    let get_records = |wal_dir: &Path| -> Vec<WalRec> {
        let sr = wlog::new_segments_reader(wal_dir).unwrap();
        let mut r = wlog::Reader::new(sr);
        let mut records = Vec::new();
        let mut dec = record::Decoder::new(tsdb::Logger::nop());
        while r.next() {
            let rec = r.record();
            match dec.r#type(rec) {
                record::Type::Series => {
                    let mut v = Vec::new();
                    dec.series(rec, &mut v).unwrap();
                    records.push(WalRec::Series(v));
                }
                record::Type::Samples | record::Type::SamplesV2 => {
                    let mut v = Vec::new();
                    dec.samples(rec, &mut v).unwrap();
                    records.push(WalRec::Samples(v));
                }
                record::Type::MmapMarkers => {
                    let mut v = Vec::new();
                    dec.mmap_markers(rec, &mut v).unwrap();
                    records.push(WalRec::Markers(v));
                }
                record::Type::HistogramSamples | record::Type::CustomBucketsHistogramSamples => {
                    let mut v = Vec::new();
                    dec.histogram_samples(rec, &mut v).unwrap();
                    records.push(WalRec::Histograms(v));
                }
                record::Type::FloatHistogramSamples | record::Type::CustomBucketsFloatHistogramSamples => {
                    let mut v = Vec::new();
                    dec.float_histogram_samples(rec, &mut v).unwrap();
                    records.push(WalRec::FloatHistograms(v));
                }
                typ => panic!("got a WAL record that is not series or samples: {typ:?}"),
            }
        }
        records
    };

    // The normal WAL.
    let act_recs = get_records(&db.dir().join("wal"));
    assert_eq!(expected_ino_records, act_recs, "{name}");

    // The WBL.
    let act_recs = get_records(&db.dir().join(wlog::WBL_DIR_NAME));
    assert_eq!(expected_ooo_records, act_recs, "{name}");
}

#[test]
fn test_db_panic_on_mmapping_head_chunk_append_v2() {
    let db = new_test_db(vec![]);
    db.disable_compactions();

    // Choosing scrape interval of 45s to have chunk larger than 1h.
    let itvl = 45 * 1000i64;

    let mut last_ts = 0i64;
    let mut add_samples = |db: &DB, num_samples: i64| {
        let mut app = db.appender_v2(&ctx());
        let mut r = 0;
        let lbls = ls(&["__name__", "testing", "foo", "bar"]);
        for i in 0..num_samples {
            r = app.append(r, &lbls, 0, last_ts, last_ts as f64, None, None, &ao()).unwrap();
            last_ts += itvl;
            if i % 10 == 0 {
                assert!(app.commit().is_none());
                app = db.appender_v2(&ctx());
            }
        }
        assert!(app.commit().is_none());
    };

    // Ingest samples up to 2h50m to make the head "about to compact".
    let num_samples = 170 * 60 * 1000 / itvl;
    add_samples(&db, num_samples);

    assert!(db.blocks().is_empty());
    db.compact(&ctx()).unwrap();
    assert!(db.blocks().is_empty());

    // Restarting.
    db.close().unwrap();

    let db = new_test_db(vec![with_dir(db.dir())]);
    db.disable_compactions();

    // Ingest samples up to 20m more to make the head compact.
    let num_samples = 20 * 60 * 1000 / itvl;
    add_samples(&db, num_samples);

    assert!(db.blocks().is_empty());
    db.compact(&ctx()).unwrap();
    assert_eq!(1, db.blocks().len());

    // More samples to m-map and panic.
    let num_samples = 120 * 60 * 1000 / itvl;
    add_samples(&db, num_samples);

    db.close().unwrap();
}
