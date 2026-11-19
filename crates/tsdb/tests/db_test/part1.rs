//! Port of `tsdb/db_test.go`, lines 1..3398.
//!
//! Rust notes:
//! - Go `newTestDB` closes and removes the DB at the end of the test; here the [`TestDb`] does it on drop, so a
//!   test that reopens a closed DB keeps the first value alive while it uses the directory.
//! - Go `prom_testutil.ToFloat64(metric)` is the metric's `get()`.

use std::collections::{BTreeMap, HashMap};
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};

use chunkenc::{ChunkIterator, ValueType};
use model::histogram::{FloatHistogram, Histogram, Span};
use model::labels::{self, Labels, ScratchBuilder};
use model::matcher::{MatchType, Matcher};
use storage::{
    Annotations, Appender, ChunkQuerier, Context, ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP, ERR_OUT_OF_BOUNDS, Error,
    Meta, Sample, SeriesSet,
};
use tsdb::block::{Block, open_block};
use tsdb::compact::Compactor;
use tsdb::db::{
    ApplyConfig, DB, DbReadOnly, ERR_CLOSED, Options, TimeRange, TsdbConfig, TsdbRetentionConfig, block_dirs,
    default_options, overlapping_blocks, validate_opts, zero_options,
};
use tsdb::head::{ERR_INVALID_SAMPLE, Head, HeadOptions, mmapped_chunks_dir};
use tsdb::meta::Ulid;
use tsdb::{BlockMeta, BlockReader, IndexReader};
use tsdb_format::chunks;
use tsdb_format::fileutil;
use tsdb_format::index::all_postings_key;
use tsdb_format::record::{self, Decoder, Encoder, RefSample, RefSeries};
use tsdb_format::tombstones::{Interval, MemTombstones};
use tsdb_format::wlog::{self, WL, compression};

use crate::common::{
    DEFAULT_LABEL_NAME, TempDir, TestDb, create_block, fs, gen_series, gen_series_from_sample_generator, new_test_db,
    query, rand_f64, with_dir, with_opts, with_rngs,
};

// ---------------------------------------------------------------------------------------------------------------
// Helpers.

fn ls(kv: &[&str]) -> Labels {
    labels::from_strings(kv)
}

fn m(t: MatchType, name: &str, value: &str) -> Matcher {
    Matcher::must_new(t, name, value)
}

fn eq(name: &str, value: &str) -> Matcher {
    m(MatchType::Equal, name, value)
}

fn ctx() -> Context {
    Context::background()
}

#[track_caller]
fn commit(app: &mut dyn Appender) {
    if let Some(e) = app.commit() {
        panic!("commit: {e}");
    }
}

/// Go `rand.Int63n(n)`.
fn rand_i63n(n: i64) -> i64 {
    let mut b = [0u8; 8];
    crypto::random_bytes(&mut b);
    ((u64::from_le_bytes(b) >> 1) % n as u64) as i64
}

/// Go `newTestDB(t)` with only options.
fn db_with(opts: Options) -> TestDb {
    new_test_db(vec![with_opts(opts)])
}

/// A new DB in `dir` (Go `newTestDB(t, withDir(dir))`).
fn db_in(dir: &Path) -> TestDb {
    new_test_db(vec![with_dir(dir)])
}

/// The first block range of the DB's leveled compactor (Go `db.compactor.(*LeveledCompactor).ranges[0]`).
fn block_range(db: &DB) -> i64 {
    db.leveled_compactor.as_ref().expect("leveled compactor").ranges()[0]
}

/// Go `db.compactor.(*LeveledCompactor).metrics.Ran`.
fn compactions_ran(db: &DB) -> i64 {
    db.leveled_compactor.as_ref().expect("leveled compactor").metrics().ran.get() as i64
}

/// Go `query` on a chunk querier (`queryChunks`): the chunk metas of each series by its label string.
fn query_chunks(q: Box<dyn ChunkQuerier>, matchers: &[Matcher]) -> BTreeMap<String, Vec<Meta>> {
    let mut q = q;
    let mut result = BTreeMap::new();
    {
        let mut ss = q.select(&ctx(), false, None, matchers);
        while ss.next() {
            let series = ss.at().clone();
            let mut chks = Vec::new();
            let mut it = series.iterator(None);
            while it.next() {
                chks.push(it.at().clone());
            }
            assert!(it.err().is_none(), "{:?}", it.err());
            if chks.is_empty() {
                continue;
            }
            result.insert(series.labels().to_string(), chks);
        }
        assert!(ss.err().is_none(), "{:?}", ss.err());
        assert!(ss.warnings().is_empty());
    }
    assert!(q.close().is_none());
    result
}

/// Go `queryAndExpandChunks`.
fn query_and_expand_chunks(q: Box<dyn ChunkQuerier>, matchers: &[Matcher]) -> BTreeMap<String, Vec<Vec<Sample>>> {
    let mut res = BTreeMap::new();
    for (k, v) in query_chunks(q, matchers) {
        let mut samples = Vec::new();
        for chk in v {
            let c = chk.chunk.as_ref().expect("chunk");
            let mut it = c.iterator(None);
            samples.push(storage::expand_samples(&mut it).unwrap());
        }
        res.insert(k, samples);
    }
    res
}

/// Go `expandSeriesSet`: the labels in the order of the set and the float samples by label string.
#[allow(clippy::type_complexity)]
fn expand_series_set(
    ss: &mut dyn SeriesSet,
) -> (Vec<Labels>, BTreeMap<String, Vec<(i64, f64)>>, Annotations, Option<Error>) {
    let mut result_labels = Vec::new();
    let mut result_samples = BTreeMap::new();
    while ss.next() {
        let series = ss.at().clone();
        let mut samples = Vec::new();
        let mut it = series.iterator(None);
        while it.next() == ValueType::Float {
            samples.push(it.at());
        }
        result_labels.push(series.labels().clone());
        result_samples.insert(series.labels().to_string(), samples);
    }
    (result_labels, result_samples, ss.warnings(), ss.err())
}

/// Go `boundedSamples` (head_test.go).
fn bounded_samples(full: &[Sample], mint: i64, maxt: i64) -> Vec<Sample> {
    let mut full = full;
    while let Some(first) = full.first() {
        if first.t >= mint {
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

/// Compares a series set against the expected series one by one, like the Go loops over `newMockSeriesSet` and
/// the result set (labels, then `storage.ExpandSamples` of both).
fn require_series_set_equal(res: &mut dyn SeriesSet, exp: &[(Labels, Vec<Sample>)], check_warnings: bool) {
    let mut i = 0;
    loop {
        let eok = i < exp.len();
        let rok = res.next();
        assert_eq!(eok, rok);
        if !eok {
            if check_warnings {
                assert!(res.warnings().is_empty());
            }
            break;
        }
        let sres = res.at().clone();
        assert_eq!(&exp[i].0, sres.labels());
        let smpl_exp = exp[i].1.clone();
        let mut it = sres.iterator(None);
        let smpl_res = storage::expand_samples(&mut it).unwrap();
        assert_eq!(smpl_exp, smpl_res);
        i += 1;
    }
}

/// Go `testutil.DirHash`: a sha256 over the content, size, name and modification time of every file.
fn dir_hash(dir: &Path) -> [u8; 32] {
    fn walk(p: &Path, h: &mut crypto::Sha256) {
        let md = std::fs::metadata(p).unwrap();
        if md.is_dir() {
            let mut entries: Vec<PathBuf> = std::fs::read_dir(p).unwrap().map(|e| e.unwrap().path()).collect();
            entries.sort();
            for e in entries {
                walk(&e, h);
            }
            return;
        }
        h.update(&std::fs::read(p).unwrap());
        h.update(md.len().to_string().as_bytes());
        h.update(p.file_name().unwrap().to_string_lossy().as_bytes());
        // adapted: Go hashes the gob encoding of the modification time; the nanoseconds since the epoch
        // identify it the same way.
        let mt = md.modified().unwrap().duration_since(std::time::UNIX_EPOCH).unwrap();
        h.update(&mt.as_nanos().to_le_bytes());
    }
    let mut h = crypto::Sha256::new();
    walk(dir, &mut h);
    h.finish()
}

/// Go `createHead` (block_test.go): a head with the WAL and the chunk directory, with the series appended.
fn create_head(w: Arc<WL>, series: &[storage::SeriesArc], chunk_dir: &Path) -> Arc<Head> {
    let opts = HeadOptions { chunk_dir_root: chunk_dir.to_path_buf(), ..HeadOptions::default() };
    let head = Head::new(Some(w), None, opts, None).unwrap();
    let mut app = head.appender(&ctx());
    for s in series {
        let mut r = 0;
        let lset = s.labels().clone();
        let mut it = s.iterator(None);
        let mut typ = it.next();
        let mut last_typ = typ;
        while typ != ValueType::None {
            if last_typ != typ {
                commit(app.as_mut());
                app = head.appender(&ctx());
            }
            r = match typ {
                ValueType::Float => {
                    let (t, v) = it.at();
                    app.append(r, &lset, t, v)
                }
                ValueType::Histogram => {
                    let (t, h) = it.at_histogram(None);
                    app.append_histogram(r, &lset, t, Some(&h), None)
                }
                ValueType::FloatHistogram => {
                    let (t, fh) = it.at_float_histogram(None);
                    app.append_histogram(r, &lset, t, None, Some(&fh))
                }
                ValueType::None => unreachable!(),
            }
            .unwrap();
            last_typ = typ;
            typ = it.next();
        }
        assert!(it.err().is_none());
    }
    commit(app.as_mut());
    head
}

fn meta(min_time: i64, max_time: i64) -> BlockMeta {
    BlockMeta { min_time, max_time, ..BlockMeta::default() }
}

fn time_range(min: i64, max: i64) -> TimeRange {
    TimeRange { min, max }
}

fn file_names(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> =
        std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect();
    v.sort();
    v
}

// ---------------------------------------------------------------------------------------------------------------
// Tests.

#[test]
fn test_db_close_after_close() {
    let db = new_test_db(vec![]);
    db.close().unwrap();
    db.close().unwrap();

    // Double check if we are closing correct DB after reuse.
    let db = new_test_db(vec![]);
    db.close().unwrap();
    db.close().unwrap();
}

// Ensure that blocks are held in memory in their time order and not in ULID order as they are read from the
// directory.
#[test]
fn test_db_reload_order() {
    let db = new_test_db(vec![]);

    let metas = [meta(90, 100), meta(70, 80), meta(100, 110)];
    for m in &metas {
        create_block(db.dir(), &gen_series(1, 1, m.min_time, m.max_time));
    }

    db.reload_blocks().unwrap();
    let blocks = db.blocks();
    assert_eq!(blocks.len(), 3);
    assert_eq!(metas[1].min_time, blocks[0].meta().min_time);
    assert_eq!(metas[1].max_time, blocks[0].meta().max_time);
    assert_eq!(metas[0].min_time, blocks[1].meta().min_time);
    assert_eq!(metas[0].max_time, blocks[1].meta().max_time);
    assert_eq!(metas[2].min_time, blocks[2].meta().min_time);
    assert_eq!(metas[2].max_time, blocks[2].meta().max_time);
}

#[test]
fn test_data_available_only_after_commit() {
    let db = new_test_db(vec![]);

    let mut app = db.appender(&ctx());
    app.append(0, &ls(&["foo", "bar"]), 0, 0.0).unwrap();

    let querier = db.querier(0, 1).unwrap();
    let series_set = query(querier, &[eq("foo", "bar")]);
    assert_eq!(BTreeMap::new(), series_set);

    commit(app.as_mut());

    let querier = db.querier(0, 1).unwrap();
    let series_set = query(querier, &[eq("foo", "bar")]);

    assert_eq!(BTreeMap::from([(r#"{foo="bar"}"#.to_string(), vec![fs(0, 0.0)])]), series_set);
}

// TestNoPanicAfterWALCorruption ensures that querying the db after a WAL corruption doesn't cause a panic.
// https://github.com/prometheus/prometheus/issues/7548
#[test]
fn test_no_panic_after_wal_corruption() {
    let db = db_with(Options { wal_segment_size: 32 * 1024, ..zero_options() });

    // Append until the first mmapped head chunk.
    // This is to ensure that all samples can be read from the mmapped chunks when the WAL is corrupted.
    let mut exp_samples = Vec::new();
    let mut maxt = 0i64;
    {
        // Appending 121 samples because on the 121st a new chunk will be created.
        for _ in 0..121 {
            let mut app = db.appender(&ctx());
            app.append(0, &ls(&["foo", "bar"]), maxt, 0.0).unwrap();
            exp_samples.push(fs(maxt, 0.0));
            commit(app.as_mut());
            maxt += 1;
        }
        db.close().unwrap();
    }

    // Corrupt the WAL after the first sample of the series so that it has at least one sample and it is not
    // garbage collected. The repair deletes all WAL records after the corrupted record and these are read from
    // the mmapped chunk.
    {
        let wal_dir = db.dir().join("wal");
        let wal_files = file_names(&wal_dir);
        let p = wal_dir.join(&wal_files[0]);
        let f = std::fs::OpenOptions::new().read(true).write(true).open(&p).unwrap();
        let mut r = wlog::Reader::new(BufReader::new(f.try_clone().unwrap()));
        assert!(r.next(), "reading the series record");
        assert!(r.next(), "reading the first sample record");
        // Write an invalid record header to corrupt everything after the first wal sample.
        use std::os::unix::fs::FileExt;
        f.write_at(&[99], r.offset() as u64).unwrap();
    }

    // Query the data.
    {
        let db = db_in(db.dir());
        assert_eq!(1.0, db.head().metrics.wal_corruptions_total.get(), "WAL corruption count mismatch");

        let querier = db.querier(0, maxt).unwrap();
        let series_set = query(querier, &[eq("", "")]);
        // The last sample should be missing as it was after the WAL segment corruption.
        assert_eq!(
            BTreeMap::from([(r#"{foo="bar"}"#.to_string(), exp_samples[..exp_samples.len() - 1].to_vec())]),
            series_set
        );
    }
}

#[test]
fn test_data_not_available_after_rollback() {
    let db = new_test_db(vec![]);

    let mut app = db.appender(&ctx());
    app.append(0, &ls(&["type", "float"]), 0, 0.0).unwrap();

    app.append_histogram(
        0,
        &ls(&["type", "histogram"]),
        0,
        Some(&Histogram { count: 42, sum: f64::NAN, ..Histogram::default() }),
        None,
    )
    .unwrap();

    app.append_histogram(
        0,
        &ls(&["type", "floathistogram"]),
        0,
        None,
        Some(&FloatHistogram { count: 42.0, sum: f64::NAN, ..FloatHistogram::default() }),
    )
    .unwrap();

    assert!(app.rollback().is_none());

    for typ in ["float", "histogram", "floathistogram"] {
        let querier = db.querier(0, 1).unwrap();
        let series_set = query(querier, &[eq("type", typ)]);
        assert_eq!(BTreeMap::new(), series_set);
    }

    let sr = wlog::new_segments_reader(db.head().wal().unwrap().dir()).unwrap();

    // Read records from WAL and check for expected count of series and samples.
    let mut r = wlog::Reader::new(sr);
    let mut dec = Decoder::new(record::Logger::nop());
    let (mut wal_series_count, mut wal_samples_count, mut wal_histogram_count) = (0, 0, 0);
    let (mut wal_float_histogram_count, mut wal_exemplars_count) = (0, 0);
    while r.next() {
        let rec = r.record().to_vec();
        match dec.r#type(&rec) {
            record::Type::Series => {
                let mut series = Vec::new();
                dec.series(&rec, &mut series).unwrap();
                wal_series_count += series.len();
            }
            record::Type::Samples | record::Type::SamplesV2 => {
                let mut samples = Vec::new();
                dec.samples(&rec, &mut samples).unwrap();
                wal_samples_count += samples.len();
            }
            record::Type::Exemplars => {
                let mut exemplars = Vec::new();
                dec.exemplars(&rec, &mut exemplars).unwrap();
                wal_exemplars_count += exemplars.len();
            }
            record::Type::HistogramSamples
            | record::Type::CustomBucketsHistogramSamples
            | record::Type::HistogramSamplesV2 => {
                let mut histograms = Vec::new();
                dec.histogram_samples(&rec, &mut histograms).unwrap();
                wal_histogram_count += histograms.len();
            }
            record::Type::FloatHistogramSamples
            | record::Type::CustomBucketsFloatHistogramSamples
            | record::Type::FloatHistogramSamplesV2 => {
                let mut float_histograms = Vec::new();
                dec.float_histogram_samples(&rec, &mut float_histograms).unwrap();
                wal_float_histogram_count += float_histograms.len();
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
fn test_db_appender_add_ref() {
    let db = new_test_db(vec![]);

    let mut app1 = db.appender(&ctx());

    let ref1 = app1.append(0, &ls(&["a", "b"]), 123, 0.0).unwrap();

    // Reference should already work before commit.
    let ref2 = app1.append(ref1, &Labels::empty(), 124, 1.0).unwrap();
    assert_eq!(ref1, ref2);

    commit(app1.as_mut());

    let mut app2 = db.appender(&ctx());

    // first ref should already work in next transaction.
    let ref3 = app2.append(ref1, &Labels::empty(), 125, 0.0).unwrap();
    assert_eq!(ref1, ref3);

    let ref4 = app2.append(ref1, &ls(&["a", "b"]), 133, 1.0).unwrap();
    assert_eq!(ref1, ref4);

    // Reference must be valid to add another sample.
    let ref5 = app2.append(ref2, &Labels::empty(), 143, 2.0).unwrap();
    assert_eq!(ref1, ref5);

    // Missing labels & invalid refs should fail.
    let err = app2.append(9999999, &Labels::empty(), 1, 1.0).unwrap_err();
    assert!(err.is(&ERR_INVALID_SAMPLE), "{err}");

    commit(app2.as_mut());

    let q = db.querier(0, 200).unwrap();
    let res = query(q, &[eq("a", "b")]);

    assert_eq!(
        BTreeMap::from([(
            ls(&["a", "b"]).to_string(),
            vec![fs(123, 0.0), fs(124, 1.0), fs(125, 0.0), fs(133, 1.0), fs(143, 2.0)]
        )]),
        res
    );
}

#[test]
fn test_append_empty_labels_ignored() {
    let db = new_test_db(vec![]);

    let mut app1 = db.appender(&ctx());

    let ref1 = app1.append(0, &ls(&["a", "b"]), 123, 0.0).unwrap();

    // Add with empty label.
    let ref2 = app1.append(0, &ls(&["a", "b", "c", ""]), 124, 0.0).unwrap();

    // Should be the same series.
    assert_eq!(ref1, ref2);

    commit(app1.as_mut());
}

#[test]
fn test_delete_simple() {
    const NUM_SAMPLES: i64 = 10;

    let cases: Vec<(Vec<Interval>, Vec<i64>)> = vec![
        (vec![Interval { mint: 0, maxt: 3 }], vec![4, 5, 6, 7, 8, 9]),
        (vec![Interval { mint: 1, maxt: 3 }], vec![0, 4, 5, 6, 7, 8, 9]),
        (vec![Interval { mint: 1, maxt: 3 }, Interval { mint: 4, maxt: 7 }], vec![0, 8, 9]),
        (vec![Interval { mint: 1, maxt: 3 }, Interval { mint: 4, maxt: 700 }], vec![0]),
        // This case is to ensure that labels and symbols are deleted.
        (vec![Interval { mint: 0, maxt: 9 }], vec![]),
    ];

    for (intervals, remaint) in cases {
        let db = new_test_db(vec![]);

        let mut app = db.appender(&ctx());

        let mut smpls = vec![0.0; NUM_SAMPLES as usize];
        for i in 0..NUM_SAMPLES {
            smpls[i as usize] = rand_f64();
            let _ = app.append(0, &ls(&["a", "b"]), i, smpls[i as usize]);
        }

        commit(app.as_mut());

        // Delete the ranges.
        for r in &intervals {
            db.delete(&ctx(), r.mint, r.maxt, &[eq("a", "b")]).unwrap();
        }

        // Compare the result.
        let q = db.querier(0, NUM_SAMPLES).unwrap();

        let mut res = q.select(&ctx(), false, None, &[eq("a", "b")]);

        let exp_samples: Vec<Sample> = remaint.iter().map(|&ts| fs(ts, smpls[ts as usize])).collect();
        // Go `newMockSeriesSet` of one list series.
        let exp = vec![(ls(&["a", "b"]), exp_samples)];
        require_series_set_equal(res.as_mut(), &exp, true);
    }
}

#[test]
fn test_amend_histogram_datapoint_causes_error() {
    let db = new_test_db(vec![]);

    let mut app = db.appender(&ctx());
    app.append(0, &ls(&["a", "b"]), 0, 0.0).unwrap();
    commit(app.as_mut());

    let mut app = db.appender(&ctx());
    app.append(0, &ls(&["a", "b"]), 0, 0.0).unwrap();
    let err = app.append(0, &ls(&["a", "b"]), 0, 1.0).unwrap_err();
    assert!(err.is(&ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP), "{err}");
    assert!(app.rollback().is_none());

    let mut h = Histogram {
        schema: 3,
        count: 52,
        sum: 2.7,
        zero_threshold: 0.1,
        zero_count: 42,
        positive_spans: vec![Span { offset: 0, length: 4 }, Span { offset: 10, length: 3 }],
        positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
        ..Histogram::default()
    };
    let mut fh = h.to_float();

    let mut app = db.appender(&ctx());
    app.append_histogram(0, &ls(&["a", "c"]), 0, Some(&h.clone()), None).unwrap();
    commit(app.as_mut());

    let mut app = db.appender(&ctx());
    app.append_histogram(0, &ls(&["a", "c"]), 0, Some(&h.clone()), None).unwrap();
    h.schema = 2;
    let err = app.append_histogram(0, &ls(&["a", "c"]), 0, Some(&h.clone()), None).unwrap_err();
    // Go require.Equal on the sentinel: the same error, not wrapped.
    assert!(err.is(&ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP), "{err}");
    assert_eq!(ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP.to_string(), err.to_string());
    assert!(app.rollback().is_none());

    // Float histogram.
    let mut app = db.appender(&ctx());
    app.append_histogram(0, &ls(&["a", "d"]), 0, None, Some(&fh.clone())).unwrap();
    commit(app.as_mut());

    let mut app = db.appender(&ctx());
    app.append_histogram(0, &ls(&["a", "d"]), 0, None, Some(&fh.clone())).unwrap();
    fh.schema = 2;
    let err = app.append_histogram(0, &ls(&["a", "d"]), 0, None, Some(&fh.clone())).unwrap_err();
    assert!(err.is(&ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP), "{err}");
    assert_eq!(ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP.to_string(), err.to_string());
    assert!(app.rollback().is_none());
}

#[test]
fn test_duplicate_na_n_datapoint_no_amend_error() {
    let db = new_test_db(vec![]);

    let mut app = db.appender(&ctx());
    app.append(0, &ls(&["a", "b"]), 0, f64::NAN).unwrap();
    commit(app.as_mut());

    let mut app = db.appender(&ctx());
    app.append(0, &ls(&["a", "b"]), 0, f64::NAN).unwrap();
}

#[test]
fn test_non_duplicate_na_n_datapoints_causes_amend_error() {
    let db = new_test_db(vec![]);

    let mut app = db.appender(&ctx());
    app.append(0, &ls(&["a", "b"]), 0, f64::from_bits(0x7ff0000000000001)).unwrap();
    commit(app.as_mut());

    let mut app = db.appender(&ctx());
    let err = app.append(0, &ls(&["a", "b"]), 0, f64::from_bits(0x7ff0000000000002)).unwrap_err();
    assert!(err.is(&ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP), "{err}");
}

#[test]
fn test_empty_labelset_causes_error() {
    let db = new_test_db(vec![]);

    let mut app = db.appender(&ctx());
    let err = app.append(0, &Labels::empty(), 0, 0.0).unwrap_err();
    assert_eq!("empty labelset: invalid sample", err.to_string());
}

#[test]
fn test_skipping_invalid_values_in_same_txn() {
    let db = new_test_db(vec![]);

    // Append AmendedValue.
    let mut app = db.appender(&ctx());
    app.append(0, &ls(&["a", "b"]), 0, 1.0).unwrap();
    app.append(0, &ls(&["a", "b"]), 0, 2.0).unwrap();
    commit(app.as_mut());

    // Make sure the right value is stored.
    let q = db.querier(0, 10).unwrap();
    let ss_map = query(q, &[eq("a", "b")]);
    assert_eq!(BTreeMap::from([(ls(&["a", "b"]).to_string(), vec![fs(0, 1.0)])]), ss_map);

    // Append Out of Order Value.
    let mut app = db.appender(&ctx());
    app.append(0, &ls(&["a", "b"]), 10, 3.0).unwrap();
    app.append(0, &ls(&["a", "b"]), 7, 5.0).unwrap();
    commit(app.as_mut());

    let q = db.querier(0, 10).unwrap();
    let ss_map = query(q, &[eq("a", "b")]);
    assert_eq!(BTreeMap::from([(ls(&["a", "b"]).to_string(), vec![fs(0, 1.0), fs(10, 3.0)])]), ss_map);
}

/// Sums the float values of the series that match `foo="bar"` (the loops of the snapshot tests).
fn sum_values(q: &dyn storage::Querier, matcher: Matcher) -> f64 {
    let mut series_set = q.select(&ctx(), false, None, &[matcher]);
    let mut sum = 0.0;
    while series_set.next() {
        let s = series_set.at().clone();
        let mut series = s.iterator(None);
        while series.next() == ValueType::Float {
            let (_, v) = series.at();
            sum += v;
        }
        assert!(series.err().is_none());
    }
    assert!(series_set.err().is_none());
    assert!(series_set.warnings().is_empty());
    sum
}

#[test]
fn test_db_snapshot() {
    let db = new_test_db(vec![]);

    // append data
    let mut app = db.appender(&ctx());
    let mint = 1414141414000i64;
    for i in 0..1000 {
        app.append(0, &ls(&["foo", "bar"]), mint + i, 1.0).unwrap();
    }
    commit(app.as_mut());

    // create snapshot
    let snap = TempDir::new();
    db.snapshot(snap.path(), true).unwrap();
    db.close().unwrap();

    // reopen DB from snapshot
    let db = db_in(snap.path());

    let mut querier = db.querier(mint, mint + 1000).unwrap();
    // sum values
    let sum = sum_values(querier.as_ref(), eq("foo", "bar"));
    assert!(querier.close().is_none());
    assert_eq!(1000.0, sum);
}

#[test]
fn test_db_snapshot_json_logger_block() {
    // adapted: Go logs JSON through promslog; the Rust logger writes one slog text line per entry
    // (`level=INFO msg="Snapshotting block" block=<ULID>`). The test checks the same: the block attribute of the
    // "Snapshotting block" entry is the block's ULID.
    let output = Arc::new(Mutex::new(Vec::<String>::new()));
    let out = output.clone();
    let logger = tsdb::Logger::new(move |line| out.lock().unwrap().push(line.to_string()));

    let db_dir = TempDir::new();
    let block_dir = create_block(db_dir.path(), &gen_series(1, 1, 0, 10));
    let db = tsdb::db::open(db_dir.path(), Some(logger), Some(default_options()), None).unwrap();
    db.disable_compactions();

    let snap = TempDir::new();
    db.snapshot(snap.path(), false).unwrap();

    let mut found = false;
    for line in output.lock().unwrap().iter() {
        let mut msg = None;
        let mut block = None;
        // Parse `key=value` pairs; quoted values are Rust debug strings.
        let mut rest = line.as_str();
        while !rest.is_empty() {
            let (k, r) = rest.split_once('=').expect("key=value");
            let (v, r) = if let Some(r) = r.strip_prefix('"') {
                let end = r.find('"').expect("closing quote");
                (&r[..end], r[end + 1..].trim_start())
            } else {
                match r.split_once(' ') {
                    Some((v, r)) => (v, r),
                    None => (r, ""),
                }
            };
            match k {
                "msg" => msg = Some(v.to_string()),
                "block" => block = Some(v.to_string()),
                _ => {}
            }
            rest = r;
        }
        if msg.as_deref() != Some("Snapshotting block") {
            continue;
        }
        assert_eq!(block_dir.file_name().unwrap().to_str().unwrap(), block.unwrap());
        found = true;
    }
    assert!(found, "snapshot block log not found");
    db.close().unwrap();
}

// TestDB_Snapshot_ChunksOutsideOfCompactedRange ensures that a snapshot removes chunks samples that are outside
// the set block time range. See https://github.com/prometheus/prometheus/issues/5105
#[test]
fn test_db_snapshot_chunks_outside_of_compacted_range() {
    let db = new_test_db(vec![]);

    let mut app = db.appender(&ctx());
    let mint = 1414141414000i64;
    for i in 0..1000 {
        app.append(0, &ls(&["foo", "bar"]), mint + i, 1.0).unwrap();
    }
    commit(app.as_mut());

    let snap = TempDir::new();

    // Hackingly introduce "race", by having lower max time then maxTime in last chunk.
    db.head().max_time.fetch_sub(10, Ordering::SeqCst);

    db.snapshot(snap.path(), true).unwrap();
    db.close().unwrap();

    // reopen DB from snapshot
    let db = db_in(snap.path());

    let mut querier = db.querier(mint, mint + 1000).unwrap();
    let sum = sum_values(querier.as_ref(), eq("foo", "bar"));
    assert!(querier.close().is_none());

    // Since we snapshotted with MaxTime - 10, so expect 10 less samples.
    assert_eq!(1000.0 - 10.0, sum);
}

#[test]
fn test_db_snapshot_with_delete() {
    const NUM_SAMPLES: i64 = 10;

    let db = new_test_db(vec![]);

    let mut app = db.appender(&ctx());

    let mut smpls = vec![0.0; NUM_SAMPLES as usize];
    for i in 0..NUM_SAMPLES {
        smpls[i as usize] = rand_f64();
        let _ = app.append(0, &ls(&["a", "b"]), i, smpls[i as usize]);
    }

    commit(app.as_mut());
    let cases: Vec<(Vec<Interval>, Vec<i64>)> =
        vec![(vec![Interval { mint: 1, maxt: 3 }, Interval { mint: 4, maxt: 7 }], vec![0, 8, 9])];

    for (intervals, remaint) in cases {
        // Delete the ranges.
        for r in &intervals {
            db.delete(&ctx(), r.mint, r.maxt, &[eq("a", "b")]).unwrap();
        }

        // create snapshot
        let snap = TempDir::new();

        db.snapshot(snap.path(), true).unwrap();

        // reopen DB from snapshot
        let db = db_in(snap.path());

        // Compare the result.
        let mut q = db.querier(0, NUM_SAMPLES).unwrap();
        {
            let mut res = q.select(&ctx(), false, None, &[eq("a", "b")]);

            let exp_samples: Vec<Sample> = remaint.iter().map(|&ts| fs(ts, smpls[ts as usize])).collect();

            if exp_samples.is_empty() {
                assert!(!res.next());
            } else {
                require_series_set_equal(res.as_mut(), &[(ls(&["a", "b"]), exp_samples)], true);
            }
        }
        assert!(q.close().is_none());
    }
}

#[test]
fn test_db_e2e() {
    const NUM_DATAPOINTS: usize = 1000;
    const NUM_RANGES: usize = 1000;
    const TIME_INTERVAL: i64 = 3;
    // Create 8 series with 1000 data-points of different ranges and run queries.
    let lbls: Vec<Labels> = vec![
        ls(&["a", "b", "instance", "localhost:9090", "job", "prometheus"]),
        ls(&["a", "b", "instance", "127.0.0.1:9090", "job", "prometheus"]),
        ls(&["a", "b", "instance", "127.0.0.1:9090", "job", "prom-k8s"]),
        ls(&["a", "b", "instance", "localhost:9090", "job", "prom-k8s"]),
        ls(&["a", "c", "instance", "localhost:9090", "job", "prometheus"]),
        ls(&["a", "c", "instance", "127.0.0.1:9090", "job", "prometheus"]),
        ls(&["a", "c", "instance", "127.0.0.1:9090", "job", "prom-k8s"]),
        ls(&["a", "c", "instance", "localhost:9090", "job", "prom-k8s"]),
    ];

    let mut series_map: HashMap<String, Vec<Sample>> = HashMap::new();
    for l in &lbls {
        series_map.insert(l.to_string(), Vec::new());
    }

    let db = new_test_db(vec![]);

    let mut app = db.appender(&ctx());

    for lset in &lbls {
        let mut series = Vec::new();

        let mut ts = rand_i63n(300);
        for _ in 0..NUM_DATAPOINTS {
            let v = rand_f64();

            series.push(fs(ts, v));

            app.append(0, lset, ts, v).unwrap();

            ts += rand_i63n(TIME_INTERVAL) + 1;
        }

        series_map.insert(lset.to_string(), series);
    }

    commit(app.as_mut());

    // Query each selector on 1000 random time-ranges.
    let queries: Vec<Vec<Matcher>> = vec![
        vec![eq("a", "b")],
        vec![eq("a", "b"), eq("job", "prom-k8s")],
        vec![eq("a", "c"), eq("instance", "localhost:9090"), eq("job", "prometheus")],
        // TODO: Add Regexp Matchers.
    ];

    for qry in &queries {
        let mut matched: Vec<Labels> = Vec::new();
        for l in &lbls {
            // Go `labels.Selector(qry.ms).Matches(ls)`.
            if qry.iter().all(|mm| mm.matches(l.get(&mm.name))) {
                matched.push(l.clone());
            }
        }

        matched.sort();

        for _ in 0..NUM_RANGES {
            let mint = rand_i63n(300);
            let maxt = mint + rand_i63n(TIME_INTERVAL * NUM_DATAPOINTS as i64);

            let mut expected = BTreeMap::new();

            // Build the mockSeriesSet.
            for mm in &matched {
                let smpls = bounded_samples(&series_map[&mm.to_string()], mint, maxt);
                if !smpls.is_empty() {
                    expected.insert(mm.to_string(), smpls);
                }
            }

            let mut q = db.querier(mint, maxt).unwrap();
            let mut result = BTreeMap::new();
            {
                let mut ss = q.select(&ctx(), false, None, qry);

                while ss.next() {
                    let x = ss.at().clone();
                    let mut it = x.iterator(None);
                    let smpls = storage::expand_samples(&mut it).unwrap();
                    if !smpls.is_empty() {
                        result.insert(x.labels().to_string(), smpls);
                    }
                }

                assert!(ss.err().is_none());
                assert!(ss.warnings().is_empty());
            }
            assert_eq!(expected, result);

            q.close();
        }
    }
}

#[test]
fn test_wal_flushed_on_db_close() {
    let db = new_test_db(vec![]);

    let lbls = ls(&["labelname", "labelvalue"]);

    let mut app = db.appender(&ctx());
    app.append(0, &lbls, 0, 1.0).unwrap();
    commit(app.as_mut());

    db.close().unwrap();

    let db2 = db_in(db.dir());

    let q = db2.querier(0, 1).unwrap();

    let (values, ws, err) = q.label_values(&ctx(), "labelname", None, &[]);
    assert!(err.is_none());
    assert!(ws.is_empty());
    assert_eq!(vec!["labelvalue".to_string()], values);
}

#[test]
fn test_wal_segment_size_options() {
    fn wal_files(db_dir: &Path) -> Vec<(String, u64)> {
        let mut files: Vec<(String, u64)> = Vec::new();
        for name in file_names(&db_dir.join("wal")) {
            let md = std::fs::metadata(db_dir.join("wal").join(&name)).unwrap();
            if !md.is_dir() {
                files.push((name, md.len()));
            }
        }
        files
    }
    let tests: Vec<(i64, Box<dyn Fn(&Path, i64)>)> = vec![
        // Default Wal Size.
        (
            0,
            Box::new(|db_dir: &Path, _: i64| {
                let files = wal_files(db_dir);
                // All the full segment files (all but the last) should match the segment size option.
                for (name, size) in &files[..files.len() - 1] {
                    assert_eq!(
                        default_options().wal_segment_size as u64,
                        *size,
                        "WAL file size doesn't match WALSegmentSize option, filename: {name}"
                    );
                }
                let (name, size) = &files[files.len() - 1];
                assert!(
                    (default_options().wal_segment_size as u64) > *size,
                    "last WAL file size is not smaller than the WALSegmentSize option, filename: {name}"
                );
            }),
        ),
        // Custom Wal Size.
        (
            2 * 32 * 1024,
            Box::new(|db_dir: &Path, segment_size: i64| {
                let files = wal_files(db_dir);
                assert!(!files.is_empty(), "current WALSegmentSize should result in more than a single WAL file.");
                // All the full segment files (all but the last) should match the segment size option.
                for (name, size) in &files[..files.len() - 1] {
                    assert_eq!(
                        segment_size as u64, *size,
                        "WAL file size doesn't match WALSegmentSize option, filename: {name}"
                    );
                }
                let (name, size) = &files[files.len() - 1];
                assert!(
                    (segment_size as u64) > *size,
                    "last WAL file size is not smaller than the WALSegmentSize option, filename: {name}"
                );
            }),
        ),
        // Wal disabled.
        (
            -1,
            Box::new(|db_dir: &Path, _: i64| {
                // Check that WAL dir is not there.
                assert!(std::fs::metadata(db_dir.join("wal")).is_err());
                // Check that there is chunks dir.
                std::fs::metadata(mmapped_chunks_dir(db_dir)).unwrap();
            }),
        ),
    ];
    for (segment_size, test_func) in tests {
        // t.Run(fmt.Sprintf("WALSegmentSize %d test", segmentSize))
        let mut opts = default_options();
        opts.wal_segment_size = segment_size;
        let db = db_with(opts);

        for i in 0..155i64 {
            let mut app = db.appender(&ctx());
            let r = app.append(0, &ls(&[&format!("wal{i}"), "size"]), i, rand_f64()).unwrap();
            for j in 1..=78i64 {
                app.append(r, &Labels::empty(), i + j, rand_f64()).unwrap();
            }
            commit(app.as_mut());
        }

        db.close().unwrap();
        test_func(db.dir(), segment_size);
    }
}

// https://github.com/prometheus/prometheus/issues/9846
// https://github.com/prometheus/prometheus/issues/9859
#[test]
fn test_wal_replay_race_on_samples_logged_before_series() {
    const NUM_RUNS: usize = 1;
    const NUM_SAMPLES_BEFORE_SERIES_CREATION: usize = 1000;

    // We test both with few and many samples appended after series creation. If samples are < 120 then there's no
    // mmap-ed chunk, otherwise there's at least 1 mmap-ed chunk when replaying the WAL.
    for enable_st_storage in [false, true] {
        for num_samples_after_series_creation in [1, 1000] {
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

    let mut buf = Vec::new();
    for series_ref in 1..=NUM_SERIES {
        // Log samples before the series is logged to the WAL.
        let enc = Encoder { enable_st_storage };
        let mut samples = Vec::new();

        for ts in 0..num_samples_before_series_creation {
            samples.push(RefSample { r#ref: series_ref as u64, st: 0, t: ts as i64, v: ts as f64 });
        }

        buf.clear();
        enc.samples(&samples, &mut buf);
        db.head().wal().unwrap().log(&buf).unwrap();

        // Add samples via appender so that they're logged after the series in the WAL.
        let mut app = db.appender(&ctx());
        let lbls = ls(&["series_id", &series_ref.to_string()]);

        for ts in num_samples_before_series_creation..num_samples_before_series_creation + num_samples_after_series_creation
        {
            app.append(0, &lbls, ts as i64, ts as f64).unwrap();
        }
        commit(app.as_mut());
    }

    db.close().unwrap();

    // Reopen the DB, replaying the WAL.
    let db2 = db_in(db.dir());

    // Query back chunks for all series.
    let mut q = db2.chunk_querier(i64::MIN, i64::MAX).unwrap();
    {
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
            assert_eq!(
                (num_samples_after_series_creation / 120) + 1,
                actual_chunks,
                "series: {}",
                s.labels()
            );
        }

        assert!(set.err().is_none());
        assert_eq!(NUM_SERIES, actual_series);
    }
    q.close();
}

#[test]
fn test_tombstone_clean() {
    const NUM_SAMPLES: i64 = 10;

    let db = new_test_db(vec![]);

    let mut app = db.appender(&ctx());

    let mut smpls = vec![0.0; NUM_SAMPLES as usize];
    for i in 0..NUM_SAMPLES {
        smpls[i as usize] = rand_f64();
        let _ = app.append(0, &ls(&["a", "b"]), i, smpls[i as usize]);
    }

    commit(app.as_mut());
    let cases: Vec<(Vec<Interval>, Vec<i64>)> =
        vec![(vec![Interval { mint: 1, maxt: 3 }, Interval { mint: 4, maxt: 7 }], vec![0, 8, 9])];

    for (intervals, remaint) in cases {
        // Delete the ranges.

        // Create snapshot.
        let snap = TempDir::new();
        db.snapshot(snap.path(), true).unwrap();
        db.close().unwrap();

        // Reopen DB from snapshot.
        let db = db_in(snap.path());

        for r in &intervals {
            db.delete(&ctx(), r.mint, r.maxt, &[eq("a", "b")]).unwrap();
        }

        // All of the setup for THIS line.
        db.clean_tombstones().unwrap();

        // Compare the result.
        let mut q = db.querier(0, NUM_SAMPLES).unwrap();
        {
            let mut res = q.select(&ctx(), false, None, &[eq("a", "b")]);

            let exp_samples: Vec<Sample> = remaint.iter().map(|&ts| fs(ts, smpls[ts as usize])).collect();

            if exp_samples.is_empty() {
                assert!(!res.next());
                continue;
            }

            require_series_set_equal(res.as_mut(), &[(ls(&["a", "b"]), exp_samples)], false);
            assert!(res.warnings().is_empty());
        }
        q.close();

        for b in db.blocks() {
            // Go: require.Equal(t, tombstones.NewMemTombstones(), b.tombstones).
            let t = b.tombstones().unwrap();
            assert_eq!(0, t.total());
            let mut n = 0;
            t.iter(&mut |_, _| {
                n += 1;
                Ok(())
            })
            .unwrap();
            assert_eq!(0, n);
        }
    }
}

// TestTombstoneCleanResultEmptyBlock tests that a TombstoneClean that results in empty blocks (no timeseries)
// will also delete the resultant block.
#[test]
fn test_tombstone_clean_result_empty_block() {
    let num_samples = 10i64;

    let db = new_test_db(vec![]);

    let mut app = db.appender(&ctx());

    let mut smpls = vec![0.0; num_samples as usize];
    for i in 0..num_samples {
        smpls[i as usize] = rand_f64();
        let _ = app.append(0, &ls(&["a", "b"]), i, smpls[i as usize]);
    }

    commit(app.as_mut());
    // Interval should cover the whole block.
    let intervals = [Interval { mint: 0, maxt: num_samples }];

    // Create snapshot.
    let snap = TempDir::new();
    db.snapshot(snap.path(), true).unwrap();
    db.close().unwrap();

    // Reopen DB from snapshot.
    let db = db_in(snap.path());

    // Create tombstones by deleting all samples.
    for r in &intervals {
        db.delete(&ctx(), r.mint, r.maxt, &[eq("a", "b")]).unwrap();
    }

    db.clean_tombstones().unwrap();

    // After cleaning tombstones that covers the entire block, no blocks should be left behind.
    let actual_block_dirs = block_dirs(db.dir()).unwrap();
    assert!(actual_block_dirs.is_empty());
}

// TestTombstoneCleanFail tests that a failing TombstoneClean doesn't leave any blocks behind. When
// TombstoneClean errors the original block that should be rebuilt doesn't get deleted so if TombstoneClean
// leaves any blocks behind these will overlap.
#[test]
fn test_tombstone_clean_fail() {
    let db = new_test_db(vec![]);

    let mut old_block_dirs = Vec::new();

    // Create some blocks pending for compaction.
    // totalBlocks should be >=2 so we have enough blocks to trigger compaction failure.
    let total_blocks = 2;
    for i in 0..total_blocks as i64 {
        let block_dir = create_block(db.dir(), &gen_series(1, 1, i, i + 1));
        let block = open_block(None, &block_dir, None, None).unwrap();
        // Add some fake tombstones to trigger the compaction.
        let tomb = MemTombstones::new();
        tomb.add_interval(0, &[Interval { mint: i, maxt: i + 1 }]);
        block.set_tombstones(Arc::new(tomb));

        db.mtx.write().unwrap().blocks.push(block);
        old_block_dirs.push(block_dir);
    }

    // Initialize the mockCompactorFailing with a room for a single compaction iteration.
    // mockCompactorFailing will fail on the second iteration so we can check if the cleanup works as expected.
    let blocks: Vec<Ulid> = db.blocks().iter().map(|b| b.meta().ulid).collect();
    *db.compactor.write().unwrap() =
        Arc::new(MockCompactorFailing { blocks: Mutex::new(blocks), max: total_blocks + 1 });

    // The compactor should trigger a failure here.
    assert!(db.clean_tombstones().is_err());

    // Now check that the CleanTombstones replaced the old block even after a failure.
    let actual_block_dirs = block_dirs(db.dir()).unwrap();
    // Only one block should have been replaced by a new block.
    assert_eq!(actual_block_dirs.len(), old_block_dirs.len());
    assert_eq!(intersection(&old_block_dirs, &actual_block_dirs).len(), actual_block_dirs.len() - 1);
}

fn intersection(old_blocks: &[PathBuf], actual_blocks: &[PathBuf]) -> Vec<PathBuf> {
    let hash: std::collections::HashSet<&PathBuf> = old_blocks.iter().collect();
    actual_blocks.iter().filter(|e| hash.contains(e)).cloned().collect()
}

/// Go `mockCompactorFailing`: creates a new empty block on every write and fails when reached the max allowed
/// total. (Go `CompactOOO` always fails; the Rust compactor has no `CompactOOO`.)
struct MockCompactorFailing {
    blocks: Mutex<Vec<Ulid>>,
    max: usize,
}

impl Compactor for MockCompactorFailing {
    fn plan(&self, _: &Path) -> Result<Vec<PathBuf>, Error> {
        Ok(Vec::new())
    }

    fn write(&self, dest: &Path, _: &dyn BlockReader, _: i64, _: i64, _: Option<&BlockMeta>) -> Result<Vec<Ulid>, Error> {
        let mut blocks = self.blocks.lock().unwrap();
        if blocks.len() >= self.max {
            return Err(Error::msg("the compactor already did the maximum allowed blocks so it is time to fail"));
        }

        let block = open_block(None, &create_block(dest, &gen_series(1, 1, 0, 1)), None, None).unwrap();
        block.close().unwrap(); // Close block as we won't be using anywhere.
        let id = block.meta().ulid;
        blocks.push(id);

        // Now check that all expected blocks are actually persisted on disk.
        // This way we make sure that we have some blocks that are supposed to be removed.
        let expected_blocks: Vec<PathBuf> = blocks.iter().map(|b| dest.join(b.to_string())).collect();
        let actual_block_dirs = block_dirs(dest).unwrap();

        assert_eq!(expected_blocks, actual_block_dirs);

        Ok(vec![id])
    }

    fn compact(&self, _: &Path, _: &[PathBuf], _: &[Block]) -> Result<Vec<Ulid>, Error> {
        Ok(Vec::new())
    }
}

#[test]
fn test_time_retention() {
    struct Case {
        name: &'static str,
        blocks: Vec<BlockMeta>,
        exp_blocks: Vec<BlockMeta>,
        retention_duration: i64,
    }
    let test_cases = vec![
        Case {
            name: "Block max time delta greater than retention duration",
            blocks: vec![
                meta(500, 900), // Oldest block, beyond retention
                meta(1000, 1500),
                meta(1500, 2000), // Newest block
            ],
            exp_blocks: vec![meta(1000, 1500), meta(1500, 2000)],
            retention_duration: 1000,
        },
        Case {
            name: "Block max time delta equal to retention duration",
            blocks: vec![
                meta(500, 900),   // Oldest block
                meta(1000, 1500), // Coinciding exactly with the retention duration.
                meta(1500, 2000), // Newest block
            ],
            exp_blocks: vec![meta(1500, 2000)],
            retention_duration: 500,
        },
    ];
    for tc in test_cases {
        let db = new_test_db(vec![with_rngs(&[1000])]);

        for m in &tc.blocks {
            create_block(db.dir(), &gen_series(10, 10, m.min_time, m.max_time));
        }

        db.reload_blocks().unwrap(); // Reload the db to register the new blocks.
        assert_eq!(db.blocks().len(), tc.blocks.len(), "{}", tc.name); // Ensure all blocks are registered.

        db.opts.write().unwrap().retention_duration = tc.retention_duration;
        // Reloading should truncate the blocks which are >= the retention duration vs the first block.
        db.reload_blocks().unwrap();

        let act_blocks = db.blocks();

        assert_eq!(1, db.metrics.time_retention_count.get() as i64, "metric retention count mismatch");
        assert_eq!(act_blocks.len(), tc.exp_blocks.len(), "{}", tc.name);
        for (i, eb) in tc.exp_blocks.iter().enumerate() {
            assert_eq!(eb.min_time, act_blocks[i].meta().min_time);
            assert_eq!(eb.max_time, act_blocks[i].meta().max_time);
        }
    }
}

#[test]
fn test_retention_duration_metric() {
    let db = new_test_db(vec![with_opts(Options { retention_duration: 1000, ..zero_options() }), with_rngs(&[100])]);

    let exp_retention_duration = 1.0;
    let act_retention_duration = db.metrics.retention_duration.get();
    assert_eq!(exp_retention_duration, act_retention_duration, "metric retention duration mismatch");
}

#[test]
fn test_size_retention() {
    for enable_st_storage in [false, true] {
        let mut opts = default_options();
        opts.out_of_order_time_window = 100;
        let db = new_test_db(vec![with_opts(opts), with_rngs(&[100])]);

        let blocks = [
            meta(100, 200), // Oldest block
            meta(200, 300),
            meta(300, 400),
            meta(400, 500),
            meta(500, 600), // Newest Block
        ];

        for m in &blocks {
            create_block(db.dir(), &gen_series(100, 10, m.min_time, m.max_time));
        }

        let head_blocks = [meta(700, 800)];

        // Add some data to the WAL.
        let mut head_app = db.head().appender(&ctx());
        let mut a_series = Labels::empty();
        for m in &head_blocks {
            let series = gen_series(100, 10, m.min_time, m.max_time + 1);
            for s in &series {
                a_series = s.labels().clone();
                let mut it = s.iterator(None);
                while it.next() == ValueType::Float {
                    let (tim, v) = it.at();
                    head_app.append(0, s.labels(), tim, v).unwrap();
                }
                assert!(it.err().is_none());
            }
        }
        commit(head_app.as_mut());
        db.head().mmap_head_chunks();

        // Go require.Eventually(2s, 100ms).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while !db.head().chunk_disk_mapper.is_queue_empty() {
            assert!(std::time::Instant::now() < deadline, "chunk write queue not empty");
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        let wal = db.head().wal().unwrap().clone();
        let cdm = &db.head().chunk_disk_mapper;

        // Test that registered size matches the actual disk size.
        db.reload_blocks().unwrap(); // Reload the db to register the new db size.
        assert_eq!(db.blocks().len(), blocks.len()); // Ensure all blocks are registered.
        let mut block_size = db.metrics.blocks_bytes.get() as i64; // Use the actual internal metrics.
        let mut wal_size = wal.size().unwrap();
        let mut cdm_size = cdm.size().unwrap();
        assert_ne!(0, cdm_size);
        // Expected size should take into account block size + WAL size + Head chunks size
        let mut exp_size = block_size + wal_size + cdm_size;
        let mut act_size = fileutil::dir_size(db.dir()).unwrap();
        assert_eq!(exp_size, act_size, "registered size doesn't match actual disk size");

        // Create a WAL checkpoint, and compare sizes.
        let (first, last) = wlog::segments(wal.dir()).unwrap();
        wlog::checkpoint(&wal, first, last - 1, &|_| false, 0, enable_st_storage, false).unwrap();
        block_size = db.metrics.blocks_bytes.get() as i64; // Use the actual internal metrics.
        wal_size = wal.size().unwrap();
        cdm_size = cdm.size().unwrap();
        assert_ne!(0, cdm_size);
        exp_size = block_size + wal_size + cdm_size;
        act_size = fileutil::dir_size(db.dir()).unwrap();
        assert_eq!(exp_size, act_size, "registered size doesn't match actual disk size");

        // Truncate Chunk Disk Mapper and compare sizes.
        cdm.truncate(900).unwrap();
        cdm_size = cdm.size().unwrap();
        assert_ne!(0, cdm_size);
        exp_size = block_size + wal_size + cdm_size;
        act_size = fileutil::dir_size(db.dir()).unwrap();
        assert_eq!(exp_size, act_size, "registered size doesn't match actual disk size");

        // Add some out of order samples to check the size of WBL.
        let mut head_app = db.head().appender(&ctx());
        for ts in 750..800i64 {
            head_app.append(0, &a_series, ts, ts as f64).unwrap();
        }
        commit(head_app.as_mut());

        wal_size = wal.size().unwrap();
        let wbl_size = db.head().wbl().unwrap().size().unwrap();
        assert_ne!(0, wbl_size);
        cdm_size = cdm.size().unwrap();
        exp_size = block_size + wal_size + wbl_size + cdm_size;
        act_size = fileutil::dir_size(db.dir()).unwrap();
        assert_eq!(exp_size, act_size, "registered size doesn't match actual disk size");

        // Decrease the max bytes limit so that a delete is triggered.
        // Check total size, total count and check that the oldest block was deleted.
        let first_block_size = db.blocks()[0].size();
        let size_limit = act_size - first_block_size;
        db.opts.write().unwrap().max_bytes = size_limit; // Set the new db size limit one block smaller that the actual size.
        db.reload_blocks().unwrap(); // Reload the db to register the new db size.

        let exp_blocks = &blocks[1..];
        let act_blocks = db.blocks();
        block_size = db.metrics.blocks_bytes.get() as i64;
        wal_size = wal.size().unwrap();
        cdm_size = cdm.size().unwrap();
        assert_ne!(0, cdm_size);
        // Expected size should take into account block size + WAL size + WBL size
        exp_size = block_size + wal_size + wbl_size + cdm_size;
        let act_retention_count = db.metrics.size_retention_count.get() as i64;
        act_size = fileutil::dir_size(db.dir()).unwrap();

        assert_eq!(1, act_retention_count, "metric retention count mismatch");
        assert_eq!(exp_size, act_size, "metric db size doesn't match actual disk size");
        assert!(
            exp_size <= size_limit,
            "actual size ({exp_size}) is expected to be less than or equal to limit ({size_limit})"
        );
        assert_eq!(
            act_blocks.len(),
            blocks.len() - 1,
            "new block count should be decreased from:{} to:{}",
            blocks.len(),
            blocks.len() - 1
        );
        assert_eq!(exp_blocks[0].max_time, act_blocks[0].meta().max_time, "maxT mismatch of the first block");
        assert_eq!(
            exp_blocks[exp_blocks.len() - 1].max_time,
            act_blocks[act_blocks.len() - 1].meta().max_time,
            "maxT mismatch of the last block"
        );
    }
}

#[test]
fn test_size_retention_metric() {
    let cases = [(1000i64, 1000i64), (0, 0), (-1000, 0)];

    for (max_bytes, exp_max_bytes) in cases {
        let db = new_test_db(vec![with_opts(Options { max_bytes, ..zero_options() }), with_rngs(&[100])]);

        let act_max_bytes = db.metrics.max_bytes.get() as i64;
        assert_eq!(exp_max_bytes, act_max_bytes, "metric retention limit bytes mismatch");
    }
}

fn retention_cfg(time_ms: i64) -> ApplyConfig {
    ApplyConfig {
        tsdb: Some(TsdbConfig {
            retention: Some(TsdbRetentionConfig { time: time_ms, ..TsdbRetentionConfig::default() }),
            ..TsdbConfig::default()
        }),
        ..ApplyConfig::default()
    }
}

// TestRuntimeRetentionConfigChange tests that retention configuration can be changed at runtime via ApplyConfig
// and that the retention logic properly deletes blocks when retention is shortened. This test also ensures
// race-free concurrent access to retention settings.
#[test]
fn test_runtime_retention_config_change() {
    const HOUR: i64 = 60 * 60 * 1000;
    const INITIAL_RETENTION_DURATION: i64 = 10 * HOUR; // 10 hours
    const SHORTER_RETENTION_DURATION: i64 = HOUR; // 1 hour

    let db = new_test_db(vec![
        with_opts(Options { retention_duration: INITIAL_RETENTION_DURATION, ..zero_options() }),
        with_rngs(&[100]),
    ]);

    let nine_hours_ms = 9 * HOUR;
    let nine_and_half_hours_ms = 9 * HOUR + 30 * 60 * 1000;
    let blocks = [
        meta(0, 100),                                           // 10 hours old (beyond new retention)
        meta(100, 200),                                         // 9.9 hours old (beyond new retention)
        meta(nine_hours_ms, nine_and_half_hours_ms),            // 1 hour old (within new retention)
        meta(nine_and_half_hours_ms, INITIAL_RETENTION_DURATION), // 0.5 hours old (within new retention)
    ];

    for m in &blocks {
        create_block(
            db.dir(),
            &gen_series_from_sample_generator(10, 10, m.min_time, m.max_time, 60 * 1000, |ts| fs(ts, rand_f64())),
        );
    }

    // Reload blocks and verify all are loaded.
    db.reload_blocks().unwrap();
    assert_eq!(db.blocks().len(), blocks.len(), "expected all blocks to be loaded initially");

    db.apply_config(&retention_cfg(SHORTER_RETENTION_DURATION)).expect("ApplyConfig should succeed");

    let actual_retention = db.opts.read().unwrap().retention_duration;
    assert_eq!(SHORTER_RETENTION_DURATION, actual_retention, "retention duration should be updated");

    let expected_retention_seconds = SHORTER_RETENTION_DURATION as f64 / 1000.0;
    let actual_retention_seconds = db.metrics.retention_duration.get();
    assert_eq!(expected_retention_seconds, actual_retention_seconds, "retention duration metric should be updated");

    db.reload_blocks().unwrap();

    // Verify that blocks beyond the new retention were deleted.
    // We expect only the last 2 blocks to remain (those within 1 hour).
    let act_blocks = db.blocks();
    assert_eq!(act_blocks.len(), 2, "expected old blocks to be deleted after retention change");

    // Verify the remaining blocks are the newest ones.
    assert_eq!(nine_hours_ms, act_blocks[0].meta().min_time, "first remaining block should be within retention");
    assert_eq!(nine_and_half_hours_ms, act_blocks[1].meta().min_time, "last remaining block should be the newest");

    assert!(db.metrics.time_retention_count.get() as i64 > 0, "time retention count should be incremented");
}

// TestApplyConfigRetentionDurationMetricUnit verifies that after a config reload the
// prometheus_tsdb_retention_limit_seconds metric reports the retention in seconds.
#[test]
fn test_apply_config_retention_duration_metric_unit() {
    let one_hour_ms = 60 * 60 * 1000i64;
    let db = db_with(Options { retention_duration: one_hour_ms, ..zero_options() });

    db.apply_config(&retention_cfg(one_hour_ms)).unwrap();

    assert_eq!(one_hour_ms, db.opts.read().unwrap().retention_duration);

    let got_seconds = db.metrics.retention_duration.get();
    let want_seconds = 3600.0;
    assert_eq!(want_seconds, got_seconds);
}

fn xor_cfg(floats: &str) -> ApplyConfig {
    ApplyConfig {
        tsdb: Some(TsdbConfig { float_chunk_encoding: floats.to_string(), ..TsdbConfig::default() }),
        ..ApplyConfig::default()
    }
}

fn opts_with_encoding(enc: chunkenc::Encoding, st: bool) -> Options {
    let mut opts = default_options();
    opts.float_chunk_encoding = enc;
    opts.enable_st_storage = st;
    opts
}

// TestDBApplyConfigChunkEncoding verifies that ApplyConfig correctly handles the chunk_encoding setting.
#[test]
fn test_db_apply_config_chunk_encoding() {
    use tsdb::db::{FLOAT_CHUNK_ENCODING_XOR, FLOAT_CHUNK_ENCODING_XOR2};

    // xor_default_accepts_config_xor2
    {
        let db = db_with(opts_with_encoding(chunkenc::ENC_XOR, false));
        db.apply_config(&xor_cfg(FLOAT_CHUNK_ENCODING_XOR2)).unwrap();
        assert!(db.head().opts.use_xor2_float_encoding());
    }

    // explicit_xor_overrides_xor2_default
    {
        let db = db_with(opts_with_encoding(chunkenc::ENC_XOR2, false));
        db.apply_config(&xor_cfg(FLOAT_CHUNK_ENCODING_XOR)).unwrap();
        assert!(!db.head().opts.use_xor2_float_encoding());
        assert_eq!(chunkenc::ENC_XOR2, db.opts.read().unwrap().float_chunk_encoding, "startup default must not be mutated");
    }

    // xor2_default_succeeds
    {
        let db = db_with(opts_with_encoding(chunkenc::ENC_XOR2, false));
        db.apply_config(&xor_cfg(FLOAT_CHUNK_ENCODING_XOR2)).unwrap();
        assert!(db.head().opts.use_xor2_float_encoding());
    }

    // empty_encoding_uses_startup_option
    for enc in [chunkenc::ENC_XOR2, chunkenc::ENC_XOR] {
        let db = db_with(opts_with_encoding(enc, false));
        db.apply_config(&xor_cfg("")).unwrap();
        assert_eq!(enc == chunkenc::ENC_XOR2, db.head().opts.use_xor2_float_encoding(), "{enc}");
    }

    // sequential_reload_reverts_to_startup_option
    {
        let db = db_with(opts_with_encoding(chunkenc::ENC_XOR2, false));

        db.apply_config(&xor_cfg(FLOAT_CHUNK_ENCODING_XOR)).unwrap();
        assert!(!db.head().opts.use_xor2_float_encoding());

        db.apply_config(&xor_cfg("")).unwrap();
        assert!(db.head().opts.use_xor2_float_encoding(), "empty encoding must revert to startup option");
    }

    // nil_TSDBConfig_resets_to_startup_option
    {
        let db = db_with(opts_with_encoding(chunkenc::ENC_XOR2, false));

        db.apply_config(&xor_cfg(FLOAT_CHUNK_ENCODING_XOR)).unwrap();
        assert!(!db.head().opts.use_xor2_float_encoding());

        db.apply_config(&ApplyConfig::default()).unwrap();
        assert!(db.head().opts.use_xor2_float_encoding(), "nil TSDBConfig must revert to startup option");
    }

    // xor_with_st_storage_returns_error
    {
        let db = db_with(opts_with_encoding(chunkenc::ENC_XOR2, true));
        let err = db.apply_config(&xor_cfg(FLOAT_CHUNK_ENCODING_XOR)).unwrap_err();
        assert!(err.to_string().contains("is incompatible with start-timestamp storage"), "{err}");
    }

    // empty_encoding_with_st_storage_keeps_startup_option
    {
        let db = db_with(opts_with_encoding(chunkenc::ENC_XOR2, true));

        // Removing the field must fall back to the startup option, not to the XOR default; otherwise st-storage
        // would silently lose XOR2.
        db.apply_config(&xor_cfg("")).unwrap();
        assert!(db.head().opts.use_xor2_float_encoding());
        db.apply_config(&ApplyConfig::default()).unwrap();
        assert!(db.head().opts.use_xor2_float_encoding());
    }

    // rejected_reload_keeps_active_encoding
    {
        let db = db_with(opts_with_encoding(chunkenc::ENC_XOR2, true));

        let err = db.apply_config(&xor_cfg(FLOAT_CHUNK_ENCODING_XOR)).unwrap_err();
        assert!(err.to_string().contains("is incompatible with start-timestamp storage"), "{err}");
        assert!(db.head().opts.use_xor2_float_encoding(), "active encoding must not change on a rejected reload");
    }
}

// requireBlockFloatChunkEncoding queries all chunks of the single series in the block and asserts they all use
// the expected encoding.
#[track_caller]
fn require_block_float_chunk_encoding(b: &Block, expected: chunkenc::Encoding) {
    let q = tsdb::querier::new_block_chunk_querier(b, b.min_time(), b.max_time()).unwrap();
    let res = query_chunks(q, &[eq(DEFAULT_LABEL_NAME, "0")]);
    assert_eq!(res.len(), 1);
    for chks in res.values() {
        assert!(!chks.is_empty());
        for chk in chks {
            assert_eq!(expected, chk.chunk.as_ref().unwrap().encoding());
        }
    }
}

// createOverlappingBlocks writes two overlapping blocks for the same series.
fn create_overlapping_blocks(dir: &Path, mint: i64) {
    create_block(dir, &gen_series(1, 1, mint, mint + 50));
    create_block(dir, &gen_series(1, 1, mint + 25, mint + 75));
}

// TestVerticalCompactionFloatChunkEncoding verifies that chunks re-encoded while compacting overlapping blocks
// use the configured float chunk encoding, including after a runtime configuration reload.
#[test]
fn test_vertical_compaction_float_chunk_encoding() {
    use tsdb::db::{FLOAT_CHUNK_ENCODING_XOR, FLOAT_CHUNK_ENCODING_XOR2};

    // default_xor
    {
        let dir = TempDir::new();
        create_overlapping_blocks(dir.path(), 0);

        let db = db_in(dir.path());
        db.disable_compactions();

        db.compact(&ctx()).unwrap();
        let blocks = db.blocks();
        assert_eq!(blocks.len(), 1);
        require_block_float_chunk_encoding(&blocks[0], chunkenc::ENC_XOR);
    }

    // config_xor2
    {
        let dir = TempDir::new();
        create_overlapping_blocks(dir.path(), 0);

        let db = new_test_db(vec![with_dir(dir.path()), with_opts(opts_with_encoding(chunkenc::ENC_XOR2, false))]);
        db.disable_compactions();

        db.compact(&ctx()).unwrap();
        let blocks = db.blocks();
        assert_eq!(blocks.len(), 1);
        require_block_float_chunk_encoding(&blocks[0], chunkenc::ENC_XOR2);
    }

    // apply_config_flip
    {
        let dir = TempDir::new();
        create_overlapping_blocks(dir.path(), 0);

        let db = new_test_db(vec![with_dir(dir.path()), with_opts(default_options())]);
        db.disable_compactions();

        // Flip the startup XOR default to XOR2 before the first compaction.
        db.apply_config(&xor_cfg(FLOAT_CHUNK_ENCODING_XOR2)).unwrap();
        db.compact(&ctx()).unwrap();
        let blocks = db.blocks();
        assert_eq!(blocks.len(), 1);
        require_block_float_chunk_encoding(&blocks[0], chunkenc::ENC_XOR2);

        // Flip back to XOR; the next compaction of a new pair of overlapping blocks must re-encode to XOR while
        // the previous block stays XOR2.
        create_overlapping_blocks(db.dir(), 100);
        db.reload_blocks().unwrap();
        db.apply_config(&xor_cfg(FLOAT_CHUNK_ENCODING_XOR)).unwrap();
        db.compact(&ctx()).unwrap();
        let blocks = db.blocks();
        assert_eq!(blocks.len(), 2);
        require_block_float_chunk_encoding(&blocks[0], chunkenc::ENC_XOR2);
        require_block_float_chunk_encoding(&blocks[1], chunkenc::ENC_XOR);
    }
}

// TestChunkQuerierFloatChunkEncoding verifies that chunks re-encoded while merging overlapping blocks at query
// time use the configured float chunk encoding.
#[test]
fn test_chunk_querier_float_chunk_encoding() {
    for (name, xor2, expected) in [("default_xor", false, chunkenc::ENC_XOR), ("config_xor2", true, chunkenc::ENC_XOR2)] {
        let dir = TempDir::new();
        create_block(dir.path(), &gen_series(1, 1, 0, 50));
        create_block(dir.path(), &gen_series(1, 1, 25, 75));

        let mut opts = default_options();
        if xor2 {
            opts.float_chunk_encoding = chunkenc::ENC_XOR2;
        }
        let db = new_test_db(vec![with_dir(dir.path()), with_opts(opts)]);
        db.disable_compactions();

        let q = db.chunk_querier(0, 75).unwrap();
        let res = query_chunks(q, &[eq(DEFAULT_LABEL_NAME, "0")]);
        assert_eq!(res.len(), 1, "{name}");
        for chks in res.values() {
            assert!(!chks.is_empty());
            for chk in chks {
                assert_eq!(expected, chk.chunk.as_ref().unwrap().encoding(), "{name}");
            }
        }
    }
}

#[test]
fn test_head_options_use_xor2_float_encoding() {
    let opts = HeadOptions::default();
    assert!(!opts.use_xor2_float_encoding(), "default must be XOR, not XOR2");

    opts.float_chunk_encoding.store(chunkenc::ENC_XOR2.0 as u32, Ordering::Relaxed);
    assert!(opts.use_xor2_float_encoding());

    opts.float_chunk_encoding.store(chunkenc::ENC_XOR.0 as u32, Ordering::Relaxed);
    assert!(!opts.use_xor2_float_encoding());

    // EncNone (zero value) must not be treated as XOR2.
    opts.float_chunk_encoding.store(chunkenc::ENC_NONE.0 as u32, Ordering::Relaxed);
    assert!(!opts.use_xor2_float_encoding(), "EncNone must not be treated as XOR2");
}

#[test]
fn test_default_options_float_chunk_encoding() {
    assert_eq!(
        chunkenc::ENC_XOR,
        default_options().float_chunk_encoding,
        "DefaultOptions must use EncXOR as the float chunk encoding"
    );
}

#[test]
fn test_validate_opts_invalid_float_chunk_encoding() {
    let mut opts = default_options();
    opts.float_chunk_encoding = chunkenc::ENC_HISTOGRAM;
    let err = validate_opts(Some(opts), Vec::new()).err().unwrap();
    assert!(err.to_string().contains("unsupported float chunk encoding"), "{err}");
}

#[test]
fn test_validate_opts_st_storage_requires_xor2() {
    let mut opts = default_options();
    opts.enable_st_storage = true;
    // Default encoding is EncXOR; combining it with EnableSTStorage must be rejected.
    let err = validate_opts(Some(opts.clone()), Vec::new()).err().unwrap();
    assert!(err.to_string().contains("is incompatible with start-timestamp storage"), "{err}");

    // EncXOR2 + st-storage must be accepted.
    opts.float_chunk_encoding = chunkenc::ENC_XOR2;
    validate_opts(Some(opts), Vec::new()).unwrap();
}

#[test]
fn test_not_matcher_selects_labels_unset_series() {
    let db = new_test_db(vec![]);

    let labelpairs = vec![ls(&["a", "abcd", "b", "abcde"]), ls(&["labelname", "labelvalue"])];

    let mut app = db.appender(&ctx());
    for lbls in &labelpairs {
        app.append(0, lbls, 0, 1.0).unwrap();
    }
    commit(app.as_mut());

    let cases: Vec<(Vec<Matcher>, Vec<Labels>)> = vec![
        (vec![m(MatchType::NotEqual, "lname", "lvalue")], labelpairs.clone()),
        (vec![eq("a", "abcd"), m(MatchType::NotEqual, "b", "abcde")], vec![]),
        (vec![eq("a", "abcd"), m(MatchType::NotEqual, "b", "abc")], vec![labelpairs[0].clone()]),
        (vec![m(MatchType::NotRegexp, "a", "abd.*")], labelpairs.clone()),
        (vec![m(MatchType::NotRegexp, "a", "abc.*")], labelpairs[1..].to_vec()),
        (vec![m(MatchType::NotRegexp, "c", "abd.*")], labelpairs.clone()),
        (vec![m(MatchType::NotRegexp, "labelname", "labelvalue")], labelpairs[..1].to_vec()),
    ];

    let mut q = db.querier(0, 10).unwrap();

    for (selector, series) in &cases {
        let mut ss = q.select(&ctx(), false, None, selector);
        let (lres, _, ws, err) = expand_series_set(ss.as_mut());
        assert!(err.is_none());
        assert!(ws.is_empty());
        assert_eq!(series, &lres);
    }
    assert!(q.close().is_none());
}

#[test]
fn test_overlapping_blocks_detects_all_overlaps() {
    // Create 10 blocks that does not overlap (0-10, 10-20, ..., 100-110) but in reverse order to ensure our
    // algorithm will handle that.
    let mut metas = vec![BlockMeta::default(); 11];
    for i in (0..=10).rev() {
        metas[i] = meta(i as i64 * 10, (i as i64 + 1) * 10);
    }

    assert!(overlapping_blocks(&metas).is_empty(), "we found unexpected overlaps");

    // Add overlapping blocks. We've to establish order again since we aren't interested in trivial overlaps
    // caused by unorderedness.
    let add = |ms: &[BlockMeta]| -> Vec<BlockMeta> {
        let mut repl: Vec<BlockMeta> = metas.iter().cloned().chain(ms.iter().cloned()).collect();
        // Go sort.Slice is not stable; the only ties here are between equal metas, so the order is the same.
        repl.sort_by_key(|m| m.min_time);
        repl
    };
    let ov = |entries: Vec<((i64, i64), Vec<BlockMeta>)>| -> tsdb::db::Overlaps {
        entries.into_iter().map(|((a, b), v)| (time_range(a, b), v)).collect()
    };

    // o1 overlaps with 10-20.
    let o1 = meta(15, 17);
    assert_eq!(ov(vec![((15, 17), vec![metas[1].clone(), o1.clone()])]), overlapping_blocks(&add(&[o1.clone()])));

    // o2 overlaps with 20-30 and 30-40.
    let o2 = meta(21, 31);
    assert_eq!(
        ov(vec![((21, 30), vec![metas[2].clone(), o2.clone()]), ((30, 31), vec![o2.clone(), metas[3].clone()])]),
        overlapping_blocks(&add(&[o2.clone()]))
    );

    // o3a and o3b overlaps with 30-40 and each other.
    let o3a = meta(33, 39);
    let o3b = meta(34, 36);
    assert_eq!(
        ov(vec![((34, 36), vec![metas[3].clone(), o3a.clone(), o3b.clone()])]),
        overlapping_blocks(&add(&[o3a.clone(), o3b.clone()]))
    );

    // o4 is 1:1 overlap with 50-60.
    let o4 = meta(50, 60);
    assert_eq!(ov(vec![((50, 60), vec![metas[5].clone(), o4.clone()])]), overlapping_blocks(&add(&[o4.clone()])));

    // o5 overlaps with 60-70, 70-80 and 80-90.
    let o5 = meta(61, 85);
    assert_eq!(
        ov(vec![
            ((61, 70), vec![metas[6].clone(), o5.clone()]),
            ((70, 80), vec![o5.clone(), metas[7].clone()]),
            ((80, 85), vec![o5.clone(), metas[8].clone()]),
        ]),
        overlapping_blocks(&add(&[o5.clone()]))
    );

    // o6a overlaps with 90-100, 100-110 and o6b, o6b overlaps with 90-100 and o6a.
    let o6a = meta(92, 105);
    let o6b = meta(94, 99);
    assert_eq!(
        ov(vec![
            ((94, 99), vec![metas[9].clone(), o6a.clone(), o6b.clone()]),
            ((100, 105), vec![o6a.clone(), metas[10].clone()]),
        ]),
        overlapping_blocks(&add(&[o6a.clone(), o6b.clone()]))
    );

    // All together.
    assert_eq!(
        ov(vec![
            ((15, 17), vec![metas[1].clone(), o1.clone()]),
            ((21, 30), vec![metas[2].clone(), o2.clone()]),
            ((30, 31), vec![o2.clone(), metas[3].clone()]),
            ((34, 36), vec![metas[3].clone(), o3a.clone(), o3b.clone()]),
            ((50, 60), vec![metas[5].clone(), o4.clone()]),
            ((61, 70), vec![metas[6].clone(), o5.clone()]),
            ((70, 80), vec![o5.clone(), metas[7].clone()]),
            ((80, 85), vec![o5.clone(), metas[8].clone()]),
            ((94, 99), vec![metas[9].clone(), o6a.clone(), o6b.clone()]),
            ((100, 105), vec![o6a.clone(), metas[10].clone()]),
        ]),
        overlapping_blocks(&add(&[o1, o2, o3a, o3b, o4, o5, o6a, o6b]))
    );

    // Additional case.
    let nc1 = vec![
        meta(1, 5),
        meta(2, 3),
        meta(2, 3),
        meta(2, 3),
        meta(2, 3),
        meta(2, 6),
        meta(3, 5),
        meta(5, 7),
        meta(7, 10),
        meta(8, 9),
    ];
    assert_eq!(
        ov(vec![
            // 1-5, 2-3, 2-3, 2-3, 2-3, 2,6
            ((2, 3), vec![nc1[0].clone(), nc1[1].clone(), nc1[2].clone(), nc1[3].clone(), nc1[4].clone(), nc1[5].clone()]),
            ((3, 5), vec![nc1[0].clone(), nc1[5].clone(), nc1[6].clone()]), // 1-5, 2-6, 3-5
            ((5, 6), vec![nc1[5].clone(), nc1[7].clone()]),                 // 2-6, 5-7
            ((8, 9), vec![nc1[8].clone(), nc1[9].clone()]),                 // 7-10, 8-9
        ]),
        overlapping_blocks(&nc1)
    );
}

// Regression test for https://github.com/prometheus/tsdb/issues/347
#[test]
fn test_chunk_at_block_boundary() {
    let db = new_test_db(vec![]);

    let mut app = db.appender(&ctx());

    let block_range = block_range(&db);
    let label = ls(&["foo", "bar"]);

    for i in 0..3i64 {
        app.append(0, &label, i * block_range, 0.0).unwrap();
        app.append(0, &label, i * block_range + 1000, 0.0).unwrap();
    }

    commit(app.as_mut());

    db.compact(&ctx()).unwrap();

    let mut builder = ScratchBuilder::new(0);

    for block in db.blocks() {
        let r = block.index().unwrap();

        let meta = block.meta();

        let (k, v) = all_postings_key();
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
fn test_querier_with_boundary_chunks() {
    let db = new_test_db(vec![]);

    let mut app = db.appender(&ctx());

    let block_range = block_range(&db);
    let label = ls(&["foo", "bar"]);

    for i in 0..5i64 {
        app.append(0, &label, i * block_range, 0.0).unwrap();
        app.append(0, &ls(&["blockID", &i.to_string()]), i * block_range, 0.0).unwrap();
    }

    commit(app.as_mut());

    db.compact(&ctx()).unwrap();

    assert!(db.blocks().len() >= 3, "invalid test, less than three blocks in DB");

    let mut q = db.querier(block_range, 2 * block_range).unwrap();

    // The requested interval covers 2 blocks, so the querier's label values for blockID should give us 2 values,
    // one from each block.
    let (b, ws, err) = q.label_values(&ctx(), "blockID", None, &[]);
    assert!(err.is_none());
    assert!(ws.is_empty());
    assert_eq!(vec!["1".to_string(), "2".to_string()], b);
    q.close();
}

/// Writes the series and samples records of the WAL-only tests of `TestInitializeHeadTimestamp`.
fn write_test_wal(dir: &Path, enable_st_storage: bool) {
    std::fs::create_dir_all(dir.join("wal")).unwrap();
    let w = WL::new(dir.join("wal"), compression::Type::None).unwrap();

    let enc = Encoder { enable_st_storage };
    let mut series = Vec::new();
    enc.series(
        &[
            RefSeries { r#ref: 123, labels: ls(&["a", "1"]) },
            RefSeries { r#ref: 124, labels: ls(&["a", "2"]) },
        ],
        &mut series,
    );
    let mut samples = Vec::new();
    enc.samples(
        &[RefSample { r#ref: 123, st: 0, t: 5000, v: 1.0 }, RefSample { r#ref: 124, st: 0, t: 15000, v: 1.0 }],
        &mut samples,
    );
    w.log_batch(&[series, samples]).unwrap();
    w.close().unwrap();
}

// TestInitializeHeadTimestamp ensures that the h.minTime is set properly.
//   - no blocks no WAL: set to the time of the first  appended sample
//   - no blocks with WAL: set to the smallest sample from the WAL
//   - with blocks no WAL: set to the last block maxT
//   - with blocks with WAL: same as above
#[test]
fn test_initialize_head_timestamp() {
    // clean
    {
        let db = new_test_db(vec![]);

        // Should be set to init values if no WAL or blocks exist so far.
        assert_eq!(i64::MAX, db.head().min_time());
        assert_eq!(i64::MIN, db.head().max_time());
        assert!(!db.head().initialized());

        // First added sample initializes the writable range.
        let mut app = db.appender(&ctx());
        app.append(0, &ls(&["a", "b"]), 1000, 1.0).unwrap();

        assert_eq!(1000, db.head().min_time());
        assert_eq!(1000, db.head().max_time());
        assert!(db.head().initialized());
    }

    for enable_st_storage in [false, true] {
        // wal-only-st-<enableSTStorage>
        let dir = TempDir::new();
        write_test_wal(dir.path(), enable_st_storage);

        let db = db_in(dir.path());

        assert_eq!(5000, db.head().min_time());
        assert_eq!(15000, db.head().max_time());
        assert!(db.head().initialized());
    }

    // existing-block
    {
        let dir = TempDir::new();

        create_block(dir.path(), &gen_series(1, 1, 1000, 2000));

        let db = db_in(dir.path());

        assert_eq!(2000, db.head().min_time());
        assert_eq!(2000, db.head().max_time());
        assert!(db.head().initialized());
    }

    for enable_st_storage in [false, true] {
        // existing-block-and-wal,enableSTStorage=<enableSTStorage>
        let dir = TempDir::new();

        create_block(dir.path(), &gen_series(1, 1, 1000, 6000));
        write_test_wal(dir.path(), enable_st_storage);

        let db = db_in(dir.path());

        assert_eq!(6000, db.head().min_time());
        assert_eq!(15000, db.head().max_time());
        assert!(db.head().initialized());
        // Check that old series has been GCed.
        // adapted: Go reads the `prometheus_tsdb_head_series` gauge, a function of the head's series count.
        assert_eq!(1, db.head().num_series());
    }
}

#[test]
fn test_no_empty_blocks() {
    let db = new_test_db(vec![with_rngs(&[100])]);

    db.disable_compactions();

    let range_to_trigger_compaction = block_range(&db) / 2 * 3 - 1;
    let default_label = ls(&["foo", "bar"]);
    let default_matcher = m(MatchType::Regexp, "", ".*");

    // Test no blocks after compact with empty head.
    {
        db.compact(&ctx()).unwrap();
        let act_blocks = block_dirs(db.dir()).unwrap();
        assert_eq!(act_blocks.len(), db.blocks().len());
        assert!(act_blocks.is_empty());
        assert_eq!(0, compactions_ran(&db), "no compaction should be triggered here");
    }

    // Test no blocks after deleting all samples from head.
    {
        let mut app = db.appender(&ctx());
        app.append(0, &default_label, 1, 0.0).unwrap();
        app.append(0, &default_label, 2, 0.0).unwrap();
        app.append(0, &default_label, 3 + range_to_trigger_compaction, 0.0).unwrap();
        commit(app.as_mut());
        db.delete(&ctx(), i64::MIN, i64::MAX, std::slice::from_ref(&default_matcher)).unwrap();
        db.compact(&ctx()).unwrap();
        assert_eq!(1, compactions_ran(&db), "compaction should have been triggered here");

        let act_blocks = block_dirs(db.dir()).unwrap();
        assert_eq!(act_blocks.len(), db.blocks().len());
        assert!(act_blocks.is_empty());

        let mut app = db.appender(&ctx());
        let err = app.append(0, &default_label, 1, 0.0).unwrap_err();
        assert!(
            err.is(&ERR_OUT_OF_BOUNDS) && err.to_string() == ERR_OUT_OF_BOUNDS.to_string(),
            "the head should be truncated so no samples in the past should be allowed: {err}"
        );

        // Adding new blocks.
        let current_time = db.head().max_time();
        app.append(0, &default_label, current_time, 0.0).unwrap();
        app.append(0, &default_label, current_time + 1, 0.0).unwrap();
        app.append(0, &default_label, current_time + range_to_trigger_compaction, 0.0).unwrap();
        commit(app.as_mut());

        db.compact(&ctx()).unwrap();
        assert_eq!(2, compactions_ran(&db), "compaction should have been triggered here");
        let act_blocks = block_dirs(db.dir()).unwrap();
        assert_eq!(act_blocks.len(), db.blocks().len());
        assert_eq!(act_blocks.len(), 1, "No blocks created when compacting with >0 samples");
    }

    // When no new block is created from head, and there are some blocks on disk compaction should not run into
    // infinite loop (was seen during development).
    {
        let old_blocks = db.blocks();
        let mut app = db.appender(&ctx());
        let current_time = db.head().max_time();
        app.append(0, &default_label, current_time, 0.0).unwrap();
        app.append(0, &default_label, current_time + 1, 0.0).unwrap();
        app.append(0, &default_label, current_time + range_to_trigger_compaction, 0.0).unwrap();
        commit(app.as_mut());
        db.head().delete(&ctx(), i64::MIN, i64::MAX, std::slice::from_ref(&default_matcher)).unwrap();
        db.compact(&ctx()).unwrap();
        assert_eq!(3, compactions_ran(&db), "compaction should have been triggered here");
        // Go require.Equal on the block pointers: the same blocks.
        let blocks = db.blocks();
        assert_eq!(old_blocks.len(), blocks.len());
        for (a, b) in old_blocks.iter().zip(&blocks) {
            assert!(a.ptr_eq(b));
        }
    }

    // Test no blocks remaining after deleting all samples from disk.
    {
        let current_time = db.head().max_time();
        let r0 = block_range(&db);
        let blocks = [meta(current_time, current_time + r0), meta(current_time + 100, current_time + 100 + r0)];
        for m in &blocks {
            create_block(db.dir(), &gen_series(2, 2, m.min_time, m.max_time));
        }

        let old_blocks = db.blocks();
        db.reload_blocks().unwrap(); // Reload the db to register the new blocks.
        assert_eq!(db.blocks().len(), blocks.len() + old_blocks.len()); // Ensure all blocks are registered.
        db.delete(&ctx(), i64::MIN, i64::MAX, std::slice::from_ref(&default_matcher)).unwrap();
        db.compact(&ctx()).unwrap();
        assert_eq!(
            5,
            compactions_ran(&db),
            "compaction should have been triggered here once for each block that have tombstones"
        );

        let act_blocks = block_dirs(db.dir()).unwrap();
        assert_eq!(act_blocks.len(), db.blocks().len());
        assert_eq!(
            act_blocks.len(),
            1,
            "All samples are deleted. Only the most recent block should remain after compaction."
        );
    }
}

#[test]
fn test_db_label_names() {
    struct Case {
        // Add 'sampleLabels1' -> Test Head -> Compact -> Test Disk -> -> Add 'sampleLabels2' -> Test Head+Disk
        sample_labels1: Vec<[&'static str; 2]>, // For checking head and disk separately.
        // To test Head+Disk, sampleLabels2 should have at least 1 unique label name which is not in
        // sampleLabels1.
        sample_labels2: Vec<[&'static str; 2]>, // For checking head and disk together.
        exp1: Vec<&'static str>,                // after adding sampleLabels1.
        exp2: Vec<&'static str>,                // after adding sampleLabels1 and sampleLabels2.
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
        let mut app = db.appender(&ctx());
        for i in mint..=maxt {
            for tuple in sample_labels {
                let label = ls(&[tuple[0], tuple[1]]);
                app.append(0, &label, i * block_range, 0.0).unwrap();
            }
        }
        commit(app.as_mut());
    };
    let strs = |v: &[&str]| -> Vec<String> { v.iter().map(|s| s.to_string()).collect() };
    for tst in &tests {
        let db = new_test_db(vec![]);

        append_samples(&db, 0, 4, &tst.sample_labels1);

        // Testing head.
        let head_indexr = db.head().index();
        let label_names = head_indexr.label_names(&ctx(), &[]).unwrap();
        assert_eq!(strs(&tst.exp1), label_names);
        head_indexr.close().unwrap();

        // Testing disk.
        db.compact(&ctx()).unwrap();
        // All blocks have same label names, hence check them individually.
        // No need to aggregate and check.
        for b in db.blocks() {
            let block_indexr = b.index().unwrap();
            let label_names = block_indexr.label_names(&ctx(), &[]).unwrap();
            assert_eq!(strs(&tst.exp1), label_names);
            block_indexr.close().unwrap();
        }

        // Adding more samples to head with new label names so that we can test (head+disk).LabelNames(ctx) (the
        // union).
        append_samples(&db, 5, 9, &tst.sample_labels2);

        // Testing DB (union).
        let mut q = db.querier(i64::MIN, i64::MAX).unwrap();
        let (label_names, ws, err) = q.label_names(&ctx(), None, &[]);
        assert!(err.is_none());
        assert!(ws.is_empty());
        assert!(q.close().is_none());
        assert_eq!(strs(&tst.exp2), label_names);
    }
}

#[test]
fn test_correct_num_tombstones() {
    let db = new_test_db(vec![]);

    let block_range = block_range(&db);
    let (name, value) = ("foo", "bar");
    let default_label = ls(&[name, value]);
    let default_matcher = [eq(name, value)];

    let mut app = db.appender(&ctx());
    for i in 0..3i64 {
        for j in 0..15i64 {
            app.append(0, &default_label, i * block_range + j, 0.0).unwrap();
        }
    }
    commit(app.as_mut());

    db.compact(&ctx()).unwrap();
    assert_eq!(db.blocks().len(), 1);

    db.delete(&ctx(), 0, 1, &default_matcher).unwrap();
    assert_eq!(1, db.blocks()[0].meta().stats.num_tombstones);

    // {0, 1} and {2, 3} are merged to form 1 tombstone.
    db.delete(&ctx(), 2, 3, &default_matcher).unwrap();
    assert_eq!(1, db.blocks()[0].meta().stats.num_tombstones);

    db.delete(&ctx(), 5, 6, &default_matcher).unwrap();
    assert_eq!(2, db.blocks()[0].meta().stats.num_tombstones);

    db.delete(&ctx(), 9, 11, &default_matcher).unwrap();
    assert_eq!(3, db.blocks()[0].meta().stats.num_tombstones);
}

// TestBlockRanges checks the following use cases:
//   - No samples can be added with timestamps lower than the last block maxt.
//   - The compactor doesn't create overlapping blocks even when the last blocks is not within the default
//     boundaries.
//   - Lower boundary is based on the smallest sample in the head and upper boundary is rounded to the configured
//     block range.
//
// This ensures that a snapshot that includes the head and creates a block with a custom time range will not
// overlap with the first block created by the next compaction.
#[test]
fn test_block_ranges() {
    let logger = tsdb::Logger::nop();

    let dir = TempDir::new();

    // Test that the compactor doesn't create overlapping blocks when a non standard block already exists.
    let first_block_max_t = 3i64;
    create_block(dir.path(), &gen_series(1, 1, 0, first_block_max_t));
    let db = tsdb::db::open_with_ranges(dir.path(), Some(logger.clone()), default_options(), vec![10000], None)
        .unwrap();
    db.disable_compactions();

    let range_to_trigger_compaction = block_range(&db) / 2 * 3 + 1;

    let mut app = db.appender(&ctx());
    let lbl = ls(&["a", "b"]);
    assert!(
        app.append(0, &lbl, first_block_max_t - 1, rand_f64()).is_err(),
        "appending a sample with a timestamp covered by a previous block shouldn't be possible"
    );
    app.append(0, &lbl, first_block_max_t + 1, rand_f64()).unwrap();
    app.append(0, &lbl, first_block_max_t + 2, rand_f64()).unwrap();
    let second_block_maxt = first_block_max_t + range_to_trigger_compaction;
    app.append(0, &lbl, second_block_maxt, rand_f64()).unwrap(); // Add samples to trigger a new compaction

    commit(app.as_mut());
    db.compact(&ctx()).unwrap();
    let blocks = db.blocks();
    assert_eq!(blocks.len(), 2, "no new block after compaction");

    assert!(
        blocks[1].meta().min_time >= blocks[0].meta().max_time,
        "new block overlaps  old:{:?},new:{:?}",
        blocks[0].meta(),
        blocks[1].meta()
    );

    // Test that wal records are skipped when an existing block covers the same time ranges and compaction
    // doesn't create an overlapping block.
    let mut app = db.appender(&ctx());
    app.append(0, &lbl, second_block_maxt + 1, rand_f64()).unwrap();
    app.append(0, &lbl, second_block_maxt + 2, rand_f64()).unwrap();
    app.append(0, &lbl, second_block_maxt + 3, rand_f64()).unwrap();
    app.append(0, &lbl, second_block_maxt + 4, rand_f64()).unwrap();
    commit(app.as_mut());
    db.close().unwrap();

    let third_block_maxt = second_block_maxt + 2;
    create_block(dir.path(), &gen_series(1, 1, second_block_maxt + 1, third_block_maxt));

    let db = tsdb::db::open_with_ranges(dir.path(), Some(logger), default_options(), vec![10000], None).unwrap();
    db.disable_compactions();

    assert_eq!(db.blocks().len(), 3, "db doesn't include expected number of blocks");
    assert_eq!(db.blocks()[2].meta().max_time, third_block_maxt, "unexpected maxt of the last block");

    let mut app = db.appender(&ctx());
    app.append(0, &lbl, third_block_maxt + range_to_trigger_compaction, rand_f64()).unwrap(); // Trigger a compaction
    commit(app.as_mut());
    db.compact(&ctx()).unwrap();
    let blocks = db.blocks();
    assert_eq!(blocks.len(), 4, "no new block after compaction");

    assert!(
        blocks[3].meta().min_time >= blocks[2].meta().max_time,
        "new block overlaps  old:{:?},new:{:?}",
        blocks[2].meta(),
        blocks[3].meta()
    );
    db.close().unwrap();
}

// TestDBReadOnly ensures that opening a DB in readonly mode doesn't modify any files on the disk. It also checks
// that the API calls return equivalent results as a normal db.Open() mode.
#[test]
fn test_db_read_only() {
    let db_dir = TempDir::new();
    let match_all = [eq("", "")];

    // Bootstrap the db.
    {
        let db_blocks = [
            // Create three 2-sample blocks.
            meta(10, 12),
            meta(12, 14),
            meta(14, 16),
        ];

        for m in &db_blocks {
            let _ = create_block(db_dir.path(), &gen_series(1, 1, m.min_time, m.max_time));
        }

        // Add head to test DBReadOnly WAL reading capabilities.
        let w = Arc::new(WL::new(db_dir.path().join("wal"), compression::Type::Snappy).unwrap());
        let h = create_head(w, &gen_series(1, 1, 16, 18), db_dir.path());
        h.close().unwrap();
    }

    // Open a normal db to use for a comparison.
    let (exp_blocks, exp_series, exp_chunks, exp_db_hash);
    {
        let db_writable = db_in(db_dir.path());
        db_writable.disable_compactions();

        let db_size_before_append = fileutil::dir_size(db_writable.dir()).unwrap();
        let mut app = db_writable.appender(&ctx());
        app.append(0, &ls(&["foo", "bar"]), db_writable.head().max_time() + 1, 0.0).unwrap();
        commit(app.as_mut());

        exp_blocks = db_writable.blocks().iter().map(|b| b.meta()).collect::<Vec<_>>();
        let exp_db_size = fileutil::dir_size(db_writable.dir()).unwrap();
        assert!(exp_db_size > db_size_before_append, "db size didn't increase after an append");

        let q = db_writable.querier(i64::MIN, i64::MAX).unwrap();
        exp_series = query(q, &match_all);
        let cq = db_writable.chunk_querier(i64::MIN, i64::MAX).unwrap();
        exp_chunks = query_and_expand_chunks(cq, &match_all);

        db_writable.close().unwrap(); // Close here to allow getting the dir hash for windows.
        exp_db_hash = dir_hash(db_writable.dir());
    }
    let exp_block = exp_blocks[0].clone();

    // Open a read only db and ensure that the API returns the same result as the normal DB.
    let mut db_read_only = DbReadOnly::open(db_dir.path(), None, None).unwrap();

    // blocks
    {
        let blocks = db_read_only.blocks().unwrap();
        assert_eq!(blocks.len(), exp_blocks.len());
        for (i, exp_block) in exp_blocks.iter().enumerate() {
            assert_eq!(exp_block, &blocks[i].meta(), "block meta mismatch");
        }
    }
    // block
    {
        let block_id = exp_block.ulid.to_string();
        let block = db_read_only.block(&block_id, None).unwrap();
        assert_eq!(exp_block, block.meta(), "block meta mismatch");
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
        let read_only_series = query(q, &match_all);
        let read_only_db_hash = dir_hash(db_dir.path());

        assert_eq!(read_only_series.len(), exp_series.len(), "total series mismatch");
        assert_eq!(exp_series, read_only_series, "series mismatch");
        assert_eq!(exp_db_hash, read_only_db_hash, "after all read operations the db hash should remain the same");
    }
    // chunk querier
    {
        let cq = db_read_only.chunk_querier(i64::MIN, i64::MAX).unwrap();
        let read_only_series = query_and_expand_chunks(cq, &match_all);
        let read_only_db_hash = dir_hash(db_dir.path());

        assert_eq!(read_only_series.len(), exp_chunks.len(), "total series mismatch");
        assert_eq!(exp_chunks, read_only_series, "series chunks mismatch");
        assert_eq!(exp_db_hash, read_only_db_hash, "after all read operations the db hash should remain the same");
    }
    db_read_only.close().unwrap();
}

// TestDBReadOnlyClosing ensures that after closing the db all api methods return an ErrClosed.
#[test]
fn test_db_read_only_closing() {
    let sandbox_dir = TempDir::new();
    let dir = TempDir::new();
    let mut db = DbReadOnly::open(dir.path(), Some(sandbox_dir.path()), Some(tsdb::Logger::nop())).unwrap();
    // The sandboxDir was there.
    assert!(db.sandbox_dir().is_dir());
    db.close().unwrap();
    // The sandboxDir was deleted when closing.
    assert!(!db.sandbox_dir().exists());
    let err = db.close().unwrap_err();
    assert!(err.is(&ERR_CLOSED), "{err}");
    let err = db.blocks().err().unwrap();
    assert!(err.is(&ERR_CLOSED), "{err}");
    let err = db.querier(0, 1).err().unwrap();
    assert!(err.is(&ERR_CLOSED), "{err}");
}

#[test]
fn test_db_read_only_flush_wal() {
    let db_dir = TempDir::new();
    let maxt = 1000;

    // Bootstrap the db.
    {
        // Append data to the WAL.
        let db = db_in(db_dir.path());
        db.disable_compactions();
        let mut app = db.appender(&ctx());
        for i in 0..maxt {
            app.append(0, &ls(&[DEFAULT_LABEL_NAME, "flush"]), i, 1.0).unwrap();
        }
        commit(app.as_mut());
        db.close().unwrap();
    }

    // Flush WAL.
    let mut db = DbReadOnly::open(db_dir.path(), None, None).unwrap();

    let flush = TempDir::new();
    db.flush_wal(flush.path()).unwrap();
    db.close().unwrap();

    // Reopen the DB from the flushed WAL block.
    let mut db = DbReadOnly::open(flush.path(), None, None).unwrap();
    let blocks = db.blocks().unwrap();
    assert_eq!(blocks.len(), 1);

    let mut querier = db.querier(0, maxt - 1).unwrap();

    // Sum the values.
    let sum = sum_values(querier.as_ref(), eq(DEFAULT_LABEL_NAME, "flush"));
    assert!(querier.close().is_none());
    assert_eq!(1000.0, sum);
    db.close().unwrap();
}

#[test]
fn test_db_read_only_querier_no_alteration() {
    let count_chunks = |dir: &Path| -> usize { std::fs::read_dir(mmapped_chunks_dir(dir)).unwrap().count() };

    let spin_up_querier_and_check = |dir: &Path, sandbox_dir: &Path, chunks_count: usize| {
        let db_dir_hash = dir_hash(dir);
        // Bootstrap a RO db from the same dir and set up a querier.
        let mut db_read_only = DbReadOnly::open(dir, Some(sandbox_dir), None).unwrap();
        assert_eq!(chunks_count, count_chunks(dir));
        let mut q = db_read_only.querier(i64::MIN, i64::MAX).unwrap();
        assert!(q.close().is_none());
        db_read_only.close().unwrap();
        // The RO Head doesn't alter RW db chunks_head/.
        assert_eq!(chunks_count, count_chunks(dir));
        assert_eq!(dir_hash(dir), db_dir_hash);
    };

    // doesn't cut chunks while replaying WAL
    {
        let db = new_test_db(vec![]);

        // Append until the first mmapped head chunk.
        for i in 0..121 {
            let mut app = db.appender(&ctx());
            app.append(0, &ls(&["foo", "bar"]), i, 0.0).unwrap();
            commit(app.as_mut());
        }

        let sandbox = TempDir::new();
        spin_up_querier_and_check(db.dir(), sandbox.path(), 0);

        // The RW Head should have no problem cutting its own chunk, this also proves that a chunk needed to be
        // cut.
        db.force_head_mmap();
        assert_eq!(1, count_chunks(db.dir()));
    }

    // doesn't truncate corrupted chunks
    {
        let db = new_test_db(vec![]);
        db.close().unwrap();

        // Simulate a corrupted chunk: without a header.
        std::fs::File::create(mmapped_chunks_dir(db.dir()).join("000001")).unwrap();

        let sandbox = TempDir::new();
        spin_up_querier_and_check(db.dir(), sandbox.path(), 1);

        // The RW Head should have no problem truncating its corrupted file: this proves that the chunk needed to
        // be truncated.
        let db2 = db_in(db.dir());

        assert_eq!(0, count_chunks(db2.dir()));
    }
}

#[test]
fn test_db_cannot_see_partial_commits() {
    // Go skips the test when isolation is disabled (defaultIsolationDisabled); it is enabled here.
    let db = new_test_db(vec![]);

    let stop = Arc::new(AtomicBool::new(false));
    let (first_insert_tx, first_insert_rx) = mpsc::channel::<()>();

    // Insert data in batches.
    let writer = {
        let db: Arc<DB> = db.db.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut iter = 0i64;
            loop {
                let mut app = db.appender(&ctx());

                for j in 0..100 {
                    app.append(0, &ls(&["foo", "bar", "a", &j.to_string()]), iter, iter as f64).unwrap();
                }
                commit(app.as_mut());

                if iter == 0 {
                    first_insert_tx.send(()).unwrap();
                }
                iter += 1;

                if stop.load(Ordering::SeqCst) {
                    return;
                }
            }
        })
    };

    first_insert_rx.recv().unwrap();

    // This is a race condition, so do a few tests to tickle it.
    // Usually most will fail.
    let mut inconsistencies = 0;
    for _ in 0..10 {
        let mut querier = db.querier(0, 1000000).unwrap();
        {
            let mut ss = querier.select(&ctx(), false, None, &[eq("foo", "bar")]);
            let (_, series_set, ws, err) = expand_series_set(ss.as_mut());
            assert!(err.is_none());
            assert!(ws.is_empty());

            let mut values = std::collections::HashSet::new();
            for series in series_set.values() {
                values.insert(series[series.len() - 1].1.to_bits());
            }
            if values.len() != 1 {
                inconsistencies += 1;
            }
        }
        querier.close();
    }
    stop.store(true, Ordering::SeqCst);
    writer.join().unwrap();

    assert_eq!(0, inconsistencies, "Some queries saw inconsistent results.");
}

#[test]
fn test_db_query_doesnt_see_appends_after_creation() {
    // Go skips the test when isolation is disabled (defaultIsolationDisabled); it is enabled here.
    let db = new_test_db(vec![]);
    let mut querier_before_add = db.querier(0, 1000000).unwrap();

    let mut app = db.appender(&ctx());
    app.append(0, &ls(&["foo", "bar"]), 0, 0.0).unwrap();

    let mut querier_after_add_but_before_commit = db.querier(0, 1000000).unwrap();

    let expand = |q: &dyn storage::Querier| {
        let mut ss = q.select(&ctx(), false, None, &[eq("foo", "bar")]);
        let (_, series_set, ws, err) = expand_series_set(ss.as_mut());
        assert!(err.is_none());
        assert!(ws.is_empty());
        series_set
    };

    // None of the queriers should return anything after the Add but before the commit.
    assert_eq!(BTreeMap::new(), expand(querier_before_add.as_ref()));
    assert_eq!(BTreeMap::new(), expand(querier_after_add_but_before_commit.as_ref()));

    // This commit is after the queriers are created, so should not be returned.
    commit(app.as_mut());

    // Nothing returned for querier created before the Add.
    assert_eq!(BTreeMap::new(), expand(querier_before_add.as_ref()));

    // Series exists but has no samples for querier created after Add.
    assert_eq!(
        BTreeMap::from([(r#"{foo="bar"}"#.to_string(), Vec::new())]),
        expand(querier_after_add_but_before_commit.as_ref())
    );

    let mut querier_after_commit = db.querier(0, 1000000).unwrap();

    // Samples are returned for querier created after Commit.
    assert_eq!(
        BTreeMap::from([(r#"{foo="bar"}"#.to_string(), vec![(0, 0.0)])]),
        expand(querier_after_commit.as_ref())
    );
    querier_after_commit.close();
    querier_after_add_but_before_commit.close();
    querier_before_add.close();
}

fn assure_chunk_from_samples(samples: &[Sample]) -> Meta {
    storage::chunks::chunk_from_samples(samples).unwrap()
}

/// The length of the uvarint encoding of `x` (Go `binary.PutUvarint`).
fn uvarint_len(mut x: u64) -> usize {
    let mut n = 1;
    while x >= 0x80 {
        x >>= 7;
        n += 1;
    }
    n
}

// TestChunkWriter_ReadAfterWrite ensures that chunk segment are cut at the set segment size and that the
// resulted segments includes the expected chunks data.
#[test]
fn test_chunk_writer_read_after_write() {
    let chk1 = assure_chunk_from_samples(&[fs(1, 1.0)]);
    let chk2 = assure_chunk_from_samples(&[fs(1, 2.0)]);
    let chk3 = assure_chunk_from_samples(&[fs(1, 3.0)]);
    let chk4 = assure_chunk_from_samples(&[fs(1, 4.0)]);
    let chk5 = assure_chunk_from_samples(&[fs(1, 5.0)]);
    let chunk_size = chk1.chunk.as_ref().unwrap().bytes().len()
        + chunks::MAX_CHUNK_LENGTH_FIELD_SIZE
        + chunks::CHUNK_ENCODING_SIZE
        + 4; // crc32.Size

    struct Case {
        chks: Vec<Vec<Meta>>,
        segment_size: usize,
        exp_segments_count: usize,
        exp_segment_sizes: Vec<usize>,
    }
    let c = |v: &[&Meta]| -> Vec<Meta> { v.iter().map(|m| (*m).clone()).collect() };
    let tests = vec![
        // 0:Last chunk ends at the segment boundary so all chunks should fit in a single segment.
        Case {
            chks: vec![c(&[&chk1, &chk2, &chk3])],
            segment_size: 3 * chunk_size,
            exp_segment_sizes: vec![3 * chunk_size],
            exp_segments_count: 1,
        },
        // 1:Two chunks can fit in a single segment so the last one should result in a new segment.
        Case {
            chks: vec![c(&[&chk1, &chk2, &chk3, &chk4, &chk5])],
            segment_size: 2 * chunk_size,
            exp_segment_sizes: vec![2 * chunk_size, 2 * chunk_size, chunk_size],
            exp_segments_count: 3,
        },
        // 2:When the segment size is smaller than the size of 2 chunks the last segment should still create a
        // new segment.
        Case {
            chks: vec![c(&[&chk1, &chk2, &chk3])],
            segment_size: 2 * chunk_size - 1,
            exp_segment_sizes: vec![chunk_size, chunk_size, chunk_size],
            exp_segments_count: 3,
        },
        // 3:When the segment is smaller than a single chunk it should still be written by ignoring the max
        // segment size.
        Case {
            chks: vec![c(&[&chk1])],
            segment_size: chunk_size - 1,
            exp_segment_sizes: vec![chunk_size],
            exp_segments_count: 1,
        },
        // 4:All chunks are bigger than the max segment size, but these should still be written even when this
        // will result in bigger segment than the set size. Each segment will hold a single chunk.
        Case {
            chks: vec![c(&[&chk1, &chk2, &chk3])],
            segment_size: 1,
            exp_segment_sizes: vec![chunk_size, chunk_size, chunk_size],
            exp_segments_count: 3,
        },
        // 5:Adding multiple batches of chunks.
        Case {
            chks: vec![c(&[&chk1, &chk2, &chk3]), c(&[&chk4, &chk5])],
            segment_size: 3 * chunk_size,
            exp_segment_sizes: vec![3 * chunk_size, 2 * chunk_size],
            exp_segments_count: 2,
        },
        // 6:Adding multiple batches of chunks.
        Case {
            chks: vec![c(&[&chk1]), c(&[&chk2, &chk3]), c(&[&chk4])],
            segment_size: 2 * chunk_size,
            exp_segment_sizes: vec![2 * chunk_size, 2 * chunk_size],
            exp_segments_count: 2,
        },
    ];

    for (i, mut test) in tests.into_iter().enumerate() {
        let temp_dir = TempDir::new();

        let mut chunkw = chunks::new_writer(
            temp_dir.path(),
            &[chunks::with_segment_size((chunks::SEGMENT_HEADER_SIZE + test.segment_size) as i64)],
        )
        .unwrap();

        for chks in &mut test.chks {
            chunkw.write_chunks(chks).unwrap();
        }
        chunkw.close().unwrap();

        let files = file_names(temp_dir.path());
        assert_eq!(files.len(), test.exp_segments_count, "case {i}: expected segments count mismatch");

        // Verify that all data is written to the segments.
        let mut size_exp = 0;
        let mut size_act = 0;

        for chks in &test.chks {
            for chk in chks {
                let n = chk.chunk.as_ref().unwrap().bytes().len();
                size_exp += uvarint_len(n as u64); // The length field.
                size_exp += chunks::CHUNK_ENCODING_SIZE;
                size_exp += n; // The data itself.
                size_exp += 4; // The 4 bytes of crc32
            }
        }
        size_exp += test.exp_segments_count * chunks::SEGMENT_HEADER_SIZE; // The segment header bytes.

        for (j, f) in files.iter().enumerate() {
            let size = std::fs::metadata(temp_dir.path().join(f)).unwrap().len() as usize;
            // Verify that the segment is the same or smaller than the expected size.
            assert!(
                chunks::SEGMENT_HEADER_SIZE + test.exp_segment_sizes[j] >= size,
                "Segment:{} should NOT be bigger than:{} actual:{}",
                j,
                chunks::SEGMENT_HEADER_SIZE + test.exp_segment_sizes[j],
                size
            );

            size_act += size;
        }
        assert_eq!(size_exp, size_act, "case {i}");

        // Check the content of the chunks.
        let r = chunks::new_dir_reader(temp_dir.path(), None).unwrap();

        for chks in &test.chks {
            for chk_exp in chks {
                // adapted: the Rust block chunk reader returns the chunk only (never an iterable), so Go's
                // `require.Nil(t, iterable)` holds by type.
                let chk_act = r.chunk_or_iterable(chk_exp).unwrap();
                assert_eq!(chk_exp.chunk.as_ref().unwrap().bytes(), chk_act.bytes());
            }
        }
        r.close().unwrap();
    }
}
