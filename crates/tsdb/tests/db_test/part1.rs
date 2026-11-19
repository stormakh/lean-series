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
    LabelQuerier, Meta, Querier, Sample, SeriesSet,
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
use tsdb_format::tombstones::{self, Interval, MemTombstones};
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
