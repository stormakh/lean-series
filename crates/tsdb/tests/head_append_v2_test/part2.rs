//! Port of `tsdb/head_append_v2_test.go` lines 2832..end.
//!
//! Not ported yet (they need the DB, `db.rs`, which is not merged yet):
//! - TestHeadAppenderV2_Append_DifferentEncodingSameSeries (`Open`, `db.AppenderV2`, `db.Querier`).
//! - TestHeadAppenderV2_ExemplarAppenderV2 (its last part checks `(*DB).AppenderV2`).
//!
//! adapted (all tests): Go changes `head.opts` fields that are plain bools (`EnableMemorySnapshotOnShutdown`)
//! on a live head and passes `head.opts` to the next `NewHead`. Here the options are moved into the head and
//! the bools are not atomics, so each restart builds a copy of the options ([`copy_opts`]) with the changed
//! field. The heads see the same values at the same points (the field is read only on `Init` and `Close`).

use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use chunkenc::{AnyIterator, Appender as ChunkAppender, Chunk, ChunkIterator, Encoding, ValueType, XorChunk};
use chunkenc::{CounterResetHeader, FloatHistogramChunk, HistogramChunk};
use model::exemplar::Exemplar;
use model::histogram::{CounterResetHint, FloatHistogram, Histogram};
use model::labels::{self, Labels, ScratchBuilder};
use model::matcher::{MatchType, Matcher};
use model::value::STALE_NAN;
use storage::tsdbutil as tu;
use storage::{AOptions, AppenderV2, Context, Error, Sample, SeriesRef};
use tsdb::head::{Head, HeadOptions, MmappedChunk};
use tsdb::head_wal::last_chunk_snapshot;
use tsdb::querier::new_block_querier;
use tsdb::reader::{ChunkOrIterable, ChunkReader, IndexReader};
use tsdb_format::chunks::ChunkDiskMapperRef;
use tsdb_format::record::{
    self, Decoder, RefExemplar, RefFloatHistogramSample, RefHistogramSample, RefMetadata, RefSample, RefSeries,
};
use tsdb_format::tombstones::Stone;
use tsdb_format::wlog::{self, compression, WL};

use crate::common::{
    fs, new_head_no_wal, new_test_head, new_test_head_with_options, query, sample_type_scenarios, SampleTypeScenario,
    TempDir, DEFAULT_BLOCK_DURATION, FLOAT,
};

/// Go `time.Minute.Milliseconds()`.
const MINUTE: i64 = 60 * 1000;
/// Go `config.DefaultExemplarsConfig.MaxExemplars`.
const DEFAULT_MAX_EXEMPLARS: i64 = 100000;

// ---------------------------------------------------------------------------------------------------------------
// Helpers.

fn ctx() -> Context {
    Context::background()
}

fn ls(kv: &[&str]) -> Labels {
    labels::from_strings(kv)
}

fn eq(name: &str, value: &str) -> Matcher {
    Matcher::must_new(MatchType::Equal, name, value)
}

fn stale() -> f64 {
    f64::from_bits(STALE_NAN)
}

/// Go `wlog.NewSize(nil, nil, dir, 32768, compress)`.
fn new_wl(dir: impl AsRef<Path>, compress: compression::Type) -> Arc<WL> {
    Arc::new(WL::new_size(dir, 32768, compress).expect("wal"))
}

/// A copy of the head options (Go reuses the `*HeadOptions` for the next head).
fn copy_opts(o: &HeadOptions) -> HeadOptions {
    HeadOptions {
        max_exemplars: o.max_exemplars.load(Ordering::SeqCst).into(),
        out_of_order_time_window: o.out_of_order_time_window.load(Ordering::SeqCst).into(),
        out_of_order_cap_max: o.out_of_order_cap_max.load(Ordering::SeqCst).into(),
        enable_st_storage: o.enable_st_storage.load(Ordering::SeqCst).into(),
        float_chunk_encoding: o.float_chunk_encoding.load(Ordering::SeqCst).into(),
        enable_histogram_st_encoding: o.enable_histogram_st_encoding.load(Ordering::SeqCst).into(),
        chunk_range: o.chunk_range,
        chunk_dir_root: o.chunk_dir_root.clone(),
        chunk_pool: o.chunk_pool.clone(),
        chunk_write_buffer_size: o.chunk_write_buffer_size,
        chunk_write_queue_size: o.chunk_write_queue_size,
        samples_per_chunk: o.samples_per_chunk,
        stripe_size: o.stripe_size,
        series_callback: o.series_callback.clone(),
        enable_exemplar_storage: o.enable_exemplar_storage,
        enable_memory_snapshot_on_shutdown: std::sync::atomic::AtomicBool::new(o.enable_memory_snapshot_on_shutdown.load(std::sync::atomic::Ordering::Relaxed)),
        isolation_disabled: o.isolation_disabled,
        wal_replay_concurrency: o.wal_replay_concurrency,
        enable_sharding: o.enable_sharding,
        enable_st_as_zero_sample: o.enable_st_as_zero_sample,
        enable_metadata_wal_records: o.enable_metadata_wal_records,
        enable_fast_startup: o.enable_fast_startup,
        logger: o.logger.clone(),
    }
}

/// Go `newTestHeadDefaultOptions`.
fn new_test_head_default_options(chunk_range: i64, ooo_enabled: bool) -> HeadOptions {
    let opts = HeadOptions {
        chunk_range,
        enable_exemplar_storage: true,
        ..HeadOptions::default()
    };
    opts.max_exemplars.store(DEFAULT_MAX_EXEMPLARS, Ordering::SeqCst);
    if ooo_enabled {
        opts.out_of_order_time_window.store(10 * MINUTE, Ordering::SeqCst);
    }
    opts
}

/// Go `scenario.appendFunc(storage.AppenderV2AsLimitedV1(app), ...)`: the V1 call through the V2 appender with
/// start timestamp 0 and default options (what `AppenderV2AsLimitedV1` does).
fn scenario_append(
    sc: &SampleTypeScenario,
    app: &mut dyn AppenderV2,
    lbls: &Labels,
    ts: i64,
    value: i64,
) -> (Result<SeriesRef, Error>, Sample) {
    let s = (sc.sample_func)(ts, value);
    let o = AOptions::default();
    let r = if let Some(h) = &s.h {
        app.append(0, lbls, 0, ts, 0.0, Some(h), None, &o)
    } else if let Some(fh) = &s.fh {
        app.append(0, lbls, 0, ts, 0.0, None, Some(fh), &o)
    } else {
        app.append(0, lbls, 0, ts, s.f, None, None, &o)
    };
    (r, s)
}

/// One decoded WAL record (Go `readTestWAL` returns `[]any`).
#[derive(Clone, Debug, PartialEq)]
enum Rec {
    Series(Vec<RefSeries>),
    Samples(Vec<RefSample>),
    Histograms(Vec<RefHistogramSample>),
    FloatHistograms(Vec<RefFloatHistogramSample>),
    Tombstones(Vec<Stone>),
    Metadata(Vec<RefMetadata>),
    Exemplars(Vec<RefExemplar>),
}

/// Go `readTestWAL` (head_test.go).
fn read_test_wal(dir: &Path) -> Vec<Rec> {
    let sr = wlog::new_segments_reader(dir).expect("segments reader");
    let mut dec = Decoder::new(record::Logger::nop());
    let mut r = wlog::Reader::new(sr);
    let mut recs = Vec::new();
    while r.next() {
        let rec = r.record().to_vec();
        match dec.r#type(&rec) {
            record::Type::Series => {
                let mut v = Vec::new();
                dec.series(&rec, &mut v).expect("series");
                recs.push(Rec::Series(v));
            }
            record::Type::Samples | record::Type::SamplesV2 => {
                let mut v = Vec::new();
                dec.samples(&rec, &mut v).expect("samples");
                recs.push(Rec::Samples(v));
            }
            record::Type::HistogramSamples
            | record::Type::CustomBucketsHistogramSamples
            | record::Type::HistogramSamplesV2 => {
                let mut v = Vec::new();
                dec.histogram_samples(&rec, &mut v).expect("histograms");
                recs.push(Rec::Histograms(v));
            }
            record::Type::FloatHistogramSamples
            | record::Type::CustomBucketsFloatHistogramSamples
            | record::Type::FloatHistogramSamplesV2 => {
                let mut v = Vec::new();
                dec.float_histogram_samples(&rec, &mut v).expect("float histograms");
                recs.push(Rec::FloatHistograms(v));
            }
            record::Type::Tombstones => {
                let mut v = Vec::new();
                dec.tombstones(&rec, &mut v).expect("tombstones");
                recs.push(Rec::Tombstones(v));
            }
            record::Type::Metadata => {
                let mut v = Vec::new();
                dec.metadata(&rec, &mut v).expect("metadata");
                recs.push(Rec::Metadata(v));
            }
            record::Type::Exemplars => {
                let mut v = Vec::new();
                dec.exemplars(&rec, &mut v).expect("exemplars");
                recs.push(Rec::Exemplars(v));
            }
            record::Type::MinValidTime => {
                dec.min_valid_time(&rec).expect("min valid time");
            }
            t => panic!("unknown record type {:?}", t),
        }
    }
    assert!(r.err().is_none(), "{:?}", r.err());
    recs
}

fn sample_type_name(s: &Sample) -> &'static str {
    if s.h.is_some() {
        "histogram"
    } else if s.fh.is_some() {
        "floathistogram"
    } else {
        "float"
    }
}

/// Go `requireEqualSamples` (testutil.go) with `requireEqualSamplesIgnoreCounterResets` as a flag.
fn require_equal_samples(name: &str, expected: &[Sample], actual: &[Sample], ignore_counter_resets: bool) {
    assert_eq!(expected.len(), actual.len(), "Length not equal to expected for {name}");
    for (i, (e, a)) in expected.iter().zip(actual).enumerate() {
        assert_eq!(e.t, a.t, "Different timestamps for {name}[{i}]");
        assert_eq!(sample_type_name(e), sample_type_name(a), "Different types for {name}[{i}] at ts {}", e.t);
        if let Some(eh) = &e.h {
            let mut eh = (**eh).clone();
            let mut ah = (**a.h.as_ref().unwrap()).clone();
            if ignore_counter_resets && eh.counter_reset_hint != CounterResetHint::GaugeType {
                eh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                ah.counter_reset_hint = CounterResetHint::UnknownCounterReset;
            } else {
                assert_eq!(eh.counter_reset_hint, ah.counter_reset_hint, "Sample header doesn't match for {name}[{i}]");
            }
            assert_eq!(eh, ah, "Sample doesn't match for {name}[{i}] at ts {}", e.t);
        } else if let Some(efh) = &e.fh {
            let mut efh = (**efh).clone();
            let mut afh = (**a.fh.as_ref().unwrap()).clone();
            if ignore_counter_resets {
                efh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                afh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
            } else {
                assert_eq!(efh.counter_reset_hint, afh.counter_reset_hint, "Sample header doesn't match for {name}[{i}]");
            }
            assert_eq!(efh, afh, "Sample doesn't match for {name}[{i}] at ts {}", e.t);
        } else {
            assert_eq!(e.f.to_bits(), a.f.to_bits(), "Sample doesn't match for {name}[{i}] at ts {}", e.t);
        }
    }
}

fn expand(c: &dyn Chunk) -> Vec<Sample> {
    let mut it = c.iterator(None);
    storage::expand_samples(&mut it).expect("expand samples")
}

/// The chunk metas of the series `foo="bar"` of the head (Go `idxReader.Postings` + `Series`).
fn series_chunk_metas(h: &Head, name: &str, value: &str, metas: &mut Vec<storage::Meta>) {
    let ir = h.index();
    let mut p = ir.postings(&ctx(), name, &[value]).expect("postings");
    let mut b = ScratchBuilder::new(0);
    assert!(p.next());
    let s_ref = p.at();
    ir.series(s_ref, &mut b, metas).expect("series");
}

fn chunk_of(cr: &dyn ChunkReader, m: &storage::Meta) -> Arc<dyn Chunk> {
    match cr.chunk_or_iterable(m).expect("chunk") {
        ChunkOrIterable::Chunk(c) => c,
        ChunkOrIterable::Iterable(_) => panic!("iterable must be nil"),
    }
}

// ---------------------------------------------------------------------------------------------------------------

#[test]
fn test_chunk_snapshot_taken_after_incomplete_snapshot_appender_v2() {
    let dir = TempDir::new();
    let wl_temp = new_wl(dir.path().join("wal"), compression::Type::Snappy);

    // Write a snapshot with .tmp suffix. This used to fail taking any further snapshots or replay of snapshots.
    let snapshot_name = tsdb::head_wal::chunk_snapshot_dir(0, 100) + ".tmp";
    let cpdir = dir.path().join(snapshot_name);
    std::fs::create_dir_all(&cpdir).unwrap();

    let opts = HeadOptions {
        chunk_dir_root: dir.path().to_path_buf(),
        enable_memory_snapshot_on_shutdown: std::sync::atomic::AtomicBool::new(true),
        ..HeadOptions::default()
    };
    let head = Head::new(Some(wl_temp), None, opts, None).expect("new head");
    head.init(i64::MIN).expect("init");

    assert_eq!(0.0, head.metrics.snapshot_replay_error_total.get());

    // Add some samples for the snapshot.
    let mut app = head.appender_v2(&ctx());
    app.append(0, &ls(&["foo", "bar"]), 0, 10, 10.0, None, None, &AOptions::default()).expect("append");
    assert!(app.commit().is_none());

    // Should not return any error for a successful snapshot.
    head.close().expect("close");

    // Verify the snapshot.
    let (name, idx, offset) = last_chunk_snapshot(dir.path()).expect("last snapshot");
    assert!(!name.as_os_str().is_empty());
    assert_eq!(0, idx);
    assert!(offset > 0);
}

/// TestWBLReplay_AppenderV2 checks the replay at a low level.
#[test]
fn test_wbl_replay_appender_v2() {
    for (name, scenario) in sample_type_scenarios() {
        for enable_st_storage in [false, true] {
            eprintln!("{name}/st-storage={enable_st_storage}");
            wbl_replay_appender_v2(&scenario, enable_st_storage);
        }
    }
}

fn wbl_opts(dir: &Path, enable_st_storage: bool) -> HeadOptions {
    let opts = HeadOptions {
        chunk_range: 1000,
        chunk_dir_root: dir.to_path_buf(),
        ..HeadOptions::default()
    };
    opts.out_of_order_time_window.store(30 * MINUTE, Ordering::SeqCst);
    opts.enable_st_storage.store(enable_st_storage, Ordering::SeqCst);
    if enable_st_storage {
        opts.float_chunk_encoding.store(chunkenc::ENC_XOR2.0 as u32, Ordering::SeqCst);
    } else {
        opts.float_chunk_encoding.store(chunkenc::ENC_XOR.0 as u32, Ordering::SeqCst);
    }
    opts.enable_histogram_st_encoding.store(enable_st_storage, Ordering::SeqCst);
    opts
}

fn wbl_replay_appender_v2(scenario: &SampleTypeScenario, enable_st_storage: bool) {
    let dir = TempDir::new();
    let wal = new_wl(dir.path().join("wal"), compression::Type::Snappy);
    let ooo_wlog = new_wl(dir.path().join(wlog::WBL_DIR_NAME), compression::Type::Snappy);

    let opts = wbl_opts(dir.path(), enable_st_storage);
    let h = Head::new(Some(wal), Some(ooo_wlog), opts, None).expect("new head");
    h.init(0).expect("init");

    let mut exp_ooo_samples: Vec<Sample> = Vec::new();
    let l = ls(&["foo", "bar"]);
    let mut append_sample = |mins: i64, _v: f64, is_ooo: bool| {
        let mut app = h.appender_v2(&ctx());
        let (r, s) = scenario_append(scenario, app.as_mut(), &l, mins * MINUTE, mins);
        r.expect("append");
        assert!(app.commit().is_none());
        if is_ooo {
            exp_ooo_samples.push(s);
        }
    };

    // In-order sample.
    append_sample(60, 60.0, false);

    // Out of order samples.
    append_sample(40, 40.0, true);
    append_sample(35, 35.0, true);
    append_sample(50, 50.0, true);
    append_sample(55, 55.0, true);
    append_sample(59, 59.0, true);
    append_sample(31, 31.0, true);

    // Check that Head's time ranges are set properly.
    assert_eq!(60 * MINUTE, h.min_time());
    assert_eq!(60 * MINUTE, h.max_time());
    assert_eq!(31 * MINUTE, h.min_ooo_time());
    assert_eq!(59 * MINUTE, h.max_ooo_time());

    // Restart head.
    h.close().expect("close");
    let wal = new_wl(dir.path().join("wal"), compression::Type::Snappy);
    let ooo_wlog = new_wl(dir.path().join(wlog::WBL_DIR_NAME), compression::Type::Snappy);
    let h = Head::new(Some(wal), Some(ooo_wlog), wbl_opts(dir.path(), enable_st_storage), None).expect("new head");
    h.init(0).expect("init"); // Replay happens here.

    // Get the ooo samples from the Head.
    let (ms, ok) = h.get_or_create(l.hash(), &l, false).expect("get or create");
    assert!(!ok);

    let chks = {
        let mut g = ms.lock();
        let ooo = g.ooo_mut().expect("ooo fields");
        let hc = ooo.ooo_head_chunk.as_mut().expect("ooo head chunk");
        hc.chunk
            .to_encoded_chunks(
                i64::MIN,
                i64::MAX,
                h.opts.use_xor2_float_encoding(),
                h.opts.enable_histogram_st_encoding.load(Ordering::SeqCst),
            )
            .expect("encoded chunks")
    };
    assert_eq!(1, chks.len());

    let act_ooo_samples = expand(chks[0].chunk.as_ref());

    // OOO chunk will be sorted. Hence sort the expected samples.
    exp_ooo_samples.sort_by_key(|s| s.t);

    // Passing in true for the 'ignoreCounterResets' parameter prevents differences in counter reset headers
    // from being factored in to the sample comparison.
    require_equal_samples(&l.to_string(), &exp_ooo_samples, &act_ooo_samples, true);

    h.close().expect("close");
}

/// TestOOOMmapReplay_AppenderV2 checks the replay at a low level.
#[test]
fn test_ooo_mmap_replay_appender_v2() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("{name}");
        ooo_mmap_replay_appender_v2(&scenario);
    }
}

fn ooo_mmap_opts(dir: &Path) -> HeadOptions {
    let opts = HeadOptions {
        chunk_range: 1000,
        chunk_dir_root: dir.to_path_buf(),
        ..HeadOptions::default()
    };
    opts.out_of_order_cap_max.store(30, Ordering::SeqCst);
    opts.out_of_order_time_window.store(1000 * MINUTE, Ordering::SeqCst);
    opts
}

fn ooo_mmap_replay_appender_v2(scenario: &SampleTypeScenario) {
    let dir = TempDir::new();
    let wal = new_wl(dir.path().join("wal"), compression::Type::Snappy);
    let ooo_wlog = new_wl(dir.path().join(wlog::WBL_DIR_NAME), compression::Type::Snappy);

    let h = Head::new(Some(wal), Some(ooo_wlog), ooo_mmap_opts(dir.path()), None).expect("new head");
    h.init(0).expect("init");

    let l = ls(&["foo", "bar"]);
    let append_sample = |mins: i64| {
        let mut app = h.appender_v2(&ctx());
        let (r, _) = scenario_append(scenario, app.as_mut(), &l, mins * MINUTE, mins);
        r.expect("append");
        assert!(app.commit().is_none());
    };

    // In-order sample.
    append_sample(200);

    // Out of order samples. 92 samples to create 3 m-map chunks.
    for mins in 100..=191 {
        append_sample(mins);
    }

    let (ms, ok) = h.get_or_create(l.hash(), &l, false).expect("get or create");
    assert!(!ok);

    let exp_mmap_chunks: Vec<MmappedChunk> = {
        let g = ms.lock();
        let chunks = &g.ooo().expect("ooo").ooo_mmapped_chunks;
        assert_eq!(3, chunks.len());
        // Verify that we can access the chunks without error.
        for m in chunks {
            let chk = h.chunk_disk_mapper.chunk(m.r#ref).expect("chunk");
            assert_eq!(m.num_samples as usize, chk.num_samples());
        }
        chunks.clone()
    };

    // Restart head.
    h.close().expect("close");

    let wal = new_wl(dir.path().join("wal"), compression::Type::Snappy);
    let ooo_wlog = new_wl(dir.path().join(wlog::WBL_DIR_NAME), compression::Type::Snappy);
    let h = Head::new(Some(wal), Some(ooo_wlog), ooo_mmap_opts(dir.path()), None).expect("new head");
    h.init(0).expect("init"); // Replay happens here.

    // Get the mmap chunks from the Head.
    let (ms, ok) = h.get_or_create(l.hash(), &l, false).expect("get or create");
    assert!(!ok);

    let act_mmap_chunks: Vec<MmappedChunk> = {
        let g = ms.lock();
        let chunks = &g.ooo().expect("ooo").ooo_mmapped_chunks;
        assert_eq!(exp_mmap_chunks.len(), chunks.len());
        // Verify that we can access the chunks without error.
        for m in chunks {
            let chk = h.chunk_disk_mapper.chunk(m.r#ref).expect("chunk");
            assert_eq!(m.num_samples as usize, chk.num_samples());
        }
        chunks.clone()
    };

    assert_eq!(exp_mmap_chunks, act_mmap_chunks);

    h.close().expect("close");
}

/// Go `UnsupportedMask`.
const UNSUPPORTED_MASK: u8 = 0b1000_0000;
/// Go `EncUnsupportedXOR`.
const ENC_UNSUPPORTED_XOR: Encoding = Encoding(chunkenc::ENC_XOR.0 | UNSUPPORTED_MASK);

/// Go `unsupportedChunk`: holds a XOR chunk and overrides the encoding.
#[derive(Debug)]
struct UnsupportedChunk(XorChunk);

impl Chunk for UnsupportedChunk {
    fn bytes(&self) -> &[u8] {
        self.0.bytes()
    }
    fn encoding(&self) -> Encoding {
        ENC_UNSUPPORTED_XOR
    }
    fn appender(&mut self) -> Result<ChunkAppender, chunkenc::Error> {
        Chunk::appender(&mut self.0)
    }
    fn iterator<'a>(&'a self, it: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        Chunk::iterator(&self.0, it)
    }
    fn num_samples(&self) -> usize {
        self.0.num_samples()
    }
    fn compact(&mut self) {
        self.0.compact()
    }
    fn reset(&mut self, stream: Vec<u8>) {
        self.0.reset(stream)
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[test]
fn test_head_init_discard_chunks_with_unsupported_encoding() {
    let mut th = new_test_head("discard-unsupported", 1000, compression::Type::None, false);
    let h = th.head.clone();

    h.init(0).expect("init");

    let mut app = h.appender_v2(&ctx());
    let series_labels = ls(&["a", "1"]);
    let mut series_ref: SeriesRef = 0;
    for i in 0..400 {
        series_ref = app
            .append(0, &series_labels, 0, i, i as f64, None, None, &AOptions::default())
            .expect("append");
    }
    assert!(app.commit().is_none());
    assert!(h.metrics.chunks_created.get() > 1.0);

    let uc = UnsupportedChunk(XorChunk::new());
    // Make this chunk not overlap with the previous and the next.
    h.chunk_disk_mapper.write_chunk(
        series_ref,
        500,
        600,
        Arc::new(uc),
        false,
        Some(Box::new(|err| assert!(err.is_ok(), "{:?}", err.err()))),
    );

    let mut app = h.appender_v2(&ctx());
    for i in 700..1200 {
        app.append(0, &series_labels, 0, i, i as f64, None, None, &AOptions::default()).expect("append");
    }
    assert!(app.commit().is_none());
    assert!(h.metrics.chunks_created.get() > 4.0);

    let (series, created) = h.get_or_create(series_labels.hash(), &series_labels, false).expect("get or create");
    assert!(!created, "should already exist");

    let exp_chunks: Vec<MmappedChunk> = {
        let mut g = series.lock();
        g.mmap_chunks(&h.chunk_disk_mapper, series.r#ref);
        g.mmapped_chunks.clone()
    };

    h.close().expect("close");

    let wal = new_wl(h.opts.chunk_dir_root.join("wal"), compression::Type::None);
    let h2 = Head::new(Some(wal.clone()), None, copy_opts(&h.opts), None).expect("new head");
    th.head = h2.clone();
    th.wal = Some(wal);
    h2.init(0).expect("init");

    let (series, created) = h2.get_or_create(series_labels.hash(), &series_labels, false).expect("get or create");
    assert!(!created, "should already exist");

    assert_eq!(exp_chunks, series.lock().mmapped_chunks);
}

/// Tests https://github.com/prometheus/prometheus/issues/10277.
#[test]
fn test_mmap_panic_after_mmap_replay_corruption_appender_v2() {
    let dir = TempDir::new();
    let wal = new_wl(dir.path().join("wal"), compression::Type::None);

    let mk_opts = || {
        let opts = HeadOptions {
            chunk_range: DEFAULT_BLOCK_DURATION,
            chunk_dir_root: dir.path().to_path_buf(),
            enable_exemplar_storage: true,
            ..HeadOptions::default()
        };
        opts.max_exemplars.store(DEFAULT_MAX_EXEMPLARS, Ordering::SeqCst);
        opts
    };

    let mut h = Head::new(Some(wal), None, mk_opts(), None).expect("new head");
    h.init(0).expect("init");

    let mut last_ts = 0i64;
    let mut r: SeriesRef = 0;
    let lbls = ls(&["__name__", "testing", "foo", "bar"]);
    let mut add_chunks = |h: &Arc<Head>| {
        let interval = DEFAULT_BLOCK_DURATION / (4 * 120);
        let mut app = h.appender_v2(&ctx());
        for i in 0..250 {
            // Go ignores the error here.
            if let Ok(nr) = app.append(r, &lbls, 0, last_ts, last_ts as f64, None, None, &AOptions::default()) {
                r = nr;
            } else {
                r = 0;
            }
            last_ts += interval;
            if i % 10 == 0 {
                assert!(app.commit().is_none());
                app = h.appender_v2(&ctx());
            }
        }
        assert!(app.commit().is_none());
    };

    add_chunks(&h);

    h.close().expect("close");
    let wal = new_wl(dir.path().join("wal"), compression::Type::None);

    let mmap_file_path = dir.path().join("chunks_head").join("000001");
    {
        use std::os::unix::fs::FileExt;
        let f = std::fs::OpenOptions::new().write(true).open(&mmap_file_path).expect("open");
        f.write_at(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], 17).expect("write");
    }

    h = Head::new(Some(wal), None, mk_opts(), None).expect("new head");
    h.init(0).expect("init");

    add_chunks(&h);

    h.close().expect("close");
}

/// Tests https://github.com/prometheus/prometheus/issues/10277.
#[test]
fn test_replay_after_mmap_replay_error_appender_v2() {
    let dir = TempDir::new();

    let open_head = || {
        let wal = new_wl(dir.path().join("wal"), compression::Type::None);
        let opts = HeadOptions {
            chunk_range: DEFAULT_BLOCK_DURATION,
            chunk_dir_root: dir.path().to_path_buf(),
            enable_memory_snapshot_on_shutdown: std::sync::atomic::AtomicBool::new(true),
            ..HeadOptions::default()
        };
        opts.max_exemplars.store(DEFAULT_MAX_EXEMPLARS, Ordering::SeqCst);
        let h = Head::new(Some(wal), None, opts, None).expect("new head");
        h.init(0).expect("init");
        h
    };

    let mut h = open_head();

    let itvl: i64 = 15 * 1000;
    let mut last_ts = 0i64;
    let lbls = ls(&["__name__", "testing", "foo", "bar"]);
    let mut exp_samples: Vec<Sample> = Vec::new();
    let mut add_samples = |h: &Arc<Head>, num_samples: usize| {
        let mut app = h.appender_v2(&ctx());
        let mut r: SeriesRef = 0;
        for i in 0..num_samples {
            let res = app.append(r, &lbls, 0, last_ts, last_ts as f64, None, None, &AOptions::default());
            exp_samples.push(fs(last_ts, last_ts as f64));
            r = res.expect("append");
            last_ts += itvl;
            if i % 10 == 0 {
                assert!(app.commit().is_none());
                app = h.appender_v2(&ctx());
            }
        }
        assert!(app.commit().is_none());
    };

    // Creating multiple m-map files.
    for i in 0..5 {
        add_samples(&h, 250);
        h.close().expect("close");
        if i != 4 {
            // Don't open head for the last iteration.
            h = open_head();
        }
    }

    let files = std::fs::read_dir(dir.path().join("chunks_head")).expect("read dir").count();
    assert_eq!(5, files);

    // Corrupt a m-map file.
    let mmap_file_path = dir.path().join("chunks_head").join("000002");
    {
        use std::os::unix::fs::FileExt;
        let f = std::fs::OpenOptions::new().write(true).open(&mmap_file_path).expect("open");
        f.write_at(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], 17).expect("write");
    }

    h = open_head();
    h.mmap_head_chunks();

    // There should be less m-map files due to corruption.
    let files = std::fs::read_dir(dir.path().join("chunks_head")).expect("read dir").count();
    assert_eq!(2, files);

    // Querying should not panic.
    let q = new_block_querier(&*h, 0, last_ts).expect("querier");
    let res = query(q, &[eq("__name__", "testing")]);
    let mut exp = std::collections::BTreeMap::new();
    exp.insert(lbls.to_string(), exp_samples);
    assert_eq!(exp, res);

    h.close().expect("close");
}

#[test]
fn test_head_appender_v2_append_ooo_with_no_series() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("{name}");
        head_appender_v2_append_ooo_with_no_series(&scenario);
    }
}

fn head_appender_v2_append_ooo_with_no_series(scenario: &SampleTypeScenario) {
    let dir = TempDir::new();
    let wal = new_wl(dir.path().join("wal"), compression::Type::Snappy);
    let ooo_wlog = new_wl(dir.path().join(wlog::WBL_DIR_NAME), compression::Type::Snappy);

    let opts = HeadOptions {
        chunk_dir_root: dir.path().to_path_buf(),
        ..HeadOptions::default()
    };
    opts.out_of_order_cap_max.store(30, Ordering::SeqCst);
    opts.out_of_order_time_window.store(120 * MINUTE, Ordering::SeqCst);

    let h = Head::new(Some(wal), Some(ooo_wlog), opts, None).expect("new head");
    h.init(0).expect("init");

    let append_sample = |lbls: &Labels, ts: i64| {
        let mut app = h.appender_v2(&ctx());
        let (r, _) = scenario_append(scenario, app.as_mut(), lbls, ts * MINUTE, ts);
        r.expect("append");
        assert!(app.commit().is_none());
    };

    let verify_ooo_samples = |lbls: &Labels, exp_samples: usize| {
        let (ms, created) = h.get_or_create(lbls.hash(), lbls, false).expect("get or create");
        assert!(!created);
        let g = ms.lock();
        assert!(g.head_chunks.is_empty());
        let hc = g.ooo().expect("ooo").ooo_head_chunk.as_ref().expect("ooo head chunk");
        assert_eq!(exp_samples, hc.chunk.num_samples());
    };

    let verify_in_order_samples = |lbls: &Labels, exp_samples: usize| {
        let (ms, created) = h.get_or_create(lbls.hash(), lbls, false).expect("get or create");
        assert!(!created);
        let g = ms.lock();
        assert!(g.ooo().is_none());
        let hc = g.head_chunk().expect("head chunk");
        assert_eq!(exp_samples, hc.chunk.num_samples());
    };

    let new_labels = |idx: usize| ls(&["foo", &idx.to_string()]);

    let s1 = new_labels(1);
    append_sample(&s1, 300); // At 300m.
    verify_in_order_samples(&s1, 1);

    // At 239m, the sample cannot be appended to in-order chunk since it is beyond the minValidTime. So it should
    // go in OOO chunk. Series does not exist for s2 yet.
    let s2 = new_labels(2);
    append_sample(&s2, 239); // OOO sample.
    verify_ooo_samples(&s2, 1);

    // Similar for 180m.
    let s3 = new_labels(3);
    append_sample(&s3, 180); // OOO sample.
    verify_ooo_samples(&s3, 1);

    // Now 179m is too old.
    let s4 = new_labels(4);
    let mut app = h.appender_v2(&ctx());
    let (r, _) = scenario_append(scenario, app.as_mut(), &s4, 179 * MINUTE, 179);
    assert_eq!(Err(storage::ERR_TOO_OLD_SAMPLE), r);
    assert!(app.rollback().is_none());
    verify_ooo_samples(&s3, 1);

    // Samples still go into in-order chunk for samples within appendable minValidTime.
    let s5 = new_labels(5);
    append_sample(&s5, 240);
    verify_in_order_samples(&s5, 1);

    h.close().expect("close");
}

#[test]
fn test_head_min_ooo_time_update_appender_v2() {
    for (name, scenario) in sample_type_scenarios() {
        eprintln!("{name}");
        if scenario.sample_type == FLOAT {
            head_min_ooo_time_update_appender_v2(&scenario);
        }
    }
}

fn head_min_ooo_time_update_appender_v2(scenario: &SampleTypeScenario) {
    let dir = TempDir::new();
    let wal = new_wl(dir.path().join("wal"), compression::Type::Snappy);
    let ooo_wlog = new_wl(dir.path().join(wlog::WBL_DIR_NAME), compression::Type::Snappy);

    let opts = HeadOptions {
        chunk_dir_root: dir.path().to_path_buf(),
        ..HeadOptions::default()
    };
    opts.out_of_order_time_window.store(10 * MINUTE, Ordering::SeqCst);

    let h = Head::new(Some(wal), Some(ooo_wlog), opts, None).expect("new head");
    h.init(0).expect("init");

    let append_sample = |ts: i64| {
        let mut app = h.appender_v2(&ctx());
        let (r, _) = scenario_append(scenario, app.as_mut(), &ls(&["a", "b"]), ts * MINUTE, ts);
        r.expect("append");
        assert!(app.commit().is_none());
    };

    append_sample(300); // In-order sample.
    assert_eq!(i64::MAX, h.min_ooo_time());

    append_sample(295); // OOO sample.
    assert_eq!(295 * MINUTE, h.min_ooo_time());

    // Allowed window for OOO is >=290, which is before the earliest ooo sample 295, so it gets set to the lower
    // value.
    h.truncate_ooo(0, ChunkDiskMapperRef(1)).expect("truncate ooo");
    assert_eq!(290 * MINUTE, h.min_ooo_time());

    append_sample(310); // In-order sample.
    append_sample(305); // OOO sample.
    assert_eq!(290 * MINUTE, h.min_ooo_time());

    // Now the OOO sample 295 was not gc'ed yet. And allowed window for OOO is now >=300. So the lowest among
    // them, 295, is set as minOOOTime.
    h.truncate_ooo(0, ChunkDiskMapperRef(2)).expect("truncate ooo");
    assert_eq!(295 * MINUTE, h.min_ooo_time());

    h.close().expect("close");
}

fn histogram_chunk_header(c: &dyn Chunk) -> CounterResetHeader {
    c.as_any().downcast_ref::<HistogramChunk>().expect("histogram chunk").get_counter_reset_header()
}

fn float_histogram_chunk_header(c: &dyn Chunk) -> CounterResetHeader {
    c.as_any()
        .downcast_ref::<FloatHistogramChunk>()
        .expect("float histogram chunk")
        .get_counter_reset_header()
}

#[test]
fn test_gauge_histogram_wal_and_chunk_header_appender_v2() {
    let l = ls(&["a", "b"]);
    let mut th = new_test_head("gauge-hist-wal", 1000, compression::Type::None, false);
    th.head.init(0).expect("init");

    let mut ts = 0i64;
    let mut append_histogram = |head: &Arc<Head>, h: &Histogram| {
        ts += 1;
        let mut app = head.appender_v2(&ctx());
        app.append(0, &l, 0, ts, 0.0, Some(h), None, &AOptions::default()).expect("append");
        assert!(app.commit().is_none());
    };

    let mut hists = tu::generate_test_gauge_histograms(5);
    hists[0].counter_reset_hint = CounterResetHint::UnknownCounterReset;
    append_histogram(&th.head, &hists[0]);
    append_histogram(&th.head, &hists[1]);
    append_histogram(&th.head, &hists[2]);
    hists[3].counter_reset_hint = CounterResetHint::UnknownCounterReset;
    append_histogram(&th.head, &hists[3]);
    append_histogram(&th.head, &hists[3]);
    append_histogram(&th.head, &hists[4]);

    let check_headers = |head: &Arc<Head>| {
        head.mmap_head_chunks();
        let (ms, _) = head.get_or_create(l.hash(), &l, false).expect("get or create");
        let g = ms.lock();
        assert_eq!(3, g.mmapped_chunks.len());
        let exp_headers = [
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
            CounterResetHeader::GAUGE_TYPE,
            CounterResetHeader::NOT_COUNTER_RESET,
            CounterResetHeader::GAUGE_TYPE,
        ];
        for (i, m) in g.mmapped_chunks.iter().enumerate() {
            let chk = head.chunk_disk_mapper.chunk(m.r#ref).expect("chunk");
            assert_eq!(exp_headers[i], histogram_chunk_header(chk.as_ref()));
        }
        assert_eq!(exp_headers[exp_headers.len() - 1], histogram_chunk_header(g.head_chunk().unwrap().chunk.as_ref()));
    };
    check_headers(&th.head);

    let wal_dir = th.wal.as_ref().unwrap().dir().to_path_buf();
    let recs = read_test_wal(&wal_dir);
    let hs = |t: i64, h: &Histogram| {
        Rec::Histograms(vec![RefHistogramSample {
            r#ref: 1,
            st: 0,
            t,
            h: Arc::new(h.clone()),
        }])
    };
    assert_eq!(
        vec![
            Rec::Series(vec![RefSeries {
                r#ref: 1,
                labels: ls(&["a", "b"]),
            }]),
            hs(1, &hists[0]),
            hs(2, &hists[1]),
            hs(3, &hists[2]),
            hs(4, &hists[3]),
            hs(5, &hists[3]),
            hs(6, &hists[4]),
        ],
        recs
    );

    // Restart Head without mmap chunks to expect the WAL replay to recognize gauge histograms.
    th.head.close().expect("close");
    std::fs::remove_dir_all(tsdb::head::mmapped_chunks_dir(&th.head.opts.chunk_dir_root)).expect("remove");

    let w = new_wl(&wal_dir, compression::Type::None);
    let head = Head::new(Some(w.clone()), None, copy_opts(&th.head.opts), None).expect("new head");
    th.head = head.clone();
    th.wal = Some(w);
    head.init(0).expect("init");

    check_headers(&head);
}

#[test]
fn test_gauge_float_histogram_wal_and_chunk_header_appender_v2() {
    let l = ls(&["a", "b"]);
    let mut th = new_test_head("gauge-fhist-wal", 1000, compression::Type::None, false);
    th.head.init(0).expect("init");

    let mut ts = 0i64;
    let mut append_histogram = |head: &Arc<Head>, h: &FloatHistogram| {
        ts += 1;
        let mut app = head.appender_v2(&ctx());
        app.append(0, &l, 0, ts, 0.0, None, Some(h), &AOptions::default()).expect("append");
        assert!(app.commit().is_none());
    };

    let mut hists = tu::generate_test_gauge_float_histograms(5);
    hists[0].counter_reset_hint = CounterResetHint::UnknownCounterReset;
    append_histogram(&th.head, &hists[0]);
    append_histogram(&th.head, &hists[1]);
    append_histogram(&th.head, &hists[2]);
    hists[3].counter_reset_hint = CounterResetHint::UnknownCounterReset;
    append_histogram(&th.head, &hists[3]);
    append_histogram(&th.head, &hists[3]);
    append_histogram(&th.head, &hists[4]);

    let check_headers = |head: &Arc<Head>| {
        let (ms, _) = head.get_or_create(l.hash(), &l, false).expect("get or create");
        head.mmap_head_chunks();
        let g = ms.lock();
        assert_eq!(3, g.mmapped_chunks.len());
        let exp_headers = [
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
            CounterResetHeader::GAUGE_TYPE,
            CounterResetHeader::UNKNOWN_COUNTER_RESET,
            CounterResetHeader::GAUGE_TYPE,
        ];
        for (i, m) in g.mmapped_chunks.iter().enumerate() {
            let chk = head.chunk_disk_mapper.chunk(m.r#ref).expect("chunk");
            assert_eq!(exp_headers[i], float_histogram_chunk_header(chk.as_ref()));
        }
        assert_eq!(
            exp_headers[exp_headers.len() - 1],
            float_histogram_chunk_header(g.head_chunk().unwrap().chunk.as_ref())
        );
    };
    check_headers(&th.head);

    let wal_dir = th.wal.as_ref().unwrap().dir().to_path_buf();
    let recs = read_test_wal(&wal_dir);
    let fhs = |t: i64, fh: &FloatHistogram| {
        Rec::FloatHistograms(vec![RefFloatHistogramSample {
            r#ref: 1,
            st: 0,
            t,
            fh: Arc::new(fh.clone()),
        }])
    };
    assert_eq!(
        vec![
            Rec::Series(vec![RefSeries {
                r#ref: 1,
                labels: ls(&["a", "b"]),
            }]),
            fhs(1, &hists[0]),
            fhs(2, &hists[1]),
            fhs(3, &hists[2]),
            fhs(4, &hists[3]),
            fhs(5, &hists[3]),
            fhs(6, &hists[4]),
        ],
        recs
    );

    // Restart Head without mmap chunks to expect the WAL replay to recognize gauge histograms.
    th.head.close().expect("close");
    std::fs::remove_dir_all(tsdb::head::mmapped_chunks_dir(&th.head.opts.chunk_dir_root)).expect("remove");

    let w = new_wl(&wal_dir, compression::Type::None);
    let head = Head::new(Some(w.clone()), None, copy_opts(&th.head.opts), None).expect("new head");
    th.head = head.clone();
    th.wal = Some(w);
    head.init(0).expect("init");

    check_headers(&head);
}

#[test]
fn test_snapshot_ahead_of_wal_error_appender_v2() {
    // adapted: Go sets `head.opts.EnableMemorySnapshotOnShutdown = true` right after `newTestHead`; here the
    // head is made with the option set (it is read only on Init and Close).
    let mut opts = new_test_head_default_options(120 * 4, false);
    opts.enable_memory_snapshot_on_shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
    let mut th = new_test_head_with_options("snapshot-ahead", compression::Type::None, opts);
    let wal_dir = th.wal.as_ref().unwrap().dir().to_path_buf();
    let chunk_dir_root = th.head.opts.chunk_dir_root.clone();

    // Add a sample to fill WAL.
    let mut app = th.head.appender_v2(&ctx());
    app.append(0, &ls(&["foo", "bar"]), 0, 10, 10.0, None, None, &AOptions::default()).expect("append");
    assert!(app.commit().is_none());

    // Increment snapshot index to create sufficiently large difference.
    for _ in 0..2 {
        th.wal.as_ref().unwrap().next_segment().expect("next segment");
    }
    th.head.close().expect("close"); // This will create a snapshot.

    let (_, idx, _) = last_chunk_snapshot(&chunk_dir_root).expect("last snapshot");
    assert_eq!(2, idx);

    // Restart the WAL while keeping the old snapshot. The new head is created manually in this case in order to
    // keep using the same snapshot directory instead of a random one.
    std::fs::remove_dir_all(&wal_dir).expect("remove wal");
    let mut opts = copy_opts(&th.head.opts);
    opts.enable_memory_snapshot_on_shutdown.store(false, std::sync::atomic::Ordering::Relaxed);
    let w = new_wl(&wal_dir, compression::Type::None);
    let head = Head::new(Some(w.clone()), None, opts, None).expect("new head");
    th.head = head.clone();
    th.wal = Some(w.clone());
    // Add a sample to fill WAL.
    let mut app = head.appender_v2(&ctx());
    app.append(0, &ls(&["foo", "bar"]), 0, 10, 10.0, None, None, &AOptions::default()).expect("append");
    assert!(app.commit().is_none());
    let (last_segment, _) = w.last_segment_and_offset().expect("last segment");
    assert_eq!(0, last_segment);
    head.close().expect("close");

    // New WAL is saved, but old snapshot still exists.
    let (_, idx, _) = last_chunk_snapshot(&chunk_dir_root).expect("last snapshot");
    assert_eq!(2, idx);

    // Create new Head which should detect the incorrect index and delete the snapshot.
    let mut opts = copy_opts(&head.opts);
    opts.enable_memory_snapshot_on_shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
    let w = new_wl(&wal_dir, compression::Type::None);
    let head = Head::new(Some(w.clone()), None, opts, None).expect("new head");
    th.head = head.clone();
    th.wal = Some(w);
    head.init(i64::MIN).expect("init");

    // Verify that snapshot directory does not exist anymore.
    let err = last_chunk_snapshot(&chunk_dir_root).expect_err("no snapshot");
    assert_eq!(storage::ERR_NOT_FOUND, err);

    head.close().expect("close");
}

#[test]
fn test_cutting_new_head_chunks_appender_v2() {
    struct Case {
        name: &'static str,
        num_total_samples: usize,
        timestamp_jitter: bool,
        float_val_func: Option<fn(usize) -> f64>,
        hists: Option<Vec<Histogram>>,
        expected_chks: Vec<(usize, usize)>,
    }
    let cases = vec![
        Case {
            name: "float samples",
            num_total_samples: 180,
            timestamp_jitter: false,
            float_val_func: Some(|_| 1.0),
            hists: None,
            expected_chks: vec![(120, 46), (60, 32)],
        },
        Case {
            // Normally 120 samples would fit into a single chunk but these chunks violate the 1005 byte soft cap.
            name: "large float samples",
            num_total_samples: 120,
            timestamp_jitter: true,
            float_val_func: Some(|i| {
                // Flipping between these two make each sample val take at least 64 bits.
                let vals = [f64::MAX, 0.0];
                vals[i % vals.len()]
            }),
            hists: None,
            expected_chks: vec![(99, 1008), (21, 219)],
        },
        Case {
            name: "small histograms",
            num_total_samples: 240,
            timestamp_jitter: false,
            float_val_func: None,
            hists: Some(model::histogram::generate_big_test_histograms(240, 10)),
            expected_chks: vec![(120, 1087), (120, 1039)],
        },
        Case {
            name: "large histograms",
            num_total_samples: 240,
            timestamp_jitter: false,
            float_val_func: None,
            hists: Some(model::histogram::generate_big_test_histograms(240, 100)),
            expected_chks: vec![(40, 896), (40, 899), (40, 896), (30, 690), (30, 691), (30, 694), (30, 693)],
        },
        Case {
            // Really large histograms; each chunk can only contain a single histogram but we have a 10 sample
            // minimum per chunk.
            name: "really large histograms",
            num_total_samples: 11,
            timestamp_jitter: false,
            float_val_func: None,
            hists: Some(model::histogram::generate_big_test_histograms(11, 100000)),
            expected_chks: vec![(10, 200103), (1, 87540)],
        },
    ];
    for tc in cases {
        eprintln!("{}", tc.name);
        let h = new_test_head("cutting-chunks", DEFAULT_BLOCK_DURATION, compression::Type::None, false);

        let mut a = h.appender_v2(&ctx());

        let mut ts: i64 = 10000;
        let lbls = ls(&["foo", "bar"]);
        let jitter = [0i64, 1]; // A bit of jitter to prevent dod=0.

        for i in 0..tc.num_total_samples {
            if let Some(f) = tc.float_val_func {
                a.append(0, &lbls, 0, ts, f(i), None, None, &AOptions::default()).expect("append");
            } else if let Some(hists) = &tc.hists {
                a.append(0, &lbls, 0, ts, 0.0, Some(&hists[i]), None, &AOptions::default()).expect("append");
            }
            ts += 60 * 1000;
            if tc.timestamp_jitter {
                ts += jitter[i % jitter.len()];
            }
        }

        assert!(a.commit().is_none());

        let idx_reader = h.index();
        let chk_reader = h.chunks().expect("chunks");
        let mut p = idx_reader.postings(&ctx(), "foo", &["bar"]).expect("postings");
        let mut lbl_builder = ScratchBuilder::new(0);

        while p.next() {
            let s_ref = p.at();
            let mut chk_metas = Vec::with_capacity(tc.expected_chks.len());
            idx_reader.series(s_ref, &mut lbl_builder, &mut chk_metas).expect("series");
            assert_eq!(tc.expected_chks.len(), chk_metas.len(), "{}", tc.name);
            for (i, (num_samples, num_bytes)) in tc.expected_chks.iter().enumerate() {
                let chk = chunk_of(&chk_reader, &chk_metas[i]);
                assert_eq!(*num_samples, chk.num_samples(), "{} chunk {i}", tc.name);
                assert_eq!(*num_bytes, chk.bytes().len(), "{} chunk {i}", tc.name);
            }
        }
    }
}

/// TestHeadDetectsDuplicateSampleAtSizeLimit_AppenderV2 tests a regression where a duplicate sample is appended
/// to the head, right when the head chunk is at the size limit. The test adds all samples as duplicate, thus
/// expecting that the result has exactly half of the samples.
#[test]
fn test_head_detects_duplicate_sample_at_size_limit_appender_v2() {
    let num_samples = 1000usize;
    let base_ts: i64 = 1695209650;

    let h = new_test_head("dup-size-limit", DEFAULT_BLOCK_DURATION, compression::Type::None, false);

    let mut a = h.appender_v2(&ctx());
    // Use the worst case scenario for the XOR encoding. Otherwise we hit the sample limit before the size limit.
    let vals = [f64::MAX, 0.0];
    for i in 0..num_samples {
        let ts = base_ts + (i / 2) as i64 * 10000;
        let _ = a.append(0, &ls(&["foo", "bar"]), 0, ts, vals[(i / 2) % vals.len()], None, None, &AOptions::default());
        assert!(a.commit().is_none());
        a = h.appender_v2(&ctx());
    }
    drop(a);

    let index_reader = h.index();
    let mut chunks = Vec::new();
    let mut builder = ScratchBuilder::new(0);
    index_reader.series(1, &mut builder, &mut chunks).expect("series");

    let chunk_reader = h.chunks().expect("chunks");

    let mut stored_sample_count = 0;
    for chunk_meta in &chunks {
        stored_sample_count += chunk_of(&chunk_reader, chunk_meta).num_samples();
    }

    assert_eq!(num_samples / 2, stored_sample_count);
}

#[test]
fn test_wal_sample_and_exemplar_order_appender_v2() {
    let lbls = ls(&["foo", "bar"]);
    let ex = [Exemplar {
        value: 1.0,
        ts: 5,
        ..Default::default()
    }];
    #[derive(PartialEq, Debug)]
    enum Kind {
        Samples,
        Histograms,
        FloatHistograms,
    }
    let h1 = tu::generate_test_histogram(1);
    let fh1 = tu::generate_test_float_histogram(1);
    let cases: Vec<(&str, Option<&Histogram>, Option<&FloatHistogram>, Kind)> = vec![
        ("float sample", None, None, Kind::Samples),
        ("histogram sample", Some(&h1), None, Kind::Histograms),
        ("float histogram sample", None, Some(&fh1), Kind::FloatHistograms),
    ];
    for (name, h, fh, expected_type) in cases {
        eprintln!("{name}");
        let th = new_test_head("wal-order", 1000, compression::Type::None, false);

        let mut app = th.appender_v2(&ctx());
        let v = if h.is_none() && fh.is_none() { 1.0 } else { 0.0 };
        app.append(
            0,
            &lbls,
            0,
            10,
            v,
            h,
            fh,
            &AOptions {
                exemplars: &ex,
                ..Default::default()
            },
        )
        .expect("append");
        assert!(app.commit().is_none());

        let recs = read_test_wal(th.wal.as_ref().unwrap().dir());
        assert_eq!(3, recs.len());
        assert!(matches!(recs[0], Rec::Series(_)), "expected first record to be a RefSeries");
        let actual_type = match &recs[1] {
            Rec::Samples(_) => Kind::Samples,
            Rec::Histograms(_) => Kind::Histograms,
            Rec::FloatHistograms(_) => Kind::FloatHistograms,
            r => panic!("unexpected second record {:?}", r),
        };
        assert_eq!(expected_type, actual_type, "expected second record to be a {:?}", expected_type);
        assert!(matches!(recs[2], Rec::Exemplars(_)), "expected third record to be a RefExemplar");
    }
}

#[test]
fn test_head_appender_v2_append_float_with_same_timestamp_as_previous_histogram() {
    let head = new_test_head("float-same-ts-hist", DEFAULT_BLOCK_DURATION, compression::Type::None, false);

    let l = ls(&[labels::METRIC_NAME, "test"]);

    {
        // Append a float 10.0 @ 1_000.
        let mut app = head.appender_v2(&ctx());
        app.append(0, &l, 0, 1_000, 10.0, None, None, &AOptions::default()).expect("append");
        assert!(app.commit().is_none());
    }

    {
        // Append a float histogram @ 2_000.
        let mut app = head.appender_v2(&ctx());
        let h = tu::generate_test_histogram(1);
        app.append(0, &l, 0, 2_000, 0.0, Some(&h), None, &AOptions::default()).expect("append");
        assert!(app.commit().is_none());
    }

    let mut app = head.appender_v2(&ctx());
    let err = app.append(0, &l, 0, 2_000, 10.0, None, None, &AOptions::default()).expect_err("error");
    assert!(err.is(&storage::new_duplicate_histogram_to_float_err(2_000, 10.0)), "{err}");
}

/// A sample value of the ST zero sample tables.
#[derive(Clone)]
enum Val {
    F(f64),
    H(Histogram),
    Fh(FloatHistogram),
}

impl Val {
    fn sample(&self, t: i64) -> Sample {
        match self {
            Val::F(f) => fs(t, *f),
            Val::H(h) => Sample::histogram(0, t, h.clone()),
            Val::Fh(fh) => Sample::float_histogram(0, t, fh.clone()),
        }
    }
    /// The sample with `CounterResetHint` set to unknown (Go `firstSample.CounterResetHint = Unknown`).
    fn first_sample(&self, t: i64) -> Sample {
        match self {
            Val::F(f) => fs(t, *f),
            Val::H(h) => {
                let mut h = h.clone();
                h.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                Sample::histogram(0, t, h)
            }
            Val::Fh(fh) => {
                let mut fh = fh.clone();
                fh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                Sample::float_histogram(0, t, fh)
            }
        }
    }
}

#[test]
fn test_head_appender_v2_append_enable_st_as_zero_sample() {
    // Make sure counter resets hints are non-zero, so we can detect ST histogram samples.
    let mut test_histogram = tu::generate_test_histogram(1);
    test_histogram.counter_reset_hint = CounterResetHint::NotCounterReset;

    let mut test_float_histogram = tu::generate_test_float_histogram(1);
    test_float_histogram.counter_reset_hint = CounterResetHint::NotCounterReset;

    let mut test_nhcb = tu::generate_test_custom_buckets_histogram(1);
    test_nhcb.counter_reset_hint = CounterResetHint::NotCounterReset;

    let mut test_float_nhcb = tu::generate_test_custom_buckets_float_histogram(1);
    test_float_nhcb.counter_reset_hint = CounterResetHint::NotCounterReset;

    // TODO(beorn7): Once issue #15346 is fixed, the CounterResetHint of the following zero histograms should be
    // histogram.CounterReset.
    let test_zero_histogram = Histogram {
        schema: test_histogram.schema,
        zero_threshold: test_histogram.zero_threshold,
        positive_spans: test_histogram.positive_spans.clone(),
        negative_spans: test_histogram.negative_spans.clone(),
        positive_buckets: vec![0, 0, 0, 0],
        negative_buckets: vec![0, 0, 0, 0],
        ..Default::default()
    };
    let test_zero_float_histogram = FloatHistogram {
        schema: test_float_histogram.schema,
        zero_threshold: test_float_histogram.zero_threshold,
        positive_spans: test_float_histogram.positive_spans.clone(),
        negative_spans: test_float_histogram.negative_spans.clone(),
        positive_buckets: vec![0.0, 0.0, 0.0, 0.0],
        negative_buckets: vec![0.0, 0.0, 0.0, 0.0],
        ..Default::default()
    };
    let test_zero_nhcb = Histogram {
        schema: test_nhcb.schema,
        positive_spans: test_nhcb.positive_spans.clone(),
        positive_buckets: vec![0, 0, 0, 0],
        custom_values: test_nhcb.custom_values.clone(),
        ..Default::default()
    };
    let test_zero_float_nhcb = FloatHistogram {
        schema: test_float_nhcb.schema,
        positive_spans: test_float_nhcb.positive_spans.clone(),
        positive_buckets: vec![0.0, 0.0, 0.0, 0.0],
        custom_values: test_float_nhcb.custom_values.clone(),
        ..Default::default()
    };

    // (type suffix, value, zero value): the five sample types of the Go table, in its order.
    let kinds: Vec<(&str, Val, Val)> = vec![
        ("floatSample", Val::F(10.0), Val::F(0.0)),
        ("histogram", Val::H(test_histogram.clone()), Val::H(test_zero_histogram.clone())),
        ("floathistogram", Val::Fh(test_float_histogram.clone()), Val::Fh(test_zero_float_histogram.clone())),
        ("NHCB", Val::H(test_nhcb.clone()), Val::H(test_zero_nhcb.clone())),
        ("floatNHCB", Val::Fh(test_float_nhcb.clone()), Val::Fh(test_zero_float_nhcb.clone())),
    ];

    // (name, appendable samples as (ts, value, st), expected samples).
    type Case = (String, Vec<(i64, Val, i64)>, Vec<Sample>);
    let mut cases: Vec<Case> = Vec::new();
    for (k, v, z) in &kinds {
        cases.push((
            format!("In order ct+normal sample/{k}"),
            vec![(100, v.clone(), 1), (101, v.clone(), 1)],
            vec![z.sample(1), v.sample(100), v.sample(101)],
        ));
    }
    for (k, v, z) in &kinds {
        cases.push((
            format!("Consecutive appends with same st ignore st/{k}"),
            vec![(100, v.clone(), 1), (101, v.clone(), 1)],
            vec![z.sample(1), v.sample(100), v.sample(101)],
        ));
    }
    for (k, v, z) in &kinds {
        cases.push((
            format!("Consecutive appends with newer st do not ignore st/{k}"),
            vec![(100, v.clone(), 1), (102, v.clone(), 101)],
            vec![z.sample(1), v.sample(100), z.sample(101), v.sample(102)],
        ));
    }
    for (k, v, z) in &kinds {
        cases.push((
            format!("ST equals to previous sample timestamp is ignored/{k}"),
            vec![(100, v.clone(), 1), (101, v.clone(), 100)],
            vec![z.sample(1), v.sample(100), v.sample(101)],
        ));
    }
    // The Go names of these cases use "float" instead of "floatSample".
    let short = |k: &str| if k == "floatSample" { "float" } else { k }.to_string();
    for (k, v, _) in &kinds {
        // ST results ErrOutOfBounds, but ST append is best effort, so ST should be ignored, but sample appended.
        // NOTE: Without ST, on query, first histogram sample will get CounterReset adjusted to 0.
        cases.push((
            format!("ST lower than minValidTime/{}", short(k)),
            vec![(100, v.clone(), -1)],
            vec![v.first_sample(100)],
        ));
    }
    for (k, v, _) in &kinds {
        // ST results ErrDuplicateSampleForTimestamp, but ST append is best effort, so ST should be ignored, but
        // sample appended.
        cases.push((
            format!("ST duplicates an existing sample/{}", short(k)),
            vec![(100, v.clone(), 0), (200, v.clone(), 100)],
            vec![v.first_sample(100), v.sample(200)],
        ));
    }
    for (k, v, _) in kinds.iter().take(3) {
        // ST results ErrOutOfOrderSample, but ST append is best effort, so ST should be ignored, but sample
        // appended.
        cases.push((
            format!("ST is out of order/{}", short(k)),
            vec![(200, v.clone(), 0), (300, v.clone(), 100)],
            vec![v.first_sample(200), v.sample(300)],
        ));
    }
    assert_eq!(33, cases.len());

    for (name, appendable_samples, expected_samples) in cases {
        eprintln!("{name}");
        let mut opts = new_test_head_default_options(DEFAULT_BLOCK_DURATION, false);
        opts.enable_st_as_zero_sample = true;
        let h = new_test_head_with_options("st-zero", compression::Type::None, opts);

        let mut a = h.appender_v2(&ctx());
        let lbls = ls(&["foo", "bar"]);

        for (ts, v, st) in &appendable_samples {
            let r = match v {
                Val::F(f) => a.append(0, &lbls, *st, *ts, *f, None, None, &AOptions::default()),
                Val::H(hh) => a.append(0, &lbls, *st, *ts, 0.0, Some(hh), None, &AOptions::default()),
                Val::Fh(fh) => a.append(0, &lbls, *st, *ts, 0.0, None, Some(fh), &AOptions::default()),
            };
            r.expect("append");
        }
        assert!(a.commit().is_none());

        let q = new_block_querier(&**h, i64::MIN, i64::MAX).expect("querier");
        let result = query(q, &[eq("foo", "bar")]);
        assert_eq!(Some(&expected_samples), result.get(r#"{foo="bar"}"#), "{name}");
    }
}

#[test]
fn test_head_appender_v2_best_effort_st_zero_sample_ooo() {
    let mut test_histogram = tu::generate_test_histogram(1);
    test_histogram.counter_reset_hint = CounterResetHint::NotCounterReset;
    let mut test_float_histogram = tu::generate_test_float_histogram(1);
    test_float_histogram.counter_reset_hint = CounterResetHint::NotCounterReset;

    let cases: Vec<(&str, f64, Option<&Histogram>, Option<&FloatHistogram>)> = vec![
        ("float", 10.0, None, None),
        ("histogram", 0.0, Some(&test_histogram), None),
        ("floathistogram", 0.0, None, Some(&test_float_histogram)),
    ];
    for (name, v, h, fh) in cases {
        eprintln!("{name}");
        let mut opts = new_test_head_default_options(DEFAULT_BLOCK_DURATION, false);
        opts.enable_st_as_zero_sample = true;
        let head = new_test_head_with_options("st-zero-ooo", compression::Type::None, opts);

        let lbls = ls(&["foo", "bar"]);

        // First appender: commit a sample at ts=200 to establish headMaxt and populate headChunks on the series.
        let mut app = head.appender_v2(&ctx());
        app.append(0, &lbls, 0, 200, v, h, fh, &AOptions::default()).expect("append");
        assert!(app.commit().is_none());

        // Second appender: st=100 is OOO (headMaxt=200, OOO disabled). Because the series now has committed
        // data, bestEffortAppendSTZeroSample calls appendFloat/appendHistogram/appendFloatHistogram with
        // fastRejectOOO=true, gets ErrOutOfOrderSample, and silently ignores it. The main sample must still be
        // appended successfully.
        let mut app = head.appender_v2(&ctx());
        app.append(0, &lbls, 100, 300, v, h, fh, &AOptions::default()).expect("append");
        assert!(app.commit().is_none());

        let q = new_block_querier(&**head, i64::MIN, i64::MAX).expect("querier");
        let result = query(q, &[eq("foo", "bar")]);
        // Only the two main samples should be present; the OOO zero sample at ts=100 must have been dropped.
        assert_eq!(2, result.get(r#"{foo="bar"}"#).map_or(0, |s| s.len()), "{name}");
    }
}

/// Regression test for data race https://github.com/prometheus/prometheus/issues/15139.
#[test]
fn test_head_appender_v2_append_histogram_and_commit_concurrency() {
    let h = Arc::new(tu::generate_test_histogram(1));
    let fh = Arc::new(tu::generate_test_float_histogram(1));

    type AppendFn = Box<dyn Fn(&mut dyn AppenderV2, usize) -> Result<SeriesRef, Error> + Send + Sync>;
    let h1 = h.clone();
    let fh1 = fh.clone();
    let cases: Vec<(&str, AppendFn)> = vec![
        (
            "integer histogram",
            Box::new(move |app, i| {
                app.append(0, &ls(&["foo", "bar", "serial", &i.to_string()]), 0, 1, 0.0, Some(&h1), None, &AOptions::default())
            }),
        ),
        (
            "float histogram",
            Box::new(move |app, i| {
                app.append(0, &ls(&["foo", "bar", "serial", &i.to_string()]), 0, 1, 0.0, None, Some(&fh1), &AOptions::default())
            }),
        ),
    ];
    for (name, append_fn) in cases {
        eprintln!("{name}");
        head_appender_v2_append_histogram_and_commit_concurrency(Arc::new(append_fn));
    }
}

#[allow(clippy::type_complexity)]
fn head_appender_v2_append_histogram_and_commit_concurrency(
    append_fn: Arc<Box<dyn Fn(&mut dyn AppenderV2, usize) -> Result<SeriesRef, Error> + Send + Sync>>,
) {
    let th = new_test_head("hist-commit-concurrency", 1000, compression::Type::None, false);
    let head = th.head.clone();

    // How this works: Commit() should be atomic, thus one of the commits will be first and the other second. The
    // first commit will create a new series and write a sample. The second commit will see an exact duplicate
    // sample which it should ignore. Unless there's a race that causes the memSeries.lastHistogram to be corrupt
    // and fail the duplicate check.
    let mut handles = Vec::new();
    for _ in 0..2 {
        let head = head.clone();
        let append_fn = append_fn.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..10000 {
                let mut app = head.appender_v2(&Context::background());
                append_fn(app.as_mut(), i).expect("append");
                assert!(app.commit().is_none());
            }
        }));
    }
    for h in handles {
        h.join().expect("thread");
    }
}

#[test]
fn test_head_appender_v2_num_stale_series() {
    let mut th = new_test_head("num-stale-series", 1000, compression::Type::None, false);
    th.head.init(0).expect("init");

    // Initially, no series should be stale.
    assert_eq!(0, th.head.num_stale_series());

    fn append_sample(head: &Head, lbls: &Labels, ts: i64, val: f64) {
        let mut app = head.appender_v2(&Context::background());
        app.append(0, lbls, 0, ts, val, None, None, &AOptions::default()).expect("append");
        assert!(app.commit().is_none());
    }
    fn append_histogram(head: &Head, lbls: &Labels, ts: i64, val: &Histogram) {
        let mut app = head.appender_v2(&Context::background());
        app.append(0, lbls, 0, ts, 0.0, Some(val), None, &AOptions::default()).expect("append");
        assert!(app.commit().is_none());
    }
    fn append_float_histogram(head: &Head, lbls: &Labels, ts: i64, val: &FloatHistogram) {
        let mut app = head.appender_v2(&Context::background());
        app.append(0, lbls, 0, ts, 0.0, None, Some(val), &AOptions::default()).expect("append");
        assert!(app.commit().is_none());
    }
    fn verify_series_counts(head: &Head, num_stale_series: u64, num_series: u64) {
        assert_eq!(num_stale_series, head.num_stale_series(), "stale series");
        assert_eq!(num_series, head.num_series(), "series");
    }
    // Restarts the head with a copy of its options; `snapshot` changes EnableMemorySnapshotOnShutdown first.
    let restart_head_and_verify_series_counts =
        |th: &mut crate::common::TestHead, num_stale_series: u64, num_series: u64, snapshot: Option<bool>| {
            verify_series_counts(&th.head, num_stale_series, num_series);

            // `snapshot` sets EnableMemorySnapshotOnShutdown on the options of the next head only (see the
            // adapted note at the snapshot restart below).
            let mut opts = copy_opts(&th.head.opts);
            if let Some(s) = snapshot {
                opts.enable_memory_snapshot_on_shutdown.store(s, std::sync::atomic::Ordering::Relaxed);
            }
            th.head.close().expect("close");

            let wal = new_wl(opts.chunk_dir_root.join("wal"), compression::Type::None);
            let head = Head::new(Some(wal.clone()), None, opts, None).expect("new head");
            th.head = head.clone();
            th.wal = Some(wal);
            head.init(0).expect("init");

            verify_series_counts(&head, num_stale_series, num_series);
        };

    // Create some series with normal samples.
    let series1 = ls(&["name", "series1", "label", "value1"]);
    let series2 = ls(&["name", "series2", "label", "value2"]);
    let series3 = ls(&["name", "series3", "label", "value3"]);

    // Add normal samples to all series.
    append_sample(&th.head, &series1, 100, 1.0);
    append_sample(&th.head, &series2, 100, 2.0);
    append_sample(&th.head, &series3, 100, 3.0);
    // Still no stale series.
    verify_series_counts(&th.head, 0, 3);

    // Make series1 stale by appending a stale sample. Now we should have 1 stale series.
    append_sample(&th.head, &series1, 200, stale());
    verify_series_counts(&th.head, 1, 3);

    // Make series2 stale as well.
    append_sample(&th.head, &series2, 200, stale());
    verify_series_counts(&th.head, 2, 3);
    restart_head_and_verify_series_counts(&mut th, 2, 3, None);

    // Add a non-stale sample to series1. It should not be counted as stale now.
    append_sample(&th.head, &series1, 300, 10.0);
    verify_series_counts(&th.head, 1, 3);
    restart_head_and_verify_series_counts(&mut th, 1, 3, None);

    // Test that series3 doesn't become stale when we add another normal sample.
    append_sample(&th.head, &series3, 200, 10.0);
    verify_series_counts(&th.head, 1, 3);

    // Test histogram stale samples as well.
    let series4 = ls(&["name", "series4", "type", "histogram"]);
    let h = tu::generate_test_histograms(1).remove(0);
    append_histogram(&th.head, &series4, 100, &h);
    verify_series_counts(&th.head, 1, 4);

    // Make histogram series stale.
    let mut stale_hist = h.clone();
    stale_hist.sum = stale();
    append_histogram(&th.head, &series4, 200, &stale_hist);
    verify_series_counts(&th.head, 2, 4);

    // Test float histogram stale samples.
    let series5 = ls(&["name", "series5", "type", "float_histogram"]);
    let fh = tu::generate_test_float_histograms(1).remove(0);
    append_float_histogram(&th.head, &series5, 100, &fh);
    verify_series_counts(&th.head, 2, 5);
    restart_head_and_verify_series_counts(&mut th, 2, 5, None);

    // Make float histogram series stale.
    let mut stale_fh = fh.clone();
    stale_fh.sum = stale();
    append_float_histogram(&th.head, &series5, 200, &stale_fh);
    verify_series_counts(&th.head, 3, 5);

    // Make histogram sample non-stale and stale back again.
    append_histogram(&th.head, &series4, 210, &h);
    verify_series_counts(&th.head, 2, 5);
    append_histogram(&th.head, &series4, 220, &stale_hist);
    verify_series_counts(&th.head, 3, 5);

    // Make float histogram sample non-stale and stale back again.
    append_float_histogram(&th.head, &series5, 210, &fh);
    verify_series_counts(&th.head, 2, 5);
    append_float_histogram(&th.head, &series5, 220, &stale_fh);
    verify_series_counts(&th.head, 3, 5);

    // Series 1 and 3 are not stale at this point. Add a new sample to series 1 and series 5, so after the GC and
    // removing series 2, 3, 4, we should be left with 1 stale and 1 non-stale series.
    append_sample(&th.head, &series1, 400, 10.0);
    append_float_histogram(&th.head, &series5, 400, &stale_fh);
    restart_head_and_verify_series_counts(&mut th, 3, 5, None);

    // This will test restarting with snapshot.
    // adapted: Go sets the flag on the live head, whose Close then writes the snapshot that the next head
    // loads. Here the flag can only be given to the next head, so one extra restart with the flag set makes the
    // snapshot (written by the Close of a head with the flag), and the restart after it loads it.
    restart_head_and_verify_series_counts(&mut th, 3, 5, Some(true));
    restart_head_and_verify_series_counts(&mut th, 3, 5, Some(true));

    // Test garbage collection behavior - stale series should be decremented when GC'd.
    // Force a garbage collection by truncating old data.
    th.head.truncate(300).expect("truncate");

    // After truncation, run GC to collect old chunks/series.
    th.head.gc();

    // series 1 and series 5 are left.
    verify_series_counts(&th.head, 1, 2);

    // Test creating a new series for each of float, histogram, float histogram that starts as stale. This should
    // be counted as stale.
    let series6 = ls(&["name", "series6", "direct", "stale"]);
    let series7 = ls(&["name", "series7", "direct", "stale"]);
    let series8 = ls(&["name", "series8", "direct", "stale"]);
    append_sample(&th.head, &series6, 400, stale());
    verify_series_counts(&th.head, 2, 3);
    append_histogram(&th.head, &series7, 400, &stale_hist);
    verify_series_counts(&th.head, 3, 4);
    append_float_histogram(&th.head, &series8, 400, &stale_fh);
    verify_series_counts(&th.head, 4, 5);
}

/// TestHeadAppenderV2_Append_HistogramStalenessConversionMetrics verifies that staleness marker conversion
/// correctly increments the right appender metrics for both histogram and float histogram scenarios.
#[test]
fn test_head_appender_v2_append_histogram_staleness_conversion_metrics() {
    let cases: Vec<(&str, bool)> = vec![("float_staleness_to_histogram", false), ("float_staleness_to_float_histogram", true)];

    for (name, float_hist) in cases {
        eprintln!("{name}");
        let opts = new_test_head_default_options(1000, false);
        opts.enable_st_storage.store(true, Ordering::SeqCst);
        opts.float_chunk_encoding.store(chunkenc::ENC_XOR2.0 as u32, Ordering::SeqCst);
        opts.enable_histogram_st_encoding.store(true, Ordering::SeqCst);
        let head = new_test_head_with_options("staleness-conversion", compression::Type::None, opts);

        let lbls = ls(&["name", name]);

        // Helper to get counter values.
        let get_sample_counter = |sample_type: &str| head.metrics.samples_appended.with_label_values(sample_type).get();

        // Step 1: Establish a series with histogram data.
        let mut app = head.appender_v2(&ctx());
        if float_hist {
            let fh = tu::generate_test_float_histograms(1).remove(0);
            app.append(0, &lbls, 0, 1000, 0.0, None, Some(&fh), &AOptions::default()).expect("append");
        } else {
            let h = tu::generate_test_histograms(1).remove(0);
            app.append(0, &lbls, 0, 1000, 0.0, Some(&h), None, &AOptions::default()).expect("append");
        }
        assert!(app.commit().is_none());

        // Step 2: Add a float staleness marker with a start timestamp. It is converted to a (float) histogram
        // staleness marker, which must keep the start timestamp like the plain float staleness path does.
        const MARKER_ST: i64 = 1900;
        let mut app = head.appender_v2(&ctx());
        app.append(0, &lbls, MARKER_ST, 2000, stale(), None, None, &AOptions::default()).expect("append");
        assert!(app.commit().is_none());

        // Count what was actually stored by querying the series.
        let mut q = new_block_querier(&**head, 0, 3000).expect("querier");
        let mut actual_float_samples = 0;
        let mut actual_histogram_samples = 0;
        let mut marker_found = false;
        let mut marker_got_st = 0i64;
        {
            let mut ss = q.select(&ctx(), false, None, &[eq("name", name)]);
            assert!(ss.next());
            let series = ss.at().clone();
            let mut it = series.iterator(None);
            loop {
                let vt = it.next();
                if vt == ValueType::None {
                    break;
                }
                match vt {
                    ValueType::Float => actual_float_samples += 1,
                    ValueType::Histogram | ValueType::FloatHistogram => actual_histogram_samples += 1,
                    _ => {}
                }
                if it.at_t() == 2000 {
                    marker_found = true;
                    marker_got_st = it.at_st();
                }
            }
            assert!(it.err().is_none());
        }
        assert!(q.close().is_none());

        // Verify what was actually stored - should be 0 floats, 2 histograms (original + converted staleness
        // marker).
        assert_eq!(0, actual_float_samples, "Should have 0 float samples stored");
        assert_eq!(2, actual_histogram_samples, "Should have 2 histogram samples: original + converted staleness marker");

        // The converted staleness marker must keep the start timestamp.
        assert!(marker_found, "converted staleness marker not found");
        assert_eq!(MARKER_ST, marker_got_st, "start timestamp must be preserved on the converted staleness marker");

        // The metrics should match what was actually stored.
        assert_eq!(
            actual_float_samples as f64,
            get_sample_counter("float"),
            "Float counter should match actual float samples stored"
        );
        assert_eq!(
            actual_histogram_samples as f64,
            get_sample_counter("histogram"),
            "Histogram counter should match actual histogram samples stored"
        );
    }
}

/// The start timestamps of the series `foo="bar"`, read from the head chunks and through the querier.
fn head_sts(h: &Arc<Head>) -> (Vec<i64>, Vec<i64>) {
    // Verify ST values are stored in chunks.
    let mut chk_metas = Vec::new();
    series_chunk_metas(h, "foo", "bar", &mut chk_metas);
    let chk_reader = h.chunks().expect("chunks");
    let mut chunk_sts = Vec::new();
    for meta in &chk_metas {
        let chk = chunk_of(&chk_reader, meta);
        let mut it = chk.iterator(None);
        while it.next() != ValueType::None {
            chunk_sts.push(it.at_st());
        }
        assert!(it.err().is_none());
    }
    chk_reader.close().expect("close");

    // Also verify via querier.
    let mut q = new_block_querier(&**h, i64::MIN, i64::MAX).expect("querier");
    let mut queried_sts = Vec::new();
    {
        let mut ss = q.select(&ctx(), false, None, &[eq("foo", "bar")]);
        assert!(ss.next());
        let series = ss.at().clone();
        assert!(ss.err().is_none());
        let mut it = series.iterator(None);
        while it.next() != ValueType::None {
            queried_sts.push(it.at_st());
        }
        assert!(it.err().is_none());
    }
    assert!(q.close().is_none());
    (chunk_sts, queried_sts)
}

/// TestHeadAppenderV2_STStorage verifies that when EnableSTStorage is true, start timestamps are properly stored
/// in chunks and returned by queries. This test uses AppenderV2 which has native ST support.
#[test]
fn test_head_appender_v2_st_storage() {
    let mut test_histogram = tu::generate_test_histogram(1);
    test_histogram.counter_reset_hint = CounterResetHint::NotCounterReset;

    // (st, ts, fSample, h).
    type SampleData<'a> = (i64, i64, f64, Option<&'a Histogram>);
    let th = Some(&test_histogram);
    let cases: Vec<(&str, Vec<SampleData>, Vec<i64>)> = vec![
        (
            "Float samples with ST",
            vec![(10, 100, 1.0, None), (20, 200, 2.0, None), (30, 300, 3.0, None)],
            vec![10, 20, 30],
        ),
        (
            "Float samples with varying ST",
            vec![(5, 100, 1.0, None), (5, 200, 2.0, None), (150, 300, 3.0, None)],
            vec![5, 5, 150],
        ),
        ("Histogram samples", vec![(10, 100, 0.0, th), (20, 200, 0.0, th), (30, 300, 0.0, th)], vec![10, 20, 30]),
        ("Float staleness marker keeps ST", vec![(10, 100, 1.0, None), (20, 200, stale(), None)], vec![10, 20]),
    ];

    for (name, samples, expected_sts) in cases {
        eprintln!("{name}");
        let opts = new_test_head_default_options(DEFAULT_BLOCK_DURATION, false);
        opts.enable_st_storage.store(true, Ordering::SeqCst);
        opts.float_chunk_encoding.store(chunkenc::ENC_XOR2.0 as u32, Ordering::SeqCst);
        opts.enable_histogram_st_encoding.store(true, Ordering::SeqCst);
        let h = new_test_head_with_options("st-storage", compression::Type::None, opts);

        let lbls = ls(&["foo", "bar"]);

        let mut a = h.appender_v2(&ctx());
        for (st, ts, f, hh) in &samples {
            a.append(0, &lbls, *st, *ts, *f, *hh, None, &AOptions::default()).expect("append");
        }
        assert!(a.commit().is_none());

        let (actual_sts, queried_sts) = head_sts(&h.head);
        assert_eq!(expected_sts, actual_sts, "ST values should round-trip through chunks ({name})");
        assert_eq!(expected_sts, queried_sts, "Querier should return same ST values as chunk iterator ({name})");
    }
}

/// TestHeadAppenderV2_Histogram_STStorage verifies that start timestamps supplied via AppenderV2.Append
/// round-trip through histogram and float-histogram chunks and are returned by both the chunk iterator and the
/// querier. When ST encoding is disabled the histograms fall back to V1 chunks and report ST=0.
#[test]
fn test_head_appender_v2_histogram_st_storage() {
    let int_hist = |i: i64| {
        let mut hh = tu::generate_test_histogram(i);
        hh.counter_reset_hint = CounterResetHint::NotCounterReset;
        Val::H(hh)
    };
    let float_hist = |i: i64| {
        let mut fh = tu::generate_test_float_histogram(i);
        fh.counter_reset_hint = CounterResetHint::NotCounterReset;
        Val::Fh(fh)
    };

    // (name, enableSTStorage, samples as (st, ts, value), expectedSTs).
    type Case = (&'static str, bool, Vec<(i64, i64, Val)>, Vec<i64>);
    let cases: Vec<Case> = vec![
        (
            "Integer histograms with ST enabled",
            true,
            vec![(10, 100, int_hist(1)), (100, 200, int_hist(2)), (200, 300, int_hist(3))],
            vec![10, 100, 200],
        ),
        (
            "Float histograms with ST enabled",
            true,
            vec![(11, 101, float_hist(1)), (101, 202, float_hist(2)), (202, 303, float_hist(3))],
            vec![11, 101, 202],
        ),
        (
            "Integer histograms with ST disabled report ST=0",
            false,
            vec![(10, 100, int_hist(1)), (100, 200, int_hist(2))],
            vec![0, 0],
        ),
        (
            "Float histograms with ST disabled report ST=0",
            false,
            vec![(11, 101, float_hist(1)), (101, 202, float_hist(2))],
            vec![0, 0],
        ),
    ];

    for (name, enable_st_storage, samples, expected_sts) in cases {
        eprintln!("{name}");
        let opts = new_test_head_default_options(DEFAULT_BLOCK_DURATION, false);
        opts.enable_st_storage.store(enable_st_storage, Ordering::SeqCst);
        opts.float_chunk_encoding.store(chunkenc::ENC_XOR2.0 as u32, Ordering::SeqCst);
        opts.enable_histogram_st_encoding.store(enable_st_storage, Ordering::SeqCst);
        let h = new_test_head_with_options("hist-st-storage", compression::Type::None, opts);

        let lbls = ls(&["foo", "bar"]);

        let mut a = h.appender_v2(&ctx());
        for (st, ts, v) in &samples {
            let r = match v {
                Val::H(hh) => a.append(0, &lbls, *st, *ts, 0.0, Some(hh), None, &AOptions::default()),
                Val::Fh(fh) => a.append(0, &lbls, *st, *ts, 0.0, None, Some(fh), &AOptions::default()),
                Val::F(_) => unreachable!(),
            };
            r.expect("append");
        }
        assert!(a.commit().is_none());

        let (chunk_sts, queried_sts) = head_sts(&h.head);
        assert_eq!(expected_sts, chunk_sts, "ST values should round-trip through chunks ({name})");
        assert_eq!(expected_sts, queried_sts, "querier should return same ST values as chunk iterator ({name})");
    }
}

#[test]
fn test_head_appender_v2_exemplars_survive_truncation() {
    for with_sample in [false, true] {
        eprintln!("withSample={with_sample}");
        let opts = HeadOptions {
            chunk_range: 1000,
            enable_exemplar_storage: true,
            ..HeadOptions::default()
        };
        opts.max_exemplars.store(10, Ordering::SeqCst);
        let th = new_head_no_wal("exemplars-survive", opts);
        let h = th.head.clone();
        h.init(0).expect("init");

        let lset = ls(&["name", "exemplar_series"]);
        let mut app = h.appender_v2(&ctx());
        let r = app.append(0, &lset, 0, 100, 1.0, None, None, &AOptions::default()).expect("append");
        assert!(app.commit().is_none());

        let e = Exemplar {
            labels: ls(&["trace_id", "abc123"]),
            value: 1.0,
            ts: 10000,
            has_ts: true,
        };
        let mut app = h.appender_v2(&ctx());
        let es = [e.clone()];
        if with_sample {
            app.append(
                r,
                &lset,
                0,
                200,
                2.0,
                None,
                None,
                &AOptions {
                    exemplars: &es,
                    ..Default::default()
                },
            )
            .expect("append");
        } else {
            app.as_exemplar_appender_v2()
                .expect("ExemplarAppenderV2")
                .append_exemplars(r, &lset, &es)
                .expect("append exemplars");
        }
        let series = h.series.get_by_id(r).expect("series");
        // Protect accepted exemplars even before their transaction commits.
        {
            let g = series.lock();
            assert!(g.has_exemplar());
            assert_eq!(e.ts, g.last_exemplar_ts());
        }
        assert!(app.commit().is_none());

        h.truncate(1000).expect("truncate");
        assert!(Arc::ptr_eq(&series, &h.series.get_by_id(r).expect("series")));
        {
            let g = series.lock();
            assert!(g.head_chunks.is_empty());
            assert!(g.app.is_none());
        }
        let q = h.exemplar_querier(&ctx()).expect("exemplar querier");
        let got = q.select(0, e.ts, &[vec![eq("name", "exemplar_series")]]).expect("select");
        assert_eq!(1, got.len());
        assert_eq!(lset, got[0].series_labels);
        assert_eq!(1, got[0].exemplars.len());
        assert!(e.equals(&got[0].exemplars[0]));

        // New samples can use the retained identity and create a new chunk appender.
        let mut app = h.appender_v2(&ctx());
        let new_ref = app.append(r, &lset, 0, e.ts + 1, 3.0, None, None, &AOptions::default()).expect("append");
        assert_eq!(r, new_ref);
        assert!(app.commit().is_none());
        {
            let g = series.lock();
            assert!(!g.head_chunks.is_empty());
            assert!(!g.app.is_none());
        }

        // Once the cutoff passes both the exemplar and the samples, eviction is safe.
        h.truncate(e.ts + 2).expect("truncate");
        assert!(h.series.get_by_id(r).is_none());
    }
}
