//! Shared helpers of the agent tests (Go: `createTestAgentDB`, `labelsForTest`, `readTestWAL`,
//! `readWALSamples`, `genCheckpointTestSamples` of db_test.go and checkpoint_test.go).

#![allow(dead_code)]

use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use agent::{Db, Logger, Options};
use model::histogram::{FloatHistogram, Histogram};
use model::labels::{self, Labels};
use storage::SeriesRef;
use tsdb_format::record::{
    Decoder, RefExemplar, RefFloatHistogramSample, RefHistogramSample, RefMetadata, RefSample, RefSeries, Type,
};
use tsdb_format::tombstones::Stone;
use tsdb_format::wlog;

/// Go `walSegmentSize`: must be aligned to the page size.
pub const WAL_SEGMENT_SIZE: i64 = 32 << 10;

/// A fresh temporary directory (Go `t.TempDir()`); removed on drop.
pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new() -> TempDir {
        let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../bench/tmp/w3c-agent/tests");
        std::fs::create_dir_all(&base).unwrap();
        loop {
            let mut b = [0u8; 8];
            crypto::random_bytes(&mut b);
            let p = base.join(format!("t{}", u64::from_le_bytes(b)));
            if std::fs::create_dir(&p).is_ok() {
                return TempDir(p);
            }
        }
    }
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A test DB with its temporary directory (dropped after the DB).
pub struct TestDb {
    pub db: Db,
    pub dir: TempDir,
}

impl Deref for TestDb {
    type Target = Db;
    fn deref(&self) -> &Db {
        &self.db
    }
}

/// Go `createTestAgentDB`. adapted: there is no remote storage (Go `remote.NewStorage`) and no registry; the
/// metrics are fields of the DB.
pub fn create_test_agent_db(opts: Options) -> TestDb {
    let dir = TempDir::new();
    let db = agent::open(Logger::nop(), None, dir.path(), Some(opts)).expect("open agent db");
    TestDb { db, dir }
}

/// Go `labelsForTest`: `{a=<name>, instance=localhost<i>, job=prometheus}`.
pub fn labels_for_test(l_name: &str, series_count: usize) -> Vec<Labels> {
    (0..series_count)
        .map(|i| {
            let inst = format!("localhost{i}");
            labels::from_strings(&["a", l_name, "instance", &inst, "job", "prometheus"])
        })
        .collect()
}

/// One decoded WAL record (Go: the `[]any` elements of `readTestWAL`).
#[derive(Debug, Clone, PartialEq)]
pub enum Rec {
    Series(Vec<RefSeries>),
    Samples(Vec<RefSample>),
    Histograms(Vec<RefHistogramSample>),
    FloatHistograms(Vec<RefFloatHistogramSample>),
    Tombstones(Vec<Stone>),
    Metadata(Vec<RefMetadata>),
    Exemplars(Vec<RefExemplar>),
}

/// Calls `f` with the type and bytes of every record in the WAL (or checkpoint) directory `dir`.
pub fn for_each_record(dir: &Path, mut f: impl FnMut(Type, &[u8])) {
    let sr = wlog::new_segments_reader(dir).expect("segments reader");
    let mut r = wlog::Reader::new(sr);
    let dec = Decoder::new(Logger::nop());
    while r.next() {
        let rec = r.record();
        f(dec.r#type(rec), rec);
    }
    assert!(r.err().is_none(), "{:?}", r.err());
    r.into_inner().close().unwrap();
}

/// Go `readTestWAL`.
pub fn read_test_wal(dir: &Path) -> Vec<Rec> {
    let mut recs = Vec::new();
    let mut dec = Decoder::new(Logger::nop());
    for_each_record(dir, |t, rec| match t {
        Type::Series => {
            let mut v = Vec::new();
            dec.series(rec, &mut v).unwrap();
            recs.push(Rec::Series(v));
        }
        Type::Samples | Type::SamplesV2 => {
            let mut v = Vec::new();
            dec.samples(rec, &mut v).unwrap();
            recs.push(Rec::Samples(v));
        }
        Type::HistogramSamples | Type::CustomBucketsHistogramSamples | Type::HistogramSamplesV2 => {
            let mut v = Vec::new();
            dec.histogram_samples(rec, &mut v).unwrap();
            recs.push(Rec::Histograms(v));
        }
        Type::FloatHistogramSamples | Type::CustomBucketsFloatHistogramSamples | Type::FloatHistogramSamplesV2 => {
            let mut v = Vec::new();
            dec.float_histogram_samples(rec, &mut v).unwrap();
            recs.push(Rec::FloatHistograms(v));
        }
        Type::Tombstones => {
            let mut v = Vec::new();
            dec.tombstones(rec, &mut v).unwrap();
            recs.push(Rec::Tombstones(v));
        }
        Type::Metadata => {
            let mut v = Vec::new();
            dec.metadata(rec, &mut v).unwrap();
            recs.push(Rec::Metadata(v));
        }
        Type::Exemplars => {
            let mut v = Vec::new();
            dec.exemplars(rec, &mut v).unwrap();
            recs.push(Rec::Exemplars(v));
        }
        _ => panic!("unknown record type"),
    });
    recs
}

/// The metadata records of `recs`.
pub fn metadata_records(recs: &[Rec]) -> Vec<Vec<RefMetadata>> {
    recs.iter()
        .filter_map(|r| match r {
            Rec::Metadata(m) => Some(m.clone()),
            _ => None,
        })
        .collect()
}

/// Go `walSample`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct WalSample {
    pub st: i64,
    pub t: i64,
    pub f: f64,
    pub h: Option<Arc<Histogram>>,
    pub fh: Option<Arc<FloatHistogram>>,
    pub lbls: Labels,
    pub r#ref: SeriesRef,
}

/// Go `readWALSamples`: every sample of the WAL with the labels and ref of the last series record before it.
pub fn read_wal_samples(wal_dir: &Path) -> Vec<WalSample> {
    let mut dec = Decoder::new(Logger::nop());
    let mut last_series = RefSeries::default();
    let mut out = Vec::new();
    for_each_record(wal_dir, |t, rec| match t {
        Type::Series => {
            let mut v = Vec::new();
            dec.series(rec, &mut v).unwrap();
            last_series = v[0].clone();
        }
        Type::Samples | Type::SamplesV2 => {
            let mut v = Vec::new();
            dec.samples(rec, &mut v).unwrap();
            for s in v {
                out.push(WalSample {
                    st: s.st,
                    t: s.t,
                    f: s.v,
                    lbls: last_series.labels.copy(),
                    r#ref: last_series.r#ref,
                    ..Default::default()
                });
            }
        }
        Type::HistogramSamples | Type::CustomBucketsHistogramSamples | Type::HistogramSamplesV2 => {
            let mut v = Vec::new();
            dec.histogram_samples(rec, &mut v).unwrap();
            for h in v {
                out.push(WalSample {
                    st: h.st,
                    t: h.t,
                    h: Some(h.h),
                    lbls: last_series.labels.copy(),
                    r#ref: last_series.r#ref,
                    ..Default::default()
                });
            }
        }
        Type::FloatHistogramSamples | Type::CustomBucketsFloatHistogramSamples | Type::FloatHistogramSamplesV2 => {
            let mut v = Vec::new();
            dec.float_histogram_samples(rec, &mut v).unwrap();
            for h in v {
                out.push(WalSample {
                    st: h.st,
                    t: h.t,
                    fh: Some(h.fh),
                    lbls: last_series.labels.copy(),
                    r#ref: last_series.r#ref,
                    ..Default::default()
                });
            }
        }
        _ => {}
    });
    out
}

/// Counts of the records of a WAL directory (the loops of TestCommit and friends).
#[derive(Debug, Default)]
pub struct WalCounts {
    pub series: usize,
    pub samples: usize,
    pub exemplars: usize,
    pub histograms: usize,
    pub float_histograms: usize,
    /// Record types seen, in order.
    pub types: Vec<Type>,
    pub sample_sts: Vec<i64>,
    pub histogram_sts: Vec<i64>,
    pub float_histogram_sts: Vec<i64>,
}

/// Reads the WAL directory and counts series, samples, exemplars and histograms.
pub fn count_wal(dir: &Path) -> WalCounts {
    let mut c = WalCounts::default();
    let mut dec = Decoder::new(Logger::nop());
    for_each_record(dir, |t, rec| {
        c.types.push(t);
        match t {
            Type::Series => {
                let mut v = Vec::new();
                dec.series(rec, &mut v).unwrap();
                c.series += v.len();
            }
            Type::Samples | Type::SamplesV2 => {
                let mut v = Vec::new();
                dec.samples(rec, &mut v).unwrap();
                if t == Type::SamplesV2 {
                    c.sample_sts.extend(v.iter().map(|s| s.st));
                }
                c.samples += v.len();
            }
            Type::HistogramSamples | Type::CustomBucketsHistogramSamples | Type::HistogramSamplesV2 => {
                let mut v = Vec::new();
                dec.histogram_samples(rec, &mut v).unwrap();
                if t == Type::HistogramSamplesV2 {
                    c.histogram_sts.extend(v.iter().map(|s| s.st));
                }
                c.histograms += v.len();
            }
            Type::FloatHistogramSamples
            | Type::CustomBucketsFloatHistogramSamples
            | Type::FloatHistogramSamplesV2 => {
                let mut v = Vec::new();
                dec.float_histogram_samples(rec, &mut v).unwrap();
                if t == Type::FloatHistogramSamplesV2 {
                    c.float_histogram_sts.extend(v.iter().map(|s| s.st));
                }
                c.float_histograms += v.len();
            }
            Type::Exemplars => {
                let mut v = Vec::new();
                dec.exemplars(rec, &mut v).unwrap();
                c.exemplars += v.len();
            }
            _ => {}
        }
    });
    c
}

/// Go `startTime` is not needed (no remote storage).
///
/// Go `checkpointTestSamples`.
pub struct CheckpointTestSamples {
    pub datapoint_labels: Vec<Labels>,
    pub histogram_labels: Vec<Labels>,
    pub datapoint_samples: Vec<Vec<storage::Sample>>,
    pub histogram_samples: Vec<Vec<Histogram>>,
}

/// Go `genCheckpointTestSamples`.
pub fn gen_checkpoint_test_samples(
    label_prefix: &str,
    num_datapoints: usize,
    num_histograms: usize,
    num_series: usize,
) -> CheckpointTestSamples {
    let datapoint_labels = labels_for_test(label_prefix, num_series);
    let histogram_labels = labels_for_test(&format!("{label_prefix}_histogram"), num_series);
    let datapoint_samples = (0..num_datapoints).map(|_| storage::chunks::generate_samples(0, 1)).collect();
    let histogram_samples = histogram_labels
        .iter()
        .map(|_| storage::tsdbutil::generate_test_histograms(num_histograms))
        .collect();
    CheckpointTestSamples {
        datapoint_labels,
        histogram_labels,
        datapoint_samples,
        histogram_samples,
    }
}
