//! Test helpers shared by the tsdb tests (Go `newTestHead`, `newTestHeadWithOptions`, `t.TempDir`).
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tsdb::head::{Head, HeadOptions};
use tsdb_format::wlog::{compression, WL};

/// A temporary directory that is removed on drop (Go `t.TempDir()`). Lives under `bench/tmp/w2-tsdb/head` of
/// the workspace.
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(name: &str) -> TempDir {
        static N: AtomicU64 = AtomicU64::new(0);
        let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../bench/tmp/w2-tsdb/head");
        let dir = base.join(format!("{}-{}-{}", name, std::process::id(), N.fetch_add(1, Ordering::SeqCst)));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create temp dir");
        TempDir(dir)
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

/// A head with its WAL and directory; the head is closed on drop (Go `t.Cleanup(h.Close)`).
pub struct TestHead {
    pub head: Arc<Head>,
    pub wal: Option<Arc<WL>>,
    pub dir: TempDir,
}

impl std::ops::Deref for TestHead {
    type Target = Arc<Head>;
    fn deref(&self) -> &Arc<Head> {
        &self.head
    }
}

impl Drop for TestHead {
    fn drop(&mut self) {
        if !self.head.is_closed() {
            self.head.close().expect("close head");
        }
    }
}

/// Go `newTestHeadWithOptions`.
pub fn new_test_head_with_options(name: &str, compress_wal: compression::Type, mut opts: HeadOptions) -> TestHead {
    let dir = TempDir::new(name);
    let wal = Arc::new(WL::new_size(dir.path().join("wal"), 32768, compress_wal).expect("wal"));
    opts.chunk_dir_root = dir.path().to_path_buf();
    let head = Head::new(Some(wal.clone()), None, opts, None).expect("new head");
    head.chunk_disk_mapper.iterate_all_chunks(|_, _, _, _, _, _, _| Ok(())).expect("iterate chunks");
    TestHead {
        head,
        wal: Some(wal),
        dir,
    }
}

/// Go `newTestHead(t, chunkRange, compressWAL, oooEnabled)`.
pub fn new_test_head(name: &str, chunk_range: i64, compress_wal: compression::Type, ooo_enabled: bool) -> TestHead {
    let opts = HeadOptions {
        chunk_range,
        enable_exemplar_storage: true,
        ..HeadOptions::default()
    };
    opts.max_exemplars.store(10, Ordering::Relaxed);
    if ooo_enabled {
        opts.out_of_order_time_window.store(10 * 60 * 60 * 1000, Ordering::Relaxed);
    }
    new_test_head_with_options(name, compress_wal, opts)
}

/// A head without WAL in a temporary directory (Go `NewHead(nil, nil, nil, nil, opts, nil)`).
pub fn new_head_no_wal(name: &str, mut opts: HeadOptions) -> TestHead {
    let dir = TempDir::new(name);
    opts.chunk_dir_root = dir.path().to_path_buf();
    let head = Head::new(None, None, opts, None).expect("new head");
    TestHead { head, wal: None, dir }
}

// ---------------------------------------------------------------------------------------------------------------
// Go testutil.go: sample type scenarios.

use model::labels::Labels;
use storage::tsdbutil as tu;
use storage::{Appender, Error, Sample, SeriesRef};

pub const FLOAT: &str = "float";
pub const INT_HISTOGRAM: &str = "integer histogram";
pub const FLOAT_HISTOGRAM: &str = "float histogram";
pub const CUSTOM_BUCKETS_INT_HISTOGRAM: &str = "custom buckets int histogram";
pub const CUSTOM_BUCKETS_FLOAT_HISTOGRAM: &str = "custom buckets float histogram";
pub const GAUGE_INT_HISTOGRAM: &str = "gauge int histogram";
pub const GAUGE_FLOAT_HISTOGRAM: &str = "gauge float histogram";

/// Go `sampleTypeScenario`.
#[derive(Clone, Copy)]
pub struct SampleTypeScenario {
    pub sample_type: &'static str,
    pub sample_func: fn(i64, i64) -> Sample,
}

impl SampleTypeScenario {
    /// Go `appendFunc` (through a V1 appender).
    pub fn append(&self, app: &mut dyn Appender, lbls: &Labels, ts: i64, value: i64) -> (Result<SeriesRef, Error>, Sample) {
        let s = (self.sample_func)(ts, value);
        let r = if let Some(h) = &s.h {
            app.append_histogram(0, lbls, ts, Some(h), None)
        } else if let Some(fh) = &s.fh {
            app.append_histogram(0, lbls, ts, None, Some(fh))
        } else {
            app.append(0, lbls, ts, s.f)
        };
        (r, s)
    }
}

fn sc(sample_type: &'static str, sample_func: fn(i64, i64) -> Sample) -> SampleTypeScenario {
    SampleTypeScenario { sample_type, sample_func }
}

/// Go `sampleTypeScenarios`.
pub fn sample_type_scenarios() -> Vec<(&'static str, SampleTypeScenario)> {
    vec![
        (FLOAT, sc("float", |ts, v| Sample::float(0, ts, v as f64))),
        (INT_HISTOGRAM, sc("histogram", |ts, v| Sample::histogram(0, ts, tu::generate_test_histogram(v)))),
        (FLOAT_HISTOGRAM, sc("histogram", |ts, v| Sample::float_histogram(0, ts, tu::generate_test_float_histogram(v)))),
        (CUSTOM_BUCKETS_INT_HISTOGRAM, sc("histogram", |ts, v| Sample::histogram(0, ts, tu::generate_test_custom_buckets_histogram(v)))),
        (CUSTOM_BUCKETS_FLOAT_HISTOGRAM, sc("histogram", |ts, v| Sample::float_histogram(0, ts, tu::generate_test_custom_buckets_float_histogram(v)))),
        (GAUGE_INT_HISTOGRAM, sc("histogram", |ts, v| Sample::histogram(0, ts, tu::generate_test_gauge_histogram(v)))),
        (GAUGE_FLOAT_HISTOGRAM, sc("histogram", |ts, v| Sample::float_histogram(0, ts, tu::generate_test_gauge_float_histogram(v)))),
    ]
}
