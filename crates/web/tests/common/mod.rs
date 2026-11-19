//! Shared test helpers (Go `util/teststorage`, `promqltest.LoadedStorage`, and the doubles of `api_test.go`).
#![allow(dead_code)]
pub mod doubles;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use model::exemplar::QueryResult as ExemplarQueryResult;
use promql::promqltest::{Test, TestCommand};
use promql::{Engine, EngineOpts};
use storage::{Context, Storage};
use tsdb::db::DB;
use web::api::{ApiFuncResult, ErrorType, ERROR_NONE};

/// Go `teststorage.TestStorage`: a TSDB in a temporary directory with exemplar storage, removed on drop.
pub struct TestDb {
    pub db: Arc<DB>,
    dir: PathBuf,
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = self.db.close();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

static N: AtomicU64 = AtomicU64::new(0);

/// A new temporary directory under the target's test temp dir.
pub fn temp_dir(name: &str) -> PathBuf {
    let d = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("web-{}-{}-{}", name, std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Go `teststorage.New`.
pub fn new_test_storage() -> TestDb {
    let mut opts = tsdb::db::default_options();
    opts.min_block_duration = 24 * 60 * 60 * 1000;
    opts.max_block_duration = 24 * 60 * 60 * 1000;
    opts.retention_duration = 0;
    opts.out_of_order_time_window = 0;
    opts.enable_exemplar_storage = true;
    opts.max_exemplars = 100_000;
    let dir = temp_dir("test_storage");
    let db = tsdb::db::open(&dir, None, Some(opts), None).expect("opening test storage");
    TestDb { db, dir }
}

/// Go `promqltest.LoadedStorage`.
pub fn loaded_storage(input: &str) -> TestDb {
    let st = new_test_storage();
    let db = st.db.clone();
    let open = move || -> Arc<dyn Storage> { db.clone() };
    let test = Test::new(input, false, &open).expect("parsing test input");
    for cmd in test.commands() {
        match cmd {
            TestCommand::Load(l) => {
                let mut app = st.db.appender_v2(&Context::background());
                l.append(&mut *app).expect("loading samples");
                assert!(app.commit().is_none());
            }
            _ => panic!("only 'load' commands accepted"),
        }
    }
    st
}

/// Go `appendExemplars`.
pub fn append_exemplars(s: &DB, ex: &[ExemplarQueryResult]) {
    let mut app = s.appender_v2(&Context::background());
    {
        let ea = app.as_exemplar_appender_v2().expect("exemplar appender");
        for ed in ex {
            ea.append_exemplars(0, &ed.series_labels, &ed.exemplars).unwrap();
        }
    }
    assert!(app.commit().is_none());
}

/// Go `testEngine`.
pub fn test_engine() -> Engine {
    promql::promqltest::new_test_engine_with_opts(EngineOpts {
        max_samples: 10000,
        timeout: Duration::from_secs(100),
        no_step_subquery_interval_fn: Some(Arc::new(|_| 60 * 1000)),
        enable_at_modifier: true,
        enable_negative_offset: true,
        enable_per_step_stats: true,
        ..Default::default()
    })
}

/// Go `assertAPIError`.
pub fn assert_api_error(res: &ApiFuncResult, exp: ErrorType) {
    match &res.err {
        None => assert_eq!(exp, ERROR_NONE, "Expected error of type {:?} but got none", exp.str),
        Some(e) => assert_eq!(exp, e.typ, "(%T) Expected error of type {:?} but got type {:?} ({})", exp.str, e.typ.str, e.err),
    }
}
