//! Port of cmd/promtool/tsdb_posix_test.go.

#![cfg(unix)]

mod pt_tsdb_common;

use std::collections::HashMap;
use std::io::Write;
use std::time::Duration;

use promtool::tsdb_cmd::{backfill_open_metrics, format_series_set_open_metrics};
use pt_tsdb_common::*;

#[test]
fn test_tsdb_dump_open_metrics_round_trip_pipe() {
    let initial_metrics = std::fs::read(testdata("dump-openmetrics-roundtrip-test.prom")).expect("read");
    let initial_metrics = normalize_new_line(initial_metrics);

    let pipe_dir = TempDir::new();
    let db_dir = TempDir::new();

    // create pipe
    let pipe = pipe_dir.path().join("pipe");
    let status = std::process::Command::new("mkfifo").arg("-m").arg("666").arg(&pipe).status().expect("mkfifo");
    assert!(status.success());

    let writer = {
        let pipe = pipe.clone();
        let data = initial_metrics.clone();
        std::thread::spawn(move || {
            // open pipe to write
            let mut f = std::fs::OpenOptions::new().write(true).open(&pipe).expect("open pipe");
            f.write_all(&data).expect("write pipe");
        })
    };

    // Import samples from OM format
    let (mut out, mut stderr) = (Vec::new(), Vec::new());
    let code = backfill_open_metrics(
        &pipe.to_string_lossy(),
        &db_dir.str(),
        false,
        false,
        Duration::from_secs(2 * 3600),
        &HashMap::new(),
        &mut out,
        &mut stderr,
    );
    assert_eq!(0, code, "{}", String::from_utf8_lossy(&stderr));
    writer.join().expect("writer");
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
