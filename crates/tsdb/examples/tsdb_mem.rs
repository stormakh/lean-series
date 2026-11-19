//! Memory of the Rust TSDB for the same workload as bench/tsdb-mem/main.go (the upstream Go TSDB).
//! usage: tsdb_mem <dir> <series> <samples>
//! Prints PSS (KiB, from /proc/self/smaps_rollup) after opening, after creating the series and after the samples.

use std::path::Path;

use model::labels::{self, Labels};
use storage::Context;
use tsdb::db::{open, prometheus_options};

fn pss() -> i64 {
    let s = std::fs::read_to_string("/proc/self/smaps_rollup").unwrap_or_default();
    s.lines()
        .find(|l| l.starts_with("Pss:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn report(stage: &str, n: usize) {
    let p = pss();
    let per = if n > 0 { p * 1024 / n as i64 } else { 0 };
    println!("rust {stage} pss_kib={p} bytes_per_series={per}");
}

fn lbls(i: usize) -> Labels {
    labels::from_strings(&[
        "__name__",
        &format!("bench_metric_{}", i % 600),
        "instance",
        &format!("host-{}", i / 600),
        "job",
        "bench",
        "series_id",
        &i.to_string(),
    ])
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = Path::new(&args[1]);
    let n: usize = args[2].parse().unwrap();
    let s: i64 = args[3].parse().unwrap();
    let db = open(dir, None, Some(prometheus_options(15 * 24 * 3600 * 1000, false)), None).expect("open");
    report("open", 0);
    let ctx = Context::background();
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
    let start = now - now % 15000 - s * 15000;
    let mut refs = vec![0u64; n];
    let mut seed: u64 = 1;
    for step in 0..s {
        let mut app = db.appender(&ctx);
        let t = start + step * 15000;
        for (i, r) in refs.iter_mut().enumerate() {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let v = ((seed >> 33) % 101) as f64;
            // Like Go's app.Append(ref, lbls(i), ...), the labels are built for every append.
            *r = app.append(*r, &lbls(i), t, v).expect("append");
        }
        if let Some(e) = app.commit() {
            panic!("commit: {e}");
        }
        if step == 0 {
            report("series", n);
        }
    }
    report("samples", n);
    db.close().expect("close");
}
