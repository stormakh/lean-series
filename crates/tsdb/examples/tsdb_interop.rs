//! Data dir compatibility helper (used by scripts/interop/tsdb/run.sh).
//!
//!   tsdb_interop write <dir>   writes 5 h of float, histogram and float histogram series (with a stale marker)
//!                              ending now with the DB, compacts the oldest part into a block, deletes data in
//!                              the block and in the head (tombstones), and closes (WAL and chunks_head stay)
//!   tsdb_interop dump <dir>    prints every sample as `promtool tsdb dump` does (opens the dir read-only)

use std::path::Path;

use model::labels::{self, Labels, format_float_g};
use model::matcher::{MatchType, Matcher};
use storage::{Context, tsdbutil};
use tsdb::db::{DbReadOnly, open, prometheus_options};

const STEP: i64 = 15_000;

fn write(dir: &Path) {
    let opts = prometheus_options(10 * 365 * 24 * 3600 * 1000, false);
    let db = open(dir, None, Some(opts), None).expect("open db");
    let ctx = Context::background();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let end = now - now % STEP;
    let start = end - 5 * 3600 * 1000;

    let floats: Vec<Labels> = (0..20)
        .map(|i| {
            labels::from_strings(&["__name__", "interop_rs_float", "series", &i.to_string(), "job", "rust"])
        })
        .collect();
    let hists: Vec<Labels> =
        (0..3).map(|i| labels::from_strings(&["__name__", "interop_rs_hist", "series", &i.to_string()])).collect();
    let fhists: Vec<Labels> =
        (0..2).map(|i| labels::from_strings(&["__name__", "interop_rs_fhist", "series", &i.to_string()])).collect();

    let mut refs = vec![0u64; floats.len() + hists.len() + fhists.len()];
    let mut t = start;
    let mut step = 0i64;
    while t <= end {
        let mut app = db.appender(&ctx);
        for (i, l) in floats.iter().enumerate() {
            let v = match i % 5 {
                0 => step as f64,
                1 => (step as f64 * 0.37).sin() * 1e3,
                2 => -1.0 / (step as f64 + 3.0),
                3 => 1e300 / (step as f64 + 1.0),
                _ => (step % 7) as f64 * 0.1,
            };
            // Series 4 goes stale for a while (stale marker), then comes back.
            let v = if i == 4 && step % 400 == 200 { model::value::stale_nan() } else { v };
            if i == 4 && step % 400 > 200 && step % 400 < 220 {
                continue;
            }
            refs[i] = app.append(refs[i], l, t, v).expect("append float");
        }
        if step % 2 == 0 {
            for (j, l) in hists.iter().enumerate() {
                let h = tsdbutil::generate_test_histogram(step / 2 + j as i64);
                let k = floats.len() + j;
                refs[k] = app.append_histogram(refs[k], l, t, Some(&h), None).expect("append histogram");
            }
            for (j, l) in fhists.iter().enumerate() {
                let fh = tsdbutil::generate_test_float_histogram(step / 2 + j as i64);
                let k = floats.len() + hists.len() + j;
                refs[k] = app.append_histogram(refs[k], l, t, None, Some(&fh)).expect("append float histogram");
            }
        }
        if let Some(e) = app.commit() {
            panic!("commit: {e}");
        }
        t += STEP;
        step += 1;
    }
    db.compact(&ctx).expect("compact");
    assert!(!db.blocks().is_empty(), "no block was compacted");

    let m = |v: &str| Matcher::new(MatchType::Equal, "series", v).unwrap();
    let name = |n: &str| Matcher::new(MatchType::Equal, "__name__", n).unwrap();
    // In the block: one hour of series 1; in the head: ten minutes of series 2 and of histogram series 0.
    db.delete(&ctx, start + 3600_000, start + 2 * 3600_000, &[name("interop_rs_float"), m("1")]).expect("delete");
    db.delete(&ctx, end - 1200_000, end - 600_000, &[name("interop_rs_float"), m("2")]).expect("delete");
    db.delete(&ctx, end - 1200_000, end - 600_000, &[name("interop_rs_hist"), m("0")]).expect("delete");
    db.close().expect("close");
}

fn dump(dir: &Path) {
    use std::io::Write;
    let mut db = DbReadOnly::open(dir, None, None).expect("open read-only");
    let q = db.querier(i64::MIN, i64::MAX).expect("querier");
    let ctx = Context::background();
    let all = Matcher::new(MatchType::Regexp, "__name__", "(?s:.*)").unwrap();
    let mut ss = q.select(&ctx, false, None, &[all]);
    let out = std::io::stdout();
    let mut out = std::io::BufWriter::new(out.lock());
    while ss.next() {
        let s = ss.at().clone();
        let lbs = s.labels().to_string();
        let mut it = s.iterator(None);
        use chunkenc::{ChunkIterator, ValueType};
        loop {
            match it.next() {
                ValueType::None => break,
                ValueType::Float => {
                    let (t, v) = it.at();
                    writeln!(out, "{} {} {}", lbs, format_float_g(v), t).unwrap();
                }
                ValueType::FloatHistogram => {
                    let (t, fh) = it.at_float_histogram(None);
                    writeln!(out, "{} {} {}", lbs, fh.string(), t).unwrap();
                }
                ValueType::Histogram => {
                    let (t, h) = it.at_histogram(None);
                    writeln!(out, "{} {} {}", lbs, h.string(), t).unwrap();
                }
            }
        }
        if let Some(e) = it.err() {
            panic!("iterator: {e}");
        }
    }
    if let Some(e) = ss.err() {
        panic!("select: {e}");
    }
    drop(ss);
    drop(q);
    db.close().expect("close");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match (args.get(1).map(String::as_str), args.get(2)) {
        (Some("write"), Some(d)) => write(Path::new(d)),
        (Some("dump"), Some(d)) => dump(Path::new(d)),
        _ => {
            eprintln!("usage: tsdb_interop write|dump <dir>");
            std::process::exit(2);
        }
    }
}
