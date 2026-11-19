//! The Rust side of Go's `BenchmarkScrapeLoopAppend` (withStorage=false, appendMetadataToWAL=false,
//! parseST=true, AppenderV2 with a no-op recording mock, rollback after each append): ns/op, bytes and
//! allocations per op. Run with `--release`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

struct Counting;
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(l.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        BYTES.fetch_add(n as u64, Ordering::Relaxed);
        unsafe { System.realloc(p, l, n) }
    }
}

#[global_allocator]
static A: Counting = Counting;

fn make_test_gauges(n: usize) -> Vec<u8> {
    let mut s = String::from("# TYPE metric_a gauge\n# HELP metric_a help text\n");
    for i in 0..n {
        s.push_str(&format!("metric_a{{foo=\"{}\",bar=\"{}\"}} 1\n", i, i * 100));
    }
    s.push_str("# EOF\n");
    s.into_bytes()
}

fn gauges_proto(n: usize) -> Vec<u8> {
    let mut t = String::from("name: \"metric_a\"\nhelp: \"help text\"\ntype: GAUGE\n");
    for i in 0..n {
        // expfmt sorts labels by name: bar before foo.
        t.push_str(&format!(
            "metric: < label: < name: \"bar\" value: \"{}\" > label: < name: \"foo\" value: \"{}\" > gauge: < value: 1 > >\n",
            i * 100,
            i
        ));
    }
    scrape::testing::dto::proto_marshal_delimited(&t)
}

fn testdata(f: &str) -> Vec<u8> {
    let p = format!("{}/../../upstream/model/textparse/testdata/{f}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&p).unwrap_or_else(|e| panic!("{p}: {e}")).into_iter().filter(|&b| b != b'\r').collect()
}

fn bench(name: &str, body: &[u8], ct: &str, cold: bool, nhcb: bool) {
    if let Some(f) = std::env::args().nth(1)
        && !name.contains(&f)
    {
        return;
    }
    let app = util::teststorage::Appendable::new().skip_recording(true);
    let (mut sl, _) = scrape::testing::new_test_scrape_loop(|sl| {
        sl.appendable_v2 = Some(Arc::new(app.clone()));
        sl.append_metadata_to_wal = false;
        sl.enable_native_histogram_scraping = true;
        sl.convert_classic_hist_to_nhcb = nhcb;
        sl.always_scrape_classic_hist = false;
        sl.enable_open_metrics2 = true;
        sl.parse_st = true;
    });
    let mut ts = SystemTime::UNIX_EPOCH;
    let mut run = |n: u64, sl: &mut scrape::ScrapeLoop| {
        for _ in 0..n {
            if cold {
                sl.cache = scrape::ScrapeCache::new(sl.metrics.clone());
            }
            ts += Duration::from_secs(1);
            let mut a = sl.appender();
            let (_, err) = a.append(body, ct, ts);
            if let Some(e) = err {
                panic!("{name}: {e}");
            }
            a.rollback();
        }
    };
    run(20, &mut sl);
    // Calibrate to about 2 s.
    let t = Instant::now();
    run(10, &mut sl);
    let per = t.elapsed().as_secs_f64() / 10.0;
    let n = ((2.0 / per) as u64).max(10);
    let (a0, b0) = (ALLOCS.load(Ordering::Relaxed), BYTES.load(Ordering::Relaxed));
    let t = Instant::now();
    run(n, &mut sl);
    let el = t.elapsed();
    let (a1, b1) = (ALLOCS.load(Ordering::Relaxed), BYTES.load(Ordering::Relaxed));
    println!(
        "{name:<60} {n:>8} {:>12.0} ns/op {:>10} B/op {:>8} allocs/op",
        el.as_nanos() as f64 / n as f64,
        (b1 - b0) / n,
        (a1 - a0) / n
    );
}

fn main() {
    let gauges = make_test_gauges(2000);
    let gauges_pb = gauges_proto(2000);
    let om1 = testdata("prometheus.om1.txt");
    let om2 = testdata("prometheus.om2.txt");
    for cold in [false, true] {
        let c = format!("coldCache={cold}");
        bench(&format!("data=1Fam2000Gauges/fmt=PromText/{c}"), &gauges, "text/plain", cold, false);
        bench(&format!("data=1Fam2000Gauges/fmt=OMText/{c}"), &gauges, "application/openmetrics-text", cold, false);
        bench(&format!("data=1Fam2000Gauges/fmt=PromProto/{c}"), &gauges_pb, "application/vnd.google.protobuf", cold, false);
        for nhcb in [false, true] {
            let d = if nhcb { "265FamsPrometheusNHCB" } else { "265FamsPrometheus" };
            bench(&format!("data={d}/fmt=OMText/{c}"), &om1, "application/openmetrics-text", cold, nhcb);
            bench(&format!("data={d}/fmt=OMText2/{c}"), &om2, "application/openmetrics-text; version=2.0.0", cold, nhcb);
        }
    }
}
