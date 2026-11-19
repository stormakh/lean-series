//! Speed check against Go's `BenchmarkParse*` (model/textparse/benchmark_test.go): the same inputs and the same
//! loop as Go's `benchParse` (Next, then Type/Help/Unit or Series/Histogram, Labels, StartTimestamp, Exemplars).
//! Prints ns/op, MB/s and allocations per op (counted with a wrapping global allocator).
//!
//! Run: `cargo run --release -p textparse --example bench_parse [filter]` (BENCH_SCRAPE=1 skips Labels, like
//! a scrape whose series are cached).

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use model::exemplar::Exemplar;
use model::labels::Labels;
use textparse::{
    Entry, NHCBParser, OpenMetrics2Parser, OpenMetricsOptions, OpenMetricsParser, Parser, ParserOptions, PromParser,
    ProtobufParser,
};

struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static NO_LABELS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

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

fn new_parser<'a>(kind: &str, b: &'a [u8]) -> Box<dyn Parser + 'a> {
    let om = || OpenMetricsParser::new(b, OpenMetricsOptions { skip_st_series: true, ..Default::default() });
    match kind {
        "promtext" => Box::new(PromParser::new(b, false)),
        "promproto" => Box::new(ProtobufParser::new(b, false, true, false, false)),
        "omtext" => Box::new(om()),
        "omtext_with_nhcb" => Box::new(NHCBParser::new(Box::new(om()), false, false)),
        "omtext_with_nhcb_st" => Box::new(NHCBParser::new(Box::new(om()), false, true)),
        "om2text" => Box::new(OpenMetrics2Parser::new(b, &ParserOptions::default())),
        "om2text_with_nhcb" => Box::new(OpenMetrics2Parser::new(
            b,
            &ParserOptions { convert_classic_histograms_to_nhcb: true, ..Default::default() },
        )),
        _ => panic!("unknown parser {kind}"),
    }
}

fn parse_once(kind: &str, data: &[u8], res: &mut Labels, e: &mut Exemplar) -> usize {
    let mut p = new_parser(kind, data);
    let mut n = 0;
    loop {
        let t = match p.next() {
            Ok(t) => t,
            Err(err) if err.is_eof() => break,
            Err(err) => panic!("{kind}: {err}"),
        };
        match t {
            Entry::Type => {
                let _ = p.typ();
                continue;
            }
            Entry::Help => {
                let _ = p.help();
                continue;
            }
            Entry::Unit => {
                let _ = p.unit();
                continue;
            }
            Entry::Comment => continue,
            Entry::Histogram => {
                let _ = p.histogram();
            }
            Entry::Series => {
                let _ = p.series();
            }
            Entry::Invalid => panic!("invalid entry"),
        }
        n += 1;
        if NO_LABELS.load(Ordering::Relaxed) {
            // Scrape cache hit: the series bytes find the cached labels, so Labels() is not called.
            continue;
        }
        p.labels(res);
        std::hint::black_box(&*res);
        let _ = p.start_timestamp();
        while p.exemplar(e) {}
    }
    n
}

fn bench(name: &str, kind: &str, data: &[u8]) {
    let mut res = Labels::default();
    let mut e = Exemplar::default();
    let samples = parse_once(kind, data, &mut res, &mut e);
    // Warm-up, then time for about one second.
    let start = Instant::now();
    let mut iters = 0u64;
    while start.elapsed() < Duration::from_millis(200) {
        parse_once(kind, data, &mut res, &mut e);
        iters += 1;
    }
    // Best of 5 rounds of about 0.2 s each (other processes on the machine add noise).
    let target = iters.max(1);
    let mut ns = f64::MAX;
    let mut allocs = 0.0;
    let mut bytes = 0.0;
    for _ in 0..5 {
        let a0 = ALLOCS.load(Ordering::Relaxed);
        let b0 = BYTES.load(Ordering::Relaxed);
        let t0 = Instant::now();
        for _ in 0..target {
            parse_once(kind, data, &mut res, &mut e);
        }
        let el = t0.elapsed();
        ns = ns.min(el.as_nanos() as f64 / target as f64);
        allocs = (ALLOCS.load(Ordering::Relaxed) - a0) as f64 / target as f64;
        bytes = (BYTES.load(Ordering::Relaxed) - b0) as f64 / target as f64;
    }
    println!(
        "{name:<58} {target:>8} {ns:>12.0} ns/op {:>9.2} MB/s {bytes:>9.0} B/op {allocs:>7.1} allocs/op  ({samples} samples)",
        data.len() as f64 / ns * 1e3,
    );
}

fn read(name: &str) -> Vec<u8> {
    let p = format!("{}/../../upstream/model/textparse/testdata/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&p).unwrap_or_else(|e| panic!("{p}: {e}"))
}

fn main() {
    let filter = std::env::args().nth(1).unwrap_or_default();
    // BENCH_SCRAPE=1: like a scrape whose series are all in the cache (no Labels/StartTimestamp/Exemplar calls).
    NO_LABELS.store(std::env::var("BENCH_SCRAPE").is_ok_and(|v| v == "1"), Ordering::Relaxed);
    let mut cases: Vec<(String, &str, Vec<u8>)> = vec![
        ("ParsePromText/parser=promtext".into(), "promtext", read("alltypes.237mfs.prom.txt")),
        ("ParsePromText/parser=omtext".into(), "omtext", read("alltypes.237mfs.prom.txt")),
        ("ParsePromText_NoMeta/parser=promtext".into(), "promtext", read("alltypes.237mfs.nometa.prom.txt")),
        ("ParseOMText".into(), "omtext", read("alltypes.5mfs.om.txt")),
        ("ParseOM2Text".into(), "om2text", read("alltypes.om2.txt")),
        ("ParseOM1VsOM2_AllTypes/parser=omtext".into(), "omtext", read("alltypes.bench.om.txt")),
        ("ParseOM1VsOM2_AllTypes/parser=omtext_with_nhcb".into(), "omtext_with_nhcb", read("alltypes.bench.om.txt")),
        ("ParseOM1VsOM2_AllTypes/parser=om2text".into(), "om2text", read("alltypes.bench.om2.txt")),
        ("ParseOM1VsOM2_AllTypes/parser=om2text_with_nhcb".into(), "om2text_with_nhcb", read("alltypes.bench.om2.txt")),
        ("ParseOM1VsOM2_CT/parser=omtext".into(), "omtext", read("ct.bench.om.txt")),
        ("ParseOM1VsOM2_CT/parser=om2text".into(), "om2text", read("ct.bench.om2.txt")),
        ("ParseOM1VsOM2_Histograms/parser=omtext".into(), "omtext", read("histograms.bench.om.txt")),
        ("ParseOM1VsOM2_Histograms/parser=omtext_with_nhcb".into(), "omtext_with_nhcb", read("histograms.bench.om.txt")),
        ("ParseOM1VsOM2_Histograms/parser=om2text".into(), "om2text", read("histograms.bench.om2.txt")),
        ("ParseOM1VsOM2_Histograms/parser=om2text_with_nhcb".into(), "om2text_with_nhcb", read("histograms.bench.om2.txt")),
        ("ParseOpenMetricsNHCB/parser=omtext".into(), "omtext", read("1histogram.om.txt")),
        ("ParseOpenMetricsNHCB/parser=omtext_with_nhcb".into(), "omtext_with_nhcb", read("1histogram.om.txt")),
        ("ParseOpenMetricsNHCB/parser=omtext_with_nhcb_st".into(), "omtext_with_nhcb_st", read("1histogram.om.txt")),
    ];
    let proto = format!("{}/../../tests-data/textparse/protobufparse_test/createTestProtoBuf.bin", env!("CARGO_MANIFEST_DIR"));
    if let Ok(b) = std::fs::read(&proto) {
        cases.push(("ParsePromProto/parser=promproto".into(), "promproto", b));
    }
    for (name, kind, data) in &cases {
        if name.contains(&filter) {
            bench(name, kind, data);
        }
    }
}
