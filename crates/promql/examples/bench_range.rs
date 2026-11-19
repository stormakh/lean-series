//! Range query benchmark: the data and queries of upstream `BenchmarkRangeQuery` (promql/bench_test.go), stored in
//! XOR chunks of 120 samples like the TSDB head. Run with
//! `cargo run --release -p promql --example bench_range -- [filter] [iterations]` and compare with
//! `go test -run XXX -bench BenchmarkRangeQuery ./promql/` in the upstream tree.

use std::sync::Arc;
use std::time::{Duration, Instant};

use chunkenc::{AnyIterator, Chunk, ChunkIterator, ENC_XOR, ValueType, new_empty_chunk};
use model::histogram::{FloatHistogram, Histogram};
use model::labels::{Labels, Matcher, from_strings};
use promql::parser::{self, Parser};
use promql::{Engine, EngineOpts};
use storage::{
    Annotations, BoxQuerier, BoxSeriesSet, Context, Error, LabelHints, LabelQuerier, LabelResult, Querier, Queryable,
    SelectHints, Series, SeriesArc, SeriesSet,
};

struct ChunkSeries {
    labels: Labels,
    chunks: Vec<(i64, i64, Box<dyn Chunk>)>,
}

impl Series for ChunkSeries {
    fn labels(&self) -> &Labels {
        &self.labels
    }
    fn iterator<'a>(&'a self, _reuse: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        AnyIterator::Other(Box::new(ChunksIter { chunks: &self.chunks, i: 0, cur: None }))
    }
}

struct ChunksIter<'a> {
    chunks: &'a [(i64, i64, Box<dyn Chunk>)],
    i: usize,
    cur: Option<AnyIterator<'a>>,
}

impl ChunkIterator for ChunksIter<'_> {
    fn next(&mut self) -> ValueType {
        loop {
            if let Some(c) = &mut self.cur {
                let vt = c.next();
                if vt != ValueType::None {
                    return vt;
                }
                self.i += 1;
            }
            if self.i >= self.chunks.len() {
                self.cur = None;
                return ValueType::None;
            }
            self.cur = Some(self.chunks[self.i].2.iterator(None));
        }
    }
    fn seek(&mut self, t: i64) -> ValueType {
        if let Some(c) = &self.cur {
            if c.at_t() >= t {
                return ValueType::Float;
            }
        }
        // Skip whole chunks that end before t.
        let mut j = self.i;
        while j < self.chunks.len() && self.chunks[j].1 < t {
            j += 1;
        }
        if j != self.i || self.cur.is_none() {
            self.i = j;
            if j >= self.chunks.len() {
                self.cur = None;
                return ValueType::None;
            }
            self.cur = Some(self.chunks[j].2.iterator(None));
            if self.cur.as_mut().unwrap().next() == ValueType::None {
                return ValueType::None;
            }
        }
        loop {
            if self.cur.as_ref().unwrap().at_t() >= t {
                return ValueType::Float;
            }
            if self.next() == ValueType::None {
                return ValueType::None;
            }
        }
    }
    fn at(&self) -> (i64, f64) {
        self.cur.as_ref().unwrap().at()
    }
    fn at_histogram(&self, h: Option<Histogram>) -> (i64, Histogram) {
        self.cur.as_ref().unwrap().at_histogram(h)
    }
    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        self.cur.as_ref().unwrap().at_float_histogram(fh)
    }
    fn at_t(&self) -> i64 {
        self.cur.as_ref().unwrap().at_t()
    }
    fn at_st(&self) -> i64 {
        0
    }
    fn err(&self) -> Option<&chunkenc::Error> {
        None
    }
}

struct VecSet {
    series: Vec<SeriesArc>,
    i: isize,
}

impl SeriesSet for VecSet {
    fn next(&mut self) -> bool {
        self.i += 1;
        (self.i as usize) < self.series.len()
    }
    fn at(&self) -> &SeriesArc {
        &self.series[self.i as usize]
    }
    fn err(&self) -> Option<Error> {
        None
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

#[derive(Clone)]
struct Store(Arc<Vec<SeriesArc>>);

impl LabelQuerier for Store {
    fn label_values(&self, _: &Context, _: &str, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), None)
    }
    fn label_names(&self, _: &Context, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), None)
    }
    fn close(&mut self) -> Option<Error> {
        None
    }
}

impl Querier for Store {
    fn select(&self, _: &Context, _: bool, _: Option<&SelectHints>, ms: &[Matcher]) -> BoxSeriesSet {
        let series =
            self.0.iter().filter(|s| ms.iter().all(|m| m.matches(s.labels().get(&m.name)))).cloned().collect();
        Box::new(VecSet { series, i: -1 })
    }
}

impl Queryable for Store {
    fn querier(&self, _: i64, _: i64) -> Result<BoxQuerier, Error> {
        Ok(Box::new(self.clone()))
    }
}

fn setup(interval: i64, num_intervals: usize) -> Store {
    let mut metrics: Vec<Labels> = Vec::new();
    metrics.push(from_strings(&["__name__", "a_one"]));
    metrics.push(from_strings(&["__name__", "b_one"]));
    for j in 0..10 {
        metrics.push(from_strings(&["__name__", "h_one", "le", &j.to_string()]));
    }
    metrics.push(from_strings(&["__name__", "h_one", "le", "+Inf"]));
    for i in 0..10 {
        let l = i.to_string();
        metrics.push(from_strings(&["__name__", "a_ten", "l", &l]));
        metrics.push(from_strings(&["__name__", "b_ten", "l", &l]));
        for j in 0..10 {
            metrics.push(from_strings(&["__name__", "h_ten", "l", &l, "le", &j.to_string()]));
        }
        metrics.push(from_strings(&["__name__", "h_ten", "l", &l, "le", "+Inf"]));
    }
    for i in 0..100 {
        let l = i.to_string();
        metrics.push(from_strings(&["__name__", "a_hundred", "l", &l]));
        metrics.push(from_strings(&["__name__", "b_hundred", "l", &l]));
        for j in 0..10 {
            metrics.push(from_strings(&["__name__", "h_hundred", "l", &l, "le", &j.to_string()]));
        }
        metrics.push(from_strings(&["__name__", "h_hundred", "l", &l, "le", "+Inf"]));
    }
    let n = metrics.len();
    let mut out: Vec<SeriesArc> = Vec::new();
    for (i, m) in metrics.into_iter().enumerate() {
        let samples = (0..num_intervals).map(|s| (s as i64 * interval, s as f64 + i as f64 / n as f64));
        out.push(Arc::new(build(m, samples)));
    }
    let per_sparse = num_intervals / 50;
    let mut s = 0;
    while s < num_intervals {
        let l = (s / per_sparse).to_string();
        let end = (s + per_sparse).min(num_intervals);
        let samples = (s..end).map(|k| (k as i64 * interval, k as f64 / n as f64));
        out.push(Arc::new(build(from_strings(&["__name__", "sparse", "l", &l]), samples)));
        s = end;
    }
    out.sort_by(|a, b| a.labels().cmp(b.labels()));
    Store(Arc::new(out))
}

fn build(labels: Labels, samples: impl Iterator<Item = (i64, f64)>) -> ChunkSeries {
    let mut chunks = Vec::new();
    let mut cur: Option<(i64, i64, Box<dyn Chunk>, chunkenc::Appender, usize)> = None;
    for (t, v) in samples {
        if cur.as_ref().is_some_and(|c| c.4 >= 120) {
            let (a, b, c, _, _) = cur.take().unwrap();
            chunks.push((a, b, c));
        }
        if cur.is_none() {
            let mut c = new_empty_chunk(ENC_XOR).unwrap();
            let app = c.appender().unwrap();
            cur = Some((t, t, c, app, 0));
        }
        let c = cur.as_mut().unwrap();
        c.3.append(&mut *c.2, 0, t, v);
        c.1 = t;
        c.4 += 1;
    }
    if let Some((a, b, c, _, _)) = cur {
        chunks.push((a, b, c));
    }
    ChunkSeries { labels, chunks }
}

/// The CPU time of this thread in nanoseconds (the first field of /proc/thread-self/schedstat).
fn thread_cpu_ns() -> u64 {
    std::fs::read_to_string("/proc/thread-self/schedstat")
        .ok()
        .and_then(|s| s.split_whitespace().next().and_then(|v| v.parse().ok()))
        .unwrap_or(0)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let filter = args.get(1).cloned().unwrap_or_default();
    let iters: u32 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(5);
    let interval = 10_000;
    let num_intervals = 8640 + 10000;
    let t0 = Instant::now();
    let store = setup(interval, num_intervals);
    eprintln!("setup: {:?}", t0.elapsed());
    let engine = Engine::new(EngineOpts {
        max_samples: 50_000_000,
        timeout: Duration::from_secs(100),
        enable_at_modifier: true,
        parser: Some(Parser::new(parser::Options { enable_experimental_functions: true, ..Default::default() })),
        ..Default::default()
    });
    let cases: &[(&str, usize)] = &[
        ("a_hundred", 1000),
        ("rate(a_hundred[1m])", 1000),
        ("rate(a_hundred[1m])", 10000),
        ("rate(a_hundred[1d])", 1000),
        ("-a_hundred", 1000),
        ("a_hundred - b_hundred", 1000),
        ("a_hundred - b_hundred", 10000),
        ("abs(a_hundred)", 1000),
        ("sum(a_hundred)", 1000),
        ("avg(a_hundred)", 1000),
        ("sum without (l)(h_hundred)", 1000),
        ("sum by (le)(h_hundred)", 1000),
        ("topk(5, a_hundred)", 1000),
        ("rate(a_hundred[1m]) + rate(b_hundred[1m])", 1000),
        ("sum without (l)(rate(a_hundred[1m]))", 1000),
        ("sum without (l)(rate(a_hundred[1m])) / sum without (l)(rate(b_hundred[1m]))", 1000),
        ("histogram_quantile(0.9, rate(h_hundred[5m]))", 1000),
        ("a_hundred + on(l) group_right a_one", 1000),
        ("quantile_over_time(time() % 100 / 100, a_hundred[1m:10s] @ 86400)", 1000),
        ("max_over_time(sum(a_hundred)[10m:1m])", 1000),
        ("timestamp(a_hundred)", 1000),
    ];
    let ctx = Context::background();
    for &(expr, steps) in cases {
        let skip = match filter.strip_prefix('=') { Some(f) => expr != f, None => !expr.contains(filter.as_str()) };
        if skip {
            continue;
        }
        let start = ((num_intervals - steps) * 10 * 1000) as i64;
        let end = (num_intervals * 10 * 1000) as i64;
        let mut best = Duration::MAX;
        let mut total = Duration::ZERO;
        let mut err = None;
        let cpu0 = thread_cpu_ns();
        for _ in 0..iters {
            let t = Instant::now();
            let mut q = engine
                .new_range_query(&ctx, Arc::new(store.clone()), None, expr, start, end, 10_000)
                .expect("query");
            let r = q.exec(&ctx);
            let d = t.elapsed();
            if let Some(e) = r.err {
                err = Some(e);
                break;
            }
            best = best.min(d);
            total += d;
        }
        let cpu = (thread_cpu_ns() - cpu0) as f64 / iters as f64;
        match err {
            Some(e) => println!("expr={},steps={}\terror: {}", expr, steps, e),
            None => println!(
                "expr={},steps={}\t{:.0} ns/op (best {:.0})\tcpu {:.2} ms/op",
                expr,
                steps,
                total.as_nanos() as f64 / iters as f64,
                best.as_nanos() as f64,
                cpu / 1e6
            ),
        }
    }
}
