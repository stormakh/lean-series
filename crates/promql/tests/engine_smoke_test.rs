//! Quick checks of the engine with a minimal in-memory storage (not an upstream test).

use std::sync::Arc;
use std::time::Duration;

use model::labels::Labels;
use promql::parser::{self, Parser};
use promql::{Engine, EngineOpts, Value};
use storage::{
    Annotations, BoxQuerier, BoxSeriesSet, Context, Error, LabelHints, LabelQuerier, LabelResult, Querier, Queryable,
    Sample, SelectHints, SeriesArc, SeriesSet, new_list_series,
};

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
struct Mem(Arc<Vec<(Labels, Vec<Sample>)>>);

struct MemQ(Arc<Vec<(Labels, Vec<Sample>)>>);

impl LabelQuerier for MemQ {
    fn label_values(&self, _: &Context, _: &str, _: Option<&LabelHints>, _: &[model::labels::Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), None)
    }
    fn label_names(&self, _: &Context, _: Option<&LabelHints>, _: &[model::labels::Matcher]) -> LabelResult {
        (Vec::new(), Annotations::new(), None)
    }
    fn close(&mut self) -> Option<Error> {
        None
    }
}

impl Querier for MemQ {
    fn select(&self, _: &Context, _: bool, _: Option<&SelectHints>, ms: &[model::labels::Matcher]) -> BoxSeriesSet {
        let series = self
            .0
            .iter()
            .filter(|(l, _)| ms.iter().all(|m| m.matches(l.get(&m.name))))
            .map(|(l, s)| new_list_series(l.clone(), s.clone()))
            .collect();
        Box::new(VecSet { series, i: -1 })
    }
}

impl Queryable for Mem {
    fn querier(&self, _: i64, _: i64) -> Result<BoxQuerier, Error> {
        Ok(Box::new(MemQ(self.0.clone())))
    }
}

fn load(lines: &[&str], interval_ms: i64) -> Mem {
    let p = Parser::new(parser::Options::default());
    let mut v = Vec::new();
    for l in lines {
        let (lbls, vals) = p.parse_series_desc(l).unwrap();
        let mut samples = Vec::new();
        for (i, sv) in vals.iter().enumerate() {
            if sv.omitted {
                continue;
            }
            samples.push(Sample::float(0, i as i64 * interval_ms, sv.value));
        }
        v.push((lbls, samples));
    }
    v.sort_by(|a, b| a.0.cmp(&b.0));
    Mem(Arc::new(v))
}

fn engine() -> Engine {
    Engine::new(EngineOpts {
        max_samples: 50_000_000,
        timeout: Duration::from_secs(100),
        enable_at_modifier: true,
        enable_negative_offset: true,
        ..Default::default()
    })
}

fn instant(m: &Mem, q: &str, ts: i64) -> String {
    let e = engine();
    let mut qry = e.new_instant_query(&Context::background(), Arc::new(m.clone()), None, q, ts).unwrap();
    let r = qry.exec(&Context::background());
    match (&r.err, &r.value) {
        (Some(e), _) => format!("error: {}", e),
        (None, Some(Value::Vector(v))) => {
            let mut v: Vec<String> = v.iter().map(|s| format!("{} {}", s.metric, s.f)).collect();
            v.sort();
            v.join("\n")
        }
        (None, Some(v)) => v.to_string(),
        _ => String::new(),
    }
}

#[test]
fn smoke() {
    let m = load(
        &[
            r#"http_requests{job="api-server", instance="0", group="production"} 0+10x10"#,
            r#"http_requests{job="api-server", instance="1", group="production"} 0+20x10"#,
            r#"http_requests{job="app-server", instance="0", group="canary"} 0+70x10"#,
        ],
        300_000,
    );
    assert_eq!(
        instant(&m, "sum by (job) (http_requests)", 3_000_000),
        "{job=\"api-server\"} 300\n{job=\"app-server\"} 700"
    );
    assert_eq!(instant(&m, "count(http_requests)", 3_000_000), "{} 3");
    assert_eq!(instant(&m, "1 + 2 * 3", 0), "scalar: 7 @[0]");
    assert_eq!(
        instant(&m, "http_requests{job=\"api-server\"} + on(instance) group_left http_requests{job=\"app-server\"}", 3_000_000),
        "{group=\"production\", instance=\"0\", job=\"api-server\"} 800"
    );
    assert_eq!(
        instant(&m, "topk(1, http_requests)", 3_000_000),
        "{__name__=\"http_requests\", group=\"canary\", instance=\"0\", job=\"app-server\"} 700"
    );
    assert_eq!(
        instant(&m, "max_over_time(sum(http_requests)[30m:5m])", 3_000_000),
        "{} 1000"
    );
}
