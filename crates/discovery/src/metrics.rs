//! A small metrics registry with the parts of client_golang that service discovery uses: counters, gauges,
//! summaries (count and sum only) and histograms, with label vectors, and a registry that rejects duplicate
//! registrations and gathers metric families. The names and labels are upstream's; the server's `/metrics`
//! handler can render `Registry::gather`.
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Gauge,
    Summary,
    Histogram,
}

/// The value of one series.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricValue {
    Counter(f64),
    Gauge(f64),
    Summary { count: u64, sum: f64 },
    /// Cumulative counts per upper bound (without +Inf).
    Histogram { count: u64, sum: f64, buckets: Vec<(f64, u64)> },
}

/// One series of a family.
#[derive(Debug, Clone, PartialEq)]
pub struct Metric {
    /// Const labels then variable labels, sorted by name.
    pub labels: Vec<(String, String)>,
    pub value: MetricValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Family {
    pub name: String,
    pub help: String,
    pub typ: MetricType,
    pub metrics: Vec<Metric>,
}

/// Something that can be registered: it describes its series ids and collects its families.
pub trait Collector: Send + Sync {
    /// Descriptor ids: the metric name and const labels (Go `Desc` identity).
    fn describe(&self) -> Vec<String>;
    fn collect(&self, out: &mut Vec<Family>);
}

/// The options of a metric (Go `prometheus.Opts`).
#[derive(Debug, Clone, Default)]
pub struct Opts {
    pub name: String,
    pub help: String,
    pub const_labels: Vec<(String, String)>,
}

impl Opts {
    pub fn new(name: &str, help: &str) -> Opts {
        Opts { name: name.to_string(), help: help.to_string(), const_labels: Vec::new() }
    }
    pub fn const_label(mut self, k: &str, v: &str) -> Opts {
        self.const_labels.push((k.to_string(), v.to_string()));
        self.const_labels.sort();
        self
    }
    fn desc_id(&self) -> String {
        let mut s = self.name.clone();
        for (k, v) in &self.const_labels {
            s.push('\u{0}');
            s.push_str(k);
            s.push('=');
            s.push_str(v);
        }
        s
    }
}

// ---------------- float cell ----------------

#[derive(Debug, Default)]
struct F64(AtomicU64);
impl F64 {
    fn get(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Relaxed))
    }
    fn set(&self, v: f64) {
        self.0.store(v.to_bits(), Ordering::Relaxed)
    }
    fn add(&self, v: f64) {
        let mut cur = self.0.load(Ordering::Relaxed);
        loop {
            let new = (f64::from_bits(cur) + v).to_bits();
            match self.0.compare_exchange_weak(cur, new, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return,
                Err(c) => cur = c,
            }
        }
    }
}

/// A counter or gauge value.
#[derive(Debug, Default)]
pub struct Value(F64);
impl Value {
    pub fn get(&self) -> f64 {
        self.0.get()
    }
    pub fn set(&self, v: f64) {
        self.0.set(v)
    }
    pub fn add(&self, v: f64) {
        self.0.add(v)
    }
    pub fn inc(&self) {
        self.0.add(1.0)
    }
    pub fn dec(&self) {
        self.0.add(-1.0)
    }
    /// Go `Gauge.SetToCurrentTime`.
    pub fn set_to_current_time(&self) {
        let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        self.0.set(d.as_nanos() as f64 / 1e9)
    }
}

/// Observations: count, sum and optional cumulative buckets.
#[derive(Debug)]
pub struct Observer {
    m: Mutex<(u64, f64, Vec<u64>)>,
    bounds: Vec<f64>,
}
impl Observer {
    fn new(bounds: Vec<f64>) -> Observer {
        let n = bounds.len();
        Observer { m: Mutex::new((0, 0.0, vec![0; n])), bounds }
    }
    pub fn observe(&self, v: f64) {
        let mut g = self.m.lock().unwrap_or_else(|e| e.into_inner());
        g.0 += 1;
        g.1 += v;
        for (i, b) in self.bounds.iter().enumerate() {
            if v <= *b {
                g.2[i] += 1;
            }
        }
    }
    pub fn count(&self) -> u64 {
        self.m.lock().unwrap_or_else(|e| e.into_inner()).0
    }
    pub fn sum(&self) -> f64 {
        self.m.lock().unwrap_or_else(|e| e.into_inner()).1
    }
    fn value(&self, histogram: bool) -> MetricValue {
        let g = self.m.lock().unwrap_or_else(|e| e.into_inner());
        if histogram {
            MetricValue::Histogram { count: g.0, sum: g.1, buckets: self.bounds.iter().copied().zip(g.2.iter().copied()).collect() }
        } else {
            MetricValue::Summary { count: g.0, sum: g.1 }
        }
    }
}

// ---------------- vectors ----------------

enum Kind {
    Value(MetricType),
    Summary,
    Histogram(Vec<f64>),
}

/// A metric with variable labels (Go `*prometheus.GaugeVec` etc.); with no variable labels it is a single
/// metric (`Gauge`, `Counter`).
pub struct MetricVec<T> {
    opts: Opts,
    label_names: Vec<String>,
    kind: Kind,
    children: Mutex<BTreeMap<Vec<String>, Arc<T>>>,
}

pub type CounterVec = MetricVec<Value>;
pub type GaugeVec = MetricVec<Value>;
pub type SummaryVec = MetricVec<Observer>;
pub type HistogramVec = MetricVec<Observer>;

trait Child: Send + Sync + 'static {
    fn make(kind: &Kind) -> Self;
    fn value(&self, kind: &Kind) -> MetricValue;
}
impl Child for Value {
    fn make(_: &Kind) -> Self {
        Value::default()
    }
    fn value(&self, kind: &Kind) -> MetricValue {
        match kind {
            Kind::Value(MetricType::Counter) => MetricValue::Counter(self.get()),
            _ => MetricValue::Gauge(self.get()),
        }
    }
}
impl Child for Observer {
    fn make(kind: &Kind) -> Self {
        match kind {
            Kind::Histogram(b) => Observer::new(b.clone()),
            _ => Observer::new(Vec::new()),
        }
    }
    fn value(&self, kind: &Kind) -> MetricValue {
        self.value(matches!(kind, Kind::Histogram(_)))
    }
}

#[allow(private_bounds)]
impl<T: Child> MetricVec<T> {
    fn with_kind(opts: Opts, labels: &[&str], kind: Kind) -> Arc<Self> {
        Arc::new(MetricVec { opts, label_names: labels.iter().map(|s| s.to_string()).collect(), kind, children: Mutex::new(BTreeMap::new()) })
    }
    /// The child for these label values (created on first use).
    pub fn with_label_values(&self, vals: &[&str]) -> Arc<T> {
        assert_eq!(vals.len(), self.label_names.len(), "inconsistent label cardinality for {}", self.opts.name);
        let key: Vec<String> = vals.iter().map(|s| s.to_string()).collect();
        let mut c = self.children.lock().unwrap_or_else(|e| e.into_inner());
        c.entry(key).or_insert_with(|| Arc::new(T::make(&self.kind))).clone()
    }
    /// Removes the child; true if it existed.
    pub fn delete_label_values(&self, vals: &[&str]) -> bool {
        let key: Vec<String> = vals.iter().map(|s| s.to_string()).collect();
        self.children.lock().unwrap_or_else(|e| e.into_inner()).remove(&key).is_some()
    }
    /// The single child of a metric without variable labels.
    pub fn get(&self) -> Arc<T> {
        self.with_label_values(&[])
    }
    pub fn name(&self) -> &str {
        &self.opts.name
    }
    /// The number of series (Go `testutil.CollectAndCount`).
    pub fn len(&self) -> usize {
        self.children.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl MetricVec<Value> {
    pub fn new_counter_vec(opts: Opts, labels: &[&str]) -> Arc<CounterVec> {
        Self::with_kind(opts, labels, Kind::Value(MetricType::Counter))
    }
    pub fn new_gauge_vec(opts: Opts, labels: &[&str]) -> Arc<GaugeVec> {
        Self::with_kind(opts, labels, Kind::Value(MetricType::Gauge))
    }
    /// A counter without variable labels; its single series exists from the start, as in Go.
    pub fn new_counter(opts: Opts) -> Arc<CounterVec> {
        let v = Self::new_counter_vec(opts, &[]);
        v.get();
        v
    }
    pub fn new_gauge(opts: Opts) -> Arc<GaugeVec> {
        let v = Self::new_gauge_vec(opts, &[]);
        v.get();
        v
    }
}

impl MetricVec<Observer> {
    pub fn new_summary_vec(opts: Opts, labels: &[&str]) -> Arc<SummaryVec> {
        Self::with_kind(opts, labels, Kind::Summary)
    }
    pub fn new_histogram_vec(opts: Opts, labels: &[&str], buckets: &[f64]) -> Arc<HistogramVec> {
        Self::with_kind(opts, labels, Kind::Histogram(buckets.to_vec()))
    }
    pub fn new_summary(opts: Opts) -> Arc<SummaryVec> {
        let v = Self::new_summary_vec(opts, &[]);
        v.get();
        v
    }
}

#[allow(private_bounds)]
impl<T: Child> Collector for MetricVec<T> {
    fn describe(&self) -> Vec<String> {
        vec![self.opts.desc_id()]
    }
    fn collect(&self, out: &mut Vec<Family>) {
        let typ = match &self.kind {
            Kind::Value(t) => *t,
            Kind::Summary => MetricType::Summary,
            Kind::Histogram(_) => MetricType::Histogram,
        };
        let c = self.children.lock().unwrap_or_else(|e| e.into_inner());
        let mut metrics = Vec::with_capacity(c.len());
        for (vals, child) in c.iter() {
            let mut labels = self.opts.const_labels.clone();
            for (n, v) in self.label_names.iter().zip(vals) {
                labels.push((n.clone(), v.clone()));
            }
            labels.sort();
            metrics.push(Metric { labels, value: child.value(&self.kind) });
        }
        out.push(Family { name: self.opts.name.clone(), help: self.opts.help.clone(), typ, metrics });
    }
}

// ---------------- registry ----------------

/// Go `prometheus.Registry`.
#[derive(Default)]
pub struct Registry {
    collectors: Mutex<Vec<Arc<dyn Collector>>>,
}

impl Registry {
    pub fn new() -> Arc<Registry> {
        Arc::new(Registry::default())
    }

    /// Registers a collector; a collector with an already registered descriptor is an error.
    pub fn register(&self, c: Arc<dyn Collector>) -> Result<(), String> {
        let mut cs = self.collectors.lock().unwrap_or_else(|e| e.into_inner());
        let ids = c.describe();
        for o in cs.iter() {
            if Arc::ptr_eq(o, &c) {
                return Err("duplicate metrics collector registration attempted".to_string());
            }
            for id in o.describe() {
                if ids.contains(&id) {
                    return Err("duplicate metrics collector registration attempted".to_string());
                }
            }
        }
        cs.push(c);
        Ok(())
    }

    /// Unregisters a collector (by identity); true if it was registered.
    pub fn unregister(&self, c: &Arc<dyn Collector>) -> bool {
        let mut cs = self.collectors.lock().unwrap_or_else(|e| e.into_inner());
        let before = cs.len();
        cs.retain(|o| !Arc::ptr_eq(o, c));
        cs.len() != before
    }

    /// All families, merged by name and sorted (Go `Gather`); families without series are left out.
    pub fn gather(&self) -> Vec<Family> {
        let cs: Vec<Arc<dyn Collector>> = self.collectors.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let mut fams: Vec<Family> = Vec::new();
        for c in cs {
            c.collect(&mut fams);
        }
        let mut merged: BTreeMap<String, Family> = BTreeMap::new();
        for f in fams {
            match merged.get_mut(&f.name) {
                Some(m) => m.metrics.extend(f.metrics),
                None => {
                    merged.insert(f.name.clone(), f);
                }
            }
        }
        merged
            .into_values()
            .filter(|f| !f.metrics.is_empty())
            .map(|mut f| {
                f.metrics.sort_by(|a, b| a.labels.cmp(&b.labels));
                f
            })
            .collect()
    }

    /// Go `testutil.GatherAndCount(reg, name)`: the number of series of the family.
    pub fn gather_and_count(&self, name: &str) -> usize {
        self.gather().iter().filter(|f| f.name == name).map(|f| f.metrics.len()).sum()
    }
}

/// Registers several collectors; on an error unregisters those registered so far (Go `MetricRegisterer`).
pub struct MetricRegisterer {
    reg: Arc<Registry>,
    metrics: Vec<Arc<dyn Collector>>,
}

impl MetricRegisterer {
    pub fn new(reg: Arc<Registry>, metrics: Vec<Arc<dyn Collector>>) -> MetricRegisterer {
        MetricRegisterer { reg, metrics }
    }
    pub fn register_metrics(&self) -> Result<(), String> {
        for c in &self.metrics {
            if let Err(e) = self.reg.register(c.clone()) {
                self.unregister_metrics();
                return Err(format!("failed to register metric: {e}"));
            }
        }
        Ok(())
    }
    pub fn unregister_metrics(&self) {
        for c in &self.metrics {
            self.reg.unregister(c);
        }
    }
}
