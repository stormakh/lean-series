//! Port of cmd/promtool/analyze.go (`promtool query analyze`).

use std::collections::{BTreeMap, HashSet};
use std::io::Write;
use std::sync::Arc;

use yaml::url::GoUrl;

use crate::api::{self, HttpApi, Time};
use crate::client_model::{self, LabelSet, Matrix, SampleStream, Value};

pub const ERR_NOT_NATIVE_HISTOGRAM: &str = "not a native histogram";
pub const ERR_NOT_ENOUGH_DATA: &str = "not enough data";

const OUTPUT_HEADER: &str = "Bucket stats for each histogram series over time
------------------------------------------------
First the min, avg, and max number of populated buckets, followed by the total
number of buckets (only if different from the max number of populated buckets
which is typical for classic but not native histograms).";
const OUTPUT_FOOTER: &str = "Aggregated bucket stats
-----------------------
Each line shows min/avg/max over the series above.";

/// Go `QueryAnalyzeConfig`. `duration` is in nanoseconds.
#[derive(Clone, Debug, Default)]
pub struct QueryAnalyzeConfig {
    pub metric_type: String,
    pub duration: i64,
    pub time: String,
    pub matchers: Vec<String>,
}

impl QueryAnalyzeConfig {
    /// Go `run`.
    pub fn run(&self, url: &GoUrl, rt: Arc<dyn net::RoundTripper>, out: &mut dyn Write) -> Result<(), crate::Error> {
        if self.metric_type != "histogram" {
            return Err(format!("analyze type is {}, must be 'histogram'", self.metric_type).into());
        }
        let mut url = url.clone();
        let api = api::new_api(&mut url, rt, &BTreeMap::new())?;
        let end_time = if !self.time.is_empty() {
            crate::query::parse_time(&self.time).map_err(|e| format!("error parsing time '{}': {e}", self.time))?
        } else {
            Time::now()
        };
        self.get_stats_from_metrics(&api, end_time, out, &self.matchers)
    }

    /// Go `getStatsFromMetrics`.
    pub fn get_stats_from_metrics(&self, api: &HttpApi, end_time: Time, out: &mut dyn Write, matchers: &[String]) -> Result<(), crate::Error> {
        write!(out, "{OUTPUT_HEADER}\n\n")?;
        let mut native = MetaStatistics::new();
        let mut classic = MetaStatistics::new();
        for matcher in matchers {
            let sel = series_selector(matcher, self.duration);
            let matrix = query_samples(api, &sel, end_time)?;
            // Go iterates a map (random order); this port uses the sorted key order.
            let mut matrices: BTreeMap<String, Matrix> = BTreeMap::new();
            for series in matrix {
                if series.values.is_empty() {
                    match calc_native_bucket_statistics(&series) {
                        Ok(stats) => {
                            writeln!(out, "- {} (native): {}", client_model::metric_string(&series.metric), stats)?;
                            native.update(&stats);
                        }
                        Err(e) if e == ERR_NOT_NATIVE_HISTOGRAM || e == ERR_NOT_ENOUGH_DATA => continue,
                        Err(e) => return Err(e.into()),
                    }
                } else {
                    let mut lbs = series.metric.clone();
                    if !lbs.contains_key("le") {
                        continue;
                    }
                    let metric_name = lbs.get("__name__").cloned().unwrap_or_default();
                    if !metric_name.ends_with("_bucket") {
                        continue;
                    }
                    lbs.remove("__name__");
                    lbs.remove("le");
                    let key = format_series_name(&metric_name, &lbs);
                    matrices.entry(key).or_default().push(series);
                }
            }
            for (key, mut matrix) in matrices {
                match calc_classic_bucket_statistics(&mut matrix) {
                    Ok(stats) => {
                        writeln!(out, "- {key} (classic): {stats}")?;
                        classic.update(&stats);
                    }
                    Err(e) if e == ERR_NOT_ENOUGH_DATA => continue,
                    Err(e) => return Err(e.into()),
                }
            }
        }
        write!(out, "\n{OUTPUT_FOOTER}\n")?;
        if native.count() > 0 {
            write!(out, "\nNative {native}\n")?;
        }
        if classic.count() > 0 {
            write!(out, "\nClassic {classic}\n")?;
        }
        Ok(())
    }
}

/// Go `seriesSelector`.
pub fn series_selector(metric_name: &str, duration: i64) -> String {
    format!("{metric_name}[{}]", scrape::util::go_duration_string_nanos(duration))
}

/// Go `formatSeriesName`.
pub fn format_series_name(metric_name: &str, lbs: &LabelSet) -> String {
    format!("{metric_name}{}", client_model::labelset_string(lbs))
}

/// Go `querySamples`.
pub fn query_samples(api: &HttpApi, query: &str, end: Time) -> Result<Matrix, crate::Error> {
    let (v, _, _) = api.query(query, Some(end))?;
    match v {
        Value::Matrix(m) => Ok(m),
        _ => Err("query of buckets resulted in non-Matrix".into()),
    }
}

/// Go `statistics`: min/avg/max populated buckets and the total number of buckets.
#[derive(Clone, Debug, PartialEq)]
pub struct Statistics {
    pub min_pop: i64,
    pub max_pop: i64,
    pub total: i64,
    pub avg_pop: f64,
}

impl std::fmt::Display for Statistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.max_pop == self.total {
            write!(f, "{}/{:.3}/{}", self.min_pop, self.avg_pop, self.max_pop)
        } else {
            write!(f, "{}/{:.3}/{}/{}", self.min_pop, self.avg_pop, self.max_pop, self.total)
        }
    }
}

/// Go `calcClassicBucketStatistics`. Errors are the Go error texts.
pub fn calc_classic_bucket_statistics(matrix: &mut Matrix) -> Result<Statistics, String> {
    let num_buckets = matrix.len();
    let mut stats = Statistics { min_pop: i64::MAX, max_pop: 0, total: num_buckets as i64, avg_pop: 0.0 };
    if num_buckets == 0 || matrix[0].values.len() < 2 {
        return Err(ERR_NOT_ENOUGH_DATA.to_string());
    }
    let num_samples = matrix[0].values.len();
    sort_matrix(matrix);
    let mut total_pop = 0i64;
    for time_idx in 0..num_samples {
        let curr = get_bucket_counts_at_time(matrix, num_buckets, time_idx)?;
        let count_pop = curr.iter().filter(|b| **b != 0).count() as i64;
        total_pop += count_pop;
        stats.min_pop = stats.min_pop.min(count_pop);
        stats.max_pop = stats.max_pop.max(count_pop);
    }
    stats.avg_pop = total_pop as f64 / num_samples as f64;
    Ok(stats)
}

/// Go `sortMatrix`: stable sort by the `le` label.
pub fn sort_matrix(matrix: &mut Matrix) {
    matrix.sort_by(|a, b| get_le(a).partial_cmp(&get_le(b)).unwrap_or(std::cmp::Ordering::Equal));
}

fn get_le(s: &SampleStream) -> f64 {
    s.metric.get("le").and_then(|v| client_model::go_parse_float(v).ok()).unwrap_or(0.0)
}

/// Go `getBucketCountsAtTime`.
pub fn get_bucket_counts_at_time(matrix: &Matrix, num_buckets: usize, time_idx: usize) -> Result<Vec<i64>, String> {
    let mut counts = vec![0i64; num_buckets];
    if time_idx >= matrix[0].values.len() {
        return Ok(counts);
    }
    counts[0] = matrix[0].values[time_idx].value as i64;
    for (i, bucket) in matrix[1..].iter().enumerate() {
        if time_idx >= bucket.values.len() {
            return Ok(counts);
        }
        let curr = &bucket.values[time_idx];
        let prev = &matrix[i].values[time_idx];
        if curr.timestamp != prev.timestamp {
            return Err("matrix result is not time aligned".to_string());
        }
        counts[i + 1] = (curr.value - prev.value) as i64;
    }
    Ok(counts)
}

/// Go `calcNativeBucketStatistics`.
pub fn calc_native_bucket_statistics(series: &SampleStream) -> Result<Statistics, String> {
    let mut stats = Statistics { min_pop: i64::MAX, max_pop: 0, total: 0, avg_pop: 0.0 };
    let mut overall: HashSet<(i32, u64, u64)> = HashSet::new();
    if series.histograms.is_empty() {
        return Err(ERR_NOT_NATIVE_HISTOGRAM.to_string());
    }
    if series.histograms.len() == 1 {
        return Err(ERR_NOT_ENOUGH_DATA.to_string());
    }
    let mut total_pop = 0i64;
    for h in &series.histograms {
        for b in &h.histogram.buckets {
            // Go compares the float bounds with ==; normalize -0 to 0 so the set matches.
            let norm = |f: f64| if f == 0.0 { 0f64.to_bits() } else { f.to_bits() };
            overall.insert((b.boundaries, norm(b.upper), norm(b.lower)));
        }
        let count_pop = h.histogram.buckets.len() as i64;
        total_pop += count_pop;
        stats.min_pop = stats.min_pop.min(count_pop);
        stats.max_pop = stats.max_pop.max(count_pop);
    }
    stats.avg_pop = total_pop as f64 / series.histograms.len() as f64;
    stats.total = overall.len() as i64;
    Ok(stats)
}

/// Go `distribution`.
#[derive(Clone, Debug, PartialEq)]
pub struct Distribution {
    pub min: i64,
    pub max: i64,
    pub count: i64,
    pub avg: f64,
}

impl Distribution {
    pub fn new() -> Distribution {
        Distribution { min: i64::MAX, max: 0, count: 0, avg: 0.0 }
    }
    pub fn update(&mut self, num: i64) {
        self.min = self.min.min(num);
        self.max = self.max.max(num);
        self.count += 1;
        self.avg += num as f64 / self.count as f64 - self.avg / self.count as f64;
    }
}

impl Default for Distribution {
    fn default() -> Self {
        Distribution::new()
    }
}

impl std::fmt::Display for Distribution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{:.3}/{}", self.min, self.avg, self.max)
    }
}

/// Go `metaStatistics`.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct MetaStatistics {
    pub min_pop: Distribution,
    pub avg_pop: Distribution,
    pub max_pop: Distribution,
    pub total: Distribution,
}

impl MetaStatistics {
    pub fn new() -> MetaStatistics {
        MetaStatistics::default()
    }
    pub fn count(&self) -> i64 {
        self.min_pop.count
    }
    pub fn update(&mut self, s: &Statistics) {
        self.min_pop.update(s.min_pop);
        self.avg_pop.update(s.avg_pop as i64);
        self.max_pop.update(s.max_pop);
        self.total.update(s.total);
    }
}

impl std::fmt::Display for MetaStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "histogram series ({} in total):\n- min populated: {}\n- avg populated: {}\n- max populated: {}",
            self.count(),
            self.min_pop,
            self.avg_pop,
            self.max_pop
        )?;
        if self.max_pop != self.total {
            write!(f, "\n- total: {}", self.total)?;
        }
        Ok(())
    }
}
