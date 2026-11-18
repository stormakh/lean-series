//! Port of `util/stats`: query timers and sample statistics (`query_stats.go`, `timer.go`).
//!
//! Tracing spans and Prometheus observers of Go's `SpanTimer` are not ported; a span timer only drives its timer
//! and calls the given observers with the elapsed seconds.

use std::fmt;
use std::time::{Duration, Instant};

/// A part of query execution that is timed (Go `QueryTiming`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum QueryTiming {
    EvalTotalTime,
    ResultSortTime,
    QueryPreparationTime,
    InnerEvalTime,
    ExecQueueTime,
    ExecTotalTime,
}

impl QueryTiming {
    /// Go `String()`.
    pub fn as_str(self) -> &'static str {
        match self {
            QueryTiming::EvalTotalTime => "Eval total time",
            QueryTiming::ResultSortTime => "Result sorting time",
            QueryTiming::QueryPreparationTime => "Query preparation time",
            QueryTiming::InnerEvalTime => "Inner eval time",
            QueryTiming::ExecQueueTime => "Exec queue wait time",
            QueryTiming::ExecTotalTime => "Exec total time",
        }
    }

    /// Go `SpanOperation()`.
    pub fn span_operation(self) -> &'static str {
        match self {
            QueryTiming::EvalTotalTime => "promqlEval",
            QueryTiming::ResultSortTime => "promqlSort",
            QueryTiming::QueryPreparationTime => "promqlPrepare",
            QueryTiming::InnerEvalTime => "promqlInnerEval",
            QueryTiming::ExecQueueTime => "promqlExecQueue",
            QueryTiming::ExecTotalTime => "promqlExec",
        }
    }
}

impl fmt::Display for QueryTiming {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Formats a duration like Go's `time.Duration.String()`.
pub fn go_duration_string(d: Duration) -> String {
    let ns = d.as_nanos() as u64;
    if ns == 0 {
        return "0s".to_string();
    }
    if ns < 1_000 {
        return format!("{}ns", ns);
    }
    if ns < 1_000_000 {
        return format!("{}µs", trim_frac(ns, 1_000));
    }
    if ns < 1_000_000_000 {
        return format!("{}ms", trim_frac(ns, 1_000_000));
    }
    let mut out = String::new();
    let total_secs = ns / 1_000_000_000;
    let frac = ns % 1_000_000_000;
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    if h > 0 {
        out.push_str(&format!("{}h", h));
    }
    if h > 0 || m > 0 {
        out.push_str(&format!("{}m", m));
    }
    let mut secs = s.to_string();
    if frac > 0 {
        let mut f = format!("{:09}", frac);
        while f.ends_with('0') {
            f.pop();
        }
        secs.push('.');
        secs.push_str(&f);
    }
    out.push_str(&secs);
    out.push('s');
    out
}

fn trim_frac(ns: u64, unit: u64) -> String {
    let whole = ns / unit;
    let frac = ns % unit;
    if frac == 0 {
        return whole.to_string();
    }
    let width = (unit as f64).log10() as usize;
    let mut f = format!("{:0width$}", frac, width = width);
    while f.ends_with('0') {
        f.pop();
    }
    format!("{}.{}", whole, f)
}

/// A timer that can be started and stopped and accumulates the time it ran (Go `Timer`).
#[derive(Clone, Debug)]
pub struct Timer {
    name: QueryTiming,
    created: usize,
    start: Option<Instant>,
    duration: Duration,
}

impl Timer {
    /// Starts the timer.
    pub fn start(&mut self) -> &mut Timer {
        self.start = Some(Instant::now());
        self
    }

    /// Stops the timer.
    pub fn stop(&mut self) {
        if let Some(s) = self.start {
            self.duration += s.elapsed();
        }
    }

    /// The time since the timer was started.
    pub fn elapsed_time(&self) -> Duration {
        self.start.map(|s| s.elapsed()).unwrap_or_default()
    }

    /// The accumulated duration in seconds.
    pub fn duration(&self) -> f64 {
        self.duration.as_secs_f64()
    }

    /// The accumulated duration.
    pub fn raw_duration(&self) -> Duration {
        self.duration
    }

    pub fn name(&self) -> QueryTiming {
        self.name
    }
}

impl fmt::Display for Timer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name, go_duration_string(self.duration))
    }
}

/// The timers of one query (Go `TimerGroup`).
#[derive(Clone, Debug, Default)]
pub struct TimerGroup {
    timers: Vec<Timer>,
}

impl TimerGroup {
    pub fn new() -> TimerGroup {
        TimerGroup::default()
    }

    /// The timer for a code section, created if necessary.
    pub fn get_timer(&mut self, name: QueryTiming) -> &mut Timer {
        if let Some(i) = self.timers.iter().position(|t| t.name == name) {
            return &mut self.timers[i];
        }
        let created = self.timers.len();
        self.timers.push(Timer { name, created, start: None, duration: Duration::ZERO });
        self.timers.last_mut().unwrap()
    }

    /// The timers in creation order.
    pub fn timers(&self) -> &[Timer] {
        &self.timers
    }

    /// Starts the timer of `name` (Go `GetSpanTimer` without tracing).
    pub fn start(&mut self, name: QueryTiming) {
        self.get_timer(name).start();
    }

    /// Stops the timer of `name` and returns the time since it was started (Go `SpanTimer.Finish`).
    pub fn finish(&mut self, name: QueryTiming) -> Duration {
        let t = self.get_timer(name);
        t.stop();
        t.elapsed_time()
    }
}

impl fmt::Display for TimerGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut timers: Vec<&Timer> = self.timers.iter().collect();
        timers.sort_by_key(|t| t.created);
        for t in timers {
            writeln!(f, "{}", t)?;
        }
        Ok(())
    }
}

/// The timers of a query (Go `QueryTimers`).
pub type QueryTimers = TimerGroup;

/// One per-step statistic (Go `stepStat`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StepStat {
    pub t: i64,
    pub v: i64,
}

impl fmt::Display for StepStat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} @[{}]", self.v, self.t)
    }
}

/// The sample statistics of a query (Go `QuerySamples`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QuerySamples {
    /// The highest count of samples considered while evaluating a query.
    pub peak_samples: usize,
    /// The total number of samples considered while evaluating a query.
    pub total_samples: i64,
    pub total_samples_per_step: Vec<i64>,
    /// The number of samples read from storage.
    pub samples_read: i64,
    pub samples_read_per_step: Vec<i64>,
    /// The number of series touched (summed per selector).
    pub total_series: i64,
    pub enable_per_step_stats: bool,
    pub start_timestamp: i64,
    pub interval: i64,
    /// Go keeps nil slices apart from empty ones; this tells whether step tracking was initialized.
    pub step_tracking: bool,
}

/// Total samples per step timestamp (Go `TotalSamplesPerStep`).
pub type TotalSamplesPerStep = std::collections::HashMap<i64, i64>;

impl QuerySamples {
    /// Go `NewQuerySamples`.
    pub fn new(enable_per_step_stats: bool) -> QuerySamples {
        QuerySamples { enable_per_step_stats, ..Default::default() }
    }

    /// Go `NewChild`.
    pub fn new_child(&self) -> QuerySamples {
        QuerySamples::new(false)
    }

    /// Go `NewChildWithStepTracking`.
    pub fn new_child_with_step_tracking(start: i64, end: i64, interval: i64) -> QuerySamples {
        let mut qs = QuerySamples::new(true);
        qs.init_step_tracking(start, end, interval);
        qs
    }

    /// Go `InitStepTracking`.
    pub fn init_step_tracking(&mut self, start: i64, end: i64, interval: i64) {
        if !self.enable_per_step_stats {
            return;
        }
        let num_steps = ((end - start) / interval) as usize + 1;
        self.total_samples_per_step = vec![0; num_steps];
        self.samples_read_per_step = vec![0; num_steps];
        self.start_timestamp = start;
        self.interval = interval;
        self.step_tracking = true;
    }

    /// Go `StepTrackingEnabled`.
    pub fn step_tracking_enabled(&self) -> bool {
        self.enable_per_step_stats
    }

    /// Go `IncrementSamplesAtStep`.
    #[inline]
    pub fn increment_samples_at_step(&mut self, i: usize, samples: i64) {
        self.total_samples += samples;
        if self.step_tracking {
            self.total_samples_per_step[i] += samples;
        }
    }

    /// Go `IncrementSamplesAtTimestamp`.
    #[inline]
    pub fn increment_samples_at_timestamp(&mut self, t: i64, samples: i64) {
        self.total_samples += samples;
        if self.step_tracking {
            let i = ((t - self.start_timestamp) / self.interval) as usize;
            self.total_samples_per_step[i] += samples;
        }
    }

    /// Go `IncrementSamplesReadAtStep`.
    #[inline]
    pub fn increment_samples_read_at_step(&mut self, i: usize, n: i64) {
        self.samples_read += n;
        if self.step_tracking {
            self.samples_read_per_step[i] += n;
        }
    }

    /// Go `IncrementSamplesReadAtTimestamp`.
    #[inline]
    pub fn increment_samples_read_at_timestamp(&mut self, t: i64, n: i64) {
        self.samples_read += n;
        if self.step_tracking {
            let i = ((t - self.start_timestamp) / self.interval) as usize;
            self.samples_read_per_step[i] += n;
        }
    }

    /// Go `IncrementSeries`.
    #[inline]
    pub fn increment_series(&mut self, n: i64) {
        self.total_series += n;
    }

    /// Go `UpdatePeak`.
    #[inline]
    pub fn update_peak(&mut self, samples: usize) {
        if samples > self.peak_samples {
            self.peak_samples = samples;
        }
    }

    /// Go `UpdatePeakFromSubquery`.
    pub fn update_peak_from_subquery(&mut self, other: &QuerySamples) {
        if other.peak_samples > self.peak_samples {
            self.peak_samples = other.peak_samples;
        }
    }

    /// Go `MergeSeriesFromSubquery`.
    pub fn merge_series_from_subquery(&mut self, child: &QuerySamples) {
        self.total_series += child.total_series;
    }

    fn step_timestamp(&self, step: usize) -> i64 {
        self.start_timestamp + step as i64 * self.interval
    }

    fn per_step_points(&self, v: &[i64]) -> Vec<StepStat> {
        v.iter().enumerate().map(|(i, &c)| StepStat { t: self.step_timestamp(i), v: c }).collect()
    }

    /// The total samples per step (Go `totalSamplesPerStepPoints`); `None` without per-step stats.
    pub fn total_samples_per_step_points(&self) -> Option<Vec<StepStat>> {
        if !self.enable_per_step_stats {
            return None;
        }
        Some(self.per_step_points(&self.total_samples_per_step))
    }

    /// The samples read per step (Go `samplesReadPerStepPoints`).
    pub fn samples_read_per_step_points(&self) -> Option<Vec<StepStat>> {
        if !self.enable_per_step_stats || !self.step_tracking {
            return None;
        }
        Some(self.per_step_points(&self.samples_read_per_step))
    }

    /// Go `TotalSamplesPerStepMap`.
    pub fn total_samples_per_step_map(&self) -> Option<TotalSamplesPerStep> {
        self.total_samples_per_step_points().map(|v| v.into_iter().map(|s| (s.t, s.v)).collect())
    }

    /// Go `SamplesReadPerStepMap`.
    pub fn samples_read_per_step_map(&self) -> Option<TotalSamplesPerStep> {
        self.samples_read_per_step_points().map(|v| v.into_iter().map(|s| (s.t, s.v)).collect())
    }

    /// Go `MergeTotalSamplesFromSubquery`.
    pub fn merge_total_samples_from_subquery(&mut self, child: &QuerySamples, c: &SubqueryConsumer) {
        if c.num_steps <= 1 {
            self.increment_samples_at_step(0, child.total_samples);
        } else if c.range <= 0 {
            let per_step = child.total_samples_per_step.clone();
            self.merge_steps_once(child, &per_step, c, false);
        } else if let Some(at) = c.at_timestamp {
            let n = child.sum_in_window(&child.total_samples_per_step, c.offset, at - c.range, at);
            for step in 0..c.num_steps {
                self.increment_samples_at_step(step, n);
            }
        } else {
            let per_step = &child.total_samples_per_step;
            let mut sum = 0i64;
            let (mut lo, mut hi) = (0usize, 0usize);
            for step in 0..c.num_steps {
                let maxt = c.step_timestamp(step);
                while hi < per_step.len() && child.step_timestamp(hi) + c.offset <= maxt {
                    sum += per_step[hi];
                    hi += 1;
                }
                while lo < hi && child.step_timestamp(lo) + c.offset <= maxt - c.range {
                    sum -= per_step[lo];
                    lo += 1;
                }
                self.increment_samples_at_step(step, sum);
            }
        }
    }

    /// Go `MergeSamplesReadFromSubquery`.
    pub fn merge_samples_read_from_subquery(&mut self, child: &QuerySamples, c: &SubqueryConsumer) {
        if c.num_steps <= 1 {
            self.increment_samples_read_at_step(0, child.samples_read);
        } else if c.at_timestamp.is_some() && c.range > 0 {
            let at = c.at_timestamp.unwrap();
            let n = child.sum_in_window(&child.samples_read_per_step, c.offset, at - c.range, at);
            self.increment_samples_read_at_step(0, n);
        } else {
            self.merge_steps_once(child, &child.samples_read_per_step, c, true);
        }
    }

    fn merge_steps_once(&mut self, child: &QuerySamples, per_step: &[i64], c: &SubqueryConsumer, read: bool) {
        for (k, &n) in per_step.iter().enumerate() {
            if n == 0 {
                continue;
            }
            if let Some(step) = c.consuming_step(child.step_timestamp(k) + c.offset) {
                if read {
                    self.increment_samples_read_at_step(step, n);
                } else {
                    self.increment_samples_at_step(step, n);
                }
            }
        }
    }

    fn sum_in_window(&self, per_step: &[i64], offset: i64, mint: i64, maxt: i64) -> i64 {
        let mut sum = 0;
        for (k, &n) in per_step.iter().enumerate() {
            let tk = self.step_timestamp(k) + offset;
            if tk > mint && tk <= maxt {
                sum += n;
            }
        }
        sum
    }
}

/// How a parent consumes the steps of a subquery (Go `SubqueryConsumer`).
#[derive(Clone, Debug, Default)]
pub struct SubqueryConsumer {
    pub start: i64,
    pub interval: i64,
    pub num_steps: usize,
    pub offset: i64,
    pub range: i64,
    pub at_timestamp: Option<i64>,
}

impl SubqueryConsumer {
    fn step_timestamp(&self, step: usize) -> i64 {
        self.start + step as i64 * self.interval
    }

    fn consuming_step(&self, tk: i64) -> Option<usize> {
        let mut step = 0usize;
        if tk > self.start {
            step = ((tk - self.start + self.interval - 1) / self.interval) as usize;
        }
        if self.range <= 0 {
            return Some(step.min(self.num_steps - 1));
        }
        if step >= self.num_steps || tk <= self.step_timestamp(step) - self.range {
            return None;
        }
        Some(step)
    }
}

/// The statistics of a query (Go `Statistics`).
#[derive(Clone, Debug, Default)]
pub struct Statistics {
    pub timers: QueryTimers,
    pub samples: Option<QuerySamples>,
}

/// The query timings for the API (Go `queryTimings`), in seconds.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryTimings {
    pub eval_total_time: f64,
    pub result_sort_time: f64,
    pub query_preparation_time: f64,
    pub inner_eval_time: f64,
    pub exec_queue_time: f64,
    pub exec_total_time: f64,
}

/// The sample statistics for the API (Go `querySamples`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QuerySamplesStats {
    pub total_queryable_samples_per_step: Option<Vec<StepStat>>,
    pub total_queryable_samples: i64,
    pub samples_read_per_step: Option<Vec<StepStat>>,
    pub samples_read: i64,
    pub series_touched: i64,
    pub peak_samples: usize,
}

/// The built-in statistics of a query (Go `BuiltinStats`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BuiltinStats {
    pub timings: QueryTimings,
    pub samples: Option<QuerySamplesStats>,
}

/// Go `NewQueryStats`.
pub fn new_query_stats(s: &Statistics) -> BuiltinStats {
    let mut qt = QueryTimings::default();
    for t in s.timers.timers() {
        let d = t.duration();
        match t.name() {
            QueryTiming::EvalTotalTime => qt.eval_total_time = d,
            QueryTiming::ResultSortTime => qt.result_sort_time = d,
            QueryTiming::QueryPreparationTime => qt.query_preparation_time = d,
            QueryTiming::InnerEvalTime => qt.inner_eval_time = d,
            QueryTiming::ExecQueueTime => qt.exec_queue_time = d,
            QueryTiming::ExecTotalTime => qt.exec_total_time = d,
        }
    }
    let samples = s.samples.as_ref().map(|sp| QuerySamplesStats {
        total_queryable_samples: sp.total_samples,
        samples_read: sp.samples_read,
        series_touched: sp.total_series,
        peak_samples: sp.peak_samples,
        total_queryable_samples_per_step: sp.total_samples_per_step_points(),
        samples_read_per_step: sp.samples_read_per_step_points(),
    });
    BuiltinStats { timings: qt, samples }
}
