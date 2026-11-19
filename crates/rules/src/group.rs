//! Port of `rules/group.go`: rule groups, their evaluation loop, staleness handling, 'for' state restoration,
//! metrics, and the rule dependency map.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use discovery::metrics::{CounterVec, GaugeVec, HistogramVec, Opts, Registry, SummaryVec};
use model::labels::{self, Labels};
use model::matcher::{MatchType, Matcher};
use model::value::{is_stale_nan, STALE_NAN};
use chunkenc::ChunkIterator as _;
use promql::parser::{Expr, inspect};
use storage::{AppendV2Options, Context, Error, ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP, ERR_OUT_OF_ORDER_SAMPLE, ERR_TOO_OLD_SAMPLE};

use crate::alerting::{ALERT_FOR_STATE_METRIC_NAME, ALERT_METRIC_NAME};
use crate::log::Logger;
use crate::manager::{ManagerOptions, default_eval_iteration_func, SequentialRuleEvalController};
use crate::rule::{dur_nanos, same_rule, Rule, RuleRef, Time};

/// A one-shot signal (Go `chan struct{}` that is closed).
#[derive(Default)]
pub struct Signal {
    closed: Mutex<bool>,
    cv: Condvar,
}

impl Signal {
    pub fn new() -> Arc<Signal> {
        Arc::new(Signal::default())
    }
    /// Go `close(ch)`. Closing twice has no effect.
    pub fn close(&self) {
        *self.closed.lock().unwrap() = true;
        self.cv.notify_all();
    }
    pub fn is_closed(&self) -> bool {
        *self.closed.lock().unwrap()
    }
    /// Go `<-ch`.
    pub fn wait(&self) {
        let mut g = self.closed.lock().unwrap();
        while !*g {
            g = self.cv.wait(g).unwrap();
        }
    }
    /// Waits until the signal is closed or `deadline` passes; true if closed.
    pub fn wait_until(&self, deadline: Instant) -> bool {
        let mut g = self.closed.lock().unwrap();
        loop {
            if *g {
                return true;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            g = self.cv.wait_timeout(g, deadline - now).unwrap().0;
        }
    }
}

/// Go `GroupEvalIterationFunc`: evaluates the rules of a group at a time and updates the group state.
pub type GroupEvalIterationFunc = Arc<dyn Fn(&Context, &Group, Time) + Send + Sync>;

/// Go `GroupOptions`.
#[derive(Clone, Default)]
pub struct GroupOptions {
    pub name: String,
    pub file: String,
    pub interval: Duration,
    pub limit: i64,
    pub rules: Vec<RuleRef>,
    pub should_restore: bool,
    pub opts: Option<Arc<ManagerOptions>>,
    pub query_offset: Option<Duration>,
    pub done: Option<Arc<Signal>>,
    pub eval_iteration_func: Option<GroupEvalIterationFunc>,
}

#[derive(Default)]
struct Times {
    evaluation_time: Duration,
    evaluation_rule_time_sum: Duration,
    last_evaluation: Time,
    last_eval_timestamp: Time,
}

/// Go `Group`: a set of rules that are evaluated together at an interval.
pub struct Group {
    name: String,
    file: String,
    interval: Duration,
    query_offset: Option<Duration>,
    limit: i64,
    rules: Vec<RuleRef>,
    /// One map per rule: the series the rule returned in its last evaluation, keyed by their label bytes.
    series_in_previous_eval: Mutex<Vec<HashMap<Vec<u8>, Labels>>>,
    stale_series: Mutex<Vec<Labels>>,
    opts: Arc<ManagerOptions>,
    times: Mutex<Times>,
    should_restore: AtomicBool,
    mark_stale: AtomicBool,
    done: Arc<Signal>,
    terminated: Arc<Signal>,
    manager_done: Option<Arc<Signal>>,
    logger: Logger,
    metrics: Arc<Metrics>,
    eval_iteration_func: GroupEvalIterationFunc,
}

impl Group {
    /// Go `NewGroup`.
    pub fn new(o: GroupOptions) -> Group {
        let opts = o.opts.unwrap_or_else(|| Arc::new(ManagerOptions::default()));
        let metrics = match &opts.metrics {
            Some(m) => m.clone(),
            None => Metrics::new(opts.registerer.as_ref()),
        };
        let key = group_key(&o.file, &o.name);
        metrics.iterations_missed.with_label_values(&[&key]);
        metrics.iterations_scheduled.with_label_values(&[&key]);
        metrics.eval_total.with_label_values(&[&key]);
        metrics.eval_failures.with_label_values(&[&key]);
        metrics.group_last_eval_time.with_label_values(&[&key]);
        metrics.group_last_duration.with_label_values(&[&key]);
        metrics.group_last_rule_duration_sum.with_label_values(&[&key]);
        metrics.group_rules.with_label_values(&[&key]).set(o.rules.len() as f64);
        metrics.group_samples.with_label_values(&[&key]);
        metrics.group_interval.with_label_values(&[&key]).set(o.interval.as_secs_f64());

        let eval_iteration_func = o.eval_iteration_func.unwrap_or_else(|| Arc::new(default_eval_iteration_func));
        let n = o.rules.len();
        Group {
            logger: opts.logger.with(&[("file", &o.file), ("group", &o.name)]),
            name: o.name,
            file: o.file,
            interval: o.interval,
            query_offset: o.query_offset,
            limit: o.limit,
            rules: o.rules,
            series_in_previous_eval: Mutex::new(vec![HashMap::new(); n]),
            stale_series: Mutex::new(Vec::new()),
            opts,
            times: Mutex::new(Times::default()),
            should_restore: AtomicBool::new(o.should_restore),
            mark_stale: AtomicBool::new(false),
            done: Signal::new(),
            terminated: Signal::new(),
            manager_done: o.done,
            metrics,
            eval_iteration_func,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn file(&self) -> &str {
        &self.file
    }
    /// Go `Rules(matcherSets...)`: the rules whose labels match any of the matcher sets (all without sets).
    pub fn rules(&self, matcher_sets: &[Vec<Matcher>]) -> Vec<RuleRef> {
        if matcher_sets.is_empty() {
            return self.rules.clone();
        }
        self.rules.iter().filter(|r| matches_matcher_sets(matcher_sets, r.labels())).cloned().collect()
    }
    /// All rules of the group (Go `g.rules`).
    pub fn all_rules(&self) -> &[RuleRef] {
        &self.rules
    }
    pub fn queryable(&self) -> Option<Arc<dyn storage::Queryable>> {
        self.opts.queryable.clone()
    }
    pub fn context(&self) -> &Context {
        &self.opts.context
    }
    pub fn interval(&self) -> Duration {
        self.interval
    }
    pub fn limit(&self) -> i64 {
        self.limit
    }
    pub fn logger(&self) -> &Logger {
        &self.logger
    }
    pub fn opts(&self) -> &Arc<ManagerOptions> {
        &self.opts
    }
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
    /// Whether the 'for' state still has to be restored (Go `g.shouldRestore`).
    pub fn should_restore(&self) -> bool {
        self.should_restore.load(Ordering::SeqCst)
    }
    pub fn set_should_restore(&self, v: bool) {
        self.should_restore.store(v, Ordering::SeqCst);
    }
    /// Go `g.markStale`.
    pub fn set_mark_stale(&self, v: bool) {
        self.mark_stale.store(v, Ordering::SeqCst);
    }
    /// The series each rule returned in its last evaluation (Go `g.seriesInPreviousEval`).
    pub fn series_in_previous_eval(&self) -> Vec<HashMap<Vec<u8>, Labels>> {
        self.series_in_previous_eval.lock().unwrap().clone()
    }
    /// Sets the series of the last evaluation (Go tests set `g.seriesInPreviousEval`).
    pub fn set_series_in_previous_eval(&self, v: Vec<HashMap<Vec<u8>, Labels>>) {
        *self.series_in_previous_eval.lock().unwrap() = v;
    }
    /// The series to mark stale at the next evaluation (Go `g.staleSeries`).
    pub fn stale_series(&self) -> Vec<Labels> {
        self.stale_series.lock().unwrap().clone()
    }
    /// The query offset of the group (Go `g.queryOffset`).
    pub fn configured_query_offset(&self) -> Option<Duration> {
        self.query_offset
    }

    /// Go `run`: evaluates the group every interval until stopped. Blocks.
    pub fn run(self: &Arc<Group>, ctx: &Context) {
        let g = self.clone();
        let _terminated = TerminateOnDrop(self.terminated.clone());

        // Wait an initial amount to have consistently slotted intervals.
        let mut eval_timestamp = self.eval_timestamp(Time::now().unix_nano()).add(self.interval);
        if self.done.wait_until(deadline_for(eval_timestamp)) {
            return;
        }

        let ctx = &promql::OriginContext::attach(
            ctx,
            vec![(
                "ruleGroup".to_string(),
                promql::LogValue::Map(vec![
                    ("file".to_string(), promql::LogValue::Str(self.file().to_string())),
                    ("name".to_string(), promql::LogValue::Str(self.name().to_string())),
                ]),
            )],
        );

        let _stale = MarkStaleOnDrop(g.clone(), ctx.clone());

        let interval_ns = dur_nanos(self.interval).max(1);
        let mut ticker = Ticker::new(self.interval);

        (self.eval_iteration_func)(ctx, self, eval_timestamp);
        if self.should_restore() {
            // If we have to restore, we wait for another Eval to finish: during the first one we might not have
            // enough data, and recording rules would not have updated the latest values that alerts depend on.
            if !ticker.wait(&self.done) {
                return;
            }
            let missed = Time::now().sub(eval_timestamp) / interval_ns - 1;
            if missed > 0 {
                let key = group_key(&self.file, &self.name);
                self.metrics.iterations_missed.with_label_values(&[&key]).add(missed as f64);
                self.metrics.iterations_scheduled.with_label_values(&[&key]).add(missed as f64);
            }
            eval_timestamp = eval_timestamp.add_nanos((missed.max(0) + 1) * interval_ns);
            (self.eval_iteration_func)(ctx, self, eval_timestamp);
            self.restore_for_state(Time::now());
        }

        loop {
            if self.done.is_closed() {
                return;
            }
            if !ticker.wait(&self.done) {
                return;
            }
            let missed = Time::now().sub(eval_timestamp) / interval_ns - 1;
            if missed > 0 {
                let key = group_key(&self.file, &self.name);
                self.metrics.iterations_missed.with_label_values(&[&key]).add(missed as f64);
                self.metrics.iterations_scheduled.with_label_values(&[&key]).add(missed as f64);
            }
            eval_timestamp = eval_timestamp.add_nanos((missed.max(0) + 1) * interval_ns);
            (self.eval_iteration_func)(ctx, self, eval_timestamp);
        }
    }

    /// Go `stopAsync`.
    pub fn stop_async(&self) {
        self.done.close();
    }
    /// Go `waitStopped`.
    pub fn wait_stopped(&self) {
        self.terminated.wait();
    }
    /// Go `stop`.
    pub fn stop(&self) {
        self.stop_async();
        self.wait_stopped();
    }

    fn hash(&self) -> u64 {
        labels::from_strings(&["file", &self.file, "name", &self.name]).hash()
    }

    /// Go `AlertingRules`: the alerting rules, sorted by state (firing first) and name.
    pub fn alerting_rules(&self) -> Vec<RuleRef> {
        let mut alerts: Vec<RuleRef> = self.rules.iter().filter(|r| r.as_alerting().is_some()).cloned().collect();
        alerts.sort_by(|a, b| {
            let (aa, ba) = (a.as_alerting().unwrap(), b.as_alerting().unwrap());
            let (sa, sb) = (aa.state(), ba.state());
            if sa == sb { aa.name().cmp(ba.name()) } else { sb.cmp(&sa) }
        });
        alerts
    }

    /// Go `HasAlertingRules`.
    pub fn has_alerting_rules(&self) -> bool {
        self.rules.iter().any(|r| r.as_alerting().is_some())
    }

    /// Go `GetEvaluationTime`.
    pub fn get_evaluation_time(&self) -> Duration {
        self.times.lock().unwrap().evaluation_time
    }
    pub fn set_evaluation_time(&self, dur: Duration) {
        self.metrics.group_last_duration.with_label_values(&[&group_key(&self.file, &self.name)]).set(dur.as_secs_f64());
        self.times.lock().unwrap().evaluation_time = dur;
    }
    /// Go `GetRuleEvaluationTimeSum`.
    pub fn get_rule_evaluation_time_sum(&self) -> Duration {
        self.times.lock().unwrap().evaluation_rule_time_sum
    }
    pub(crate) fn update_rule_evaluation_time_sum(&self) {
        let sum: Duration = self.rules.iter().map(|r| r.get_evaluation_duration()).sum();
        self.metrics.group_last_rule_duration_sum.with_label_values(&[&group_key(&self.file, &self.name)]).set(sum.as_secs_f64());
        self.times.lock().unwrap().evaluation_rule_time_sum = sum;
    }
    /// Go `GetLastEvaluation`.
    pub fn get_last_evaluation(&self) -> Time {
        self.times.lock().unwrap().last_evaluation
    }
    pub fn set_last_evaluation(&self, ts: Time) {
        self.metrics.group_last_eval_time.with_label_values(&[&group_key(&self.file, &self.name)]).set(ts.unix_nano() as f64 / 1e9);
        self.times.lock().unwrap().last_evaluation = ts;
    }
    /// Go `GetLastEvalTimestamp`.
    pub fn get_last_eval_timestamp(&self) -> Time {
        self.times.lock().unwrap().last_eval_timestamp
    }
    pub(crate) fn set_last_eval_timestamp(&self, ts: Time) {
        self.times.lock().unwrap().last_eval_timestamp = ts;
    }

    /// Go `EvalTimestamp`: the immediately preceding consistently slotted evaluation time.
    pub fn eval_timestamp(&self, start_time: i64) -> Time {
        let interval = dur_nanos(self.interval).max(1);
        let offset = (self.hash() % interval as u64) as i64;
        // Evaluation times differ from perfect intervals by `offset`; subtract it first so the result is in the
        // past, align, and add it back.
        let adj_now = start_time - offset;
        let base = adj_now - adj_now % interval;
        Time::from_unix_nano(base + offset)
    }

    /// Go `CopyState`: copies the alerting and staleness state from `from`. Rules match by name and labels; with
    /// duplicates, the first matches the first and so on.
    pub fn copy_state(&self, from: &Group) {
        {
            let ft = from.times.lock().unwrap();
            let mut t = self.times.lock().unwrap();
            t.evaluation_time = ft.evaluation_time;
            t.last_evaluation = ft.last_evaluation;
        }
        let mut rule_map: HashMap<String, Vec<usize>> = HashMap::new();
        for (fi, fr) in from.rules.iter().enumerate() {
            rule_map.entry(name_and_labels(fr.as_ref())).or_default().push(fi);
        }
        let from_prev = from.series_in_previous_eval.lock().unwrap().clone();
        {
            let mut prev = self.series_in_previous_eval.lock().unwrap();
            for (i, rule) in self.rules.iter().enumerate() {
                let key = name_and_labels(rule.as_ref());
                let Some(indexes) = rule_map.get_mut(&key) else { continue };
                if indexes.is_empty() {
                    continue;
                }
                let fi = indexes.remove(0);
                if let Some(m) = from_prev.get(fi) {
                    prev[i] = m.clone();
                }
                let (Some(ar), Some(far)) = (rule.as_alerting(), from.rules[fi].as_alerting()) else { continue };
                let src = far.active_map().clone();
                ar.active_map().extend(src);
            }
        }
        // Handle deleted and unmatched duplicate rules.
        let mut stale = from.stale_series.lock().unwrap().clone();
        for (fi, fr) in from.rules.iter().enumerate() {
            let key = name_and_labels(fr.as_ref());
            if rule_map.get(&key).is_some_and(|l| !l.is_empty())
                && let Some(m) = from_prev.get(fi)
            {
                stale.extend(m.values().cloned());
            }
        }
        *self.stale_series.lock().unwrap() = stale;
    }

    /// Go `Eval`: one evaluation cycle of all rules, sequentially or in concurrent batches.
    pub fn eval(&self, ctx: &Context, ts: Time) {
        let samples_total = Mutex::new(0f64);
        let rule_query_offset = self.query_offset();
        let ctrl: Arc<dyn RuleConcurrencyController> =
            self.opts.rule_concurrency_controller.clone().unwrap_or_else(|| Arc::new(SequentialRuleEvalController));
        let batches = ctrl.split_group_into_batches(ctx, self);
        if batches.is_empty() {
            // Sequential evaluation when batches are not set.
            for (i, rule) in self.rules.iter().enumerate() {
                if self.done.is_closed() {
                    return;
                }
                self.eval_rule(ctx, i, rule, ts, rule_query_offset, &samples_total);
            }
        } else {
            for batch in &batches {
                let stopped = std::thread::scope(|s| {
                    for &rule_index in batch {
                        if self.done.is_closed() {
                            return true;
                        }
                        let rule = &self.rules[rule_index];
                        if batch.len() > 1 && ctrl.allow(ctx, self, rule) {
                            let ctrl = ctrl.clone();
                            let samples_total = &samples_total;
                            s.spawn(move || {
                                self.eval_rule(ctx, rule_index, rule, ts, rule_query_offset, samples_total);
                                ctrl.done(ctx);
                            });
                        } else {
                            self.eval_rule(ctx, rule_index, rule, ts, rule_query_offset, &samples_total);
                        }
                    }
                    false
                });
                // Every rule of this batch finished before the next batch starts.
                if stopped {
                    return;
                }
            }
        }
        let total = *samples_total.lock().unwrap();
        self.metrics.group_samples.with_label_values(&[&group_key(&self.file, &self.name)]).set(total);
        self.cleanup_stale_series(ctx, ts);
    }

    fn eval_rule(&self, ctx: &Context, i: usize, rule: &RuleRef, ts: Time, rule_query_offset: Duration, samples_total: &Mutex<f64>) {
        let i_s = i.to_string();
        let logger = self.logger.with(&[("name", rule.name()), ("index", &i_s)]);
        let start = Instant::now();
        let start_time = Time::now();
        let key = group_key(&self.file, &self.name);
        let _finish = FinishEval { g: self, rule, start, start_time };

        self.metrics.eval_total.with_label_values(&[&key]).inc();

        let query_func = self.opts.query_func.clone().expect("rules: ManagerOptions.query_func is not set");
        let vector = match rule.eval(ctx, rule_query_offset, ts, &query_func, self.opts.external_url.as_ref(), self.limit) {
            Ok(v) => v,
            Err(err) => {
                rule.set_health(crate::RuleHealth::Bad);
                rule.set_last_error(Some(err.clone()));
                self.metrics.eval_failures.with_label_values(&[&key]).inc();
                // Canceled queries are intentional terminations (normally at shutdown): not logged.
                if !is_query_canceled(&err) {
                    logger.warn("Evaluating rule failed", &[("rule", &rule.string()), ("err", &err.to_string())]);
                }
                return;
            }
        };
        rule.set_health(crate::RuleHealth::Good);
        rule.set_last_error(None);
        *samples_total.lock().unwrap() += vector.len() as f64;

        if let Some(ar) = rule.as_alerting()
            && let Some(notify) = &self.opts.notify_func
        {
            ar.send_alerts(ctx, ts, self.opts.resend_delay, self.interval, notify);
        }
        let (mut num_out_of_order, mut num_too_old, mut num_duplicates) = (0, 0, 0);

        let appendable = self.opts.appendable_v2.clone().expect("rules: ManagerOptions.appendable_v2 is not set");
        let mut app = appendable.appender_v2(ctx);
        let prev = self.series_in_previous_eval.lock().unwrap().get(i).cloned().unwrap_or_default();
        let mut series_returned: HashMap<Vec<u8>, Labels> = HashMap::with_capacity(prev.len());
        let app_opts = AppendV2Options { reject_out_of_order: true, ..Default::default() };

        for s in &vector {
            let fh = s.h.as_deref();
            let res = app.append(0, &s.metric, 0, s.t, s.f, None, fh, &app_opts);
            match res {
                Err(err) => {
                    rule.set_health(crate::RuleHealth::Bad);
                    rule.set_last_error(Some(err.clone()));
                    let sample = sample_string(s);
                    if err.is(&ERR_OUT_OF_ORDER_SAMPLE) {
                        num_out_of_order += 1;
                        logger.debug("Rule evaluation result discarded", &[("err", &err.to_string()), ("sample", &sample)]);
                    } else if err.is(&ERR_TOO_OLD_SAMPLE) {
                        num_too_old += 1;
                        logger.debug("Rule evaluation result discarded", &[("err", &err.to_string()), ("sample", &sample)]);
                    } else if err.is(&ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP) {
                        num_duplicates += 1;
                        logger.debug("Rule evaluation result discarded", &[("err", &err.to_string()), ("sample", &sample)]);
                    } else {
                        logger.warn("Rule evaluation result discarded", &[("err", &err.to_string()), ("sample", &sample)]);
                    }
                }
                Ok(_) => {
                    series_returned.insert(s.metric.data().to_vec(), s.metric.clone());
                }
            }
        }
        if num_out_of_order > 0 {
            logger.warn("Error on ingesting out-of-order result from rule evaluation", &[("num_dropped", &num_out_of_order.to_string())]);
        }
        if num_too_old > 0 {
            logger.warn("Error on ingesting too old result from rule evaluation", &[("num_dropped", &num_too_old.to_string())]);
        }
        if num_duplicates > 0 {
            logger.warn(
                "Error on ingesting results from rule evaluation with different value but same timestamp",
                &[("num_dropped", &num_duplicates.to_string())],
            );
        }

        for (metric, lset) in &prev {
            if !series_returned.contains_key(metric) {
                // Series no longer exposed, mark it stale.
                let r = app.append(0, lset, 0, ts.sub_dur(rule_query_offset).millis(), f64::from_bits(STALE_NAN), None, None, &app_opts);
                if let Err(err) = r
                    && !(err.is(&ERR_OUT_OF_ORDER_SAMPLE) || err.is(&ERR_TOO_OLD_SAMPLE) || err.is(&ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP))
                {
                    // Out-of-order, too old and duplicate errors are expected if the series is exposed by another
                    // rule.
                    logger.warn("Adding stale sample failed", &[("sample", &lset.to_string()), ("err", &err.to_string())]);
                }
            }
        }

        if let Some(err) = app.commit() {
            rule.set_health(crate::RuleHealth::Bad);
            rule.set_last_error(Some(err.clone()));
            self.metrics.eval_failures.with_label_values(&[&key]).inc();
            logger.warn("Rule sample appending failed", &[("err", &err.to_string())]);
            return;
        }
        if let Some(slot) = self.series_in_previous_eval.lock().unwrap().get_mut(i) {
            *slot = series_returned;
        }
    }

    /// Go `QueryOffset`: the group's offset, else the manager default, else zero.
    pub fn query_offset(&self) -> Duration {
        if let Some(o) = self.query_offset {
            return o;
        }
        if let Some(f) = &self.opts.default_rule_query_offset {
            return f();
        }
        Duration::ZERO
    }

    fn cleanup_stale_series(&self, ctx: &Context, ts: Time) {
        let stale = self.stale_series.lock().unwrap().clone();
        if stale.is_empty() {
            return;
        }
        let Some(appendable) = self.opts.appendable_v2.clone() else { return };
        let mut app = appendable.appender_v2(ctx);
        let query_offset = self.query_offset();
        let app_opts = AppendV2Options { reject_out_of_order: true, ..Default::default() };
        for s in &stale {
            // The rule that produced the series is no longer configured: mark it stale.
            let r = app.append(0, s, 0, ts.sub_dur(query_offset).millis(), f64::from_bits(STALE_NAN), None, None, &app_opts);
            if let Err(err) = r
                && !(err.is(&ERR_OUT_OF_ORDER_SAMPLE) || err.is(&ERR_TOO_OLD_SAMPLE) || err.is(&ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP))
            {
                self.logger.warn("Adding stale sample for previous configuration failed", &[("sample", &s.to_string()), ("err", &err.to_string())]);
            }
        }
        match app.commit() {
            Some(err) => self.logger.warn("Stale sample appending for previous configuration failed", &[("err", &err.to_string())]),
            None => self.stale_series.lock().unwrap().clear(),
        }
    }

    /// Go `RestoreForState`: restores the 'for' state of alerts from the last ActiveAt in storage.
    pub fn restore_for_state(&self, ts: Time) {
        struct Done<'a>(&'a Group, Time, Instant);
        impl Drop for Done<'_> {
            fn drop(&mut self) {
                let secs = self.2.elapsed().as_secs_f64();
                let g = self.0;
                g.metrics.group_last_restore_duration.with_label_values(&[&group_key(&g.file, &g.name)]).set(secs);
                g.logger.debug("'for' state restoration completed", &[("duration_seconds", &secs.to_string())]);
                g.should_restore.store(false, Ordering::SeqCst);
                let _ = self.1;
            }
        }
        let _done = Done(self, ts, Instant::now());
        let maxt_ms = ts.unix_nano().div_euclid(1_000_000);
        // Restoration is allowed only for alerts that were active after a certain time.
        let mint_ms = ts.sub_dur(self.opts.outage_tolerance).unix_nano().div_euclid(1_000_000);
        let Some(queryable) = self.opts.queryable.clone() else { return };
        let mut q = match queryable.querier(mint_ms, maxt_ms) {
            Ok(q) => q,
            Err(err) => {
                self.logger.error("Failed to get Querier", &[("err", &err.to_string())]);
                return;
            }
        };

        for rule in &self.rules {
            let Some(alert_rule) = rule.as_alerting() else { continue };
            let hold = dur_nanos(alert_rule.hold_duration());
            let mut sset = match alert_rule.query_for_state_series(&self.opts.context, q.as_ref()) {
                Ok(s) => s,
                Err(err) => {
                    self.logger.error(
                        "Failed to restore 'for' state",
                        &[(labels::ALERT_NAME, alert_rule.name()), ("stage", "Select"), ("err", &err.to_string())],
                    );
                    // There is no way to retry the restore: mark the rule as restored and let it fire eventually.
                    alert_rule.set_restored(true);
                    continue;
                }
            };
            let mut series_by_labels: HashMap<String, storage::SeriesArc> = HashMap::with_capacity(alert_rule.active_alerts_count());
            while sset.next() {
                let s = sset.at();
                let key = s.labels().drop_reserved(|n| n == labels::METRIC_NAME).to_string();
                series_by_labels.insert(key, s.clone());
            }
            if series_by_labels.is_empty() {
                self.logger.debug("No series found to restore the 'for' state of the alert rule", &[(labels::ALERT_NAME, alert_rule.name())]);
                alert_rule.set_restored(true);
                continue;
            }
            let grace = dur_nanos(self.opts.for_grace_period);
            alert_rule.for_each_active_alert(|a| {
                let Some(s) = series_by_labels.get(&a.labels.to_string()) else { return };
                // Series found for the 'for' state.
                let (mut t, mut v) = (0i64, 0f64);
                let mut it = s.iterator(None);
                while it.next() == chunkenc::ValueType::Float {
                    (t, v) = it.at();
                }
                if let Some(err) = it.err() {
                    self.logger.error(
                        "Failed to restore 'for' state",
                        &[(labels::ALERT_NAME, alert_rule.name()), ("stage", "Iterator"), ("err", &err.to_string())],
                    );
                    return;
                }
                if is_stale_nan(v) {
                    // The alert was not active.
                    return;
                }
                let down_at = Time::unix(t / 1000, 0);
                let mut restored_active_at = Time::unix(v as i64, 0);
                let time_spent_pending = down_at.sub(restored_active_at);
                let time_remaining_pending = hold - time_spent_pending;
                if time_remaining_pending <= 0 {
                    // The alert was firing before the outage. Keep ActiveAt so the next Eval fires it.
                } else if hold < grace {
                    // Pending alerts with a hold duration shorter than the grace period restart their hold
                    // duration.
                    return;
                } else if time_remaining_pending < grace {
                    restored_active_at = ts.add_nanos(grace).add_nanos(-hold);
                } else {
                    // Shift ActiveAt by the down time.
                    let down_duration = ts.sub(down_at);
                    restored_active_at = restored_active_at.add_nanos(down_duration);
                }
                a.active_at = restored_active_at;
                self.logger.debug(
                    "'for' state restored",
                    &[(labels::ALERT_NAME, alert_rule.name()), ("restored_time", &a.active_at.format_rfc3339()), ("labels", &a.labels.to_string())],
                );
            });
            alert_rule.set_restored(true);
        }
        if let Some(err) = q.close() {
            self.logger.error("Failed to close Querier", &[("err", &err.to_string())]);
        }
    }

    /// Go `Equals`.
    pub fn equals(&self, ng: &Group) -> bool {
        if self.name != ng.name || self.file != ng.file || self.interval != ng.interval || self.limit != ng.limit {
            return false;
        }
        if self.query_offset != ng.query_offset {
            return false;
        }
        if self.rules.len() != ng.rules.len() {
            return false;
        }
        self.rules.iter().zip(&ng.rules).all(|(a, b)| a.string() == b.string())
    }
}

/// Go `errors.AsType[promql.ErrQueryCanceled]`.
fn is_query_canceled(err: &Error) -> bool {
    matches!(promql::PromqlError::find(err), Some(promql::PromqlError::QueryCanceled(_)))
}

/// Go's `%v` of a `promql.Sample` in logs.
fn sample_string(s: &promql::Sample) -> String {
    s.to_string()
}

struct FinishEval<'a> {
    g: &'a Group,
    rule: &'a RuleRef,
    start: Instant,
    start_time: Time,
}

impl Drop for FinishEval<'_> {
    fn drop(&mut self) {
        let since = self.start.elapsed();
        self.g.metrics.eval_duration.get().observe(since.as_secs_f64());
        self.g.metrics.eval_duration_histogram.get().observe(since.as_secs_f64());
        self.rule.set_evaluation_duration(since);
        self.rule.set_evaluation_timestamp(self.start_time);
    }
}

struct TerminateOnDrop(Arc<Signal>);
impl Drop for TerminateOnDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}

/// On the exit of `run`: if the group is marked stale, waits two intervals (or for the manager to stop) and then
/// writes staleness markers for all series of the group (Go's deferred goroutine).
struct MarkStaleOnDrop(Arc<Group>, Context);
impl Drop for MarkStaleOnDrop {
    fn drop(&mut self) {
        let g = self.0.clone();
        if !g.mark_stale.load(Ordering::SeqCst) {
            return;
        }
        let ctx = self.1.clone();
        let now = Time::now();
        std::thread::spawn(move || {
            {
                let mut prev = g.series_in_previous_eval.lock().unwrap();
                let mut stale = g.stale_series.lock().unwrap();
                for m in prev.iter() {
                    stale.extend(m.values().cloned());
                }
                // Can be freed now.
                prev.clear();
            }
            // Wait for 2 intervals to give renamed rules the opportunity to insert new series.
            let deadline = Instant::now() + 2 * g.interval;
            let manager_stopped = match &g.manager_done {
                Some(d) => d.wait_until(deadline),
                None => {
                    std::thread::sleep(2 * g.interval);
                    false
                }
            };
            if !manager_stopped {
                g.cleanup_stale_series(&ctx, now);
            }
        });
    }
}

/// A Go `time.Ticker` started now: ticks at every interval; one tick is buffered if the receiver is late.
struct Ticker {
    next: Instant,
    interval: Duration,
}

impl Ticker {
    fn new(interval: Duration) -> Ticker {
        Ticker { next: Instant::now() + interval, interval }
    }
    /// Waits for the next tick; false if `done` was closed first.
    fn wait(&mut self, done: &Signal) -> bool {
        if done.wait_until(self.next) {
            return false;
        }
        let now = Instant::now();
        self.next += self.interval;
        if self.next <= now {
            // Dropped ticks: the next one comes at the next multiple of the interval.
            let behind = (now - self.next).as_nanos() / self.interval.as_nanos().max(1) + 1;
            self.next += self.interval * behind as u32;
        }
        true
    }
}

fn deadline_for(t: Time) -> Instant {
    let d = t.sub(Time::now());
    if d <= 0 { Instant::now() } else { Instant::now() + Duration::from_nanos(d as u64) }
}

fn name_and_labels(rule: &dyn Rule) -> String {
    format!("{}{}", rule.name(), rule.labels())
}

fn matches(lbls: &Labels, matchers: &[Matcher]) -> bool {
    matchers.iter().all(|m| m.matches(lbls.get(&m.name)))
}

/// Go `matchesMatcherSets`: the matchers of a set are ANDed, the sets ORed.
pub fn matches_matcher_sets(matcher_sets: &[Vec<Matcher>], lbls: &Labels) -> bool {
    if matcher_sets.is_empty() {
        return true;
    }
    matcher_sets.iter().any(|ms| matches(lbls, ms))
}

/// Go `GroupKey`: group names need not be unique across files.
pub fn group_key(file: &str, name: &str) -> String {
    format!("{file};{name}")
}

/// Go `Metrics`: the metrics of rule evaluation.
pub struct Metrics {
    pub eval_duration: Arc<SummaryVec>,
    pub eval_duration_histogram: Arc<HistogramVec>,
    pub iteration_duration: Arc<SummaryVec>,
    pub iteration_duration_histogram: Arc<HistogramVec>,
    pub iterations_missed: Arc<CounterVec>,
    pub iterations_scheduled: Arc<CounterVec>,
    pub eval_total: Arc<CounterVec>,
    pub eval_failures: Arc<CounterVec>,
    pub group_interval: Arc<GaugeVec>,
    pub group_last_eval_time: Arc<GaugeVec>,
    pub group_last_duration: Arc<GaugeVec>,
    pub group_last_rule_duration_sum: Arc<GaugeVec>,
    pub group_last_restore_duration: Arc<GaugeVec>,
    pub group_rules: Arc<GaugeVec>,
    pub group_samples: Arc<GaugeVec>,
}

impl Metrics {
    /// Go `NewGroupMetrics`: creates the metrics and registers them with `reg` if given. Panics if they are
    /// already registered (Go `MustRegister`).
    pub fn new(reg: Option<&Arc<Registry>>) -> Arc<Metrics> {
        let o = Opts::new;
        let g = |name: &str, help: &str| GaugeVec::new_gauge_vec(o(name, help), &["rule_group"]);
        let c = |name: &str, help: &str| CounterVec::new_counter_vec(o(name, help), &["rule_group"]);
        let m = Metrics {
            eval_duration: SummaryVec::new_summary(o("prometheus_rule_evaluation_duration_seconds", "The duration for a rule to execute.")),
            eval_duration_histogram: {
                let h = HistogramVec::new_histogram_vec(
                    o("prometheus_rule_evaluation_duration_histogram_seconds", "The duration for a rule to execute."),
                    &[],
                    &[0.01, 0.1, 1.0, 10.0],
                );
                h.get();
                h
            },
            iteration_duration: SummaryVec::new_summary(o("prometheus_rule_group_duration_seconds", "The duration of rule group evaluations.")),
            iteration_duration_histogram: {
                let h = HistogramVec::new_histogram_vec(
                    o("prometheus_rule_group_duration_histogram_seconds", "The duration of rule group evaluations."),
                    &[],
                    &[0.01, 0.1, 1.0, 10.0],
                );
                h.get();
                h
            },
            iterations_missed: c(
                "prometheus_rule_group_iterations_missed_total",
                "The total number of rule group evaluations missed due to slow rule group evaluation.",
            ),
            iterations_scheduled: c(
                "prometheus_rule_group_iterations_total",
                "The total number of scheduled rule group evaluations, whether executed or missed.",
            ),
            eval_total: c("prometheus_rule_evaluations_total", "The total number of rule evaluations."),
            eval_failures: c("prometheus_rule_evaluation_failures_total", "The total number of rule evaluation failures."),
            group_interval: g("prometheus_rule_group_interval_seconds", "The interval of a rule group."),
            group_last_eval_time: g(
                "prometheus_rule_group_last_evaluation_timestamp_seconds",
                "The timestamp of the last rule group evaluation in seconds.",
            ),
            group_last_duration: g("prometheus_rule_group_last_duration_seconds", "The duration of the last rule group evaluation."),
            group_last_rule_duration_sum: g(
                "prometheus_rule_group_last_rule_duration_sum_seconds",
                "The sum of time in seconds it took to evaluate each rule in the group regardless of concurrency. This should be higher than the group duration if rules are evaluated concurrently.",
            ),
            group_last_restore_duration: g(
                "prometheus_rule_group_last_restore_duration_seconds",
                "The duration of the last alert rules alerts restoration using the `ALERTS_FOR_STATE` series.",
            ),
            group_rules: g("prometheus_rule_group_rules", "The number of rules."),
            group_samples: g("prometheus_rule_group_last_evaluation_samples", "The number of samples returned during the last rule group evaluation."),
        };
        if let Some(reg) = reg {
            let cs: Vec<Arc<dyn discovery::metrics::Collector>> = vec![
                m.eval_duration.clone(),
                m.eval_duration_histogram.clone(),
                m.iteration_duration.clone(),
                m.iteration_duration_histogram.clone(),
                m.iterations_missed.clone(),
                m.iterations_scheduled.clone(),
                m.eval_total.clone(),
                m.eval_failures.clone(),
                m.group_interval.clone(),
                m.group_last_eval_time.clone(),
                m.group_last_duration.clone(),
                m.group_last_rule_duration_sum.clone(),
                m.group_last_restore_duration.clone(),
                m.group_rules.clone(),
                m.group_samples.clone(),
            ];
            for c in cs {
                if let Err(e) = reg.register(c) {
                    panic!("{e}");
                }
            }
        }
        Arc::new(m)
    }
}

// ---------------- dependency map ----------------

/// Go `dependencyMap`: for each rule, the rules that use its output. `None` means "indeterminate".
#[derive(Clone, Default)]
pub struct DependencyMap {
    entries: Vec<(RuleRef, Vec<RuleRef>)>,
}

impl DependencyMap {
    fn push(&mut self, rule: &RuleRef, dependent: &RuleRef) {
        match self.entries.iter_mut().find(|(r, _)| same_rule(r, rule)) {
            Some((_, v)) => v.push(dependent.clone()),
            None => self.entries.push((rule.clone(), vec![dependent.clone()])),
        }
    }
    /// The number of rules with dependents (Go `len(m)`).
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    /// Go `dependents`: the rules that use the output of `r`.
    pub fn dependents(&self, r: &RuleRef) -> Vec<RuleRef> {
        self.entries.iter().find(|(x, _)| same_rule(x, r)).map(|(_, v)| v.clone()).unwrap_or_default()
    }
    /// Go `dependencies`: the rules whose output `r` uses.
    pub fn dependencies(&self, r: &RuleRef) -> Vec<RuleRef> {
        self.entries.iter().filter(|(_, deps)| deps.iter().any(|d| same_rule(d, r))).map(|(x, _)| x.clone()).collect()
    }
    /// Go `isIndependent`.
    pub fn is_independent(&self, r: &RuleRef) -> bool {
        self.dependents(r).is_empty() && self.dependencies(r).is_empty()
    }
}

/// Go `isIndependent` on a possibly nil map (nil is never independent).
pub fn is_independent(m: Option<&DependencyMap>, r: &RuleRef) -> bool {
    m.is_some_and(|m| m.is_independent(r))
}

/// Go `buildDependencyMap`: which rules of a group use the output of which others. `None` when a rule has a
/// selector without a metric name, so no relationships can be inferred.
pub fn build_dependency_map(rules: &[RuleRef]) -> Option<DependencyMap> {
    let mut dependencies = DependencyMap::default();
    if rules.len() <= 1 {
        // No relationships if the group has one rule or less.
        return Some(dependencies);
    }
    let mut indeterminate = false;
    for rule in rules {
        if indeterminate {
            break;
        }
        let Some(q) = rule.query() else { continue };
        let _ = inspect(q, |node: &Expr, _path: &[&Expr]| -> Result<(), ()> {
            let Expr::VectorSelector(n) = node else { return Ok(()) };
            // Find the name matcher of the selector.
            let name_matcher = if !n.name.is_empty() {
                Matcher::new(MatchType::Equal, labels::METRIC_NAME, &n.name).ok()
            } else {
                n.label_matchers.iter().find(|m| m.name == labels::METRIC_NAME).cloned()
            };
            // A wildcard metric expression: we cannot know whether this rule depends on others, so no rules can
            // run concurrently.
            let Some(name_matcher) = name_matcher else {
                indeterminate = true;
                return Ok(());
            };
            // Is the selector querying the ALERTS and ALERTS_FOR_STATE meta-metrics, and with which alertname?
            let name_matches_alerts = name_matcher.matches(ALERT_METRIC_NAME) || name_matcher.matches(ALERT_FOR_STATE_METRIC_NAME);
            let alerts_name_matcher =
                if name_matches_alerts { n.label_matchers.iter().find(|m| m.name == labels::ALERT_NAME) } else { None };
            for other in rules {
                // Only rules defined before this one can be dependencies.
                if same_rule(other, rule) {
                    break;
                }
                let other_name = other.name();
                if name_matcher.matches(other_name) {
                    dependencies.push(other, rule);
                    continue;
                }
                if name_matches_alerts
                    && other.as_alerting().is_some()
                    && alerts_name_matcher.is_none_or(|m| m.matches(other_name))
                {
                    dependencies.push(other, rule);
                }
            }
            Ok(())
        });
    }
    if indeterminate {
        return None;
    }
    Some(dependencies)
}

/// Go `RuleConcurrencyController`: bounds concurrent rule evaluation.
pub trait RuleConcurrencyController: Send + Sync {
    /// The ordered batches of rule indexes; the rules of a batch may run concurrently.
    fn split_group_into_batches(&self, ctx: &Context, group: &Group) -> Vec<Vec<usize>>;
    /// Whether the rule may run concurrently now. If true, `done` must be called after it.
    fn allow(&self, ctx: &Context, group: &Group, rule: &RuleRef) -> bool;
    /// Releases a concurrent evaluation slot.
    fn done(&self, ctx: &Context);
}
