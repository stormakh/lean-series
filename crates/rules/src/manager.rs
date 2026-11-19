//! Port of `rules/manager.go`: the rule manager, rule loading, query and notify functions, and the rule
//! dependency and concurrency controllers.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use discovery::metrics::Registry;
use model::labels::{self, Labels, ValidationScheme};
use model::matcher::Matcher;
use promql::features::Collector;
use promql::parser::{Expr, Parser};
use promql::{QueryResult, Sample, Value, Vector};
use storage::{AppendableV2, Context, Error, Queryable};

use crate::alerting::{Alert, AlertingRule, NotifyFunc};
use crate::group::{build_dependency_map, Group, GroupEvalIterationFunc, GroupOptions, Metrics, RuleConcurrencyController, Signal};
use crate::log::Logger;
use crate::recording::RecordingRule;
use crate::rule::{QueryFunc, RuleRef, Time};
use crate::rulefmt::{self, BoxError, RuleGroups};

/// The query engine interface that [`engine_query_func`] needs (Go `promql.QueryEngine`).
pub trait QueryEngine: Send + Sync {
    /// Go `NewInstantQuery` (`ts` in milliseconds).
    fn new_instant_query(&self, ctx: &Context, q: Arc<dyn Queryable>, qs: &str, ts: i64) -> Result<Box<dyn EngineQuery>, Error>;
}

/// A prepared query (Go `promql.Query`).
pub trait EngineQuery {
    fn exec(&mut self, ctx: &Context) -> QueryResult;
    fn close(&mut self);
}

impl QueryEngine for promql::Engine {
    fn new_instant_query(&self, ctx: &Context, q: Arc<dyn Queryable>, qs: &str, ts: i64) -> Result<Box<dyn EngineQuery>, Error> {
        Ok(Box::new(promql::Engine::new_instant_query(self, ctx, q, None, qs, ts)?))
    }
}

impl EngineQuery for promql::Query {
    fn exec(&mut self, ctx: &Context) -> QueryResult {
        promql::Query::exec(self, ctx)
    }
    fn close(&mut self) {
        promql::Query::close(self)
    }
}

/// Go `EngineQueryFunc`: runs instant queries against the engine; scalar results become one-sample vectors.
pub fn engine_query_func<E: QueryEngine + ?Sized + 'static>(engine: Arc<E>, q: Arc<dyn Queryable>) -> QueryFunc {
    Arc::new(move |ctx: &Context, qs: &str, t: Time| -> Result<Vector, Error> {
        let mut query = engine.new_instant_query(ctx, q.clone(), qs, t.millis())?;
        let res = query.exec(ctx);
        query.close();
        if let Some(err) = res.err {
            return Err(err);
        }
        match res.value {
            Some(Value::Vector(v)) => Ok(v),
            Some(Value::Scalar(s)) => Ok(vec![Sample { t: s.t, f: s.v, h: None, metric: Labels::empty(), drop_name: false }]),
            _ => Err(Error::msg("rule result is not a vector or scalar")),
        }
    })
}

/// Go `DefaultEvalIterationFunc`: evaluates the group at `eval_timestamp` and updates its state and metrics.
pub fn default_eval_iteration_func(ctx: &Context, g: &Group, eval_timestamp: Time) {
    let key = crate::group::group_key(g.file(), g.name());
    g.metrics().iterations_scheduled.with_label_values(&[&key]).inc();
    let start = Instant::now();
    let start_time = Time::now();
    g.eval(ctx, eval_timestamp);
    let since = start.elapsed();
    g.metrics().iteration_duration.get().observe(since.as_secs_f64());
    g.metrics().iteration_duration_histogram.get().observe(since.as_secs_f64());
    g.update_rule_evaluation_time_sum();
    g.set_evaluation_time(since);
    g.set_last_evaluation(start_time);
    g.set_last_eval_timestamp(eval_timestamp);
}

/// Go `GroupLoader`: loads and parses rule groups from some source.
pub trait GroupLoader: Send + Sync {
    fn load(&self, identifier: &str, ignore_unknown_fields: bool, scheme: ValidationScheme) -> (Option<RuleGroups>, Vec<BoxError>);
    fn parse(&self, query: &str) -> Result<Expr, Error>;
}

/// Go `FileLoader`: loads rule files with rulefmt and parses expressions with the configured parser.
pub struct FileLoader {
    pub parser: Parser,
    pub logger: Logger,
}

impl GroupLoader for FileLoader {
    fn load(&self, identifier: &str, ignore_unknown_fields: bool, scheme: ValidationScheme) -> (Option<RuleGroups>, Vec<BoxError>) {
        rulefmt::parse_file(identifier, ignore_unknown_fields, scheme, &self.parser, &self.logger)
    }
    fn parse(&self, query: &str) -> Result<Expr, Error> {
        self.parser.parse_expr(query).map_err(Error::other)
    }
}

/// Go `RuleDependencyController`.
pub trait RuleDependencyController: Send + Sync {
    /// Analyses dependencies between the rules and records them with `set_dependent_rules` and
    /// `set_dependency_rules`.
    fn analyse_rules(&self, rules: &[RuleRef]);
}

/// Go `ruleDependencyController`.
#[derive(Default, Clone, Copy)]
pub struct DefaultRuleDependencyController;

impl RuleDependencyController for DefaultRuleDependencyController {
    fn analyse_rules(&self, rules: &[RuleRef]) {
        let Some(dep_map) = build_dependency_map(rules) else { return };
        for r in rules {
            r.set_dependent_rules(&dep_map.dependents(r));
            r.set_dependency_rules(&dep_map.dependencies(r));
        }
    }
}

/// Go `concurrentRuleEvalController`: a weighted semaphore over concurrent rule evaluations.
pub struct ConcurrentRuleEvalController {
    max: i64,
    used: AtomicI64,
}

/// Go `newRuleConcurrencyController`.
pub fn new_rule_concurrency_controller(max_concurrency: i64) -> Arc<dyn RuleConcurrencyController> {
    Arc::new(ConcurrentRuleEvalController { max: max_concurrency, used: AtomicI64::new(0) })
}

impl RuleConcurrencyController for ConcurrentRuleEvalController {
    fn split_group_into_batches(&self, _ctx: &Context, g: &Group) -> Vec<Vec<usize>> {
        // 1. Concurrently, all rules without dependencies; 2. sequentially, rules with dependencies and
        // dependents; 3. concurrently, all rules without dependents.
        let (mut no_dependencies, mut both, mut no_dependants) = (Vec::new(), Vec::new(), Vec::new());
        for (i, r) in g.all_rules().iter().enumerate() {
            if r.no_dependency_rules() {
                no_dependencies.push(i);
            } else if !r.no_dependent_rules() && !r.no_dependency_rules() {
                both.push(i);
            } else if r.no_dependent_rules() {
                no_dependants.push(i);
            }
        }
        let mut order = Vec::new();
        if !no_dependencies.is_empty() {
            order.push(no_dependencies);
        }
        for r in both {
            order.push(vec![r]);
        }
        if !no_dependants.is_empty() {
            order.push(no_dependants);
        }
        order
    }
    fn allow(&self, _ctx: &Context, _g: &Group, _r: &RuleRef) -> bool {
        // Go `semaphore.TryAcquire(1)`.
        let mut cur = self.used.load(Ordering::SeqCst);
        loop {
            if cur + 1 > self.max {
                return false;
            }
            match self.used.compare_exchange(cur, cur + 1, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return true,
                Err(c) => cur = c,
            }
        }
    }
    fn done(&self, _ctx: &Context) {
        if self.used.fetch_sub(1, Ordering::SeqCst) <= 0 {
            panic!("semaphore: released more than held");
        }
    }
}

/// Go `sequentialRuleEvalController`: every rule runs sequentially.
#[derive(Default, Clone, Copy)]
pub struct SequentialRuleEvalController;

impl RuleConcurrencyController for SequentialRuleEvalController {
    fn split_group_into_batches(&self, _ctx: &Context, _g: &Group) -> Vec<Vec<usize>> {
        Vec::new()
    }
    fn allow(&self, _ctx: &Context, _g: &Group, _r: &RuleRef) -> bool {
        false
    }
    fn done(&self, _ctx: &Context) {}
}

/// Go `ManagerOptions`.
#[derive(Clone)]
pub struct ManagerOptions {
    pub name_validation_scheme: ValidationScheme,
    /// The external URL (`--web.external-url`), as a string.
    pub external_url: Option<String>,
    pub query_func: Option<QueryFunc>,
    pub notify_func: Option<NotifyFunc>,
    pub context: Context,
    pub appendable_v2: Option<Arc<dyn AppendableV2>>,
    pub queryable: Option<Arc<dyn Queryable>>,
    pub logger: Logger,
    pub registerer: Option<Arc<Registry>>,
    pub outage_tolerance: Duration,
    pub for_grace_period: Duration,
    pub resend_delay: Duration,
    pub group_loader: Option<Arc<dyn GroupLoader>>,
    pub default_rule_query_offset: Option<Arc<dyn Fn() -> Duration + Send + Sync>>,
    pub max_concurrent_evals: i64,
    pub concurrent_evals_enabled: bool,
    pub rule_concurrency_controller: Option<Arc<dyn RuleConcurrencyController>>,
    pub rule_dependency_controller: Option<Arc<dyn RuleDependencyController>>,
    /// Restore the 'for' state also for groups added to a running manager.
    pub restore_new_rule_groups: bool,
    pub metrics: Option<Arc<Metrics>>,
    /// Registers the rule manager features.
    pub feature_registry: Option<Arc<dyn Collector + Send + Sync>>,
    /// The PromQL parser for rule expressions.
    pub parser: Option<Parser>,
}

impl Default for ManagerOptions {
    fn default() -> ManagerOptions {
        ManagerOptions {
            name_validation_scheme: ValidationScheme::Unset,
            external_url: None,
            query_func: None,
            notify_func: None,
            context: Context::background(),
            appendable_v2: None,
            queryable: None,
            logger: Logger::nop(),
            registerer: None,
            outage_tolerance: Duration::ZERO,
            for_grace_period: Duration::ZERO,
            resend_delay: Duration::ZERO,
            group_loader: None,
            default_rule_query_offset: None,
            max_concurrent_evals: 0,
            concurrent_evals_enabled: false,
            rule_concurrency_controller: None,
            rule_dependency_controller: None,
            restore_new_rule_groups: false,
            metrics: None,
            feature_registry: None,
            parser: None,
        }
    }
}

struct ManagerState {
    groups: HashMap<String, Arc<Group>>,
    restored: bool,
}

/// Go `Manager`: manages recording and alerting rules.
pub struct Manager {
    opts: Arc<ManagerOptions>,
    state: RwLock<ManagerState>,
    block: Arc<Signal>,
    done: Arc<Signal>,
    restore_new_rule_groups: bool,
    logger: Logger,
    threads: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl Manager {
    /// Go `NewManager`: fills the defaults of the options.
    pub fn new(mut o: ManagerOptions) -> Manager {
        if o.name_validation_scheme == ValidationScheme::Unset {
            o.name_validation_scheme = ValidationScheme::Utf8;
        }
        if o.metrics.is_none() {
            o.metrics = Some(Metrics::new(o.registerer.as_ref()));
        }
        if o.parser.is_none() {
            o.parser = Some(Parser::new(promql::parser::Options::default()));
        }
        if o.group_loader.is_none() {
            o.group_loader = Some(Arc::new(FileLoader { parser: o.parser.clone().unwrap(), logger: o.logger.clone() }));
        }
        if o.rule_concurrency_controller.is_none() {
            o.rule_concurrency_controller = Some(if o.concurrent_evals_enabled {
                new_rule_concurrency_controller(o.max_concurrent_evals)
            } else {
                Arc::new(SequentialRuleEvalController)
            });
        }
        if o.rule_dependency_controller.is_none() {
            o.rule_dependency_controller = Some(Arc::new(DefaultRuleDependencyController));
        }
        if let Some(fr) = &o.feature_registry {
            fr.set("rules", "concurrent_rule_eval", o.concurrent_evals_enabled);
            fr.enable("rules", "query_offset");
            fr.enable("rules", "keep_firing_for");
        }
        let logger = o.logger.clone();
        let restore_new_rule_groups = o.restore_new_rule_groups;
        Manager {
            opts: Arc::new(o),
            state: RwLock::new(ManagerState { groups: HashMap::new(), restored: false }),
            block: Signal::new(),
            done: Signal::new(),
            restore_new_rule_groups,
            logger,
            threads: Mutex::new(Vec::new()),
        }
    }

    /// The options with defaults filled in.
    pub fn opts(&self) -> &Arc<ManagerOptions> {
        &self.opts
    }

    /// Go `Run`: starts rule evaluation and blocks until `stop`.
    pub fn run(&self) {
        self.logger.info("Starting rule manager...", &[]);
        self.start();
        self.done.wait();
    }

    /// Go `start`: lets the groups start evaluating.
    pub fn start(&self) {
        self.block.close();
    }

    /// Go `Stop`: stops all groups and waits for them.
    pub fn stop(&self) {
        let st = self.state.write().unwrap();
        self.logger.info("Stopping rule manager...", &[]);
        // Stop all groups asynchronously, then wait for them to finish.
        for g in st.groups.values() {
            g.stop_async();
        }
        for g in st.groups.values() {
            g.wait_stopped();
        }
        // Shut down the groups waiting multiple evaluation intervals to write staleness markers.
        self.done.close();
        self.logger.info("Rule manager stopped", &[]);
        drop(st);
        let threads = std::mem::take(&mut *self.threads.lock().unwrap());
        for t in threads {
            let _ = t.join();
        }
    }

    /// Go `Update`: loads the rule files and replaces the groups. If loading fails the old rule set stays. A
    /// no-op once the manager is stopped.
    pub fn update(
        &self,
        interval: Duration,
        files: &[String],
        external_labels: &Labels,
        external_url: &str,
        group_eval_iteration_func: Option<GroupEvalIterationFunc>,
    ) -> Result<(), Error> {
        let mut st = self.state.write().unwrap();
        // We cannot update a stopped manager.
        if self.done.is_closed() {
            return Ok(());
        }
        let mut groups = match self.load_groups_locked(&st, interval, external_labels, external_url, group_eval_iteration_func, false, files) {
            Ok(g) => g,
            Err(errs) => {
                for e in errs {
                    self.logger.error("loading groups failed", &[("err", &e.to_string())]);
                }
                return Err(Error::msg("error loading rules, previous rule set restored"));
            }
        };
        st.restored = true;

        let mut handles = Vec::new();
        let mut ready = Vec::new();
        let keys: Vec<String> = groups.keys().cloned().collect();
        for gn in keys {
            let newg = groups.get(&gn).unwrap().clone();
            // An old group with the same identifier is kept if equal; otherwise it is stopped and its state is
            // copied into the new group.
            let oldg = st.groups.remove(&gn);
            if let Some(o) = &oldg
                && o.equals(&newg)
            {
                groups.insert(gn, o.clone());
                continue;
            }
            let block = self.block.clone();
            let ctx = self.opts.context.clone();
            let started = Signal::new();
            ready.push(started.clone());
            handles.push(std::thread::spawn(move || {
                if let Some(o) = oldg {
                    o.stop();
                    newg.copy_state(&o);
                }
                started.close();
                // Wait with starting evaluation until the manager runs, to not query a bootstrapping storage.
                block.wait();
                newg.run(&ctx);
            }));
        }

        // Stop the remaining old groups.
        let old: Vec<(String, Arc<Group>)> = st.groups.drain().collect();
        let stoppers: Vec<_> = old
            .into_iter()
            .map(|(n, g)| {
                std::thread::spawn(move || {
                    g.set_mark_stale(true);
                    g.stop();
                    let m = g.metrics();
                    let v = [n.as_str()];
                    m.iterations_missed.delete_label_values(&v);
                    m.iterations_scheduled.delete_label_values(&v);
                    m.eval_total.delete_label_values(&v);
                    m.eval_failures.delete_label_values(&v);
                    m.group_interval.delete_label_values(&v);
                    m.group_last_eval_time.delete_label_values(&v);
                    m.group_last_duration.delete_label_values(&v);
                    m.group_rules.delete_label_values(&v);
                    m.group_samples.delete_label_values(&v);
                    m.group_last_rule_duration_sum.delete_label_values(&v);
                    m.group_last_restore_duration.delete_label_values(&v);
                })
            })
            .collect();
        for r in ready {
            r.wait();
        }
        for s in stoppers {
            let _ = s.join();
        }
        st.groups = groups;
        let mut threads = self.threads.lock().unwrap();
        threads.retain(|t| !t.is_finished());
        threads.extend(handles);
        Ok(())
    }

    /// Go `LoadGroups`: reads groups from the files.
    #[allow(clippy::too_many_arguments)]
    pub fn load_groups(
        &self,
        interval: Duration,
        external_labels: &Labels,
        external_url: &str,
        group_eval_iteration_func: Option<GroupEvalIterationFunc>,
        ignore_unknown_fields: bool,
        filenames: &[String],
    ) -> Result<HashMap<String, Arc<Group>>, Vec<BoxError>> {
        let st = self.state.read().unwrap();
        self.load_groups_locked(&st, interval, external_labels, external_url, group_eval_iteration_func, ignore_unknown_fields, filenames)
    }

    #[allow(clippy::too_many_arguments)]
    fn load_groups_locked(
        &self,
        st: &ManagerState,
        interval: Duration,
        external_labels: &Labels,
        external_url: &str,
        group_eval_iteration_func: Option<GroupEvalIterationFunc>,
        ignore_unknown_fields: bool,
        filenames: &[String],
    ) -> Result<HashMap<String, Arc<Group>>, Vec<BoxError>> {
        let mut groups = HashMap::new();
        let should_restore = !st.restored || self.restore_new_rule_groups;
        let loader = self.opts.group_loader.clone().expect("group loader");
        for fn_ in filenames {
            let (rgs, errs) = loader.load(fn_, ignore_unknown_fields, self.opts.name_validation_scheme);
            if !errs.is_empty() {
                return Err(errs);
            }
            let Some(rgs) = rgs else { continue };
            for rg in &rgs.groups {
                let itv = if rg.interval.0 != 0 { Duration::from_nanos(rg.interval.0.max(0) as u64) } else { interval };
                let mut rules: Vec<RuleRef> = Vec::with_capacity(rg.rules.len());
                for r in &rg.rules {
                    let expr = match loader.parse(&r.expr) {
                        Ok(e) => e,
                        Err(err) => return Err(vec![rulefmt::wrap(fn_.clone(), Box::new(err))]),
                    };
                    let m_labels = from_maps(&[&rg.labels, &r.labels]);
                    if !r.alert.is_empty() {
                        rules.push(Arc::new(AlertingRule::new(
                            &r.alert,
                            expr,
                            Duration::from_nanos(r.r#for.0.max(0) as u64),
                            Duration::from_nanos(r.keep_firing_for.0.max(0) as u64),
                            m_labels,
                            labels::from_map(r.annotations.iter().map(|(k, v)| (k.as_str(), v.as_str()))),
                            external_labels.clone(),
                            external_url,
                            !should_restore,
                            self.logger.with(&[("alert", &r.alert)]),
                        )));
                        continue;
                    }
                    rules.push(Arc::new(RecordingRule::new(&r.record, expr, m_labels)));
                }
                // Check dependencies between rules and store them on the rules.
                self.opts.rule_dependency_controller.as_ref().expect("dependency controller").analyse_rules(&rules);

                groups.insert(
                    crate::group::group_key(fn_, &rg.name),
                    Arc::new(Group::new(GroupOptions {
                        name: rg.name.clone(),
                        file: fn_.clone(),
                        interval: itv,
                        limit: rg.limit,
                        rules,
                        should_restore,
                        opts: Some(self.opts.clone()),
                        query_offset: rg.query_offset.map(|d| Duration::from_nanos(d.0.max(0) as u64)),
                        done: Some(self.done.clone()),
                        eval_iteration_func: group_eval_iteration_func.clone(),
                    })),
                );
            }
        }
        Ok(groups)
    }

    /// Go `RuleGroups`: the groups sorted by file and name.
    pub fn rule_groups(&self) -> Vec<Arc<Group>> {
        let st = self.state.read().unwrap();
        let mut rgs: Vec<Arc<Group>> = st.groups.values().cloned().collect();
        rgs.sort_by(|a, b| a.file().cmp(b.file()).then_with(|| a.name().cmp(b.name())));
        rgs
    }

    /// The groups by key (Go `m.groups`).
    pub fn groups(&self) -> HashMap<String, Arc<Group>> {
        self.state.read().unwrap().groups.clone()
    }

    /// Go `Rules`.
    pub fn rules(&self, matcher_sets: &[Vec<Matcher>]) -> Vec<RuleRef> {
        let st = self.state.read().unwrap();
        st.groups.values().flat_map(|g| g.rules(matcher_sets)).collect()
    }

    /// Go `AlertingRules`.
    pub fn alerting_rules(&self) -> Vec<RuleRef> {
        self.rules(&[]).into_iter().filter(|r| r.as_alerting().is_some()).collect()
    }
}

/// Go `Sender`: what `send_alerts` sends to (the notifier manager).
pub trait Sender: Send + Sync {
    fn send(&self, alerts: Vec<notifier::Alert>);
}

impl Sender for notifier::Manager {
    fn send(&self, alerts: Vec<notifier::Alert>) {
        notifier::Manager::send(self, alerts)
    }
}

/// Go `strutil.TableLinkForExpression`.
pub fn table_link_for_expression(expr: &str) -> String {
    format!("/graph?g0.expr={}&g0.tab=1", query_escape(expr))
}

/// Go `strutil.GraphLinkForExpression`.
pub fn graph_link_for_expression(expr: &str) -> String {
    format!("/graph?g0.expr={}&g0.tab=0", query_escape(expr))
}

/// Go `url.QueryEscape`.
pub fn query_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            b' ' => out.push('+'),
            _ => {
                out.push('%');
                out.push(char::from(b"0123456789ABCDEF"[(b >> 4) as usize]));
                out.push(char::from(b"0123456789ABCDEF"[(b & 15) as usize]));
            }
        }
    }
    out
}

/// Go `SendAlerts`: a notify function that sends the alerts to `s`.
pub fn send_alerts(s: Arc<dyn Sender>, external_url: &str) -> NotifyFunc {
    let external_url = external_url.to_string();
    Arc::new(move |_ctx: &Context, expr: &str, alerts: Vec<Alert>| {
        if alerts.is_empty() {
            return;
        }
        let res: Vec<notifier::Alert> = alerts
            .iter()
            .map(|alert| notifier::Alert {
                starts_at: alert.fired_at.to_system(),
                labels: alert.labels.clone(),
                annotations: alert.annotations.clone(),
                generator_url: format!("{external_url}{}", table_link_for_expression(expr)),
                ends_at: if !alert.resolved_at.is_zero() { alert.resolved_at.to_system() } else { alert.valid_until.to_system() },
            })
            .collect();
        s.send(res);
    })
}

/// Go `FromMaps`: sorted labels from maps, later maps overriding earlier ones.
pub fn from_maps(maps: &[&BTreeMap<String, String>]) -> Labels {
    let mut m: BTreeMap<&str, &str> = BTreeMap::new();
    for mm in maps {
        for (k, v) in mm.iter() {
            m.insert(k, v);
        }
    }
    labels::from_map(m)
}

/// Go `ParseFiles`: parses the rule files matching the glob patterns.
pub fn parse_files(patterns: &[String], scheme: ValidationScheme, p: &Parser, logger: &Logger) -> Result<(), BoxError> {
    let mut files: BTreeMap<String, String> = BTreeMap::new();
    for pat in patterns {
        let fns = discovery::glob::glob(pat).map_err(|e| rulefmt::wrap(format!("failed retrieving rule files for {}", rulefmt::go_quote(pat)), Box::new(rulefmt::Msg(e))))?;
        for fn_ in fns {
            let abs = std::path::absolute(&fn_).map(|p| p.to_string_lossy().into_owned()).unwrap_or(fn_.clone());
            let clean = match std::fs::canonicalize(&abs) {
                Ok(p) => p.to_string_lossy().into_owned(),
                Err(e) => {
                    return Err(rulefmt::wrap(
                        format!("failed evaluating rule file path {} (pattern: {})", rulefmt::go_quote(&abs), rulefmt::go_quote(pat)),
                        Box::new(rulefmt::Msg(format!("lstat {abs}: {}", go_errno(&e)))),
                    ));
                }
            };
            files.insert(clean, pat.clone());
        }
    }
    for (fn_, pat) in &files {
        let (_, errs) = rulefmt::parse_file(fn_, false, scheme, p, logger);
        if !errs.is_empty() {
            let joined = errs.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("\n");
            return Err(rulefmt::wrap(
                format!("parse rules from file {} (pattern: {})", rulefmt::go_quote(fn_), rulefmt::go_quote(pat)),
                Box::new(rulefmt::Msg(joined)),
            ));
        }
    }
    Ok(())
}

fn go_errno(e: &std::io::Error) -> String {
    match e.kind() {
        std::io::ErrorKind::NotFound => "no such file or directory".to_string(),
        std::io::ErrorKind::PermissionDenied => "permission denied".to_string(),
        _ => e.to_string(),
    }
}
