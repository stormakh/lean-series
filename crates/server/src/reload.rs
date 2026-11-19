//! Go `reloadConfig`, the reloaders, the config reload metrics and `updateGoGC`.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use discovery::Logger;
use discovery::metrics::{Collector, Family, GaugeVec, Metric, MetricType, MetricValue, Opts};

use crate::helpers::SafeNoStepSubqueryInterval;

/// A named step of a reload (Go `reloader`).
pub struct Reloader {
    pub name: String,
    pub reloader: Box<dyn Fn(&Arc<config::Config>) -> Result<(), String> + Send + Sync>,
}

impl Reloader {
    pub fn new(name: &str, f: impl Fn(&Arc<config::Config>) -> Result<(), String> + Send + Sync + 'static) -> Reloader {
        Reloader { name: name.to_string(), reloader: Box::new(f) }
    }
}

/// Go `configSuccess`.
pub fn config_success() -> &'static Arc<GaugeVec> {
    static G: OnceLock<Arc<GaugeVec>> = OnceLock::new();
    G.get_or_init(|| {
        GaugeVec::new_gauge(Opts::new(
            "prometheus_config_last_reload_successful",
            "Whether the last configuration reload attempt was successful.",
        ))
    })
}

/// Go `configSuccessTime`.
pub fn config_success_time() -> &'static Arc<GaugeVec> {
    static G: OnceLock<Arc<GaugeVec>> = OnceLock::new();
    G.get_or_init(|| {
        GaugeVec::new_gauge(Opts::new(
            "prometheus_config_last_reload_success_timestamp_seconds",
            "Timestamp of the last successful configuration reload.",
        ))
    })
}

/// What a reload changes besides the components: the log level and the default subquery step.
pub struct ReloadContext<'a> {
    pub filename: &'a str,
    pub enable_exemplar_storage: bool,
    pub agent_mode: bool,
    pub logger: &'a Logger,
    pub no_step_subquery_interval: &'a SafeNoStepSubqueryInterval,
    pub log_level: &'a util::logging::Level,
}

/// Go `reloadConfig`: loads the file, applies it with every reloader (all run even if one fails), then sets the
/// log level, GOGC and the default subquery step. `callback(success)` runs at the end, as Go's deferred call.
pub fn reload_config(rc: &ReloadContext<'_>, callback: &mut dyn FnMut(bool), rls: &[Reloader]) -> Result<(), String> {
    let res = reload_config_inner(rc, rls);
    match &res {
        Ok(()) => {
            config_success().get().set(1.0);
            config_success_time().get().set_to_current_time();
            callback(true);
        }
        Err(_) => {
            config_success().get().set(0.0);
            callback(false);
        }
    }
    res
}

fn reload_config_inner(rc: &ReloadContext<'_>, rls: &[Reloader]) -> Result<(), String> {
    let start = Instant::now();
    let logger = rc.logger;
    logger.info("Loading configuration file", &[("filename", rc.filename)]);
    let mut conf = config::load_file(rc.filename, rc.agent_mode, logger)
        .map_err(|e| format!("couldn't load configuration (--config.file={}): {e}", yaml::go_quote(rc.filename)))?;
    if rc.enable_exemplar_storage && conf.storage_config.exemplars_config.is_none() {
        conf.storage_config.exemplars_config = Some(config::default_exemplars_config());
    }
    let conf = Arc::new(conf);
    let mut failed = false;
    let mut timings: Vec<(String, String)> = Vec::with_capacity(rls.len());
    for rl in rls {
        let rstart = Instant::now();
        if let Err(e) = (rl.reloader)(&conf) {
            logger.error("Failed to apply configuration", &[("err", &e)]);
            failed = true;
        }
        timings.push((rl.name.clone(), util::logging::go_duration(rstart.elapsed())));
    }
    if failed {
        return Err(format!(
            "one or more errors occurred while applying the new configuration (--config.file={})",
            yaml::go_quote(rc.filename)
        ));
    }
    rc.log_level.set(&conf.runtime.log_level.0).map_err(|e| format!("applying log level: {e}"))?;
    update_go_gc(&conf, logger);
    rc.no_step_subquery_interval.set(conf.global_config.evaluation_interval.0);
    let kv: Vec<(&str, &str)> = timings.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    let total = util::logging::go_duration(start.elapsed());
    let mut all = kv;
    all.push(("filename", rc.filename));
    all.push(("totalDuration", &total));
    logger.info("Completed loading of configuration file", &all);
    Ok(())
}

/// The GOGC percentage (Go `debug.SetGCPercent`). This process has no garbage collector: the value is kept so
/// that the configuration, the logs and `go_gc_gogc_percent` behave as upstream.
static GOGC: AtomicI64 = AtomicI64::new(i64::MIN);

/// The GOGC value at process start (Go runtime: the GOGC environment variable, default 100, "off" is -1).
fn initial_gogc() -> i64 {
    match std::env::var("GOGC") {
        Ok(v) if v.eq_ignore_ascii_case("off") => -1,
        Ok(v) => v.trim().parse().unwrap_or(100),
        Err(_) => 100,
    }
}

/// The current GOGC percentage.
pub fn gogc() -> i64 {
    let v = GOGC.load(Ordering::SeqCst);
    if v == i64::MIN { initial_gogc() } else { v }
}

/// Go `updateGoGC`: sets GOGC from `runtime.gogc` and logs a change.
pub fn update_go_gc(conf: &config::Config, logger: &Logger) {
    let old = gogc();
    let new = conf.runtime.gogc;
    GOGC.store(new, Ordering::SeqCst);
    if old != new {
        logger.info("updated GOGC", &[("old", &old.to_string()), ("new", &new.to_string())]);
    }
}

/// `go_gc_gogc_percent` (Go's runtime metrics collector).
pub struct GoGcCollector;

impl Collector for GoGcCollector {
    fn describe(&self) -> Vec<String> {
        vec!["go_gc_gogc_percent".to_string()]
    }
    fn collect(&self, out: &mut Vec<Family>) {
        out.push(Family {
            name: "go_gc_gogc_percent".to_string(),
            help: "Heap size target percentage configured by the user, otherwise 100. This value is set by the GOGC environment variable, and the runtime/debug.SetGCPercent function. Sourced from /gc/gogc:percent.".to_string(),
            typ: MetricType::Gauge,
            metrics: vec![Metric { labels: Vec::new(), value: MetricValue::Gauge(gogc() as f64) }],
        });
    }
}
