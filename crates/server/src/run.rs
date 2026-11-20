//! Go `main()` of `cmd/prometheus`: flags, config validation, component wiring, the run group, reloads and
//! shutdown.
//!
//! Threads: the actors of the run group (mostly blocked), the signal thread (blocked in `sigwait`), the
//! components' own threads, and the HTTP connection threads. Nothing wakes on a timer while idle except what the
//! components need (scrapes, rule evaluation, the TSDB's compaction timer) and, with `--config.auto-reload`, the
//! config checksum check.

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use discovery::Logger;
use discovery::metrics::Registry;
use storage::{AppendableV2, Context, Queryable, SampleAndChunkQueryable, Storage};

use crate::features::{self, FeatureOptions};
use crate::flags::{self, FlagConfig};
use crate::helpers::{self, SafeNoStepSubqueryInterval};
use crate::kingpin::GoParse;
use crate::ready::{LocalDb, ReadyScrapeManager, ReadyStorage};
use crate::reload::{self, ReloadContext, Reloader};
use crate::rungroup::{Event, Group};
use crate::sys;

/// Stack size of the threads that run queries (HTTP connections, rule groups): deep PromQL expressions recurse.
/// Only the pages a thread touches use memory. 256 MiB takes about 400k nested parentheses in a release build
/// (Go: a goroutine stack grows to 1 GB; at depth 200k Go used 900 MB of RSS, this port 15 MB).
pub const QUERY_STACK_SIZE: usize = 256 << 20;

/// The component logger (`discovery::Logger`, which most crates take) over the process logger (Go
/// `promslog.New`): every crate logs through one stream in Go's format.
pub fn new_logger(root: util::logging::Logger) -> Logger {
    Logger::new(Arc::new(move |level: discovery::log::Level, msg: &str, attrs: &[(String, String)]| {
        let l = match level {
            discovery::log::Level::Debug => util::logging::Level::Debug,
            discovery::log::Level::Info => util::logging::Level::Info,
            discovery::log::Level::Warn => util::logging::Level::Warn,
            discovery::log::Level::Error => util::logging::Level::Error,
        };
        if !root.enabled(l) {
            return;
        }
        let kv: Vec<(&str, &dyn util::logging::LogValue)> =
            attrs.iter().map(|(k, v)| (k.as_str(), v as &dyn util::logging::LogValue)).collect();
        root.log(l, msg, &kv);
    }))
}

/// The TSDB logger (also used by the agent and the WAL) over the process logger.
pub fn tsdb_logger(logger: &Logger) -> tsdb::Logger {
    let l = logger.clone();
    tsdb::Logger::structured(move |level: &str, msg: &str, kv: &[(&str, &dyn std::fmt::Display)]| {
        let level = match level {
            "DEBUG" => discovery::log::Level::Debug,
            "WARN" => discovery::log::Level::Warn,
            "ERROR" => discovery::log::Level::Error,
            _ => discovery::log::Level::Info,
        };
        let vals: Vec<String> = kv.iter().map(|(_, v)| v.to_string()).collect();
        let pairs: Vec<(&str, &str)> = kv.iter().zip(vals.iter()).map(|((k, _), v)| (*k, v.as_str())).collect();
        l.log(level, msg, &pairs);
    })
}

fn exit(code: i32) -> ! {
    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().flush();
    std::process::exit(code)
}

/// Go `computeExternalURL` result as the web handler takes it.
fn web_external_url(u: &yaml::url::GoUrl) -> web::web::ExternalUrl {
    web::web::ExternalUrl { scheme: u.scheme.clone(), host: u.host.clone(), path: u.path.clone() }
}

/// Registers the features that Go's packages register themselves (discovery providers, scrape, TSDB, rules,
/// UI, OTLP receiver) where the Rust crates do not.
fn register_component_features(r: &promql::features::Registry, f: &FeatureOptions, agent_mode: bool, auto_reload: bool) {
    use promql::features::Collector;
    r.set(features::PROMETHEUS, "agent_mode", agent_mode);
    r.set(features::PROMETHEUS, "server_mode", !agent_mode);
    r.set(features::PROMETHEUS, "auto_reload_config", auto_reload);
    // labels.ImplementationName: the port uses the stringlabels form.
    r.enable(features::PROMETHEUS, "stringlabels");
    for name in template::prom::template_function_names() {
        r.enable("templating_functions", &name);
    }
    for name in discovery::registered_config_names() {
        r.enable("service_discovery_providers", &name);
    }
    // scrape.NewManager.
    r.set(features::SCRAPE, "extra_scrape_metrics", true);
    r.set(features::SCRAPE, "start_timestamp_zero_ingestion", f.scrape_enable_start_timestamp_zero_ingestion);
    r.set(features::SCRAPE, "type_and_unit_labels", f.scrape_enable_type_and_unit_labels);
    r.set(features::SCRAPE, "openmetrics2", f.scrape_enable_open_metrics2);
    // tsdb.Open (server mode only).
    if !agent_mode {
        r.set(features::TSDB, "exemplar_storage", f.tsdb_enable_exemplar_storage);
        r.set(features::TSDB, "delayed_compaction", f.tsdb_enable_delayed_compaction);
        r.set(features::TSDB, "use_uncached_io", f.tsdb_use_uncached_io);
        r.set(features::TSDB, "st_storage", f.tsdb_enable_st_storage);
        r.set(features::TSDB, "histograms_st_encoding", f.tsdb_enable_histogram_st_encoding);
        r.enable(features::TSDB, "isolation");
        r.enable(features::TSDB, "native_histograms");
        r.enable(features::TSDB, "xor2_encoding");
    }
    // web.New.
    r.set("ui", "ui_v2", f.web_use_old_ui);
    r.set("ui", "ui_v3", !f.web_use_old_ui);
    r.set("otlp_receiver", "delta_conversion", f.web_convert_otlp_delta);
    r.set("otlp_receiver", "native_delta_ingestion", f.web_native_otlp_delta_ingestion);
}

/// The components a reload touches, shared between the reload handler and the initial load.
struct Reloading {
    rls: Vec<Reloader>,
    filename: String,
    enable_exemplar_storage: bool,
    agent_mode: bool,
    logger: Logger,
    no_step: Arc<SafeNoStepSubqueryInterval>,
    root_logger: util::logging::Logger,
}

impl Reloading {
    fn reload(&self, callback: &mut dyn FnMut(bool)) -> Result<(), String> {
        let rc = ReloadContext {
            filename: &self.filename,
            enable_exemplar_storage: self.enable_exemplar_storage,
            agent_mode: self.agent_mode,
            logger: &self.logger,
            no_step_subquery_interval: &self.no_step,
            root_logger: &self.root_logger,
        };
        reload::reload_config(&rc, callback, &self.rls)
    }
}

/// Events of the reload handler (Go's select over SIGHUP, the web reload channel and the cancel channel).
enum ReloadEvent {
    Hup,
    Web(web::web::ReloadRequest),
    Cancel(Sender<()>),
}

/// Events of the termination handler.
enum TermEvent {
    Signal(sys::Signal),
    WebQuit,
    Cancel,
}

/// Go `main()`.
pub fn main() {
    sys::set_malloc_tunables();
    sys::tune_malloc();
    // Before any thread starts: every thread inherits the blocked signals, which only the signal thread takes.
    sys::block_signals();

    let args: Vec<String> = std::env::args().collect();
    let app_name = std::path::Path::new(args.first().map(|s| s.as_str()).unwrap_or("prometheus"))
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "prometheus".to_string());
    let app = flags::app(&app_name);
    let rest = &args[1.min(args.len())..];
    let parsed = {
        let mut out = std::io::stdout();
        match app.try_parse(rest, &mut out) {
            GoParse::Run(p) => p,
            GoParse::Exit(c) => exit(c),
            GoParse::ContextErr(e) | GoParse::ValueErr(e) => {
                eprintln!("Error parsing command line arguments: {e}");
                // Go `a.Usage(os.Args[1:])`: a context error ends in FatalIfError (exit 1).
                match app.usage_for(rest) {
                    Ok(u) => {
                        print!("{u}");
                        exit(2)
                    }
                    Err(e) => {
                        eprintln!("{app_name}: error: {e}");
                        exit(1)
                    }
                }
            }
        }
    };
    let mut cfg = FlagConfig::from_parsed(&app, &parsed);
    if cfg.write_documentation {
        print!("{}", app.generate_markdown());
        exit(0);
    }

    let root_logger = util::logging::Logger::new(util::logging::Config {
        level: util::logging::Level::parse(&cfg.log_level).unwrap_or_default(),
        format: util::logging::Format::parse(&cfg.log_format).unwrap_or_default(),
        writer: None,
    });
    let logger = new_logger(root_logger.clone());

    // The CLI log level is the default when runtime.log_level is absent from the config file.
    let level_string = root_logger.level().unwrap_or_default().to_string();
    config::set_default_runtime_config(|r| r.log_level = config::LogLevel(level_string.clone()));

    let notifs = web::notifications::Notifications::new(cfg.max_notifications_subscribers.max(0) as usize);
    notifs.add_notification(web::notifications::STARTING_UP);

    let mut fo = FeatureOptions::default();
    if let Err(e) = features::set_feature_list_options(&mut fo, &cfg.feature_list, &logger) {
        eprintln!("Error parsing feature list: {e}");
        exit(1);
    }
    cfg.enable_auto_reload = cfg.enable_auto_reload || fo.enable_auto_reload;
    if cfg.enable_auto_reload {
        if cfg.auto_reload_interval < 1_000_000_000 {
            cfg.auto_reload_interval = 1_000_000_000;
        }
        logger.info(
            "Automatic configuration file reloading enabled",
            &[("interval", &yaml::types::Duration(cfg.auto_reload_interval).go_string())],
        );
    }
    if cfg.max_search_limit < 0 {
        eprintln!("--web.search.max-limit must be non-negative; got {} (use 0 to disable the cap)", cfg.max_search_limit);
        exit(1);
    }
    let parser = promql::parser::Parser::new(fo.parser_options());

    let agent_mode = cfg.agent_mode;
    if agent_mode && !cfg.server_only_flags.is_empty() {
        eprint!("The following flag(s) can not be used in agent mode: {}", go_quote_list(&cfg.server_only_flags));
        exit(3);
    }
    if !agent_mode && !cfg.agent_only_flags.is_empty() {
        eprint!("The following flag(s) can only be used in agent mode: {}", go_quote_list(&cfg.agent_only_flags));
        exit(3);
    }
    if agent_mode && cfg.agent.checkpoint_batch_size <= 0 {
        eprintln!("--storage.agent.checkpoint-batch-size must be greater than 0.");
        exit(1);
    }
    if cfg.memlimit_ratio <= 0.0 || cfg.memlimit_ratio > 1.0 {
        eprint!("--auto-gomemlimit.ratio must be greater than 0 and less than or equal to 1.");
        exit(1);
    }

    let local_storage_path = if agent_mode { cfg.agent_storage_path.clone() } else { cfg.server_storage_path.clone() };

    let external_url = match helpers::compute_external_url(&cfg.prometheus_url, &cfg.listen_addresses[0], &cfg.web_config_file) {
        Ok(u) => u,
        Err(e) => {
            eprintln!("parse external URL {}: {e}", yaml::go_quote(&cfg.prometheus_url));
            exit(2);
        }
    };
    let cors_origin = match web::httputil::CorsOrigin::new(&cfg.cors_regex_string) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("could not compile CORS regex string {}: {e}", yaml::go_quote(&cfg.cors_regex_string));
            exit(2);
        }
    };

    // TSDB retention defaults from the flags, before any config file is loaded.
    let mut cli_retention = cfg.tsdb.retention_duration;
    let cli_max_bytes = cfg.tsdb.max_bytes;
    if cli_retention == 0 && cli_max_bytes == 0 {
        cli_retention = yaml::types::parse_duration(flags::DEFAULT_RETENTION_STRING).map(|d| d.0).unwrap_or(0);
    }
    config::set_default_tsdb_retention_config(config::TsdbRetentionConfig {
        time: yaml::types::Duration(cli_retention),
        size: yaml::types::Base2Bytes(cli_max_bytes),
        percentage: 0.0,
    });

    // Invalid config errors before other components start.
    let abs_config = std::path::absolute(&cfg.config_file).map(|p| p.to_string_lossy().into_owned()).unwrap_or_else(|_| cfg.config_file.clone());
    let mut cfg_file = match config::load_file(&cfg.config_file, agent_mode, &Logger::nop()) {
        Ok(c) => c,
        Err(e) => {
            logger.error(&format!("Error loading config (--config.file={})", cfg.config_file), &[("file", &abs_config), ("err", &e)]);
            exit(2);
        }
    };
    if let Err(e) = cfg_file.get_scrape_configs() {
        logger.error(
            &format!("Error loading dynamic scrape config files from config (--config.file={})", yaml::go_quote(&cfg.config_file)),
            &[("file", &abs_config), ("err", &e)],
        );
        exit(2);
    }
    if let Err(e) = rules::parse_files(&cfg_file.rule_files, cfg_file.global_config.metric_name_validation_scheme, &parser, &logger) {
        logger.error(
            &format!("Error loading rule file patterns from config (--config.file={})", yaml::go_quote(&cfg.config_file)),
            &[("file", &abs_config), ("err", &e.to_string())],
        );
        exit(2);
    }

    let mut tsdb_opts = tsdb::db::zero_options();
    let mut max_exemplars = 0i64;
    if fo.tsdb_enable_exemplar_storage {
        if cfg_file.storage_config.exemplars_config.is_none() {
            cfg_file.storage_config.exemplars_config = Some(config::default_exemplars_config());
        }
        max_exemplars = cfg_file.storage_config.exemplars_config.as_ref().map(|e| e.max_exemplars).unwrap_or(0);
    }
    let min_reload = tsdb::db::MIN_BLOCK_RELOAD_INTERVAL.as_nanos() as i64;
    if cfg.tsdb.block_reload_interval < min_reload {
        let s = yaml::types::Duration(min_reload).go_string();
        logger.warn(&format!("The option --storage.tsdb.block-reload-interval is set to a value less than {s}. Setting it to {s} to avoid overload."), &[]);
        cfg.tsdb.block_reload_interval = min_reload;
    }
    // The configuration file takes precedence over the flag-derived default chunk encoding.
    let tsdb_conf = cfg_file.storage_config.tsdb_config.clone().unwrap_or_default();
    let mut float_encoding = if fo.tsdb_float_chunk_encoding_xor2 { chunkenc::ENC_XOR2 } else { chunkenc::ENC_XOR };
    match tsdb_conf.chunk_encoding.floats.as_str() {
        config::FLOAT_CHUNK_ENCODING_XOR => float_encoding = chunkenc::ENC_XOR,
        config::FLOAT_CHUNK_ENCODING_XOR2 => float_encoding = chunkenc::ENC_XOR2,
        _ => {}
    }
    let retention = tsdb_conf.retention.clone().unwrap_or_default();
    let mut retention_duration = retention.time.0;
    let max_bytes = retention.size.0;
    let max_percentage = retention.percentage;

    reload::update_go_gc(&cfg_file, &logger);
    // Go's runtime reads GOGC from the environment; the runtime info API shows it. Set before any thread starts.
    // SAFETY: no other thread exists yet (the signal thread and the components start later), so nothing reads
    // the environment concurrently.
    unsafe {
        std::env::set_var("GOGC", if cfg_file.runtime.gogc >= 0 { cfg_file.runtime.gogc.to_string() } else { "off".to_string() });
    }
    // --auto-gomaxprocs and --auto-gomemlimit: Go runtime settings with no equivalent here (no garbage collector,
    // threads are sized by the components). The flags are accepted and validated.

    if !cfg.tsdb_delay_compact_file_path.is_empty() {
        logger.info(
            "Compactions will be delayed for blocks not marked as uploaded in the file tracking uploads",
            &[("path", &cfg.tsdb_delay_compact_file_path)],
        );
        tsdb_opts.block_compaction_exclude_func =
            Some(helpers::exclude_blocks_pending_upload(logger.clone(), cfg.tsdb_delay_compact_file_path.clone()));
    }

    // The config is valid: set the success metrics now so they do not stay at zero during a long startup.
    reload::config_success().get().set(1.0);
    reload::config_success_time().get().set_to_current_time();

    let mut route_prefix = if cfg.route_prefix.is_empty() { external_url.path.clone() } else { cfg.route_prefix.clone() };
    route_prefix = format!("/{}", route_prefix.trim_matches('/'));

    let mut max_block_duration = cfg.tsdb.max_block_duration;
    let mut compaction_delay_max_percent = cfg.tsdb.compaction_delay_max_percent;
    let tsdb_wal_compression;
    if !agent_mode {
        if retention_duration < 0 {
            retention_duration = yaml::types::parse_duration("100y").map(|d| d.0).unwrap_or(0);
            logger.warn(&format!("Time retention value is too high. Limiting to: {}", yaml::types::Duration(retention_duration).go_string()), &[]);
        }
        if max_percentage > 0.0 {
            if max_bytes > 0 {
                logger.warn("storage.tsdb.retention.size is ignored, because storage.tsdb.retention.percentage is specified", &[]);
            }
            if helpers::storage_path_fs_size(&local_storage_path) == 0 {
                eprintln!(
                    "unable to detect total capacity of metric storage at {local_storage_path}, please disable retention percentage ({}%)",
                    helpers::format_float_g(max_percentage)
                );
                exit(2);
            }
        }
        if max_block_duration == 0 {
            let mut mbd = yaml::types::parse_duration("31d").map(|d| d.0).unwrap_or(0);
            if retention_duration != 0 && retention_duration / 10 < mbd {
                mbd = retention_duration / 10;
            }
            max_block_duration = mbd;
        }
        if fo.tsdb_enable_delayed_compaction && (compaction_delay_max_percent > 100 || compaction_delay_max_percent <= 0) {
            logger.warn(
                "The --storage.tsdb.delayed-compaction.max-percent should have a value between 1 and 100. Using default",
                &[("default", &tsdb::db::DEFAULT_COMPACTION_DELAY_MAX_PERCENT.to_string())],
            );
            compaction_delay_max_percent = tsdb::db::DEFAULT_COMPACTION_DELAY_MAX_PERCENT as i64;
        }
        tsdb_wal_compression = compression_type(cfg.tsdb_wal_compression, &cfg.tsdb_wal_compression_type);
    } else {
        tsdb_wal_compression = compression_type(cfg.agent_wal_compression, &cfg.agent_wal_compression_type);
    }

    // Go builds the flags map of the status API from the flag variables after main changed them.
    {
        let d = |ns: i64| yaml::types::Duration(ns).go_string();
        let fv = &mut cfg.flag_values;
        fv.insert("config.auto-reload".into(), cfg.enable_auto_reload.to_string());
        fv.insert("config.auto-reload-interval".into(), d(cfg.auto_reload_interval));
        fv.insert("storage.tsdb.block-reload-interval".into(), d(cfg.tsdb.block_reload_interval));
        fv.insert("web.route-prefix".into(), route_prefix.clone());
        if !agent_mode {
            fv.insert("storage.tsdb.retention.time".into(), d(retention_duration));
            fv.insert("storage.tsdb.retention.size".into(), yaml::types::Base2Bytes(max_bytes).go_string());
            fv.insert("storage.tsdb.max-block-duration".into(), d(max_block_duration));
            fv.insert("storage.tsdb.delayed-compaction.max-percent".into(), compaction_delay_max_percent.to_string());
        }
    }

    let no_step = Arc::new(SafeNoStepSubqueryInterval::default());
    no_step.set(config::default_global_config().evaluation_interval.0);

    let (mode_app_name, mode) = if agent_mode { ("Prometheus Agent", "agent") } else { ("Prometheus Server", "server") };
    logger.info(&format!("Starting {mode_app_name}"), &[("mode", mode), ("version", &flags::version_info())]);
    logger.info(
        "operational information",
        &[
            ("build_context", &flags::build_context()),
            ("host_details", &sys::uname()),
            ("fd_limits", &sys::fd_limits()),
            ("vm_limits", &sys::vm_limits()),
        ],
    );

    let feature_registry = features::default_registry().clone();
    register_component_features(&feature_registry, &fo, agent_mode, cfg.enable_auto_reload);

    // ---- Components.
    let reg: Arc<Registry> = web::metrics::default_registry().clone();
    let local_storage = Arc::new(ReadyStorage::new());
    let scraper = Arc::new(ReadyScrapeManager::default());
    if let Err(e) = remote::write::register_global_metrics(&reg) {
        logger.error("failed to register remote storage metrics", &[("err", &e)]);
        exit(1);
    }
    let st_local = local_storage.clone();
    let remote_storage = Arc::new(remote::write::new_storage(
        Some(logger.with(&[("component", "remote")])),
        Some(reg.clone()),
        Some(Arc::new(move || st_local.start_time())),
        std::path::Path::new(&local_storage_path),
        Duration::from_nanos(cfg.remote_flush_deadline.max(0) as u64),
        Some(scraper.clone() as Arc<dyn remote::metadata_watcher::ReadyScrapeManager>),
        fo.scrape_enable_type_and_unit_labels,
        None,
    ));
    let fanout_logger = logger.clone();
    let fanout = Arc::new(storage::new_fanout(
        Some(Arc::new(move |msg: &str, err: &storage::Error| fanout_logger.error(msg, &[("err", &err.to_string())]))),
        local_storage.clone() as Arc<dyn Storage>,
        vec![remote_storage.clone() as Arc<dyn Storage>],
    ));

    let notifier_manager = Arc::new(notifier::Manager::new(
        notifier::Options {
            queue_capacity: cfg.notifier_queue_capacity.max(0) as usize,
            drain_on_shutdown: cfg.notifier_drain_on_shutdown,
            max_batch_size: cfg.notifier_max_batch_size.max(0) as usize,
            registerer: Some(reg.clone()),
            ..Default::default()
        },
        cfg_file.global_config.metric_name_validation_scheme,
        Some(logger.with(&[("component", "notifier")])),
    ));

    let ctx_scrape = discovery::CancelToken::new();
    let ctx_notify = discovery::CancelToken::new();
    let sd_metrics = match discovery::create_and_register_sd_metrics(&reg) {
        Ok(m) => m,
        Err(e) => {
            logger.error("failed to register service discovery metrics", &[("err", &e)]);
            exit(1);
        }
    };
    let dm_scrape = match discovery::Manager::new(
        &ctx_scrape,
        logger.with(&[("component", "discovery manager scrape")]),
        &reg,
        &sd_metrics,
        discovery::ManagerOptions { name: "scrape".to_string(), ..Default::default() },
    ) {
        Ok(m) => Arc::new(m),
        Err(_) => {
            logger.error("failed to create a discovery manager scrape", &[]);
            exit(1);
        }
    };
    let dm_notify = match discovery::Manager::new(
        &ctx_notify,
        logger.with(&[("component", "discovery manager notify")]),
        &reg,
        &sd_metrics,
        discovery::ManagerOptions { name: "notify".to_string(), ..Default::default() },
    ) {
        Ok(m) => Arc::new(m),
        Err(_) => {
            logger.error("failed to create a discovery manager notify", &[]);
            exit(1);
        }
    };

    if !cfg.scrape_adjust_timestamps {
        scrape::scrape_loop::ALIGN_SCRAPE_TIMESTAMPS.store(false, std::sync::atomic::Ordering::Relaxed);
    }
    let scrape_opts = scrape::Options {
        append_metadata: fo.scrape_append_metadata,
        discovery_reload_interval: Duration::from_nanos(cfg.discovery_reload_interval.max(0) as u64),
        enable_start_timestamp_zero_ingestion: fo.scrape_enable_start_timestamp_zero_ingestion,
        parse_st: fo.scrape_parse_st,
        synthesize_st: fo.scrape_synthesize_st,
        enable_type_and_unit_labels: fo.scrape_enable_type_and_unit_labels,
        enable_open_metrics2: fo.scrape_enable_open_metrics2,
        enable_zstd_scrape: fo.scrape_enable_zstd_scrape,
        ..Default::default()
    };
    let new_failure_logger: scrape::manager::NewFailureLogger = Arc::new(|p: &str| scrape::JsonFileLogger::new(p));
    let scrape_manager = match scrape::Manager::new(
        Some(scrape_opts),
        Some(logger.with(&[("component", "scrape manager")])),
        Some(new_failure_logger),
        None,
        Some(fanout.clone() as Arc<dyn AppendableV2>),
        Some(reg.clone()),
    ) {
        Ok(m) => m,
        Err(e) => {
            logger.error("failed to create a scrape manager", &[("err", &e)]);
            exit(1);
        }
    };

    let mut query_engine: Option<promql::Engine> = None;
    let mut rule_manager: Option<Arc<rules::Manager>> = None;
    if !agent_mode {
        let tracker = match promql::query_logger::ActiveQueryTracker::new(
            std::path::Path::new(&local_storage_path),
            cfg.query_concurrency.max(0) as usize,
        ) {
            Ok(t) => Arc::new(t),
            Err(e) => {
                logger.error("failed to initialize active query tracker", &[("err", &e.to_string())]);
                exit(1);
            }
        };
        let ns = no_step.clone();
        let engine = promql::Engine::new(promql::EngineOpts {
            max_samples: cfg.query_max_samples.max(0) as usize,
            timeout: Duration::from_nanos(cfg.query_timeout.max(0) as u64),
            active_query_tracker: Some(tracker),
            lookback_delta: Duration::from_nanos(cfg.lookback_delta.max(0) as u64),
            no_step_subquery_interval_fn: Some(Arc::new(move |r| ns.get(r))),
            // Always on for regular PromQL since v2.33.
            enable_at_modifier: true,
            enable_negative_offset: true,
            enable_per_step_stats: fo.enable_per_step_stats,
            enable_delayed_name_removal: fo.promql_enable_delayed_name_removal,
            enable_type_and_unit_labels: fo.scrape_enable_type_and_unit_labels,
            use_start_timestamps: fo.use_start_timestamps,
            enable_query_cost: fo.enable_query_cost,
            feature_registry: Some(feature_registry.clone()),
            parser: Some(parser.clone()),
        });
        if let Err(e) = crate::selfmetrics::register_engine_metrics(&reg, &engine) {
            logger.error("failed to register query engine metrics", &[("err", &e)]);
            exit(1);
        }
        let rule_query_offset = cfg_file.global_config.rule_query_offset.to_std();
        let rm = rules::Manager::new(rules::ManagerOptions {
            name_validation_scheme: cfg_file.global_config.metric_name_validation_scheme,
            appendable_v2: Some(fanout.clone() as Arc<dyn AppendableV2>),
            queryable: Some(local_storage.clone() as Arc<dyn Queryable>),
            query_func: Some(rules::engine_query_func(Arc::new(engine.clone()), fanout.clone() as Arc<dyn Queryable>)),
            notify_func: Some(rules::send_alerts(
                notifier_manager.clone() as Arc<dyn rules::Sender>,
                &external_url.to_url_string(),
            )),
            context: Context::background(),
            external_url: Some(external_url.clone()),
            registerer: Some(reg.clone()),
            logger: logger.with(&[("component", "rule manager")]),
            outage_tolerance: Duration::from_nanos(cfg.outage_tolerance.max(0) as u64),
            for_grace_period: Duration::from_nanos(cfg.for_grace_period.max(0) as u64),
            resend_delay: Duration::from_nanos(cfg.resend_delay.max(0) as u64),
            max_concurrent_evals: cfg.max_concurrent_evals,
            concurrent_evals_enabled: fo.enable_concurrent_rule_eval,
            default_rule_query_offset: Some(Arc::new(move || rule_query_offset)),
            feature_registry: Some(feature_registry.clone()),
            parser: Some(parser.clone()),
            ..Default::default()
        });
        query_engine = Some(engine);
        rule_manager = Some(Arc::new(rm));
    }

    scraper.set(scrape_manager.clone());

    // ---- Web handler.
    let current_config: Arc<RwLock<Arc<config::Config>>> = Arc::new(RwLock::new(Arc::new(cfg_file.clone())));
    let web_logger = logger.with(&[("component", "web")]);
    let cc = current_config.clone();
    let external_labels_fn: remote::read_handler::ExternalLabelsFn =
        Arc::new(move || cc.read().unwrap_or_else(|p| p.into_inner()).global_config.external_labels.clone());
    let read_handler = Arc::new(remote::read_handler::new_read_handler(
        web_logger.clone(),
        Some(&reg),
        fanout.clone() as Arc<dyn SampleAndChunkQueryable>,
        external_labels_fn,
        cfg.remote_read_sample_limit.max(0) as usize,
        cfg.remote_read_concurrency_limit.max(0) as usize,
        cfg.remote_read_bytes_in_frame,
    ));
    let remote_read_handler: web::api::HttpHandler = Arc::new(move |req: http::Request| {
        // Go aborts the connection (http.ErrAbortHandler); this server answers 500 instead.
        read_handler.serve(&Context::background(), &req).unwrap_or_else(|_| http::Response::new(500))
    });
    let mut remote_write_handler: Option<web::api::HttpHandler> = None;
    if cfg.enable_remote_write_receiver {
        let msgs = remote::remoteapi::MessageTypes(
            cfg.accept_remote_write_proto_msgs
                .iter()
                .map(|m| if m == flags::WRITE_V2_MESSAGE_TYPE { remote::remoteapi::WriteMessageType::V2 } else { remote::remoteapi::WriteMessageType::V1 })
                .collect(),
        );
        let h = Arc::new(remote::write_handler::new_write_handler(
            web_logger.clone(),
            Some(&reg),
            fanout.clone() as Arc<dyn AppendableV2>,
            msgs,
            fo.web_enable_type_and_unit_labels,
            fo.web_append_metadata,
        ));
        remote_write_handler = Some(Arc::new(move |req: http::Request| h.serve(&Context::background(), req)));
    }
    let mut otlp_write_handler: Option<web::api::HttpHandler> = None;
    if cfg.enable_otlp_write_receiver {
        let cc = current_config.clone();
        let h = Arc::new(remote::write_otlp_handler::new_otlp_write_handler(
            web_logger.clone(),
            Some(&reg),
            fanout.clone() as Arc<dyn AppendableV2>,
            Arc::new(move || cc.read().unwrap_or_else(|p| p.into_inner()).otlp_config.clone()),
            remote::write_otlp_handler::OtlpOptions {
                convert_delta: fo.web_convert_otlp_delta,
                native_delta: fo.web_native_otlp_delta_ingestion,
                lookback_delta: Duration::from_nanos(cfg.lookback_delta.max(0) as u64),
                enable_type_and_unit_labels: fo.web_enable_type_and_unit_labels,
            },
        ));
        otlp_write_handler = Some(Arc::new(move |req: http::Request| h.serve_http(&req)));
    }

    let console_expander = query_engine.as_ref().map(|e| {
        crate::consoles::console_expander(e.clone(), fanout.clone() as Arc<dyn Queryable>, &external_url.to_url_string())
    });
    let n1 = notifs.clone();
    let n2 = notifs.clone();
    let web_external_url = web_external_url(&external_url);
    let web_handler = web::web::Handler::new(web::web::Options {
        tsdb_retention_duration: yaml::types::Duration(retention_duration),
        tsdb_dir: local_storage_path.clone(),
        tsdb_max_bytes: yaml::types::Base2Bytes(max_bytes),
        tsdb_max_percentage: max_percentage,
        local_storage: Some(local_storage.clone() as Arc<dyn Queryable>),
        tsdb_admin: Some(local_storage.clone() as Arc<dyn web::api::TsdbAdminStats>),
        storage: Some(fanout.clone() as Arc<dyn SampleAndChunkQueryable>),
        exemplar_storage: Some(local_storage.clone() as Arc<dyn storage::ExemplarQueryable>),
        cost_sampling: Some(fanout.clone() as Arc<dyn storage::CostSamplingQueryable + Send + Sync>),
        query_engine: query_engine.clone().map(|e| Arc::new(e) as Arc<dyn web::api::QueryEngine>),
        lookback_delta: Duration::from_nanos(cfg.lookback_delta.max(0) as u64),
        query_timeout: Duration::from_nanos(cfg.query_timeout.max(0) as u64),
        scrape_pools_retriever: Some(scrape_manager.clone() as Arc<dyn web::api::ScrapePoolsRetriever>),
        target_retriever: Some(scrape_manager.clone() as Arc<dyn web::api::TargetRetriever>),
        rules_retriever: rule_manager
            .clone()
            .map(|m| Arc::new(web::api::adapters::RulesManagerRetriever::new(m)) as Arc<dyn web::api::RulesRetriever>),
        alertmanager_retriever: Some(Arc::new(web::api::adapters::NotifierRetriever::new(notifier_manager.clone()))
            as Arc<dyn web::api::AlertmanagerRetriever>),
        version: Some(web::api::PrometheusVersion {
            version: flags::VERSION.to_string(),
            revision: flags::REVISION.to_string(),
            branch: flags::BRANCH.to_string(),
            build_user: flags::BUILD_USER.to_string(),
            build_date: flags::BUILD_DATE.to_string(),
            go_version: flags::GO_VERSION.to_string(),
        }),
        notifications_getter: Some(Arc::new(move || n1.get())),
        notifications_sub: Some(Arc::new(move || n2.sub())),
        flags: cfg.flag_values.clone(),
        listen_addresses: cfg.listen_addresses.clone(),
        cors_origin: Some(cors_origin),
        read_timeout: Duration::from_nanos(cfg.web_timeout.max(0) as u64),
        max_connections: cfg.max_connections.max(0) as usize,
        external_url: web_external_url,
        route_prefix: route_prefix.clone(),
        user_assets_path: cfg.user_assets_path.clone(),
        console_templates_path: cfg.console_templates_path.clone(),
        console_libraries_path: cfg.console_libraries_path.clone(),
        use_old_ui: fo.web_use_old_ui,
        enable_lifecycle: cfg.enable_lifecycle,
        enable_admin_api: cfg.enable_admin_api,
        enable_search: fo.web_enable_search,
        enable_query_cost: fo.web_enable_query_cost,
        max_search_limit: cfg.max_search_limit,
        page_title: cfg.page_title.clone(),
        enable_remote_write_receiver: cfg.enable_remote_write_receiver,
        enable_otlp_write_receiver: cfg.enable_otlp_write_receiver,
        convert_otlp_delta: fo.web_convert_otlp_delta,
        native_otlp_delta_ingestion: fo.web_native_otlp_delta_ingestion,
        is_agent: agent_mode,
        app_name: mode_app_name.to_string(),
        remote_read_handler: if agent_mode { None } else { Some(remote_read_handler) },
        remote_write_handler,
        otlp_write_handler,
        gatherer: None,
        registerer: Some(reg.clone()),
        feature_registry: Some(feature_registry.clone()),
        parser: Some(Arc::new(parser.clone())),
        console_expander,
        logger: web_logger.clone(),
        stack_size: QUERY_STACK_SIZE,
        on_idle: Some(sys::on_idle),
        ..Default::default()
    });

    // ---- Reloaders (Go order).
    let mut rls: Vec<Reloader> = Vec::new();
    {
        let ls = local_storage.clone();
        let l = logger.clone();
        let last = Mutex::new(config::TsdbRetentionConfig::default());
        rls.push(Reloader::new("db_storage", move |c| {
            ls.apply_config(c).map_err(|e| e.to_string())?;
            if agent_mode {
                return Ok(());
            }
            let Some(curr) = c.storage_config.tsdb_config.as_ref().and_then(|t| t.retention.as_ref()) else {
                return Ok(());
            };
            let mut last = last.lock().unwrap_or_else(|p| p.into_inner());
            if *curr == *last {
                return Ok(());
            }
            l.info(
                "TSDB retention updated",
                &[
                    ("duration", &curr.time.go_string()),
                    ("size", &curr.size.go_string()),
                    ("percentage", &helpers::format_float_g(curr.percentage)),
                ],
            );
            *last = curr.clone();
            Ok(())
        }));
    }
    {
        let rs = remote_storage.clone();
        rls.push(Reloader::new("remote_storage", move |c| rs.apply_config(c).map_err(|e| e.to_string())));
    }
    {
        let wh = web_handler.clone();
        let cc = current_config.clone();
        rls.push(Reloader::new("web_handler", move |c| {
            *cc.write().unwrap_or_else(|p| p.into_inner()) = c.clone();
            wh.apply_config(c.clone());
            Ok(())
        }));
    }
    {
        let qe = query_engine.clone();
        let enable_query_cost = fo.enable_query_cost;
        rls.push(Reloader::new("query_engine", move |c| {
            let Some(qe) = &qe else {
                // No-op in agent mode.
                return Ok(());
            };
            if enable_query_cost {
                qe.set_query_limits(
                    c.global_config.query_max_series,
                    c.global_config.query_max_samples_read,
                    c.global_config.query_max_duration.to_std(),
                );
            }
            if c.global_config.query_log_file.is_empty() {
                qe.set_query_logger(None);
                return Ok(());
            }
            let l = promql::query_logger::JsonFileLogger::new(&c.global_config.query_log_file).map_err(|e| e.to_string())?;
            qe.set_query_logger(Some(Arc::new(l)));
            Ok(())
        }));
    }
    {
        // The scrape and notifier managers reload before the discovery managers: they must read the new config
        // when they receive the new target lists.
        let sm = scrape_manager.clone();
        rls.push(Reloader::new("scrape", move |c| sm.apply_config(c)));
    }
    {
        let dm = dm_scrape.clone();
        rls.push(Reloader::new("scrape_sd", move |c| {
            let mut m: BTreeMap<String, discovery::Configs> = BTreeMap::new();
            for v in c.get_scrape_configs()? {
                m.insert(v.job_name.clone(), v.service_discovery_configs.clone());
            }
            dm.apply_config(&m);
            Ok(())
        }));
    }
    {
        let nm = notifier_manager.clone();
        rls.push(Reloader::new("notify", move |c| nm.apply_config(c)));
    }
    {
        let dm = dm_notify.clone();
        rls.push(Reloader::new("notify_sd", move |c| {
            let mut m: BTreeMap<String, discovery::Configs> = BTreeMap::new();
            for (k, v) in c.alerting_config.alertmanager_configs_map() {
                m.insert(k, v.service_discovery_configs.clone());
            }
            dm.apply_config(&m);
            Ok(())
        }));
    }
    {
        let rm = rule_manager.clone();
        let eu = external_url.to_url_string();
        rls.push(Reloader::new("rules", move |c| {
            let Some(rm) = &rm else {
                return Ok(());
            };
            let mut files = Vec::new();
            for pat in &c.rule_files {
                match discovery::glob::glob(pat) {
                    Ok(fs) => files.extend(fs),
                    Err(e) => return Err(format!("error retrieving rule files for {pat}: {e}")),
                }
            }
            rm.update(c.global_config.evaluation_interval.to_std(), &files, &c.global_config.external_labels, &eu, None)
                .map_err(|e| e.to_string())
        }));
    }
    // Tracing (OTLP export) is not part of this port: the tracing config is accepted and ignored.
    rls.push(Reloader::new("tracing", |_| Ok(())));

    for c in [reload::config_success().clone(), reload::config_success_time().clone()] {
        if let Err(e) = reg.register(c) {
            panic!("{e}");
        }
    }
    let _ = reg.register(Arc::new(reload::GoGcCollector));
    if let Err(e) = crate::metrics::register(&reg, &notifs) {
        panic!("{e}");
    }

    let reloading = Arc::new(Reloading {
        rls,
        filename: cfg.config_file.clone(),
        enable_exemplar_storage: fo.tsdb_enable_exemplar_storage,
        agent_mode,
        logger: logger.clone(),
        no_step: no_step.clone(),
        root_logger: root_logger.clone(),
    });

    let db_open = Event::new();
    let reload_ready = Event::new();

    let listeners = match web_handler.listeners() {
        Ok(l) => l,
        Err(e) => {
            logger.error("Unable to start web listener", &[("err", &e)]);
            exit(1);
        }
    };
    if !cfg.web_config_file.is_empty() {
        logger.error("Unable to validate web configuration file", &[("err", "web config files (TLS, basic auth) are not supported by this port")]);
        exit(1);
    }

    // ---- Signals and channels.
    let (term_tx, term_rx) = channel::<TermEvent>();
    let (reload_tx, reload_rx) = channel::<ReloadEvent>();
    {
        let term_tx = term_tx.clone();
        let reload_tx = reload_tx.clone();
        std::thread::Builder::new()
            .name("signals".into())
            .stack_size(64 * 1024)
            .spawn(move || {
                loop {
                    match sys::wait_signal() {
                        sys::Signal::Hangup => {
                            let _ = reload_tx.send(ReloadEvent::Hup);
                        }
                        s => {
                            let _ = term_tx.send(TermEvent::Signal(s));
                        }
                    }
                }
            })
            .expect("spawn signal thread");
    }
    {
        let term_tx = term_tx.clone();
        let wh = web_handler.clone();
        std::thread::Builder::new()
            .name("web-quit".into())
            .stack_size(64 * 1024)
            .spawn(move || {
                wh.quit_signal().wait();
                let _ = term_tx.send(TermEvent::WebQuit);
            })
            .expect("spawn quit thread");
    }
    if let Some(rx) = web_handler.reload_requests() {
        let reload_tx = reload_tx.clone();
        std::thread::Builder::new()
            .name("web-reload".into())
            .stack_size(64 * 1024)
            .spawn(move || {
                while let Ok(r) = rx.recv() {
                    if reload_tx.send(ReloadEvent::Web(r)).is_err() {
                        break;
                    }
                }
            })
            .expect("spawn reload thread");
    }
    sys::start_trim_thread();

    let mut g = Group::new();
    {
        // Termination handler.
        let rr = reload_ready.clone();
        let l = logger.clone();
        let wh = web_handler.clone();
        let nf = notifs.clone();
        let term_tx = term_tx.clone();
        g.add(
            "termination",
            move || {
                match term_rx.recv() {
                    Ok(TermEvent::Signal(s)) => {
                        l.warn("Received an OS signal, exiting gracefully...", &[("signal", s.go_name())]);
                        rr.set();
                    }
                    Ok(TermEvent::WebQuit) => {
                        l.warn("Received termination request via web service, exiting gracefully...", &[]);
                    }
                    Ok(TermEvent::Cancel) | Err(_) => rr.set(),
                }
                Ok(())
            },
            move |_| {
                let _ = term_tx.send(TermEvent::Cancel);
                wh.set_ready(web::web::ReadyStatus::Stopping);
                nf.add_notification(web::notifications::SHUTTING_DOWN);
            },
        );
    }
    {
        // Scrape discovery manager.
        let dm = dm_scrape.clone();
        let (l1, l2) = (logger.clone(), logger.clone());
        let ctx = ctx_scrape.clone();
        g.add(
            "discovery-scrape",
            move || {
                let r = dm.run();
                l1.info("Scrape discovery manager stopped", &[]);
                r
            },
            move |_| {
                l2.info("Stopping scrape discovery manager...", &[]);
                ctx.cancel();
            },
        );
    }
    {
        // Notify discovery manager.
        let dm = dm_notify.clone();
        let (l1, l2) = (logger.clone(), logger.clone());
        let ctx = ctx_notify.clone();
        g.add(
            "discovery-notify",
            move || {
                let r = dm.run();
                l1.info("Notify discovery manager stopped", &[]);
                r
            },
            move |_| {
                l2.info("Stopping notify discovery manager...", &[]);
                ctx.cancel();
            },
        );
    }
    if let Some(rm) = rule_manager.clone() {
        // Rule manager.
        let rr = reload_ready.clone();
        let (l1, l2) = (logger.clone(), logger.clone());
        let rm2 = rm.clone();
        g.add(
            "rule-manager",
            move || {
                rr.wait();
                rm.run();
                l1.info("Rule manager stopped", &[]);
                Ok(())
            },
            move |_| {
                l2.info("Stopping rule manager manager...", &[]);
                rm2.stop();
            },
        );
    }
    {
        // Scrape manager.
        let rr = reload_ready.clone();
        let (l1, l2) = (logger.clone(), logger.clone());
        let sm = scrape_manager.clone();
        let sm2 = scrape_manager.clone();
        let dm = dm_scrape.clone();
        g.add(
            "scrape-manager",
            move || {
                // The scrape manager needs a valid config for each job: wait until the config is loaded.
                rr.wait();
                let r = sm.run(dm.sync_ch());
                l1.info("Scrape manager stopped", &[]);
                r
            },
            move |_| {
                // Stop scraping before the TSDB closes, and after the rule manager stopped (no false absent()
                // alerts).
                l2.info("Stopping scrape manager...", &[]);
                sm2.stop();
            },
        );
    }
    {
        // Reload handler.
        let rr = reload_ready.clone();
        let l = logger.clone();
        let l2 = logger.clone();
        let reloading = reloading.clone();
        let nf = notifs.clone();
        let enable_auto_reload = cfg.enable_auto_reload;
        let interval = Duration::from_nanos(cfg.auto_reload_interval.max(1) as u64);
        let filename = cfg.config_file.clone();
        let mut checksum = String::new();
        if enable_auto_reload {
            match config::generate_checksum(&filename) {
                Ok(c) => checksum = c,
                Err(e) => logger.error("Failed to generate initial checksum for configuration file", &[("err", &e)]),
            }
        }
        let reload_tx2 = reload_tx.clone();
        g.add(
            "reload",
            move || {
                rr.wait();
                let checksum = std::cell::RefCell::new(checksum);
                let mut callback = |success: bool| {
                    if success {
                        nf.delete_notification(web::notifications::CONFIGURATION_UNSUCCESSFUL);
                        return;
                    }
                    *checksum.borrow_mut() = String::new();
                    nf.add_notification(web::notifications::CONFIGURATION_UNSUCCESSFUL);
                };
                let regenerate = |checksum: &std::cell::RefCell<String>| match config::generate_checksum(&filename) {
                    Ok(c) => *checksum.borrow_mut() = c,
                    Err(e) => l.error("Failed to generate checksum during configuration reload", &[("err", &e)]),
                };
                let mut next_tick = std::time::Instant::now() + interval;
                loop {
                    let ev = if enable_auto_reload {
                        let now = std::time::Instant::now();
                        match reload_rx.recv_timeout(next_tick.saturating_duration_since(now)) {
                            Ok(ev) => Some(ev),
                            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
                            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
                        }
                    } else {
                        match reload_rx.recv() {
                            Ok(ev) => Some(ev),
                            Err(_) => return Ok(()),
                        }
                    };
                    match ev {
                        Some(ReloadEvent::Hup) => {
                            if let Err(e) = reloading.reload(&mut callback) {
                                l.error("Error reloading config", &[("err", &e)]);
                            } else if enable_auto_reload {
                                regenerate(&checksum);
                            }
                        }
                        Some(ReloadEvent::Web(rc)) => {
                            match reloading.reload(&mut callback) {
                                Err(e) => {
                                    l.error("Error reloading config", &[("err", &e)]);
                                    let _ = rc.send(Err(e));
                                }
                                Ok(()) => {
                                    let _ = rc.send(Ok(()));
                                    if enable_auto_reload {
                                        regenerate(&checksum);
                                    }
                                }
                            }
                        }
                        Some(ReloadEvent::Cancel(ack)) => {
                            l.info("Reloaders stopped", &[]);
                            let _ = ack.send(());
                            return Ok(());
                        }
                        None => {
                            next_tick = std::time::Instant::now() + interval;
                            let current = match config::generate_checksum(&filename) {
                                Ok(c) => {
                                    if c == *checksum.borrow() {
                                        continue;
                                    }
                                    c
                                }
                                Err(e) => {
                                    *checksum.borrow_mut() = String::new();
                                    l.error("Failed to generate checksum during configuration reload", &[("err", &e)]);
                                    String::new()
                                }
                            };
                            l.info("Configuration file change detected, reloading the configuration.", &[]);
                            if let Err(e) = reloading.reload(&mut callback) {
                                l.error("Error reloading config", &[("err", &e)]);
                            } else {
                                *checksum.borrow_mut() = current;
                            }
                        }
                    }
                }
            },
            move |_| {
                // Wait for an in-progress reload to finish, so nothing reloads after it was shut down.
                l2.info("Stopping reloaders...", &[]);
                let (ack_tx, ack_rx) = channel();
                if reload_tx2.send(ReloadEvent::Cancel(ack_tx)).is_ok() {
                    let _ = ack_rx.recv();
                }
            },
        );
    }
    {
        // Initial configuration loading.
        let wake = Event::new();
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (dbo, rr, l, wh, nf, reloading) =
            (db_open.clone(), reload_ready.clone(), logger.clone(), web_handler.clone(), notifs.clone(), reloading.clone());
        let (w2, c2) = (wake.clone(), cancelled.clone());
        let filename = cfg.config_file.clone();
        // The TSDB actor wakes this one too when the DB is open.
        let db_open_wake = wake.clone();
        let dbo2 = db_open.clone();
        std::thread::Builder::new()
            .name("db-open-wait".into())
            .stack_size(64 * 1024)
            .spawn(move || {
                dbo2.wait();
                db_open_wake.set();
            })
            .expect("spawn thread");
        let cancel_ev = Event::new();
        let cancel_ev2 = cancel_ev.clone();
        g.add(
            "initial-config",
            move || {
                wake.wait();
                if !dbo.is_set() {
                    // Shutdown before the storage opened.
                    rr.set();
                    return Ok(());
                }
                if let Err(e) = reloading.reload(&mut |_| {}) {
                    return Err(format!("error loading config from {}: {e}", yaml::go_quote(&filename)));
                }
                rr.set();
                wh.set_ready(web::web::ReadyStatus::Ready);
                nf.delete_notification(web::notifications::STARTING_UP);
                l.info("Server is ready to receive web requests.", &[]);
                // Startup is over: give back its heap and the pages of code that only startup ran.
                sys::on_idle();
                cancel_ev.wait();
                Ok(())
            },
            move |_| {
                c2.store(true, std::sync::atomic::Ordering::SeqCst);
                cancel_ev2.set();
                w2.set();
            },
        );
    }
    if !agent_mode {
        // TSDB.
        let opts = tsdb::db::Options {
            wal_segment_size: cfg.tsdb.wal_segment_size,
            max_block_chunk_segment_size: cfg.tsdb.max_block_chunk_segment_size,
            retention_duration: retention_duration / 1_000_000,
            max_bytes,
            max_percentage,
            no_lockfile: cfg.tsdb.no_lockfile,
            wal_compression: tsdb_wal_compression,
            head_chunks_write_queue_size: cfg.tsdb.head_chunks_write_queue_size,
            samples_per_chunk: cfg.tsdb.samples_per_chunk,
            stripe_size: 0,
            min_block_duration: cfg.tsdb.min_block_duration / 1_000_000,
            max_block_duration: max_block_duration / 1_000_000,
            enable_exemplar_storage: fo.tsdb_enable_exemplar_storage,
            max_exemplars,
            enable_memory_snapshot_on_shutdown: fo.tsdb_enable_memory_snapshot_on_shutdown,
            out_of_order_time_window: tsdb_conf.out_of_order_time_window,
            enable_delayed_compaction: fo.tsdb_enable_delayed_compaction,
            compaction_delay_max_percent: compaction_delay_max_percent as i32,
            enable_overlapping_compaction: cfg.tsdb.enable_overlapping_compaction,
            use_uncached_io: fo.tsdb_use_uncached_io,
            block_reload_interval: Duration::from_nanos(cfg.tsdb.block_reload_interval.max(0) as u64),
            enable_st_as_zero_sample: fo.tsdb_enable_st_as_zero_sample,
            enable_st_storage: fo.tsdb_enable_st_storage,
            enable_histogram_st_encoding: fo.tsdb_enable_histogram_st_encoding,
            stale_series_compaction_threshold: tsdb_conf.stale_series_compaction_threshold,
            enable_fast_startup: fo.tsdb_enable_fast_startup,
            float_chunk_encoding: float_encoding,
            enable_metadata_wal_records: fo.tsdb_enable_metadata_wal_records,
            ..tsdb_opts
        };
        let cancel = Event::new();
        let cancel2 = cancel.clone();
        let (l, l2) = (logger.clone(), logger.clone());
        let (ls, rs, dbo, reg2) = (local_storage.clone(), remote_storage.clone(), db_open.clone(), reg.clone());
        let fo2 = fanout.clone();
        let path = local_storage_path.clone();
        let (wal_seg, chunk_seg, min_block) = (cfg.tsdb.wal_segment_size, cfg.tsdb.max_block_chunk_segment_size, cfg.tsdb.min_block_duration);
        g.add(
            "tsdb",
            move || {
                l.info("Starting TSDB ...", &[]);
                if wal_seg != 0 && !(10 * 1024 * 1024..=256 * 1024 * 1024).contains(&wal_seg) {
                    return Err("flag 'storage.tsdb.wal-segment-size' must be set between 10MB and 256MB".to_string());
                }
                if chunk_seg != 0 && chunk_seg < 1024 * 1024 {
                    return Err("flag 'storage.tsdb.max-block-chunk-segment-size' must be set over 1MB".to_string());
                }
                // The WAL replay and the block loading recurse and allocate big buffers: open on a thread with a
                // large stack.
                let tl = tsdb_logger(&l.with(&[("component", "tsdb")]));
                let stats = ls.stats();
                let path2 = path.clone();
                let reg3 = reg2.clone();
                let db = std::thread::Builder::new()
                    .name("tsdb-open".into())
                    .stack_size(QUERY_STACK_SIZE)
                    .spawn(move || crate::ready::open_db_with_metrics(&path2, Some(tl), &reg3, Some(opts), Some(stats)))
                    .map_err(|e| e.to_string())?
                    .join()
                    .map_err(|_| "opening storage failed: panic".to_string())?
                    .map_err(|e| format!("opening storage failed: {e}"))?;
                if let Err(e) = crate::selfmetrics::register_tsdb_metrics(&reg2, &db) {
                    l.error("failed to register TSDB metrics", &[("err", &e)]);
                }
                match sys::fs_type(&path).as_str() {
                    "NFS_SUPER_MAGIC" => l.warn("This filesystem is not supported and may lead to data corruption and data loss. Please carefully read https://prometheus.io/docs/prometheus/latest/storage/ to learn more about supported filesystems.", &[("fs_type", "NFS_SUPER_MAGIC")]),
                    t => l.info("filesystem information", &[("fs_type", t)]),
                }
                l.info("TSDB started", &[]);
                // startTimeMargin: 2 * MinBlockDuration in milliseconds.
                let start_time_margin = 2 * (min_block / 1_000_000_000) * 1000;
                ls.set(Some(LocalDb::Tsdb(db.clone())), start_time_margin);
                db.set_write_notified(Some(Arc::new(RemoteNotify(rs))));
                dbo.set();
                cancel.wait();
                l.info("TSDB stopped", &[]);
                Ok(())
            },
            move |_| {
                l2.info("Stopping storage...", &[]);
                if let Some(e) = fo2.close() {
                    l2.error("Error stopping storage", &[("err", &e.to_string())]);
                }
                cancel2.set();
            },
        );
    } else {
        // WAL storage (agent mode).
        let opts = agent::Options {
            wal_segment_size: cfg.agent.wal_segment_size,
            wal_compression: tsdb_wal_compression,
            stripe_size: 0,
            truncate_frequency: Duration::from_nanos(cfg.agent.truncate_frequency.max(0) as u64),
            min_wal_time: cfg.agent.min_wal_time / 1_000_000,
            max_wal_time: cfg.agent.max_wal_time / 1_000_000,
            no_lockfile: cfg.agent.no_lockfile,
            out_of_order_time_window: tsdb_conf.out_of_order_time_window.max(0),
            enable_st_as_zero_sample: fo.agent_enable_st_as_zero_sample,
            enable_st_storage: fo.agent_enable_st_storage,
            enable_metadata_wal_records: fo.agent_enable_metadata_wal_records,
            checkpoint_from_in_memory_series: cfg.agent.checkpoint_from_in_memory_series,
            checkpoint_batch_size: cfg.agent.checkpoint_batch_size.max(0) as usize,
        };
        let cancel = Event::new();
        let cancel2 = cancel.clone();
        let (l, l2) = (logger.clone(), logger.clone());
        let (ls, rs, dbo) = (local_storage.clone(), remote_storage.clone(), db_open.clone());
        let fo2 = fanout.clone();
        let path = local_storage_path.clone();
        let wal_seg = cfg.agent.wal_segment_size;
        g.add(
            "agent-storage",
            move || {
                l.info("Starting WAL storage ...", &[]);
                if wal_seg != 0 && !(10 * 1024 * 1024..=256 * 1024 * 1024).contains(&wal_seg) {
                    return Err("flag 'storage.agent.wal-segment-size' must be set between 10MB and 256MB".to_string());
                }
                let al = tsdb_logger(&l.with(&[("component", "agent")]));
                let rsn: Arc<dyn agent::LowestSentTimestamp> = Arc::new(RemoteNotify(rs.clone()));
                let db = agent::open(al, Some(rsn), std::path::Path::new(&path), Some(opts))
                    .map_err(|e| format!("opening storage failed: {e}"))?;
                let db = Arc::new(db);
                let fs_type = sys::fs_type(&path);
                if fs_type == "NFS_SUPER_MAGIC" {
                    l.warn(&fs_type, &[("msg", "This filesystem is not supported and may lead to data corruption and data loss. Please carefully read https://prometheus.io/docs/prometheus/latest/storage/ to learn more about supported filesystems.")]);
                } else {
                    l.info(&fs_type, &[]);
                }
                l.info("Agent WAL storage started", &[]);
                ls.set(Some(LocalDb::Other(db.clone() as Arc<dyn Storage>)), 0);
                db.set_write_notified(Some(Arc::new(RemoteNotify(rs))));
                dbo.set();
                cancel.wait();
                l.info("Agent WAL storage stopped", &[]);
                Ok(())
            },
            move |_| {
                l2.info("Stopping agent WAL storage...", &[]);
                if let Some(e) = fo2.close() {
                    l2.error("Error stopping storage", &[("err", &e.to_string())]);
                }
                cancel2.set();
            },
        );
    }
    {
        // Web handler.
        let token = web::web::CancelToken::new();
        let t2 = token.clone();
        let (l1, l2) = (logger.clone(), logger.clone());
        let wh = web_handler.clone();
        let web_config_file = cfg.web_config_file.clone();
        g.add(
            "web",
            move || {
                wh.run(&token, listeners, &web_config_file).map_err(|e| format!("error starting web server: {e}"))?;
                l1.info("Web handler stopped", &[]);
                Ok(())
            },
            move |_| {
                l2.info("Stopping web handler...", &[]);
                t2.cancel();
            },
        );
    }
    {
        // Notifier. Its interrupt comes after the rule manager's.
        let rr = reload_ready.clone();
        let (l1, l2) = (logger.clone(), logger.clone());
        let (nm, nm2) = (notifier_manager.clone(), notifier_manager.clone());
        let dm = dm_notify.clone();
        g.add(
            "notifier",
            move || {
                rr.wait();
                nm.run(Some(Box::new(dm.sync_ch())));
                l1.info("Notifier manager stopped", &[]);
                Ok(())
            },
            move |_| {
                l2.info("Stopping notifier manager...", &[]);
                nm2.stop();
            },
        );
    }
    drop(term_tx);
    drop(reload_tx);
    if let Err(e) = g.run() {
        logger.error("Fatal error", &[("err", &e)]);
        exit(1);
    }
    logger.info("See you next time!", &[]);
    exit(0);
}

/// Go `%q` of a `[]string`: `["--a" "--b"]`.
fn go_quote_list(v: &[String]) -> String {
    format!("[{}]", v.iter().map(|s| yaml::go_quote(s)).collect::<Vec<_>>().join(" "))
}

/// Go `parseCompressionType`.
fn compression_type(compress: bool, t: &str) -> tsdb_format::wlog::compression::Type {
    use tsdb_format::wlog::compression::Type;
    if !compress {
        return Type::None;
    }
    match t {
        "zstd" => Type::Zstd,
        _ => Type::Snappy,
    }
}

/// The remote storage as the WAL's write notification target (Go `db.SetWriteNotified(remoteStorage)`).
struct RemoteNotify(Arc<remote::storage::Storage>);

impl tsdb_format::wlog::WriteNotified for RemoteNotify {
    fn notify(&self) {
        self.0.notify();
    }
}

impl agent::LowestSentTimestamp for RemoteNotify {
    fn lowest_sent_timestamp(&self) -> i64 {
        self.0.lowest_sent_timestamp()
    }
}

#[allow(dead_code)]
fn _assert_send(_: Receiver<()>) {}
