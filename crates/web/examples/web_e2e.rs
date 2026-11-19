//! End-to-end check of the web layer: TSDB + service discovery + scrape + PromQL + the web handler (API v1, UI,
//! federation, lifecycle endpoints, /metrics), serving like `prometheus --config.file ... --web.listen-address ...`.
//!
//! Usage: web_e2e <prometheus.yml> <listen address> <data dir>
//! Stops on SIGTERM/Ctrl-C (the process is killed) or after a `POST /-/quit` when started with WEB_E2E_LIFECYCLE=1.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use web::api::PrometheusVersion;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: web_e2e <prometheus.yml> <listen address> <data dir>");
        std::process::exit(2);
    }
    let (cfg_file, listen, data_dir) = (args[1].clone(), args[2].clone(), args[3].clone());
    let logger = discovery::Logger::nop();
    let mut cfg = config::load_file(&cfg_file, false, &logger).expect("load config");
    // Go's main sets config.DefaultTSDBRetentionConfig from the retention flags (default 15d).
    if let Some(t) = cfg.storage_config.tsdb_config.as_mut() {
        let r = t.retention.get_or_insert_with(Default::default);
        if r.time.0 == 0 && r.size.0 == 0 {
            r.time = yaml::types::Duration(15 * 24 * 3600 * 1_000_000_000);
        }
    }
    let cfg = Arc::new(cfg);

    // TSDB (Prometheus defaults: 15d retention, exemplar storage off).
    let opts = tsdb::db::prometheus_options(15 * 24 * 3600 * 1000, false);
    let db = tsdb::db::open(std::path::Path::new(&data_dir), None, Some(opts), None).expect("open tsdb");

    // Discovery and scrape.
    let reg = web::metrics::default_registry().clone();
    let sd_metrics = discovery::create_and_register_sd_metrics(&reg).expect("sd metrics");
    let ctx = discovery::CancelToken::new();
    let dm = discovery::Manager::new(&ctx, logger.clone(), &reg, &sd_metrics, discovery::ManagerOptions::default()).expect("discovery manager");
    let mut sd: BTreeMap<String, discovery::Configs> = BTreeMap::new();
    for sc in cfg.get_scrape_configs().expect("scrape configs") {
        sd.insert(sc.job_name.clone(), sc.service_discovery_configs.clone());
    }
    dm.apply_config(&sd);
    let dm2 = dm.clone();
    std::thread::spawn(move || {
        let _ = dm2.run();
    });
    let sopts = scrape::Options { discovery_reload_interval: Duration::from_secs(5), ..Default::default() };
    let sm = scrape::Manager::new(Some(sopts), Some(logger), None, None, Some(db.clone()), Some(reg.clone())).expect("scrape manager");
    sm.apply_config(&cfg).expect("apply config");
    let sm2 = sm.clone();
    let rx = dm.sync_ch();
    std::thread::spawn(move || {
        let _ = sm2.run(rx);
    });

    // PromQL (Prometheus defaults: 2m timeout, 50M samples, @ and negative offsets on).
    let engine = Arc::new(promql::Engine::new(promql::EngineOpts {
        max_samples: 50_000_000,
        timeout: Duration::from_secs(120),
        lookback_delta: Duration::from_secs(300),
        enable_at_modifier: true,
        enable_negative_offset: true,
        no_step_subquery_interval_fn: Some({
            let ms = cfg.global_config.evaluation_interval.0 / 1_000_000;
            Arc::new(move |_| ms)
        }),
        ..Default::default()
    }));

    let mut flags = BTreeMap::new();
    flags.insert("config.file".to_string(), cfg_file.clone());
    flags.insert("web.listen-address".to_string(), listen.clone());
    flags.insert("storage.tsdb.path".to_string(), data_dir.clone());
    // Go's default external URL: http://<hostname>:<port>/.
    let port = listen.rsplit(':').next().unwrap_or("9090").to_string();
    let hostname = std::fs::read_to_string("/proc/sys/kernel/hostname").unwrap_or_default().trim().to_string();
    let features = Arc::new(promql::features::Registry::new());
    let adapter = Arc::new(web::web::TsdbAdapter(db.clone()));
    let h = web::web::Handler::new(web::web::Options {
        tsdb_retention_duration: yaml::types::Duration(15 * 24 * 3600 * 1_000_000_000),
        tsdb_dir: data_dir.clone(),
        local_storage: Some(adapter.clone()),
        tsdb_admin: Some(adapter),
        storage: Some(db.clone()),
        exemplar_storage: Some(db.clone()),
        cost_sampling: Some(db.clone()),
        query_engine: Some(engine),
        lookback_delta: Duration::from_secs(300),
        query_timeout: Duration::from_secs(120),
        scrape_pools_retriever: Some(sm.clone()),
        target_retriever: Some(sm.clone()),
        version: Some(PrometheusVersion {
            version: env!("CARGO_PKG_VERSION").to_string(),
            revision: String::new(),
            branch: String::new(),
            build_user: String::new(),
            build_date: String::new(),
            go_version: "rust".to_string(),
        }),
        flags,
        listen_addresses: vec![listen.clone()],
        external_url: web::web::ExternalUrl::parse(&format!("http://{}:{}", hostname, port)),
        route_prefix: "/".to_string(),
        enable_lifecycle: std::env::var("WEB_E2E_LIFECYCLE").is_ok(),
        page_title: "Prometheus Time Series Collection and Processing Server".to_string(),
        app_name: "Prometheus Server".to_string(),
        registerer: Some(reg.clone()),
        feature_registry: Some(features),
        ..Default::default()
    });
    h.apply_config(cfg.clone());
    h.set_ready(web::web::ReadyStatus::Ready);
    let listeners = h.listeners().expect("listen");
    eprintln!("web_e2e listening on {}", listen);
    let token = web::web::CancelToken::new();
    let (h2, t2) = (h.clone(), token.clone());
    std::thread::spawn(move || {
        h2.quit_signal().wait();
        t2.cancel();
    });
    if let Err(e) = h.run(&token, listeners, "") {
        eprintln!("web: {}", e);
    }
    sm.stop();
    ctx.cancel();
    let _ = db.close();
}
