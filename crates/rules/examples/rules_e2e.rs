//! End-to-end check of rules and notifier: loads a Prometheus config, scrapes its targets into the TSDB (opened
//! in <data-dir> with the server's default options), evaluates the rule files with the PromQL engine, and sends alerts to
//! the Alertmanagers of the config, as the Prometheus server wires these parts.
//!
//! Usage: rules_e2e <prometheus.yml> <external-url> <seconds> <data-dir>

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use storage::{AppendableV2, Queryable};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        eprintln!("usage: rules_e2e <prometheus.yml> <external-url> <seconds> <data-dir>");
        std::process::exit(2);
    }
    let external_url = args[2].clone();
    let secs: u64 = args[3].parse().expect("seconds");
    let logger = discovery::Logger::new(Arc::new(|l, m: &str, kv: &[(String, String)]| {
        eprintln!("level={} msg={m:?} {}", l.as_str(), kv.iter().map(|(k, v)| format!("{k}={v:?}")).collect::<Vec<_>>().join(" "));
    }));
    let cfg = config::load_file(&args[1], false, &logger).expect("load config");
    let reg = discovery::Registry::new();
    let sd_metrics = discovery::create_and_register_sd_metrics(&reg).expect("sd metrics");
    let ctx = discovery::CancelToken::new();

    // Discovery for scraping and for the Alertmanagers.
    let dm_scrape = discovery::Manager::new(&ctx, logger.clone(), &reg, &sd_metrics, discovery::ManagerOptions { name: "scrape".into(), ..Default::default() })
        .expect("discovery manager");
    let mut sd: BTreeMap<String, discovery::Configs> = BTreeMap::new();
    for sc in cfg.get_scrape_configs().expect("scrape configs") {
        sd.insert(sc.job_name.clone(), sc.service_discovery_configs.clone());
    }
    dm_scrape.apply_config(&sd);
    let dm_notify = discovery::Manager::new(&ctx, logger.clone(), &reg, &sd_metrics, discovery::ManagerOptions { name: "notify".into(), ..Default::default() })
        .expect("discovery manager");
    let mut sd: BTreeMap<String, discovery::Configs> = BTreeMap::new();
    for (k, am) in cfg.alerting_config.alertmanager_configs_map() {
        sd.insert(k, am.service_discovery_configs.clone());
    }
    dm_notify.apply_config(&sd);
    for dm in [dm_scrape.clone(), dm_notify.clone()] {
        std::thread::spawn(move || {
            let _ = dm.run();
        });
    }

    let st = tsdb::db::open(std::path::Path::new(&args[4]), None, Some(tsdb::db::prometheus_options(15 * 24 * 3600 * 1000, false)), None).expect("open tsdb");

    // Notifier.
    let nm = Arc::new(notifier::Manager::new(
        notifier::Options { queue_capacity: 10_000, registerer: Some(reg.clone()), ..Default::default() },
        model::labels::ValidationScheme::Utf8,
        Some(logger.clone()),
    ));
    nm.apply_config(&cfg).expect("notifier config");
    let nm2 = nm.clone();
    let rx = dm_notify.sync_ch();
    let notifier_thread = std::thread::spawn(move || nm2.run(Some(Box::new(rx))));

    // Scrape.
    let sm = scrape::Manager::new(None, Some(logger.clone()), None, None, Some(st.clone() as Arc<dyn AppendableV2>), Some(reg.clone()))
        .expect("scrape manager");
    sm.apply_config(&cfg).expect("apply config");
    let sm2 = sm.clone();
    let rx = dm_scrape.sync_ch();
    std::thread::spawn(move || {
        let _ = sm2.run(rx);
    });

    // Rules (as cmd/prometheus/main.go sets them up, with its default flag values).
    let engine = Arc::new(promql::Engine::new(promql::EngineOpts {
        max_samples: 50_000_000,
        timeout: Duration::from_secs(120),
        enable_at_modifier: true,
        enable_negative_offset: true,
        ..Default::default()
    }));
    let rm = Arc::new(rules::Manager::new(rules::ManagerOptions {
        appendable_v2: Some(st.clone() as Arc<dyn AppendableV2>),
        queryable: Some(st.clone() as Arc<dyn Queryable>),
        query_func: Some(rules::engine_query_func(engine, st.clone() as Arc<dyn Queryable>)),
        notify_func: Some(rules::send_alerts(nm.clone() as Arc<dyn rules::Sender>, &external_url)),
        external_url: Some(yaml::url::parse(&external_url).expect("external url")),
        registerer: Some(reg.clone()),
        logger: logger.clone(),
        outage_tolerance: Duration::from_secs(3600),
        for_grace_period: Duration::from_secs(600),
        resend_delay: Duration::from_secs(60),
        ..Default::default()
    }));
    let base = std::path::Path::new(&args[1]).parent().map(|p| p.to_path_buf()).unwrap_or_default();
    let mut files = Vec::new();
    for pat in &cfg.rule_files {
        let pat = if std::path::Path::new(pat).is_absolute() { pat.clone() } else { base.join(pat).to_string_lossy().into_owned() };
        files.extend(discovery::glob::glob(&pat).expect("rule file glob"));
    }
    rm.update(
        cfg.global_config.evaluation_interval.to_std(),
        &files,
        &cfg.global_config.external_labels,
        &external_url,
        None,
    )
    .expect("load rules");
    let rm2 = rm.clone();
    let rules_thread = std::thread::spawn(move || rm2.run());

    std::thread::sleep(Duration::from_secs(secs));
    for g in rm.rule_groups() {
        for r in g.all_rules() {
            let state = r.as_alerting().map(|a| a.state().to_string()).unwrap_or_default();
            println!("rule group={} name={} health={} state={state} last_error={:?}", g.name(), r.name(), r.health(), r.last_error().map(|e| e.to_string()));
        }
    }
    rm.stop();
    let _ = rules_thread.join();
    sm.stop();
    nm.stop();
    let _ = notifier_thread.join();
    ctx.cancel();
    st.close().expect("close tsdb");
}
