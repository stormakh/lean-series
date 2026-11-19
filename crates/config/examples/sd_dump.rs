//! End-to-end check tool for service discovery: loads a Prometheus config file, runs the discovery manager on
//! its scrape configs for a while, and prints the discovered targets as JSON lines
//! `{"job":..., "labels":{...}}` (the target labels merged over the group labels: `__address__` and `__meta_*`,
//! as in Prometheus' `discoveredLabels` without the scrape defaults), sorted.
//!
//! usage: cargo run -p config --example sd_dump -- <prometheus.yml> [seconds]
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use discovery::{CancelToken, Logger, Manager, ManagerOptions, Registry};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = args.get(1).expect("usage: sd_dump <prometheus.yml> [seconds]");
    let secs: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10);
    let cfg = config::load_file(path, false, &Logger::nop()).unwrap_or_else(|e| panic!("load {path}: {e}"));
    let scfgs = cfg.get_scrape_configs().expect("scrape configs");
    let mut sd: BTreeMap<String, discovery::Configs> = BTreeMap::new();
    for sc in &scfgs {
        sd.insert(sc.job_name.clone(), sc.service_discovery_configs.clone());
    }
    let reg = Registry::new();
    let sd_metrics = discovery::create_and_register_sd_metrics(&reg).expect("sd metrics");
    let ctx = CancelToken::new();
    let m = Manager::new(&ctx, Logger::nop(), &reg, &sd_metrics, ManagerOptions { name: "scrape".into(), updatert: Duration::from_millis(500), ..Default::default() }).expect("manager");
    m.apply_config(&sd);
    let runner = m.clone();
    let t = std::thread::spawn(move || runner.run());
    let rx = m.sync_ch();
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut last = None;
    while Instant::now() < deadline {
        if let Ok(ts) = rx.recv_timeout(deadline - Instant::now()) {
            last = Some(ts);
        }
    }
    ctx.cancel();
    let _ = t.join();
    let mut lines = Vec::new();
    for (job, groups) in last.unwrap_or_default() {
        for g in groups {
            for t in &g.targets {
                let mut l = g.labels.clone();
                for (k, v) in &t.0 {
                    l.0.insert(k.clone(), v.clone());
                }
                lines.push(json::Json::obj().with("job", job.as_str()).with("labels", l.to_json()).stringify());
            }
        }
    }
    lines.sort();
    for l in lines {
        println!("{l}");
    }
}
