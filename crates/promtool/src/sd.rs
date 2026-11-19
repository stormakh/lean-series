//! Port of cmd/promtool/sd.go (`promtool check service-discovery`).
//!
//! Differs from Go: results of different target group sources are printed in source order (Go iterates a map).

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use config::ScrapeConfig;
use discovery::{CancelToken, DiscovererOptions, Group, Recv};
use model::labels::{Builder, Labels};

use crate::{FAILURE_EXIT_CODE, SUCCESS_EXIT_CODE};

/// Go `sdCheckResult`. `error` is the error text (Go marshals the error value as `{}`).
#[derive(Clone, Debug, PartialEq)]
pub struct SdCheckResult {
    pub discovered_labels: Labels,
    pub labels: Labels,
    pub error: Option<String>,
}

/// Go `CheckSD`. `timeout` is in nanoseconds.
pub fn check_sd(sd_config_files: &str, sd_job_name: &str, timeout: i64, out: &mut dyn Write, err: &mut dyn Write) -> i32 {
    let logger = crate::promslog::stderr_logger();
    let cfg = match config::load_file(sd_config_files, false, &logger) {
        Ok(c) => c,
        Err(e) => {
            let _ = writeln!(err, "Cannot load config {e}");
            return FAILURE_EXIT_CODE;
        }
    };
    let scfgs = match cfg.get_scrape_configs() {
        Ok(s) => s,
        Err(e) => {
            let _ = writeln!(err, "Cannot load scrape configs {e}");
            return FAILURE_EXIT_CODE;
        }
    };
    let mut jobs = Vec::new();
    let mut scrape_config = None;
    for v in scfgs.iter() {
        jobs.push(v.job_name.clone());
        if v.job_name == sd_job_name {
            scrape_config = Some(v.clone());
            break;
        }
    }
    let Some(scrape_config) = scrape_config else {
        let _ = writeln!(err, "Job {sd_job_name} not found. Select one of:");
        for j in jobs {
            let _ = writeln!(err, "\t{j}");
        }
        return FAILURE_EXIT_CODE;
    };

    let ctx = CancelToken::new();
    let (tx, rx) = discovery::channel();
    let mut handles = Vec::new();
    for c in scrape_config.service_discovery_configs.iter() {
        let reg = discovery::Registry::new();
        let refresh = discovery::new_refresh_metrics(&reg);
        let metrics = c.new_discoverer_metrics(&reg, refresh.clone());
        if let Err(e) = metrics.register() {
            let _ = writeln!(err, "Could not register service discovery metrics {e}");
            return FAILURE_EXIT_CODE;
        }
        let opts = DiscovererOptions { logger: logger.clone(), metrics: Some(metrics.clone()), ..Default::default() };
        let mut d = match c.new_discoverer(opts) {
            Ok(d) => d,
            Err(e) => {
                let _ = writeln!(err, "Could not create new discoverer {e}");
                return FAILURE_EXIT_CODE;
            }
        };
        let ctx = ctx.clone();
        let tx = tx.clone();
        handles.push(std::thread::spawn(move || {
            d.run(&ctx, tx);
            metrics.unregister();
        }));
    }
    drop(tx);

    let deadline = Instant::now() + Duration::from_nanos(timeout.max(0) as u64);
    let mut results_by_source: BTreeMap<String, Vec<Arc<Group>>> = BTreeMap::new();
    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        match rx.recv_timeout(deadline - now) {
            Recv::Update(tgs) => {
                for tg in tgs {
                    results_by_source.entry(tg.source.clone()).or_default().push(tg);
                }
            }
            Recv::Closed => {
                // Go keeps waiting for the context; nothing more can arrive.
                std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
                break;
            }
            Recv::Cancelled | Recv::Timeout => break,
        }
    }
    ctx.cancel();
    for h in handles {
        let _ = h.join();
    }

    let mut results = Vec::new();
    for tgs in results_by_source.values() {
        let groups: Vec<Group> = tgs.iter().map(|g| (**g).clone()).collect();
        results.extend(get_sd_check_result(&groups, &scrape_config));
    }
    let _ = write!(out, "{}", marshal_results(&results));
    SUCCESS_EXIT_CODE
}

fn labels_json(l: &Labels, indent: &str) -> String {
    let mut pairs: Vec<(String, String)> = Vec::new();
    l.range(|n, v| pairs.push((n.to_string(), v.to_string())));
    if pairs.is_empty() {
        return "{}".to_string();
    }
    let mut s = String::from("{\n");
    for (i, (k, v)) in pairs.iter().enumerate() {
        s.push_str(indent);
        s.push_str("  ");
        json::write_str(k, &mut s);
        s.push_str(": ");
        json::write_str(v, &mut s);
        if i + 1 < pairs.len() {
            s.push(',');
        }
        s.push('\n');
    }
    s.push_str(indent);
    s.push('}');
    s
}

/// Go `json.MarshalIndent(results, "", "  ")`.
pub fn marshal_results(results: &[SdCheckResult]) -> String {
    if results.is_empty() {
        return "[]".to_string();
    }
    let mut s = String::from("[\n");
    for (i, r) in results.iter().enumerate() {
        s.push_str("  {\n");
        s.push_str(&format!("    \"discoveredLabels\": {}", labels_json(&r.discovered_labels, "    ")));
        s.push_str(&format!(",\n    \"labels\": {}", labels_json(&r.labels, "    ")));
        if r.error.is_some() {
            s.push_str(",\n    \"error\": {}");
        }
        s.push_str("\n  }");
        if i + 1 < results.len() {
            s.push(',');
        }
        s.push('\n');
    }
    s.push(']');
    s
}

/// Go `getSDCheckResult`.
pub fn get_sd_check_result(target_groups: &[Group], scrape_config: &ScrapeConfig) -> Vec<SdCheckResult> {
    let mut results: Vec<SdCheckResult> = Vec::new();
    let mut lb = Builder::new(Labels::default());
    for tg in target_groups {
        for target in tg.targets.iter() {
            scrape::target::populate_discovered_labels(&mut lb, scrape_config, target, &tg.labels);
            let orig = lb.labels();
            let res = scrape::populate_labels(&mut lb, scrape_config, target, &tg.labels);
            let result = match res {
                Ok(l) => SdCheckResult { discovered_labels: orig, labels: l, error: None },
                Err(e) => SdCheckResult { discovered_labels: orig, labels: Labels::default(), error: Some(e.to_string()) },
            };
            if !results.contains(&result) {
                results.push(result);
            }
        }
    }
    results
}
