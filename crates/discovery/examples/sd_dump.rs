//! Runs the SD configs of a file once and prints the discovered targets as JSON, for end-to-end comparisons with
//! Prometheus' `/api/v1/targets?state=any` (`discoveredLabels`).
//!
//! usage: sd_dump <file.yml> [timeout-seconds]
//! The file is a list of `{job_name: <name>, <provider>_sd_configs: [...]}`. Output: one JSON object
//! `{job: [{label: value, ...}, ...]}` with group labels merged into each target, sorted.
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use discovery::{CancelToken, Configs, DiscovererOptions, Recv, Registry};
use yaml::{Decoder, Node, Unmarshal};

#[derive(Default)]
struct Job {
    job_name: String,
    configs: Configs,
}

#[derive(Default)]
struct Plain {
    job_name: String,
}
yaml::yaml_struct!(Plain, "main.job", [
    (job_name: "job_name")
]);

impl Unmarshal for Job {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            let mut p = Plain::default();
            discovery::unmarshal_yaml_with_inline_configs(n, d, &mut p, &mut self.configs)?;
            self.job_name = p.job_name;
            Ok(())
        })
    }
    fn go_type() -> String {
        "main.job".to_string()
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let text = std::fs::read_to_string(&args[1]).expect("read config");
    let timeout = Duration::from_secs(args.get(2).and_then(|s| s.parse().ok()).unwrap_or(20));
    let jobs: Vec<Job> = yaml::unmarshal(&text).unwrap_or_else(|e| panic!("config: {e}"));
    let reg = Registry::new();
    let sd = discovery::create_and_register_sd_metrics(&reg).expect("metrics");
    let logger = discovery::Logger::new(Arc::new(|level, msg: &str, kv: &[(String, String)]| eprintln!("{level:?} {msg} {kv:?}")));
    let mut out = json::Json::obj();
    for job in &jobs {
        let mut targets: Vec<BTreeMap<String, String>> = Vec::new();
        for cfg in job.configs.iter() {
            let opts = DiscovererOptions { logger: logger.clone(), metrics: sd.mechanism_metrics.get(cfg.name()).cloned(), set_name: job.job_name.clone(), ..Default::default() };
            let mut d = match cfg.new_discoverer(opts) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("{}: {e}", job.job_name);
                    continue;
                }
            };
            let ctx = CancelToken::new();
            let (tx, rx) = discovery::channel_with_cancel(&ctx);
            let c2 = ctx.clone();
            let h = std::thread::spawn(move || d.run(&c2, tx));
            if let Recv::Update(groups) = rx.recv_timeout(timeout) {
                for g in groups.iter() {
                    for t in &g.targets {
                        let mut l: BTreeMap<String, String> = g.labels.0.clone();
                        l.extend(t.0.clone());
                        targets.push(l);
                    }
                }
            } else {
                eprintln!("{}: no update", job.job_name);
            }
            ctx.cancel();
            let _ = h.join();
        }
        targets.sort();
        let arr = json::Json::Arr(targets.into_iter().map(|t| json::Json::Obj(t.into_iter().map(|(k, v)| (k, json::Json::Str(v))).collect())).collect());
        out.set(&job.job_name, arr);
    }
    println!("{}", out.stringify_pretty("  "));
}
