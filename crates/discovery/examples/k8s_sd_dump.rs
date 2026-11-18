//! Runs Kubernetes SD configs for a while and prints every target group update as one JSON line:
//! `{"cfg": <index>, "source": "...", "targets": [<target labels merged with group labels>, ...]}`.
//!
//! usage: k8s_sd_dump <file with a YAML list of kubernetes_sd_configs entries> <seconds>
//! Used for the end-to-end comparison with Prometheus against a fake API server.
use std::sync::Arc;
use std::time::Duration;

use discovery::kubernetes::SdConfig;
use discovery::{CancelToken, Config, DiscovererOptions, Recv, Registry, channel};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let text = std::fs::read_to_string(&args[1]).expect("read config");
    let secs: u64 = args[2].parse().expect("seconds");
    let cfgs: Vec<SdConfig> = yaml::unmarshal_strict(&text).unwrap_or_else(|e| panic!("config: {e}"));
    let ctx = CancelToken::new();
    let (tx, rx) = channel();
    let mut threads = Vec::new();
    for (i, c) in cfgs.into_iter().enumerate() {
        let reg = Registry::new();
        let m = c.new_discoverer_metrics(&reg, discovery::new_refresh_metrics(&reg));
        m.register().unwrap();
        let logger = discovery::Logger::new(Arc::new(move |lvl: discovery::log::Level, msg: &str, kv: &[(String, String)]| eprintln!("cfg={i} level={} msg={msg:?} {kv:?}", lvl.as_str())));
        let mut d = c.new_discoverer(DiscovererOptions { metrics: Some(m), logger, ..Default::default() }).unwrap_or_else(|e| panic!("discoverer: {e}"));
        let (ctx, tx) = (ctx.clone(), tx.clone());
        // tag the updates with the config index through a relay channel
        let (itx, irx) = channel();
        threads.push(std::thread::spawn(move || d.run(&ctx, itx)));
        threads.push(std::thread::spawn(move || {
            while let Recv::Update(u) = irx.recv() {
                let tagged = u.into_iter().map(|g| Arc::new(discovery::Group { source: format!("{i}\u{0}{}", g.source), ..(*g).clone() })).collect();
                if !tx.send(tagged) {
                    return;
                }
            }
        }));
    }
    drop(tx);
    let c2 = ctx.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(secs));
        c2.cancel();
    });
    loop {
        match rx.recv_timeout(Duration::from_millis(200)) {
            Recv::Update(u) => {
                for g in u {
                    let (cfg, source) = g.source.split_once('\u{0}').unwrap();
                    let targets: Vec<json::Json> = g.targets.iter().map(|t| g.labels.merge(t).to_json()).collect();
                    let line = json::Json::obj().with("cfg", cfg.parse::<i64>().unwrap()).with("source", source).with("targets", json::Json::Arr(targets));
                    println!("{}", line.stringify());
                }
            }
            Recv::Timeout => {
                if ctx.is_cancelled() {
                    break;
                }
            }
            Recv::Closed | Recv::Cancelled => break,
        }
    }
    ctx.cancel();
    drop(rx);
    for t in threads {
        let _ = t.join();
    }
}
