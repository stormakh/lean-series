//! End-to-end check: loads a Prometheus config, runs service discovery and the scrape manager into an in-memory
//! appendable, prints per-scrape statistics, and at the end writes the last value of every series.
//!
//! Usage: scrape_e2e <prometheus.yml> <seconds> <out.txt>
//! The output has one line per series: `<labels> <value>` (sorted), the format `scripts` compares with the
//! `/federate` output of real Prometheus.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use model::histogram::{FloatHistogram, Histogram};
use model::labels::Labels;
use storage::{AppendV2Options, AppendableV2, AppenderTransaction, AppenderV2, Context, Error, SeriesRef};

#[derive(Default)]
struct Mem {
    last: Mutex<BTreeMap<String, (i64, f64)>>,
    commits: Mutex<u64>,
}

struct MemAppender {
    mem: Arc<Mem>,
    pending: Vec<(Labels, i64, f64)>,
}

impl AppendableV2 for MemStore {
    fn appender_v2(&self, _ctx: &Context) -> Box<dyn AppenderV2> {
        Box::new(MemAppender { mem: self.0.clone(), pending: Vec::new() })
    }
}

struct MemStore(Arc<Mem>);

impl AppenderTransaction for MemAppender {
    fn commit(&mut self) -> Option<Error> {
        let mut last = self.mem.last.lock().unwrap();
        let mut stats: BTreeMap<String, (String, f64)> = BTreeMap::new();
        for (l, t, v) in self.pending.drain(..) {
            let name = l.get("__name__").to_string();
            if matches!(
                name.as_str(),
                "up" | "scrape_samples_scraped" | "scrape_samples_post_metric_relabeling" | "scrape_series_added" | "scrape_duration_seconds"
            ) {
                stats.insert(name, (l.get("instance").to_string(), v));
            }
            last.insert(l.to_string(), (t, v));
        }
        *self.mem.commits.lock().unwrap() += 1;
        if let Some((inst, up)) = stats.get("up") {
            let g = |k: &str| stats.get(k).map(|x| x.1).unwrap_or(f64::NAN);
            println!(
                "scrape instance={inst} up={up} samples_scraped={} post_relabel={} series_added={} duration={:.4}s series_total={}",
                g("scrape_samples_scraped"),
                g("scrape_samples_post_metric_relabeling"),
                g("scrape_series_added"),
                g("scrape_duration_seconds"),
                last.len()
            );
        }
        None
    }
    fn rollback(&mut self) -> Option<Error> {
        self.pending.clear();
        None
    }
}

impl AppenderV2 for MemAppender {
    fn append(
        &mut self,
        r: SeriesRef,
        ls: &Labels,
        _st: i64,
        t: i64,
        v: f64,
        _h: Option<&Histogram>,
        _fh: Option<&FloatHistogram>,
        _opts: &AppendV2Options<'_>,
    ) -> Result<SeriesRef, Error> {
        self.pending.push((ls.clone(), t, v));
        Ok(if r != 0 { r } else { ls.hash() })
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: scrape_e2e <prometheus.yml> <seconds> <out.txt>");
        std::process::exit(2);
    }
    let secs: u64 = args[2].parse().expect("seconds");
    let logger = discovery::Logger::nop();
    let cfg = config::load_file(&args[1], false, &logger).expect("load config");

    let reg = discovery::Registry::new();
    let sd_metrics = discovery::create_and_register_sd_metrics(&reg).expect("sd metrics");
    let ctx = discovery::CancelToken::new();
    let dm = discovery::Manager::new(&ctx, logger.clone(), &reg, &sd_metrics, discovery::ManagerOptions::default())
        .expect("discovery manager");
    let mut sd: BTreeMap<String, discovery::Configs> = BTreeMap::new();
    for sc in cfg.get_scrape_configs().expect("scrape configs") {
        sd.insert(sc.job_name.clone(), sc.service_discovery_configs.clone());
    }
    dm.apply_config(&sd);
    let dm2 = dm.clone();
    std::thread::spawn(move || {
        let _ = dm2.run();
    });

    let mem = Arc::new(Mem::default());
    let opts = scrape::Options { discovery_reload_interval: Duration::from_secs(5), ..Default::default() };
    let sm = scrape::Manager::new(Some(opts), Some(logger), None, None, Some(Arc::new(MemStore(mem.clone()))), Some(reg.clone()))
        .expect("scrape manager");
    sm.apply_config(&cfg).expect("apply config");
    let sm2 = sm.clone();
    let rx = dm.sync_ch();
    std::thread::spawn(move || {
        let _ = sm2.run(rx);
    });

    std::thread::sleep(Duration::from_secs(secs));
    for (job, ts) in sm.targets_active() {
        for t in ts {
            println!(
                "target job={job} url={} health={} last_error={:?} last_duration={:?}",
                t.url_string(),
                t.health(),
                t.last_error().map(|e| e.to_string()),
                t.last_scrape_duration()
            );
        }
    }
    sm.stop();
    ctx.cancel();
    let last = mem.last.lock().unwrap();
    let mut out = String::new();
    for (l, (_, v)) in last.iter() {
        out.push_str(&format!("{l} {v}\n"));
    }
    std::fs::write(&args[3], out).expect("write output");
    println!("series={} commits={}", last.len(), mem.commits.lock().unwrap());
}
