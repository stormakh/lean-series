//! End-to-end helper for `scripts/e2e/remote/run.sh` (not a library API).
//!
//! `rw_e2e receive <addr> <out-dir>`: a remote write receiver built on `remote::write_handler`. Real Prometheus
//! sends to `http://<addr>/v1` (remote write 1.0) and `/v2` (2.0). Each committed sample is kept in memory; when
//! the file `<out-dir>/stop` appears, the samples are written to `<out-dir>/v1.jsonl` and `v2.jsonl` (one JSON
//! object per sample: metric labels, timestamp, value as Go `FormatFloat(v, 'f', -1)` or histogram count/sum)
//! and the process exits.
//!
//! `rw_e2e send ...` (see `send` below): drives the queue manager against a real Prometheus receiver.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use model::histogram::{FloatHistogram, Histogram};
use model::labels::Labels;
use remote::remoteapi::{MessageTypes, WriteMessageType};
use storage::{
    AppendV2Options, AppendableV2, AppenderTransaction, AppenderV2, Context, Error, ExemplarAppenderV2, SeriesRef,
};

/// A thread-safe recording `AppendableV2` (remote write shards send concurrently).
#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<String>>>);

struct RecAppender {
    out: Arc<Mutex<Vec<String>>>,
    pending: Vec<String>,
}

fn json_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Go `strconv.FormatFloat(v, 'f', -1, 64)` as the Prometheus API prints it.
fn go_float(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v == f64::INFINITY {
        "+Inf".into()
    } else if v == f64::NEG_INFINITY {
        "-Inf".into()
    } else {
        format!("{v}")
    }
}

fn line(ls: &Labels, st: i64, t: i64, v: f64, h: Option<&Histogram>, fh: Option<&FloatHistogram>) -> String {
    let mut s = String::from("{\"metric\":{");
    for (i, l) in ls.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        json_str(&mut s, l.0);
        s.push(':');
        json_str(&mut s, l.1);
    }
    let _ = write!(s, "}},\"st\":{st},\"t\":{t},");
    if let Some(fh) = fh {
        let _ = write!(s, "\"hcount\":\"{}\",\"hsum\":\"{}\"", go_float(fh.count), go_float(fh.sum));
    } else if let Some(h) = h {
        let _ = write!(s, "\"hcount\":\"{}\",\"hsum\":\"{}\"", h.count, go_float(h.sum));
    } else {
        let stale = model::value::is_stale_nan(v);
        let _ = write!(s, "\"v\":\"{}\",\"stale\":{stale}", go_float(v));
    }
    s.push('}');
    s
}

impl AppenderTransaction for RecAppender {
    fn commit(&mut self) -> Option<Error> {
        self.out.lock().unwrap().append(&mut self.pending);
        None
    }
    fn rollback(&mut self) -> Option<Error> {
        self.pending.clear();
        None
    }
}

impl AppenderV2 for RecAppender {
    fn append(
        &mut self,
        r: SeriesRef,
        ls: &Labels,
        st: i64,
        t: i64,
        v: f64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
        _opts: &AppendV2Options<'_>,
    ) -> Result<SeriesRef, Error> {
        self.pending.push(line(ls, st, t, v, h, fh));
        Ok(if r == 0 { ls.hash() } else { r })
    }
    fn as_exemplar_appender_v2(&mut self) -> Option<&mut dyn ExemplarAppenderV2> {
        Some(self)
    }
}

impl ExemplarAppenderV2 for RecAppender {
    fn append_exemplars(&mut self, r: SeriesRef, _ls: &Labels, _e: &[model::exemplar::Exemplar]) -> Result<SeriesRef, Error> {
        Ok(r)
    }
}

impl AppendableV2 for Recorder {
    fn appender_v2(&self, _ctx: &Context) -> Box<dyn AppenderV2> {
        Box::new(RecAppender { out: self.0.clone(), pending: Vec::new() })
    }
}

struct Svc {
    v1: remote::remoteapi::WriteHandler,
    v2: remote::remoteapi::WriteHandler,
}

impl http::server::Service for Svc {
    fn handle(&self, req: http::Request) -> http::Response {
        let ctx = Context::background();
        match req.path.as_str() {
            "/v1" => self.v1.serve(&ctx, req),
            "/v2" => self.v2.serve(&ctx, req),
            _ => http::Response::new(404),
        }
    }
}

fn receive(addr: &str, out: &Path) {
    let r1 = Recorder::default();
    let r2 = Recorder::default();
    let all = MessageTypes(vec![WriteMessageType::V1, WriteMessageType::V2]);
    let log = discovery::log::Logger::nop();
    let svc = Svc {
        v1: remote::write_handler::new_write_handler(log.clone(), None, Arc::new(r1.clone()), all.clone(), false, false),
        v2: remote::write_handler::new_write_handler(log, None, Arc::new(r2.clone()), all, false, false),
    };
    let srv = http::server::Server::bind(addr, http::server::ServerConfig::default()).expect("bind");
    std::thread::spawn(move || srv.run(Arc::new(svc)));
    println!("receiving on {addr}");
    let stop = out.join("stop");
    while !stop.exists() {
        std::thread::sleep(Duration::from_millis(200));
    }
    for (name, r) in [("v1.jsonl", &r1), ("v2.jsonl", &r2)] {
        let lines = r.0.lock().unwrap();
        let mut s = lines.join("\n");
        s.push('\n');
        std::fs::write(out.join(name), s).unwrap();
        println!("{name}: {} samples", lines.len());
    }
}

/// `rw_e2e send <v1|v2> <url> <data-dir> <sent.jsonl>`: opens a real TSDB in `<data-dir>`, starts a
/// `WriteStorage` (one queue manager with a WAL watcher on `<data-dir>/wal`) that sends to `<url>`, then appends
/// float and histogram samples through the TSDB (so they reach the WAL), waits, and closes the write storage
/// (which flushes the queue). Every appended sample is written to `<sent.jsonl>`.
fn send(proto: &str, url: &str, dir: &Path, sent: &Path) {
    let msg = if proto == "v2" { "io.prometheus.write.v2.Request" } else { "prometheus.WriteRequest" };
    let yaml = format!(
        "remote_write:\n  - url: {url}\n    name: e2e\n    protobuf_message: {msg}\n    send_native_histograms: true\n    queue_config: {{batch_send_deadline: 1s, max_samples_per_send: 100}}\n"
    );
    let cfg = config::load(&yaml, &discovery::log::Logger::nop()).expect("config");
    std::fs::create_dir_all(dir).unwrap();
    let db = tsdb::db::open(dir, None, Some(tsdb::db::prometheus_options(15 * 24 * 3600 * 1000, true)), None).expect("open tsdb");
    let ws = remote::write::WriteStorage::new(None, None, dir, Duration::from_secs(30), None, false, None);
    ws.apply_config(&cfg).expect("apply_config");
    // The watcher only sends samples newer than its start time.
    std::thread::sleep(Duration::from_millis(500));

    let start = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
    let mut lines = Vec::new();
    let opts = AppendV2Options::default();
    let fh = FloatHistogram {
        schema: 1,
        count: 10.5,
        sum: 3.25,
        zero_threshold: 0.001,
        zero_count: 0.5,
        positive_spans: vec![model::histogram::Span::new(0, 2), model::histogram::Span::new(3, 1)],
        positive_buckets: vec![4.0, 5.0, 1.0],
        ..Default::default()
    };
    for round in 0..20i64 {
        let t = start + round * 100;
        let mut app = db.appender_v2(&Context::background());
        for s in 0..50 {
            let ls = model::labels::from_strings(&[
                "__name__",
                "e2e_metric",
                "series",
                &s.to_string(),
                "kind",
                if s % 2 == 0 { "even" } else { "odd" },
            ]);
            let v = (round * 1000 + s) as f64 / 8.0 - 3.0;
            app.append(0, &ls, 0, t, v, None, None, &opts).expect("append");
            lines.push(line(&ls, 0, t, v, None, None));
        }
        // An integer and a float native histogram series.
        let h = Histogram {
            schema: 0,
            count: 8 + round as u64,
            sum: 12.5 + round as f64,
            zero_threshold: 0.001,
            zero_count: 1,
            positive_spans: vec![model::histogram::Span::new(0, 2)],
            positive_buckets: vec![2, 3 + round],
            ..Default::default()
        };
        let lh = model::labels::from_strings(&["__name__", "e2e_hist"]);
        app.append(0, &lh, 0, t, 0.0, Some(&h), None, &opts).expect("append h");
        lines.push(line(&lh, 0, t, 0.0, Some(&h), None));
        let lf = model::labels::from_strings(&["__name__", "e2e_float_hist"]);
        app.append(0, &lf, 0, t, 0.0, None, Some(&fh), &opts).expect("append fh");
        lines.push(line(&lf, 0, t, 0.0, None, Some(&fh)));
        if let Some(e) = app.commit() {
            panic!("commit: {e}");
        }
        ws.notify();
    }
    std::thread::sleep(Duration::from_secs(4));
    ws.close().expect("close write storage");
    db.close().expect("close tsdb");
    let mut s = lines.join("\n");
    s.push('\n');
    std::fs::write(sent, s).unwrap();
    println!("sent {} samples with remote write {proto}", lines.len());
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("receive") => receive(&args[2], &PathBuf::from(&args[3])),
        Some("send") => send(&args[2], &args[3], &PathBuf::from(&args[4]), &PathBuf::from(&args[5])),
        _ => {
            eprintln!("usage: rw_e2e receive <addr> <out-dir> | send <v1|v2> <url> <data-dir> <sent.jsonl>");
            std::process::exit(2);
        }
    }
}
