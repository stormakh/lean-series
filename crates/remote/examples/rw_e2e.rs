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

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("receive") => receive(&args[2], &PathBuf::from(&args[3])),
        _ => {
            eprintln!("usage: rw_e2e receive <addr> <out-dir>");
            std::process::exit(2);
        }
    }
}
