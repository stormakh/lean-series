//! Port of `notifier/sendloop.go`: one queue and one sending thread per Alertmanager.

use std::sync::{Arc, Condvar, Mutex, MutexGuard, Once, OnceLock};
use std::time::{Duration, Instant};

use config::{AlertmanagerConfig, ALERTMANAGER_API_VERSION_V2, SUPPORTED_ALERTMANAGER_API_VERSIONS};
use discovery::Logger;

use crate::alert::Alert;
use crate::manager::{Options, CONTENT_TYPE_JSON, USER_AGENT};
use crate::metric::AlertMetrics;
use crate::util::{alerts_to_open_api_alerts, marshal_postable_alerts};

/// Go `context.Context` as the send path uses it: an optional deadline.
#[derive(Clone, Copy, Debug)]
pub struct Context {
    deadline: Option<Instant>,
}

impl Context {
    /// Go `context.Background()`.
    pub fn background() -> Context {
        Context { deadline: None }
    }
    /// Go `context.WithTimeout(context.Background(), d)`.
    pub fn with_timeout(d: Duration) -> Context {
        Context { deadline: Instant::now().checked_add(d) }
    }
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
    /// Time left before the deadline (None: no deadline).
    pub fn remaining(&self) -> Option<Duration> {
        self.deadline.map(|d| d.saturating_duration_since(Instant::now()))
    }
    /// Blocks until the context is done (Go `<-ctx.Done()`); a context without deadline blocks forever.
    pub fn wait_done(&self) {
        loop {
            match self.remaining() {
                Some(r) if r.is_zero() => return,
                Some(r) => std::thread::sleep(r),
                None => std::thread::park(),
            }
        }
    }
    /// Go `ctx.Err()`: "context deadline exceeded" once the deadline passed.
    pub fn err(&self) -> Option<net::Error> {
        match self.remaining() {
            Some(r) if r.is_zero() => Some(net::Error::timeout()),
            _ => None,
        }
    }
}

/// Go `Options.Do`: sends one HTTP request. `client` is None when the set has no client (Go nil `*http.Client`).
pub type DoFn = Arc<dyn Fn(&Context, Option<&net::Client>, net::Request) -> Result<net::Response, net::Error> + Send + Sync>;

/// Go `http.DefaultClient`.
pub fn default_client() -> &'static net::Client {
    static C: OnceLock<net::Client> = OnceLock::new();
    C.get_or_init(|| {
        let cfg = net::HttpClientConfig::default_config();
        match net::new_client_from_config(&cfg, &net::HttpClientOptions::default()) {
            Ok(c) => c,
            Err(e) => panic!("default HTTP client: {e}"),
        }
    })
}

/// Go `do`: `client.Do(req.WithContext(ctx))`, with the default client when `client` is None.
pub fn do_request(ctx: &Context, client: Option<&net::Client>, mut req: net::Request) -> Result<net::Response, net::Error> {
    let c = client.unwrap_or_else(|| default_client());
    if let Some(r) = ctx.remaining() {
        if r.is_zero() {
            return Err(net::Error::timeout());
        }
        req.timeout = Some(r);
    }
    c.do_request(req)
}

#[derive(Default)]
struct Sig {
    has_work: bool,
    stopped: bool,
}

/// Go `sendLoop`.
pub struct SendLoop {
    pub alertmanager_url: String,
    cfg: Option<Arc<AlertmanagerConfig>>,
    client: Option<net::Client>,
    opts: Arc<Options>,
    pub metrics: Arc<AlertMetrics>,
    queue: Mutex<Vec<Arc<Alert>>>,
    sig: Mutex<Sig>,
    cv: Condvar,
    stop_once: Once,
    logger: Logger,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl SendLoop {
    /// Go `newSendLoop`: also initializes the per-Alertmanager series of the metrics.
    pub fn new(
        alertmanager_url: &str,
        client: Option<net::Client>,
        cfg: Option<Arc<AlertmanagerConfig>>,
        opts: Arc<Options>,
        logger: Logger,
        metrics: Arc<AlertMetrics>,
    ) -> Arc<SendLoop> {
        let l = &[alertmanager_url];
        metrics.dropped.with_label_values(l);
        metrics.errors.with_label_values(l);
        metrics.sent.with_label_values(l);
        metrics.queue_length.with_label_values(l);
        let cap = opts.queue_capacity;
        Arc::new(SendLoop {
            alertmanager_url: alertmanager_url.to_string(),
            cfg,
            client,
            opts,
            metrics,
            queue: Mutex::new(Vec::with_capacity(cap.min(1 << 16))),
            sig: Mutex::new(Sig::default()),
            cv: Condvar::new(),
            stop_once: Once::new(),
            logger,
        })
    }

    /// Starts the sending thread (Go `go sendLoop.loop()`).
    pub fn start(self: &Arc<Self>) {
        let s = self.clone();
        let spawned = std::thread::Builder::new().name("notifier-send".to_string()).stack_size(256 * 1024).spawn(move || s.run_loop());
        if let Err(e) = spawned {
            self.logger.error("Starting send loop failed", &[("err", &e.to_string())]);
        }
    }

    /// The queued alerts, oldest first.
    pub fn queue(&self) -> Vec<Arc<Alert>> {
        lock(&self.queue).clone()
    }

    /// Direct access to the queue (tests fill it like Go's `sendLoop.queue = append(...)`).
    pub fn queue_mut(&self) -> MutexGuard<'_, Vec<Arc<Alert>>> {
        lock(&self.queue)
    }

    /// Go `stopped` channel closed.
    pub fn is_stopped(&self) -> bool {
        lock(&self.sig).stopped
    }

    /// Go `add`: queues alerts, dropping the oldest ones when the queue is full.
    pub fn add(&self, alerts: &[Arc<Alert>]) {
        if self.is_stopped() {
            return;
        }
        let mut q = lock(&self.queue);
        let cap = self.opts.queue_capacity;
        let mut dropped = 0usize;
        let mut alerts = alerts;
        // Queue capacity should be significantly larger than a single alert batch could be.
        if alerts.len() > cap {
            let d = alerts.len() - cap;
            self.logger.warn("Alert batch larger than queue capacity, dropping alerts", &[("count", &d.to_string())]);
            dropped += d;
            alerts = &alerts[d..];
        }
        // If the queue is full, remove the oldest alerts in favor of newer ones.
        if q.len() + alerts.len() > cap {
            let d = q.len() + alerts.len() - cap;
            self.logger.warn("Alert notification queue full, dropping alerts", &[("count", &d.to_string())]);
            dropped += d;
            q.drain(..d);
        }
        q.extend_from_slice(alerts);

        // Notify the sending thread that there are alerts to be processed.
        self.notify_work();

        let url = &[self.alertmanager_url.as_str()];
        self.metrics.queue_length.with_label_values(url).set(q.len() as f64);
        if dropped > 0 {
            self.metrics.dropped.with_label_values(url).add(dropped as f64);
        }
    }

    fn notify_work(&self) {
        let mut s = lock(&self.sig);
        if s.stopped {
            return;
        }
        s.has_work = true;
        self.cv.notify_one();
    }

    /// Go `stop`: drains the queue when `drain_on_shutdown` is set (otherwise counts the queued alerts as
    /// dropped) and deletes the per-Alertmanager metric series. Safe to call more than once.
    pub fn stop(&self) {
        self.stop_once.call_once(|| {
            self.logger.debug("Stopping send loop", &[]);
            {
                let mut s = lock(&self.sig);
                s.stopped = true;
                self.cv.notify_all();
            }
            let url = &[self.alertmanager_url.as_str()];
            if self.opts.drain_on_shutdown {
                self.drain_queue();
            } else {
                let ql = self.queue_len();
                self.logger.warn("Alert notification queue not drained on shutdown, dropping alerts", &[("count", &ql.to_string())]);
                self.metrics.dropped.with_label_values(url).add(ql as f64);
            }
            self.metrics.latency_summary.delete_label_values(url);
            self.metrics.latency_histogram.delete_label_values(url);
            self.metrics.sent.delete_label_values(url);
            self.metrics.dropped.delete_label_values(url);
            self.metrics.errors.delete_label_values(url);
            self.metrics.queue_length.delete_label_values(url);
        });
    }

    fn drain_queue(&self) {
        while self.queue_len() > 0 {
            self.send_one_batch();
        }
    }

    pub fn queue_len(&self) -> usize {
        lock(&self.queue).len()
    }

    /// Go `nextBatch`: takes up to `max_batch_size` alerts from the front of the queue.
    pub fn next_batch(&self) -> Vec<Arc<Alert>> {
        let mut q = lock(&self.queue);
        let max = self.opts.max_batch_size;
        let alerts: Vec<Arc<Alert>> = if q.len() > max { q.drain(..max).collect() } else { std::mem::take(&mut *q) };
        self.metrics.queue_length.with_label_values(&[self.alertmanager_url.as_str()]).set(q.len() as f64);
        alerts
    }

    fn send_one_batch(&self) {
        let alerts = self.next_batch();
        if !self.send_all(&alerts) {
            self.metrics.dropped.with_label_values(&[self.alertmanager_url.as_str()]).add(alerts.len() as f64);
        }
    }

    /// Go `loop`: sends batches while there is work, until stopped.
    pub fn run_loop(&self) {
        self.logger.debug("Starting send loop", &[]);
        loop {
            {
                let mut s = lock(&self.sig);
                while !s.has_work && !s.stopped {
                    s = self.cv.wait(s).unwrap_or_else(|e| e.into_inner());
                }
                // If we've been asked to stop, that takes priority over sending any further notifications.
                if s.stopped {
                    return;
                }
                s.has_work = false;
            }
            self.send_one_batch();
            // If the queue still has items left, kick off the next iteration.
            if self.queue_len() > 0 {
                self.notify_work();
            }
        }
    }

    /// Go `sendAll`: true when the batch was sent (or empty).
    pub fn send_all(&self, alerts: &[Arc<Alert>]) -> bool {
        if alerts.is_empty() {
            return true;
        }
        let begin = Instant::now();
        let cfg = match &self.cfg {
            Some(c) => c,
            None => return false,
        };
        let payload = if cfg.api_version.0 == ALERTMANAGER_API_VERSION_V2 {
            marshal_postable_alerts(&alerts_to_open_api_alerts(alerts))
        } else {
            let msg = format!("Invalid Alertmanager API version '{}', expected one of '[{}]'", cfg.api_version.0, SUPPORTED_ALERTMANAGER_API_VERSIONS.join(" "));
            self.logger.error(&msg, &[("err", "<nil>")]);
            return false;
        };

        let ctx = Context::with_timeout(Duration::from_nanos(cfg.timeout.0.max(0) as u64));
        let url = &[self.alertmanager_url.as_str()];
        if let Err(e) = self.send_one(&ctx, self.client.as_ref(), &self.alertmanager_url, payload.into_bytes()) {
            self.logger.error("Error sending alerts", &[("count", &alerts.len().to_string()), ("err", &e)]);
            self.metrics.errors.with_label_values(url).add(alerts.len() as f64);
            return false;
        }
        let secs = begin.elapsed().as_secs_f64();
        self.metrics.latency_summary.with_label_values(url).observe(secs);
        self.metrics.latency_histogram.with_label_values(url).observe(secs);
        self.metrics.sent.with_label_values(url).add(alerts.len() as f64);
        true
    }

    /// Go `sendOne`: POSTs the body; any 2xx status is OK.
    pub fn send_one(&self, ctx: &Context, client: Option<&net::Client>, url: &str, body: Vec<u8>) -> Result<(), String> {
        let mut req = net::Request::new("POST", url, body).map_err(|e| e.msg)?;
        req.header.set("User-Agent", USER_AGENT);
        req.header.set("Content-Type", CONTENT_TYPE_JSON);
        let resp = match &self.opts.do_fn {
            Some(f) => f(ctx, client, req),
            None => do_request(ctx, client, req),
        }
        .map_err(|e| e.msg)?;
        if resp.status / 100 != 2 {
            return Err(format!("bad response status {}", resp.status_text));
        }
        Ok(())
    }
}
