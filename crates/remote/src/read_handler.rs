//! Port of `storage/remote/read_handler.go`: the remote read HTTP handler (sampled and streamed responses),
//! with a concurrency gate (port of `util/gate`).
//!
//! Rust note: the HTTP crate has no streaming body, so a streamed (chunked) response is built into one byte
//! body. Go aborts the connection when a stream fails after the first write (`panic(http.ErrAbortHandler)`);
//! here [`ReadHandler::serve`] returns [`AbortHandler`] and the server must close the connection without a
//! response. A client then sees no frames at all, where Go's client may have read some frames first.

use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use discovery::log::Logger;
use discovery::metrics::{GaugeVec, HistogramVec, MetricVec, Opts, Registry};
use model::labels::Labels;
use model::matcher::{MatchType, Matcher};
use storage::{Context, Error, SampleAndChunkQueryable, SelectHints};

use crate::chunked::{ChunkedWriter, NopFlusher};
use crate::codec::{
    as_http_error, decode_read_request, encode_read_response, from_label_matchers, merge_labels,
    negotiate_response_type, stream_chunked_read_responses, to_query_result,
};
use crate::prompb::{self, LabelMatcher, ResponseType};
use crate::remoteapi::{http_error, http_error_into};

/// Go `util/gate.Gate`: limits the number of concurrent holders.
pub struct Gate {
    max: usize,
    used: Mutex<usize>,
    cv: Condvar,
}

/// How often a waiter re-checks its context (the context has no wake-up of its own). Only while waiting.
const GATE_POLL: Duration = Duration::from_millis(5);

impl Gate {
    /// Go `gate.New`.
    pub fn new(max: usize) -> Gate {
        Gate { max, used: Mutex::new(0), cv: Condvar::new() }
    }

    /// Go `Start`: waits for a free slot or the end of the context (then its error).
    pub fn start(&self, ctx: &Context) -> Result<(), Error> {
        let mut used = self.used.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            if *used < self.max {
                *used += 1;
                return Ok(());
            }
            if let Some(e) = ctx.err() {
                return Err(e);
            }
            let mut wait = GATE_POLL;
            if let Some(d) = ctx.deadline() {
                wait = wait.min(d.saturating_duration_since(Instant::now()));
            }
            used = self.cv.wait_timeout(used, wait).unwrap_or_else(|p| p.into_inner()).0;
        }
    }

    /// Go `Done`: releases a slot.
    pub fn done(&self) {
        let mut used = self.used.lock().unwrap_or_else(|p| p.into_inner());
        if *used == 0 {
            panic!("gate.Done: more Dones than Starts");
        }
        *used -= 1;
        self.cv.notify_one();
    }
}

/// Go `http.ErrAbortHandler`: the server must close the connection without sending a response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AbortHandler;

/// Returns the external labels of the current configuration (Go passes `func() config.Config`).
pub type ExternalLabelsFn = Arc<dyn Fn() -> Labels + Send + Sync>;

/// Go `readHandler`.
pub struct ReadHandler {
    logger: Logger,
    queryable: Arc<dyn SampleAndChunkQueryable>,
    external_labels: ExternalLabelsFn,
    remote_read_sample_limit: usize,
    remote_read_max_bytes_in_frame: i64,
    remote_read_gate: Gate,
    queries: Arc<GaugeVec>,
    gate_wait_duration: Arc<HistogramVec>,
}

/// Go `NewReadHandler`. `external_labels` gives `config().GlobalConfig.ExternalLabels`. Metrics are
/// registered in `reg` when given.
pub fn new_read_handler(
    logger: Logger,
    reg: Option<&Registry>,
    queryable: Arc<dyn SampleAndChunkQueryable>,
    external_labels: ExternalLabelsFn,
    remote_read_sample_limit: usize,
    remote_read_concurrency_limit: usize,
    remote_read_max_bytes_in_frame: i64,
) -> ReadHandler {
    let queries = MetricVec::new_gauge(Opts::new(
        "prometheus_remote_read_handler_queries",
        "The current number of remote read queries that are either in execution or queued on the handler.",
    ));
    // Go: a native histogram (factor 1.1); here count and sum without classic buckets.
    let gate_wait_duration = MetricVec::new_histogram_vec(
        Opts::new(
            "prometheus_remote_read_handler_gate_wait_duration_seconds",
            "How long a remote read request spent waiting for the concurrency gate, including requests canceled while waiting.",
        ),
        &[],
        &[],
    );
    if let Some(r) = reg {
        r.register(queries.clone()).expect("register prometheus_remote_read_handler_queries");
        r.register(gate_wait_duration.clone())
            .expect("register prometheus_remote_read_handler_gate_wait_duration_seconds");
    }
    ReadHandler {
        logger,
        queryable,
        external_labels,
        remote_read_sample_limit,
        remote_read_max_bytes_in_frame,
        remote_read_gate: Gate::new(remote_read_concurrency_limit),
        queries,
        gate_wait_duration,
    }
}

fn hints_of(query: &prompb::Query) -> Option<SelectHints> {
    query.hints.as_ref().map(|h| SelectHints {
        start: h.start_ms,
        end: h.end_ms,
        step: h.step_ms,
        func: h.func.clone(),
        grouping: h.grouping.clone(),
        range: h.range_ms,
        by: h.by,
        ..Default::default()
    })
}

/// The status and text of an error response (Go `errors.As(err, &HTTPError{})`, else 500).
fn error_response(err: &Error) -> http::Response {
    match as_http_error(err) {
        Some(h) => http_error(&h.msg, h.status),
        None => http_error(&err.to_string(), 500),
    }
}

impl ReadHandler {
    /// The concurrency gate (for tests).
    pub fn gate(&self) -> &Gate {
        &self.remote_read_gate
    }

    /// Go `ServeHTTP`. `Err(AbortHandler)` means: close the connection without a response.
    pub fn serve(&self, ctx: &Context, r: &http::Request) -> Result<http::Response, AbortHandler> {
        let gate_start = Instant::now();
        let res = self.remote_read_gate.start(ctx);
        // Observe before checking the error, so that requests canceled while waiting count too.
        self.gate_wait_duration.get().observe(gate_start.elapsed().as_secs_f64());
        if let Err(e) = res {
            return Ok(http_error(&e.to_string(), 500));
        }
        self.queries.get().inc();
        let result = self.serve_inner(ctx, r);
        self.queries.get().dec();
        self.remote_read_gate.done();
        result
    }

    fn serve_inner(&self, ctx: &Context, r: &http::Request) -> Result<http::Response, AbortHandler> {
        let req = match decode_read_request(r) {
            Ok(req) => req,
            Err(e) => return Ok(http_error(&e.to_string(), 400)),
        };

        let external_labels = (self.external_labels)();
        // Labels are sorted by name already.
        let sorted_external_labels: Vec<prompb::Label> =
            external_labels.iter().map(|(n, v)| prompb::Label::new(n, v)).collect();

        let response_type = match negotiate_response_type(&req.accepted_response_types) {
            Ok(t) => t,
            Err(e) => return Ok(http_error(&e.to_string(), 400)),
        };

        if response_type == ResponseType::STREAMED_XOR_CHUNKS {
            self.remote_read_streamed_xor_chunks(ctx, &req, &external_labels, &sorted_external_labels)
        } else {
            // Empty or unknown accepted types default to the sampled response.
            Ok(self.remote_read_samples(ctx, &req, &external_labels, &sorted_external_labels))
        }
    }

    fn remote_read_samples(
        &self,
        ctx: &Context,
        req: &prompb::ReadRequest,
        external_labels: &Labels,
        sorted_external_labels: &[prompb::Label],
    ) -> http::Response {
        let mut w = http::Response::new(200);
        w.set_header("Content-Type", "application/x-protobuf");
        w.set_header("Content-Encoding", "snappy");

        let mut resp = prompb::ReadResponse { results: Vec::with_capacity(req.queries.len()) };
        for query in &req.queries {
            let res = (|| -> Result<prompb::QueryResult, Error> {
                let filtered_matchers = filter_ext_labels_from_matchers(&query.matchers, external_labels)?;
                let mut querier = self.queryable.querier(query.start_timestamp_ms, query.end_timestamp_ms)?;
                let hints = hints_of(query);
                let mut ss = querier.select(ctx, false, hints.as_ref(), &filtered_matchers);
                let (mut result, ws, err) = to_query_result(&mut *ss, self.remote_read_sample_limit);
                drop(ss);
                if let Some(e) = querier.close() {
                    self.logger.warn("Error on querier close", &[("err", &e.to_string())]);
                }
                if let Some(e) = err {
                    return Err(e);
                }
                for w in ws.iter() {
                    self.logger.warn("Warnings on remote read query", &[("err", &w.to_string())]);
                }
                if !sorted_external_labels.is_empty() {
                    for ts in &mut result.timeseries {
                        ts.labels = merge_labels(&ts.labels, sorted_external_labels);
                    }
                }
                Ok(result)
            })();
            match res {
                Ok(r) => resp.results.push(r),
                Err(e) => {
                    let code = as_http_error(&e).map(|h| h.status).unwrap_or(500);
                    let msg = as_http_error(&e).map(|h| h.msg.clone()).unwrap_or_else(|| e.to_string());
                    http_error_into(&mut w, &msg, code);
                    return w;
                }
            }
        }
        w.body = http::Body::Bytes(encode_read_response(&resp));
        w
    }

    fn remote_read_streamed_xor_chunks(
        &self,
        ctx: &Context,
        req: &prompb::ReadRequest,
        external_labels: &Labels,
        sorted_external_labels: &[prompb::Label],
    ) -> Result<http::Response, AbortHandler> {
        let mut w = http::Response::new(200);
        w.set_header("Content-Type", "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse");

        let mut cw = ChunkedWriter::new(Vec::new(), NopFlusher);
        for (i, query) in req.queries.iter().enumerate() {
            let res = (|| -> Result<(), Error> {
                let filtered_matchers = filter_ext_labels_from_matchers(&query.matchers, external_labels)?;
                let mut querier = self.queryable.chunk_querier(query.start_timestamp_ms, query.end_timestamp_ms)?;
                let hints = hints_of(query);
                // The streaming API has to provide the series sorted.
                let mut ss = querier.select(ctx, true, hints.as_ref(), &filtered_matchers);
                let (ws, err) = stream_chunked_read_responses(
                    &mut cw,
                    i as i64,
                    &mut *ss,
                    sorted_external_labels,
                    self.remote_read_max_bytes_in_frame,
                );
                drop(ss);
                if let Some(e) = querier.close() {
                    self.logger.warn("Error on chunk querier close", &[("err", &e.to_string())]);
                }
                if let Some(e) = err {
                    return Err(e);
                }
                for w in ws.iter() {
                    self.logger.warn("Warnings on chunked remote read query", &[("warnings", &w.to_string())]);
                }
                Ok(())
            })();
            if let Err(e) = res {
                if !cw.get_ref().is_empty() {
                    // Go: the stream started; returning normally would make it look successful.
                    self.logger.error(
                        "Error streaming remote read response",
                        &[("err", &e.to_string()), ("query_index", &i.to_string())],
                    );
                    return Err(AbortHandler);
                }
                let mut resp = error_response(&e);
                // Go keeps the headers set before http.Error (here only Content-Type, which it replaces).
                resp.set_header("X-Content-Type-Options", "nosniff");
                return Ok(resp);
            }
        }
        cw.close();
        w.body = http::Body::Bytes(cw.into_inner().0);
        Ok(w)
    }
}

/// Go `filterExtLabelsFromMatchers`: equality matchers on an external label with its value become matchers
/// for an empty label, as the label is not in the storage.
pub fn filter_ext_labels_from_matchers(pb_matchers: &[LabelMatcher], external_labels: &Labels) -> Result<Vec<Matcher>, Error> {
    let matchers = from_label_matchers(pb_matchers)?;
    let mut filtered = Vec::with_capacity(matchers.len());
    for m in matchers {
        let value = external_labels.get(&m.name);
        if m.typ == MatchType::Equal && value == m.value {
            filtered.push(Matcher::new(MatchType::Equal, &m.name, "").map_err(|e| Error::msg(e.to_string()))?);
        } else {
            filtered.push(m);
        }
    }
    Ok(filtered)
}
