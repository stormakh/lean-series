//! Port of `storage/remote/write_handler.go`: the remote write 1.0 and 2.0 receiver. It decodes requests
//! (through [`crate::remoteapi::WriteHandler`]) and appends to a `storage::AppendableV2`, with partial writes for
//! 2.0, written-stats headers, and the out-of-order, duplicate, histogram and exemplar rules of upstream.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use discovery::log::Logger;
use discovery::metrics::{CounterVec, MetricVec, Opts, Registry};
use model::exemplar::{self, Exemplar};
use model::histogram::{self as mhist, FloatHistogram, Histogram, EXPONENTIAL_SCHEMA_MAX};
use model::labels::{Labels, ScratchBuilder, ValidationScheme, METRIC_NAME};
use model::metadata::{Metadata, MetricType};
use model::schema::{self, METRIC_TYPE_LABEL, METRIC_UNIT_LABEL};
use storage::{
    AppendPartialError, AppendV2Options, AppendableV2, AppenderV2, Context, Error, ExemplarError, SeriesRef,
    ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP, ERR_OUT_OF_BOUNDS, ERR_OUT_OF_ORDER_EXEMPLAR, ERR_OUT_OF_ORDER_SAMPLE,
    ERR_TOO_OLD_SAMPLE,
};

use crate::prompb::{self, writev2, Message};
use crate::remoteapi::{self, MessageTypes, WriteMessageType, WriteResponse};
use crate::stats::WriteResponseStats;

/// Go `maxAheadTime`: samples and exemplars further in the future are rejected.
pub const MAX_AHEAD_TIME: Duration = Duration::from_secs(10 * 60);

/// Go `writeHandler` (the `writeStorage` of the remote API handler).
pub struct WriteHandlerStore {
    logger: Logger,
    appendable_v2: Arc<dyn AppendableV2>,
    samples_with_invalid_labels_total: Arc<CounterVec>,
    samples_appended_without_metadata: Arc<CounterVec>,
    enable_type_and_unit_labels: bool,
    append_metadata: bool,
}

/// Go `NewWriteHandler`: an HTTP handler that accepts remote write requests of the `accepted_msgs` types and
/// writes them to `appendable_v2`, whose appenders must also be `storage::ExemplarAppenderV2`. With 2.0
/// messages, partial writes are possible (spec "partial write").
pub fn new_write_handler(
    logger: Logger,
    reg: Option<&Registry>,
    appendable_v2: Arc<dyn AppendableV2>,
    accepted_msgs: MessageTypes,
    enable_type_and_unit_labels: bool,
    append_metadata: bool,
) -> remoteapi::WriteHandler {
    let samples_with_invalid_labels_total = MetricVec::new_counter(Opts::new(
        "prometheus_api_remote_write_invalid_labels_samples_total",
        "The total number of received remote write samples and histogram samples which were rejected due to invalid labels.",
    ));
    let samples_appended_without_metadata = MetricVec::new_counter(Opts::new(
        "prometheus_api_remote_write_without_metadata_appended_samples_total",
        "The total number of received remote write samples (and histogram samples) which were ingested without corresponding metadata.",
    ));
    samples_with_invalid_labels_total.get();
    samples_appended_without_metadata.get();
    if let Some(r) = reg {
        r.register(samples_with_invalid_labels_total.clone())
            .expect("register prometheus_api_remote_write_invalid_labels_samples_total");
        r.register(samples_appended_without_metadata.clone())
            .expect("register prometheus_api_remote_write_without_metadata_appended_samples_total");
    }
    let h = WriteHandlerStore {
        logger: logger.clone(),
        appendable_v2,
        samples_with_invalid_labels_total,
        samples_appended_without_metadata,
        enable_type_and_unit_labels,
        append_metadata,
    };
    remoteapi::new_write_handler(Box::new(h), accepted_msgs, logger)
}

/// Go `errors.As(err, &histogram.Error{})`.
pub fn is_histogram_validation_error(err: &Error) -> bool {
    match err {
        Error::Chunk(chunkenc::Error::Histogram(e)) => e.is_histogram_error(),
        Error::Wrapped(w) => is_histogram_validation_error(&w.1),
        Error::Join(v) => v.iter().any(is_histogram_validation_error),
        Error::Other(_) => err.downcast_ref::<mhist::Error>().is_some_and(|e| e.is_histogram_error()),
        _ => false,
    }
}

fn is_append_partial(err: &Error) -> bool {
    err.as_append_partial().is_some()
}

/// The append errors that are the sender's fault (400, no retry).
fn is_out_of_order_like(err: &Error) -> bool {
    err.is(&ERR_OUT_OF_ORDER_SAMPLE)
        || err.is(&ERR_OUT_OF_BOUNDS)
        || err.is(&ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP)
        || err.is(&ERR_TOO_OLD_SAMPLE)
}

/// The series reference of an append result (Go returns it next to the error; a partial error carries it).
fn ref_of(res: &Result<SeriesRef, Error>) -> SeriesRef {
    match res {
        Ok(r) => *r,
        Err(e) => e.as_append_partial().map(|p| p.series_ref).unwrap_or(0),
    }
}

/// Go `%v` of a `[]uint32`.
fn fmt_refs(refs: &[u32]) -> String {
    let parts: Vec<String> = refs.iter().map(|r| r.to_string()).collect();
    format!("[{}]", parts.join(" "))
}

fn now_plus_max_ahead() -> i64 {
    model::timestamp::from_time(SystemTime::now() + MAX_AHEAD_TIME)
}

fn too_far_in_future() -> Error {
    Error::wrapped(format!("{}: timestamp is too far in the future", ERR_OUT_OF_BOUNDS), ERR_OUT_OF_BOUNDS)
}

impl remoteapi::WriteStorage for WriteHandlerStore {
    /// Go `Store`: the body is already decompressed.
    fn store(&self, ctx: &Context, r: &http::Request, msg_type: WriteMessageType) -> (Option<WriteResponse>, Option<Error>) {
        let mut wr = WriteResponse::new();
        if msg_type == WriteMessageType::V1 {
            // Remote write 1.0: another message and no partial writes.
            let req = match prompb::WriteRequest::unmarshal(&r.body) {
                Ok(req) => req,
                Err(e) => {
                    self.logger.error(
                        "Error decoding v1 remote write request",
                        &[("protobuf_message", msg_type.as_str()), ("err", &e.to_string())],
                    );
                    wr.set_status_code(400);
                    return (Some(wr), Some(Error::msg(e.to_string())));
                }
            };
            if let Some(err) = self.write(ctx, &req) {
                if is_out_of_order_like(&err) || is_histogram_validation_error(&err) {
                    // A bad request, so that the sender does not retry.
                    wr.set_status_code(400);
                } else {
                    wr.set_status_code(500);
                }
                return (Some(wr), Some(err));
            }
            return (Some(wr), None);
        }

        // Remote write 2.x.
        let req = match writev2::Request::unmarshal(&r.body) {
            Ok(req) => req,
            Err(e) => {
                self.logger.error(
                    "Error decoding v2 remote write request",
                    &[("protobuf_message", msg_type.as_str()), ("err", &e.to_string())],
                );
                wr.set_status_code(400);
                return (Some(wr), Some(Error::msg(e.to_string())));
            }
        };
        let (resp_stats, err_http_code, err) = self.write_v2(ctx, &req);
        // Stats for the X-Prometheus-Remote-Write-*-Written response headers.
        wr.add(resp_stats);
        if let Some(err) = err {
            wr.set_status_code(err_http_code);
            return (Some(wr), Some(err));
        }
        (Some(wr), None)
    }
}

impl WriteHandlerStore {
    /// Go `write` (remote write 1.0).
    fn write(&self, ctx: &Context, req: &prompb::WriteRequest) -> Option<Error> {
        let mut app_v2 = self.appendable_v2.appender_v2(ctx);
        if app_v2.as_exemplar_appender_v2().is_none() {
            let err = Error::msg(format!(
                "remote write 1.0 receiver requires a storage.ExemplarAppenderV2, got {}",
                app_v2.type_name()
            ));
            let _ = app_v2.rollback();
            return Some(err);
        }
        let mut samples_appended = 0usize;
        let res = {
            let mut app = TimeLimitAppenderV2::new(&mut *app_v2, now_plus_max_ahead());
            self.write_inner(&mut app, req, &mut samples_appended)
        };
        if let Some(e) = res {
            let _ = app_v2.rollback();
            return Some(e);
        }
        let err = app_v2.commit();
        if err.is_some() {
            // Upstream counts here only when the commit fails.
            self.samples_appended_without_metadata.get().add(samples_appended as f64);
        }
        err
    }

    fn write_inner(&self, app: &mut TimeLimitAppenderV2<'_>, req: &prompb::WriteRequest, samples_appended: &mut usize) -> Option<Error> {
        let mut out_of_order_exemplar_errs = 0usize;
        let mut samples_with_invalid_labels = 0usize;
        let mut b = ScratchBuilder::new(0);
        let mut exemplars: Vec<Exemplar> = Vec::with_capacity(1);
        for ts in &req.timeseries {
            let ls = ts.to_labels(&mut b);

            // Even as per 1.0 spec, this should be a 400 error while other samples are written.
            if !ls.has(METRIC_NAME) || !ls.is_valid(ValidationScheme::Utf8) {
                self.logger.warn("Invalid metric names or labels", &[("got", &ls.to_string())]);
                samples_with_invalid_labels += 1;
                continue;
            } else if let Some(dup) = ls.has_duplicate_label_names() {
                self.logger.warn("Invalid labels for series.", &[("labels", &ls.to_string()), ("duplicated_label", dup)]);
                samples_with_invalid_labels += 1;
                continue;
            }

            let r = match self.append_v1_samples(app, &ts.samples, &ls) {
                Ok(r) => r,
                Err(e) => return Some(e),
            };
            *samples_appended += ts.samples.len();

            let r = match self.append_v1_histograms(app, &ts.histograms, &ls, r) {
                Ok(r) => r,
                Err(e) => return Some(e),
            };
            *samples_appended += ts.histograms.len();

            if !ts.exemplars.is_empty() {
                exemplars.clear();
                for ep in &ts.exemplars {
                    exemplars.push(ep.to_exemplar(&mut b));
                }
                // AppendExemplars needs exemplars sorted by timestamp.
                exemplars.sort_by(|a, b| exemplar::compare(a, b).cmp(&0));
                if let Err(err) = app.append_exemplars(r, &ls, &exemplars) {
                    if let Some(p) = err.as_append_partial() {
                        for e_err in &p.exemplar_errors {
                            if e_err.is(&ERR_OUT_OF_ORDER_EXEMPLAR) {
                                out_of_order_exemplar_errs += 1;
                            }
                        }
                    }
                    // Exemplar storage is experimental: ingestion errors do not fail the request.
                    self.logger.debug(
                        "Error while adding exemplars",
                        &[("series", &ls.to_string()), ("exemplars", &format!("{:?}", exemplars)), ("err", &err.to_string())],
                    );
                }
            }
        }
        if out_of_order_exemplar_errs > 0 {
            self.logger.warn(
                "Error on ingesting out-of-order exemplars",
                &[("num_dropped", &out_of_order_exemplar_errs.to_string())],
            );
        }
        if samples_with_invalid_labels > 0 {
            self.samples_with_invalid_labels_total.get().add(samples_with_invalid_labels as f64);
        }
        None
    }

    fn append_v1_samples(&self, app: &mut TimeLimitAppenderV2<'_>, ss: &[prompb::Sample], ls: &Labels) -> Result<SeriesRef, Error> {
        let mut r: SeriesRef = 0;
        for s in ss {
            let res = app.append(r, ls, 0, s.timestamp, s.value, None, None, &AppendV2Options::default());
            r = ref_of(&res);
            if let Err(err) = res {
                if !is_append_partial(&err) {
                    if is_out_of_order_like(&err) {
                        self.logger.error(
                            "Out of order sample from remote write",
                            &[("err", &err.to_string()), ("series", &ls.to_string()), ("timestamp", &s.timestamp.to_string())],
                        );
                    }
                    return Err(err);
                }
            }
        }
        Ok(r)
    }

    fn append_v1_histograms(
        &self,
        app: &mut TimeLimitAppenderV2<'_>,
        hh: &[prompb::Histogram],
        ls: &Labels,
        mut r: SeriesRef,
    ) -> Result<SeriesRef, Error> {
        for hp in hh {
            let res = if hp.is_float_histogram() {
                let fh = hp.to_float_histogram();
                app.append(r, ls, 0, hp.timestamp, 0.0, None, Some(&fh), &AppendV2Options::default())
            } else {
                let h = hp.to_int_histogram().expect("integer histogram");
                app.append(r, ls, 0, hp.timestamp, 0.0, Some(&h), None, &AppendV2Options::default())
            };
            r = ref_of(&res);
            if let Err(err) = res {
                if !is_append_partial(&err) {
                    if is_out_of_order_like(&err) {
                        self.logger.error(
                            "Out of order histogram from remote write",
                            &[("err", &err.to_string()), ("series", &ls.to_string()), ("timestamp", &hp.timestamp.to_string())],
                        );
                    }
                    return Err(err);
                }
            }
        }
        Ok(r)
    }

    /// Go `writeV2`: appends a 2.0 request with partial 4xx writes; returns the stats, and on error the HTTP code
    /// and the error. On a 5xx error everything is rolled back (TSDB appends are not idempotent).
    fn write_v2(&self, ctx: &Context, req: &writev2::Request) -> (WriteResponseStats, u16, Option<Error>) {
        let mut app_v2 = self.appendable_v2.appender_v2(ctx);
        if app_v2.as_exemplar_appender_v2().is_none() {
            let err = Error::msg(format!(
                "remote write 2.x receiver requires a storage.ExemplarAppenderV2, got {}",
                app_v2.type_name()
            ));
            let _ = app_v2.rollback();
            return (WriteResponseStats::default(), 500, Some(err));
        }

        let mut s = WriteResponseStats::default();
        let (samples_without_metadata, err_http_code, err) = {
            let mut app = TimeLimitAppenderV2::new(&mut *app_v2, now_plus_max_ahead());
            self.append_v2(&mut app, req, &mut s)
        };
        if let Some(err) = err {
            if err_http_code / 5 == 100 {
                // On 5xx always roll back: the sender retries and the TSDB is not idempotent.
                if let Some(rerr) = app_v2.rollback() {
                    self.logger.error("writev2 rollback failed on retry-able error", &[("err", &rerr.to_string())]);
                }
                return (WriteResponseStats::default(), err_http_code, Some(err));
            }
            // Not retriable (for example a bad request): may be written partially.
            if let Some(commit_err) = app_v2.commit() {
                // The bad request does not matter: this is an internal (retriable) error.
                return (WriteResponseStats::default(), 500, Some(commit_err));
            }
            // A bad request error happened, but the rest of the data (if any) was written.
            self.samples_appended_without_metadata.get().add(samples_without_metadata as f64);
            return (s, err_http_code, Some(err));
        }

        // All good, commit.
        if let Some(err) = app_v2.commit() {
            return (WriteResponseStats::default(), 500, Some(err));
        }
        self.samples_appended_without_metadata.get().add(samples_without_metadata as f64);
        (s, 0, None)
    }

    fn append_v2(
        &self,
        app: &mut TimeLimitAppenderV2<'_>,
        req: &writev2::Request,
        rs: &mut WriteResponseStats,
    ) -> (i64, u16, Option<Error>) {
        let mut bad_request_errs: Vec<Error> = Vec::new();
        let mut out_of_order_exemplar_errs = 0usize;
        let mut samples_with_invalid_labels = 0usize;
        let mut samples_without_metadata = 0i64;
        let mut b = ScratchBuilder::new(0);
        let mut exemplars: Vec<Exemplar> = Vec::with_capacity(1);

        for ts in &req.timeseries {
            let mut ls = match ts.to_labels(&mut b, &req.symbols) {
                Ok(ls) => ls,
                Err(e) => {
                    bad_request_errs.push(Error::msg(format!("parsing labels for series {}: {}", fmt_refs(&ts.labels_refs), e)));
                    samples_with_invalid_labels += ts.samples.len() + ts.histograms.len();
                    continue;
                }
            };

            let m = match ts.to_metadata(&req.symbols) {
                Ok(m) => m,
                Err(e) => {
                    bad_request_errs.push(Error::msg(format!("parsing metadata for series {}: {}", fmt_refs(&ts.labels_refs), e)));
                    continue;
                }
            };
            if self.enable_type_and_unit_labels && (m.typ != MetricType::Unknown || !m.unit.is_empty()) {
                let mut slb = ScratchBuilder::new(ls.len() + 2); // +2 for __type__ and __unit__.
                for (n, v) in ls.iter() {
                    // Skip __type__ and __unit__ of the incoming labels; they come from the metadata.
                    if n != METRIC_TYPE_LABEL && n != METRIC_UNIT_LABEL {
                        slb.add(n, v);
                    }
                }
                schema::Metadata { name: String::new(), typ: m.typ.as_str().to_string(), unit: m.unit.clone() }
                    .add_to_labels(&mut slb);
                slb.sort();
                ls = slb.labels();
            }

            // Validate the series labels early.
            if !ls.has(METRIC_NAME) || !ls.is_valid(ValidationScheme::Utf8) {
                bad_request_errs.push(Error::msg(format!("invalid metric name or labels, got {}", ls)));
                samples_with_invalid_labels += ts.samples.len() + ts.histograms.len();
                continue;
            } else if let Some(dup) = ls.has_duplicate_label_names() {
                bad_request_errs.push(Error::msg(format!(
                    "invalid labels for series, labels {}, duplicated label {}",
                    ls, dup
                )));
                samples_with_invalid_labels += ts.samples.len() + ts.histograms.len();
                continue;
            }

            // A series must have at least one sample or histogram.
            if ts.samples.is_empty() && ts.histograms.is_empty() {
                bad_request_errs.push(Error::msg(format!(
                    "TimeSeries must contain at least one sample or histogram for series {}",
                    ls
                )));
                continue;
            }

            let all_samples_so_far = rs.all_samples();
            let mut r: SeriesRef = 0;
            // Metadata is attached only with the metadata-wal-records feature (else it is not persisted).
            let md: Option<&Metadata> = if self.append_metadata { Some(&m) } else { None };
            let opts = AppendV2Options { metadata: md, ..Default::default() };
            for s in &ts.samples {
                let res = app.append(r, &ls, s.start_timestamp, s.timestamp, s.value, None, None, &opts);
                r = ref_of(&res);
                let (_, err) = self.handle_v2_partial_err(res.err(), &ls, &mut out_of_order_exemplar_errs, &mut bad_request_errs);
                let Some(err) = err else {
                    rs.samples += 1;
                    continue;
                };
                if is_out_of_order_like(&err) {
                    self.logger.error(
                        "Out of order sample from remote write",
                        &[("err", &err.to_string()), ("series", &ls.to_string()), ("timestamp", &s.timestamp.to_string())],
                    );
                    bad_request_errs.push(Error::wrapped(format!("{} for series {}", err, ls), err));
                    continue;
                }
                return (0, 500, Some(err));
            }

            // Native histograms.
            for hp in &ts.histograms {
                let res = if hp.is_float_histogram() {
                    let fh = hp.to_float_histogram();
                    app.append(r, &ls, hp.start_timestamp, hp.timestamp, 0.0, None, Some(&fh), &opts)
                } else {
                    let h = hp.to_int_histogram().expect("integer histogram");
                    app.append(r, &ls, hp.start_timestamp, hp.timestamp, 0.0, Some(&h), None, &opts)
                };
                r = ref_of(&res);
                let (_, err) = self.handle_v2_partial_err(res.err(), &ls, &mut out_of_order_exemplar_errs, &mut bad_request_errs);
                let Some(err) = err else {
                    rs.histograms += 1;
                    continue;
                };
                if is_out_of_order_like(&err) {
                    self.logger.error(
                        "Out of order histogram from remote write",
                        &[("err", &err.to_string()), ("series", &ls.to_string()), ("timestamp", &hp.timestamp.to_string())],
                    );
                    bad_request_errs.push(Error::wrapped(format!("{} for series {}", err, ls), err));
                    continue;
                }
                if is_histogram_validation_error(&err) {
                    self.logger.error(
                        "Invalid histogram received",
                        &[("err", &err.to_string()), ("series", &ls.to_string()), ("timestamp", &hp.timestamp.to_string())],
                    );
                    bad_request_errs.push(Error::wrapped(format!("{} for series {}", err, ls), err));
                    continue;
                }
                return (0, 500, Some(err));
            }

            if m.is_empty() {
                samples_without_metadata += rs.all_samples() - all_samples_so_far;
            }

            // Exemplars.
            if !ts.exemplars.is_empty() {
                exemplars.clear();
                for ep in &ts.exemplars {
                    match ep.to_exemplar(&mut b, &req.symbols) {
                        Ok(e) => exemplars.push(e),
                        Err(e) => {
                            bad_request_errs.push(Error::msg(format!("parsing exemplar for series {}: {}", ls, e)));
                        }
                    }
                }
                if !exemplars.is_empty() {
                    // AppendExemplars needs exemplars sorted by timestamp.
                    exemplars.sort_by(|a, b| exemplar::compare(a, b).cmp(&0));
                    let res = app.append_exemplars(r, &ls, &exemplars);
                    let (failed, err) =
                        self.handle_v2_partial_err(res.err(), &ls, &mut out_of_order_exemplar_errs, &mut bad_request_errs);
                    if let Some(err) = err {
                        self.logger.error(
                            "failed to ingest exemplars, emitting error log, but no error for PRW caller",
                            &[("err", &err.to_string()), ("series", &ls.to_string())],
                        );
                    } else {
                        rs.exemplars += exemplars.len().saturating_sub(failed) as i64;
                    }
                }
            }
        }

        if out_of_order_exemplar_errs > 0 {
            self.logger.warn(
                "Error on ingesting out-of-order exemplars",
                &[("num_dropped", &out_of_order_exemplar_errs.to_string())],
            );
        }
        self.samples_with_invalid_labels_total.get().add(samples_with_invalid_labels as f64);

        if bad_request_errs.is_empty() {
            return (samples_without_metadata, 0, None);
        }
        (samples_without_metadata, 400, Error::join(bad_request_errs.into_iter().map(Some)))
    }

    /// Go `handleV2PartialErr`: for a partial error, records the exemplar errors and returns the number of
    /// failed exemplars and no error (the sample itself was appended); else returns 0 and the error.
    fn handle_v2_partial_err(
        &self,
        err: Option<Error>,
        ls: &Labels,
        out_of_order_exemplar_errs: &mut usize,
        bad_request_errs: &mut Vec<Error>,
    ) -> (usize, Option<Error>) {
        let Some(err) = err else {
            return (0, None);
        };
        let Some(p) = err.as_append_partial() else {
            return (0, Some(err));
        };
        for e_err in &p.exemplar_errors {
            if e_err.is(&ERR_OUT_OF_ORDER_EXEMPLAR) {
                *out_of_order_exemplar_errs += 1;
                bad_request_errs.push(Error::wrapped(format!("{} for series {}", e_err.err, ls), e_err.err.clone()));
                continue;
            }
            // For now exemplars are dropped with an error log, without rollback and 5xx.
            self.logger.error(
                "failed to ingest exemplar, emitting error log, but no error for PRW caller",
                &[("err", &e_err.err.to_string()), ("series", &ls.to_string())],
            );
        }
        (p.exemplar_errors.len(), None)
    }
}

/// Go `timeLimitExemplarAppenderV2` (with `timeLimitAppenderV2`): rejects samples, histograms and exemplars
/// newer than `max_time`, and reduces the resolution of histograms with a schema above the supported maximum.
/// The wrapped appender must be an `ExemplarAppenderV2`.
pub struct TimeLimitAppenderV2<'a> {
    app: &'a mut dyn AppenderV2,
    pub max_time: i64,
}

impl<'a> TimeLimitAppenderV2<'a> {
    pub fn new(app: &'a mut dyn AppenderV2, max_time: i64) -> TimeLimitAppenderV2<'a> {
        TimeLimitAppenderV2 { app, max_time }
    }

    /// Go `Append`.
    #[allow(clippy::too_many_arguments)]
    pub fn append(
        &mut self,
        r: SeriesRef,
        ls: &Labels,
        st: i64,
        t: i64,
        v: f64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
        opts: &AppendV2Options<'_>,
    ) -> Result<SeriesRef, Error> {
        if t > self.max_time {
            return Err(too_far_in_future());
        }

        let mut h_reduced: Option<Histogram> = None;
        if let Some(h) = h {
            if mhist::is_exponential_schema_reserved(h.schema) && h.schema > EXPONENTIAL_SCHEMA_MAX {
                let mut c = h.clone();
                c.reduce_resolution(EXPONENTIAL_SCHEMA_MAX)?;
                h_reduced = Some(c);
            }
        }
        let mut fh_reduced: Option<FloatHistogram> = None;
        if let Some(fh) = fh {
            if mhist::is_exponential_schema_reserved(fh.schema) && fh.schema > EXPONENTIAL_SCHEMA_MAX {
                let mut c = fh.clone();
                c.reduce_resolution(EXPONENTIAL_SCHEMA_MAX)?;
                fh_reduced = Some(c);
            }
        }
        let h = h_reduced.as_ref().or(h);
        let fh = fh_reduced.as_ref().or(fh);

        let mut partial_err: Option<AppendPartialError> = None;
        let limited;
        let mut opts = *opts;
        if !opts.exemplars.is_empty() {
            let (valid, p) = self.limit_exemplars(opts.exemplars);
            limited = valid;
            partial_err = p;
            opts.exemplars = &limited[..];
        }
        let res = self.app.append(r, ls, st, t, v, h, fh, &opts);
        let (r, err) = match res {
            Ok(r) => (r, None),
            Err(e) => (ref_of(&Err(e.clone())), Some(e)),
        };
        if let Some(err) = AppendPartialError::handle(&mut partial_err, err) {
            return Err(err);
        }
        finish_partial(r, partial_err)
    }

    /// Go `limitExemplars`: the exemplars not too far in the future, and an error for each dropped one (by
    /// index). Exemplars are sorted by timestamp, so the dropped ones come last and the kept ones keep their
    /// indexes.
    pub fn limit_exemplars<'e>(&self, exemplars: &'e [Exemplar]) -> (std::borrow::Cow<'e, [Exemplar]>, Option<AppendPartialError>) {
        let mut future_errs: Vec<ExemplarError> = Vec::new();
        let mut valid: Option<Vec<Exemplar>> = None;
        for (i, e) in exemplars.iter().enumerate() {
            if e.ts > self.max_time {
                if valid.is_none() {
                    let mut v = Vec::with_capacity(exemplars.len().saturating_sub(1));
                    v.extend_from_slice(&exemplars[..i]);
                    valid = Some(v);
                }
                future_errs.push(ExemplarError { index: i, err: too_far_in_future() });
                continue;
            }
            if let Some(v) = valid.as_mut() {
                v.push(e.clone());
            }
        }
        if future_errs.is_empty() {
            return (std::borrow::Cow::Borrowed(exemplars), None);
        }
        (
            std::borrow::Cow::Owned(valid.unwrap_or_default()),
            Some(AppendPartialError { series_ref: 0, exemplar_errors: future_errs }),
        )
    }

    /// Go `AppendExemplars`: rejects exemplars too far in the future, then appends the rest.
    pub fn append_exemplars(&mut self, r: SeriesRef, ls: &Labels, exemplars: &[Exemplar]) -> Result<SeriesRef, Error> {
        let (valid, mut partial_err) = self.limit_exemplars(exemplars);
        if valid.is_empty() {
            return finish_partial(r, partial_err);
        }
        let ex_app = self.app.as_exemplar_appender_v2().expect("an ExemplarAppenderV2");
        let res = ex_app.append_exemplars(r, ls, &valid);
        let (r, err) = match res {
            Ok(r) => (r, None),
            Err(e) => (ref_of(&Err(e.clone())), Some(e)),
        };
        if let Some(err) = AppendPartialError::handle(&mut partial_err, err) {
            return Err(err);
        }
        finish_partial(r, partial_err)
    }
}

/// Go `return ref, partialErr.ToError()`: the reference travels in the partial error.
fn finish_partial(r: SeriesRef, partial_err: Option<AppendPartialError>) -> Result<SeriesRef, Error> {
    match partial_err {
        Some(mut p) if !p.exemplar_errors.is_empty() => {
            p.series_ref = r;
            Err(Error::AppendPartial(Arc::new(p)))
        }
        _ => Ok(r),
    }
}
