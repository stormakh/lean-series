//! Port of `storage/remote/write_otlp_handler.go` and of `DecodeOTLPWriteRequest` from `storage/remote/codec.go`:
//! the OTLP/HTTP metrics receiver (`/api/v1/otlp/v1/metrics`).
//!
//! The handler decodes an OTLP export request (protobuf or JSON, optionally gzip), optionally converts delta
//! temporality to cumulative, translates the metrics with `otlptranslator::prometheusremotewrite` and appends
//! them to a `storage::AppendableV2` in one transaction.

use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use discovery::log::Logger;
use discovery::metrics::{CounterVec, Opts, Registry};
use model::exemplar::Exemplar;
use model::histogram::{EXPONENTIAL_SCHEMA_MAX, FloatHistogram, Histogram, is_exponential_schema_reserved};
use model::labels::Labels;
use storage::{
    AppendPartialError, AppendV2Options, AppendableV2, AppenderTransaction, AppenderV2, Context, ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP,
    ERR_OUT_OF_BOUNDS, ERR_OUT_OF_ORDER_EXEMPLAR, ERR_OUT_OF_ORDER_SAMPLE, ERR_TOO_OLD_SAMPLE, Error, ExemplarError,
    SeriesRef,
};

use crate::otlptranslator::deltatocumulative::DeltaToCumulative;
use crate::otlptranslator::namer;
use crate::otlptranslator::pdata::{AggregationTemporality, MetricData, Metrics};
use crate::otlptranslator::pdata_json::unmarshal_export_request_json;
use crate::otlptranslator::pdata_proto::unmarshal_export_request;
use crate::otlptranslator::prometheusremotewrite::{
    PrometheusConverter, Settings, count_warnings_by_category, new_promote_resource_attributes,
};

/// Go `decodeReadLimit`: the maximum size of a (decompressed) request body.
pub const DECODE_READ_LIMIT: usize = 32 * 1024 * 1024;
/// Go `pbContentType`.
pub const PB_CONTENT_TYPE: &str = "application/x-protobuf";
/// Go `jsonContentType`.
pub const JSON_CONTENT_TYPE: &str = "application/json";
/// Go `maxAheadTime`: samples further in the future are rejected.
pub const MAX_AHEAD_TIME: Duration = Duration::from_secs(10 * 60);

/// Go `DecodeOTLPWriteRequest`: decodes the body by `Content-Type` (`application/x-protobuf` or
/// `application/json`) and `Content-Encoding` (`gzip` or none). At most `DECODE_READ_LIMIT` decompressed bytes
/// are read; a longer body is truncated and then fails to decode.
pub fn decode_otlp_write_request(req: &http::Request) -> Result<Metrics, String> {
    let content_type = req.header("Content-Type").unwrap_or("");
    let is_json = match content_type {
        PB_CONTENT_TYPE => false,
        JSON_CONTENT_TYPE => true,
        _ => {
            return Err(format!(
                "unsupported content type: {content_type}, supported: [{JSON_CONTENT_TYPE}, {PB_CONTENT_TYPE}]"
            ));
        }
    };
    let encoding = req.header("Content-Encoding").unwrap_or("");
    let decompressed;
    let body: &[u8] = match encoding {
        "gzip" => {
            decompressed = gunzip_limited(&req.body, DECODE_READ_LIMIT)?;
            &decompressed
        }
        "" => &req.body[..req.body.len().min(DECODE_READ_LIMIT)],
        _ => {
            return Err(format!("unsupported compression: {encoding}. Only \"gzip\" or no compression supported"));
        }
    };
    if is_json {
        unmarshal_export_request_json(body)
    } else {
        unmarshal_export_request(body).map_err(|e| e.to_string())
    }
}

/// Go `gzip.NewReader` and `io.ReadAll(io.LimitReader(gr, limit))`: at most `limit` decompressed bytes.
/// Concatenated gzip members are read as one stream, as Go does.
fn gunzip_limited(body: &[u8], limit: usize) -> Result<Vec<u8>, String> {
    if body.is_empty() {
        // Go's gzip.NewReader fails to read the header.
        return Err("EOF".to_string());
    }
    if body.len() < 10 || body[0] != 0x1f || body[1] != 0x8b || body[2] != 8 {
        return Err(if body.len() < 10 { "unexpected EOF" } else { "gzip: invalid header" }.to_string());
    }
    let mut out = Vec::new();
    let d = flate2::read::MultiGzDecoder::new(body);
    d.take(limit as u64).read_to_end(&mut out).map_err(|e| gzip_error_text(&e))?;
    Ok(out)
}

fn gzip_error_text(e: &std::io::Error) -> String {
    match e.kind() {
        std::io::ErrorKind::UnexpectedEof => "unexpected EOF".to_string(),
        std::io::ErrorKind::InvalidInput | std::io::ErrorKind::InvalidData => {
            let s = e.to_string();
            if s.contains("checksum") || s.contains("crc") { "gzip: invalid checksum".to_string() } else { format!("flate: {s}") }
        }
        _ => e.to_string(),
    }
}

/// Go `OTLPOptions`.
#[derive(Clone, Copy, Debug, Default)]
pub struct OtlpOptions {
    /// Convert delta samples to cumulative by aggregating in memory.
    pub convert_delta: bool,
    /// Store delta samples as they are, as metrics of unknown type.
    pub native_delta: bool,
    /// The query lookback delta, for the target_info sample interval.
    pub lookback_delta: Duration,
    /// Add `__type__` and `__unit__` labels.
    pub enable_type_and_unit_labels: bool,
}

/// Returns the OTLP configuration of the current config (Go passes `func() config.Config`; only its
/// `OTLPConfig` is read, so the handler takes that part).
pub type OtlpConfigFn = Arc<dyn Fn() -> config::OtlpConfig + Send + Sync>;

/// Go `rwExporter`: translates and appends decoded metrics.
pub struct RwExporter {
    logger: Logger,
    appendable: OtlpInstrumentedAppendable,
    config: OtlpConfigFn,
    allow_delta_temporality: bool,
    lookback_delta: Duration,
    enable_type_and_unit_labels: bool,
    /// `prometheus_api_otlp_translation_warnings_total{category}`.
    pub translation_warnings: Arc<CounterVec>,
    converter_pool: Mutex<Vec<PrometheusConverter>>,
}

/// Go `otlpWriteHandler` (an `http.Handler`).
pub struct OtlpWriteHandler {
    logger: Logger,
    /// Stores deltas as they are.
    pub default_consumer: RwExporter,
    /// Converts deltas to cumulative, when enabled.
    d2c: Option<DeltaToCumulative>,
}

fn register(reg: Option<&Registry>, c: Arc<CounterVec>) -> Arc<CounterVec> {
    if let Some(r) = reg {
        // promauto panics on a duplicate registration.
        r.register(c.clone()).unwrap_or_else(|e| panic!("{e}"));
    }
    c
}

/// Go `NewOTLPWriteHandler`. `reg` may be `None` (metrics are then not registered). Panics when both
/// `native_delta` and `convert_delta` are set.
pub fn new_otlp_write_handler(
    logger: Logger,
    reg: Option<&Registry>,
    appendable: Arc<dyn AppendableV2>,
    config_fn: OtlpConfigFn,
    opts: OtlpOptions,
) -> OtlpWriteHandler {
    if opts.native_delta && opts.convert_delta {
        // Feature flag validation rejects this earlier.
        panic!("cannot enable native delta ingestion and delta2cumulative conversion at the same time");
    }
    let translation_warnings = register(
        reg,
        CounterVec::new_counter_vec(
            Opts::new(
                "prometheus_api_otlp_translation_warnings_total",
                "The total number of warnings produced while translating OTLP metrics to the Prometheus model, by category.",
            ),
            &["category"],
        ),
    );
    let ex = RwExporter {
        logger: logger.clone(),
        appendable: new_otlp_instrumented_appendable(reg, appendable),
        config: config_fn,
        allow_delta_temporality: opts.native_delta,
        lookback_delta: opts.lookback_delta,
        enable_type_and_unit_labels: opts.enable_type_and_unit_labels,
        translation_warnings,
        converter_pool: Mutex::new(Vec::new()),
    };
    OtlpWriteHandler {
        logger,
        default_consumer: ex,
        d2c: if opts.convert_delta { Some(DeltaToCumulative::default()) } else { None },
    }
}

/// Go `http.Error`.
fn http_error(msg: &str, status: u16) -> http::Response {
    http::Response::text(status, "text/plain; charset=utf-8", format!("{msg}\n"))
        .with_header("X-Content-Type-Options", "nosniff")
}

impl RwExporter {
    /// Go `rwExporter.ConsumeMetrics`.
    pub fn consume_metrics(&self, ctx: &Context, md: &Metrics) -> Result<(), Error> {
        let otlp_cfg = (self.config)();
        let app = self.appendable.appender_v2(ctx);
        let mut converter = self
            .converter_pool
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop()
            .unwrap_or_else(|| PrometheusConverter::new(None));
        converter.reset(Some(app));
        let settings = Settings {
            add_metric_suffixes: namer::should_add_suffixes(&otlp_cfg.translation_strategy),
            allow_utf8: !namer::should_escape(&otlp_cfg.translation_strategy),
            promote_resource_attributes: Some(new_promote_resource_attributes(&otlp_cfg)),
            keep_identifying_resource_attributes: otlp_cfg.keep_identifying_resource_attributes,
            convert_histograms_to_nhcb: otlp_cfg.convert_histograms_to_nhcb,
            promote_scope_metadata: otlp_cfg.promote_scope_metadata,
            allow_delta_temporality: self.allow_delta_temporality,
            lookback_delta: self.lookback_delta,
            enable_type_and_unit_labels: self.enable_type_and_unit_labels,
            label_name_underscore_sanitization: otlp_cfg.label_name_underscore_sanitization,
            label_name_preserve_multiple_underscores: otlp_cfg.label_name_preserve_multiple_underscores,
            ..Default::default()
        };
        let (annots, err) = converter.from_metrics(ctx, md, &settings);

        let (ws, _) = annots.as_strings("", 0, 0);
        if !ws.is_empty() {
            for (category, count) in count_warnings_by_category(&annots) {
                self.translation_warnings.with_label_values(&[category.as_str()]).add(count as f64);
            }
            let joined = format!("[{}]", ws.join(" "));
            self.logger.warn("Warnings translating OTLP metrics to Prometheus write request", &[("warnings", &joined)]);
        }

        // Go's deferred function: roll back on error, otherwise commit. The commit error is assigned to a local
        // after the return value was taken, so Go never returns it; neither does this port.
        let mut app = converter.reset(None).expect("appender set above");
        if err.is_some() {
            let _ = app.rollback();
        } else {
            let _ = app.commit();
        }
        self.converter_pool.lock().unwrap_or_else(|e| e.into_inner()).push(converter);
        match err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

impl OtlpWriteHandler {
    /// Go `ServeHTTP`.
    pub fn serve_http(&self, req: &http::Request) -> http::Response {
        self.serve_http_ctx(&Context::background(), req)
    }

    /// `serve_http` with a request context.
    pub fn serve_http_ctx(&self, ctx: &Context, req: &http::Request) -> http::Response {
        let mut md = match decode_otlp_write_request(req) {
            Ok(md) => md,
            Err(e) => {
                self.logger.error("Error decoding OTLP write request", &[("err", &e)]);
                return http_error(&e, 400);
            }
        };
        // The delta to cumulative path is slower (it locks); use it only when there are deltas.
        let res = match &self.d2c {
            Some(d2c) if has_delta(&md) => {
                if d2c.process(&mut md) { self.default_consumer.consume_metrics(ctx, &md) } else { Ok(()) }
            }
            _ => self.default_consumer.consume_metrics(ctx, &md),
        };
        match res {
            Ok(()) => http::Response::new(200),
            Err(e)
                if e.is(&ERR_OUT_OF_ORDER_SAMPLE)
                    || e.is(&ERR_OUT_OF_BOUNDS)
                    || e.is(&ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP)
                    || e.is(&ERR_TOO_OLD_SAMPLE) =>
            {
                // A bad request, so that the client does not retry.
                http_error(&e.to_string(), 400)
            }
            Err(e) => {
                let msg = e.to_string();
                self.logger.error("Error appending remote write", &[("err", &msg)]);
                http_error(&msg, 500)
            }
        }
    }
}

/// Go `hasDelta`.
pub fn has_delta(md: &Metrics) -> bool {
    md.resource_metrics.iter().flat_map(|rm| rm.scope_metrics.iter()).flat_map(|sm| sm.metrics.iter()).any(|m| {
        let t = match &m.data {
            MetricData::Sum(s) => s.aggregation_temporality,
            MetricData::ExponentialHistogram(h) => h.aggregation_temporality,
            MetricData::Histogram(h) => h.aggregation_temporality,
            _ => AggregationTemporality::UNSPECIFIED,
        };
        t == AggregationTemporality::DELTA
    })
}

// ---------------------------------------------------------------------------------------------------------------
// Instrumented appendable

/// Go `otlpInstrumentedAppendable`: counts samples without metadata and out of order exemplars, and hides
/// partial (exemplar) errors from the converter.
pub struct OtlpInstrumentedAppendable {
    inner: Arc<dyn AppendableV2>,
    /// `prometheus_api_otlp_appended_samples_without_metadata_total`.
    pub samples_appended_without_metadata: Arc<CounterVec>,
    /// `prometheus_api_otlp_out_of_order_exemplars_total`.
    pub out_of_order_exemplars: Arc<CounterVec>,
}

/// Go `newOTLPInstrumentedAppendable`.
pub fn new_otlp_instrumented_appendable(reg: Option<&Registry>, app: Arc<dyn AppendableV2>) -> OtlpInstrumentedAppendable {
    OtlpInstrumentedAppendable {
        inner: app,
        samples_appended_without_metadata: register(
            reg,
            CounterVec::new_counter(Opts::new(
                "prometheus_api_otlp_appended_samples_without_metadata_total",
                "The total number of samples ingested from OTLP without corresponding metadata.",
            )),
        ),
        out_of_order_exemplars: register(
            reg,
            CounterVec::new_counter(Opts::new(
                "prometheus_api_otlp_out_of_order_exemplars_total",
                "The total number of received OTLP exemplars which were rejected because they were out of order.",
            )),
        ),
    }
}

impl AppendableV2 for OtlpInstrumentedAppendable {
    fn appender_v2(&self, ctx: &Context) -> Box<dyn AppenderV2> {
        let max_time = model::timestamp::from_time(SystemTime::now() + MAX_AHEAD_TIME);
        Box::new(OtlpInstrumentedAppender {
            inner: TimeLimitAppenderV2 { inner: self.inner.appender_v2(ctx), max_time },
            samples_appended_without_metadata: self.samples_appended_without_metadata.clone(),
            out_of_order_exemplars: self.out_of_order_exemplars.clone(),
        })
    }
}

/// Go `otlpInstrumentedAppender`.
struct OtlpInstrumentedAppender {
    inner: TimeLimitAppenderV2,
    samples_appended_without_metadata: Arc<CounterVec>,
    out_of_order_exemplars: Arc<CounterVec>,
}

impl AppenderTransaction for OtlpInstrumentedAppender {
    fn commit(&mut self) -> Option<Error> {
        self.inner.commit()
    }
    fn rollback(&mut self) -> Option<Error> {
        self.inner.rollback()
    }
}

impl AppenderV2 for OtlpInstrumentedAppender {
    fn append(
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
        let r = match self.inner.append(r, ls, st, t, v, h, fh, opts) {
            Ok(r) => r,
            Err(err) => {
                let mut partial: Option<AppendPartialError> = None;
                if let Some(e) = AppendPartialError::handle(&mut partial, Some(err)) {
                    // Not a partial error.
                    return Err(e);
                }
                let p = partial.unwrap_or_default();
                for e in &p.exemplar_errors {
                    if e.err.is(&ERR_OUT_OF_ORDER_EXEMPLAR) {
                        self.out_of_order_exemplars.get().inc();
                    }
                }
                // The converter does not handle partial errors: hide them.
                p.series_ref
            }
        };
        if opts.metadata.is_none_or(|m| m.is_empty()) {
            self.samples_appended_without_metadata.get().inc();
        }
        Ok(r)
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Time limit appender (Go `timeLimitAppenderV2` of write_handler.go; the remote write handler has the same type).

/// Go `timeLimitAppenderV2`: rejects samples and drops exemplars after `max_time`, and reduces the resolution of
/// exponential histograms with a schema above the maximum.
pub struct TimeLimitAppenderV2 {
    pub inner: Box<dyn AppenderV2>,
    pub max_time: i64,
}

/// Go `fmt.Errorf("%w: timestamp is too far in the future", storage.ErrOutOfBounds)`.
fn too_far_in_future() -> Error {
    Error::wrapped(format!("{ERR_OUT_OF_BOUNDS}: timestamp is too far in the future"), ERR_OUT_OF_BOUNDS)
}

impl TimeLimitAppenderV2 {
    /// Go `limitExemplars`: the exemplars not too far in the future, and an error for each dropped one.
    /// Exemplars are sorted by timestamp, so the dropped ones come last and the kept ones keep their indexes.
    fn limit_exemplars(&self, exemplars: &[Exemplar]) -> (Vec<Exemplar>, Option<AppendPartialError>) {
        let mut errs: Vec<ExemplarError> = Vec::new();
        let mut valid = Vec::with_capacity(exemplars.len());
        for (i, e) in exemplars.iter().enumerate() {
            if e.ts > self.max_time {
                errs.push(ExemplarError { index: i, err: too_far_in_future() });
                continue;
            }
            valid.push(e.clone());
        }
        if errs.is_empty() {
            return (valid, None);
        }
        (valid, Some(AppendPartialError { series_ref: 0, exemplar_errors: errs }))
    }
}

impl AppenderTransaction for TimeLimitAppenderV2 {
    fn commit(&mut self) -> Option<Error> {
        self.inner.commit()
    }
    fn rollback(&mut self) -> Option<Error> {
        self.inner.rollback()
    }
}

impl AppenderV2 for TimeLimitAppenderV2 {
    fn append(
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
        let mut h_owned;
        let mut h = h;
        if let Some(hh) = h
            && is_exponential_schema_reserved(hh.schema)
            && hh.schema > EXPONENTIAL_SCHEMA_MAX
        {
            h_owned = hh.clone();
            h_owned.reduce_resolution(EXPONENTIAL_SCHEMA_MAX).map_err(|e| Error::msg(e.to_string()))?;
            h = Some(&h_owned);
        }
        let mut fh_owned;
        let mut fh = fh;
        if let Some(f) = fh
            && is_exponential_schema_reserved(f.schema)
            && f.schema > EXPONENTIAL_SCHEMA_MAX
        {
            fh_owned = f.clone();
            fh_owned.reduce_resolution(EXPONENTIAL_SCHEMA_MAX).map_err(|e| Error::msg(e.to_string()))?;
            fh = Some(&fh_owned);
        }
        let mut partial = None;
        let limited;
        let mut opts = *opts;
        if !opts.exemplars.is_empty() {
            let (valid, p) = self.limit_exemplars(opts.exemplars);
            partial = p;
            limited = valid;
            opts.exemplars = &limited;
        }
        let res = self.inner.append(r, ls, st, t, v, h, fh, &opts);
        let r = match res {
            Ok(r) => r,
            Err(e) => {
                let sr = e.as_append_partial().map(|p| p.series_ref);
                if let Some(e) = AppendPartialError::handle(&mut partial, Some(e)) {
                    return Err(e);
                }
                sr.unwrap_or(0)
            }
        };
        if let Some(p) = &mut partial {
            p.series_ref = r;
        }
        match AppendPartialError::to_error(partial.as_ref()) {
            Some(e) => Err(e),
            None => Ok(r),
        }
    }
}
