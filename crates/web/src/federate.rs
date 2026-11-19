//! Port of `web/federate.go`: the `/federate` endpoint. It selects the latest sample (within the lookback delta)
//! of every series that matches one of the `match[]` selectors, adds the external labels, and writes the result
//! in the negotiated exposition format (protobuf delimited, protobuf text, compact protobuf text, or the text
//! format 0.0.4; Go's `expfmt.NegotiateAccept` with these four formats).
//!
//! Native histograms are only written in the protobuf formats; custom-bucket histograms (NHCB) are written as
//! classic histograms in every format. The response is built in memory and then sent (Go streams it).

use std::collections::BTreeMap;

use chunkenc::{ChunkIterator, ValueType};
use http::{Request, Response};
use model::histogram::{BucketIterator, CounterResetHint, FloatHistogram};
use model::labels::Labels;
use storage::{SelectHints, chained_series_merge, new_buffer_iterator, new_merge_series_set};

use crate::expfmt::{self, DtoBucket, DtoFamily, DtoHistogram, DtoMetric, DtoType, DtoValue, FormatType};
use crate::route::http_error;
use crate::web::{Handler, go_parse_query};

/// One selected sample (Go `promql.Sample` as federation fills it).
struct FedSample {
    metric: Labels,
    t: i64,
    f: f64,
    h: Option<FloatHistogram>,
}

impl Handler {
    /// Go `(*Handler).federation`.
    pub fn federation(&self, req: &Request) -> Response {
        let st = self.state();
        let form = match go_parse_query(&req.query) {
            Ok(f) => f,
            Err(e) => return http_error(&format!("error parsing form values: {}", e), 400),
        };
        let matches: Vec<&str> = form.iter().filter(|(k, _)| k == "match[]").map(|(_, v)| v.as_str()).collect();
        let matcher_sets = match self.parser.parse_metric_selectors(&matches) {
            Ok(m) => m,
            Err(e) => return http_error(&e.to_string(), 400),
        };

        let now = (self.now)();
        let lookback_ms = self.options.lookback_delta.as_millis() as i64;
        let mint = now - lookback_ms;
        let maxt = now;
        let format = expfmt::negotiate_accept(
            req.header("accept").unwrap_or(""),
            &[expfmt::FMT_PROTO_DELIM, expfmt::FMT_PROTO_TEXT, expfmt::FMT_PROTO_COMPACT, expfmt::FMT_TEXT],
        );
        let enc = expfmt::Encoder::new(&format);

        let Some(local) = &self.options.local_storage else {
            self.metrics.federation_errors.inc();
            return http_error("no local storage configured", 500);
        };
        let q = match local.querier(mint, maxt) {
            Ok(q) => q,
            Err(e) => {
                self.metrics.federation_errors.inc();
                if e.is(&tsdb::db::ERR_NOT_READY) {
                    return http_error(&e.to_string(), 503);
                }
                return http_error(&e.to_string(), 500);
            }
        };

        let ctx = storage::Context::background();
        let hints = SelectHints { start: mint, end: maxt, ..Default::default() };
        let sets = matcher_sets.iter().map(|ms| q.select(&ctx, true, Some(&hints), ms)).collect();
        let mut set = new_merge_series_set(sets, 0, std::sync::Arc::new(chained_series_merge));

        let mut vec: Vec<FedSample> = Vec::with_capacity(64);
        while set.next() {
            let s = set.at().clone();
            let mut it = new_buffer_iterator(s.iterator(None), lookback_ms);
            let (t, f, fh) = match it.seek(maxt) {
                ValueType::Float => {
                    let (t, f) = it.at();
                    (t, f, None)
                }
                ValueType::FloatHistogram | ValueType::Histogram => {
                    let (t, fh) = it.at_float_histogram(None);
                    (t, 0.0, Some(fh))
                }
                ValueType::None => {
                    let Some(sample) = it.peek_back(1) else { continue };
                    match sample.typ() {
                        ValueType::Float => (sample.t(), sample.f(), None),
                        ValueType::Histogram => (sample.t(), 0.0, sample.h().map(|h| h.to_float())),
                        ValueType::FloatHistogram => (sample.t(), 0.0, sample.fh().cloned()),
                        ValueType::None => continue,
                    }
                }
            };
            // The exposition formats do not support stale markers, so drop them.
            if model::value::is_stale_nan(f) || fh.as_ref().is_some_and(|h| model::value::is_stale_nan(h.sum)) {
                continue;
            }
            vec.push(FedSample { metric: s.labels().clone(), t, f, h: fh });
        }
        let ws = set.warnings();
        if !ws.is_empty() {
            self.logger.debug("Federation select returned warnings", &[("warnings", &format!("{:?}", ws.as_strings("", 0, 0)))]);
            self.metrics.federation_warnings.add(ws.len() as f64);
        }
        if let Some(e) = set.err() {
            self.metrics.federation_errors.inc();
            return http_error(&e.to_string(), 500);
        }

        vec.sort_by(|a, b| a.metric.get("__name__").cmp(b.metric.get("__name__")));

        let mut external: BTreeMap<String, String> = BTreeMap::new();
        st.config.global_config.external_labels.range(|n, v| {
            external.insert(n.to_string(), v.to_string());
        });
        external.entry("instance".to_string()).or_default();

        let format_type = format.format_type();
        let is_proto = matches!(format_type, FormatType::ProtoDelim | FormatType::ProtoText | FormatType::ProtoCompact);
        let mut body: Vec<u8> = Vec::with_capacity(4096);
        let mut last_metric_name = String::new();
        let (mut last_was_histogram, mut last_histogram_was_gauge) = (false, false);
        let mut fam: Option<DtoFamily> = None;
        for s in &vec {
            let is_histogram = s.h.is_some();
            if let Some(h) = &s.h {
                if !h.uses_custom_buckets() && !is_proto {
                    // Can't serve a native histogram with a non-protobuf format (an NHCB is converted to a
                    // classic histogram).
                    continue;
                }
            }
            let is_gauge_hist = s.h.as_ref().is_some_and(|h| h.counter_reset_hint == CounterResetHint::GaugeType);
            let mut name_seen = false;
            let mut global_used: Vec<&str> = Vec::new();
            let mut labels: Vec<(String, String)> = Vec::new();
            let mut enc_err: Option<String> = None;
            for (l_name, l_value) in s.metric.iter() {
                if l_value.is_empty() {
                    // No value means unset.
                    continue;
                }
                if l_name == "__name__" {
                    name_seen = true;
                    if l_value == last_metric_name
                        && last_was_histogram == is_histogram
                        && (!is_histogram || last_histogram_was_gauge == is_gauge_hist)
                    {
                        continue;
                    }
                    // Ship off the old family (if any) before starting a new one.
                    if let Some(f) = fam.take() {
                        if let Err(e) = enc.encode(&mut body, &f) {
                            enc_err = Some(e);
                            break;
                        }
                    }
                    let typ = if !is_histogram {
                        DtoType::Untyped
                    } else if is_gauge_hist {
                        DtoType::GaugeHistogram
                    } else {
                        DtoType::Histogram
                    };
                    fam = Some(DtoFamily { name: l_value.to_string(), help: None, typ, unit: None, metrics: Vec::new() });
                    last_metric_name = l_value.to_string();
                    continue;
                }
                labels.push((l_name.to_string(), l_value.to_string()));
                if let Some((k, _)) = external.get_key_value(l_name) {
                    global_used.push(k.as_str());
                }
            }
            if let Some(e) = enc_err {
                self.metrics.federation_errors.inc();
                self.logger.error("federation failed", &[("err", &e)]);
                return self.federation_response(&format, body);
            }
            if !name_seen {
                self.logger.warn("Ignoring nameless metric during federation", &[("metric", &s.metric.to_string())]);
                continue;
            }
            // Attach global labels if they do not exist yet.
            for (ln, lv) in &external {
                if !global_used.contains(&ln.as_str()) {
                    labels.push((ln.clone(), lv.clone()));
                }
            }
            let value = match &s.h {
                None => {
                    last_histogram_was_gauge = false;
                    DtoValue::Untyped(s.f)
                }
                Some(h) => {
                    last_histogram_was_gauge = is_gauge_hist;
                    DtoValue::Histogram(if h.uses_custom_buckets() { make_classic_histogram(h) } else { make_native_histogram(h) })
                }
            };
            last_was_histogram = is_histogram;
            if let Some(f) = fam.as_mut() {
                f.metrics.push(DtoMetric { labels, value, timestamp_ms: Some(s.t) });
            }
        }
        if let Some(f) = fam.take() {
            if let Err(e) = enc.encode(&mut body, &f) {
                self.metrics.federation_errors.inc();
                self.logger.error("federation failed", &[("err", &e)]);
            }
        }
        self.federation_response(&format, body)
    }

    fn federation_response(&self, format: &expfmt::Format, body: Vec<u8>) -> Response {
        let mut resp = Response::new(200);
        resp.headers.push(("Content-Type".to_string(), format.0.clone()));
        resp.body = http::Body::Bytes(body);
        resp
    }
}

/// Go `makeNativeHistogram`: a native histogram for standard exponential schemas.
fn make_native_histogram(h: &FloatHistogram) -> DtoHistogram {
    DtoHistogram {
        sample_count_float: Some(h.count),
        sample_sum: Some(h.sum),
        schema: Some(h.schema),
        zero_threshold: Some(h.zero_threshold),
        zero_count_float: Some(h.zero_count),
        negative_counts: h.negative_buckets.clone(),
        positive_counts: h.positive_buckets.clone(),
        positive_spans: h.positive_spans.iter().map(|s| (s.offset, s.length)).collect(),
        negative_spans: h.negative_spans.iter().map(|s| (s.offset, s.length)).collect(),
        ..Default::default()
    }
}

/// Go `makeClassicHistogram`: a classic histogram for an NHCB (schema -53). The +Inf bucket is not added (it is
/// optional in protobuf; the text encoder adds it).
fn make_classic_histogram(h: &FloatHistogram) -> DtoHistogram {
    let cv = h.custom_values_slice();
    let mut buckets = Vec::with_capacity(cv.len());
    let mut cumulative = 0.0;
    let mut it = h.positive_bucket_iterator();
    let mut available = it.next();
    for (i, le) in cv.iter().enumerate() {
        while available && (it.at().index as i64) < i as i64 {
            available = it.next();
        }
        if available && it.at().index as i64 == i as i64 {
            cumulative += it.at().count;
        }
        buckets.push(DtoBucket { cumulative_count: None, cumulative_count_float: Some(cumulative), upper_bound: *le });
    }
    DtoHistogram { sample_count_float: Some(h.count), sample_sum: Some(h.sum), buckets, ..Default::default() }
}
