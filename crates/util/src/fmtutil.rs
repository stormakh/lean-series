//! Port of `util/fmtutil`: converts the Prometheus text format to a remote write request (promtool push
//! metrics).
//!
//! The write request types have the fields of `prompb.WriteRequest` and its parts. They are defined here
//! because the `remote` crate, which owns the protobuf codec, depends on this crate.

use std::collections::BTreeMap;
use std::io::Read;

use model::labels::ValidationScheme;
use textparse::expfmt::{Metric, MetricFamily, TextParser, metric_type};
use textparse::strconv::format_g;

const SUM_STR: &str = "_sum";
const COUNT_STR: &str = "_count";
const BUCKET_STR: &str = "_bucket";

/// Go `model.MetricNameLabel`.
const METRIC_NAME_LABEL: &str = "__name__";
/// Go `model.QuantileLabel`.
const QUANTILE_LABEL: &str = "quantile";
/// Go `model.BucketLabel`.
const BUCKET_LABEL: &str = "le";
/// Go `model.JobLabel`.
const JOB_LABEL: &str = "job";
/// Go `model.ExportedLabelPrefix`.
const EXPORTED_LABEL_PREFIX: &str = "exported_";

/// `prompb.Label`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Label {
    pub name: String,
    pub value: String,
}

/// `prompb.Sample`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Sample {
    pub value: f64,
    pub timestamp: i64,
}

/// `prompb.TimeSeries` (labels and samples).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct TimeSeries {
    pub labels: Vec<Label>,
    pub samples: Vec<Sample>,
}

/// `prompb.MetricMetadata`; `type` is a `prompb.MetricMetadata_MetricType` value.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetricMetadata {
    pub r#type: i32,
    pub metric_family_name: String,
    pub help: String,
    pub unit: String,
}

/// `prompb.WriteRequest`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct WriteRequest {
    pub timeseries: Vec<TimeSeries>,
    pub metadata: Vec<MetricMetadata>,
}

/// Go `MetricMetadataTypeValue`: the `prompb.MetricMetadata_MetricType` of a `dto.MetricType` name.
pub fn metric_metadata_type_value(name: &str) -> Option<i32> {
    Some(match name {
        "UNKNOWN" => 0,
        "COUNTER" => 1,
        "GAUGE" => 2,
        "HISTOGRAM" => 3,
        "GAUGEHISTOGRAM" => 4,
        "SUMMARY" => 5,
        "INFO" => 6,
        "STATESET" => 7,
        _ => return None,
    })
}

/// Go `MetricTextToWriteRequest`: parses the text format and returns the data as a write request.
pub fn metric_text_to_write_request(input: impl Read, labels: &BTreeMap<String, String>) -> Result<WriteRequest, String> {
    // Lacking information about what the intended validation scheme is, Go uses the deprecated global default
    // `model.NameValidationScheme`, which is UTF-8.
    let parser = TextParser::new(ValidationScheme::Utf8);
    let mf = parser.text_to_metric_families(input).map_err(|e| e.to_string())?;
    metric_families_to_write_request(&mf, labels)
}

/// Go `MetricFamiliesToWriteRequest`: converts metric families to a write request. On error, Go also returns
/// the partial request; this returns only the error.
pub fn metric_families_to_write_request(
    mf: &BTreeMap<String, MetricFamily>,
    extra_labels: &BTreeMap<String, String>,
) -> Result<WriteRequest, String> {
    let mut wr = WriteRequest::default();
    // The map iterates in lexicographical order of the metric names, as Go sorts them.
    for (metric_name, fam) in mf {
        // An unset type has no String() in Go (nil pointer); the parser always sets it.
        let mtype = metric_metadata_type_value(&metric_type::name(fam.get_type())).unwrap_or(0);
        wr.metadata.push(MetricMetadata {
            metric_family_name: fam.name.clone(),
            r#type: mtype,
            help: fam.help.clone().unwrap_or_default(),
            unit: String::new(),
        });
        for metric in &fam.metric {
            let mut labels = make_labels_map(metric, metric_name, extra_labels);
            make_timeseries(&mut wr, &mut labels, metric)?;
        }
    }
    Ok(wr)
}

fn to_timeseries(wr: &mut WriteRequest, labels: &BTreeMap<String, String>, timestamp: i64, value: f64) {
    wr.timeseries.push(TimeSeries { labels: make_labels(labels), samples: vec![Sample { timestamp, value }] });
}

/// Go `makeTimeseries`: adds the time series of one metric. Summaries and histograms change `__name__` of
/// `labels` (as Go does to its map).
pub fn make_timeseries(wr: &mut WriteRequest, labels: &mut BTreeMap<String, String>, m: &Metric) -> Result<(), String> {
    let mut timestamp = m.timestamp_ms.unwrap_or(0);
    if timestamp == 0 {
        timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
    }
    if let Some(g) = &m.gauge {
        to_timeseries(wr, labels, timestamp, g.value);
    } else if let Some(c) = &m.counter {
        to_timeseries(wr, labels, timestamp, c.value);
    } else if let Some(s) = &m.summary {
        let metric_name = labels.get(METRIC_NAME_LABEL).cloned().unwrap_or_default();
        // Preserve metric name order with first quantile labels timeseries then sum suffix timeseries and finally
        // count suffix timeseries.
        let mut quantile_labels = labels.clone();
        for q in &s.quantile {
            quantile_labels.insert(QUANTILE_LABEL.to_string(), format_g(q.quantile));
            to_timeseries(wr, &quantile_labels, timestamp, q.value);
        }
        // Overwrite __name__ for the count and sum metrics.
        labels.insert(METRIC_NAME_LABEL.to_string(), metric_name.clone() + SUM_STR);
        to_timeseries(wr, labels, timestamp, s.sample_sum.unwrap_or(0.0));
        labels.insert(METRIC_NAME_LABEL.to_string(), metric_name + COUNT_STR);
        to_timeseries(wr, labels, timestamp, s.sample_count.unwrap_or(0) as f64);
    } else if let Some(h) = &m.histogram {
        let metric_name = labels.get(METRIC_NAME_LABEL).cloned().unwrap_or_default();
        // Preserve metric name order with first bucket suffix timeseries then sum suffix timeseries and finally
        // count suffix timeseries.
        let mut bucket_labels = labels.clone();
        let mut has_inf = false;
        for b in &h.bucket {
            let upper = b.upper_bound.unwrap_or(0.0);
            if upper == f64::INFINITY {
                has_inf = true;
            }
            bucket_labels.insert(METRIC_NAME_LABEL.to_string(), metric_name.clone() + BUCKET_STR);
            bucket_labels.insert(BUCKET_LABEL.to_string(), format_g(upper));
            to_timeseries(wr, &bucket_labels, timestamp, b.cumulative_count.unwrap_or(0) as f64);
        }
        // Add the +Inf bucket if not present.
        if !has_inf {
            bucket_labels.insert(METRIC_NAME_LABEL.to_string(), metric_name.clone() + BUCKET_STR);
            bucket_labels.insert(BUCKET_LABEL.to_string(), "+Inf".to_string());
            to_timeseries(wr, &bucket_labels, timestamp, h.sample_count.unwrap_or(0) as f64);
        }
        labels.insert(METRIC_NAME_LABEL.to_string(), metric_name.clone() + SUM_STR);
        to_timeseries(wr, labels, timestamp, h.sample_sum.unwrap_or(0.0));
        labels.insert(METRIC_NAME_LABEL.to_string(), metric_name + COUNT_STR);
        to_timeseries(wr, labels, timestamp, h.sample_count.unwrap_or(0) as f64);
    } else if let Some(u) = &m.untyped {
        to_timeseries(wr, labels, timestamp, u.value);
    } else {
        return Err("unsupported metric type".to_string());
    }
    Ok(())
}

/// Go `makeLabels`: the labels sorted by name.
fn make_labels(labels_map: &BTreeMap<String, String>) -> Vec<Label> {
    labels_map.iter().map(|(name, value)| Label { name: name.clone(), value: value.clone() }).collect()
}

/// Go `makeLabelsMap`: `__name__`, the extra labels, then the metric labels (`job` becomes `exported_job`).
fn make_labels_map(m: &Metric, metric_name: &str, extra_labels: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert(METRIC_NAME_LABEL.to_string(), metric_name.to_string());
    for (k, v) in extra_labels {
        labels.insert(k.clone(), v.clone());
    }
    for l in &m.label {
        let name = if l.name == JOB_LABEL { format!("{EXPORTED_LABEL_PREFIX}{}", l.name) } else { l.name.clone() };
        labels.insert(name, l.value.clone());
    }
    labels
}
