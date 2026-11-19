//! Fixtures shared by the remote tests (the package-level variables of `codec_test.go` and the request
//! builders of `queue_manager.go` that the codec and write handler tests use).
#![allow(dead_code)]

use model::histogram::{custom_values, Histogram, Span, CUSTOM_BUCKETS_SCHEMA};
use model::metadata::{Metadata, MetricType};
use remote::codec::snappy_encode;
use remote::prompb::{self, writev2, Message};

pub fn test_histogram() -> Histogram {
    Histogram {
        schema: 2,
        zero_threshold: 1e-128,
        zero_count: 0,
        count: 3,
        sum: 20.0,
        positive_spans: vec![Span::new(0, 1)],
        positive_buckets: vec![1],
        negative_spans: vec![Span::new(0, 1)],
        negative_buckets: vec![2],
        ..Default::default()
    }
}

pub fn test_histogram_custom_buckets() -> Histogram {
    Histogram {
        schema: CUSTOM_BUCKETS_SCHEMA,
        count: 16,
        sum: 20.0,
        positive_spans: vec![Span::new(1, 2)],
        positive_buckets: vec![10, -4], // 10 observations for upper bound 1.0 and 6 for upper bound +Inf.
        custom_values: custom_values(&[0.1, 1.0]), // +Inf is implied.
        ..Default::default()
    }
}

pub fn l(name: &str, value: &str) -> prompb::Label {
    prompb::Label::new(name, value)
}

fn fixture_labels() -> Vec<prompb::Label> {
    vec![l("__name__", "test_metric1"), l("b", "c"), l("baz", "qux"), l("d", "e"), l("foo", "bar")]
}

pub fn write_request_fixture() -> prompb::WriteRequest {
    let th = test_histogram();
    prompb::WriteRequest {
        timeseries: vec![
            prompb::TimeSeries {
                labels: fixture_labels(),
                samples: vec![prompb::Sample { value: 1.0, timestamp: 1 }],
                exemplars: vec![prompb::Exemplar { labels: vec![l("f", "g")], value: 1.0, timestamp: 1 }],
                histograms: vec![prompb::from_int_histogram(1, &th), prompb::from_float_histogram(2, &th.to_float())],
            },
            prompb::TimeSeries {
                labels: fixture_labels(),
                samples: vec![prompb::Sample { value: 2.0, timestamp: 2 }],
                exemplars: vec![prompb::Exemplar { labels: vec![l("h", "i")], value: 2.0, timestamp: 2 }],
                histograms: vec![prompb::from_int_histogram(3, &th), prompb::from_float_histogram(4, &th.to_float())],
            },
        ],
        metadata: Vec::new(),
    }
}

pub fn write_v2_request_series1_metadata() -> Metadata {
    Metadata {
        typ: MetricType::Gauge,
        help: "Test gauge for test purposes".to_string(),
        unit: "Maybe op/sec who knows (:".to_string(),
    }
}

pub fn write_v2_request_series2_metadata() -> Metadata {
    Metadata { typ: MetricType::Counter, help: "Test counter for test purposes".to_string(), unit: String::new() }
}

pub fn write_v2_request_fixture() -> writev2::Request {
    let th = test_histogram();
    let thc = test_histogram_custom_buckets();
    let syms = [
        "", "__name__", "test_metric1", "b", "c", "baz", "qux", "d", "e", "foo", "bar", "f", "g", "h", "i",
        "Test gauge for test purposes", "Maybe op/sec who knows (:", "Test counter for test purposes",
    ];
    writev2::Request {
        symbols: syms.iter().map(|s| s.to_string()).collect(),
        timeseries: vec![
            writev2::TimeSeries {
                labels_refs: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                metadata: writev2::Metadata { r#type: writev2::MetadataType::GAUGE, help_ref: 15, unit_ref: 16 },
                // ST needs to be lower than the sample's timestamp.
                samples: vec![writev2::Sample { value: 1.0, timestamp: 10, start_timestamp: 1 }],
                exemplars: vec![writev2::Exemplar { labels_refs: vec![11, 12], value: 1.0, timestamp: 10 }],
                histograms: vec![
                    writev2::from_int_histogram(1, 10, &th),
                    writev2::from_float_histogram(2, 20, &th.to_float()),
                    writev2::from_int_histogram(11, 30, &thc),
                    writev2::from_float_histogram(21, 40, &thc.to_float()),
                ],
            },
            writev2::TimeSeries {
                labels_refs: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                metadata: writev2::Metadata { r#type: writev2::MetadataType::COUNTER, help_ref: 17, unit_ref: 0 },
                samples: vec![writev2::Sample { value: 2.0, timestamp: 20, start_timestamp: 0 }],
                exemplars: vec![writev2::Exemplar { labels_refs: vec![13, 14], value: 2.0, timestamp: 20 }],
                histograms: vec![
                    writev2::from_int_histogram(31, 50, &th),
                    writev2::from_float_histogram(41, 60, &th.to_float()),
                    writev2::from_int_histogram(51, 70, &thc),
                    writev2::from_float_histogram(61, 80, &thc.to_float()),
                ],
            },
        ],
    }
}

/// Go `buildWriteRequest(nil, series, metadata, nil, nil, nil, "snappy")` (queue_manager.go): the marshaled,
/// snappy-compressed request.
pub fn build_write_request(timeseries: Vec<prompb::TimeSeries>, metadata: Vec<prompb::MetricMetadata>) -> Vec<u8> {
    let req = prompb::WriteRequest { timeseries, metadata };
    snappy_encode(&req.marshal())
}

/// Go `buildV2WriteRequest(logger, series, symbols, nil, nil, nil, "snappy")`: optimized marshal, then snappy.
pub fn build_v2_write_request(timeseries: Vec<writev2::TimeSeries>, symbols: Vec<String>) -> Vec<u8> {
    let req = writev2::Request { symbols, timeseries };
    let mut buf = Vec::new();
    req.optimized_marshal(&mut buf);
    snappy_encode(&buf)
}
