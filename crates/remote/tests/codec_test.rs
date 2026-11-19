//! Port of `prompb/rwcommon/codec_test.go` (the conversions of `prompb/codec.go` and
//! `prompb/io/prometheus/write/v2/codec.go`) and of `storage/remote/codec_test.go`.

use std::sync::Arc;

use model::histogram::{CounterResetHint, FloatHistogram, Histogram, Span};
use model::labels::{self, ScratchBuilder};
use model::metadata::{Metadata, MetricType as ModelMetricType};
use remote::prompb::{self, writev2};

// ---------------------------------------------------------------- prompb/rwcommon/codec_test.go

#[test]
fn test_to_labels() {
    let expected = labels::from_strings(&["__name__", "metric1", "foo", "bar"]);

    // v1
    {
        let ts = prompb::TimeSeries {
            labels: vec![prompb::Label::new("__name__", "metric1"), prompb::Label::new("foo", "bar")],
            ..Default::default()
        };
        let mut b = ScratchBuilder::new(2);
        assert_eq!(expected, ts.to_labels(&mut b));
        assert_eq!(ts.labels, prompb::labels_to_label_protos(&expected));
        let mut buf = ts.labels.clone();
        prompb::from_labels(&expected, &mut buf);
        assert_eq!(ts.labels, buf);
    }
    // v2
    {
        let v2_symbols: Vec<String> = ["", "__name__", "metric1", "foo", "bar"].iter().map(|s| s.to_string()).collect();
        let ts = writev2::TimeSeries { labels_refs: vec![1, 2, 3, 4], ..Default::default() };
        let mut b = ScratchBuilder::new(2);
        let result = ts.to_labels(&mut b, &v2_symbols).expect("to_labels");
        assert_eq!(expected, result);
    }
}

#[test]
fn test_from_metadata_type() {
    // (desc, input, expectedV1, expectedV2). Go's model.MetricType is a string; "not-known" has no enum value
    // in Rust, so that row goes through the string form of FromMetadataType for v1 and `Unknown`'s
    // neighbour (an unparsable type) for v2.
    struct Case {
        desc: &'static str,
        input: &'static str,
        expected_v1: prompb::MetricType,
        expected_v2: writev2::MetadataType,
    }
    let cases = [
        Case {
            desc: "with a single-word metric",
            input: "counter",
            expected_v1: prompb::MetricType::COUNTER,
            expected_v2: writev2::MetadataType::COUNTER,
        },
        Case {
            desc: "with a two-word metric",
            input: "stateset",
            expected_v1: prompb::MetricType::STATESET,
            expected_v2: writev2::MetadataType::STATESET,
        },
        Case {
            desc: "with an unknown metric",
            input: "not-known",
            expected_v1: prompb::MetricType::UNKNOWN,
            expected_v2: writev2::MetadataType::UNSPECIFIED,
        },
    ];
    for c in cases {
        assert_eq!(c.expected_v1, prompb::from_metadata_type_str(c.input), "{} v1", c.desc);
        // The typed form, for the inputs that are model metric types.
        let typed = ModelMetricType::parse(c.input);
        if let Some(t) = typed {
            assert_eq!(c.expected_v1, prompb::from_metadata_type(t), "{} v1 typed", c.desc);
        }
        assert_eq!(c.expected_v2, writev2::from_metadata_type_str(c.input), "{} v2", c.desc);
    }
}

#[test]
fn test_to_metadata() {
    let mut sym = writev2::SymbolsTable::new();

    let cases = vec![
        (writev2::Metadata::default(), Metadata { typ: ModelMetricType::Unknown, ..Default::default() }),
        (
            writev2::Metadata { r#type: writev2::MetadataType(12414), ..Default::default() }, // Unknown.
            Metadata { typ: ModelMetricType::Unknown, ..Default::default() },
        ),
        (
            writev2::Metadata {
                r#type: writev2::MetadataType::COUNTER,
                help_ref: sym.symbolize("help1"),
                unit_ref: sym.symbolize("unit1"),
            },
            Metadata { typ: ModelMetricType::Counter, help: "help1".into(), unit: "unit1".into() },
        ),
        (
            writev2::Metadata { r#type: writev2::MetadataType::STATESET, help_ref: sym.symbolize("help2"), ..Default::default() },
            Metadata { typ: ModelMetricType::StateSet, help: "help2".into(), ..Default::default() },
        ),
    ];
    for (input, expected) in cases {
        let ts = writev2::TimeSeries { metadata: input, ..Default::default() };
        let meta = ts.to_metadata(sym.symbols()).expect("to_metadata");
        assert_eq!(expected, meta);
    }

    // out of bounds unit ref
    let ts = writev2::TimeSeries { metadata: writev2::Metadata { unit_ref: 999, ..Default::default() }, ..Default::default() };
    let err = ts.to_metadata(sym.symbols()).unwrap_err();
    assert!(err.contains("metadata unit_ref 999 outside of symbols table"), "{err}");

    // out of bounds help ref
    let ts = writev2::TimeSeries { metadata: writev2::Metadata { help_ref: 999, ..Default::default() }, ..Default::default() };
    let err = ts.to_metadata(sym.symbols()).unwrap_err();
    assert!(err.contains("metadata help_ref 999 outside of symbols table"), "{err}");

    // empty symbols table
    let ts = writev2::TimeSeries::default();
    assert!(ts.to_metadata(&[]).is_err());
}

#[test]
fn test_to_histogram_empty() {
    // v1
    assert!(prompb::Histogram::default().to_int_histogram().is_some());
    let _ = prompb::Histogram::default().to_float_histogram();
    // v2 (same type)
    assert!(writev2::Histogram::default().to_int_histogram().is_some());
    let _ = writev2::Histogram::default().to_float_histogram();
}

// NOTE(bwplotka): This is technically not a valid histogram, but it represents
// important cases to test when copying or converting to/from int/float histograms.
fn test_int_histogram() -> Histogram {
    Histogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        schema: 1,
        count: 19,
        sum: 2.7,
        zero_threshold: 1e-128,
        positive_spans: vec![Span::new(0, 4), Span::new(0, 0), Span::new(0, 3)],
        positive_buckets: vec![1, 2, -2, 1, -1, 0, 0],
        negative_spans: vec![Span::new(0, 5), Span::new(1, 0), Span::new(0, 1)],
        negative_buckets: vec![1, 2, -2, 1, -1, 0],
        custom_values: Some(Arc::from(&[21421.0, 523.0][..])),
        ..Default::default()
    }
}

// NOTE(bwplotka): This is technically not a valid histogram, but it represents
// important cases to test when copying or converting to/from int/float histograms.
fn test_float_histogram() -> FloatHistogram {
    FloatHistogram {
        counter_reset_hint: CounterResetHint::GaugeType,
        schema: 1,
        count: 19.0,
        sum: 2.7,
        zero_threshold: 1e-128,
        positive_spans: vec![Span::new(0, 4), Span::new(0, 0), Span::new(0, 3)],
        positive_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0, 1.0],
        negative_spans: vec![Span::new(0, 5), Span::new(1, 0), Span::new(0, 1)],
        negative_buckets: vec![1.0, 3.0, 1.0, 2.0, 1.0, 1.0],
        custom_values: Some(Arc::from(&[21421.0, 523.0][..])),
        ..Default::default()
    }
}

#[test]
fn test_from_int_to_float_or_int_histogram() {
    // v1
    {
        let test_int_hist = test_int_histogram();
        let test_float_hist = test_float_histogram();
        let h = prompb::from_int_histogram(123, &test_int_hist);
        assert!(!h.is_float_histogram());
        assert_eq!(123, h.timestamp);
        assert_eq!(test_int_hist, h.to_int_histogram().unwrap());
        assert_eq!(test_float_hist, h.to_float_histogram());
    }
    // v2
    {
        let test_int_hist = test_int_histogram();
        let test_float_hist = test_float_histogram();
        let h = writev2::from_int_histogram(0, 123, &test_int_hist);
        assert!(!h.is_float_histogram());
        assert_eq!(123, h.timestamp);
        assert_eq!(test_int_hist, h.to_int_histogram().unwrap());
        assert_eq!(test_float_hist, h.to_float_histogram());
    }
}

#[test]
fn test_from_float_to_float_histogram() {
    // v1
    {
        let test_float_hist = test_float_histogram();
        let h = prompb::from_float_histogram(123, &test_float_hist);
        assert!(h.is_float_histogram());
        assert_eq!(123, h.timestamp);
        assert!(h.to_int_histogram().is_none());
        assert_eq!(test_float_hist, h.to_float_histogram());
    }
    // v2
    {
        let test_float_hist = test_float_histogram();
        let h = writev2::from_float_histogram(0, 123, &test_float_hist);
        assert!(h.is_float_histogram());
        assert_eq!(123, h.timestamp);
        assert!(h.to_int_histogram().is_none());
        assert_eq!(test_float_hist, h.to_float_histogram());
    }
}

#[test]
fn test_from_int_or_float_histogram_reset_hint() {
    let cases = [
        (CounterResetHint::UnknownCounterReset, prompb::ResetHint::UNKNOWN),
        (CounterResetHint::CounterReset, prompb::ResetHint::YES),
        (CounterResetHint::NotCounterReset, prompb::ResetHint::NO),
        (CounterResetHint::GaugeType, prompb::ResetHint::GAUGE),
    ];
    // writev2's RESET_HINT_UNSPECIFIED/YES/NO/GAUGE have the same numbers as v1's UNKNOWN/YES/NO/GAUGE.
    for (input, expected) in cases {
        // v1
        let mut h = test_int_histogram();
        h.counter_reset_hint = input;
        assert_eq!(expected, prompb::from_int_histogram(1337, &h).get_reset_hint());
        let mut fh = test_float_histogram();
        fh.counter_reset_hint = input;
        assert_eq!(expected, prompb::from_float_histogram(1337, &fh).get_reset_hint());
        // v2
        let mut h = test_int_histogram();
        h.counter_reset_hint = input;
        assert_eq!(expected, writev2::from_int_histogram(0, 1337, &h).get_reset_hint());
        let mut fh = test_float_histogram();
        fh.counter_reset_hint = input;
        assert_eq!(expected, writev2::from_float_histogram(0, 1337, &fh).get_reset_hint());
    }
}

#[test]
fn test_from_int_or_float_histogram_start_timestamp() {
    for (name, st) in [("no start timestamp", 0i64), ("with start timestamp", 100)] {
        let h = test_int_histogram();
        let got = writev2::from_int_histogram(st, 1337, &h);
        assert_eq!(st, got.start_timestamp, "{name}");
        assert_eq!(1337, got.timestamp, "{name}");

        let fh = test_float_histogram();
        let got2 = writev2::from_float_histogram(st, 1337, &fh);
        assert_eq!(st, got2.start_timestamp, "{name}");
        assert_eq!(1337, got2.timestamp, "{name}");
    }
}
