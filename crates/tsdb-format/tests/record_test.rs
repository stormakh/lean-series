//! Port of `tsdb/record/record_test.go`.

use std::sync::{Arc, Mutex};

use model::histogram::{
    CUSTOM_BUCKETS_SCHEMA, CounterResetHint, EXPONENTIAL_SCHEMA_MAX, FloatHistogram, Histogram, Span, custom_values,
};
use model::labels::from_strings;
use tsdb_format::encoding::{self, Encbuf};
use tsdb_format::record::{
    Decoder, Encoder, Logger, MetricType, RefExemplar, RefFloatHistogramSample, RefHistogramSample, RefMetadata,
    RefSample, RefSeries, Type,
};
use tsdb_format::tombstones::{Interval, Stone};

// adapted: Go uses math/rand for some sums; a time-seeded xorshift gives the same kind of arbitrary value.
fn rand_f64() -> f64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let mut x = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64 | 1;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    (x >> 11) as f64 / (1u64 << 53) as f64
}

/// A decoder whose log lines go to a shared string (Go `promslog.New(&promslog.Config{Writer: &output})`).
fn logging_decoder() -> (Decoder, Arc<Mutex<String>>) {
    let out = Arc::new(Mutex::new(String::new()));
    let o = out.clone();
    let dec = Decoder::new(Logger::new(move |line| {
        let mut s = o.lock().unwrap();
        s.push_str(line);
        s.push('\n');
    }));
    (dec, out)
}

fn spans(v: &[(i32, u32)]) -> Vec<Span> {
    v.iter().map(|&(o, l)| Span::new(o, l)).collect()
}

fn hs(r: u64, t: i64, h: Histogram) -> RefHistogramSample {
    RefHistogramSample { r#ref: r, st: 0, t, h: Arc::new(h) }
}

fn fhs(r: u64, t: i64, fh: FloatHistogram) -> RefFloatHistogramSample {
    RefFloatHistogramSample { r#ref: r, st: 0, t, fh: Arc::new(fh) }
}

fn to_float(h: &RefHistogramSample) -> RefFloatHistogramSample {
    RefFloatHistogramSample { r#ref: h.r#ref, st: h.st, t: h.t, fh: Arc::new(h.h.to_float()) }
}

fn enc_bytes(f: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut b = Vec::new();
    f(&mut b);
    b
}

fn regular_a(sum: f64) -> Histogram {
    Histogram {
        count: 5,
        zero_count: 2,
        zero_threshold: 0.001,
        sum,
        schema: 1,
        positive_spans: spans(&[(0, 2), (1, 2)]),
        positive_buckets: vec![1, 1, -1, 0],
        ..Default::default()
    }
}

fn regular_b() -> Histogram {
    Histogram {
        count: 11,
        zero_count: 4,
        zero_threshold: 0.001,
        sum: 35.5,
        schema: 1,
        positive_spans: spans(&[(0, 2), (2, 2)]),
        positive_buckets: vec![1, 1, -1, 0],
        negative_spans: spans(&[(0, 1), (1, 2)]),
        negative_buckets: vec![1, 2, -1],
        ..Default::default()
    }
}

fn custom_h(sum: f64) -> Histogram {
    Histogram {
        count: 8,
        zero_threshold: 0.001,
        sum,
        schema: CUSTOM_BUCKETS_SCHEMA,
        positive_spans: spans(&[(0, 2), (2, 2)]),
        positive_buckets: vec![2, -1, 2, 0],
        custom_values: custom_values(&[0.0, 2.0, 4.0, 6.0, 8.0]),
        ..Default::default()
    }
}

#[test]
fn test_record_encode_decode() {
    let mut enc = Encoder::default();
    let mut dec = Decoder::new(Logger::nop());

    let series = vec![
        RefSeries { r#ref: 100, labels: from_strings(&["abc", "def", "123", "456"]) },
        RefSeries { r#ref: 1, labels: from_strings(&["abc", "def2", "1234", "4567"]) },
        RefSeries { r#ref: 435245, labels: from_strings(&["xyz", "def", "foo", "bar"]) },
    ];
    let mut dec_series = Vec::new();
    dec.series(&enc_bytes(|b| enc.series(&series, b)), &mut dec_series).unwrap();
    assert_eq!(series, dec_series);

    let metadata = vec![
        RefMetadata {
            r#ref: 100,
            r#type: MetricType::Counter as u8,
            unit: "".into(),
            help: "some magic counter".into(),
        },
        RefMetadata {
            r#ref: 1,
            r#type: MetricType::Counter as u8,
            unit: "seconds".into(),
            help: "CPU time counter".into(),
        },
        RefMetadata {
            r#ref: 147741,
            r#type: MetricType::Gauge as u8,
            unit: "percentage".into(),
            help: "current memory usage".into(),
        },
    ];
    let mut dec_metadata = Vec::new();
    dec.metadata(&enc_bytes(|b| enc.metadata(&metadata, b)), &mut dec_metadata).unwrap();
    assert_eq!(metadata, dec_metadata);

    let rs = |r: u64, t: i64, st: i64, v: f64| RefSample { r#ref: r, st, t, v };

    // Without ST.
    let samples = vec![rs(0, 12423423, 0, 1.2345), rs(123, -1231, 0, -123.0), rs(2, 0, 0, 99999.0)];
    let encoded = enc_bytes(|b| enc.samples(&samples, b));
    assert_eq!(Type::Samples, dec.r#type(&encoded));
    let mut dec_samples = Vec::new();
    dec.samples(&encoded, &mut dec_samples).unwrap();
    assert_eq!(samples, dec_samples);

    enc = Encoder { enable_st_storage: true };
    // Without ST again, but with V1 encoder that enables SamplesV2.
    let samples = vec![rs(0, 12423423, 0, 1.2345), rs(123, -1231, 0, -123.0), rs(2, 0, 0, 99999.0)];
    let encoded = enc_bytes(|b| enc.samples(&samples, b));
    assert_eq!(Type::SamplesV2, dec.r#type(&encoded));
    let mut dec_samples = Vec::new();
    dec.samples(&encoded, &mut dec_samples).unwrap();
    assert_eq!(samples, dec_samples);

    // With ST.
    let samples_with_st = vec![rs(0, 12423423, 14, 1.2345), rs(123, -1231, 14, -123.0), rs(2, 0, 14, 99999.0)];
    let encoded = enc_bytes(|b| enc.samples(&samples_with_st, b));
    assert_eq!(Type::SamplesV2, dec.r#type(&encoded));
    let mut dec_samples = Vec::new();
    dec.samples(&encoded, &mut dec_samples).unwrap();
    assert_eq!(samples_with_st, dec_samples);

    // With ST (ST[i] == T[i-1]).
    let samples_with_st_delta = vec![
        rs(0, 12423400, 12423300, 1.2345),
        rs(123, 12423500, 12423400, -123.0),
        rs(2, 12423600, 12423500, 99999.0),
    ];
    let mut dec_samples = Vec::new();
    dec.samples(&enc_bytes(|b| enc.samples(&samples_with_st_delta, b)), &mut dec_samples).unwrap();
    assert_eq!(samples_with_st_delta, dec_samples);

    // With ST (ST[i] == ST[i-1]).
    let samples_with_const_st = vec![
        rs(0, 12423400, 12423300, 1.2345),
        rs(123, 12423500, 12423300, -123.0),
        rs(2, 12423600, 12423300, 99999.0),
    ];
    let mut dec_samples = Vec::new();
    dec.samples(&enc_bytes(|b| enc.samples(&samples_with_const_st, b)), &mut dec_samples).unwrap();
    assert_eq!(samples_with_const_st, dec_samples);

    // Intervals get split up into single entries. So we don't get back exactly what we put in.
    let iv = Interval::new;
    let tstones = vec![
        Stone { r#ref: 123, intervals: vec![iv(-1000, 1231231), iv(5000, 0)] },
        Stone { r#ref: 13, intervals: vec![iv(-1000, -11), iv(5000, 1000)] },
    ];
    let mut dec_tstones = Vec::new();
    dec.tombstones(&enc_bytes(|b| enc.tombstones(&tstones, b)), &mut dec_tstones).unwrap();
    assert_eq!(
        vec![
            Stone { r#ref: 123, intervals: vec![iv(-1000, 1231231)] },
            Stone { r#ref: 123, intervals: vec![iv(5000, 0)] },
            Stone { r#ref: 13, intervals: vec![iv(-1000, -11)] },
            Stone { r#ref: 13, intervals: vec![iv(5000, 1000)] },
        ],
        dec_tstones
    );

    let exemplars = vec![
        RefExemplar { r#ref: 0, t: 12423423, v: 1.2345, labels: from_strings(&["trace_id", "qwerty"]) },
        RefExemplar { r#ref: 123, t: -1231, v: -123.0, labels: from_strings(&["trace_id", "asdf"]) },
        RefExemplar { r#ref: 2, t: 0, v: 99999.0, labels: from_strings(&["trace_id", "zxcv"]) },
    ];
    let mut dec_exemplars = Vec::new();
    dec.exemplars(&enc_bytes(|b| enc.exemplars(&exemplars, b)), &mut dec_exemplars).unwrap();
    assert_eq!(exemplars, dec_exemplars);

    // Go's enc is still the ST encoder here (it was reassigned above), so these round trips use V2.
    let mut histograms = vec![
        hs(56, 1234, regular_a(18.4 * rand_f64())),
        hs(
            42,
            5678,
            Histogram {
                count: 11,
                zero_count: 4,
                zero_threshold: 0.001,
                sum: 35.5,
                schema: 1,
                positive_spans: spans(&[(0, 2), (2, 2)]),
                positive_buckets: vec![1, 1, -1, 0],
                negative_spans: spans(&[(0, 1), (1, 2)]),
                negative_buckets: vec![1, 2, -1],
                ..Default::default()
            },
        ),
        hs(67, 5678, custom_h(35.5)),
    ];

    let round_trip_hist = |enc: &Encoder, dec: &Decoder, histograms: &[RefHistogramSample]| {
        let mut hist_samples = Vec::new();
        let custom = enc.histogram_samples(histograms, &mut hist_samples);
        let mut custom_samples = Vec::new();
        enc.custom_buckets_histogram_samples(&custom, &mut custom_samples);
        let mut dec_histograms = Vec::new();
        dec.histogram_samples(&hist_samples, &mut dec_histograms).unwrap();
        let mut dec_custom = Vec::new();
        dec.histogram_samples(&custom_samples, &mut dec_custom).unwrap();
        dec_histograms.extend(dec_custom);
        dec_histograms
    };
    let round_trip_float = |enc: &Encoder, dec: &Decoder, histograms: &[RefFloatHistogramSample]| {
        let mut hist_samples = Vec::new();
        let custom = enc.float_histogram_samples(histograms, &mut hist_samples);
        let mut custom_samples = Vec::new();
        enc.custom_buckets_float_histogram_samples(&custom, &mut custom_samples);
        let mut dec_histograms = Vec::new();
        dec.float_histogram_samples(&hist_samples, &mut dec_histograms).unwrap();
        let mut dec_custom = Vec::new();
        dec.float_histogram_samples(&custom_samples, &mut dec_custom).unwrap();
        dec_histograms.extend(dec_custom);
        dec_histograms
    };

    assert_eq!(histograms, round_trip_hist(&enc, &dec, &histograms));

    let mut float_histograms: Vec<RefFloatHistogramSample> =
        histograms.iter().map(|h| fhs(h.r#ref, h.t, h.h.to_float())).collect();
    assert_eq!(float_histograms, round_trip_float(&enc, &dec, &float_histograms));

    enc = Encoder { enable_st_storage: true };

    // Gauge integer histograms.
    for h in histograms.iter_mut() {
        Arc::make_mut(&mut h.h).counter_reset_hint = CounterResetHint::GaugeType;
    }
    assert_eq!(histograms, round_trip_hist(&enc, &dec, &histograms));

    // Gauge float histograms.
    for h in float_histograms.iter_mut() {
        Arc::make_mut(&mut h.fh).counter_reset_hint = CounterResetHint::GaugeType;
    }
    assert_eq!(float_histograms, round_trip_float(&enc, &dec, &float_histograms));

    // V2 gauge int-histogram round-trip. V2 does not disentangle regular and custom bucket histograms, so the
    // encoder never returns a slice of leftover items.
    {
        enc = Encoder { enable_st_storage: true };
        let gauge_hists_v2 = vec![
            RefHistogramSample { r#ref: 56, t: 1234, st: 1000, h: histograms[0].h.clone() },
            RefHistogramSample { r#ref: 42, t: 5678, st: 1000, h: histograms[1].h.clone() },
            RefHistogramSample { r#ref: 67, t: 5678, st: 1000, h: histograms[2].h.clone() },
        ];
        let mut hist_samples_v2 = Vec::new();
        let left_over = enc.histogram_samples(&gauge_hists_v2, &mut hist_samples_v2);
        assert!(left_over.is_empty());
        let mut dec_hists_v2 = Vec::new();
        dec.histogram_samples(&hist_samples_v2, &mut dec_hists_v2).unwrap();
        assert_eq!(gauge_hists_v2, dec_hists_v2);
    }

    // V2 gauge float-histogram round-trip.
    {
        let gauge_hists_v2 = vec![
            RefHistogramSample { r#ref: 56, t: 1234, st: 1000, h: histograms[0].h.clone() },
            RefHistogramSample { r#ref: 42, t: 5678, st: 1000, h: histograms[1].h.clone() },
            RefHistogramSample { r#ref: 67, t: 5678, st: 1000, h: histograms[2].h.clone() },
        ];
        let gauge_float_hists_v2: Vec<_> = gauge_hists_v2.iter().map(to_float).collect();
        let mut float_hist_samples_v2 = Vec::new();
        let left_over = enc.float_histogram_samples(&gauge_float_hists_v2, &mut float_hist_samples_v2);
        assert!(left_over.is_empty());
        let mut dec_float_hists_v2 = Vec::new();
        dec.float_histogram_samples(&float_hist_samples_v2, &mut dec_float_hists_v2).unwrap();
        assert_eq!(gauge_float_hists_v2, dec_float_hists_v2);
    }

    for enable_st_storage in [false, true] {
        // int-histogram empty slice.
        {
            let enc = Encoder { enable_st_storage };
            let mut hist_buf = Vec::new();
            let custom_buckets = enc.histogram_samples(&[], &mut hist_buf);
            assert!(custom_buckets.is_empty());
            let mut decoded = Vec::new();
            dec.histogram_samples(&hist_buf, &mut decoded).unwrap();
            assert!(decoded.is_empty());
        }
        // float-histogram empty slice.
        {
            let enc = Encoder { enable_st_storage };
            let mut float_buf = Vec::new();
            let custom_buckets_float = enc.float_histogram_samples(&[], &mut float_buf);
            assert!(custom_buckets_float.is_empty());
            let mut decoded = Vec::new();
            dec.float_histogram_samples(&float_buf, &mut decoded).unwrap();
            assert!(decoded.is_empty());
        }
    }

    // When all histograms are custom-bucket, V1 HistogramSamples must return an empty buffer (buf.Reset path)
    // and pass every sample through as custom.
    {
        let enc_v1 = Encoder::default();
        let all_custom = vec![
            RefHistogramSample { r#ref: 56, st: 0, t: 1234, h: histograms[2].h.clone() },
            RefHistogramSample { r#ref: 67, st: 0, t: 5678, h: histograms[2].h.clone() },
        ];
        let mut hist_buf = Vec::new();
        let custom_buckets = enc_v1.histogram_samples(&all_custom, &mut hist_buf);
        assert!(hist_buf.is_empty(), "regular histogram buffer must be empty when all samples are custom bucket");
        assert_eq!(all_custom, custom_buckets);

        let mut custom_buf = Vec::new();
        enc_v1.custom_buckets_histogram_samples(&custom_buckets, &mut custom_buf);
        let mut decoded = Vec::new();
        dec.histogram_samples(&custom_buf, &mut decoded).unwrap();
        assert_eq!(all_custom, decoded);
    }

    {
        let enc_v1 = Encoder::default();
        let all_custom_float =
            vec![fhs(56, 1234, histograms[2].h.to_float()), fhs(67, 5678, histograms[2].h.to_float())];
        let mut float_buf = Vec::new();
        let custom_buckets_float = enc_v1.float_histogram_samples(&all_custom_float, &mut float_buf);
        assert!(
            float_buf.is_empty(),
            "regular float histogram buffer must be empty when all samples are custom bucket"
        );
        assert_eq!(all_custom_float, custom_buckets_float);

        let mut custom_float_buf = Vec::new();
        enc_v1.custom_buckets_float_histogram_samples(&custom_buckets_float, &mut custom_float_buf);
        let mut decoded = Vec::new();
        dec.float_histogram_samples(&custom_float_buf, &mut decoded).unwrap();
        assert_eq!(all_custom_float, decoded);
    }

    // Backward compat: V1-encoded histograms decode with ST=0.
    {
        let enc_v1 = Encoder::default();
        for h in round_trip_hist(&enc_v1, &dec, &histograms) {
            assert_eq!(0, h.st, "V1 histogram records must decode with ST=0");
        }
    }

    // Backward compat: V1-encoded float histograms decode with ST=0.
    {
        let enc_v1 = Encoder::default();
        for h in round_trip_float(&enc_v1, &dec, &float_histograms) {
            assert_eq!(0, h.st, "V1 float histogram records must decode with ST=0");
        }
    }
}

#[test]
fn test_record_min_valid_time() {
    let enc = Encoder::default();
    let dec = Decoder::default();

    for mint in [0i64, 1, -1, 12345, i64::MIN, i64::MAX] {
        let encoded = enc_bytes(|b| enc.min_valid_time(mint, b));
        assert_eq!(Type::MinValidTime, dec.r#type(&encoded));
        let decoded = dec.min_valid_time(&encoded).unwrap();
        assert_eq!(mint, decoded);
    }
}

#[test]
fn test_record_min_valid_time_decode_wrong_type() {
    let enc = Encoder::default();
    let dec = Decoder::default();
    assert!(dec.min_valid_time(&enc_bytes(|b| enc.series(&[], b))).is_err());
}

#[test]
fn test_record_v1_mixed_regular_and_custom_bucket_histogram_permutations() {
    let dec = Decoder::new(Logger::nop());

    let regular_a = Arc::new(regular_a(18.4));
    let regular_b = Arc::new(regular_b());
    let custom = Arc::new(custom_h(42.0));

    let test_cases: [(&str, &[&str]); 4] = [
        ("regular then custom", &["regularA", "custom"]),
        ("custom then regular", &["custom", "regularA"]),
        ("regular custom regular", &["regularA", "custom", "regularB"]),
        ("custom regular custom", &["custom", "regularA", "custom"]),
    ];

    let build_int_samples = |kinds: &[&str]| -> Vec<RefHistogramSample> {
        kinds
            .iter()
            .enumerate()
            .map(|(i, kind)| {
                let h = match *kind {
                    "regularA" => regular_a.clone(),
                    "regularB" => regular_b.clone(),
                    "custom" => custom.clone(),
                    k => panic!("unknown histogram kind {k:?}"),
                };
                RefHistogramSample { r#ref: 100 + i as u64 * 11, st: 0, t: 1000 + i as i64 * 250, h }
            })
            .collect()
    };

    for (name, kinds) in test_cases {
        let enc = Encoder::default();

        let int_samples = build_int_samples(kinds);
        let (want_custom_int, want_regular_int): (Vec<_>, Vec<_>) =
            int_samples.iter().cloned().partition(|s| s.h.uses_custom_buckets());

        let mut hist_buf = Vec::new();
        let custom_int = enc.histogram_samples(&int_samples, &mut hist_buf);
        let mut custom_buf = Vec::new();
        enc.custom_buckets_histogram_samples(&custom_int, &mut custom_buf);

        let mut got_regular_int = Vec::new();
        dec.histogram_samples(&hist_buf, &mut got_regular_int).unwrap();
        let mut got_custom_int = Vec::new();
        dec.histogram_samples(&custom_buf, &mut got_custom_int).unwrap();

        assert_eq!(want_regular_int, got_regular_int, "{name}");
        assert_eq!(want_custom_int, got_custom_int, "{name}");

        let float_samples: Vec<RefFloatHistogramSample> =
            int_samples.iter().map(|s| fhs(s.r#ref, s.t, s.h.to_float())).collect();
        let (want_custom_float, want_regular_float): (Vec<_>, Vec<_>) =
            float_samples.iter().cloned().partition(|s| s.fh.uses_custom_buckets());

        let mut float_buf = Vec::new();
        let custom_float = enc.float_histogram_samples(&float_samples, &mut float_buf);
        let mut custom_float_buf = Vec::new();
        enc.custom_buckets_float_histogram_samples(&custom_float, &mut custom_float_buf);

        let mut got_regular_float = Vec::new();
        dec.float_histogram_samples(&float_buf, &mut got_regular_float).unwrap();
        let mut got_custom_float = Vec::new();
        dec.float_histogram_samples(&custom_float_buf, &mut got_custom_float).unwrap();

        assert_eq!(want_regular_float, got_regular_float, "{name}");
        assert_eq!(want_custom_float, got_custom_float, "{name}");
    }
}

#[test]
fn test_record_v2_histogram_round_trip() {
    let dec = Decoder::new(Logger::nop());

    let regular_a = Arc::new(regular_a(18.4));
    let regular_b = Arc::new(regular_b());
    let custom = Arc::new(custom_h(42.0));

    let enc = Encoder { enable_st_storage: true };

    let int_samples = vec![
        RefHistogramSample { r#ref: 100, t: 1000, st: 1000, h: regular_a },
        RefHistogramSample { r#ref: 111, t: 1250, st: 1000, h: custom },
        RefHistogramSample { r#ref: 122, t: 1500, st: 1234, h: regular_b },
    ];

    let mut hist_buf = Vec::new();
    let left_over = enc.histogram_samples(&int_samples, &mut hist_buf);
    assert!(left_over.is_empty(), "V2 must not return a separate custom-bucket slice");
    assert_eq!(
        Type::HistogramSamplesV2,
        dec.r#type(&hist_buf),
        "V2 must emit a single HistogramSamplesV2 record for mixed inputs"
    );

    let mut got_int = Vec::new();
    dec.histogram_samples(&hist_buf, &mut got_int).unwrap();
    assert_eq!(int_samples, got_int);

    let float_samples: Vec<_> = int_samples.iter().map(to_float).collect();

    let mut float_buf = Vec::new();
    let left_over_float = enc.float_histogram_samples(&float_samples, &mut float_buf);
    assert!(left_over_float.is_empty());
    assert_eq!(
        Type::FloatHistogramSamplesV2,
        dec.r#type(&float_buf),
        "V2 must emit a single FloatHistogramSamplesV2 record for mixed inputs"
    );

    let mut got_float = Vec::new();
    dec.float_histogram_samples(&float_buf, &mut got_float).unwrap();
    assert_eq!(float_samples, got_float);
}

const SKIP_MSG: &str = "skipping histogram with unknown schema in WAL record";

#[test]
fn test_record_decode_invalid_histogram_schema() {
    for enable_st_storage in [false, true] {
        for schema in [-100i32, 100] {
            let enc = Encoder { enable_st_storage };
            let (dec, output) = logging_decoder();
            let histograms = vec![hs(56, 1234, Histogram { schema, ..regular_a(18.4 * rand_f64()) })];
            let mut hist_samples = Vec::new();
            enc.histogram_samples(&histograms, &mut hist_samples);
            let mut dec_histograms = Vec::new();
            dec.histogram_samples(&hist_samples, &mut dec_histograms).unwrap();
            assert!(dec_histograms.is_empty(), "schema={schema},stStorage={enable_st_storage}");
            assert!(output.lock().unwrap().contains(SKIP_MSG), "schema={schema},stStorage={enable_st_storage}");
        }
    }
}

fn regular_a_float(sum: f64, schema: i32) -> FloatHistogram {
    FloatHistogram {
        count: 5.0,
        zero_count: 2.0,
        zero_threshold: 0.001,
        sum,
        schema,
        positive_spans: spans(&[(0, 2), (1, 2)]),
        positive_buckets: vec![1.0, 1.0, -1.0, 0.0],
        ..Default::default()
    }
}

#[test]
fn test_record_decode_invalid_float_histogram_schema() {
    for enable_st_storage in [false, true] {
        for schema in [-100i32, 100] {
            let enc = Encoder { enable_st_storage };
            let (dec, output) = logging_decoder();
            let histograms = vec![fhs(56, 1234, regular_a_float(18.4 * rand_f64(), schema))];
            let mut hist_samples = Vec::new();
            enc.float_histogram_samples(&histograms, &mut hist_samples);
            let mut dec_histograms = Vec::new();
            dec.float_histogram_samples(&hist_samples, &mut dec_histograms).unwrap();
            assert!(dec_histograms.is_empty(), "schema={schema},stStorage={enable_st_storage}");
            assert!(output.lock().unwrap().contains(SKIP_MSG), "schema={schema},stStorage={enable_st_storage}");
        }
    }
}

#[test]
fn test_record_decode_v2_unknown_first_histogram_schema() {
    let enc = Encoder { enable_st_storage: true };
    let (dec, output) = logging_decoder();
    let histograms = vec![
        RefHistogramSample {
            r#ref: 56,
            st: 1000,
            t: 1234,
            h: Arc::new(Histogram { schema: -100, ..regular_a(18.4) }), // "unknown" schema
        },
        RefHistogramSample { r#ref: 42, st: 1000, t: 5678, h: Arc::new(regular_b()) },
    ];
    let mut hist_samples = Vec::new();
    enc.histogram_samples(&histograms, &mut hist_samples);
    let mut dec_histograms = Vec::new();
    dec.histogram_samples(&hist_samples, &mut dec_histograms).unwrap();
    assert_eq!(&histograms[1..], &dec_histograms[..]);
    // Ensure that the schema ID above is actually unknown. If this fails then someone started using that value.
    assert!(output.lock().unwrap().contains(SKIP_MSG));
}

#[test]
fn test_record_decode_v2_unknown_first_float_histogram_schema() {
    let enc = Encoder { enable_st_storage: true };
    let (dec, output) = logging_decoder();
    let histograms = vec![
        RefFloatHistogramSample { r#ref: 56, st: 1000, t: 1234, fh: Arc::new(regular_a_float(18.4, -100)) },
        RefFloatHistogramSample {
            r#ref: 42,
            st: 1000,
            t: 5678,
            fh: Arc::new(FloatHistogram {
                count: 11.0,
                zero_count: 4.0,
                zero_threshold: 0.001,
                sum: 35.5,
                schema: 1,
                positive_spans: spans(&[(0, 2), (2, 2)]),
                positive_buckets: vec![1.0, 1.0, -1.0, 0.0],
                negative_spans: spans(&[(0, 1), (1, 2)]),
                negative_buckets: vec![1.0, 2.0, -1.0],
                ..Default::default()
            }),
        },
    ];
    let mut hist_samples = Vec::new();
    enc.float_histogram_samples(&histograms, &mut hist_samples);
    let mut dec_histograms = Vec::new();
    dec.float_histogram_samples(&hist_samples, &mut dec_histograms).unwrap();
    assert_eq!(&histograms[1..], &dec_histograms[..]);
    assert!(output.lock().unwrap().contains(SKIP_MSG));
}

#[test]
fn test_record_decode_too_high_resolution_histogram_schema() {
    for enable_st_storage in [false, true] {
        for schema in [9i32, 52] {
            let enc = Encoder { enable_st_storage };
            let (dec, _output) = logging_decoder();
            let histograms = vec![hs(56, 1234, Histogram { schema, ..regular_a(18.4 * rand_f64()) })];
            let mut hist_samples = Vec::new();
            enc.histogram_samples(&histograms, &mut hist_samples);
            let mut dec_histograms = Vec::new();
            dec.histogram_samples(&hist_samples, &mut dec_histograms).unwrap();
            assert_eq!(1, dec_histograms.len());
            assert_eq!(EXPONENTIAL_SCHEMA_MAX, dec_histograms[0].h.schema);
        }
    }
}

#[test]
fn test_record_decode_too_high_resolution_float_histogram_schema() {
    for enable_st_storage in [false, true] {
        for schema in [9i32, 52] {
            let enc = Encoder { enable_st_storage };
            let (dec, _output) = logging_decoder();
            let histograms = vec![fhs(56, 1234, regular_a_float(18.4 * rand_f64(), schema))];
            let mut hist_samples = Vec::new();
            enc.float_histogram_samples(&histograms, &mut hist_samples);
            let mut dec_histograms = Vec::new();
            dec.float_histogram_samples(&hist_samples, &mut dec_histograms).unwrap();
            assert_eq!(1, dec_histograms.len());
            assert_eq!(EXPONENTIAL_SCHEMA_MAX, dec_histograms[0].fh.schema);
        }
    }
}

/// Go `b[:8]`. adapted: Go may reslice past the length up to the capacity (a fresh 1-byte append has capacity
/// 8, zero filled); `resize` gives the same bytes.
fn trunc8(mut b: Vec<u8>) -> Vec<u8> {
    b.resize(8, 0);
    b
}

// TestRecord_Corrupted ensures that corrupted records return the correct error.
// Bugfix check for pull/521 and pull/523.
#[test]
fn test_record_corrupted() {
    for enable_st_storage in [false, true] {
        let enc = Encoder { enable_st_storage };
        let mut dec = Decoder::new(Logger::nop());

        // Test corrupted series record.
        {
            let series = vec![RefSeries { r#ref: 100, labels: from_strings(&["abc", "def", "123", "456"]) }];
            let corrupted = trunc8(enc_bytes(|b| enc.series(&series, b)));
            let err = dec.series(&corrupted, &mut Vec::new()).unwrap_err();
            assert_eq!(err, tsdb_format::record::Error::Encoding(encoding::Error::InvalidSize));
        }

        // Test corrupted sample record.
        {
            let samples = vec![RefSample { r#ref: 0, st: 0, t: 12423423, v: 1.2345 }];
            let corrupted = trunc8(enc_bytes(|b| enc.samples(&samples, b)));
            let err = dec.samples(&corrupted, &mut Vec::new()).unwrap_err();
            assert!(err.is(&encoding::Error::InvalidSize), "{err}");
        }

        // Test corrupted tombstone record.
        {
            let tstones =
                vec![Stone { r#ref: 123, intervals: vec![Interval::new(-1000, 1231231), Interval::new(5000, 0)] }];
            let corrupted = trunc8(enc_bytes(|b| enc.tombstones(&tstones, b)));
            let err = dec.tombstones(&corrupted, &mut Vec::new()).unwrap_err();
            assert_eq!(err, tsdb_format::record::Error::Encoding(encoding::Error::InvalidSize));
        }

        // Test corrupted exemplar record.
        {
            let exemplars =
                vec![RefExemplar { r#ref: 0, t: 12423423, v: 1.2345, labels: from_strings(&["trace_id", "asdf"]) }];
            let corrupted = trunc8(enc_bytes(|b| enc.exemplars(&exemplars, b)));
            let err = dec.exemplars(&corrupted, &mut Vec::new()).unwrap_err();
            assert!(err.is(&encoding::Error::InvalidSize), "{err}");
        }

        // Test corrupted metadata record.
        {
            let meta = vec![RefMetadata {
                r#ref: 147,
                r#type: MetricType::Counter as u8,
                unit: "unit".into(),
                help: "help".into(),
            }];
            let corrupted = trunc8(enc_bytes(|b| enc.metadata(&meta, b)));
            let err = dec.metadata(&corrupted, &mut Vec::new()).unwrap_err();
            assert!(err.is(&encoding::Error::InvalidSize), "{err}");
        }

        // Test corrupted histogram record.
        {
            let histograms = vec![hs(56, 1234, regular_a(18.4 * rand_f64())), hs(67, 5678, custom_h(35.5))];
            let mut corrupted_hists = Vec::new();
            let custom_buckets_hists = enc.histogram_samples(&histograms, &mut corrupted_hists);
            let corrupted_hists = trunc8(corrupted_hists);
            let corrupted_custom_buckets_hists =
                trunc8(enc_bytes(|b| enc.custom_buckets_histogram_samples(&custom_buckets_hists, b)));
            let err = dec.histogram_samples(&corrupted_hists, &mut Vec::new()).unwrap_err();
            assert!(err.is(&encoding::Error::InvalidSize), "{err}");
            let err = dec.histogram_samples(&corrupted_custom_buckets_hists, &mut Vec::new()).unwrap_err();
            assert!(err.is(&encoding::Error::InvalidSize), "{err}");
        }
    }
}

#[test]
fn test_record_type() {
    let mut enc = Encoder::default();
    let dec = Decoder::default();

    let series = vec![RefSeries { r#ref: 100, labels: from_strings(&["abc", "123"]) }];
    assert_eq!(Type::Series, dec.r#type(&enc_bytes(|b| enc.series(&series, b))));

    let samples = vec![RefSample { r#ref: 123, st: 0, t: 12345, v: 1.2345 }];
    assert_eq!(Type::Samples, dec.r#type(&enc_bytes(|b| enc.samples(&samples, b))));

    // With EnableSTStorage set, all Samples are V2.
    enc = Encoder { enable_st_storage: true };
    let samples = vec![RefSample { r#ref: 123, st: 0, t: 12345, v: 1.2345 }];
    assert_eq!(Type::SamplesV2, dec.r#type(&enc_bytes(|b| enc.samples(&samples, b))));

    let samples_st = vec![RefSample { r#ref: 123, st: 1, t: 12345, v: 1.2345 }];
    assert_eq!(Type::SamplesV2, dec.r#type(&enc_bytes(|b| enc.samples(&samples_st, b))));

    let tstones = vec![Stone { r#ref: 1, intervals: vec![Interval::new(1, 2)] }];
    assert_eq!(Type::Tombstones, dec.r#type(&enc_bytes(|b| enc.tombstones(&tstones, b))));

    let metadata =
        vec![RefMetadata { r#ref: 147, r#type: MetricType::Counter as u8, unit: "unit".into(), help: "help".into() }];
    assert_eq!(Type::Metadata, dec.r#type(&enc_bytes(|b| enc.metadata(&metadata, b))));

    let histograms = vec![hs(56, 1234, regular_a(18.4 * rand_f64())), hs(67, 5678, custom_h(35.5))];
    // V1 histogram type recognition (requires EnableSTStorage off).
    enc = Encoder::default();
    let mut hists = Vec::new();
    let custom_buckets_histograms = enc.histogram_samples(&histograms, &mut hists);
    assert_eq!(Type::HistogramSamples, dec.r#type(&hists));
    let custom_buckets_hists = enc_bytes(|b| enc.custom_buckets_histogram_samples(&custom_buckets_histograms, b));
    assert_eq!(Type::CustomBucketsHistogramSamples, dec.r#type(&custom_buckets_hists));

    // V2 histogram type recognition.
    enc = Encoder { enable_st_storage: true };
    let mut hists = Vec::new();
    let left_over = enc.histogram_samples(&histograms, &mut hists);
    assert!(left_over.is_empty());
    assert_eq!(Type::HistogramSamplesV2, dec.r#type(&hists));

    // V2 float-histogram type recognition.
    let float_histograms: Vec<_> = histograms.iter().map(|h| fhs(h.r#ref, h.t, h.h.to_float())).collect();
    let mut float_hists = Vec::new();
    let left_over_float = enc.float_histogram_samples(&float_histograms, &mut float_hists);
    assert!(left_over_float.is_empty());
    assert_eq!(Type::FloatHistogramSamplesV2, dec.r#type(&float_hists));

    assert_eq!(Type::Unknown, dec.r#type(&[]));
    assert_eq!(Type::Unknown, dec.r#type(&[0]));
}

#[test]
fn test_record_metadata_decode_unknown_extra_fields() {
    let mut enc = Encbuf::new();
    let dec = Decoder::default();

    // Write record type.
    enc.put_byte(Type::Metadata as u8);

    // Write first metadata entry, all known fields.
    enc.put_uvarint64(101);
    enc.put_byte(MetricType::Counter as u8);
    enc.put_uvarint(2);
    enc.put_uvarint_str("UNIT");
    enc.put_uvarint_str("");
    enc.put_uvarint_str("HELP");
    enc.put_uvarint_str("some magic counter");

    // Write second metadata entry, known fields + unknown fields.
    enc.put_uvarint64(99);
    enc.put_byte(MetricType::Counter as u8);
    enc.put_uvarint(3);
    // Known fields.
    enc.put_uvarint_str("UNIT");
    enc.put_uvarint_str("seconds");
    enc.put_uvarint_str("HELP");
    enc.put_uvarint_str("CPU time counter");
    // Unknown fields.
    enc.put_uvarint_str("an extra field name to be skipped");
    enc.put_uvarint_str("with its value");

    // Write third metadata entry, with unknown fields and different order.
    enc.put_uvarint64(47250);
    enc.put_byte(MetricType::Gauge as u8);
    enc.put_uvarint(4);
    enc.put_uvarint_str("extra name one");
    enc.put_uvarint_str("extra value one");
    enc.put_uvarint_str("HELP");
    enc.put_uvarint_str("current memory usage");
    enc.put_uvarint_str("extra name two");
    enc.put_uvarint_str("extra value two");
    enc.put_uvarint_str("UNIT");
    enc.put_uvarint_str("percentage");

    // Should yield known fields for all entries and skip over unknown fields.
    let expected_metadata = vec![
        RefMetadata {
            r#ref: 101,
            r#type: MetricType::Counter as u8,
            unit: "".into(),
            help: "some magic counter".into(),
        },
        RefMetadata {
            r#ref: 99,
            r#type: MetricType::Counter as u8,
            unit: "seconds".into(),
            help: "CPU time counter".into(),
        },
        RefMetadata {
            r#ref: 47250,
            r#type: MetricType::Gauge as u8,
            unit: "percentage".into(),
            help: "current memory usage".into(),
        },
    ];

    let mut dec_metadata = Vec::new();
    dec.metadata(enc.get(), &mut dec_metadata).unwrap();
    assert_eq!(expected_metadata, dec_metadata);
}
