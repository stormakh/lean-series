//! Port of `prompb/io/prometheus/client/decoder_test.go`. The decoder itself lives in
//! `textparse::client_pb` (the scrape protobuf parser uses it).
//!
//! adapted: Go builds the input with `proto.UnmarshalText` + `proto.Marshal` of `MetricFamily`. Here the same
//! messages are encoded with a small gogo-compatible encoder (fields in number order, zero scalars skipped),
//! which gives the same bytes.

use model::labels::{ScratchBuilder, ValidationScheme};
use remote::prompb::proto::*;
use textparse::client_pb::{MetricStreamingDecoder, MetricType};

struct Ex<'a> {
    labels: &'a [(&'a str, &'a str)],
    value: f64,
    ts: (i64, i32),
}

struct Metric<'a> {
    labels: Vec<(String, String)>,
    gauge: Option<f64>,
    counter: Option<(f64, Option<Ex<'a>>)>,
}

struct Family<'a> {
    name: String,
    help: String,
    typ: i32,
    unit: &'a str,
    metrics: Vec<Metric<'a>>,
}

fn label_pair(name: &str, value: &str) -> Vec<u8> {
    let mut b = Vec::new();
    put_bytes_field(&mut b, 1, name.as_bytes());
    put_bytes_field(&mut b, 2, value.as_bytes());
    b
}

fn sub(buf: &mut Vec<u8>, field: u32, m: &[u8]) {
    put_message_header(buf, field, m.len());
    buf.extend_from_slice(m);
}

fn encode_family(f: &Family) -> Vec<u8> {
    let mut b = Vec::new();
    put_bytes_field(&mut b, 1, f.name.as_bytes());
    put_bytes_field(&mut b, 2, f.help.as_bytes());
    put_int_field(&mut b, 3, f.typ as i64);
    for m in &f.metrics {
        let mut mb = Vec::new();
        for (n, v) in &m.labels {
            sub(&mut mb, 1, &label_pair(n, v));
        }
        if let Some(g) = m.gauge {
            let mut gb = Vec::new();
            put_double_field(&mut gb, 1, g);
            sub(&mut mb, 2, &gb);
        }
        if let Some((v, ex)) = &m.counter {
            let mut cb = Vec::new();
            put_double_field(&mut cb, 1, *v);
            if let Some(ex) = ex {
                let mut eb = Vec::new();
                for (n, v) in ex.labels {
                    sub(&mut eb, 1, &label_pair(n, v));
                }
                put_double_field(&mut eb, 2, ex.value);
                let mut tb = Vec::new();
                put_int_field(&mut tb, 1, ex.ts.0);
                put_int_field(&mut tb, 2, ex.ts.1 as i64);
                sub(&mut eb, 3, &tb);
                sub(&mut cb, 2, &eb);
            }
            sub(&mut mb, 3, &cb);
        }
        sub(&mut b, 4, &mb);
    }
    put_bytes_field(&mut b, 5, f.unit.as_bytes());
    b
}

fn l(n: &str, v: &str) -> (String, String) {
    (n.to_string(), v.to_string())
}

fn test_gauge() -> Family<'static> {
    Family {
        name: "go_build_info".into(),
        help: "Build information about the main Go module.".into(),
        typ: MetricType::GAUGE.0,
        unit: "",
        metrics: vec![
            Metric {
                labels: vec![l("checksum", ""), l("path", "github.com/prometheus/client_golang"), l("version", "(devel)")],
                gauge: Some(1.0),
                counter: None,
            },
            Metric {
                labels: vec![l("checksum", ""), l("path", "github.com/prometheus/prometheus"), l("version", "v3.0.0")],
                gauge: Some(2.0),
                counter: None,
            },
        ],
    }
}

fn test_counter() -> Family<'static> {
    Family {
        name: "go_memstats_alloc_bytes_total".into(),
        help: "Total number of bytes allocated, even if freed.".into(),
        typ: MetricType::COUNTER.0,
        unit: "bytes",
        metrics: vec![Metric {
            labels: vec![],
            gauge: None,
            counter: Some((1.546544e+06, Some(Ex { labels: &[("dummyID", "42")], value: 12.0, ts: (1625851151, 233181499) }))),
        }],
    }
}

fn delimited(buf: &mut Vec<u8>, f: &Family) {
    let pb = encode_family(f);
    put_varint(buf, pb.len() as u64);
    buf.extend_from_slice(&pb);
}

fn next(d: &mut MetricStreamingDecoder) -> std::result::Result<(), Option<String>> {
    loop {
        match d.next_metric() {
            Err(None) => {
                d.next_metric_family().map_err(|e| e.map(|e| e.to_string()))?;
                continue;
            }
            Err(Some(e)) => return Err(Some(e.to_string())),
            Ok(()) => return Ok(()),
        }
    }
}

#[test]
fn test_metric_streaming_decoder() {
    let mut buf = Vec::new();
    for f in [test_gauge(), test_counter()] {
        delimited(&mut buf, &f);
    }

    let mut d = MetricStreamingDecoder::new(&buf);
    assert!(d.next_metric_family().is_ok());

    let first_metric_lset;
    {
        next(&mut d).unwrap();
        assert_eq!(b"go_build_info", d.get_name());
        assert_eq!(b"Build information about the main Go module.", d.get_help());
        assert_eq!(MetricType::GAUGE, d.get_type());
        assert_eq!(1.0, d.gauge.value);
        let mut b = ScratchBuilder::new(0);
        d.label(&mut b).unwrap();
        first_metric_lset = b.labels();
        assert_eq!(
            r#"{checksum="", path="github.com/prometheus/client_golang", version="(devel)"}"#,
            first_metric_lset.to_string()
        );
    }
    {
        next(&mut d).unwrap();
        assert_eq!(b"go_build_info", d.get_name());
        assert_eq!(b"Build information about the main Go module.", d.get_help());
        assert_eq!(MetricType::GAUGE, d.get_type());
        assert_eq!(2.0, d.gauge.value);
        let mut b = ScratchBuilder::new(0);
        d.label(&mut b).unwrap();
        assert_eq!(r#"{checksum="", path="github.com/prometheus/prometheus", version="v3.0.0"}"#, b.labels().to_string());
    }
    {
        // Different mf now.
        next(&mut d).unwrap();
        assert_eq!(b"go_memstats_alloc_bytes_total", d.get_name());
        assert_eq!(b"Total number of bytes allocated, even if freed.", d.get_help());
        assert_eq!(b"bytes", d.get_unit());
        assert_eq!(MetricType::COUNTER, d.get_type());
        assert_eq!(1.546544e+06, d.counter.value);
        // adapted: Go sets `SetUnsafeAdd(true)`; the Rust builder always copies.
        let mut b = ScratchBuilder::new(0);
        d.label(&mut b).unwrap();
        assert_eq!("{}", b.labels().to_string());
    }
    assert_eq!(Err(None), next(&mut d));

    // Expect labels and metricBytes to be static and reusable even after parsing.
    assert_eq!(
        r#"{checksum="", path="github.com/prometheus/client_golang", version="(devel)"}"#,
        first_metric_lset.to_string()
    );
}

/// A small xorshift generator in place of Go's `math/rand` (the values only need to vary).
struct Rng(u64);
impl Rng {
    fn intn(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

fn generate_metric_family(rng: &mut Rng, labels_count: usize) -> Family<'static> {
    let name = format!("metric_{}", rng.intn(1000));
    let help = format!("Test metric to demonstrate forced corruption {}.", rng.intn(1000));
    let mut labels = Vec::new();
    for _ in 0..labels_count {
        labels.push((format!("instance_{}", rng.intn(1000)), format!("value_{}", rng.intn(1000))));
    }
    Family { name, help, typ: MetricType::GAUGE.0, unit: "", metrics: vec![Metric { labels, gauge: Some(1.0), counter: None }] }
}

// Regression test against https://github.com/prometheus/prometheus/pull/16946
#[test]
fn test_metric_streaming_decoder_labels_corruption() {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut all_previous_labels = Vec::new();
    // adapted: Go reuses scrape buffers from a pool to provoke reuse of the bytes the labels point to. Here one
    // buffer is reused (cleared) for every round, which is the same hazard.
    let mut b: Vec<u8> = Vec::with_capacity(128);
    let mut builder = ScratchBuilder::new(0);

    for labels_count in [1usize, 2, 3, 5, 8, 5, 3, 2, 1] {
        b.clear();
        let mf = generate_metric_family(&mut rng, labels_count);
        delimited(&mut b, &mf);

        let mut d = MetricStreamingDecoder::new(&b);
        assert!(d.next_metric_family().is_ok());
        assert!(d.next_metric().is_ok());

        builder.reset();
        d.label(&mut builder).unwrap();
        let lbs = builder.labels();
        all_previous_labels.push(lbs);

        // Validate all labels seen so far remain valid and not corrupted.
        for l in &all_previous_labels {
            assert!(l.is_valid(ValidationScheme::Legacy), "encountered corrupted labels: {l:?}");
        }
    }
}
