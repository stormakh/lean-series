//! Port of upstream/prompb/io/prometheus/client/decoder_test.go (the ledger lists it under crate `remote`; the
//! decoder lives in textparse::client_pb). Inputs are made by scripts/gen/textparse from the Go test sources.

use model::labels::{Labels, ScratchBuilder, ValidationScheme};
use textparse::client_pb::{DecodeError, MetricStreamingDecoder, MetricType};

fn data(name: &str) -> Vec<u8> {
    let p = format!("{}/../../tests-data/textparse/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&p).unwrap_or_else(|e| panic!("read {p}: {e}"))
}

#[test]
fn test_metric_streaming_decoder() {
    // testGauge and testCounter, varint-delimited.
    let buf = data("decoder_test/TestMetricStreamingDecoder.bin");

    let mut d = MetricStreamingDecoder::new(&buf);
    assert_eq!(Ok(()), d.next_metric_family());
    // Err(None) is io.EOF.
    let next_fn = |d: &mut MetricStreamingDecoder<'_>| -> Result<(), Option<DecodeError>> {
        loop {
            match d.next_metric() {
                Err(None) => {
                    d.next_metric_family()?;
                    continue;
                }
                r => return r,
            }
        }
    };

    let first_metric_lset: Labels;
    {
        assert_eq!(Ok(()), next_fn(&mut d));

        assert_eq!(b"go_build_info", d.get_name());
        assert_eq!(b"Build information about the main Go module.", d.get_help());
        assert_eq!(MetricType::GAUGE, d.get_type());

        assert_eq!(1.0, d.gauge.value);
        let mut b = ScratchBuilder::new(0);
        assert_eq!(Ok(()), d.label(&mut b));

        first_metric_lset = b.labels();

        assert_eq!(
            r#"{checksum="", path="github.com/prometheus/client_golang", version="(devel)"}"#,
            first_metric_lset.to_string()
        );
    }

    {
        assert_eq!(Ok(()), next_fn(&mut d));

        assert_eq!(b"go_build_info", d.get_name());
        assert_eq!(b"Build information about the main Go module.", d.get_help());
        assert_eq!(MetricType::GAUGE, d.get_type());

        assert_eq!(2.0, d.gauge.value);
        let mut b = ScratchBuilder::new(0);
        assert_eq!(Ok(()), d.label(&mut b));
        assert_eq!(r#"{checksum="", path="github.com/prometheus/prometheus", version="v3.0.0"}"#, b.labels().to_string());
    }
    {
        // Different mf now.
        assert_eq!(Ok(()), next_fn(&mut d));

        assert_eq!(b"go_memstats_alloc_bytes_total", d.get_name());
        assert_eq!(b"Total number of bytes allocated, even if freed.", d.get_help());
        assert_eq!(b"bytes", d.get_unit());
        assert_eq!(MetricType::COUNTER, d.get_type());

        assert_eq!(1.546544e+06, d.counter.value);
        // adapted: Go sets SetUnsafeAdd(true) here; the Rust ScratchBuilder always copies the strings.
        let mut b = ScratchBuilder::new(0);

        assert_eq!(Ok(()), d.label(&mut b));
        assert_eq!("{}", b.labels().to_string());
    }
    assert_eq!(Err(None), next_fn(&mut d));

    // Expect labels and metricBytes to be static and reusable even after parsing.
    assert_eq!(
        r#"{checksum="", path="github.com/prometheus/client_golang", version="(devel)"}"#,
        first_metric_lset.to_string()
    );
}

// Regression test against https://github.com/prometheus/prometheus/pull/16946
#[test]
fn test_metric_streaming_decoder_labels_corruption() {
    let mut all_previous_labels: Vec<Labels> = Vec::new();
    // adapted: Go takes scrape buffers from a pool and uses the unseeded global rand for names and values; here
    // one buffer is reused for every round and the inputs come from a fixed seed (scripts/gen/textparse).
    let mut buf: Vec<u8> = Vec::new();
    let mut builder = ScratchBuilder::new(0);

    for (i, labels_count) in [1, 2, 3, 5, 8, 5, 3, 2, 1].into_iter().enumerate() {
        buf.clear();
        buf.extend_from_slice(&data(&format!(
            "decoder_test/TestMetricStreamingDecoder_LabelsCorruption__{i}_labels{labels_count}.bin"
        )));

        // Use decoder like protobufparse.rs would.
        let mut d = MetricStreamingDecoder::new(&buf);
        assert_eq!(Ok(()), d.next_metric_family());
        assert_eq!(Ok(()), d.next_metric());

        // Get the labels. Decode is reusing strings when adding labels.
        builder.reset();
        assert_eq!(Ok(()), d.label(&mut builder));
        let lbs = builder.labels();
        all_previous_labels.push(lbs);

        // Validate all labels seen so far remain valid and not corrupted.
        for l in &all_previous_labels {
            assert!(l.is_valid(ValidationScheme::Legacy), "encountered corrupted labels: {l}");
        }
    }
}
