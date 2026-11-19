//! Port of `prompb/io/prometheus/write/v2/types_test.go`.

use std::time::{SystemTime, UNIX_EPOCH};

use remote::prompb::writev2::{Request, Sample as V2Sample, TimeSeries as V2TimeSeries};
use remote::prompb::{Label, Message, Sample, TimeSeries, WriteRequest};

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

#[test]
fn test_interop_v2_unmarshal_with_v1_deterministic_empty() {
    let expected_v1_empty = WriteRequest::default();
    let cases = vec![
        // Technically wrong, should be at least empty string in symbol.
        Request::default(),
        // NOTE: Without reserved fields, failed with "corrupted" ghost TimeSeries element.
        Request { symbols: vec!["".into()], ..Default::default() },
        // NOTE: Without reserved fields, proto: illegal wireType 7
        Request {
            symbols: vec!["".into(), "__name__".into(), "metric1".into()],
            timeseries: vec![
                V2TimeSeries { labels_refs: vec![1, 2], ..Default::default() },
                V2TimeSeries {
                    samples: vec![V2Sample { value: 21.4, timestamp: now_ms(), ..Default::default() }],
                    ..Default::default()
                },
            ],
        },
    ];
    for incoming in cases {
        let input = incoming.marshal();
        // Test accidental unmarshal of v2 payload with v1 proto. Unknown fields are dropped (Go keeps them in
        // XXX_unrecognized, which the Go test clears before comparing).
        let out = WriteRequest::unmarshal(&input).expect("unmarshal");
        assert_eq!(expected_v1_empty, out);
    }
}

#[test]
fn test_interop_v1_unmarshal_with_v2_deterministic_empty() {
    let expected_v2_empty = Request::default();
    let cases = vec![
        WriteRequest::default(),
        // NOTE: Without reserved fields, results in corrupted v2.Request.Symbols.
        WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label::new("__name__", "metric1")],
                samples: vec![Sample { value: 21.4, timestamp: now_ms() }],
                ..Default::default()
            }],
            ..Default::default()
        },
    ];
    for incoming in cases {
        let input = incoming.marshal();
        // Test accidental unmarshal of v1 payload with v2 proto.
        let out = Request::unmarshal(&input).expect("unmarshal");
        assert_eq!(expected_v2_empty, out);
    }
}
