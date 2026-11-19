//! Port of `prompb/io/prometheus/write/v2/custom_test.go`.

use remote::prompb::writev2::{Exemplar, Request, Sample, TimeSeries};
use remote::prompb::Message;

#[test]
fn test_optimized_marshal() {
    let refs: Vec<u32> = (0..16).collect();
    let cases = vec![
        ("empty", Request::default()),
        (
            "simple",
            Request {
                timeseries: vec![
                    TimeSeries {
                        labels_refs: refs.clone(),
                        samples: vec![Sample { value: 1.0, timestamp: 0, ..Default::default() }],
                        exemplars: vec![Exemplar { labels_refs: vec![0, 1], value: 1.0, timestamp: 0 }],
                        histograms: vec![],
                        ..Default::default()
                    },
                    TimeSeries {
                        labels_refs: refs.clone(),
                        samples: vec![Sample { value: 2.0, timestamp: 1, ..Default::default() }],
                        exemplars: vec![Exemplar { labels_refs: vec![0, 1], value: 2.0, timestamp: 1 }],
                        histograms: vec![],
                        ..Default::default()
                    },
                ],
                symbols: ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
            },
        ),
    ];
    for (name, m) in cases {
        // Keep the slice allocated to mimic what std Marshal would give to sized Marshal.
        let mut got = Vec::new();

        // Should be the same as the standard marshal.
        let expected = m.marshal();
        m.optimized_marshal(&mut got);
        assert_eq!(expected, got, "{name}");

        // Unmarshal should work too.
        let m2 = Request::unmarshal(&got).expect("unmarshal");
        assert_eq!(m, m2, "{name}");
    }
}
