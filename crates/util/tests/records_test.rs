//! Port of `util/testwal/records_test.go` (the code is `tsdb_format::testwal`; the benchmark is not ported).

use tsdb_format::testwal::{RecordsCase, generate_records};

#[test]
fn test_generate_records_start_timestamps() {
    for (name, no_st) in [("with start timestamp", false), ("without start timestamp", true)] {
        let recs = generate_records(&RecordsCase {
            no_st,
            series: 1,
            samples_per_series: 2,
            histograms_per_series: 2,
            float_histograms_per_series: 2,
            ts_fn: Some(Box::new(|_, j| 100 + j as i64)),
            ..Default::default()
        });
        let want_st = |ts: i64| if no_st { 0 } else { ts - 1 };

        assert_eq!(recs.samples.len(), 2, "{name}");
        for s in &recs.samples {
            assert_eq!(want_st(s.t), s.st, "{name}");
        }
        assert_eq!(recs.histograms.len(), 2, "{name}");
        for h in &recs.histograms {
            assert_eq!(want_st(h.t), h.st, "{name}");
        }
        assert_eq!(recs.float_histograms.len(), 2, "{name}");
        for fh in &recs.float_histograms {
            assert_eq!(want_st(fh.t), fh.st, "{name}");
        }
    }
}
