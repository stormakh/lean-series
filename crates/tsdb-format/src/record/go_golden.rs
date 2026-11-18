//! Byte compatibility with Go: the records below were encoded by upstream Prometheus 3.16.0 (`tsdb/record` and
//! `tsdb/tombstones`) from the same inputs. Rust must produce the same bytes and decode Go's bytes back into
//! records that encode to the same bytes again.

use std::sync::Arc;

use model::histogram::{CounterResetHint, Histogram, Span, custom_values};
use model::labels::{empty_labels, from_strings};

use super::*;
use crate::tombstones::{self, Interval, MemTombstones, Stone};

const GO: &[(&str, &str)] = &[
    (
        "series",
        "01000000000000006403085f5f6e616d655f5f02757008696e7374616e63650e6c6f63616c686f73743a39303930036a6f620a70726f6d657468657573000000000000000101016100000001000000000000",
    ),
    (
        "samples_v1",
        "0200000000000000050000018bcfe5680000003ff800000000000003cf0ffff0000000000000ce04b0ea017ff800000000000105b0ea0100000000000000000480fa014045000000000000",
    ),
    ("samples_empty_v1", "02"),
    (
        "hist_v1",
        "07000000000000003200000000000003e800000203255bba08cf8c979d0314c01d0000000000000205020801010201030a030e010614a01f0203255bba08cf8c979d0314c01d0000000000000205020801010201030a030e0106",
    ),
    (
        "hist_cb_v1",
        "09000000000000002800000000000007d000000069000000000000000000064028000000000000010003000302040100033fe00000000000003ff00000000000007ff00000000000002ad00f0069000000000000000000064028000000000000010003000302040100033fe00000000000003ff00000000000007ff0000000000000",
    ),
    (
        "fhist_v1",
        "08000000000000003200000000000003e800000203255bba08cf8c979d40080000000000004034000000000000c01d00000000000002050208010102010340140000000000004008000000000000402400000000000001400800000000000014a01f0203255bba08cf8c979d40080000000000004034000000000000c01d000000000000020502080101020103401400000000000040080000000000004024000000000000014008000000000000",
    ),
    (
        "fhist_cb_v1",
        "0a000000000000002800000000000007d000000069000000000000000000000000000000004018000000000000402800000000000001000300033ff00000000000004008000000000000400000000000000000033fe00000000000003ff00000000000007ff00000000000002ad00f0069000000000000000000000000000000004018000000000000402800000000000001000300033ff00000000000004008000000000000400000000000000000033fe00000000000003ff00000000000007ff0000000000000",
    ),
    (
        "samples_v2",
        "0b0a80a0abfef962003ff800000000000003cf0f028080f4f6905dfff0000000000000d204b0ea01017ff8000000000001d304b0ea010280a0abfef96200000000000000000a80fa01004045000000000000",
    ),
    ("samples_empty_v2", "0b"),
    (
        "hist_v2",
        "0c64d00f880e0203255bba08cf8c979d0314c01d0000000000000205020801010201030a030e010613d00f010069000000000000000000064028000000000000010003000302040100033fe00000000000003ff00000000000007ff000000000000028a01f000203255bba08cf8c979d0314c01d0000000000000205020801010201030a030e010602a01f0280190069000000000000000000064028000000000000010003000302040100033fe00000000000003ff00000000000007ff0000000000000",
    ),
    ("hist_cb_v2", "0c"),
    (
        "fhist_v2",
        "0d64d00f880e0203255bba08cf8c979d40080000000000004034000000000000c01d00000000000002050208010102010340140000000000004008000000000000402400000000000001400800000000000013d00f010069000000000000000000000000000000004018000000000000402800000000000001000300033ff00000000000004008000000000000400000000000000000033fe00000000000003ff00000000000007ff000000000000028a01f000203255bba08cf8c979d40080000000000004034000000000000c01d00000000000002050208010102010340140000000000004008000000000000402400000000000001400800000000000002a01f0280190069000000000000000000000000000000004018000000000000402800000000000001000300033ff00000000000004008000000000000400000000000000000033fe00000000000003ff00000000000007ff0000000000000",
    ),
    ("fhist_cb_v2", "0d"),
    ("metadata", "0609010204554e4954077365636f6e64730448454c500168ac02070204554e4954000448454c5000"),
    (
        "exemplars",
        "04000000000000000a000000000000000500003ff0000000000000010874726163655f696401780f0f400000000000000000",
    ),
    ("tombstones", "0300000000000000040914000000000000000428feffffffffffffffff01"),
    ("mmap", "0500000000000000010000000500001234"),
    ("minvalid", "0effffffffffffcfc7"),
    ("tombfile", "0130ba30014d13c8014dd804a006792008f2"),
];

fn go(name: &str) -> Vec<u8> {
    let h = GO.iter().find(|(n, _)| *n == name).unwrap_or_else(|| panic!("no golden {name}")).1;
    (0..h.len()).step_by(2).map(|i| u8::from_str_radix(&h[i..i + 2], 16).unwrap()).collect()
}

fn enc(f: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
    let mut b = Vec::new();
    f(&mut b);
    b
}

fn inputs() -> (Vec<RefSeries>, Vec<RefSample>, Vec<RefHistogramSample>) {
    let series = vec![
        RefSeries {
            r#ref: 100,
            labels: from_strings(&["__name__", "up", "job", "prometheus", "instance", "localhost:9090"]),
        },
        RefSeries { r#ref: 1, labels: from_strings(&["a", ""]) },
        RefSeries { r#ref: 1 << 40, labels: empty_labels() },
    ];
    let go_nan = f64::from_bits(0x7FF8_0000_0000_0001); // Go math.NaN()
    let samples = vec![
        RefSample { r#ref: 5, t: 1700000000000, st: 0, v: 1.5 },
        RefSample { r#ref: 3, t: 1699999999000, st: 1600000000000, v: f64::NEG_INFINITY },
        RefSample { r#ref: 300, t: 1700000015000, st: 1600000000000, v: go_nan },
        RefSample { r#ref: 2, t: 1700000015000, st: 1700000000000, v: 0.0 }, // Go's constant -0.0 is +0.
        RefSample { r#ref: 7, t: 1700000016000, st: 0, v: 42.0 },
    ];
    let exp = Arc::new(Histogram {
        counter_reset_hint: CounterResetHint::NotCounterReset,
        schema: -2,
        zero_threshold: 1e-128,
        zero_count: 3,
        count: 20,
        sum: -7.25,
        positive_spans: vec![Span::new(-3, 2), Span::new(4, 1)],
        positive_buckets: vec![5, -2, 7],
        negative_spans: vec![Span::new(1, 1)],
        negative_buckets: vec![3],
        ..Default::default()
    });
    let cb = Arc::new(Histogram {
        schema: -53,
        count: 6,
        sum: 12.0,
        positive_spans: vec![Span::new(0, 3)],
        positive_buckets: vec![1, 2, -1],
        custom_values: custom_values(&[0.5, 1.0, f64::INFINITY]),
        ..Default::default()
    });
    let hists = vec![
        RefHistogramSample { r#ref: 50, t: 1000, st: 900, h: exp.clone() },
        RefHistogramSample { r#ref: 40, t: 2000, st: 900, h: cb.clone() },
        RefHistogramSample { r#ref: 60, t: 3000, st: 0, h: exp },
        RefHistogramSample { r#ref: 61, t: 3000, st: 2500, h: cb },
    ];
    (series, samples, hists)
}

#[test]
fn encode_matches_go() {
    let (series, samples, hists) = inputs();
    let fhists: Vec<RefFloatHistogramSample> = hists
        .iter()
        .map(|h| RefFloatHistogramSample { r#ref: h.r#ref, st: h.st, t: h.t, fh: Arc::new(h.h.to_float()) })
        .collect();

    for (st, s) in [(false, "v1"), (true, "v2")] {
        let e = Encoder { enable_st_storage: st };
        assert_eq!(go("series"), enc(|b| e.series(&series, b)));
        assert_eq!(go(&format!("samples_{s}")), enc(|b| e.samples(&samples, b)), "samples_{s}");
        assert_eq!(go(&format!("samples_empty_{s}")), enc(|b| e.samples(&[], b)));
        let mut b = Vec::new();
        let cbs = e.histogram_samples(&hists, &mut b);
        assert_eq!(go(&format!("hist_{s}")), b, "hist_{s}");
        assert_eq!(go(&format!("hist_cb_{s}")), enc(|b| e.custom_buckets_histogram_samples(&cbs, b)));
        let mut b = Vec::new();
        let fcbs = e.float_histogram_samples(&fhists, &mut b);
        assert_eq!(go(&format!("fhist_{s}")), b, "fhist_{s}");
        assert_eq!(go(&format!("fhist_cb_{s}")), enc(|b| e.custom_buckets_float_histogram_samples(&fcbs, b)));
    }

    let e = Encoder::default();
    let meta = vec![
        RefMetadata { r#ref: 9, r#type: MetricType::Counter as u8, unit: "seconds".into(), help: "h".into() },
        RefMetadata { r#ref: 300, r#type: 7, unit: String::new(), help: String::new() },
    ];
    assert_eq!(go("metadata"), enc(|b| e.metadata(&meta, b)));
    let ex = vec![
        RefExemplar { r#ref: 10, t: 5, v: 1.0, labels: from_strings(&["trace_id", "x"]) },
        RefExemplar { r#ref: 2, t: -3, v: 2.0, labels: empty_labels() },
    ];
    assert_eq!(go("exemplars"), enc(|b| e.exemplars(&ex, b)));
    let ts = vec![Stone { r#ref: 4, intervals: vec![Interval::new(-5, 10), Interval::new(20, i64::MAX)] }];
    assert_eq!(go("tombstones"), enc(|b| e.tombstones(&ts, b)));
    let mm = vec![RefMmapMarker { r#ref: 1, mmap_ref: 0x0000_0005_0000_1234 }];
    assert_eq!(go("mmap"), enc(|b| e.mmap_markers(&mm, b)));
    assert_eq!(go("minvalid"), enc(|b| e.min_valid_time(-12345, b)));

    // Tombstones file.
    let dir = std::env::temp_dir().join(format!("tsdb-format-golden-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mt = MemTombstones::new();
    mt.add_interval(77, &[Interval::new(-10, 100), Interval::new(300, 400)]);
    let size = tombstones::write_file(&Logger::nop(), &dir, &mt).unwrap();
    let got = std::fs::read(dir.join(tombstones::TOMBSTONES_FILENAME)).unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    assert_eq!(go("tombfile"), got);
    assert_eq!(size, got.len() as i64);
}

#[test]
fn decode_go_records() {
    let (series, samples, hists) = inputs();
    let mut dec = Decoder::default();

    let mut s = Vec::new();
    dec.series(&go("series"), &mut s).unwrap();
    assert_eq!(series, s);

    for (st, name) in [(false, "samples_v1"), (true, "samples_v2")] {
        let mut got = Vec::new();
        dec.samples(&go(name), &mut got).unwrap();
        assert_eq!(got.len(), samples.len());
        for (g, w) in got.iter().zip(&samples) {
            assert_eq!((g.r#ref, g.t, g.v.to_bits()), (w.r#ref, w.t, w.v.to_bits()), "{name}");
            assert_eq!(g.st, if st { w.st } else { 0 }, "{name}");
        }
        // Re-encoding gives Go's bytes.
        assert_eq!(go(name), enc(|b| Encoder { enable_st_storage: st }.samples(&got, b)));
    }

    // V1 splits regular and custom bucket histograms into two records; V2 keeps one record.
    let (cb, regular): (Vec<_>, Vec<_>) = hists.iter().cloned().partition(|h| h.h.uses_custom_buckets());
    let v1 = |v: Vec<RefHistogramSample>| v.into_iter().map(|h| RefHistogramSample { st: 0, ..h }).collect::<Vec<_>>();
    for (name, want) in [("hist_v1", v1(regular)), ("hist_cb_v1", v1(cb)), ("hist_v2", hists.clone())] {
        let mut got = Vec::new();
        dec.histogram_samples(&go(name), &mut got).unwrap();
        assert_eq!(want, got, "{name}");
    }
    for name in ["fhist_v1", "fhist_cb_v1", "fhist_v2"] {
        let mut got = Vec::new();
        dec.float_histogram_samples(&go(name), &mut got).unwrap();
        let e = Encoder { enable_st_storage: name.ends_with("v2") };
        let b = if name == "fhist_cb_v1" {
            enc(|b| e.custom_buckets_float_histogram_samples(&got, b))
        } else {
            let mut b = Vec::new();
            assert!(e.float_histogram_samples(&got, &mut b).is_empty());
            b
        };
        assert_eq!(go(name), b, "{name}");
    }

    let mut m = Vec::new();
    dec.metadata(&go("metadata"), &mut m).unwrap();
    assert_eq!(go("metadata"), enc(|b| Encoder::default().metadata(&m, b)));
    let mut ex = Vec::new();
    dec.exemplars(&go("exemplars"), &mut ex).unwrap();
    assert_eq!(go("exemplars"), enc(|b| Encoder::default().exemplars(&ex, b)));
    let mut ts = Vec::new();
    dec.tombstones(&go("tombstones"), &mut ts).unwrap();
    assert_eq!(go("tombstones"), enc(|b| Encoder::default().tombstones(&ts, b)));
    let mut mm = Vec::new();
    dec.mmap_markers(&go("mmap"), &mut mm).unwrap();
    assert_eq!(vec![RefMmapMarker { r#ref: 1, mmap_ref: 0x0000_0005_0000_1234 }], mm);
    assert_eq!(-12345, dec.min_valid_time(&go("minvalid")).unwrap());

    // Tombstones file written by Go.
    let b = go("tombfile");
    let body = &b[4..b.len() - 4];
    let mt = tombstones::decode(body).unwrap();
    assert_eq!(vec![Interval::new(-10, 100), Interval::new(300, 400)], mt.get(77));
}
