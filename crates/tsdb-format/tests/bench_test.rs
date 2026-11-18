//! Port of `tsdb/record/bench_test.go` (the test; Go benchmarks are not ported) and the sample cases of
//! `util/testrecord`.

use tsdb_format::record::{Decoder, Encoder, RefSample};

/// Go `testrecord.RefSamplesCase`.
#[derive(Clone, Copy, Debug)]
enum RefSamplesCase {
    Realistic1000Samples,
    Realistic1000WithVariableStSamples,
    Realistic1000WithConstStSamples,
    WorstCase1000,
    WorstCase1000WithStSamples,
}

fn high_variance_int(i: usize) -> i64 {
    if i % 2 == 0 { i32::MIN as i64 } else { i32::MAX as i64 }
}

fn high_variance_float(i: usize) -> f64 {
    // Go math.SmallestNonzeroFloat32 and math.MaxFloat32.
    if i % 2 == 0 { 1.401298464324817070923729583289916131280e-45 } else { f32::MAX as f64 }
}

/// Go `testrecord.GenTestRefSamplesCase`.
fn gen_test_ref_samples_case(c: RefSamplesCase) -> Vec<RefSample> {
    let mut ret = vec![RefSample::default(); 1000];
    for (i, s) in ret.iter_mut().enumerate() {
        s.r#ref = i as u64;
        s.v = high_variance_float(i);
        match c {
            // Samples are across series, so likely all have the same timestamp.
            RefSamplesCase::Realistic1000Samples => s.t = 12423423,
            // Likely the start times will all be the same with deltas.
            RefSamplesCase::Realistic1000WithConstStSamples => {
                s.st = 12423423;
                s.t = 12423423 + 15;
            }
            // Maybe series have different start times though.
            RefSamplesCase::Realistic1000WithVariableStSamples => {
                s.st = (12423423 / 9) * (i as i64 % 10);
                s.t = 12423423;
            }
            RefSamplesCase::WorstCase1000 => s.t = high_variance_int(i),
            RefSamplesCase::WorstCase1000WithStSamples => {
                // Worst case is when the values are significantly different to each other which breaks delta
                // encoding.
                s.st = high_variance_int(i + 1) / 1024; // Make sure ST is not comparable to T
                s.t = high_variance_int(i);
            }
        }
    }
    ret
}

fn zero_out_sts(samples: &[RefSample]) -> Vec<RefSample> {
    samples.iter().map(|s| RefSample { st: 0, ..*s }).collect()
}

#[test]
fn test_encode_decode() {
    for enable_st_storage in [false, true] {
        for tcase in [
            RefSamplesCase::Realistic1000Samples,
            RefSamplesCase::Realistic1000WithVariableStSamples,
            RefSamplesCase::Realistic1000WithConstStSamples,
            RefSamplesCase::WorstCase1000,
            RefSamplesCase::WorstCase1000WithStSamples,
        ] {
            let dec = Decoder::default();
            let enc = Encoder { enable_st_storage };

            let s = gen_test_ref_samples_case(tcase);
            // If ST is off, we expect all STs to be zero.
            let expected = if enable_st_storage { s.clone() } else { zero_out_sts(&s) };

            {
                let mut b = Vec::new();
                enc.samples(&s, &mut b);
                let mut got = Vec::new();
                dec.samples(&b, &mut got).unwrap();
                assert_eq!(expected, got, "{tcase:?} st={enable_st_storage}");
            }

            // With byte buffer (append!)
            {
                let mut buf = vec![0u8; 10];
                buf.reserve(100_000 - 10);
                enc.samples(&s, &mut buf);
                let mut got = Vec::new();
                dec.samples(&buf[10..], &mut got).unwrap();
                assert_eq!(expected, got, "{tcase:?} st={enable_st_storage}");
            }

            // With sample slice
            {
                let mut b = Vec::new();
                enc.samples(&s, &mut b);
                let mut got: Vec<RefSample> = Vec::with_capacity(s.len() + 1);
                dec.samples(&b, &mut got).unwrap();
                assert_eq!(expected, got, "{tcase:?} st={enable_st_storage}");
            }

            // With compression.
            {
                let mut b = Vec::new();
                enc.samples(&s, &mut b);
                // adapted: Go uses tsdb/compression (Zstd); the zstd crate gives the same round trip.
                let c = zstd::bulk::compress(&b, 0).unwrap();
                let d = zstd::decode_all(&c[..]).unwrap();
                let mut got = Vec::new();
                dec.samples(&d, &mut got).unwrap();
                assert_eq!(expected, got, "{tcase:?} st={enable_st_storage}");
            }
        }
    }
}
