//! Port of upstream/model/textparse/protobufparse_test.go.
//!
//! The binary inputs are made from the Go test sources by scripts/gen/textparse (the Go `metricFamiliesToProtobuf`
//! output); the expected entry tables are translated value for value by scripts/gen/textparse/rustexp into
//! tests/common/protobufparse_tables.rs.

#[rustfmt::skip]
#[path = "common/protobufparse_tables.rs"]
mod tables;

use model::exemplar::Exemplar;
use model::labels::{Labels, ValidationScheme};
use textparse::testing::{require_entries, test_parse, ParsedEntry};
use textparse::{Entry, Parser, ProtobufParser};

fn data(name: &str) -> Vec<u8> {
    let p = format!("{}/../../tests-data/textparse/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&p).unwrap_or_else(|e| panic!("read {p}: {e}"))
}

#[test]
fn test_protobuf_parse() {
    let input_buf = data("protobufparse_test/createTestProtoBuf.bin");

    let scenarios: Vec<(&str, ProtobufParser<'_>, Vec<ParsedEntry>)> = vec![
        (
            "ignoreNativeHistograms=false/parseClassicHistograms=false/enableTypeAndUnitLabels=false",
            ProtobufParser::new(&input_buf, false, false, false, false),
            tables::test_protobuf_parse_0(),
        ),
        (
            "ignoreNativeHistograms=false/parseClassicHistograms=false/enableTypeAndUnitLabels=true",
            ProtobufParser::new(&input_buf, false, false, false, true),
            tables::test_protobuf_parse_1(),
        ),
        (
            "ignoreNativeHistograms=false/parseClassicHistograms=true/enableTypeAndUnitLabels=false",
            ProtobufParser::new(&input_buf, false, true, false, false),
            tables::test_protobuf_parse_2(),
        ),
        (
            "ignoreNativeHistograms=true/parseClassicHistograms=false/enableTypeAndUnitLabels=false",
            ProtobufParser::new(&input_buf, true, false, false, false),
            tables::test_protobuf_parse_3(),
        ),
    ];

    for (name, mut p, exp) in scenarios {
        eprintln!("scenario {name}");
        let got = test_parse(&mut p);
        require_entries(&exp, &got);
    }
}

#[test]
fn test_protobuf_parse_with_nhcb() {
    let data = data("protobufparse_test/TestProtobufParseWithNHCB.bin");

    // (ignoreNative, keepClassic, typeAndUnit, expected)
    let test_cases: Vec<(bool, bool, bool, Vec<ParsedEntry>)> = vec![
        (false, false, false, tables::test_protobuf_parse_with_nhcb_0()),
        (false, true, false, tables::test_protobuf_parse_with_nhcb_1()),
        (true, false, false, tables::test_protobuf_parse_with_nhcb_2()),
        (true, true, false, tables::test_protobuf_parse_with_nhcb_3()),
        (false, false, true, tables::test_protobuf_parse_with_nhcb_4()),
        (false, true, true, tables::test_protobuf_parse_with_nhcb_5()),
    ];

    for (ignore_native, keep_classic, type_and_unit, expected) in test_cases {
        eprintln!("ignoreNative={ignore_native},keepClassic={keep_classic},typeAndUnit={type_and_unit}");
        let mut p = ProtobufParser::new(&data, ignore_native, keep_classic, true, type_and_unit);
        let got = test_parse(&mut p);
        require_entries(&expected, &got);
    }
}

/// One iteration of the seed corpus: the file with the generated family and its expected name, help and unit.
struct FuzzIteration {
    file: String,
    name: String,
    help: String,
    unit: String,
}

/// The seed corpus of FuzzProtobufParser_Labels: (parseClassicHistogram, enableTypeAndUnitLabels, randSeed) and
/// the 20 iterations that Go's math/rand generates for the seed.
fn fuzz_seed_corpus() -> Vec<(bool, bool, i64, Vec<FuzzIteration>)> {
    let index = String::from_utf8(data("protobufparse_test/FuzzProtobufParser_Labels.tsv")).unwrap();
    let mut out: Vec<(bool, bool, i64, Vec<FuzzIteration>)> = Vec::new();
    for line in index.lines().filter(|l| !l.starts_with('#')) {
        let f: Vec<&str> = line.split('\t').collect();
        assert_eq!(f.len(), 8, "bad index line {line:?}");
        let (classic, type_and_unit, seed) = (f[0] == "true", f[1] == "true", f[2].parse::<i64>().unwrap());
        if out.last().is_none_or(|o| o.2 != seed) {
            out.push((classic, type_and_unit, seed, Vec::new()));
        }
        let it = &mut out.last_mut().unwrap().3;
        assert_eq!(f[3].parse::<usize>().unwrap(), it.len());
        it.push(FuzzIteration { file: f[4].into(), name: f[5].into(), help: f[6].into(), unit: f[7].into() });
    }
    out
}

// Go FuzzProtobufParser_Labels runs its seed corpus as a regular test; this runs the same corpus.
#[test]
fn fuzz_protobuf_parser_labels() {
    let corpus = fuzz_seed_corpus();
    let seeds: Vec<(bool, bool, i64)> = corpus.iter().map(|c| (c.0, c.1, c.2)).collect();
    assert_eq!(seeds, vec![(true, true, 123), (true, false, 129), (false, true, 159), (false, true, -127)]);

    for (parse_classic_histogram, enable_type_and_unit_labels, _seed, iterations) in corpus {
        assert_eq!(iterations.len(), 20);
        // adapted: Go reuses scrape buffers from a pool to find memory corruption; here one buffer is reused for
        // every iteration, and the labels kept from earlier iterations are checked after each one.
        let mut buf: Vec<u8> = Vec::new();
        let mut observed_labels: Vec<Labels> = Vec::new();

        for it in &iterations {
            buf.clear();
            buf.extend_from_slice(&data(&it.file));
            let mut p = ProtobufParser::new(&buf, false, parse_classic_histogram, false, enable_type_and_unit_labels);

            loop {
                let entry = match p.next() {
                    Err(e) if e.is_eof() => break,
                    Err(e) => panic!("{}: unexpected error: {e}", it.file),
                    Ok(entry) => entry,
                };
                match entry {
                    Entry::Help => {
                        let (name, help) = p.help();
                        assert_eq!(it.name.as_bytes(), name);
                        assert_eq!(it.help.as_bytes(), help);
                    }
                    Entry::Type => {
                        let (name, _) = p.typ();
                        assert_eq!(it.name.as_bytes(), name);
                    }
                    Entry::Unit => {
                        let (name, unit) = p.unit();
                        assert_eq!(it.name.as_bytes(), name);
                        assert_eq!(it.unit.as_bytes(), unit);
                    }
                    Entry::Series | Entry::Histogram => {
                        let mut lbs = Labels::default();
                        p.labels(&mut lbs);
                        observed_labels.push(lbs);
                    }
                    _ => {}
                }

                // Get labels from exemplars.
                loop {
                    let mut e = Exemplar::default();
                    if !p.exemplar(&mut e) {
                        break;
                    }
                    observed_labels.push(e.labels);
                }
            }

            // Validate all labels seen so far remain valid. This can find memory corruption issues.
            for l in &observed_labels {
                assert!(l.is_valid(ValidationScheme::Legacy), "encountered corrupted labels: {l}");
            }
        }
    }
}

// TestProtobufParseSummaryNoQuantilesNoPanic is a regression test for a panic when Next() is called without
// Series() on a summary with no quantiles.
#[test]
fn test_protobuf_parse_summary_no_quantiles_no_panic() {
    let buf = data("protobufparse_test/TestProtobufParseSummaryNoQuantilesNoPanic.bin");

    let mut p = ProtobufParser::new(&buf, false, false, false, false);
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        while p.next().is_ok() {}
    }));
    assert!(r.is_ok(), "parser panicked");
}

#[test]
fn test_protobuf_parse_histogram_span_bucket_mismatch() {
    for name in [
        "all zero-length positive spans with non-empty positive deltas",
        "more positive deltas than positive spans account for",
    ] {
        let file = format!("protobufparse_test/TestProtobufParseHistogramSpanBucketMismatch__{}.bin", name.replace([' ', '-'], "_"));
        let buf = data(&file);
        let mut p = ProtobufParser::new(&buf, false, false, false, false);
        let mut got_err = None;
        loop {
            match p.next() {
                Err(e) if e.is_eof() => break,
                Err(e) => {
                    got_err = Some(e);
                    break;
                }
                Ok(_) => {}
            }
        }
        let err = got_err.unwrap_or_else(|| panic!("{name}: expected an error"));
        assert!(err.to_string().contains("positive side"), "{name}: error {err:?} does not contain \"positive side\"");
    }
}

#[test]
fn test_protobuf_parse_zero_float_histogram() {
    let buf = data("protobufparse_test/TestProtobufParseZeroFloatHistogram.bin");

    let mut p = ProtobufParser::new(&buf, false, false, false, false);
    for expected in [Entry::Help, Entry::Type, Entry::Histogram] {
        let entry = p.next().expect("no error");
        assert_eq!(expected, entry);
    }

    let (_, _, h, fh) = p.histogram();
    assert!(h.is_none());
    let fh = fh.expect("float histogram");
    assert_eq!(fh.count, 0.0);
    assert_eq!(fh.zero_count, 0.0);
}

// TestProtobufParseMixedNativeAndClassicHistograms tests a metric family that contains both a native histogram
// (with classic buckets) and a classic-only histogram as separate series. With parseClassicHistograms=true and
// ignoreNativeHistograms=false, the parser must emit the native histogram, then the classic series for the first
// metric, then the classic series for the second metric.
#[test]
fn test_protobuf_parse_mixed_native_and_classic_histograms() {
    let buf = data("protobufparse_test/TestProtobufParseMixedNativeAndClassicHistograms.bin");

    // ignoreNativeHistograms=false, parseClassicHistograms=true, convertClassicHistogramsToNHCB=false,
    // enableTypeAndUnitLabels=false.
    let mut p = ProtobufParser::new(&buf, false, true, false, false);
    let expected = tables::test_protobuf_parse_mixed_native_and_classic_histograms_0();
    let got = test_parse(&mut p);
    require_entries(&expected, &got);
}

// TestProtobufParseMixedClassicAndNativeHistogramsReverseOrder tests a metric family that contains a classic-only
// histogram followed by a native histogram (with classic buckets) as separate series. The parser must emit the
// classic series for the first metric, then the native histogram for the second metric, then the classic series
// for the second metric.
#[test]
fn test_protobuf_parse_mixed_classic_and_native_histograms_reverse_order() {
    let buf = data("protobufparse_test/TestProtobufParseMixedClassicAndNativeHistogramsReverseOrder.bin");

    // ignoreNativeHistograms=false, parseClassicHistograms=true, convertClassicHistogramsToNHCB=false,
    // enableTypeAndUnitLabels=false.
    let mut p = ProtobufParser::new(&buf, false, true, false, false);
    let expected = tables::test_protobuf_parse_mixed_classic_and_native_histograms_reverse_order_0();
    let got = test_parse(&mut p);
    require_entries(&expected, &got);
}
