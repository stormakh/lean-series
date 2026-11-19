//! Port of upstream/util/fuzzing/fuzz_test.go and its seed corpus (corpus.go).
//!
//! `go test` runs each fuzz target once per seed corpus entry; these tests do the same with the Rust parsers and
//! chunks. Random fuzzing (`go test -fuzz`) is not ported. The FuzzParseProtobuf seeds are built by Go code
//! (corpus_protobuf.go, gogo/protobuf marshalling); scripts/gen/fuzzing writes them to
//! tests-data/fuzzing/protobuf_corpus.bin.
#[path = "common/gorand.rs"]
mod gorand;

use chunkenc::{ChunkIterator, ValueType, Xor2Chunk, XorChunk};
use gorand::GoRand;
use model::exemplar::Exemplar;
use model::labels::Labels;
use model::value::STALE_NAN;
use textparse::{Entry, Parser, ParserOptions, ProtobufParser};

// Input size above which we know that Prometheus would consume too much memory.
const MAX_INPUT_SIZE: usize = 10240;

// ---- corpus.go ----

fn get_corpus_for_fuzz_parse_metric_text() -> Vec<&'static [u8]> {
    vec![
        b"",
        b"metric_name 1.0",
        b"# HELP metric_name help text\n# TYPE metric_name counter\nmetric_name 1.0",
        b"o { quantile = \"1.0\", a = \"b\" } 8.3835e-05",
        b"# HELP api_http_request_count The total number of HTTP requests.\n# TYPE api_http_request_count counter\nhttp_request_count{method=\"post\",code=\"200\"} 1027 1395066363000",
        b"msdos_file_access_time_ms{path=\"C:\\\\DIR\\\\FILE.TXT\",error=\"Cannot find file:\\n\\\"FILE.TXT\\\"\"} 1.234e3",
        b"metric_without_timestamp_and_labels 12.47",
        b"something_weird{problem=\"division by zero\"} +Inf -3982045",
        b"http_request_duration_seconds_bucket{le=\"+Inf\"} 144320",
        b"go_gc_duration_seconds{ quantile=\"0.9\", a=\"b\"} 8.3835e-05",
        b"go_gc_duration_seconds{ quantile=\"1.0\", a=\"b\" } 8.3835e-05",
        b"go_gc_duration_seconds{ quantile = \"1.0\", a = \"b\" } 8.3835e-05",
    ]
}

fn get_corpus_for_fuzz_parse_open_metric() -> Vec<&'static [u8]> {
    vec![
        b"",
        b"# TYPE metric_name counter\nmetric_name_total 1.0",
        b"# HELP metric_name help text\n# TYPE metric_name counter\nmetric_name_total 1.0\n# EOF",
    ]
}

fn get_corpus_for_fuzz_parse_metric_selector() -> Vec<&'static str> {
    vec!["", "metric_name", r#"metric_name{label="value"}"#, r#"{label="value"}"#, r#"metric_name{label=~"val.*"}"#]
}

fn get_corpus_for_fuzz_parse_expr() -> Result<Vec<String>, String> {
    // Get built-in test expressions.
    let mut exprs = promql::promqltest::get_builtin_exprs()?;

    // Add additional seed corpus.
    let additional_exprs = [
        "",
        "1",
        "metric_name",
        r#""str""#,
        // Numeric literals
        ".5",
        "5.",
        "123.4567",
        "5e3",
        "5e-3",
        "+5.5e-3",
        "0xc",
        "0755",
        "-0755",
        "+Inf",
        "-Inf",
        // Basic binary operations
        "1 + 1",
        "1 - 1",
        "1 * 1",
        "1 / 1",
        "1 % 1",
        // Comparison operators
        "1 == 1",
        "1 != 1",
        "1 > 1",
        "1 >= 1",
        "1 < 1",
        "1 <= 1",
        // Operations with identifiers
        "foo == 1",
        "foo * bar",
        "2.5 / bar",
        "foo and bar",
        "foo or bar",
        // Complex expressions
        "+1 + -2 * 1",
        "1 + 2/(3*1)",
        // Comment
        "#comment",
    ];
    exprs.extend(additional_exprs.iter().map(|s| s.to_string()));
    Ok(exprs)
}

/// Go `ChunkFuzzSeed`.
struct ChunkFuzzSeed {
    seed: i64,
    n: u8,
    nan_mask: u64,
}

fn get_corpus_for_fuzz_xor_chunk() -> Vec<ChunkFuzzSeed> {
    vec![
        // Basic cases: no StaleNaN.
        ChunkFuzzSeed { seed: 0, n: 0, nan_mask: 0 },
        ChunkFuzzSeed { seed: 42, n: 2, nan_mask: 0 },
        ChunkFuzzSeed { seed: 1234567890, n: 119, nan_mask: 0 },
        // Single StaleNaN at first sample.
        ChunkFuzzSeed { seed: 0, n: 0, nan_mask: 0b1 },
        // StaleNaN in the middle of a run.
        ChunkFuzzSeed { seed: 42, n: 4, nan_mask: 0b00100 },
        // Alternating StaleNaN.
        ChunkFuzzSeed { seed: 1, n: 9, nan_mask: 0b0101010101 },
        // All StaleNaN.
        ChunkFuzzSeed { seed: 7, n: 9, nan_mask: !0u64 },
    ]
}

/// Go `XOR2ChunkFuzzSeed`.
struct Xor2ChunkFuzzSeed {
    seed: i64,
    n: u8,
    nan_mask: u64,
    st_mode: u8,
}

fn get_corpus_for_fuzz_xor2_chunk() -> Vec<Xor2ChunkFuzzSeed> {
    vec![
        // No ST at all.
        Xor2ChunkFuzzSeed { seed: 0, n: 0, nan_mask: 0, st_mode: 0 },
        Xor2ChunkFuzzSeed { seed: 1234567890, n: 119, nan_mask: 0, st_mode: 0 },
        // ST known from sample 0 and then constant.
        Xor2ChunkFuzzSeed { seed: 42, n: 2, nan_mask: 0, st_mode: 1 },
        // First ST change happens after sample 1.
        Xor2ChunkFuzzSeed { seed: 42, n: 4, nan_mask: 0b00100, st_mode: 2 },
        // Active ST with small deltas to hit compact encodings.
        Xor2ChunkFuzzSeed { seed: 1, n: 9, nan_mask: 0b0101010101, st_mode: 3 },
        // Active ST with large deltas to hit varbit fallback.
        Xor2ChunkFuzzSeed { seed: 7, n: 9, nan_mask: !0u64, st_mode: 4 },
    ]
}

/// Go `ProtobufCorpusSeed`, read from the file written by scripts/gen/fuzzing.
struct ProtobufCorpusSeed {
    data: Vec<u8>,
    ignore_native: bool,
    parse_classic: bool,
    convert_nhcb: bool,
    type_and_unit: bool,
}

fn get_corpus_for_fuzz_parse_protobuf() -> Result<Vec<ProtobufCorpusSeed>, String> {
    let p = format!("{}/../../tests-data/fuzzing/protobuf_corpus.bin", env!("CARGO_MANIFEST_DIR"));
    let b = std::fs::read(&p).map_err(|e| format!("read {p}: {e}"))?;
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let n = u32::from_le_bytes(b.get(i..i + 4).ok_or("truncated length")?.try_into().map_err(|_| "length")?) as usize;
        i += 4;
        let data = b.get(i..i + n).ok_or("truncated data")?.to_vec();
        i += n;
        let f = b.get(i..i + 4).ok_or("truncated flags")?;
        i += 4;
        out.push(ProtobufCorpusSeed { data, ignore_native: f[0] == 1, parse_classic: f[1] == 1, convert_nhcb: f[2] == 1, type_and_unit: f[3] == 1 });
    }
    Ok(out)
}

// ---- fuzz_test.go ----

/// The body of FuzzParseMetricText and FuzzParseOpenMetric: parse until an error.
fn fuzz_parse(input: &[u8], content_type: &str) {
    let (p, warning) = textparse::new(input, content_type, &ParserOptions::default());
    let Some(mut p) = p else {
        // An invalid content type is being passed, which should not happen in this context (Go: t.Skip()).
        return;
    };
    if warning.is_some() {
        return;
    }
    let err = loop {
        if let Err(e) = p.next() {
            break e;
        }
    };
    // We don't care about errors, just that we don't panic.
    let _ = err.is_eof();
}

// FuzzParseMetricText fuzzes the metric parser with "text/plain" content type.
#[test]
fn fuzz_parse_metric_text() {
    for corpus in get_corpus_for_fuzz_parse_metric_text() {
        fuzz_parse(corpus, "text/plain");
    }
}

// FuzzParseOpenMetric fuzzes the metric parser with "application/openmetrics-text" content type.
#[test]
fn fuzz_parse_open_metric() {
    for corpus in get_corpus_for_fuzz_parse_open_metric() {
        fuzz_parse(corpus, "application/openmetrics-text");
    }
}

// FuzzParseMetricSelector fuzzes the metric selector parser.
#[test]
fn fuzz_parse_metric_selector() {
    let fuzz_parser = promql::parser::Parser::new(promql::parser::Options::default());
    for input in get_corpus_for_fuzz_parse_metric_selector() {
        if input.len() > MAX_INPUT_SIZE {
            continue;
        }
        // We don't care about errors, just that we don't panic.
        let _ = fuzz_parser.parse_metric_selector(input);
    }
}

/// Go `math.Float64frombits(value.StaleNaN)`.
fn stale_nan() -> f64 {
    f64::from_bits(STALE_NAN)
}

// FuzzXORChunk fuzzes the XOR chunk round-trip.
#[test]
fn fuzz_xor_chunk() {
    for s in get_corpus_for_fuzz_xor_chunk() {
        let (seed, n, nan_mask) = (s.seed, s.n, s.nan_mask);
        let count = n as usize % 130 + 1;
        let mut r = GoRand::new(seed);

        let mut samples: Vec<(i64, f64)> = Vec::with_capacity(count);
        let mut ts: i64 = 0;
        for i in 0..count {
            ts += r.int63n(10000) + 1;
            let mut v = f64::from_bits(r.uint64());
            if i < 64 && (nan_mask >> i) & 1 == 1 {
                v = stale_nan();
            }
            samples.push((ts, v));
        }

        let mut c = XorChunk::new();
        let mut app = c.appender().unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        for &(t, v) in &samples {
            // XOR chunk does not store ST, therefore use 0 as ST.
            app.append(&mut c, 0, t, v);
        }

        let mut it = c.iterator();
        for &(want_t, want_v) in &samples {
            assert_ne!(it.next(), ValueType::None, "seed {seed}: iterator ended early");
            let (got_t, got_v) = it.at();
            assert_eq!(got_t, want_t, "seed {seed}: timestamp mismatch: got {got_t}, want {want_t}");
            assert_eq!(got_v.to_bits(), want_v.to_bits(), "seed {seed}: value mismatch: got {:x}, want {:x}", got_v.to_bits(), want_v.to_bits());
        }
        assert_eq!(it.next(), ValueType::None, "seed {seed}: iterator has extra values");
        assert!(it.err().is_none(), "seed {seed}: {:?}", it.err());
    }
}

// FuzzXOR2Chunk fuzzes the XOR2 chunk round-trip.
#[test]
fn fuzz_xor2_chunk() {
    for s in get_corpus_for_fuzz_xor2_chunk() {
        let (seed, n, nan_mask, st_mode) = (s.seed, s.n, s.nan_mask, s.st_mode);
        let count = n as usize % 130 + 1;
        let mut r = GoRand::new(seed);

        // (st, t, v)
        let mut samples: Vec<(i64, i64, f64)> = Vec::with_capacity(count);
        let mut ts: i64 = 0;
        let mut active_st: i64 = 0;
        let mut constant_st: i64 = 0;
        let mut late_st_index = 1usize;
        if count > 1 {
            late_st_index = r.int31n((count - 1) as i32) as usize + 1;
        }
        for i in 0..count {
            ts += r.int63n(10000) + 1;
            let mut v = f64::from_bits(r.uint64());
            if i < 64 && (nan_mask >> i) & 1 == 1 {
                v = stale_nan();
            }

            let mut st: i64 = 0;
            match st_mode % 5 {
                0 => st = 0,
                1 => {
                    if i == 0 {
                        constant_st = ts - (r.int63n(10000) + 1);
                    }
                    st = constant_st;
                }
                2 => {
                    if i >= late_st_index {
                        if i == late_st_index {
                            constant_st = ts - (r.int63n(10000) + 1);
                        }
                        st = constant_st;
                    }
                }
                3 => {
                    if i == 0 {
                        active_st = ts - (r.int63n(10000) + 1);
                    } else {
                        active_st -= r.int63n(8) - 3;
                    }
                    st = active_st;
                }
                _ => {
                    active_st = ts.wrapping_sub(r.int63());
                    st = active_st;
                }
            }

            samples.push((st, ts, v));
        }

        let mut c = Xor2Chunk::new();
        let mut app = c.appender().unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        for &(st, t, v) in &samples {
            app.append(&mut c, st, t, v);
        }

        let mut it = c.iterator();
        for &(want_st, want_t, want_v) in &samples {
            assert_ne!(it.next(), ValueType::None, "seed {seed}: iterator ended early");
            let (got_t, got_v) = it.at();
            assert_eq!(got_t, want_t, "seed {seed}: timestamp mismatch: got {got_t}, want {want_t}");
            assert_eq!(got_v.to_bits(), want_v.to_bits(), "seed {seed}: value mismatch: got {:x}, want {:x}", got_v.to_bits(), want_v.to_bits());
            let got_st = it.at_st();
            assert_eq!(got_st, want_st, "seed {seed}: ST mismatch: got {got_st}, want {want_st}");
        }
        assert_eq!(it.next(), ValueType::None, "seed {seed}: iterator has extra values");
        assert!(it.err().is_none(), "seed {seed}: {:?}", it.err());
    }
}

// FuzzParseProtobuf fuzzes the protobuf exposition-format parser.
#[test]
fn fuzz_parse_protobuf() {
    let corpus = get_corpus_for_fuzz_parse_protobuf().unwrap_or_else(|e| panic!("{e}"));
    for s in corpus {
        if s.data.len() > MAX_INPUT_SIZE {
            continue;
        }
        let mut p = ProtobufParser::new(&s.data, s.ignore_native, s.parse_classic, s.convert_nhcb, s.type_and_unit);
        let err = loop {
            let entry = match p.next() {
                Ok(e) => e,
                Err(e) => break e,
            };
            match entry {
                Entry::Help => {
                    let _ = p.help();
                }
                Entry::Type => {
                    let _ = p.typ();
                }
                Entry::Unit => {
                    let _ = p.unit();
                }
                Entry::Series => {
                    let mut lbs = Labels::default();
                    p.labels(&mut lbs);
                    let _ = p.series();
                    let _ = p.start_timestamp();
                    let mut ex = Exemplar::default();
                    while p.exemplar(&mut ex) {}
                }
                Entry::Histogram => {
                    let mut lbs = Labels::default();
                    p.labels(&mut lbs);
                    let _ = p.histogram();
                    let _ = p.start_timestamp();
                    let mut ex = Exemplar::default();
                    while p.exemplar(&mut ex) {}
                }
                _ => {}
            }
        };
        // We don't care about errors, just that we don't panic.
        let _ = err.is_eof();
    }
}

// FuzzParseExpr fuzzes the expression parser.
#[test]
fn fuzz_parse_expr() {
    // Add seed corpus from built-in test expressions
    let corpus = get_corpus_for_fuzz_parse_expr().unwrap_or_else(|e| panic!("{e}"));
    assert!(corpus.len() >= 1000, "loading exprs is likely broken: got {} expressions, expected at least 1000", corpus.len());

    let p = promql::parser::Parser::new(promql::parser::Options {
        enable_experimental_functions: true,
        enable_binop_fill_modifiers: true,
        ..Default::default()
    });
    for input in &corpus {
        if input.len() > MAX_INPUT_SIZE {
            continue;
        }
        let r = p.parse_expr(input);
        // The parser recovers from runtime panics and returns ErrUnexpected.
        // Flag those as test failures since they indicate a real bug.
        if let Err(promql::parser::Error::Unexpected) = r {
            panic!("parser panicked on input {input:?}: unexpected error");
        }
    }
}
