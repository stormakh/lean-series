//! Byte compatibility with upstream Go chunks. `scripts/interop/chunkenc/run.sh` runs the Go generator
//! (`scripts/interop/chunkenc/gen`), which writes one manifest per chunk to `bench/tmp/w0-hist/interop/`; this test
//! rebuilds every chunk from the same samples and asserts identical bytes, and decodes the Go bytes and asserts the
//! same samples as Go's iterator (floats compared by bit pattern). Skipped when the directory does not exist.
//!
//! Manifest lines: `encoding <n>`, `resume <k>`, `bytes <hex>`, `sample <st> <t> <f64 bits hex>` (input, in
//! append order), `decoded <st> <t> <f64 bits hex>` (Go iterator output).

use chunkenc::Iterator as _;
use chunkenc::{Appender, Chunk, Encoding, ValueType, from_data, new_empty_chunk};
use model::histogram::{CounterResetHint, FloatHistogram, Histogram, Span};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq)]
struct Sample {
    st: i64,
    t: i64,
    bits: u64,
}

struct Manifest {
    encoding: Encoding,
    resume: usize,
    bytes: Vec<u8>,
    samples: Vec<Sample>,
    decoded: Vec<Sample>,
    // Histogram cases (see scripts/interop/chunkenc/gen/hist.go for the format).
    cut: usize,
    hsamples: Vec<HSample>,
    chunks: Vec<Vec<u8>>,
    /// `hdecoded`, `fdecoded` and `decoderr` lines without the key, in order.
    hdecoded: Vec<String>,
}

#[derive(Debug, Clone)]
struct HSample {
    st: i64,
    t: i64,
    h: Option<Histogram>,
    fh: Option<FloatHistogram>,
}

fn hex_decode(s: &str) -> Vec<u8> {
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex byte")).collect()
}

fn parse_sample(rest: &str) -> Sample {
    let mut f = rest.split(' ');
    let st = f.next().unwrap().parse().unwrap();
    let t = f.next().unwrap().parse().unwrap();
    let bits = u64::from_str_radix(f.next().unwrap(), 16).unwrap();
    Sample { st, t, bits }
}

fn parse(text: &str) -> Manifest {
    let mut m = Manifest {
        encoding: Encoding(0),
        resume: 0,
        bytes: Vec::new(),
        samples: Vec::new(),
        decoded: Vec::new(),
        cut: 0,
        hsamples: Vec::new(),
        chunks: Vec::new(),
        hdecoded: Vec::new(),
    };
    for line in text.lines() {
        let (key, rest) = line.split_once(' ').unwrap_or((line, ""));
        match key {
            "encoding" => m.encoding = Encoding(rest.parse().unwrap()),
            "resume" => m.resume = rest.parse().unwrap(),
            "bytes" => {
                m.bytes = hex_decode(rest);
                m.chunks.push(m.bytes.clone());
            }
            "cut" => m.cut = rest.parse().unwrap(),
            "hsample" | "fsample" => {
                let mut f = rest.splitn(3, ' ');
                let st = f.next().unwrap().parse().unwrap();
                let t = f.next().unwrap().parse().unwrap();
                let body = f.next().unwrap();
                if key == "hsample" {
                    m.hsamples.push(HSample { st, t, h: Some(parse_h(body)), fh: None });
                } else {
                    m.hsamples.push(HSample { st, t, h: None, fh: Some(parse_fh(body)) });
                }
            }
            "hdecoded" | "fdecoded" | "decoderr" => m.hdecoded.push(line.to_string()),
            "sample" => m.samples.push(parse_sample(rest)),
            "decoded" => m.decoded.push(parse_sample(rest)),
            _ => panic!("unknown manifest line {line:?}"),
        }
    }
    m
}

fn fbits(f: f64) -> String {
    format!("{:016x}", f.to_bits())
}

fn pbits(s: &str) -> f64 {
    f64::from_bits(u64::from_str_radix(s, 16).unwrap())
}

fn fmt_spans(spans: &[Span]) -> String {
    if spans.is_empty() {
        return "-".into();
    }
    spans.iter().map(|s| format!("{}:{}", s.offset, s.length)).collect::<Vec<_>>().join(",")
}

fn fmt_ints(v: &[i64]) -> String {
    if v.is_empty() {
        return "-".into();
    }
    v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",")
}

fn fmt_floats(v: &[f64]) -> String {
    if v.is_empty() {
        return "-".into();
    }
    v.iter().map(|x| fbits(*x)).collect::<Vec<_>>().join(",")
}

fn fmt_h(h: &Histogram) -> String {
    format!(
        "{} {} {} {} {} {} {} {} {} {} {}",
        h.counter_reset_hint as u8,
        h.schema,
        fbits(h.zero_threshold),
        h.zero_count,
        h.count,
        fbits(h.sum),
        fmt_spans(&h.positive_spans),
        fmt_ints(&h.positive_buckets),
        fmt_spans(&h.negative_spans),
        fmt_ints(&h.negative_buckets),
        fmt_floats(h.custom_values_slice())
    )
}

fn fmt_fh(h: &FloatHistogram) -> String {
    format!(
        "{} {} {} {} {} {} {} {} {} {} {}",
        h.counter_reset_hint as u8,
        h.schema,
        fbits(h.zero_threshold),
        fbits(h.zero_count),
        fbits(h.count),
        fbits(h.sum),
        fmt_spans(&h.positive_spans),
        fmt_floats(&h.positive_buckets),
        fmt_spans(&h.negative_spans),
        fmt_floats(&h.negative_buckets),
        fmt_floats(h.custom_values_slice())
    )
}

fn parse_spans(s: &str) -> Vec<Span> {
    if s == "-" {
        return Vec::new();
    }
    s.split(',')
        .map(|x| {
            let (o, l) = x.split_once(':').unwrap();
            Span::new(o.parse().unwrap(), l.parse().unwrap())
        })
        .collect()
}

fn parse_ints(s: &str) -> Vec<i64> {
    if s == "-" { Vec::new() } else { s.split(',').map(|x| x.parse().unwrap()).collect() }
}

fn parse_floats(s: &str) -> Vec<f64> {
    if s == "-" { Vec::new() } else { s.split(',').map(pbits).collect() }
}

fn parse_cv(s: &str) -> Option<Arc<[f64]>> {
    if s == "-" { None } else { Some(Arc::from(parse_floats(s))) }
}

fn parse_h(body: &str) -> Histogram {
    let f: Vec<&str> = body.split(' ').collect();
    Histogram {
        counter_reset_hint: CounterResetHint::from_u8(f[0].parse().unwrap()),
        schema: f[1].parse().unwrap(),
        zero_threshold: pbits(f[2]),
        zero_count: f[3].parse().unwrap(),
        count: f[4].parse().unwrap(),
        sum: pbits(f[5]),
        positive_spans: parse_spans(f[6]),
        positive_buckets: parse_ints(f[7]),
        negative_spans: parse_spans(f[8]),
        negative_buckets: parse_ints(f[9]),
        custom_values: parse_cv(f[10]),
    }
}

fn parse_fh(body: &str) -> FloatHistogram {
    let f: Vec<&str> = body.split(' ').collect();
    FloatHistogram {
        counter_reset_hint: CounterResetHint::from_u8(f[0].parse().unwrap()),
        schema: f[1].parse().unwrap(),
        zero_threshold: pbits(f[2]),
        zero_count: pbits(f[3]),
        count: pbits(f[4]),
        sum: pbits(f[5]),
        positive_spans: parse_spans(f[6]),
        positive_buckets: parse_floats(f[7]),
        negative_spans: parse_spans(f[8]),
        negative_buckets: parse_floats(f[9]),
        custom_values: parse_cv(f[10]),
    }
}

fn is_float_hist_encoding(e: Encoding) -> bool {
    e == chunkenc::ENC_FLOAT_HISTOGRAM || e == chunkenc::ENC_FLOAT_HISTOGRAM_ST
}

/// Rebuilds the histogram chunks from the input samples, as the Go generator did (`encodeHist`).
fn encode_hist(m: &Manifest) -> Vec<Vec<u8>> {
    let float = is_float_hist_encoding(m.encoding);
    let mut out = Vec::new();
    let mut c: Box<dyn Chunk> = new_empty_chunk(m.encoding).unwrap();
    let mut app: Appender = c.appender().unwrap();
    for (i, s) in m.hsamples.iter().enumerate() {
        if m.resume > 0 && i == m.resume {
            let b = c.bytes().to_vec();
            c.reset(b);
            app = c.appender().unwrap();
        }
        let mut prev: Option<Appender> = None;
        if m.cut > 0 && i > 0 && i % m.cut == 0 {
            out.push(c.bytes().to_vec());
            prev = Some(app);
            c = new_empty_chunk(m.encoding).unwrap();
            app = c.appender().unwrap();
        }
        let (new, recoded) = if float {
            let mut fh = match (&s.fh, &s.h) {
                (Some(fh), _) => fh.copy(),
                (None, Some(h)) => h.to_float(),
                _ => unreachable!(),
            };
            app.append_float_histogram(&mut *c, prev.as_ref(), s.st, s.t, &mut fh, false).unwrap()
        } else {
            let mut h = s.h.as_ref().unwrap().copy();
            app.append_histogram(&mut *c, prev.as_ref(), s.st, s.t, &mut h, false).unwrap()
        };
        if let Some((nc, napp)) = new {
            if !recoded {
                out.push(c.bytes().to_vec());
            }
            c = nc;
            app = napp;
        }
    }
    out.push(c.bytes().to_vec());
    out
}

/// Decodes the Go histogram chunks into manifest lines.
fn decode_hist(m: &Manifest) -> Vec<String> {
    let mut out = Vec::new();
    for (ci, b) in m.chunks.iter().enumerate() {
        let c = from_data(m.encoding, b.clone()).unwrap();
        let mut it = c.iterator(None);
        loop {
            let vt = it.next();
            if vt == ValueType::None {
                break;
            }
            if vt == ValueType::Histogram {
                let (t, h) = it.at_histogram(None);
                out.push(format!("hdecoded {ci} {} {t} {}", it.at_st(), fmt_h(&h)));
            }
            let (t, fh) = it.at_float_histogram(None);
            out.push(format!("fdecoded {ci} {} {t} {}", it.at_st(), fmt_fh(&fh)));
        }
        if let Some(e) = it.err() {
            out.push(format!("decoderr {ci} {e}"));
        }
    }
    out
}

/// Rebuilds the chunk from the input samples, as the Go generator did.
fn encode(m: &Manifest) -> Vec<u8> {
    let mut c = new_empty_chunk(m.encoding).unwrap();
    let mut app = c.appender().unwrap();
    for (i, s) in m.samples.iter().enumerate() {
        if m.resume > 0 && i == m.resume {
            let b = c.bytes().to_vec();
            c.reset(b);
            app = c.appender().unwrap();
        }
        app.append(&mut *c, s.st, s.t, f64::from_bits(s.bits));
    }
    c.bytes().to_vec()
}

/// Decodes the Go bytes.
fn decode(m: &Manifest) -> Vec<Sample> {
    let c = from_data(m.encoding, m.bytes.clone()).unwrap();
    let mut it = c.iterator(None);
    let mut out = Vec::new();
    loop {
        match it.next() {
            ValueType::None => break,
            ValueType::Float => {
                let (t, v) = it.at();
                out.push(Sample { st: it.at_st(), t, bits: v.to_bits() });
            }
            vt => panic!("unexpected value type {vt}"),
        }
    }
    assert!(it.err().is_none(), "iterate: {:?}", it.err());
    out
}

#[test]
fn interop_go_chunks() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../bench/tmp/w0-hist/interop");
    if !dir.is_dir() {
        println!("skipped: {} does not exist; run scripts/interop/chunkenc/run.sh", dir.display());
        return;
    }
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "txt"))
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "no manifests in {}", dir.display());

    let mut failures = Vec::new();
    for p in &paths {
        let name = p.file_name().unwrap().to_string_lossy().into_owned();
        let m = parse(&std::fs::read_to_string(p).unwrap());

        if !m.hsamples.is_empty() {
            let ours = encode_hist(&m);
            if ours != m.chunks {
                let at = ours.iter().zip(&m.chunks).position(|(a, b)| a != b);
                failures.push(format!(
                    "{name}: encoded chunks differ (rust {} chunks, go {} chunks, first different chunk {at:?})",
                    ours.len(),
                    m.chunks.len()
                ));
            }
            let decoded = decode_hist(&m);
            if decoded != m.hdecoded {
                let at = decoded.iter().zip(&m.hdecoded).position(|(a, b)| a != b);
                failures.push(format!(
                    "{name}: decoded histograms differ (rust {} lines, go {}, first difference at {at:?}:\n  rust {:?}\n  go   {:?})",
                    decoded.len(),
                    m.hdecoded.len(),
                    at.map(|i| &decoded[i]),
                    at.map(|i| &m.hdecoded[i]),
                ));
            }
            continue;
        }

        let ours = encode(&m);
        if ours != m.bytes {
            let at = ours.iter().zip(&m.bytes).position(|(a, b)| a != b).unwrap_or(ours.len().min(m.bytes.len()));
            failures.push(format!(
                "{name}: encoded bytes differ (rust {} bytes, go {} bytes, first difference at byte {at})",
                ours.len(),
                m.bytes.len()
            ));
        }

        let decoded = decode(&m);
        if decoded != m.decoded {
            let at = decoded.iter().zip(&m.decoded).position(|(a, b)| a != b);
            failures.push(format!(
                "{name}: decoded samples differ (rust {}, go {}, first difference at {at:?}: rust {:?} go {:?})",
                decoded.len(),
                m.decoded.len(),
                at.map(|i| decoded[i]),
                at.map(|i| m.decoded[i]),
            ));
        }
    }
    println!("checked {} Go chunks", paths.len());
    assert!(failures.is_empty(), "{} of {} chunks differ:\n{}", failures.len(), paths.len(), failures.join("\n"));
}
