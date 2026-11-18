//! Port of `tsdb/chunkenc/chunk_test.go` (`testChunkOverFlowPanics` and `triple` live in `common`).

mod common;

use chunkenc::ChunkIterator as _;
use chunkenc::{
    Appender, Bstream, Chunk, AnyIterator, ENC_FLOAT_HISTOGRAM, ENC_FLOAT_HISTOGRAM_ST, ENC_HISTOGRAM,
    ENC_HISTOGRAM_ST, ENC_NONE, ENC_XOR, ENC_XOR2, Encoding, Error, FloatHistogramChunk, FloatHistogramStChunk,
    HistogramChunk, HistogramStChunk, Pool, ValueType, Xor2Chunk, XorChunk, compatible_values, new_pool,
};
use common::{Rand, Triple, triple};
use std::any::Any;

#[test]
fn test_chunk() {
    struct TestCase {
        encoding: Encoding,
        supports_st: bool,
        factory: fn() -> Box<dyn Chunk>,
    }
    let testcases = [
        TestCase { encoding: ENC_XOR, supports_st: false, factory: || Box::new(XorChunk::new()) },
        TestCase { encoding: ENC_XOR2, supports_st: true, factory: || Box::new(Xor2Chunk::new()) },
    ];
    // adapted: Go uses the randomly seeded global math/rand; a time-based seed keeps the data random per run and
    // the seed is printed in every assertion message.
    let seed = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    let mut rng = Rand::new_pcg(seed, seed ^ 0x9e3779b97f4a7c15);
    for tc in &testcases {
        for _ in 0..1 {
            let mut c = (tc.factory)();
            test_chunk_impl(&format!("{} (seed {seed})", tc.encoding), &mut rng, &mut *c, tc.supports_st);
        }
    }
}

fn test_chunk_impl(name: &str, rng: &mut Rand, c: &mut dyn Chunk, supports_st: bool) {
    let mut app: Appender = c.appender().unwrap_or_else(|e| panic!("{name}: {e}"));

    let mut exp: Vec<Triple> = Vec::new();
    let mut ts: i64 = 1234123324;
    let mut v: f64 = 1243535.123;
    for i in 0..300 {
        ts += rng.int_n(10000) + 1;
        if i % 2 == 0 {
            v += rng.int_n(1000000) as f64;
        } else {
            v -= rng.int_n(1000000) as f64;
        }

        // Start with a new appender every 10th sample. This emulates starting
        // appending to a partially filled chunk.
        if i % 10 == 0 {
            app = c.appender().unwrap_or_else(|e| panic!("{name}: {e}"));
        }

        app.append(c, ts - 100, ts, v);
        let exp_st = if supports_st { ts - 100 } else { 0 };
        exp.push(triple(exp_st, ts, v));
    }
    let c: &dyn Chunk = c;

    // 1. Expand iterator in simple case.
    let mut it1 = c.iterator(None);
    let mut res1 = Vec::new();
    while it1.next() == ValueType::Float {
        let (ts, v) = it1.at();
        res1.push(triple(it1.at_st(), ts, v));
    }
    assert!(it1.err().is_none(), "{name}: {:?}", it1.err());
    assert_eq!(exp, res1, "{name}");

    // 2. Expand second iterator while reusing first one.
    let mut it2: AnyIterator<'_> = c.iterator(Some(it1));
    let mut res2 = Vec::new();
    while it2.next() == ValueType::Float {
        let (ts, v) = it2.at();
        res2.push(triple(it2.at_st(), ts, v));
    }
    assert!(it2.err().is_none(), "{name}: {:?}", it2.err());
    assert_eq!(exp, res2, "{name}");

    // 3. Test iterator Seek.
    let mid = exp.len() / 2;

    let mut it3 = c.iterator(None);
    let mut res3 = Vec::new();
    assert_eq!(ValueType::Float, it3.seek(exp[mid].t), "{name}");
    // Below ones should not matter.
    assert_eq!(ValueType::Float, it3.seek(exp[mid].t), "{name}");
    assert_eq!(ValueType::Float, it3.seek(exp[mid].t), "{name}");
    let (ts, v) = it3.at();
    res3.push(triple(it3.at_st(), ts, v));

    let mut last_ts = ts;
    while it3.next() == ValueType::Float {
        let (ts, v) = it3.at();
        last_ts = ts;
        res3.push(triple(it3.at_st(), ts, v));
    }
    // Seeking to last timestamp should work and it is a no-op.
    assert_eq!(ValueType::Float, it3.seek(last_ts), "{name}");
    assert!(it3.err().is_none(), "{name}: {:?}", it3.err());
    assert_eq!(&exp[mid..], &res3[..], "{name}");
    assert_eq!(ValueType::None, it3.seek(exp[exp.len() - 1].t + 1), "{name}");
}

/// The bstream of a pooled chunk (Go `&c.(*XORChunk).b` etc.).
fn bstream_of(c: &dyn Chunk, e: Encoding) -> &Bstream {
    let a = c.as_any();
    match e {
        ENC_HISTOGRAM => &a.downcast_ref::<HistogramChunk>().unwrap().b,
        ENC_FLOAT_HISTOGRAM => &a.downcast_ref::<FloatHistogramChunk>().unwrap().b,
        ENC_XOR2 => &a.downcast_ref::<Xor2Chunk>().unwrap().b,
        ENC_HISTOGRAM_ST => &a.downcast_ref::<HistogramStChunk>().unwrap().b,
        ENC_FLOAT_HISTOGRAM_ST => &a.downcast_ref::<FloatHistogramStChunk>().unwrap().b,
        _ => &a.downcast_ref::<XorChunk>().unwrap().b,
    }
}

fn bstream_of_mut(c: &mut dyn Chunk, e: Encoding) -> &mut Bstream {
    let a = c.as_any_mut();
    match e {
        ENC_HISTOGRAM => &mut a.downcast_mut::<HistogramChunk>().unwrap().b,
        ENC_FLOAT_HISTOGRAM => &mut a.downcast_mut::<FloatHistogramChunk>().unwrap().b,
        ENC_XOR2 => &mut a.downcast_mut::<Xor2Chunk>().unwrap().b,
        ENC_HISTOGRAM_ST => &mut a.downcast_mut::<HistogramStChunk>().unwrap().b,
        ENC_FLOAT_HISTOGRAM_ST => &mut a.downcast_mut::<FloatHistogramStChunk>().unwrap().b,
        _ => &mut a.downcast_mut::<XorChunk>().unwrap().b,
    }
}

#[test]
fn test_pool() {
    let p = new_pool();
    struct Tc {
        name: &'static str,
        encoding: Encoding,
        exp_err: Option<&'static str>,
    }
    let cases = [
        Tc { name: "xor", encoding: ENC_XOR, exp_err: None },
        Tc { name: "histogram", encoding: ENC_HISTOGRAM, exp_err: None },
        Tc { name: "float histogram", encoding: ENC_FLOAT_HISTOGRAM, exp_err: None },
        Tc { name: "xor opt st", encoding: ENC_XOR2, exp_err: None },
        Tc { name: "histogram st", encoding: ENC_HISTOGRAM_ST, exp_err: None },
        Tc { name: "float histogram st", encoding: ENC_FLOAT_HISTOGRAM_ST, exp_err: None },
        Tc { name: "invalid encoding", encoding: ENC_NONE, exp_err: Some(r#"invalid chunk encoding "none""#) },
    ];
    for tc in &cases {
        let name = tc.name;
        let r = p.get(tc.encoding, b"test".to_vec());
        if let Some(exp_err) = tc.exp_err {
            match r {
                Err(e) => assert_eq!(exp_err, e.to_string(), "{name}"),
                Ok(c) => panic!("{name}: expected error, got chunk {c:?}"),
            }
            continue;
        }

        let mut c = r.unwrap_or_else(|e| panic!("{name}: {e}"));

        assert_eq!(&Bstream { stream: b"test".to_vec(), count: 0 }, bstream_of(&*c, tc.encoding), "{name}");

        bstream_of_mut(&mut *c, tc.encoding).count = 1;
        p.put(c).unwrap_or_else(|e| panic!("{name}: {e}"));
        // adapted: Go keeps a pointer into the chunk after Put; in Rust Put takes ownership, so the chunk is
        // inspected where Put stored it (the top of the pool's free list for this encoding).
        let free = p.free[tc.encoding.0 as usize - 1].lock().unwrap();
        let pooled = free.last().unwrap_or_else(|| panic!("{name}: chunk not pooled"));
        assert_eq!(&Bstream { stream: Vec::new(), count: 0 }, bstream_of(&**pooled, tc.encoding), "{name}");
    }

    // "put bad chunk wrapper".
    {
        // When a wrapping chunk poses as an encoding it can't be converted to, Put should skip it.
        let c = FakeChunk { encoding: ENC_XOR };
        assert!(p.put(Box::new(c)).is_ok(), "put bad chunk wrapper");
    }
    // "put invalid encoding".
    {
        let c = FakeChunk { encoding: ENC_NONE };
        let err = p.put(Box::new(c)).expect_err("put invalid encoding");
        assert_eq!(r#"invalid chunk encoding "none""#, err.to_string());
    }
}

/// Go `fakeChunk`: embeds a nil `Chunk` (every other method panics) and overrides `Encoding` and `Reset`.
#[derive(Debug)]
struct FakeChunk {
    encoding: Encoding,
}

impl Chunk for FakeChunk {
    fn bytes(&self) -> &[u8] {
        unimplemented!("nil Chunk")
    }
    fn encoding(&self) -> Encoding {
        self.encoding
    }
    fn appender(&mut self) -> Result<Appender, Error> {
        unimplemented!("nil Chunk")
    }
    fn iterator<'a>(&'a self, _it: Option<AnyIterator<'a>>) -> AnyIterator<'a> {
        unimplemented!("nil Chunk")
    }
    fn num_samples(&self) -> usize {
        unimplemented!("nil Chunk")
    }
    fn compact(&mut self) {
        unimplemented!("nil Chunk")
    }
    fn reset(&mut self, _stream: Vec<u8>) {
        panic!("Reset should not be called");
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn test_compatible_values() {
    // CompatibleValues is about whether a chunk can continue to receive appends for a
    // different encoding without being cut. Only the XOR float family (EncXOR and
    // EncXOR2) is mutually compatible. All other pairs — including same-type
    // histogram pairs — return false because histogram encodings are not part of the
    // XOR family and no cross-family compatibility is defined.
    let cases: [(Encoding, Encoding, bool); 21] = [
        (ENC_XOR, ENC_XOR, true),
        (ENC_XOR2, ENC_XOR2, true),
        (ENC_XOR, ENC_XOR2, true),
        (ENC_XOR2, ENC_XOR, true),
        (ENC_HISTOGRAM, ENC_HISTOGRAM, false),
        (ENC_HISTOGRAM, ENC_FLOAT_HISTOGRAM, false),
        (ENC_FLOAT_HISTOGRAM, ENC_HISTOGRAM, false),
        (ENC_FLOAT_HISTOGRAM, ENC_FLOAT_HISTOGRAM, false),
        (ENC_XOR, ENC_HISTOGRAM, false),
        (ENC_XOR2, ENC_HISTOGRAM, false),
        (ENC_XOR, ENC_FLOAT_HISTOGRAM, false),
        (ENC_XOR2, ENC_FLOAT_HISTOGRAM, false),
        (ENC_HISTOGRAM, ENC_XOR, false),
        (ENC_HISTOGRAM, ENC_XOR2, false),
        (ENC_FLOAT_HISTOGRAM, ENC_XOR, false),
        (ENC_FLOAT_HISTOGRAM, ENC_XOR2, false),
        (ENC_NONE, ENC_XOR, false),
        (ENC_NONE, ENC_XOR2, false),
        (ENC_XOR, ENC_NONE, false),
        (ENC_XOR2, ENC_NONE, false),
        (ENC_NONE, ENC_NONE, false),
    ];
    for (a, b, want) in cases {
        assert_eq!(want, compatible_values(a, b), "CompatibleValues({a}, {b})");
    }
}
