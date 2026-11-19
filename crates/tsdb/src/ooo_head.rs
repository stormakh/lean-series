//! The in-memory out-of-order chunk (port of `tsdb/ooo_head.go`).

use std::sync::Arc;

use chunkenc::{Appender, Chunk, Encoding, ValueType, ENC_FLOAT_HISTOGRAM, ENC_FLOAT_HISTOGRAM_ST, ENC_HISTOGRAM,
    ENC_HISTOGRAM_ST, ENC_NONE, ENC_XOR, ENC_XOR2};
use model::histogram::{FloatHistogram, Histogram};
use storage::Sample;

use crate::head::MemChunk;

/// Go `OOOChunk`: samples in time-ascending order. Inserts for timestamps already seen are dropped. Samples are
/// stored uncompressed to allow easy sorting.
#[derive(Debug, Default)]
pub struct OooChunk {
    pub samples: Vec<Sample>,
}

impl OooChunk {
    /// Go `NewOOOChunk`.
    pub fn new() -> OooChunk {
        OooChunk { samples: Vec::with_capacity(4) }
    }

    /// Go `Insert`: inserts the sample such that order is maintained. Returns false if a sample with the same
    /// timestamp already exists.
    pub fn insert(
        &mut self,
        st: i64,
        t: i64,
        v: f64,
        h: Option<Box<Histogram>>,
        fh: Option<Box<FloatHistogram>>,
    ) -> bool {
        let s = Sample { st, t, f: v, h, fh };
        // Although out-of-order samples can be out-of-order amongst themselves, we are opinionated and expect
        // them to be usually in-order meaning we could try to append at the end first if the new timestamp is
        // higher than the last known timestamp.
        if self.samples.last().is_none_or(|l| t > l.t) {
            self.samples.push(s);
            return true;
        }
        // Find index of sample we should replace.
        let i = self.samples.partition_point(|x| x.t < t);
        if i >= self.samples.len() {
            self.samples.push(s);
            return true;
        }
        // Duplicate sample for timestamp is not allowed.
        if self.samples[i].t == t {
            return false;
        }
        self.samples.insert(i, s);
        true
    }

    /// Go `NumSamples`.
    pub fn num_samples(&self) -> usize {
        self.samples.len()
    }

    /// Go `ToEncodedChunks`: chunks with the samples in [mint, maxt]. `use_xor2` selects XOR2 encoding for float
    /// samples; `use_histogram_st` the ST-capable histogram encodings. As in Go, histogram samples may get their
    /// layout adjusted by the chunk appender (hence `&mut self`).
    pub fn to_encoded_chunks(
        &mut self,
        mint: i64,
        maxt: i64,
        use_xor2: bool,
        use_histogram_st: bool,
    ) -> Result<Vec<MemChunk>, chunkenc::Error> {
        let mut chks = Vec::new();
        if self.samples.is_empty() {
            return Ok(chks);
        }
        // The most common case is that there will be a single chunk, with the same type of samples in it - this
        // is always true for float samples.
        chks.reserve(1);
        let mut cmint = 0i64;
        let mut cmaxt = 0i64;
        let mut chunk: Option<Box<dyn Chunk>> = None;
        let mut app: Option<Appender> = None;
        let mut prev_encoding: Encoding = ENC_NONE;
        let push = |chks: &mut Vec<MemChunk>, c: Box<dyn Chunk>, mint: i64, maxt: i64| {
            chks.push(MemChunk {
                chunk: Arc::from(c),
                min_time: mint,
                max_time: maxt,
            })
        };
        for s in self.samples.iter_mut() {
            if s.t < mint {
                continue;
            }
            if s.t > maxt {
                break;
            }
            let encoding = if s.h.is_some() {
                ValueType::Histogram.chunk_encoding(use_xor2, use_histogram_st)
            } else if s.fh.is_some() {
                ValueType::FloatHistogram.chunk_encoding(use_xor2, use_histogram_st)
            } else {
                ValueType::Float.chunk_encoding(use_xor2, use_histogram_st)
            };

            if encoding != prev_encoding {
                // For the first sample, this will always be true.
                if prev_encoding != ENC_NONE
                    && let Some(c) = chunk.take()
                {
                    push(&mut chks, c, cmint, cmaxt);
                }
                cmint = s.t;
                let mut c = chunkenc::new_empty_chunk(encoding)?;
                app = Some(c.appender()?);
                chunk = Some(c);
            }
            // The appender of the previous sample is only consulted by the chunk appenders for the first sample
            // of a chunk, and at that point it always has another encoding (so Go's prevApp has no effect).
            let c = chunk.as_mut().expect("chunk");
            let a = app.as_mut().expect("appender");
            match encoding {
                ENC_XOR | ENC_XOR2 => a.append(&mut **c, s.st, s.t, s.f),
                ENC_HISTOGRAM | ENC_HISTOGRAM_ST => {
                    let h = s.h.as_deref_mut().expect("histogram sample");
                    let (new_chunk, recoded) = a.append_histogram(&mut **c, None, s.st, s.t, h, false)?;
                    if let Some((nc, napp)) = new_chunk {
                        // A new chunk was allocated.
                        let old = std::mem::replace(c, nc);
                        if !recoded {
                            push(&mut chks, old, cmint, cmaxt);
                            cmint = s.t;
                        }
                        *a = napp;
                    }
                }
                ENC_FLOAT_HISTOGRAM | ENC_FLOAT_HISTOGRAM_ST => {
                    let fh = s.fh.as_deref_mut().expect("float histogram sample");
                    let (new_chunk, recoded) = a.append_float_histogram(&mut **c, None, s.st, s.t, fh, false)?;
                    if let Some((nc, napp)) = new_chunk {
                        let old = std::mem::replace(c, nc);
                        if !recoded {
                            push(&mut chks, old, cmint, cmaxt);
                            cmint = s.t;
                        }
                        *a = napp;
                    }
                }
                _ => {}
            }
            cmaxt = s.t;
            prev_encoding = encoding;
        }
        if prev_encoding != ENC_NONE
            && let Some(c) = chunk
        {
            push(&mut chks, c, cmint, cmaxt);
        }
        Ok(chks)
    }
}
