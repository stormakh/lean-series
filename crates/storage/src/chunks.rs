//! The parts of `tsdb/chunks` that the storage interfaces need: chunk metadata ([`Meta`]), the chunk iterator of a
//! chunk series ([`MetaIterator`]), chunk references, and samples ([`Sample`], from `tsdb/chunks/samples.go` and the
//! `fSample`/`hSample`/`fhSample` types of `storage/buffer.go`).
//!
//! Note for the TSDB port: these types live here because `storage` cannot depend on `tsdb`; the TSDB crates
//! should use them (and add the file formats, head chunk references and so on next to them in their own crates).

use std::sync::Arc;

use chunkenc::{Chunk, ChunkIterator, ValueType};
use model::histogram::{FloatHistogram, Histogram};

use crate::error::Error;

/// A generic reference for reading chunk data (Go `chunks.ChunkRef`). In Prometheus it is either a head chunk
/// reference or a block chunk reference.
pub type ChunkRef = u64;

/// Information about one chunk (Go `chunks.Meta`).
#[derive(Clone, Debug, Default)]
pub struct Meta {
    /// A reference to retrieve the chunk data, when `chunk` is not set (Go `Ref`).
    pub chunk_ref: ChunkRef,
    /// The chunk, if loaded. Shared, so that metas are cheap to clone.
    pub chunk: Option<Arc<dyn Chunk>>,
    /// The time range the data covers. `max_time == i64::MAX` means the chunk is still open.
    pub min_time: i64,
    pub max_time: i64,
}

impl Meta {
    /// A meta with a chunk and its time range.
    pub fn new(chunk: Arc<dyn Chunk>, min_time: i64, max_time: i64) -> Meta {
        Meta {
            chunk_ref: 0,
            chunk: Some(chunk),
            min_time,
            max_time,
        }
    }

    /// The loaded chunk. Panics if the chunk is not loaded (Go dereferences a nil interface).
    pub fn chunk(&self) -> &dyn Chunk {
        match &self.chunk {
            Some(c) => &**c,
            None => panic!("chunks.Meta: chunk is not loaded"),
        }
    }

    /// Go `OverlapsClosedInterval`: whether the chunk overlaps [mint, maxt].
    pub fn overlaps_closed_interval(&self, mint: i64, maxt: i64) -> bool {
        self.min_time <= maxt && mint <= self.max_time
    }
}

/// Iterates over the chunks of one series (Go `chunks.Iterator`).
pub trait MetaIterator {
    /// Advances the iterator by one.
    fn next(&mut self) -> bool;
    /// The current meta. Whether the chunk is loaded depends on the implementation.
    fn at(&self) -> &Meta;
    /// The error, if `next` returned false because of one.
    fn err(&self) -> Option<Error>;
}

impl<T: MetaIterator + ?Sized> MetaIterator for Box<T> {
    fn next(&mut self) -> bool {
        (**self).next()
    }
    fn at(&self) -> &Meta {
        (**self).at()
    }
    fn err(&self) -> Option<Error> {
        (**self).err()
    }
}

/// A boxed chunk iterator that may borrow from its series.
pub type BoxMetaIterator<'a> = Box<dyn MetaIterator + 'a>;

/// A sample: a float, a histogram or a float histogram with timestamps (Go `chunks.Sample` and its
/// implementations). At most one of `h` and `fh` is set; with neither it is a float sample.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Sample {
    /// The start timestamp (0 if unknown).
    pub st: i64,
    pub t: i64,
    pub f: f64,
    pub h: Option<Box<Histogram>>,
    pub fh: Option<Box<FloatHistogram>>,
}

impl Sample {
    /// A float sample (Go `fSample{st, t, f}`).
    pub fn float(st: i64, t: i64, f: f64) -> Sample {
        Sample {
            st,
            t,
            f,
            h: None,
            fh: None,
        }
    }
    /// A histogram sample (Go `hSample{st, t, h}`).
    pub fn histogram(st: i64, t: i64, h: Histogram) -> Sample {
        Sample {
            st,
            t,
            f: 0.0,
            h: Some(Box::new(h)),
            fh: None,
        }
    }
    /// A float histogram sample (Go `fhSample{st, t, fh}`).
    pub fn float_histogram(st: i64, t: i64, fh: FloatHistogram) -> Sample {
        Sample {
            st,
            t,
            f: 0.0,
            h: None,
            fh: Some(Box::new(fh)),
        }
    }
    /// A sample of the given parts (Go `sample{st, t, f, h, fh}`).
    pub fn new(
        st: i64,
        t: i64,
        f: f64,
        h: Option<Histogram>,
        fh: Option<FloatHistogram>,
    ) -> Sample {
        Sample {
            st,
            t,
            f,
            h: h.map(Box::new),
            fh: fh.map(Box::new),
        }
    }

    #[inline]
    pub fn t(&self) -> i64 {
        self.t
    }
    #[inline]
    pub fn st(&self) -> i64 {
        self.st
    }
    #[inline]
    pub fn f(&self) -> f64 {
        self.f
    }
    pub fn h(&self) -> Option<&Histogram> {
        self.h.as_deref()
    }
    pub fn fh(&self) -> Option<&FloatHistogram> {
        self.fh.as_deref()
    }

    /// The value type (Go `Type()`).
    #[inline]
    pub fn typ(&self) -> ValueType {
        if self.h.is_some() {
            ValueType::Histogram
        } else if self.fh.is_some() {
            ValueType::FloatHistogram
        } else {
            ValueType::Float
        }
    }

    /// A deep copy (Go `Copy()`); the same as `clone`.
    pub fn copy(&self) -> Sample {
        self.clone()
    }
}

/// Go `chunks.GenerateSamples`: `num_samples` float samples with t = v = i, from `start`.
pub fn generate_samples(start: i64, num_samples: i64) -> Vec<Sample> {
    (start..start + num_samples)
        .map(|i| Sample::float(0, i, i as f64))
        .collect()
}

/// Go `chunks.ChunkFromSamples`: one chunk with all samples, which must have the same type. Meant for tests.
/// The chunk stores start timestamps when any sample has one.
pub fn chunk_from_samples(s: &[Sample]) -> Result<Meta, Error> {
    let empty_chunk = || Meta {
        chunk: Some(Arc::new(chunkenc::XorChunk::new()) as Arc<dyn Chunk>),
        ..Meta::default()
    };
    if s.is_empty() {
        return Ok(empty_chunk());
    }
    let (mint, maxt) = (s[0].t, s[s.len() - 1].t);
    let sample_type = s[0].typ();
    let has_st = s.iter().any(|x| x.st != 0);

    let mut c = sample_type.new_chunk(has_st, has_st)?;
    let mut ca = c
        .appender()
        .map_err(|e| Error::wrap("failed to create chunk appender", Error::Chunk(e)))?;
    let mut h = Histogram::default();
    let mut fh = FloatHistogram::default();
    for x in s {
        match sample_type {
            ValueType::Float => ca.append(&mut *c, x.st, x.t, x.f),
            ValueType::Histogram => {
                x.h.as_deref().expect("histogram sample").copy_to(&mut h);
                let (new_chunk, _) =
                    ca.append_histogram(&mut *c, None, x.st, x.t, &mut h, false)?;
                if new_chunk.is_some() {
                    return Err(Error::msg("did not expect to start a second chunk"));
                }
            }
            ValueType::FloatHistogram => {
                x.fh.as_deref()
                    .expect("float histogram sample")
                    .copy_to(&mut fh);
                let (new_chunk, _) =
                    ca.append_float_histogram(&mut *c, None, x.st, x.t, &mut fh, false)?;
                if new_chunk.is_some() {
                    return Err(Error::msg("did not expect to start a second chunk"));
                }
            }
            ValueType::None => panic!("unknown sample type {}", sample_type),
        }
    }
    Ok(Meta {
        chunk_ref: 0,
        chunk: Some(Arc::from(c)),
        min_time: mint,
        max_time: maxt,
    })
}

/// Go `chunks.ChunkMetasToSamples`: the samples of the chunks (without start timestamps). Used in tests.
pub fn chunk_metas_to_samples(chunks: &[Meta]) -> Vec<Sample> {
    let mut result = Vec::new();
    for chunk in chunks {
        let mut it = chunk.chunk().iterator(None);
        loop {
            match it.next() {
                ValueType::None => break,
                ValueType::Float => {
                    let (t, v) = it.at();
                    result.push(Sample::float(0, t, v));
                }
                ValueType::Histogram => {
                    let (t, h) = it.at_histogram(None);
                    result.push(Sample::histogram(0, t, h));
                }
                ValueType::FloatHistogram => {
                    let (t, fh) = it.at_float_histogram(None);
                    result.push(Sample::float_histogram(0, t, fh));
                }
            }
        }
    }
    result
}
