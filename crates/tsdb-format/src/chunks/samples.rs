//! Port of `tsdb/chunks/samples.go` and the sample helpers of `chunks.go` (`ChunkFromSamples`,
//! `ChunkMetasToSamples`). They are meant for tests.

use super::{Error, Meta};
use chunkenc::{ChunkIterator, ValueType, XorChunk};
use model::histogram::{FloatHistogram, Histogram};

/// Go `chunks.Sample`.
pub trait Sample: std::fmt::Debug {
    fn t(&self) -> i64;
    fn st(&self) -> i64;
    fn f(&self) -> f64;
    fn h(&self) -> Option<&Histogram>;
    fn fh(&self) -> Option<&FloatHistogram>;
    fn r#type(&self) -> ValueType;
    /// A deep copy.
    fn copy(&self) -> Box<dyn Sample>;
}

/// Go `chunks.sample`: a float, histogram or float histogram sample with an optional start timestamp.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BasicSample {
    pub st: i64,
    pub t: i64,
    pub f: f64,
    pub h: Option<Box<Histogram>>,
    pub fh: Option<Box<FloatHistogram>>,
}

impl BasicSample {
    /// A float sample.
    pub fn float(t: i64, f: f64) -> BasicSample {
        BasicSample {
            t,
            f,
            ..Default::default()
        }
    }

    /// A float sample with a start timestamp.
    pub fn float_st(st: i64, t: i64, f: f64) -> BasicSample {
        BasicSample {
            st,
            t,
            f,
            ..Default::default()
        }
    }

    /// A histogram sample.
    pub fn histogram(t: i64, h: Histogram) -> BasicSample {
        BasicSample {
            t,
            h: Some(Box::new(h)),
            ..Default::default()
        }
    }

    /// A float histogram sample.
    pub fn float_histogram(t: i64, fh: FloatHistogram) -> BasicSample {
        BasicSample {
            t,
            fh: Some(Box::new(fh)),
            ..Default::default()
        }
    }
}

impl Sample for BasicSample {
    fn t(&self) -> i64 {
        self.t
    }
    fn st(&self) -> i64 {
        self.st
    }
    fn f(&self) -> f64 {
        self.f
    }
    fn h(&self) -> Option<&Histogram> {
        self.h.as_deref()
    }
    fn fh(&self) -> Option<&FloatHistogram> {
        self.fh.as_deref()
    }
    fn r#type(&self) -> ValueType {
        if self.h.is_some() {
            ValueType::Histogram
        } else if self.fh.is_some() {
            ValueType::FloatHistogram
        } else {
            ValueType::Float
        }
    }
    fn copy(&self) -> Box<dyn Sample> {
        // As upstream, the copy does not keep the start timestamp.
        Box::new(BasicSample {
            st: 0,
            t: self.t,
            f: self.f,
            h: self.h.clone(),
            fh: self.fh.clone(),
        })
    }
}

impl Sample for Box<dyn Sample> {
    fn t(&self) -> i64 {
        (**self).t()
    }
    fn st(&self) -> i64 {
        (**self).st()
    }
    fn f(&self) -> f64 {
        (**self).f()
    }
    fn h(&self) -> Option<&Histogram> {
        (**self).h()
    }
    fn fh(&self) -> Option<&FloatHistogram> {
        (**self).fh()
    }
    fn r#type(&self) -> ValueType {
        (**self).r#type()
    }
    fn copy(&self) -> Box<dyn Sample> {
        (**self).copy()
    }
}

/// Go `GenerateSamples`: float samples starting at `start` (t = f = i), `num_samples` of them.
pub fn generate_samples(start: usize, num_samples: usize) -> Vec<BasicSample> {
    generate_samples_with(start, num_samples, |i| {
        BasicSample::float(i as i64, i as f64)
    })
}

/// Go `generateSamples`.
pub fn generate_samples_with<S>(
    start: usize,
    num_samples: usize,
    gen_fn: impl Fn(usize) -> S,
) -> Vec<S> {
    (start..start + num_samples).map(gen_fn).collect()
}

/// Go `ChunkFromSamples` / `ChunkFromSamplesGeneric`: a chunk of all samples, which must have the same type.
/// If any sample has a start timestamp, the chunk stores start timestamps. Not efficient; for tests.
pub fn chunk_from_samples<S: Sample>(s: &[S]) -> Result<Meta, Error> {
    if s.is_empty() {
        return Ok(Meta {
            chunk: Some(std::sync::Arc::new(XorChunk::new())),
            ..Default::default()
        });
    }
    let (mint, maxt) = (s[0].t(), s[s.len() - 1].t());
    let sample_type = s[0].r#type();
    let has_st = s.iter().any(|x| x.st() != 0);

    // Request storing ST in the chunk if available.
    let mut c = sample_type.new_chunk(has_st, has_st)?;
    let mut ca = c
        .appender()
        .map_err(|e| Error::Msg(format!("failed to create chunk appender: {e}")))?;

    for x in s {
        match sample_type {
            ValueType::Float => ca.append(&mut *c, x.st(), x.t(), x.f()),
            ValueType::Histogram => {
                let mut h = x.h().cloned().unwrap_or_default();
                let (new_chunk, _) =
                    ca.append_histogram(&mut *c, None, x.st(), x.t(), &mut h, false)?;
                if new_chunk.is_some() {
                    return Err(Error::msg("did not expect to start a second chunk"));
                }
            }
            ValueType::FloatHistogram => {
                let mut fh = x.fh().cloned().unwrap_or_default();
                let (new_chunk, _) =
                    ca.append_float_histogram(&mut *c, None, x.st(), x.t(), &mut fh, false)?;
                if new_chunk.is_some() {
                    return Err(Error::msg("did not expect to start a second chunk"));
                }
            }
            ValueType::None => panic!("unknown sample type {}", sample_type.as_str()),
        }
    }
    Ok(Meta {
        chunk_ref: 0,
        chunk: Some(std::sync::Arc::from(c)),
        min_time: mint,
        max_time: maxt,
    })
}

/// Go `ChunkMetasToSamples`: the samples of the chunks (without start timestamps). For tests.
pub fn chunk_metas_to_samples(chunks: &[Meta]) -> Vec<BasicSample> {
    let mut result = Vec::new();
    for chunk in chunks {
        let Some(c) = &chunk.chunk else { continue };
        let mut it = c.iterator(None);
        loop {
            match it.next() {
                ValueType::None => break,
                ValueType::Float => {
                    let (t, f) = it.at();
                    result.push(BasicSample::float(t, f));
                }
                ValueType::Histogram => {
                    let (t, h) = it.at_histogram(None);
                    result.push(BasicSample::histogram(t, h));
                }
                ValueType::FloatHistogram => {
                    let (t, fh) = it.at_float_histogram(None);
                    result.push(BasicSample::float_histogram(t, fh));
                }
            }
        }
    }
    result
}
