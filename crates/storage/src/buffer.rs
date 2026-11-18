//! Port of `storage/buffer.go`: an iterator with a look-back buffer (a ring of samples within `delta` before
//! the current sample). Floats, histograms and float histograms have their own buffers, so the common case of
//! one sample type does not box samples; histogram buffers reuse their allocations.

use chunkenc::{AnyIterator, ChunkIterator, NopIterator, ValueType};
use model::histogram::{FloatHistogram, Histogram};

use crate::chunks::Sample;

/// A float sample in the ring (Go `fSample`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct FSample {
    pub st: i64,
    pub t: i64,
    pub f: f64,
}

/// A histogram sample in the ring (Go `hSample`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HSample {
    pub st: i64,
    pub t: i64,
    pub h: Histogram,
}

/// A float histogram sample in the ring (Go `fhSample`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FHSample {
    pub st: i64,
    pub t: i64,
    pub fh: FloatHistogram,
}

/// Wraps an iterator with a look-back buffer (Go `BufferedSeriesIterator`).
pub struct BufferedSeriesIterator<'a> {
    h_reader: Histogram,
    fh_reader: FloatHistogram,
    it: AnyIterator<'a>,
    buf: SampleRing,
    delta: i64,
    last_time: i64,
    value_type: ValueType,
}

/// Go `NewBuffer`: buffers the values within `delta` before the current element; starts with an empty iterator
/// (use `reset`).
pub fn new_buffer<'a>(delta: i64) -> BufferedSeriesIterator<'a> {
    new_buffer_iterator(AnyIterator::Nop(NopIterator), delta)
}

/// Go `NewBufferIterator`.
pub fn new_buffer_iterator(it: AnyIterator<'_>, delta: i64) -> BufferedSeriesIterator<'_> {
    let mut b = BufferedSeriesIterator {
        h_reader: Histogram::default(),
        fh_reader: FloatHistogram::default(),
        it: AnyIterator::Nop(NopIterator),
        buf: SampleRing::new(delta, 0, ValueType::None),
        delta,
        last_time: i64::MIN,
        value_type: ValueType::None,
    };
    b.reset(it);
    b
}

impl<'a> BufferedSeriesIterator<'a> {
    /// Reuses the buffer with a new iterator and the original delta. Returns the previous iterator, so that the
    /// caller can reuse it.
    pub fn reset(&mut self, it: AnyIterator<'a>) -> AnyIterator<'a> {
        let old = std::mem::replace(&mut self.it, it);
        self.last_time = i64::MIN;
        self.buf.reset();
        self.buf.delta = self.delta;
        self.value_type = self.it.next();
        old
    }

    /// Lowers the buffered delta, for the current iterator only.
    pub fn reduce_delta(&mut self, delta: i64) -> bool {
        self.buf.reduce_delta(delta)
    }

    /// The nth previous element (1 is the most recent buffered one), if buffered.
    pub fn peek_back(&self, n: usize) -> Option<Sample> {
        self.buf.nth_last(n)
    }

    /// An iterator over the buffered data.
    pub fn buffer(&self) -> SampleRingIterator<'_> {
        self.buf.iterator()
    }

    /// The underlying iterator.
    pub fn inner(&self) -> &AnyIterator<'a> {
        &self.it
    }

    fn add_current(&mut self) {
        match self.value_type {
            ValueType::None => {}
            ValueType::Float => {
                let (t, f) = self.it.at();
                let st = self.it.at_st();
                self.buf.add_f(FSample { st, t, f });
            }
            ValueType::Histogram => {
                let (t, h) = self.it.at_histogram(Some(std::mem::take(&mut self.h_reader)));
                let st = self.it.at_st();
                self.buf.add_h(st, t, &h);
                self.h_reader = h;
            }
            ValueType::FloatHistogram => {
                let (t, fh) = self.it.at_float_histogram(Some(std::mem::take(&mut self.fh_reader)));
                let st = self.it.at_st();
                self.buf.add_fh(st, t, &fh);
                self.fh_reader = fh;
            }
        }
    }
}

impl ChunkIterator for BufferedSeriesIterator<'_> {
    /// Advances to the element at time t or later.
    fn seek(&mut self, t: i64) -> ValueType {
        let t0 = t - self.buf.delta;
        // If the delta would make us seek backwards, keep the buffer and advance normally, filling it.
        if self.value_type != ValueType::None && t0 > self.last_time {
            self.buf.reset();
            self.value_type = self.it.seek(t0);
            match self.value_type {
                ValueType::None => return ValueType::None,
                _ => self.last_time = self.at_t(),
            }
        }
        if self.last_time >= t {
            return self.value_type;
        }
        loop {
            self.value_type = self.next();
            if self.value_type == ValueType::None || self.last_time >= t {
                return self.value_type;
            }
        }
    }

    /// Advances to the next element, adding the current one to the buffer.
    fn next(&mut self) -> ValueType {
        if self.value_type == ValueType::None {
            return ValueType::None;
        }
        self.add_current();
        self.value_type = self.it.next();
        if self.value_type != ValueType::None {
            self.last_time = self.at_t();
        }
        self.value_type
    }

    fn at(&self) -> (i64, f64) {
        self.it.at()
    }
    fn at_histogram(&self, h: Option<Histogram>) -> (i64, Histogram) {
        self.it.at_histogram(h)
    }
    fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        self.it.at_float_histogram(fh)
    }
    fn at_t(&self) -> i64 {
        self.it.at_t()
    }
    fn at_st(&self) -> i64 {
        self.it.at_st()
    }
    fn err(&self) -> Option<&chunkenc::Error> {
        self.it.err()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BufType {
    /// Nothing stored yet.
    None,
    /// Mixed samples.
    I,
    F,
    H,
    FH,
}

/// The look-back ring buffer (Go `sampleRing`). Only one of the four buffers is in use.
#[derive(Debug)]
pub struct SampleRing {
    delta: i64,
    i_buf: Vec<Sample>,
    f_buf: Vec<FSample>,
    h_buf: Vec<HSample>,
    fh_buf: Vec<FHSample>,
    buf_in_use: BufType,
    /// Position of the most recent element.
    i: isize,
    /// Position of the first element.
    f: usize,
    /// Number of elements.
    l: usize,
}

/// Grows the ring if it is full and moves `r.i` to the slot for the new element.
fn grow_and_advance<T: Default + Clone>(buf: &mut Vec<T>, r_i: &mut isize, r_f: &mut usize, r_l: usize) {
    let mut l = buf.len();
    if l == 0 {
        *buf = vec![T::default(); 16];
        l = 16;
    }
    if l == r_l {
        let mut new_buf = vec![T::default(); 2 * l];
        let f = *r_f;
        new_buf[l + f..].clone_from_slice(&buf[f..]);
        new_buf[..f].clone_from_slice(&buf[..f]);
        *buf = new_buf;
        *r_i = f as isize;
        *r_f += l;
    } else {
        *r_i += 1;
        if *r_i >= l as isize {
            *r_i -= l as isize;
        }
    }
}

/// Frees the head of the buffer of samples that fell out of the range.
fn free_head<T>(buf: &[T], t_of: impl Fn(&T) -> i64, tmin: i64, r_f: &mut usize, r_l: &mut usize) {
    let l = buf.len();
    while t_of(&buf[*r_f]) < tmin {
        *r_f += 1;
        if *r_f >= l {
            *r_f -= l;
        }
        *r_l -= 1;
    }
}

impl SampleRing {
    /// Go `newSampleRing`. With size 0 the first add allocates 16 slots of the type of the first sample.
    pub fn new(delta: i64, size: usize, typ: ValueType) -> SampleRing {
        let mut r = SampleRing {
            delta,
            i_buf: Vec::new(),
            f_buf: Vec::new(),
            h_buf: Vec::new(),
            fh_buf: Vec::new(),
            buf_in_use: BufType::None,
            i: -1,
            f: 0,
            l: 0,
        };
        if size == 0 {
            return r;
        }
        match typ {
            ValueType::Float => r.f_buf = vec![FSample::default(); size],
            ValueType::Histogram => r.h_buf = vec![HSample::default(); size],
            ValueType::FloatHistogram => r.fh_buf = vec![FHSample::default(); size],
            ValueType::None => {}
        }
        r
    }

    /// Go `reset`.
    pub fn reset(&mut self) {
        self.l = 0;
        self.i = -1;
        self.f = 0;
        self.buf_in_use = BufType::None;
        // The first sample after a reset goes to a specialized buffer; the mixed buffer is copied from it later.
        self.i_buf.clear();
    }

    /// The number of buffered samples.
    pub fn len(&self) -> usize {
        self.l
    }

    pub fn is_empty(&self) -> bool {
        self.l == 0
    }

    /// The current delta.
    pub fn delta(&self) -> i64 {
        self.delta
    }

    /// An iterator over the buffered samples (Go `iterator`).
    pub fn iterator(&self) -> SampleRingIterator<'_> {
        SampleRingIterator { r: self, i: -1, st: 0, t: 0, f: 0.0, h: None, fh: None }
    }

    fn at_i(&self, i: usize) -> &Sample {
        &self.i_buf[(self.f + i) % self.i_buf.len()]
    }
    fn at_f(&self, i: usize) -> FSample {
        self.f_buf[(self.f + i) % self.f_buf.len()]
    }
    fn at_h(&self, i: usize) -> &HSample {
        &self.h_buf[(self.f + i) % self.h_buf.len()]
    }
    fn at_fh(&self, i: usize) -> &FHSample {
        &self.fh_buf[(self.f + i) % self.fh_buf.len()]
    }

    /// Go `add`: adds any sample, converting the ring to the mixed buffer when the type differs.
    pub fn add(&mut self, s: &Sample) {
        let typ = s.typ();
        if self.buf_in_use == BufType::None {
            // First sample.
            match typ {
                ValueType::Float => {
                    self.buf_in_use = BufType::F;
                    self.add_f_inner(FSample { st: s.st, t: s.t, f: s.f });
                }
                ValueType::Histogram => {
                    self.buf_in_use = BufType::H;
                    self.add_h_inner(s.st, s.t, s.h.as_deref().expect("histogram"));
                }
                ValueType::FloatHistogram => {
                    self.buf_in_use = BufType::FH;
                    self.add_fh_inner(s.st, s.t, s.fh.as_deref().expect("float histogram"));
                }
                ValueType::None => {}
            }
            return;
        }
        if self.buf_in_use != BufType::I {
            // Stay specialized if the sample fits.
            match (typ, self.buf_in_use) {
                (ValueType::Float, BufType::F) => return self.add_f_inner(FSample { st: s.st, t: s.t, f: s.f }),
                (ValueType::Histogram, BufType::H) => {
                    return self.add_h_inner(s.st, s.t, s.h.as_deref().expect("histogram"));
                }
                (ValueType::FloatHistogram, BufType::FH) => {
                    return self.add_fh_inner(s.st, s.t, s.fh.as_deref().expect("float histogram"));
                }
                _ => {}
            }
            // Copy the specialized buffer into the mixed buffer (which is empty here), keeping the positions.
            match self.buf_in_use {
                BufType::F => {
                    self.i_buf.extend(self.f_buf.iter().map(|x| Sample::float(x.st, x.t, x.f)));
                    self.f_buf = Vec::new();
                }
                BufType::H => {
                    self.i_buf.extend(self.h_buf.drain(..).map(|x| Sample::histogram(x.st, x.t, x.h)));
                    self.h_buf = Vec::new();
                }
                BufType::FH => {
                    self.i_buf.extend(self.fh_buf.drain(..).map(|x| Sample::float_histogram(x.st, x.t, x.fh)));
                    self.fh_buf = Vec::new();
                }
                _ => {}
            }
            self.buf_in_use = BufType::I;
        }
        self.add_sample(s.clone());
    }

    /// Go `addF`.
    pub fn add_f(&mut self, s: FSample) {
        match self.buf_in_use {
            BufType::F => self.add_f_inner(s),
            BufType::None => {
                self.add_f_inner(s);
                self.buf_in_use = BufType::F;
            }
            BufType::I => self.add_sample(Sample::float(s.st, s.t, s.f)),
            _ => self.add(&Sample::float(s.st, s.t, s.f)),
        }
    }

    /// Go `addH`: the histogram is copied into the ring.
    pub fn add_h(&mut self, st: i64, t: i64, h: &Histogram) {
        match self.buf_in_use {
            BufType::H => self.add_h_inner(st, t, h),
            BufType::None => {
                self.add_h_inner(st, t, h);
                self.buf_in_use = BufType::H;
            }
            BufType::I => self.add_sample(Sample::histogram(st, t, h.clone())),
            _ => self.add(&Sample::histogram(st, t, h.clone())),
        }
    }

    /// Go `addFH`: the float histogram is copied into the ring.
    pub fn add_fh(&mut self, st: i64, t: i64, fh: &FloatHistogram) {
        match self.buf_in_use {
            BufType::FH => self.add_fh_inner(st, t, fh),
            BufType::None => {
                self.add_fh_inner(st, t, fh);
                self.buf_in_use = BufType::FH;
            }
            BufType::I => self.add_sample(Sample::float_histogram(st, t, fh.clone())),
            _ => self.add(&Sample::float_histogram(st, t, fh.clone())),
        }
    }

    fn add_sample(&mut self, s: Sample) {
        grow_and_advance(&mut self.i_buf, &mut self.i, &mut self.f, self.l);
        let tmin = s.t - self.delta;
        let i = self.i as usize;
        self.i_buf[i] = s;
        self.l += 1;
        free_head(&self.i_buf, |x| x.t, tmin, &mut self.f, &mut self.l);
    }

    fn add_f_inner(&mut self, s: FSample) {
        grow_and_advance(&mut self.f_buf, &mut self.i, &mut self.f, self.l);
        let i = self.i as usize;
        self.f_buf[i] = s;
        self.l += 1;
        free_head(&self.f_buf, |x| x.t, s.t - self.delta, &mut self.f, &mut self.l);
    }

    fn add_h_inner(&mut self, st: i64, t: i64, h: &Histogram) {
        grow_and_advance(&mut self.h_buf, &mut self.i, &mut self.f, self.l);
        let slot = &mut self.h_buf[self.i as usize];
        slot.t = t;
        slot.st = st;
        h.copy_to(&mut slot.h);
        self.l += 1;
        free_head(&self.h_buf, |x| x.t, t - self.delta, &mut self.f, &mut self.l);
    }

    fn add_fh_inner(&mut self, st: i64, t: i64, fh: &FloatHistogram) {
        grow_and_advance(&mut self.fh_buf, &mut self.i, &mut self.f, self.l);
        let slot = &mut self.fh_buf[self.i as usize];
        slot.t = t;
        slot.st = st;
        fh.copy_to(&mut slot.fh);
        self.l += 1;
        free_head(&self.fh_buf, |x| x.t, t - self.delta, &mut self.f, &mut self.l);
    }

    /// Go `reduceDelta`: lowers the delta and drops samples out of the new range.
    pub fn reduce_delta(&mut self, delta: i64) -> bool {
        if delta > self.delta {
            return false;
        }
        self.delta = delta;
        if self.l == 0 {
            return true;
        }
        let i = self.i as usize;
        match self.buf_in_use {
            BufType::F => free_head(&self.f_buf, |x| x.t, self.f_buf[i].t - delta, &mut self.f, &mut self.l),
            BufType::H => free_head(&self.h_buf, |x| x.t, self.h_buf[i].t - delta, &mut self.f, &mut self.l),
            BufType::FH => free_head(&self.fh_buf, |x| x.t, self.fh_buf[i].t - delta, &mut self.f, &mut self.l),
            _ => free_head(&self.i_buf, |x| x.t, self.i_buf[i].t - delta, &mut self.f, &mut self.l),
        }
        true
    }

    /// Go `nthLast`: the nth most recent element (a copy).
    pub fn nth_last(&self, n: usize) -> Option<Sample> {
        if n > self.l {
            return None;
        }
        let i = self.l - n;
        Some(match self.buf_in_use {
            BufType::F => {
                let s = self.at_f(i);
                Sample::float(s.st, s.t, s.f)
            }
            BufType::H => {
                let s = self.at_h(i);
                Sample::histogram(s.st, s.t, s.h.clone())
            }
            BufType::FH => {
                let s = self.at_fh(i);
                Sample::float_histogram(s.st, s.t, s.fh.clone())
            }
            _ => self.at_i(i).clone(),
        })
    }

    /// Go `samples`: copies of the buffered samples, oldest first.
    pub fn samples(&self) -> Vec<Sample> {
        (0..self.l)
            .map(|i| match self.buf_in_use {
                BufType::F => {
                    let s = self.at_f(i);
                    Sample::float(s.st, s.t, s.f)
                }
                BufType::H => {
                    let s = self.at_h(i);
                    Sample::histogram(s.st, s.t, s.h.clone())
                }
                BufType::FH => {
                    let s = self.at_fh(i);
                    Sample::float_histogram(s.st, s.t, s.fh.clone())
                }
                _ => self.at_i(i).clone(),
            })
            .collect()
    }
}

/// Iterates over the samples buffered in the look-back window (Go `SampleRingIterator`). Histograms are
/// borrowed from the ring.
pub struct SampleRingIterator<'r> {
    r: &'r SampleRing,
    i: isize,
    st: i64,
    t: i64,
    f: f64,
    h: Option<&'r Histogram>,
    fh: Option<&'r FloatHistogram>,
}

impl<'r> SampleRingIterator<'r> {
    pub fn next(&mut self) -> ValueType {
        self.i += 1;
        if self.i as usize >= self.r.l {
            return ValueType::None;
        }
        let i = self.i as usize;
        match self.r.buf_in_use {
            BufType::F => {
                let s = self.r.at_f(i);
                self.st = s.st;
                self.t = s.t;
                self.f = s.f;
                return ValueType::Float;
            }
            BufType::H => {
                let s = self.r.at_h(i);
                self.st = s.st;
                self.t = s.t;
                self.h = Some(&s.h);
                return ValueType::Histogram;
            }
            BufType::FH => {
                let s = self.r.at_fh(i);
                self.st = s.st;
                self.t = s.t;
                self.fh = Some(&s.fh);
                return ValueType::FloatHistogram;
            }
            _ => {}
        }
        let s = self.r.at_i(i);
        self.st = s.st;
        self.t = s.t;
        match s.typ() {
            ValueType::Histogram => {
                self.h = s.h.as_deref();
                self.fh = None;
                ValueType::Histogram
            }
            ValueType::FloatHistogram => {
                self.fh = s.fh.as_deref();
                self.h = None;
                ValueType::FloatHistogram
            }
            _ => {
                self.f = s.f;
                ValueType::Float
            }
        }
    }

    /// The current float element.
    pub fn at(&self) -> (i64, f64) {
        (self.t, self.f)
    }

    /// The current histogram, borrowed from the ring.
    pub fn at_histogram(&self) -> (i64, Option<&'r Histogram>) {
        (self.t, self.h)
    }

    /// The current histogram as a float histogram; an integer histogram is converted. `fh` is reused.
    pub fn at_float_histogram(&self, fh: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        match self.fh {
            None => {
                let mut out = fh.unwrap_or_default();
                self.h.expect("histogram sample").to_float_into(&mut out);
                (self.t, out)
            }
            Some(src) => match fh {
                Some(mut out) => {
                    src.copy_to(&mut out);
                    (self.t, out)
                }
                None => (self.t, src.copy()),
            },
        }
    }

    /// The current float histogram, borrowed, if the sample is one.
    pub fn at_float_histogram_ref(&self) -> Option<&'r FloatHistogram> {
        self.fh
    }

    pub fn at_t(&self) -> i64 {
        self.t
    }

    pub fn at_st(&self) -> i64 {
        self.st
    }
}
