//! Integer histogram (port of `model/histogram/histogram.go`).

use super::*;

/// A sparse, high-resolution histogram with integer counts. Bucket counts are deltas: the first element is an
/// absolute count, all following ones are deltas relative to the previous element.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Histogram {
    /// Counter reset information.
    pub counter_reset_hint: CounterResetHint,
    /// -4 <= n <= 8 for exponential buckets, or -53 for custom buckets.
    pub schema: i32,
    /// Width of the zero bucket.
    pub zero_threshold: f64,
    /// Observations falling into the zero bucket.
    pub zero_count: u64,
    /// Total number of observations.
    pub count: u64,
    /// Sum of observations. This is also used as the stale marker.
    pub sum: f64,
    pub positive_spans: Vec<Span>,
    pub negative_spans: Vec<Span>,
    pub positive_buckets: Vec<i64>,
    pub negative_buckets: Vec<i64>,
    /// Custom (usually upper) bounds for the custom buckets schema, otherwise `None`. Interned.
    pub custom_values: CustomValues,
}

impl Histogram {
    pub fn uses_custom_buckets(&self) -> bool {
        is_custom_buckets_schema(self.schema)
    }

    /// The custom values as a slice (empty for `None`).
    #[inline]
    pub fn custom_values_slice(&self) -> &[f64] {
        cv_slice(&self.custom_values)
    }

    /// Returns a deep copy (Go `Copy`). Fields unused by the schema are not copied.
    pub fn copy(&self) -> Histogram {
        let mut c = Histogram {
            counter_reset_hint: self.counter_reset_hint,
            schema: self.schema,
            count: self.count,
            sum: self.sum,
            ..Default::default()
        };
        if self.uses_custom_buckets() {
            c.custom_values = self.custom_values.clone();
        } else {
            c.zero_threshold = self.zero_threshold;
            c.zero_count = self.zero_count;
            c.negative_spans = self.negative_spans.clone();
            c.negative_buckets = self.negative_buckets.clone();
        }
        c.positive_spans = self.positive_spans.clone();
        c.positive_buckets = self.positive_buckets.clone();
        c
    }

    /// Makes a deep copy into `to`, reusing its allocations.
    pub fn copy_to(&self, to: &mut Histogram) {
        to.counter_reset_hint = self.counter_reset_hint;
        to.schema = self.schema;
        to.count = self.count;
        to.sum = self.sum;
        if self.uses_custom_buckets() {
            to.zero_threshold = 0.0;
            to.zero_count = 0;
            clear_vec(&mut to.negative_spans);
            clear_vec(&mut to.negative_buckets);
            to.custom_values = self.custom_values.clone();
        } else {
            to.zero_threshold = self.zero_threshold;
            to.zero_count = self.zero_count;
            copy_into(&mut to.negative_spans, &self.negative_spans);
            copy_into(&mut to.negative_buckets, &self.negative_buckets);
            to.custom_values = None;
        }
        copy_into(&mut to.positive_spans, &self.positive_spans);
        copy_into(&mut to.positive_buckets, &self.positive_buckets);
    }

    /// Go `String()`.
    pub fn string(&self) -> String {
        use std::fmt::Write;
        let mut sb = String::new();
        let _ = write!(sb, "{{count:{}, sum:", self.count);
        gofmt::write_g(&mut sb, self.sum);

        let mut n_buckets: Vec<Bucket<u64>> = Vec::new();
        let mut it = self.negative_bucket_iterator();
        while it.next() {
            let b = it.at();
            if b.count != 0 {
                n_buckets.push(b);
            }
        }
        for b in n_buckets.iter().rev() {
            sb.push_str(", ");
            b.write_to(&mut sb);
        }
        if self.zero_count != 0 {
            sb.push_str(", ");
            self.zero_bucket().write_to(&mut sb);
        }
        let mut it = self.positive_bucket_iterator();
        while it.next() {
            let b = it.at();
            if b.count != 0 {
                sb.push_str(", ");
                b.write_to(&mut sb);
            }
        }
        sb.push('}');
        sb
    }

    /// Returns the zero bucket. Panics for custom buckets.
    pub fn zero_bucket(&self) -> Bucket<u64> {
        if self.uses_custom_buckets() {
            panic!("histograms with custom buckets have no zero bucket");
        }
        Bucket {
            lower: -self.zero_threshold,
            upper: self.zero_threshold,
            lower_inclusive: true,
            upper_inclusive: true,
            count: self.zero_count,
            index: 0,
        }
    }

    /// Iterates over all positive buckets in ascending order.
    pub fn positive_bucket_iterator(&self) -> RegularBucketIterator<'_> {
        RegularBucketIterator::new(&self.positive_spans, &self.positive_buckets, self.schema, true, self.custom_values_slice())
    }

    /// Iterates over all negative buckets in descending order (starting next to the zero bucket).
    pub fn negative_bucket_iterator(&self) -> RegularBucketIterator<'_> {
        RegularBucketIterator::new(&self.negative_spans, &self.negative_buckets, self.schema, false, &[])
    }

    /// Cumulative view of the buckets. Panics if the histogram has negative buckets. Only used for testing.
    pub fn cumulative_bucket_iterator(&self) -> CumulativeBucketIterator<'_> {
        if !self.negative_buckets.is_empty() {
            panic!("CumulativeBucketIterator called on Histogram with negative buckets");
        }
        CumulativeBucketIterator {
            h: self,
            pos_spans_idx: -1,
            pos_buckets_idx: 0,
            idx_in_span: 0,
            initialized: false,
            curr_idx: 0,
            curr_upper: 0.0,
            curr_count: 0,
            curr_cumulative_count: 0,
            empty_bucket_count: 0,
        }
    }

    /// Returns true if the given histogram matches exactly (sum compared by bit pattern; spans compared by the
    /// bucket layout they represent).
    pub fn equals(&self, h2: &Histogram) -> bool {
        if self.schema != h2.schema || self.count != h2.count || self.sum.to_bits() != h2.sum.to_bits() {
            return false;
        }
        if self.uses_custom_buckets() && !custom_bucket_bounds_match(self.custom_values_slice(), h2.custom_values_slice()) {
            return false;
        }
        if self.zero_threshold != h2.zero_threshold || self.zero_count != h2.zero_count {
            return false;
        }
        if !spans_match(&self.negative_spans, &h2.negative_spans) {
            return false;
        }
        if self.negative_buckets != h2.negative_buckets {
            return false;
        }
        if !spans_match(&self.positive_spans, &h2.positive_spans) {
            return false;
        }
        if self.positive_buckets != h2.positive_buckets {
            return false;
        }
        true
    }

    /// Works like `FloatHistogram::compact`.
    pub fn compact(&mut self, max_empty_buckets: usize) -> &mut Histogram {
        compact_buckets(&mut self.positive_buckets, None, &mut self.positive_spans, max_empty_buckets, true);
        compact_buckets(&mut self.negative_buckets, None, &mut self.negative_spans, max_empty_buckets, true);
        self
    }

    /// Returns a FloatHistogram representation (deep copy).
    pub fn to_float(&self) -> FloatHistogram {
        let mut fh = FloatHistogram::default();
        self.to_float_into(&mut fh);
        fh
    }

    /// Go `ToFloat(fh)`: writes the float representation into `fh`, reusing its allocations.
    pub fn to_float_into(&self, fh: &mut FloatHistogram) {
        fh.counter_reset_hint = self.counter_reset_hint;
        fh.schema = self.schema;
        fh.count = self.count as f64;
        fh.sum = self.sum;
        if self.uses_custom_buckets() {
            fh.zero_threshold = 0.0;
            fh.zero_count = 0.0;
            clear_vec(&mut fh.negative_spans);
            clear_vec(&mut fh.negative_buckets);
            fh.custom_values = self.custom_values.clone();
        } else {
            fh.zero_threshold = self.zero_threshold;
            fh.zero_count = self.zero_count as f64;
            copy_into(&mut fh.negative_spans, &self.negative_spans);
            fh.negative_buckets.clear();
            let mut cur: f64 = 0.0;
            for &b in &self.negative_buckets {
                cur += b as f64;
                fh.negative_buckets.push(cur);
            }
            fh.custom_values = None;
        }
        copy_into(&mut fh.positive_spans, &self.positive_spans);
        fh.positive_buckets.clear();
        let mut cur: f64 = 0.0;
        for &b in &self.positive_buckets {
            cur += b as f64;
            fh.positive_buckets.push(cur);
        }
    }

    /// Validates consistency between span and bucket slices, negative counts, and schema specific fields.
    pub fn validate(&self) -> Result<(), Error> {
        let mut n_count: u64 = 0;
        let mut p_count: u64 = 0;
        if is_custom_buckets_schema(self.schema) {
            check_histogram_custom_bounds(self.custom_values_slice(), &self.positive_spans, self.positive_buckets.len())
                .map_err(|e| e.with_prefix("custom buckets: "))?;
            if self.zero_count != 0 {
                return Err(Error::new(ErrorKind::HistogramCustomBucketsZeroCount));
            }
            if self.zero_threshold != 0.0 {
                return Err(Error::new(ErrorKind::HistogramCustomBucketsZeroThresh));
            }
            if !self.negative_spans.is_empty() {
                return Err(Error::new(ErrorKind::HistogramCustomBucketsNegSpans));
            }
            if !self.negative_buckets.is_empty() {
                return Err(Error::new(ErrorKind::HistogramCustomBucketsNegBuckets));
            }
        } else if is_exponential_schema(self.schema) {
            check_histogram_spans(&self.positive_spans, self.positive_buckets.len())
                .map_err(|e| e.with_prefix("positive side: "))?;
            check_histogram_spans(&self.negative_spans, self.negative_buckets.len())
                .map_err(|e| e.with_prefix("negative side: "))?;
            check_histogram_buckets_int(&self.negative_buckets, &mut n_count, true)
                .map_err(|e| e.with_prefix("negative side: "))?;
            if self.custom_values.is_some() {
                return Err(Error::new(ErrorKind::HistogramExpSchemaCustomBounds));
            }
        } else {
            return Err(invalid_schema_error(self.schema));
        }
        check_histogram_buckets_int(&self.positive_buckets, &mut p_count, true)
            .map_err(|e| e.with_prefix("positive side: "))?;

        let sum_of_buckets = n_count.wrapping_add(p_count).wrapping_add(self.zero_count);
        if self.sum.is_nan() {
            if sum_of_buckets > self.count {
                return Err(Error::wrapped(
                    ErrorKind::HistogramCountNotBigEnough,
                    format!(
                        "{} observations found in buckets, but the Count field is {}: {}",
                        sum_of_buckets,
                        self.count,
                        ErrorKind::HistogramCountNotBigEnough.message()
                    ),
                ));
            }
        } else if sum_of_buckets != self.count {
            return Err(Error::wrapped(
                ErrorKind::HistogramCountMismatch,
                format!(
                    "{} observations found in buckets, but the Count field is {}: {}",
                    sum_of_buckets,
                    self.count,
                    ErrorKind::HistogramCountMismatch.message()
                ),
            ));
        }
        Ok(())
    }

    /// Reduces the histogram's spans and buckets into the lower `target_schema`.
    pub fn reduce_resolution(&mut self, target_schema: i32) -> Result<(), Error> {
        if self.uses_custom_buckets() {
            return Err(Error::plain("cannot reduce resolution when there are custom buckets"));
        }
        if is_custom_buckets_schema(target_schema) {
            return Err(Error::plain("cannot reduce resolution to custom buckets schema"));
        }
        if target_schema >= self.schema {
            return Err(Error::plain(format!("cannot reduce resolution from schema {} to {}", self.schema, target_schema)));
        }
        reduce_resolution_in_place(&mut self.positive_spans, &mut self.positive_buckets, self.schema, target_schema, true)?;
        reduce_resolution_in_place(&mut self.negative_spans, &mut self.negative_buckets, self.schema, target_schema, true)?;
        self.schema = target_schema;
        Ok(())
    }
}

impl std::fmt::Display for Histogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.string())
    }
}

/// Returns true if both spans represent the same bucket layout after combining zero length spans with the next
/// non-zero length span.
pub(crate) fn spans_match(s1: &[Span], s2: &[Span]) -> bool {
    if s1.is_empty() && s2.is_empty() {
        return true;
    }
    let (mut i1, mut i2) = (0usize, 0usize);
    loop {
        if i1 >= s1.len() {
            return all_empty_spans(&s2[i2..]);
        }
        if i2 >= s2.len() {
            return all_empty_spans(&s1[i1..]);
        }
        let mut c1 = s1[i1];
        let mut c2 = s2[i2];
        i1 += 1;
        i2 += 1;
        if c1.length == 0 {
            while i1 < s1.len() && s1[i1].length == 0 {
                c1.offset = c1.offset.wrapping_add(s1[i1].offset);
                i1 += 1;
            }
            if i1 < s1.len() {
                c1.offset = c1.offset.wrapping_add(s1[i1].offset);
                c1.length = s1[i1].length;
                i1 += 1;
            }
        }
        if c2.length == 0 {
            while i2 < s2.len() && s2[i2].length == 0 {
                c2.offset = c2.offset.wrapping_add(s2[i2].offset);
                i2 += 1;
            }
            if i2 < s2.len() {
                c2.offset = c2.offset.wrapping_add(s2[i2].offset);
                c2.length = s2[i2].length;
                i2 += 1;
            }
        }
        if c1.length == 0 && c2.length == 0 {
            return true;
        }
        if c1.offset != c2.offset || c1.length != c2.length {
            return false;
        }
    }
}

fn all_empty_spans(s: &[Span]) -> bool {
    s.iter().all(|x| x.length == 0)
}

/// Bucket iterator over integer (delta) buckets.
#[derive(Clone, Debug)]
pub struct RegularBucketIterator<'a> {
    schema: i32,
    spans: &'a [Span],
    buckets: &'a [i64],
    positive: bool,
    spans_idx: usize,
    idx_in_span: u32,
    buckets_idx: usize,
    curr_count: i64,
    curr_idx: i32,
    custom_values: &'a [f64],
}

impl<'a> RegularBucketIterator<'a> {
    pub fn new(spans: &'a [Span], buckets: &'a [i64], schema: i32, positive: bool, custom_values: &'a [f64]) -> Self {
        RegularBucketIterator {
            schema,
            spans,
            buckets,
            positive,
            spans_idx: 0,
            idx_in_span: 0,
            buckets_idx: 0,
            curr_count: 0,
            curr_idx: 0,
            custom_values,
        }
    }
}

impl BucketIterator<u64> for RegularBucketIterator<'_> {
    fn next(&mut self) -> bool {
        if self.spans_idx >= self.spans.len() {
            return false;
        }
        let mut span = self.spans[self.spans_idx];
        if self.buckets_idx == 0 {
            self.curr_idx = span.offset;
        } else {
            self.curr_idx = self.curr_idx.wrapping_add(1);
        }
        while self.idx_in_span >= span.length {
            // Exhausted the current span (handles pathological spans of length 0).
            self.idx_in_span = 0;
            self.spans_idx += 1;
            if self.spans_idx >= self.spans.len() {
                return false;
            }
            span = self.spans[self.spans_idx];
            self.curr_idx = self.curr_idx.wrapping_add(span.offset);
        }
        // Protects against an invalid histogram.
        if self.buckets_idx >= self.buckets.len() {
            return false;
        }
        self.curr_count = self.curr_count.wrapping_add(self.buckets[self.buckets_idx]);
        self.idx_in_span += 1;
        self.buckets_idx += 1;
        true
    }

    fn at(&self) -> Bucket<u64> {
        make_bucket(self.curr_idx, self.schema, self.positive, self.custom_values, self.curr_count as u64)
    }
}

/// Cumulative bucket iterator (only positive buckets).
#[derive(Clone, Debug)]
pub struct CumulativeBucketIterator<'a> {
    h: &'a Histogram,
    pos_spans_idx: isize,
    pos_buckets_idx: usize,
    idx_in_span: u32,
    initialized: bool,
    curr_idx: i32,
    curr_upper: f64,
    curr_count: i64,
    curr_cumulative_count: u64,
    empty_bucket_count: i32,
}

impl BucketIterator<u64> for CumulativeBucketIterator<'_> {
    fn next(&mut self) -> bool {
        let h = self.h;
        if self.pos_spans_idx == -1 {
            // Zero bucket.
            self.pos_spans_idx += 1;
            if h.zero_count == 0 {
                return self.next();
            }
            self.curr_upper = h.zero_threshold;
            self.curr_count = h.zero_count as i64;
            self.curr_cumulative_count = self.curr_count as u64;
            return true;
        }
        if self.pos_spans_idx as usize >= h.positive_spans.len() {
            return false;
        }
        if self.empty_bucket_count > 0 {
            // Traversing through empty buckets.
            self.curr_upper = get_bound(self.curr_idx, h.schema, h.custom_values_slice());
            self.curr_idx += 1;
            self.empty_bucket_count -= 1;
            return true;
        }
        let span = h.positive_spans[self.pos_spans_idx as usize];
        if self.pos_spans_idx == 0 && !self.initialized {
            self.curr_idx = span.offset;
            self.curr_count = 0;
            self.initialized = true;
        }
        if self.pos_buckets_idx >= h.positive_buckets.len() {
            return false;
        }
        self.curr_count = self.curr_count.wrapping_add(h.positive_buckets[self.pos_buckets_idx]);
        self.curr_cumulative_count = self.curr_cumulative_count.wrapping_add(self.curr_count as u64);
        self.curr_upper = get_bound(self.curr_idx, h.schema, h.custom_values_slice());

        self.pos_buckets_idx += 1;
        self.idx_in_span += 1;
        self.curr_idx += 1;
        if self.idx_in_span >= span.length {
            // Move to the next span.
            self.pos_spans_idx += 1;
            self.idx_in_span = 0;
            if (self.pos_spans_idx as usize) < h.positive_spans.len() {
                self.empty_bucket_count = h.positive_spans[self.pos_spans_idx as usize].offset;
            }
        }
        true
    }

    fn at(&self) -> Bucket<u64> {
        Bucket {
            upper: self.curr_upper,
            lower: f64::NEG_INFINITY,
            upper_inclusive: true,
            lower_inclusive: true,
            count: self.curr_cumulative_count,
            index: self.curr_idx - 1,
        }
    }
}
