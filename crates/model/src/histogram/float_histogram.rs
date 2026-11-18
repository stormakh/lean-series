//! Float histogram (port of `model/histogram/float_histogram.go`).

use super::histogram::spans_match;
use super::*;

/// Like [`Histogram`] but with float64 counts. Bucket counts are absolute, not deltas.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FloatHistogram {
    /// Counter reset information.
    pub counter_reset_hint: CounterResetHint,
    /// -4 <= n <= 8 for exponential buckets, or -53 for custom buckets.
    pub schema: i32,
    /// Width of the zero bucket.
    pub zero_threshold: f64,
    /// Observations falling into the zero bucket. Must be zero or positive.
    pub zero_count: f64,
    /// Total number of observations. Must be zero or positive.
    pub count: f64,
    /// Sum of observations. This is also used as the stale marker.
    pub sum: f64,
    pub positive_spans: Vec<Span>,
    pub negative_spans: Vec<Span>,
    pub positive_buckets: Vec<f64>,
    pub negative_buckets: Vec<f64>,
    /// Custom (usually upper) bounds for the custom buckets schema, otherwise `None`. Interned.
    pub custom_values: CustomValues,
}

/// Walks the bucket indices of `spans` like the same-schema fast path of `floatBucketIterator.Next`.
struct SpanWalker<'a> {
    spans: &'a [Span],
    n_buckets: usize,
    spans_idx: usize,
    idx_in_span: u32,
    buckets_idx: usize,
    curr_idx: i32,
}

impl<'a> SpanWalker<'a> {
    fn new(spans: &'a [Span], n_buckets: usize) -> Self {
        SpanWalker { spans, n_buckets, spans_idx: 0, idx_in_span: 0, buckets_idx: 0, curr_idx: 0 }
    }

    fn next(&mut self) -> Option<i32> {
        if self.spans_idx >= self.spans.len() {
            return None;
        }
        let mut span = self.spans[self.spans_idx];
        if self.buckets_idx == 0 {
            self.curr_idx = span.offset;
        } else {
            self.curr_idx = self.curr_idx.wrapping_add(1);
        }
        if self.buckets_idx >= self.n_buckets {
            return None;
        }
        while self.idx_in_span >= span.length {
            self.idx_in_span = 0;
            self.spans_idx += 1;
            if self.spans_idx >= self.spans.len() {
                return None;
            }
            span = self.spans[self.spans_idx];
            self.curr_idx = self.curr_idx.wrapping_add(span.offset);
        }
        self.idx_in_span += 1;
        self.buckets_idx += 1;
        Some(self.curr_idx)
    }
}

impl FloatHistogram {
    pub fn uses_custom_buckets(&self) -> bool {
        is_custom_buckets_schema(self.schema)
    }

    /// The custom values as a slice (empty for `None`).
    #[inline]
    pub fn custom_values_slice(&self) -> &[f64] {
        cv_slice(&self.custom_values)
    }

    /// Returns a deep copy (Go `Copy`). Fields unused by the schema are not copied.
    pub fn copy(&self) -> FloatHistogram {
        let mut c = FloatHistogram {
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
    pub fn copy_to(&self, to: &mut FloatHistogram) {
        to.counter_reset_hint = self.counter_reset_hint;
        to.schema = self.schema;
        to.count = self.count;
        to.sum = self.sum;
        if self.uses_custom_buckets() {
            to.zero_threshold = 0.0;
            to.zero_count = 0.0;
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

    /// Works like `copy`, but the copy has the provided lower (or equal) target schema. Panics for custom buckets
    /// (programming errors); returns an error for an invalid bucket layout.
    pub fn copy_to_schema(&self, target_schema: i32) -> Result<FloatHistogram, Error> {
        if target_schema == self.schema {
            return Ok(self.copy());
        }
        if self.uses_custom_buckets() {
            panic!("cannot reduce resolution to {} when there are custom buckets", target_schema);
        }
        if is_custom_buckets_schema(target_schema) {
            panic!("cannot reduce resolution to custom buckets schema");
        }
        if target_schema > self.schema {
            panic!("cannot copy from schema {} to {}", self.schema, target_schema);
        }
        let (ps, pb) = reduce_resolution(&self.positive_spans, &self.positive_buckets, self.schema, target_schema, false)?;
        let (ns, nb) = reduce_resolution(&self.negative_spans, &self.negative_buckets, self.schema, target_schema, false)?;
        Ok(FloatHistogram {
            counter_reset_hint: self.counter_reset_hint,
            schema: target_schema,
            zero_threshold: self.zero_threshold,
            zero_count: self.zero_count,
            count: self.count,
            sum: self.sum,
            positive_spans: ps,
            negative_spans: ns,
            positive_buckets: pb,
            negative_buckets: nb,
            custom_values: None,
        })
    }

    /// Go `String()`.
    pub fn string(&self) -> String {
        let mut sb = String::from("{count:");
        gofmt::write_g(&mut sb, self.count);
        sb.push_str(", sum:");
        gofmt::write_g(&mut sb, self.sum);

        let mut n_buckets: Vec<Bucket<f64>> = Vec::new();
        let mut it = self.negative_bucket_iterator();
        while it.next() {
            let b = it.at();
            if b.count != 0.0 {
                n_buckets.push(b);
            }
        }
        for b in n_buckets.iter().rev() {
            sb.push_str(", ");
            b.write_to(&mut sb);
        }
        if self.zero_count != 0.0 {
            sb.push_str(", ");
            self.zero_bucket().write_to(&mut sb);
        }
        let mut it = self.positive_bucket_iterator();
        while it.next() {
            let b = it.at();
            if b.count != 0.0 {
                sb.push_str(", ");
                b.write_to(&mut sb);
            }
        }
        sb.push('}');
        sb
    }

    /// The representation used by the PromQL test framework and promtool rule unit tests.
    pub fn test_expression(&self) -> String {
        let mut res: Vec<String> = Vec::new();
        let mut m = self.copy();
        m.compact(usize::MAX); // Compact to reduce the number of positive and negative spans to 1.
        if m.schema != 0 {
            res.push(format!("schema:{}", m.schema));
        }
        if m.count != 0.0 {
            res.push(format!("count:{}", gofmt::g(m.count)));
        }
        if m.sum != 0.0 {
            res.push(format!("sum:{}", gofmt::g(m.sum)));
        }
        if m.zero_count != 0.0 {
            res.push(format!("z_bucket:{}", gofmt::g(m.zero_count)));
        }
        if m.zero_threshold != 0.0 {
            res.push(format!("z_bucket_w:{}", gofmt::g(m.zero_threshold)));
        }
        if m.uses_custom_buckets() {
            res.push(format!("custom_values:{}", gofmt::g_slice(m.custom_values_slice())));
        }
        match m.counter_reset_hint {
            CounterResetHint::UnknownCounterReset => {}
            CounterResetHint::CounterReset => res.push("counter_reset_hint:reset".to_string()),
            CounterResetHint::NotCounterReset => res.push("counter_reset_hint:not_reset".to_string()),
            CounterResetHint::GaugeType => res.push("counter_reset_hint:gauge".to_string()),
        }
        let mut add_buckets = |kind: &str, buckets_key: &str, offset_key: &str, buckets: &[f64], spans: &[Span]| {
            if spans.len() > 1 {
                panic!("histogram with multiple {} spans not supported", kind);
            }
            for span in spans {
                if span.offset != 0 {
                    res.push(format!("{}:{}", offset_key, span.offset));
                }
            }
            if !buckets.is_empty() {
                let strs: Vec<String> = buckets.iter().map(|b| gofmt::g(*b)).collect();
                res.push(format!("{}:[{}]", buckets_key, strs.join(" ")));
            }
        };
        add_buckets("positive", "buckets", "offset", &m.positive_buckets, &m.positive_spans);
        add_buckets("negative", "n_buckets", "n_offset", &m.negative_buckets, &m.negative_spans);
        format!("{{{{{}}}}}", res.join(" "))
    }

    /// Returns the zero bucket. Panics for custom buckets.
    pub fn zero_bucket(&self) -> Bucket<f64> {
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

    /// Scales all counts and the sum by `factor`. A negative factor sets the hint to GaugeType.
    pub fn mul(&mut self, factor: f64) -> &mut FloatHistogram {
        self.zero_count *= factor;
        self.count *= factor;
        self.sum *= factor;
        for b in self.positive_buckets.iter_mut() {
            *b *= factor;
        }
        for b in self.negative_buckets.iter_mut() {
            *b *= factor;
        }
        if factor < 0.0 {
            self.counter_reset_hint = CounterResetHint::GaugeType;
        }
        self
    }

    /// Works like `mul` but divides. Division by zero removes all buckets.
    pub fn div(&mut self, scalar: f64) -> &mut FloatHistogram {
        self.zero_count /= scalar;
        self.count /= scalar;
        self.sum /= scalar;
        if scalar == 0.0 {
            self.positive_buckets = Vec::new();
            self.negative_buckets = Vec::new();
            self.positive_spans = Vec::new();
            self.negative_spans = Vec::new();
            return self;
        }
        for b in self.positive_buckets.iter_mut() {
            *b /= scalar;
        }
        for b in self.negative_buckets.iter_mut() {
            *b /= scalar;
        }
        if scalar < 0.0 {
            self.counter_reset_hint = CounterResetHint::GaugeType;
        }
        self
    }

    /// Adds `other` to `self` (in place). Returns (counterResetCollision, nhcbBoundsReconciled). On error, neither
    /// histogram is modified.
    pub fn add(&mut self, other: &FloatHistogram) -> Result<(bool, bool), Error> {
        self.add_or_sub(other, false)
    }

    /// Works like `add` but subtracts `other`.
    pub fn sub(&mut self, other: &FloatHistogram) -> Result<(bool, bool), Error> {
        self.add_or_sub(other, true)
    }

    fn add_or_sub(&mut self, other: &FloatHistogram, negative: bool) -> Result<(bool, bool), Error> {
        self.check_schema_and_bounds(other)?;
        check_spans_for_resolution_reduction(self, other)?;
        let counter_reset_collision = self.adjust_counter_reset(other);
        let mut nhcb_bounds_reconciled = false;
        if !self.uses_custom_buckets() {
            let (other_zero_count, _) = self.reconcile_zero_buckets(other, None);
            if negative {
                self.zero_count -= other_zero_count;
            } else {
                self.zero_count += other_zero_count;
            }
        }
        if negative {
            self.count -= other.count;
            self.sum -= other.sum;
        } else {
            self.count += other.count;
            self.sum += other.sum;
        }

        if self.uses_custom_buckets() {
            if custom_bucket_bounds_match(self.custom_values_slice(), other.custom_values_slice()) {
                add_buckets(
                    self.schema,
                    self.zero_threshold,
                    negative,
                    &mut self.positive_spans,
                    &mut self.positive_buckets,
                    &other.positive_spans,
                    &other.positive_buckets,
                );
            } else {
                nhcb_bounds_reconciled = true;
                let intersected = intersect_custom_bucket_bounds(self.custom_values_slice(), other.custom_values_slice());
                let (b, _) = add_custom_buckets_with_mismatches(
                    negative,
                    &mut self.positive_spans,
                    &self.positive_buckets,
                    cv_slice(&self.custom_values),
                    &other.positive_spans,
                    &other.positive_buckets,
                    other.custom_values_slice(),
                    None,
                    cv_slice(&intersected),
                );
                self.positive_buckets = b;
                self.custom_values = intersected;
            }
            return Ok((counter_reset_collision, nhcb_bounds_reconciled));
        }

        if other.schema < self.schema {
            must_reduce_resolution_in_place(&mut self.positive_spans, &mut self.positive_buckets, self.schema, other.schema, false);
            must_reduce_resolution_in_place(&mut self.negative_spans, &mut self.negative_buckets, self.schema, other.schema, false);
            self.schema = other.schema;
        }
        if other.schema > self.schema {
            let (ops, opb) = must_reduce_resolution(&other.positive_spans, &other.positive_buckets, other.schema, self.schema, false);
            let (ons, onb) = must_reduce_resolution(&other.negative_spans, &other.negative_buckets, other.schema, self.schema, false);
            add_buckets(self.schema, self.zero_threshold, negative, &mut self.positive_spans, &mut self.positive_buckets, &ops, &opb);
            add_buckets(self.schema, self.zero_threshold, negative, &mut self.negative_spans, &mut self.negative_buckets, &ons, &onb);
        } else {
            add_buckets(
                self.schema,
                self.zero_threshold,
                negative,
                &mut self.positive_spans,
                &mut self.positive_buckets,
                &other.positive_spans,
                &other.positive_buckets,
            );
            add_buckets(
                self.schema,
                self.zero_threshold,
                negative,
                &mut self.negative_spans,
                &mut self.negative_buckets,
                &other.negative_spans,
                &other.negative_buckets,
            );
        }
        Ok((counter_reset_collision, nhcb_bounds_reconciled))
    }

    /// Works like `add` but uses Kahan summation. `c` holds the compensation term; if `None`, a new compensation
    /// histogram is created. Returns the updated compensation histogram, counterResetCollision and
    /// nhcbBoundsReconciled.
    pub fn kahan_add(
        &mut self,
        other: &FloatHistogram,
        c: Option<FloatHistogram>,
    ) -> Result<(FloatHistogram, bool, bool), Error> {
        self.check_schema_and_bounds(other)?;
        let counter_reset_collision = self.adjust_counter_reset(other);
        let mut nhcb_bounds_reconciled = false;
        let mut c = match c {
            Some(c) => c,
            None => self.new_compensation_histogram(),
        };
        if !self.uses_custom_buckets() {
            let (other_zero_count, other_c_zero_count) = self.reconcile_zero_buckets(other, Some(&mut c));
            (self.zero_count, c.zero_count) = kahansum::inc(other_zero_count, self.zero_count, c.zero_count);
            (self.zero_count, c.zero_count) = kahansum::inc(other_c_zero_count, self.zero_count, c.zero_count);
        }
        (self.count, c.count) = kahansum::inc(other.count, self.count, c.count);
        (self.sum, c.sum) = kahansum::inc(other.sum, self.sum, c.sum);

        if self.uses_custom_buckets() {
            if custom_bucket_bounds_match(self.custom_values_slice(), other.custom_values_slice()) {
                kahan_add_buckets(
                    self.schema,
                    self.zero_threshold,
                    false,
                    &mut self.positive_spans,
                    &mut self.positive_buckets,
                    &other.positive_spans,
                    &other.positive_buckets,
                    &mut c.positive_buckets,
                    None,
                );
            } else {
                nhcb_bounds_reconciled = true;
                let intersected = intersect_custom_bucket_bounds(self.custom_values_slice(), other.custom_values_slice());
                let (b, cb) = add_custom_buckets_with_mismatches(
                    false,
                    &mut self.positive_spans,
                    &self.positive_buckets,
                    cv_slice(&self.custom_values),
                    &other.positive_spans,
                    &other.positive_buckets,
                    other.custom_values_slice(),
                    Some(&c.positive_buckets),
                    cv_slice(&intersected),
                );
                self.positive_buckets = b;
                c.positive_buckets = cb;
                self.custom_values = intersected.clone();
                c.custom_values = intersected;
            }
            copy_into(&mut c.positive_spans, &self.positive_spans);
            return Ok((c, counter_reset_collision, nhcb_bounds_reconciled));
        }

        let mut other_reduced: Option<(Vec<Span>, Vec<f64>, Vec<f64>)> = None;
        let mut other_reduced_neg: Option<(Vec<Span>, Vec<f64>, Vec<f64>)> = None;
        if other.schema < self.schema {
            kahan_reduce_resolution_in_place(
                &mut self.positive_spans,
                &mut self.positive_buckets,
                &mut c.positive_buckets,
                self.schema,
                other.schema,
            );
            kahan_reduce_resolution_in_place(
                &mut self.negative_spans,
                &mut self.negative_buckets,
                &mut c.negative_buckets,
                self.schema,
                other.schema,
            );
            self.schema = other.schema;
        } else if other.schema > self.schema {
            if !other.positive_buckets.is_empty() {
                let mut s = other.positive_spans.clone();
                let mut b = other.positive_buckets.clone();
                let mut cb = vec![0.0; other.positive_buckets.len()];
                kahan_reduce_resolution_in_place(&mut s, &mut b, &mut cb, other.schema, self.schema);
                other_reduced = Some((s, b, cb));
            }
            if !other.negative_buckets.is_empty() {
                let mut s = other.negative_spans.clone();
                let mut b = other.negative_buckets.clone();
                let mut cb = vec![0.0; other.negative_buckets.len()];
                kahan_reduce_resolution_in_place(&mut s, &mut b, &mut cb, other.schema, self.schema);
                other_reduced_neg = Some((s, b, cb));
            }
        }

        {
            let (os, ob, oc): (&[Span], &[f64], Option<&[f64]>) = match &other_reduced {
                Some((s, b, cb)) => (s, b, Some(cb)),
                None => (&other.positive_spans, &other.positive_buckets, None),
            };
            kahan_add_buckets(
                self.schema,
                self.zero_threshold,
                false,
                &mut self.positive_spans,
                &mut self.positive_buckets,
                os,
                ob,
                &mut c.positive_buckets,
                oc,
            );
        }
        {
            let (os, ob, oc): (&[Span], &[f64], Option<&[f64]>) = match &other_reduced_neg {
                Some((s, b, cb)) => (s, b, Some(cb)),
                None => (&other.negative_spans, &other.negative_buckets, None),
            };
            kahan_add_buckets(
                self.schema,
                self.zero_threshold,
                false,
                &mut self.negative_spans,
                &mut self.negative_buckets,
                os,
                ob,
                &mut c.negative_buckets,
                oc,
            );
        }

        c.schema = self.schema;
        c.zero_threshold = self.zero_threshold;
        copy_into(&mut c.positive_spans, &self.positive_spans);
        copy_into(&mut c.negative_spans, &self.negative_spans);
        Ok((c, counter_reset_collision, nhcb_bounds_reconciled))
    }

    /// Returns true if the given float histogram matches exactly (counts and sum compared by bit pattern).
    pub fn equals(&self, h2: &FloatHistogram) -> bool {
        if self.schema != h2.schema || self.count.to_bits() != h2.count.to_bits() || self.sum.to_bits() != h2.sum.to_bits() {
            return false;
        }
        if self.uses_custom_buckets() && !custom_bucket_bounds_match(self.custom_values_slice(), h2.custom_values_slice()) {
            return false;
        }
        if self.zero_threshold != h2.zero_threshold || self.zero_count.to_bits() != h2.zero_count.to_bits() {
            return false;
        }
        if !spans_match(&self.negative_spans, &h2.negative_spans) {
            return false;
        }
        if !float_buckets_match(&self.negative_buckets, &h2.negative_buckets) {
            return false;
        }
        if !spans_match(&self.positive_spans, &h2.positive_spans) {
            return false;
        }
        if !float_buckets_match(&self.positive_buckets, &h2.positive_buckets) {
            return false;
        }
        true
    }

    /// Total size in bytes as Go computes it (64 bit): struct (168) plus all slice elements.
    pub fn size(&self) -> usize {
        168 + self.positive_spans.len() * 8
            + self.negative_spans.len() * 8
            + self.positive_buckets.len() * 8
            + self.negative_buckets.len() * 8
            + self.custom_values_slice().len() * 8
    }

    /// Eliminates empty buckets at the beginning and end of each span, merges spans that are consecutive or at most
    /// `max_empty_buckets` apart, and splits spans with more consecutive empty buckets than `max_empty_buckets`.
    pub fn compact(&mut self, max_empty_buckets: usize) -> &mut FloatHistogram {
        compact_buckets(&mut self.positive_buckets, None, &mut self.positive_spans, max_empty_buckets, false);
        compact_buckets(&mut self.negative_buckets, None, &mut self.negative_spans, max_empty_buckets, false);
        self
    }

    /// Works like `compact`, but also keeps the Kahan compensation buckets of `c` aligned.
    #[doc(hidden)]
    pub fn kahan_compact(&mut self, max_empty_buckets: usize, c: &mut FloatHistogram) {
        let pc = if c.positive_buckets.is_empty() && !self.positive_buckets.is_empty() {
            None
        } else {
            Some(&mut c.positive_buckets)
        };
        compact_buckets(&mut self.positive_buckets, pc, &mut self.positive_spans, max_empty_buckets, false);
        let nc = if c.negative_buckets.is_empty() && !self.negative_buckets.is_empty() {
            None
        } else {
            Some(&mut c.negative_buckets)
        };
        compact_buckets(&mut self.negative_buckets, nc, &mut self.negative_spans, max_empty_buckets, false);
    }

    /// Returns true if `self` is missing any populated bucket of `previous`, or if any count (bucket, zero count,
    /// count of observations, but not the sum) decreased. Shortcuts on the CounterReset / NotCounterReset hints.
    pub fn detect_reset(&self, previous: &FloatHistogram) -> bool {
        if self.counter_reset_hint == CounterResetHint::CounterReset {
            return true;
        }
        if self.counter_reset_hint == CounterResetHint::NotCounterReset {
            return false;
        }
        if self.count < previous.count {
            return true;
        }
        if self.uses_custom_buckets() {
            if !previous.uses_custom_buckets() {
                return true;
            }
            if !custom_bucket_bounds_match(self.custom_values_slice(), previous.custom_values_slice()) {
                return self.detect_reset_with_mismatched_custom_bounds(
                    previous,
                    self.custom_values_slice(),
                    previous.custom_values_slice(),
                );
            }
        }
        if self.schema > previous.schema {
            return true;
        }
        if self.zero_threshold < previous.zero_threshold {
            return true;
        }
        let (previous_zero_count, new_threshold, _) = previous.zero_count_for_larger_threshold(self.zero_threshold, None);
        if new_threshold != self.zero_threshold {
            // ZeroThreshold is within a populated bucket in previous histogram.
            return true;
        }
        if self.zero_count < previous_zero_count {
            return true;
        }
        let mut curr_it = self.float_bucket_iterator(true, self.zero_threshold, self.schema);
        let mut prev_it = previous.float_bucket_iterator(true, self.zero_threshold, self.schema);
        if detect_reset(&mut curr_it, &mut prev_it) {
            return true;
        }
        let mut curr_it = self.float_bucket_iterator(false, self.zero_threshold, self.schema);
        let mut prev_it = previous.float_bucket_iterator(false, self.zero_threshold, self.schema);
        detect_reset(&mut curr_it, &mut prev_it)
    }

    /// Iterates over all positive buckets in ascending order.
    pub fn positive_bucket_iterator(&self) -> FloatBucketIterator<'_> {
        self.float_bucket_iterator(true, 0.0, self.schema)
    }

    /// Iterates over all negative buckets in descending order (starting next to the zero bucket).
    pub fn negative_bucket_iterator(&self) -> FloatBucketIterator<'_> {
        self.float_bucket_iterator(false, 0.0, self.schema)
    }

    /// Iterates over all positive buckets in descending order.
    pub fn positive_reverse_bucket_iterator(&self) -> ReverseFloatBucketIterator<'_> {
        ReverseFloatBucketIterator::new(&self.positive_spans, &self.positive_buckets, self.schema, true, self.custom_values_slice())
    }

    /// Iterates over all negative buckets in ascending order (starting at the lowest bucket).
    pub fn negative_reverse_bucket_iterator(&self) -> ReverseFloatBucketIterator<'_> {
        ReverseFloatBucketIterator::new(&self.negative_spans, &self.negative_buckets, self.schema, false, &[])
    }

    /// Iterates over all negative, zero and positive buckets in ascending order.
    pub fn all_bucket_iterator(&self) -> AllFloatBucketIterator<'_> {
        AllFloatBucketIterator {
            h: self,
            left_iter: ReverseFloatBucketIterator::new(&self.negative_spans, &self.negative_buckets, self.schema, false, &[]),
            right_iter: self.float_bucket_iterator(true, 0.0, self.schema),
            state: -1,
            curr_bucket: Bucket::default(),
        }
    }

    /// Iterates over all positive, zero and negative buckets in descending order.
    pub fn all_reverse_bucket_iterator(&self) -> AllFloatBucketIterator<'_> {
        AllFloatBucketIterator {
            h: self,
            left_iter: ReverseFloatBucketIterator::new(
                &self.positive_spans,
                &self.positive_buckets,
                self.schema,
                true,
                self.custom_values_slice(),
            ),
            right_iter: self.float_bucket_iterator(false, 0.0, self.schema),
            state: -1,
            curr_bucket: Bucket::default(),
        }
    }

    /// Validates consistency between span and bucket slices, negative counts, and schema specific fields. Does not
    /// check that the count covers the bucket counts (float precision).
    pub fn validate(&self) -> Result<(), Error> {
        let mut n_count: f64 = 0.0;
        let mut p_count: f64 = 0.0;
        if is_custom_buckets_schema(self.schema) {
            check_histogram_custom_bounds(self.custom_values_slice(), &self.positive_spans, self.positive_buckets.len())
                .map_err(|e| e.with_prefix("custom buckets: "))?;
            if self.zero_count != 0.0 {
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
            check_histogram_buckets_float(&self.negative_buckets, &mut n_count, false)
                .map_err(|e| e.with_prefix("negative side: "))?;
            if self.zero_count < 0.0 {
                return Err(Error::wrapped(
                    ErrorKind::HistogramNegativeBucketCount,
                    format!(
                        "zero bucket has observation count of {}: {}",
                        gofmt::g(self.zero_count),
                        ErrorKind::HistogramNegativeBucketCount.message()
                    ),
                ));
            }
            if self.custom_values.is_some() {
                return Err(Error::new(ErrorKind::HistogramExpSchemaCustomBounds));
            }
        } else {
            return Err(invalid_schema_error(self.schema));
        }
        if self.count < 0.0 {
            return Err(Error::wrapped(
                ErrorKind::HistogramNegativeCount,
                format!("observation count is  {}: {}", gofmt::g(self.count), ErrorKind::HistogramNegativeCount.message()),
            ));
        }
        check_histogram_buckets_float(&self.positive_buckets, &mut p_count, false)
            .map_err(|e| e.with_prefix("positive side: "))?;
        Ok(())
    }

    /// Returns what the zero count would be with the provided larger (or equal) threshold, the adjusted threshold,
    /// and the zero count of the compensation histogram `c` if provided. Panics if the threshold is smaller.
    pub fn zero_count_for_larger_threshold(&self, larger_threshold: f64, c: Option<&FloatHistogram>) -> (f64, f64, f64) {
        let mut c_zero_count = match c {
            Some(c) => c.zero_count,
            None => 0.0,
        };
        let mut larger_threshold = larger_threshold;
        // Fast path.
        if larger_threshold == self.zero_threshold {
            return (self.zero_count, larger_threshold, c_zero_count);
        }
        if larger_threshold < self.zero_threshold {
            panic!(
                "new threshold {} is less than old threshold {}",
                gofmt::f6(larger_threshold),
                gofmt::f6(self.zero_threshold)
            );
        }
        'outer: loop {
            let mut h_zero_count = self.zero_count;
            let mut i = self.positive_bucket_iterator();
            let mut buckets_idx = 0usize;
            while i.next() {
                let b = i.at();
                if b.lower >= larger_threshold {
                    break;
                }
                (h_zero_count, c_zero_count) = kahansum::inc(b.count, h_zero_count, c_zero_count);
                if let Some(c) = c {
                    (h_zero_count, c_zero_count) = kahansum::inc(c.positive_buckets[buckets_idx], h_zero_count, c_zero_count);
                }
                if b.upper > larger_threshold {
                    if b.count != 0.0 {
                        larger_threshold = b.upper;
                    }
                    break;
                }
                buckets_idx += 1;
            }
            let mut i = self.negative_bucket_iterator();
            buckets_idx = 0;
            while i.next() {
                let b = i.at();
                if b.upper <= -larger_threshold {
                    break;
                }
                (h_zero_count, c_zero_count) = kahansum::inc(b.count, h_zero_count, c_zero_count);
                if let Some(c) = c {
                    (h_zero_count, c_zero_count) = kahansum::inc(c.negative_buckets[buckets_idx], h_zero_count, c_zero_count);
                }
                if b.lower < -larger_threshold {
                    if b.count != 0.0 {
                        larger_threshold = -b.lower;
                        continue 'outer;
                    }
                    break;
                }
                buckets_idx += 1;
            }
            return (h_zero_count, larger_threshold, c_zero_count);
        }
    }

    /// Removes all buckets within the zero bucket (their counts are already part of the zero count).
    fn trim_buckets_in_zero_bucket(&mut self, mut c: Option<&mut FloatHistogram>) {
        let schema = self.schema;
        {
            let cv = cv_slice(&self.custom_values);
            let mut w = SpanWalker::new(&self.positive_spans, self.positive_buckets.len());
            let mut buckets_idx = 0usize;
            while let Some(idx) = w.next() {
                let (lower, _, _, _) = bucket_bounds(idx, schema, true, cv);
                if lower >= self.zero_threshold {
                    break;
                }
                self.positive_buckets[buckets_idx] = 0.0;
                if let Some(c) = c.as_deref_mut() {
                    c.positive_buckets[buckets_idx] = 0.0;
                }
                buckets_idx += 1;
            }
            let mut w = SpanWalker::new(&self.negative_spans, self.negative_buckets.len());
            let mut buckets_idx = 0usize;
            while let Some(idx) = w.next() {
                let (_, upper, _, _) = bucket_bounds(idx, schema, false, &[]);
                if upper <= -self.zero_threshold {
                    break;
                }
                self.negative_buckets[buckets_idx] = 0.0;
                if let Some(c) = c.as_deref_mut() {
                    c.negative_buckets[buckets_idx] = 0.0;
                }
                buckets_idx += 1;
            }
        }
        match c {
            Some(c) => self.kahan_compact(0, c),
            None => {
                self.compact(0);
            }
        }
    }

    /// Finds a zero bucket large enough to include the zero buckets of both histograms, modifies `self` (and `c`)
    /// accordingly, and returns the zero count `other` would have, with its compensation term.
    fn reconcile_zero_buckets(&mut self, other: &FloatHistogram, mut c: Option<&mut FloatHistogram>) -> (f64, f64) {
        let mut other_zero_count = other.zero_count;
        let mut other_zero_threshold = other.zero_threshold;
        let mut other_c_zero_count = 0.0;
        while other_zero_threshold != self.zero_threshold {
            if self.zero_threshold > other_zero_threshold {
                (other_zero_count, other_zero_threshold, other_c_zero_count) =
                    other.zero_count_for_larger_threshold(self.zero_threshold, None);
            }
            if other_zero_threshold > self.zero_threshold {
                let (zc, zt, czc) = self.zero_count_for_larger_threshold(other_zero_threshold, c.as_deref());
                self.zero_count = zc;
                self.zero_threshold = zt;
                if let Some(c) = c.as_deref_mut() {
                    c.zero_count = czc;
                }
                self.trim_buckets_in_zero_bucket(c.as_deref_mut());
            }
        }
        (other_zero_count, other_c_zero_count)
    }

    /// Low-level bucket iterator constructor. Skips buckets with an absolute upper bound <= `absolute_start_value`
    /// (exponential schemas only) and merges buckets to the lower or equal `target_schema`.
    pub fn float_bucket_iterator(&self, positive: bool, absolute_start_value: f64, target_schema: i32) -> FloatBucketIterator<'_> {
        if self.uses_custom_buckets() && target_schema != self.schema {
            panic!("cannot merge from custom buckets schema to exponential schema");
        }
        if !self.uses_custom_buckets() && is_custom_buckets_schema(target_schema) {
            panic!("cannot merge from exponential buckets schema to custom schema");
        }
        if target_schema > self.schema {
            panic!("cannot merge from schema {} to {}", self.schema, target_schema);
        }
        let (spans, buckets, cv): (&[Span], &[f64], &[f64]) = if positive {
            (&self.positive_spans, &self.positive_buckets, self.custom_values_slice())
        } else {
            (&self.negative_spans, &self.negative_buckets, &[])
        };
        FloatBucketIterator {
            schema: self.schema,
            spans,
            buckets,
            positive,
            spans_idx: 0,
            idx_in_span: 0,
            buckets_idx: 0,
            curr_count: 0.0,
            curr_idx: 0,
            custom_values: cv,
            target_schema,
            orig_idx: 0,
            absolute_start_value,
            bound_reached_start_value: absolute_start_value == 0.0,
        }
    }

    /// Checks if any reconciled bucket count decreased between NHCBs with mismatched custom bounds.
    fn detect_reset_with_mismatched_custom_bounds(
        &self,
        previous: &FloatHistogram,
        curr_bounds: &[f64],
        prev_bounds: &[f64],
    ) -> bool {
        if self.schema != CUSTOM_BUCKETS_SCHEMA || previous.schema != CUSTOM_BUCKETS_SCHEMA {
            panic!("detectResetWithMismatchedCustomBounds called with non-NHCB schema");
        }
        let mut curr_it = self.float_bucket_iterator(true, 0.0, CUSTOM_BUCKETS_SCHEMA);
        let mut prev_it = previous.float_bucket_iterator(true, 0.0, CUSTOM_BUCKETS_SCHEMA);

        fn rollup_sum_for_bound(
            iter: &mut FloatBucketIterator<'_>,
            iter_started: bool,
            mut iter_bucket: Bucket<f64>,
            bound: f64,
        ) -> (f64, Bucket<f64>, bool) {
            if !iter_started {
                if !iter.next() {
                    return (0.0, Bucket::default(), false);
                }
                iter_bucket = iter.at();
            }
            let mut sum = 0.0;
            while iter_bucket.upper <= bound {
                sum += iter_bucket.count;
                if !iter.next() {
                    return (sum, Bucket::default(), false);
                }
                iter_bucket = iter.at();
            }
            (sum, iter_bucket, true)
        }

        let (mut curr_bound_idx, mut prev_bound_idx) = (0usize, 0usize);
        let (mut curr_bucket, mut prev_bucket) = (Bucket::default(), Bucket::default());
        let (mut curr_iter_started, mut curr_has_more) = (false, false);
        let (mut prev_iter_started, mut prev_has_more) = (false, false);

        while curr_bound_idx <= curr_bounds.len() && prev_bound_idx <= prev_bounds.len() {
            let curr_bound = if curr_bound_idx < curr_bounds.len() { curr_bounds[curr_bound_idx] } else { f64::INFINITY };
            let prev_bound = if prev_bound_idx < prev_bounds.len() { prev_bounds[prev_bound_idx] } else { f64::INFINITY };
            if curr_bound == prev_bound {
                let mut curr_rollup_sum = 0.0;
                if !curr_iter_started || curr_has_more {
                    (curr_rollup_sum, curr_bucket, curr_has_more) =
                        rollup_sum_for_bound(&mut curr_it, curr_iter_started, curr_bucket, curr_bound);
                    curr_iter_started = true;
                }
                let mut prev_rollup_sum = 0.0;
                if !prev_iter_started || prev_has_more {
                    (prev_rollup_sum, prev_bucket, prev_has_more) =
                        rollup_sum_for_bound(&mut prev_it, prev_iter_started, prev_bucket, curr_bound);
                    prev_iter_started = true;
                }
                if curr_rollup_sum < prev_rollup_sum {
                    return true;
                }
                curr_bound_idx += 1;
                prev_bound_idx += 1;
            } else if curr_bound < prev_bound {
                curr_bound_idx += 1;
            } else {
                prev_bound_idx += 1;
            }
        }
        false
    }

    /// Reduces the float histogram's spans and buckets into the lower `target_schema`.
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
        reduce_resolution_in_place(&mut self.positive_spans, &mut self.positive_buckets, self.schema, target_schema, false)?;
        reduce_resolution_in_place(&mut self.negative_spans, &mut self.negative_buckets, self.schema, target_schema, false)?;
        self.schema = target_schema;
        Ok(())
    }

    /// A compensation histogram for Kahan summation with the layout of `self` and zero bucket values.
    pub fn new_compensation_histogram(&self) -> FloatHistogram {
        FloatHistogram {
            counter_reset_hint: self.counter_reset_hint,
            schema: self.schema,
            zero_threshold: self.zero_threshold,
            custom_values: self.custom_values.clone(),
            positive_buckets: vec![0.0; self.positive_buckets.len()],
            positive_spans: self.positive_spans.clone(),
            negative_spans: self.negative_spans.clone(),
            negative_buckets: if self.uses_custom_buckets() { Vec::new() } else { vec![0.0; self.negative_buckets.len()] },
            ..Default::default()
        }
    }

    fn check_schema_and_bounds(&self, other: &FloatHistogram) -> Result<(), Error> {
        if self.uses_custom_buckets() != other.uses_custom_buckets() {
            return Err(Error::new(ErrorKind::HistogramsIncompatibleSchema));
        }
        Ok(())
    }

    /// Adjusts the counter reset hint for addition and subtraction. Returns true on a CounterReset /
    /// NotCounterReset collision.
    #[allow(clippy::if_same_then_else)]
    fn adjust_counter_reset(&mut self, other: &FloatHistogram) -> bool {
        use CounterResetHint::*;
        if other.counter_reset_hint == self.counter_reset_hint {
        } else if self.counter_reset_hint == GaugeType {
        } else if other.counter_reset_hint == GaugeType {
            self.counter_reset_hint = GaugeType;
        } else if self.counter_reset_hint == UnknownCounterReset {
        } else if other.counter_reset_hint == UnknownCounterReset {
            self.counter_reset_hint = UnknownCounterReset;
        } else {
            self.counter_reset_hint = UnknownCounterReset;
            return true;
        }
        false
    }

    /// Reports whether any field contains an infinite value.
    pub fn has_overflow(&self) -> bool {
        if self.zero_count.is_infinite() || self.count.is_infinite() || self.sum.is_infinite() {
            return true;
        }
        self.positive_buckets.iter().any(|v| v.is_infinite())
            || self.negative_buckets.iter().any(|v| v.is_infinite())
            || self.custom_values_slice().iter().any(|v| v.is_infinite())
    }

    /// Trims buckets in place (PromQL `</` and `>/` trimming). Returns `self` for chaining.
    pub fn trim_buckets(&mut self, rhs: f64, is_upper_trim: bool) -> &mut FloatHistogram {
        let mut updated_count = 0.0;
        let mut updated_sum = 0.0;
        let mut trimmed_buckets = false;
        let is_custom_bucket = self.uses_custom_buckets();
        let mut has_positive = false;
        let mut has_negative = false;
        let schema = self.schema;
        {
            let cv = cv_slice(&self.custom_values);
            let mut w = SpanWalker::new(&self.positive_spans, self.positive_buckets.len());
            let mut i = 0usize;
            while let Some(idx) = w.next() {
                let bucket = make_bucket(idx, schema, true, cv, self.positive_buckets[i]);
                if bucket.count != 0.0 {
                    has_positive = true;
                    let (keep_count, mid) = trim_bucket(&bucket, rhs, is_upper_trim, true, is_custom_bucket);
                    updated_count += keep_count;
                    updated_sum += mid * keep_count;
                    if self.positive_buckets[i] != keep_count {
                        self.positive_buckets[i] = keep_count;
                        trimmed_buckets = true;
                    }
                }
                i += 1;
            }
            let mut w = SpanWalker::new(&self.negative_spans, self.negative_buckets.len());
            let mut i = 0usize;
            while let Some(idx) = w.next() {
                let bucket = make_bucket(idx, schema, false, &[], self.negative_buckets[i]);
                if bucket.count != 0.0 {
                    has_negative = true;
                    let (keep_count, mid) = trim_bucket(&bucket, rhs, is_upper_trim, false, is_custom_bucket);
                    updated_count += keep_count;
                    updated_sum += mid * keep_count;
                    if self.negative_buckets[i] != keep_count {
                        self.negative_buckets[i] = keep_count;
                        trimmed_buckets = true;
                    }
                }
                i += 1;
            }
        }
        if self.zero_count > 0.0 {
            let (keep_count, mid) =
                compute_zero_bucket_trim(&self.zero_bucket(), rhs, has_negative, has_positive, is_upper_trim);
            if self.zero_count != keep_count {
                self.zero_count = keep_count;
                trimmed_buckets = true;
            }
            updated_sum += mid * keep_count;
            updated_count += keep_count;
        }
        if trimmed_buckets {
            self.count = updated_count;
            self.sum = updated_sum;
            self.compact(0);
        }
        self
    }
}

impl std::fmt::Display for FloatHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.string())
    }
}

fn detect_reset(curr_it: &mut FloatBucketIterator<'_>, prev_it: &mut FloatBucketIterator<'_>) -> bool {
    if !prev_it.next() {
        return false; // No buckets in previous histogram, nothing can be reset.
    }
    let mut prev_bucket = prev_it.stripped_at();
    if !curr_it.next() {
        loop {
            if prev_bucket.count != 0.0 {
                return true;
            }
            if !prev_it.next() {
                return false;
            }
            prev_bucket = prev_it.stripped_at();
        }
    }
    let mut curr_bucket = curr_it.stripped_at();
    loop {
        // Forward curr_it until we find the bucket corresponding to prev_bucket.
        while curr_bucket.index < prev_bucket.index {
            if !curr_it.next() {
                loop {
                    if prev_bucket.count != 0.0 {
                        return true;
                    }
                    if !prev_it.next() {
                        return false;
                    }
                    prev_bucket = prev_it.stripped_at();
                }
            }
            curr_bucket = curr_it.stripped_at();
        }
        if curr_bucket.index > prev_bucket.index {
            if prev_bucket.count != 0.0 {
                return true;
            }
        } else if curr_bucket.count < prev_bucket.count {
            return true;
        }
        if !prev_it.next() {
            return false;
        }
        prev_bucket = prev_it.stripped_at();
    }
}

/// Go `floatBucketsMatch`: compares bucket values by bit pattern.
pub fn float_buckets_match(b1: &[f64], b2: &[f64]) -> bool {
    b1.len() == b2.len() && b1.iter().zip(b2).all(|(a, b)| a.to_bits() == b.to_bits())
}

/// Returns the intersection of two custom bucket boundary sets (`None` if empty).
pub fn intersect_custom_bucket_bounds(a: &[f64], b: &[f64]) -> CustomValues {
    if a.is_empty() || b.is_empty() {
        return None;
    }
    let mut result: Option<Vec<f64>> = None;
    let (mut i, mut j) = (0usize, 0usize);
    while i < a.len() && j < b.len() {
        if a[i] == b[j] {
            result.get_or_insert_with(|| Vec::with_capacity(a.len().min(b.len()))).push(a[i]);
            i += 1;
            j += 1;
        } else if a[i] < b[j] {
            i += 1;
        } else {
            j += 1;
        }
    }
    result.map(Arc::from)
}

/// Adds/subtracts custom bucket histograms with mismatched layouts by mapping both onto `intersected_bounds`.
/// `spans_a` is overwritten with the resulting spans. Returns (buckets, compensation buckets).
#[allow(clippy::too_many_arguments)]
fn add_custom_buckets_with_mismatches(
    negative: bool,
    spans_a: &mut Vec<Span>,
    buckets_a: &[f64],
    bounds_a: &[f64],
    spans_b: &[Span],
    buckets_b: &[f64],
    bounds_b: &[f64],
    buckets_c: Option<&[f64]>,
    intersected_bounds: &[f64],
) -> (Vec<f64>, Vec<f64>) {
    let n = intersected_bounds.len() + 1;
    let mut target = vec![0.0f64; n];
    let mut c_target = vec![0.0f64; n];

    let mut map_buckets = |spans: &[Span], buckets: &[f64], bounds: &[f64], negative: bool, with_compensation: bool| {
        let mut src_idx: usize = 0;
        let mut bucket_idx: usize = 0;
        let mut intersect_idx: usize = 0;
        for span in spans {
            src_idx = (src_idx as i64 + span.offset as i64) as usize;
            for _ in 0..span.length {
                if bucket_idx < buckets.len() {
                    let value = buckets[bucket_idx];
                    let mut tidx = n - 1; // Default to +Inf bucket.
                    if src_idx < bounds.len() {
                        let src_bound = bounds[src_idx];
                        while intersect_idx < intersected_bounds.len() {
                            if intersected_bounds[intersect_idx] >= src_bound {
                                tidx = intersect_idx;
                                break;
                            }
                            intersect_idx += 1;
                        }
                    }
                    if negative {
                        (target[tidx], c_target[tidx]) = kahansum::dec(value, target[tidx], c_target[tidx]);
                    } else {
                        (target[tidx], c_target[tidx]) = kahansum::inc(value, target[tidx], c_target[tidx]);
                        if with_compensation && let Some(bc) = buckets_c {
                            (target[tidx], c_target[tidx]) = kahansum::inc(bc[bucket_idx], target[tidx], c_target[tidx]);
                        }
                    }
                }
                src_idx += 1;
                bucket_idx += 1;
            }
        }
    };
    map_buckets(spans_a, buckets_a, bounds_a, false, true);
    map_buckets(spans_b, buckets_b, bounds_b, negative, false);

    // Build spans and buckets, excluding zero-valued buckets.
    spans_a.clear();
    let mut w = 0usize;
    let mut last_idx: i32 = -1;
    for i in 0..n {
        if target[i] == 0.0 && c_target[i] == 0.0 {
            continue;
        }
        target[w] = target[i];
        c_target[w] = c_target[i];
        w += 1;
        let idx = i as i32;
        if !spans_a.is_empty() && idx == last_idx + 1 {
            spans_a.last_mut().unwrap().length += 1;
        } else {
            let offset = if spans_a.is_empty() { idx } else { idx - last_idx - 1 };
            spans_a.push(Span { offset, length: 1 });
        }
        last_idx = idx;
    }
    target.truncate(w);
    c_target.truncate(w);
    (target, c_target)
}

/// Adds the buckets described by spans_b/buckets_b to spans_a/buckets_a, creating missing buckets as needed. All
/// buckets use `schema`. Buckets in B with an absolute upper limit <= `threshold` are ignored. If `negative`, B is
/// subtracted.
pub fn add_buckets(
    schema: i32,
    threshold: f64,
    negative: bool,
    spans_a: &mut Vec<Span>,
    buckets_a: &mut Vec<f64>,
    spans_b: &[Span],
    buckets_b: &[f64],
) {
    kahan_add_buckets_impl(schema, threshold, negative, spans_a, buckets_a, spans_b, buckets_b, None, None);
}

/// Like [`add_buckets`] but with Kahan compensation buckets for A (`comp_a`, modified in step) and B.
#[allow(clippy::too_many_arguments)]
pub fn kahan_add_buckets(
    schema: i32,
    threshold: f64,
    negative: bool,
    spans_a: &mut Vec<Span>,
    buckets_a: &mut Vec<f64>,
    spans_b: &[Span],
    buckets_b: &[f64],
    comp_a: &mut Vec<f64>,
    comp_b: Option<&[f64]>,
) {
    kahan_add_buckets_impl(schema, threshold, negative, spans_a, buckets_a, spans_b, buckets_b, Some(comp_a), comp_b);
}

#[allow(clippy::too_many_arguments)]
fn kahan_add_buckets_impl(
    schema: i32,
    threshold: f64,
    negative: bool,
    spans_a: &mut Vec<Span>,
    buckets_a: &mut Vec<f64>,
    spans_b: &[Span],
    buckets_b: &[f64],
    mut comp_a: Option<&mut Vec<f64>>,
    comp_b: Option<&[f64]>,
) {
    let kahan = comp_a.is_some();
    let mut i_span: isize = -1;
    let mut i_bucket: isize = -1;
    let mut i_in_span: i32 = 0;
    let mut index_a: i32 = 0;
    let mut index_b: i32 = 0;
    let mut b_idx_b: usize = 0;
    let mut lower_than_threshold = true;

    // Adds `v` (and its compensation `cv`) to bucket `i` of A.
    macro_rules! add_to {
        ($i:expr, $v:expr, $cv:expr) => {{
            let i = $i;
            match comp_a.as_deref_mut() {
                Some(ca) => {
                    (buckets_a[i], ca[i]) = kahansum::inc($v, buckets_a[i], ca[i]);
                    if $cv != 0.0 {
                        (buckets_a[i], ca[i]) = kahansum::inc($cv, buckets_a[i], ca[i]);
                    }
                }
                None => buckets_a[i] += $v,
            }
        }};
    }

    for span_b in spans_b {
        index_b = index_b.wrapping_add(span_b.offset);
        for _ in 0..span_b.length {
            'body: {
                if lower_than_threshold && is_exponential_schema(schema) && get_bound_exponential(index_b, schema) <= threshold {
                    break 'body;
                }
                lower_than_threshold = false;

                let mut bucket_b = buckets_b[b_idx_b];
                let mut comp_bucket_b = match comp_b {
                    Some(cb) => cb[b_idx_b],
                    None => 0.0,
                };
                if negative {
                    bucket_b *= -1.0;
                    comp_bucket_b *= -1.0;
                }

                if i_span == -1 {
                    if spans_a.is_empty() || spans_a[0].offset > index_b {
                        // Add bucket before all others.
                        buckets_a.insert(0, bucket_b);
                        if let Some(ca) = comp_a.as_deref_mut() {
                            ca.insert(0, comp_bucket_b);
                        }
                        if !spans_a.is_empty() && spans_a[0].offset == index_b.wrapping_add(1) {
                            spans_a[0].length += 1;
                            spans_a[0].offset -= 1;
                            break 'body;
                        }
                        spans_a.insert(0, Span { offset: index_b, length: 1 });
                        if spans_a.len() > 1 {
                            // Convert the absolute offset in the formerly first span to a relative offset.
                            spans_a[1].offset = spans_a[1].offset.wrapping_sub(index_b.wrapping_add(1));
                        }
                        break 'body;
                    } else if spans_a[0].offset == index_b {
                        // Just add to first bucket.
                        if kahan {
                            add_to!(0, bucket_b, comp_bucket_b);
                        } else {
                            buckets_a[0] += bucket_b;
                        }
                        break 'body;
                    }
                    i_span = 0;
                    i_bucket = 0;
                    i_in_span = 0;
                    index_a = spans_a[0].offset;
                }
                let mut delta_index = index_b.wrapping_sub(index_a);
                loop {
                    let remaining_in_span = spans_a[i_span as usize].length as i32 - i_in_span;
                    if delta_index < remaining_in_span {
                        // Bucket is in current span.
                        i_bucket += delta_index as isize;
                        i_in_span += delta_index;
                        add_to!(i_bucket as usize, bucket_b, comp_bucket_b);
                        break;
                    }
                    delta_index -= remaining_in_span;
                    i_bucket += remaining_in_span as isize;
                    i_span += 1;
                    if i_span as usize == spans_a.len() || delta_index < spans_a[i_span as usize].offset {
                        // Bucket is in gap behind previous span (or there are no further spans).
                        buckets_a.insert(i_bucket as usize, bucket_b);
                        if let Some(ca) = comp_a.as_deref_mut() {
                            ca.insert(i_bucket as usize, comp_bucket_b);
                        }
                        let isp = i_span as usize;
                        if delta_index == 0 {
                            // Directly after previous span, extend previous span.
                            if isp < spans_a.len() {
                                spans_a[isp].offset -= 1;
                            }
                            i_span -= 1;
                            i_in_span = spans_a[i_span as usize].length as i32;
                            spans_a[i_span as usize].length += 1;
                            break 'body;
                        } else if isp < spans_a.len() && delta_index == spans_a[isp].offset - 1 {
                            // Directly before next span, extend next span.
                            i_in_span = 0;
                            spans_a[isp].offset -= 1;
                            spans_a[isp].length += 1;
                            break 'body;
                        } else {
                            // No next span, or next span is not directly adjacent to new bucket. Add new span.
                            i_in_span = 0;
                            if isp < spans_a.len() {
                                spans_a[isp].offset -= delta_index + 1;
                            }
                            spans_a.insert(isp, Span { length: 1, offset: delta_index });
                            break 'body;
                        }
                    } else {
                        // Try start of next span.
                        delta_index -= spans_a[i_span as usize].offset;
                        i_in_span = 0;
                    }
                }
            }
            // nextLoop:
            index_a = index_b;
            index_b = index_b.wrapping_add(1);
            b_idx_b += 1;
        }
    }
}

/// Like `reduce_resolution_in_place` for float buckets with Kahan compensation buckets kept in step. No error
/// checks (panics on an invalid layout, like Go).
pub fn kahan_reduce_resolution_in_place(
    spans: &mut Vec<Span>,
    buckets: &mut Vec<f64>,
    comp: &mut Vec<f64>,
    origin_schema: i32,
    target_schema: i32,
) {
    let n_spans = spans.len();
    let mut ts: usize = 0;
    let mut tb: usize = 0;
    let mut bucket_idx: i32 = 0;
    let mut bucket_count_idx: usize = 0;
    let mut last_target_bucket_idx: i32 = 0;
    for n in 0..n_spans {
        let span = spans[n];
        bucket_idx = bucket_idx.wrapping_add(span.offset);
        for _ in 0..span.length {
            let ob = buckets[bucket_count_idx];
            let oc = comp[bucket_count_idx];
            let target_bucket_idx = target_idx(bucket_idx, origin_schema, target_schema);
            if ts == 0 {
                spans[0] = Span { offset: target_bucket_idx, length: 1 };
                ts = 1;
                buckets[0] = ob;
                comp[0] = oc;
                tb = 1;
                last_target_bucket_idx = target_bucket_idx;
            } else if last_target_bucket_idx == target_bucket_idx {
                let l = tb - 1;
                (buckets[l], comp[l]) = kahansum::inc(ob, buckets[l], comp[l]);
                (buckets[l], comp[l]) = kahansum::inc(oc, buckets[l], comp[l]);
            } else if last_target_bucket_idx + 1 == target_bucket_idx {
                spans[ts - 1].length += 1;
                last_target_bucket_idx += 1;
                buckets[tb] = ob;
                comp[tb] = oc;
                tb += 1;
            } else if last_target_bucket_idx + 1 < target_bucket_idx {
                spans[ts] = Span { offset: target_bucket_idx - last_target_bucket_idx - 1, length: 1 };
                ts += 1;
                last_target_bucket_idx = target_bucket_idx;
                buckets[tb] = ob;
                comp[tb] = oc;
                tb += 1;
            }
            bucket_idx = bucket_idx.wrapping_add(1);
            bucket_count_idx += 1;
        }
    }
    spans.truncate(ts);
    buckets.truncate(tb);
    comp.truncate(tb);
}

/// Returns an error if the histogram whose resolution Add or Sub will reduce has an invalid bucket layout.
fn check_spans_for_resolution_reduction(h: &FloatHistogram, other: &FloatHistogram) -> Result<(), Error> {
    let to_reduce = if other.schema < h.schema {
        h
    } else if other.schema > h.schema {
        other
    } else {
        return Ok(());
    };
    check_histogram_spans(&to_reduce.positive_spans, to_reduce.positive_buckets.len())?;
    check_histogram_spans(&to_reduce.negative_spans, to_reduce.negative_buckets.len())
}

fn trim_bucket(b: &Bucket<f64>, rhs: f64, is_upper_trim: bool, is_positive: bool, is_custom_bucket: bool) -> (f64, f64) {
    let (keep_all, contains_trim_point) =
        if is_upper_trim { (b.upper <= rhs, b.lower < rhs) } else { (b.lower >= rhs, b.upper > rhs) };
    if keep_all {
        (b.count, compute_midpoint(b.lower, b.upper, is_positive, is_custom_bucket))
    } else if contains_trim_point {
        compute_bucket_trim(b, rhs, is_upper_trim, is_positive, is_custom_bucket)
    } else {
        (0.0, 0.0)
    }
}

fn handle_infinity_buckets(is_upper_trim: bool, b: &Bucket<f64>, rhs: f64) -> (f64, f64) {
    let zero_if_inf = |x: f64| if x.is_infinite() { 0.0 } else { x };
    // Case 1: Bucket with lower bound -Inf.
    if b.lower == f64::NEG_INFINITY {
        if is_upper_trim {
            if rhs >= b.upper {
                return (b.count, 0.0);
            }
            if rhs > 0.0 && b.upper > 0.0 && b.upper != f64::INFINITY {
                return (b.count * rhs / b.upper, rhs / 2.0);
            }
            if b.upper <= 0.0 {
                return (b.count, rhs);
            }
            return (0.0, zero_if_inf(b.upper));
        }
        if rhs <= b.lower {
            return (b.count, 0.0);
        }
        if rhs >= 0.0 && b.upper > rhs && b.upper != f64::INFINITY {
            return (b.count * (1.0 - rhs / b.upper), (rhs + b.upper) / 2.0);
        }
        return (0.0, zero_if_inf(b.upper));
    }
    // Case 2: Bucket with upper bound +Inf.
    if b.upper == f64::INFINITY {
        if is_upper_trim {
            return (0.0, zero_if_inf(b.lower));
        }
        if rhs >= b.lower {
            return (b.count, rhs);
        }
        return (0.0, zero_if_inf(b.lower));
    }
    panic!("one of the bounds must be infinite for handleInfinityBuckets, got {:?}", b);
}

fn compute_split(b: &Bucket<f64>, rhs: f64, is_linear: bool) -> f64 {
    if rhs <= b.lower {
        return 0.0;
    }
    if rhs >= b.upper {
        return b.count;
    }
    b.count * b.fraction_below(rhs, is_linear)
}

fn compute_zero_bucket_trim(zb: &Bucket<f64>, rhs: f64, has_negative: bool, has_positive: bool, is_upper_trim: bool) -> (f64, f64) {
    let mut lower = zb.lower;
    let mut upper = zb.upper;
    if has_negative && !has_positive {
        upper = 0.0;
    }
    if has_positive && !has_negative {
        lower = 0.0;
    }
    let (fraction, midpoint);
    if is_upper_trim {
        if rhs <= lower {
            return (0.0, 0.0);
        }
        if rhs >= upper {
            return (zb.count, (lower + upper) / 2.0);
        }
        fraction = (rhs - lower) / (upper - lower);
        midpoint = (lower + rhs) / 2.0;
    } else {
        if rhs <= lower {
            return (zb.count, (lower + upper) / 2.0);
        }
        if rhs >= upper {
            return (0.0, 0.0);
        }
        fraction = (upper - rhs) / (upper - lower);
        midpoint = (rhs + upper) / 2.0;
    }
    (zb.count * fraction, midpoint)
}

fn compute_bucket_trim(b: &Bucket<f64>, rhs: f64, is_upper_trim: bool, is_positive: bool, is_custom_bucket: bool) -> (f64, f64) {
    if b.lower == f64::NEG_INFINITY || b.upper == f64::INFINITY {
        return handle_infinity_buckets(is_upper_trim, b, rhs);
    }
    let under_count = compute_split(b, rhs, is_custom_bucket);
    if is_upper_trim {
        return (under_count, compute_midpoint(b.lower, rhs, is_positive, is_custom_bucket));
    }
    (b.count - under_count, compute_midpoint(rhs, b.upper, is_positive, is_custom_bucket))
}

fn compute_midpoint(lo: f64, hi: f64, is_positive: bool, is_linear: bool) -> f64 {
    if lo.is_infinite() {
        if hi.is_infinite() {
            return 0.0;
        }
        if hi > 0.0 {
            return hi / 2.0;
        }
        return hi;
    } else if hi.is_infinite() {
        return lo;
    }
    if is_linear {
        return (lo + hi) / 2.0;
    }
    let geo_mean = (lo * hi).abs().sqrt();
    if is_positive { geo_mean } else { -geo_mean }
}

/// Bucket iterator over float buckets, optionally merging to a lower target schema and skipping buckets below a
/// start value.
#[derive(Clone, Debug)]
pub struct FloatBucketIterator<'a> {
    schema: i32,
    spans: &'a [Span],
    buckets: &'a [f64],
    positive: bool,
    spans_idx: usize,
    idx_in_span: u32,
    buckets_idx: usize,
    curr_count: f64,
    curr_idx: i32,
    custom_values: &'a [f64],
    target_schema: i32,
    orig_idx: i32,
    absolute_start_value: f64,
    bound_reached_start_value: bool,
}

impl FloatBucketIterator<'_> {
    #[inline]
    pub(crate) fn stripped_at(&self) -> StrippedBucket<f64> {
        StrippedBucket { count: self.curr_count, index: self.curr_idx }
    }
}

impl BucketIterator<f64> for FloatBucketIterator<'_> {
    fn next(&mut self) -> bool {
        loop {
            if self.spans_idx >= self.spans.len() {
                return false;
            }
            let mut span = self.spans[self.spans_idx];
            if self.schema == self.target_schema {
                // Fast path for the common case.
                if self.buckets_idx == 0 {
                    self.curr_idx = span.offset;
                } else {
                    self.curr_idx = self.curr_idx.wrapping_add(1);
                }
                if self.buckets_idx >= self.buckets.len() {
                    return false;
                }
                while self.idx_in_span >= span.length {
                    self.idx_in_span = 0;
                    self.spans_idx += 1;
                    if self.spans_idx >= self.spans.len() {
                        return false;
                    }
                    span = self.spans[self.spans_idx];
                    self.curr_idx = self.curr_idx.wrapping_add(span.offset);
                }
                self.curr_count = self.buckets[self.buckets_idx];
                self.idx_in_span += 1;
                self.buckets_idx += 1;
            } else {
                let (mut orig_idx, mut spans_idx, mut idx_in_span) = (self.orig_idx, self.spans_idx, self.idx_in_span);
                let mut first_pass = true;
                self.curr_count = 0.0;
                'merge: loop {
                    if self.buckets_idx == 0 {
                        orig_idx = span.offset;
                    } else {
                        orig_idx = orig_idx.wrapping_add(1);
                    }
                    if self.buckets_idx >= self.buckets.len() {
                        if first_pass {
                            return false;
                        }
                        break 'merge;
                    }
                    while idx_in_span >= span.length {
                        idx_in_span = 0;
                        spans_idx += 1;
                        if spans_idx >= self.spans.len() {
                            if first_pass {
                                return false;
                            }
                            break 'merge;
                        }
                        span = self.spans[spans_idx];
                        orig_idx = orig_idx.wrapping_add(span.offset);
                    }
                    let curr_idx = target_idx(orig_idx, self.schema, self.target_schema);
                    if first_pass {
                        self.curr_idx = curr_idx;
                        first_pass = false;
                    } else if curr_idx != self.curr_idx {
                        break 'merge;
                    }
                    self.curr_count += self.buckets[self.buckets_idx];
                    idx_in_span += 1;
                    self.buckets_idx += 1;
                    self.orig_idx = orig_idx;
                    self.spans_idx = spans_idx;
                    self.idx_in_span = idx_in_span;
                    if self.schema == self.target_schema {
                        break 'merge;
                    }
                }
            }
            // Skip buckets before absolute_start_value for exponential schemas.
            if !self.bound_reached_start_value
                && is_exponential_schema(self.target_schema)
                && get_bound_exponential(self.curr_idx, self.target_schema) <= self.absolute_start_value
            {
                continue;
            }
            self.bound_reached_start_value = true;
            return true;
        }
    }

    fn at(&self) -> Bucket<f64> {
        make_bucket(self.curr_idx, self.target_schema, self.positive, self.custom_values, self.curr_count)
    }
}

/// Reverse bucket iterator over float buckets.
#[derive(Clone, Debug)]
pub struct ReverseFloatBucketIterator<'a> {
    schema: i32,
    spans: &'a [Span],
    buckets: &'a [f64],
    positive: bool,
    spans_idx: isize,
    idx_in_span: i32,
    buckets_idx: isize,
    curr_count: f64,
    curr_idx: i32,
    custom_values: &'a [f64],
}

impl<'a> ReverseFloatBucketIterator<'a> {
    pub fn new(spans: &'a [Span], buckets: &'a [f64], schema: i32, positive: bool, custom_values: &'a [f64]) -> Self {
        let mut r = ReverseFloatBucketIterator {
            schema,
            spans,
            buckets,
            positive,
            spans_idx: spans.len() as isize - 1,
            idx_in_span: 0,
            buckets_idx: buckets.len() as isize - 1,
            curr_count: 0.0,
            curr_idx: 0,
            custom_values,
        };
        if r.spans_idx >= 0 {
            r.idx_in_span = spans[r.spans_idx as usize].length as i32 - 1;
        }
        for s in spans {
            r.curr_idx = r.curr_idx.wrapping_add(s.offset).wrapping_add(s.length as i32);
        }
        r
    }
}

impl BucketIterator<f64> for ReverseFloatBucketIterator<'_> {
    fn next(&mut self) -> bool {
        self.curr_idx = self.curr_idx.wrapping_sub(1);
        if self.buckets_idx < 0 {
            return false;
        }
        while self.idx_in_span < 0 {
            self.spans_idx -= 1;
            if self.spans_idx < 0 {
                return false;
            }
            self.idx_in_span = self.spans[self.spans_idx as usize].length as i32 - 1;
            self.curr_idx = self.curr_idx.wrapping_sub(self.spans[self.spans_idx as usize + 1].offset);
        }
        self.curr_count = self.buckets[self.buckets_idx as usize];
        self.buckets_idx -= 1;
        self.idx_in_span -= 1;
        true
    }

    fn at(&self) -> Bucket<f64> {
        make_bucket(self.curr_idx, self.schema, self.positive, self.custom_values, self.curr_count)
    }
}

/// Iterates over negative, zero and positive buckets (or the reverse).
#[derive(Clone, Debug)]
pub struct AllFloatBucketIterator<'a> {
    h: &'a FloatHistogram,
    left_iter: ReverseFloatBucketIterator<'a>,
    right_iter: FloatBucketIterator<'a>,
    // -1: iterating left buckets; 0: zero bucket next; 1: iterating right buckets; else: done.
    state: i8,
    curr_bucket: Bucket<f64>,
}

impl BucketIterator<f64> for AllFloatBucketIterator<'_> {
    fn next(&mut self) -> bool {
        loop {
            match self.state {
                -1 => {
                    if self.left_iter.next() {
                        self.curr_bucket = self.left_iter.at();
                        let zt = self.h.zero_threshold;
                        if self.curr_bucket.upper < 0.0 && self.curr_bucket.upper > -zt {
                            self.curr_bucket.upper = -zt;
                        } else if self.curr_bucket.lower > 0.0 && self.curr_bucket.lower < zt {
                            self.curr_bucket.lower = zt;
                        }
                        return true;
                    }
                    self.state = 0;
                }
                0 => {
                    self.state = 1;
                    if self.h.zero_count > 0.0 {
                        self.curr_bucket = self.h.zero_bucket();
                        return true;
                    }
                }
                1 => {
                    if self.right_iter.next() {
                        self.curr_bucket = self.right_iter.at();
                        let zt = self.h.zero_threshold;
                        if self.curr_bucket.lower > 0.0 && self.curr_bucket.lower < zt {
                            self.curr_bucket.lower = zt;
                        } else if self.curr_bucket.upper < 0.0 && self.curr_bucket.upper > -zt {
                            self.curr_bucket.upper = -zt;
                        }
                        return true;
                    }
                    self.state = 42;
                    return false;
                }
                _ => return false,
            }
        }
    }

    fn at(&self) -> Bucket<f64> {
        self.curr_bucket
    }
}
