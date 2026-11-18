//! NHCB to classic histogram conversion (port of `model/histogram/convert.go`).

use super::*;

/// Go's `nhcb any` argument: an integer or a float histogram.
#[derive(Clone, Copy, Debug)]
pub enum HistogramRef<'a> {
    Int(&'a Histogram),
    Float(&'a FloatHistogram),
}

const STALE_NAN: u64 = 0x7ff0000000000002;

/// Returns the cumulative bucket counts of a valid NHCB, one per custom bound followed by +Inf, along with its
/// count and sum. It reuses `buf`. Every returned value of a stale marker is a stale marker. `None` is Go's nil
/// (an unsupported type).
pub fn classic_bucket_counts(nhcb: Option<HistogramRef<'_>>, mut buf: Vec<f64>) -> Result<(Vec<f64>, f64, f64), Error> {
    let mut idx: usize = 0; // Tracks buckets in the classic histogram.
    let mut curr_idx: usize = 0; // Tracks buckets in the native histogram.
    let count;
    let sum;
    match nhcb {
        Some(HistogramRef::Int(h)) => {
            if !is_custom_buckets_schema(h.schema) {
                return Err(Error::plain("unsupported histogram schema, not a NHCB"));
            }
            if let Err(e) = h.validate() {
                return Err(Error::plain(e.to_string()));
            }
            let n = h.custom_values_slice().len() + 1;
            buf.clear();
            buf.resize(n, 0.0);
            // Histograms are in delta format so we first bring them to absolute format.
            let mut acc: i64 = 0;
            for s in &h.positive_spans {
                // Skipped buckets are empty, so leave them at zero.
                idx = (idx as i64 + s.offset as i64) as usize;
                for _ in 0..s.length {
                    acc = acc.wrapping_add(h.positive_buckets[curr_idx]);
                    buf[idx] = acc as f64;
                    idx += 1;
                    curr_idx += 1;
                }
            }
            count = h.count as f64;
            sum = h.sum;
        }
        Some(HistogramRef::Float(h)) => {
            if !is_custom_buckets_schema(h.schema) {
                return Err(Error::plain("unsupported histogram schema, not a NHCB"));
            }
            if let Err(e) = h.validate() {
                return Err(Error::plain(e.to_string()));
            }
            let n = h.custom_values_slice().len() + 1;
            buf.clear();
            buf.resize(n, 0.0);
            for s in &h.positive_spans {
                idx = (idx as i64 + s.offset as i64) as usize;
                for _ in 0..s.length {
                    buf[idx] = h.positive_buckets[curr_idx];
                    idx += 1;
                    curr_idx += 1;
                }
            }
            count = h.count;
            sum = h.sum;
        }
        None => return Err(Error::plain("unsupported histogram type: <nil>")),
    }
    // Classic buckets are cumulative, and every value of a stale marker is stale.
    let is_stale = sum.to_bits() == STALE_NAN;
    let mut curr_count = 0.0;
    for b in buf.iter_mut() {
        curr_count += *b;
        *b = curr_count;
        if is_stale {
            *b = sum;
        }
    }
    let count = if is_stale { sum } else { count };
    Ok((buf, count, sum))
}
