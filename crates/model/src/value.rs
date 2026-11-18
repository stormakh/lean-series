//! Port of `model/value`: special float values.

/// A quiet NaN. This is also `math.NaN()` in Go.
pub const NORMAL_NAN: u64 = 0x7ff8000000000001;

/// A signaling NaN, due to the MSB of the mantissa being 0. It marks a series as stale.
pub const STALE_NAN: u64 = 0x7ff0000000000002;

/// Returns true when the provided NaN value is a stale marker.
#[inline]
pub fn is_stale_nan(v: f64) -> bool {
    v.to_bits() == STALE_NAN
}

/// The stale marker as a float.
#[inline]
pub fn stale_nan() -> f64 {
    f64::from_bits(STALE_NAN)
}

/// The quiet NaN that Go's `math.NaN()` returns.
#[inline]
pub fn normal_nan() -> f64 {
    f64::from_bits(NORMAL_NAN)
}
