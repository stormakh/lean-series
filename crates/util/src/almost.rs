//! Port of `util/almost`: float comparison with a relative tolerance.

use model::value::is_stale_nan;

/// The smallest positive normal value of type float64 (Go `math.Float64frombits(0x0010000000000000)`).
const MIN_NORMAL: f64 = f64::MIN_POSITIVE;

/// Go `Equal`: true if a and b differ by less than their sum multiplied by epsilon, or if both are StaleNaN, or
/// if both are any other NaN.
pub fn equal(a: f64, b: f64, epsilon: f64) -> bool {
    // StaleNaN is a special value that is used as staleness maker, and we don't want it to compare equal to any
    // other NaN.
    if is_stale_nan(a) || is_stale_nan(b) {
        return is_stale_nan(a) && is_stale_nan(b);
    }
    // NaN has no equality but for testing we still want to know whether both values are NaN.
    if a.is_nan() && b.is_nan() {
        return true;
    }
    // Cf. http://floating-point-gui.de/errors/comparison/
    if a == b {
        return true;
    }
    let abs_sum = a.abs() + b.abs();
    let diff = (a - b).abs();
    if a == 0.0 || b == 0.0 || abs_sum < MIN_NORMAL {
        return diff < epsilon * MIN_NORMAL;
    }
    diff / abs_sum.min(f64::MAX) < epsilon
}
