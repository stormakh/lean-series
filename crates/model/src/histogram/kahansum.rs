//! Kahan-Neumaier summation (port of `util/kahansum`). Rust never fuses float operations into FMA, so no explicit
//! rounding is needed (Go needs casts for that).

#[inline]
fn is_inf(f: f64) -> bool {
    f > f64::MAX || f < -f64::MAX
}

/// Adds `inc` to `sum` with compensation `c`; returns the new sum and compensation.
#[inline]
pub fn inc(inc: f64, sum: f64, c: f64) -> (f64, f64) {
    let t = sum + inc;
    let mut c = c;
    if is_inf(t) {
        c = 0.0;
    } else if sum.abs() >= inc.abs() {
        // Using Neumaier improvement, swap if next term larger than sum.
        c += (sum - t) + inc;
    } else {
        c += (inc - t) + sum;
    }
    (t, c)
}

/// Subtracts `dec` from `sum` with compensation `c`.
#[inline]
pub fn dec(dec: f64, sum: f64, c: f64) -> (f64, f64) {
    inc(-dec, sum, c)
}
