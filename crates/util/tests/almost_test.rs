//! Port of `util/almost/almost_test.go`.

use model::value::STALE_NAN;
use util::almost::equal;

#[test]
fn test_equal() {
    let stale_nan = f64::from_bits(STALE_NAN);
    let nan = f64::NAN;
    let tests: &[(f64, f64, f64, bool)] = &[
        (0.0, 0.0, 0.0, true),
        (0.0, 0.1, 0.0, false),
        (1.0, 1.1, 0.1, true),
        (-1.0, -1.1, 0.1, true),
        (f64::MAX, f64::MAX / 10.0, 0.1, false),
        (1.0, nan, 0.1, false),
        (nan, nan, 0.1, true),
        (nan, stale_nan, 0.1, false),
        (stale_nan, nan, 0.1, false),
        (stale_nan, stale_nan, 0.1, true),
    ];
    for &(a, b, epsilon, want) in tests {
        assert_eq!(equal(a, b, epsilon), want, "Equal({a},{b},{epsilon})");
    }
}
