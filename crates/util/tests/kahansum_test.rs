//! Port of `util/kahansum/kahansum_test.go`.

use util::kahansum::is_inf;

#[test]
fn test_is_inf() {
    for f in [f64::INFINITY, f64::NEG_INFINITY, f64::MAX, -f64::MAX, 1.0, -1.0, 0.0, f64::NAN] {
        assert_eq!(f.is_infinite(), is_inf(f), "{f:e}");
    }
}
