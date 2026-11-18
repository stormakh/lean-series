// Port of model/labels/float_test.go.

use model::labels::{append_open_metrics_float, format_open_metrics_float};

#[test]
fn test_open_metrics_float_formatting() {
    let tests: Vec<(f64, &str)> = vec![
        (0.0, "0.0"),
        (-0.0, "0.0"),
        (1.0, "1.0"),
        (-1.0, "-1.0"),
        (42.0, "42.0"),
        (-100.0, "-100.0"),
        (0.005, "0.005"),
        (1.5, "1.5"),
        (1e6, "1e+06"),
        (1e-5, "1e-05"),
        (f64::NAN, "NaN"),
        (f64::INFINITY, "+Inf"),
        (f64::NEG_INFINITY, "-Inf"),
    ];
    for (f, want) in tests {
        assert_eq!(want, format_open_metrics_float(f));
        let mut s = String::new();
        append_open_metrics_float(&mut s, f);
        assert_eq!(want, s);
        // Verify existing "." or "e" bytes in dst prefix do not suppress ".0".
        let mut s = String::from("le.e=");
        append_open_metrics_float(&mut s, f);
        assert_eq!(format!("le.e={}", want), s);
    }
}
