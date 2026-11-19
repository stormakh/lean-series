//! Port of `util/namevalidationutil/namevalidationutil_test.go`.

use model::labels::ValidationScheme;
use util::namevalidationutil::check_name_validation_scheme;

#[test]
fn test_check_name_validation_scheme() {
    assert_eq!(check_name_validation_scheme(ValidationScheme::Utf8), Ok(()));
    assert_eq!(check_name_validation_scheme(ValidationScheme::Legacy), Ok(()));
    assert_eq!(check_name_validation_scheme(ValidationScheme::Unset), Err("unset nameValidationScheme".to_string()));
    // adapted: Go also checks that the out-of-range value `ValidationScheme(20)` panics with "unhandled
    // ValidationScheme: 20". A Rust enum cannot hold such a value, so the case cannot occur; the match in
    // check_name_validation_scheme is exhaustive (checked by the compiler).
}
