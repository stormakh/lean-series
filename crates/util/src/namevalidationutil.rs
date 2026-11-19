//! Port of `util/namevalidationutil`.

use model::labels::ValidationScheme;

/// Go `CheckNameValidationScheme`: an error iff the scheme is unset.
///
/// Go also panics for an out-of-range integer value; a Rust `ValidationScheme` cannot hold one.
pub fn check_name_validation_scheme(name_validation_scheme: ValidationScheme) -> Result<(), String> {
    match name_validation_scheme {
        ValidationScheme::Utf8 | ValidationScheme::Legacy => Ok(()),
        ValidationScheme::Unset => Err("unset nameValidationScheme".to_string()),
    }
}
