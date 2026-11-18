//! Port of `storage/errors_test.go`.

use storage::*;

#[test]
fn test_append_partial_error_to_error() {
    // nil receiver returns nil.
    assert!(AppendPartialError::to_error(None).is_none());

    // Empty ExemplarErrors returns nil.
    let empty_err = AppendPartialError::default();
    assert!(AppendPartialError::to_error(Some(&empty_err)).is_none());

    // Also test explicitly empty slice.
    let empty_slice_err = AppendPartialError {
        exemplar_errors: Vec::new(),
        ..Default::default()
    };
    assert!(AppendPartialError::to_error(Some(&empty_slice_err)).is_none());

    // Non-empty ExemplarErrors returns the error.
    let non_empty_err = AppendPartialError {
        exemplar_errors: vec![ExemplarError {
            index: 1,
            err: ERR_OUT_OF_ORDER_EXEMPLAR,
        }],
        ..Default::default()
    };
    let e = AppendPartialError::to_error(Some(&non_empty_err)).expect("an error");
    // adapted: Go compares with the *AppendPartialError value itself; its Is matches any AppendPartialError.
    assert!(e.is(&Error::AppendPartial(std::sync::Arc::new(
        non_empty_err.clone()
    ))));

    // Exemplar errors match their cause, and only causes are part of the message.
    assert!(non_empty_err.exemplar_errors[0].is(&ERR_OUT_OF_ORDER_EXEMPLAR));
    assert_eq!(
        non_empty_err.to_string(),
        ERR_OUT_OF_ORDER_EXEMPLAR.to_string()
    );
}

#[test]
fn test_err_duplicate_sample_for_timestamp() {
    // All errDuplicateSampleForTimestamp are ErrDuplicateSampleForTimestamp.
    assert!(
        ERR_DUPLICATE_SAMPLE_FOR_TIMESTAMP.is(&Error::DuplicateSample(DuplicateSampleError::ZERO))
    );

    // Same type only is if it has same properties.
    let err = new_duplicate_float_err(1_000, 10.0, 20.0);
    let same_err = new_duplicate_float_err(1_000, 10.0, 20.0);
    let different_err = new_duplicate_float_err(1_001, 30.0, 40.0);

    assert!(err.is(&same_err));
    assert!(!err.is(&different_err));

    // Also works when err is wrapped.
    assert!(Error::wrap("failed", err.clone()).is(&same_err));
    assert!(!Error::wrap("failed", err).is(&different_err));
}
