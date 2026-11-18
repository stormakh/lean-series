// Port of model/labels/sharding_test.go.

use model::labels::{empty_labels, from_strings, stable_hash, METRIC_NAME};

// The hashes this test asserts should not be changed.
#[test]
fn test_stable_hash() {
    for (expected, lbls) in [
        (0xef46db3751d8e999u64, empty_labels()),
        (0x347c8ee7a9e29708, from_strings(&["hello", "world"])),
        (0xcbab40540f26097d, from_strings(&[METRIC_NAME, "metric", "label", "value"])),
    ] {
        assert_eq!(expected, stable_hash(&lbls));
    }
}
