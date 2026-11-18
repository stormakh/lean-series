// Port of model/metadata/metadata_test.go.

use model::metadata::{is_zero_handle, Handle, Metadata, MetricType};

#[test]
fn test_is_zero_handle() {
    let h = Handle::default();
    assert!(is_zero_handle(&h));
    assert!(!is_zero_handle(&Handle::make(Metadata { typ: MetricType::Counter, ..Default::default() })));
}

fn md(typ: MetricType, unit: &str, help: &str) -> Metadata {
    Metadata { typ, unit: unit.to_string(), help: help.to_string() }
}

#[test]
fn test_metadata_is_empty() {
    let tests: Vec<(&str, Metadata, bool)> = vec![
        ("empty struct", Metadata::default(), true),
        ("unknown type with empty fields", md(MetricType::Unknown, "", ""), true),
        ("type", md(MetricType::Counter, "", ""), false),
        ("unit", md(MetricType::Empty, "seconds", ""), false),
        ("help", md(MetricType::Empty, "", "help text"), false),
        ("unknown type with help", md(MetricType::Unknown, "", "help text"), false),
    ];
    for (name, m, expected) in tests {
        assert_eq!(expected, m.is_empty(), "{}", name);
    }
}

#[test]
fn test_metadata_equals() {
    let tests: Vec<(&str, Metadata, Metadata, bool)> = vec![
        ("same empty", Metadata::default(), Metadata::default(), true),
        ("same", md(MetricType::Counter, "s", "doc"), md(MetricType::Counter, "s", "doc"), true),
        ("same unknown type", md(MetricType::Unknown, "s", "doc"), md(MetricType::Unknown, "s", "doc"), true),
        ("same mixed unknown type", md(MetricType::Empty, "s", "doc"), md(MetricType::Unknown, "s", "doc"), true),
        ("different unit", md(MetricType::Counter, "s", "doc"), md(MetricType::Counter, "bytes", "doc"), false),
        ("different help", md(MetricType::Counter, "s", "doc"), md(MetricType::Counter, "s", "other doc"), false),
        ("different type", md(MetricType::Counter, "s", "doc"), md(MetricType::Gauge, "s", "doc"), false),
        ("different type with unknown", md(MetricType::Unknown, "s", "doc"), md(MetricType::Counter, "s", "doc"), false),
        ("different type with empty", md(MetricType::Empty, "s", "doc"), md(MetricType::Counter, "s", "doc"), false),
    ];
    for (name, m, other, expected) in tests {
        assert_eq!(expected, m.equals(&other), "Metadata.Equals() {}", name);
    }
}
