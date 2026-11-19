//! Port of `web/api/v1/codec_test.go`.

use web::api::MimeType;
use web::api::codec::Accept;

#[test]
fn test_mime_type_string() {
    let m = MimeType::new("application", "json");
    assert_eq!("application/json", m.to_string());
}

#[test]
fn test_mime_type_satisfies() {
    let m = MimeType::new("application", "json");
    let scenarios: Vec<(&str, Accept, bool)> = vec![
        ("exact match", Accept::new("application", "json"), true),
        ("sub-type wildcard match", Accept::new("application", "*"), true),
        ("full wildcard match", Accept::new("*", "*"), true),
        ("inverted", Accept::new("json", "application"), false),
        ("inverted sub-type wildcard", Accept::new("json", "*"), false),
        ("complete mismatch", Accept::new("text", "plain"), false),
    ];
    for (name, accept, expected) in scenarios {
        assert_eq!(expected, m.satisfies(&accept), "{}", name);
    }
}
