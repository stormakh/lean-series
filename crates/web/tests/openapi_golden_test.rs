//! Port of `web/api/v1/openapi_golden_test.go`. The golden files are read from the upstream tree (the Go
//! `-update-openapi-spec` mode, which rewrites them, is not ported).

use std::sync::Arc;

use http::{Body, Request};
use web::api::{Api, ApiOptions};
use yaml::Value;

fn new_test_api() -> Arc<Api> {
    Arc::new(Api::new(ApiOptions::default()))
}

/// GET `target` through the API router; returns the status and the body.
fn get(api: &Arc<Api>, target: &str) -> (u16, String) {
    let router = api.router("/api/v1");
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let req = Request {
        method: "GET".to_string(),
        path: path.to_string(),
        query: query.to_string(),
        ..Default::default()
    };
    let resp = router.serve(req);
    let body = match resp.body {
        Body::Bytes(b) => String::from_utf8(b).unwrap(),
        other => panic!("unexpected body {other:?}"),
    };
    (resp.status, body)
}

fn golden(name: &str) -> String {
    let p = format!(
        "{}/../../upstream/web/api/v1/testdata/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(&p).unwrap_or_else(|e| panic!("failed to read golden file {p}: {e}"))
}

fn has_path(spec: &Value, path: &str) -> bool {
    match spec.get("paths") {
        Some(Value::Map(p)) => p.iter().any(|(k, _)| k.as_str() == Some(path)),
        other => panic!("paths should be a map, got {other:?}"),
    }
}

/// The OpenAPI 3.1 spec matches the golden file.
#[test]
fn test_open_api_golden_3_1() {
    let api = new_test_api();
    let (status, body) = get(&api, "/api/v1/openapi.yaml");
    assert_eq!(status, 200, "expected HTTP 200 for OpenAPI spec endpoint");
    assert!(!body.is_empty(), "OpenAPI spec should not be empty");

    let golden_data = golden("openapi_3.1_golden.yaml");
    assert!(
        golden_data == body,
        "OpenAPI 3.1 spec does not match golden file.\n{}",
        first_diff(&golden_data, &body)
    );

    // Verify version field is 3.1.0.
    let spec: Value = yaml::unmarshal(&body).unwrap();
    assert_eq!(
        spec.get("openapi"),
        Some(&Value::Str("3.1.0".to_string())),
        "OpenAPI version should be 3.1.0"
    );

    // Verify /notifications/live is NOT present in 3.1 spec.
    assert!(
        !has_path(&spec, "/notifications/live"),
        "/notifications/live should not be in OpenAPI 3.1 spec"
    );
}

/// The OpenAPI 3.2 spec matches the golden file.
#[test]
fn test_open_api_golden_3_2() {
    let api = new_test_api();
    let (status, body) = get(&api, "/api/v1/openapi.yaml?openapi_version=3.2");
    assert_eq!(status, 200, "expected HTTP 200 for OpenAPI spec endpoint");
    assert!(!body.is_empty(), "OpenAPI spec should not be empty");

    let golden_data = golden("openapi_3.2_golden.yaml");
    assert!(
        golden_data == body,
        "OpenAPI 3.2 spec does not match golden file.\n{}",
        first_diff(&golden_data, &body)
    );

    // Verify version field is 3.2.0.
    let spec: Value = yaml::unmarshal(&body).unwrap();
    assert_eq!(
        spec.get("openapi"),
        Some(&Value::Str("3.2.0".to_string())),
        "OpenAPI version should be 3.2.0"
    );

    // Verify /notifications/live IS present in 3.2 spec.
    assert!(
        has_path(&spec, "/notifications/live"),
        "/notifications/live should be in OpenAPI 3.2 spec"
    );
}

/// Version query parameter handling.
#[test]
fn test_open_api_version_selection() {
    let api = new_test_api();
    let tests = [
        ("default to 3.1.0", "/api/v1/openapi.yaml", "3.1.0", false),
        (
            "explicit 3.1",
            "/api/v1/openapi.yaml?openapi_version=3.1",
            "3.1.0",
            false,
        ),
        (
            "explicit 3.2",
            "/api/v1/openapi.yaml?openapi_version=3.2",
            "3.2.0",
            true,
        ),
        (
            "invalid version defaults to 3.1.0",
            "/api/v1/openapi.yaml?openapi_version=4.0",
            "3.1.0",
            false,
        ),
    ];
    for (name, url, expected_version, expect_live_path) in tests {
        let (status, body) = get(&api, url);
        assert_eq!(status, 200, "{name}");
        let spec: Value = yaml::unmarshal(&body).unwrap();
        assert_eq!(
            spec.get("openapi"),
            Some(&Value::Str(expected_version.to_string())),
            "{name}"
        );
        assert_eq!(
            has_path(&spec, "/notifications/live"),
            expect_live_path,
            "{name}"
        );
    }
}

/// The first differing line, to make a mismatch readable.
fn first_diff(want: &str, got: &str) -> String {
    for (i, (w, g)) in want.lines().zip(got.lines()).enumerate() {
        if w != g {
            return format!("line {}:\n  want: {w}\n  got:  {g}", i + 1);
        }
    }
    format!(
        "lengths differ: want {} lines, got {} lines",
        want.lines().count(),
        got.lines().count()
    )
}
