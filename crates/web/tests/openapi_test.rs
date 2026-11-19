//! Port of `web/api/v1/openapi_test.go`.

use http::{Body, Request, Response};
use web::api::openapi::{OpenApiBuilder, OpenApiOptions};
use yaml::Value;

fn get(builder: &OpenApiBuilder, target: &str) -> Response {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let req = Request {
        method: "GET".to_string(),
        path: path.to_string(),
        query: query.to_string(),
        ..Default::default()
    };
    builder.serve_openapi(&req)
}

fn body(r: &Response) -> String {
    match &r.body {
        Body::Bytes(b) => String::from_utf8(b.clone()).unwrap(),
        other => panic!("unexpected body {other:?}"),
    }
}

fn header<'a>(r: &'a Response, name: &str) -> &'a str {
    r.headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
        .unwrap_or("")
}

fn parse(text: &str) -> Value {
    let v: Value = yaml::unmarshal(text).expect("valid YAML");
    assert!(matches!(v, Value::Map(_)), "spec should be a map");
    v
}

fn keys(v: &Value) -> Vec<String> {
    match v {
        Value::Map(p) => p
            .iter()
            .map(|(k, _)| k.as_str().unwrap_or_default().to_string())
            .collect(),
        other => panic!("not a map: {other:?}"),
    }
}

/// The OpenAPI endpoint serves a valid specification with correct headers, structure conforming to
/// OpenAPI 3.1 standards, and consistent responses.
#[test]
fn test_open_apihttp_handler() {
    let builder = OpenApiBuilder::new(OpenApiOptions::default());

    // First request.
    let rec1 = get(&builder, "/api/v1/openapi.yaml");

    // Verify status code and headers.
    assert_eq!(rec1.status, 200);
    assert!(
        header(&rec1, "Content-Type").starts_with("application/yaml"),
        "Content-Type should start with application/yaml"
    );
    assert_eq!(
        header(&rec1, "Cache-Control"),
        "no-cache, no-store, must-revalidate"
    );

    // Verify it is valid YAML.
    let spec = parse(&body(&rec1));

    // Verify structure.
    let k = keys(&spec);
    for want in ["openapi", "info", "paths", "components"] {
        assert!(k.iter().any(|x| x == want), "missing {want}");
    }

    // Verify OpenAPI version (default is 3.1.0).
    assert_eq!(spec.get("openapi"), Some(&Value::Str("3.1.0".to_string())));

    // Verify info section.
    let info = spec.get("info").unwrap();
    assert!(matches!(info, Value::Map(_)), "info should be a map");
    assert_eq!(
        info.get("title"),
        Some(&Value::Str("Prometheus API".to_string()))
    );

    // Verify paths exist.
    let paths = spec.get("paths").unwrap();
    assert!(matches!(paths, Value::Map(_)), "paths should be a map");
    assert!(!keys(paths).is_empty(), "paths should not be empty");

    // Second request to verify response consistency.
    let rec2 = get(&builder, "/api/v1/openapi.yaml");

    // Both responses should be identical.
    assert_eq!(body(&rec1), body(&rec2));
}

/// The IncludePaths option filters which API paths are included in the generated specification.
#[test]
fn test_open_api_path_filtering() {
    struct Case {
        name: &'static str,
        include_paths: Vec<&'static str>,
        want_paths: Vec<&'static str>,
        exclude_paths: Vec<&'static str>,
    }
    let tests = vec![
        Case {
            name: "no filter includes all",
            include_paths: vec![],
            want_paths: vec!["/query", "/labels", "/alerts", "/targets"],
            exclude_paths: vec![],
        },
        Case {
            name: "filter query paths",
            include_paths: vec!["/query"],
            want_paths: vec!["/query", "/query_range", "/query_exemplars"],
            exclude_paths: vec!["/labels", "/alerts", "/targets"],
        },
        Case {
            name: "filter status paths",
            include_paths: vec!["/status"],
            want_paths: vec!["/status/config", "/status/flags", "/status/runtimeinfo"],
            exclude_paths: vec!["/query", "/alerts", "/targets"],
        },
        Case {
            name: "filter multiple prefixes",
            include_paths: vec!["/label", "/series"],
            want_paths: vec!["/labels", "/label/{name}/values", "/series"],
            exclude_paths: vec!["/query", "/alerts", "/targets"],
        },
        Case {
            name: "exact path match",
            include_paths: vec!["/alerts"],
            want_paths: vec!["/alerts"],
            exclude_paths: vec!["/alertmanagers", "/query"],
        },
    ];

    for tc in tests {
        let builder = OpenApiBuilder::new(OpenApiOptions {
            include_paths: tc.include_paths.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        });
        let rec = get(&builder, "/api/v1/openapi.yaml");
        assert_eq!(rec.status, 200, "{}", tc.name);

        let spec = parse(&body(&rec));
        let paths = spec.get("paths").unwrap();
        assert!(
            matches!(paths, Value::Map(_)),
            "{}: paths should be a map",
            tc.name
        );
        let k = keys(paths);
        for want in &tc.want_paths {
            assert!(k.iter().any(|x| x == want), "{}: missing {want}", tc.name);
        }
        for exclude in &tc.exclude_paths {
            assert!(
                !k.iter().any(|x| x == exclude),
                "{}: unexpected {exclude}",
                tc.name
            );
        }
    }
}

/// All referenced schemas in paths are defined in the components/schemas section.
#[test]
fn test_open_api_schema_completeness() {
    let builder = OpenApiBuilder::new(OpenApiOptions::default());
    let rec = get(&builder, "/api/v1/openapi.yaml");
    let spec = parse(&body(&rec));

    let components = spec.get("components").unwrap();
    assert!(
        matches!(components, Value::Map(_)),
        "components should be a map"
    );
    let schemas = components.get("schemas").unwrap();
    assert!(matches!(schemas, Value::Map(_)), "schemas should be a map");

    // Verify essential schemas are present.
    let essential_schemas = [
        "Error",
        "Labels",
        "QueryOutputBody",
        "LabelsOutputBody",
        "SeriesOutputBody",
        "ScrapePoolConfigOutputBody",
        "TargetsOutputBody",
        "AlertsOutputBody",
        "RulesOutputBody",
        "StatusConfigOutputBody",
        "StatusFlagsOutputBody",
        "PrometheusVersion",
    ];
    let k = keys(schemas);
    for schema in essential_schemas {
        assert!(k.iter().any(|x| x == schema), "missing schema {schema}");
    }
}

/// `should_include_path` matches paths against the IncludePaths filter configuration.
#[test]
fn test_open_api_should_include_path() {
    let tests: Vec<(&str, Vec<&str>, &str, bool)> = vec![
        ("empty filter includes all", vec![], "/query", true),
        ("exact match", vec!["/query"], "/query", true),
        ("prefix match", vec!["/query"], "/query_range", true),
        ("no match", vec!["/query"], "/labels", false),
        (
            "multiple filters with match",
            vec!["/labels", "/series"],
            "/series",
            true,
        ),
        (
            "multiple filters without match",
            vec!["/labels", "/series"],
            "/query",
            false,
        ),
    ];
    for (name, include_paths, path, expected) in tests {
        let builder = OpenApiBuilder::new(OpenApiOptions {
            include_paths: include_paths.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        });
        assert_eq!(builder.should_include_path(path), expected, "{name}");
    }
}

/// Both OpenAPI versions are generated and 3.2 has exactly one more path than 3.1 (/notifications/live).
#[test]
fn test_open_api_version_consistency() {
    let builder = OpenApiBuilder::new(OpenApiOptions::default());

    // Fetch OpenAPI 3.1 spec (default).
    let rec31 = get(&builder, "/api/v1/openapi.yaml");
    assert_eq!(rec31.status, 200);

    // Fetch OpenAPI 3.2 spec.
    let rec32 = get(&builder, "/api/v1/openapi.yaml?openapi_version=3.2");
    assert_eq!(rec32.status, 200);

    // Parse both specs.
    let spec31 = parse(&body(&rec31));
    let spec32 = parse(&body(&rec32));

    // Verify versions are different.
    assert_eq!(
        spec31.get("openapi"),
        Some(&Value::Str("3.1.0".to_string()))
    );
    assert_eq!(
        spec32.get("openapi"),
        Some(&Value::Str("3.2.0".to_string()))
    );

    // Verify /notifications/live is only in 3.2.
    let paths31 = keys(spec31.get("paths").unwrap());
    let paths32 = keys(spec32.get("paths").unwrap());
    assert!(!paths31.iter().any(|p| p == "/notifications/live"));
    assert!(paths32.iter().any(|p| p == "/notifications/live"));

    // Verify 3.2 has exactly one more path than 3.1.
    assert_eq!(
        paths32.len(),
        paths31.len() + 1,
        "OpenAPI 3.2 should have exactly one more path than 3.1"
    );
}

/// The search endpoint default for "limit" reflects the MaxSearchLimit option when it is smaller than the
/// built-in default.
#[test]
fn test_open_api_search_default_limit() {
    let serve_spec = |opts: OpenApiOptions| -> Value {
        let builder = OpenApiBuilder::new(opts);
        let rec = get(&builder, "/api/v1/openapi.yaml");
        assert_eq!(rec.status, 200);
        parse(&body(&rec))
    };

    let search_limit_default = |spec: &Value| -> Option<Value> {
        let get = spec.get("paths")?.get("/search/metric_names")?.get("get")?;
        let Some(Value::Seq(params)) = get.get("parameters") else {
            panic!("parameters should be a list");
        };
        for p in params {
            if p.get("name") == Some(&Value::Str("limit".to_string())) {
                return p.get("schema")?.get("default").cloned();
            }
        }
        None
    };

    // Default limit is 100 when no cap is set.
    let spec = serve_spec(OpenApiOptions::default());
    assert_eq!(search_limit_default(&spec), Some(Value::Int(100)));

    // Default limit is capped when MaxSearchLimit is smaller.
    let spec = serve_spec(OpenApiOptions {
        max_search_limit: 50,
        ..Default::default()
    });
    assert_eq!(search_limit_default(&spec), Some(Value::Int(50)));

    // Default limit is 100 when MaxSearchLimit exceeds default.
    let spec = serve_spec(OpenApiOptions {
        max_search_limit: 200,
        ..Default::default()
    });
    assert_eq!(search_limit_default(&spec), Some(Value::Int(100)));
}
