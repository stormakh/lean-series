//! Port of `web/api/v1/openapi_coverage_test.go`.
//!
//! adapted: Go parses the source of `(*API).Register` and collects every `r.Get/Post/Put/Del/Options` call with a
//! literal path. The Rust `Api::register` builds paths in loops, so the routes come from the router it fills
//! (`Router::routes`). Go's source scan also sees the routes registered only with the query-cost feature, so
//! the API is built with `enable_query_cost`.

use std::collections::HashSet;
use std::sync::Arc;

use web::api::openapi::{OpenApiBuilder, OpenApiOptions};
use web::api::{Api, ApiOptions};
use web::route::Router;

/// A route registered by `Api::register`.
struct RouteInfo {
    method: String,
    path: String,
}

fn extract_routes_from_register() -> Vec<RouteInfo> {
    let api = Arc::new(Api::new(ApiOptions {
        enable_query_cost: true,
        ..Default::default()
    }));
    let mut r = Router::new();
    api.register(&mut r, "");
    r.routes()
        .into_iter()
        .map(|(method, path)| RouteInfo { method, path })
        .collect()
}

/// Converts route paths with colon parameters to OpenAPI format, e.g. "/label/:name/values" ->
/// "/label/{name}/values".
fn normalize_path_for_open_api(path: &str) -> String {
    path.split('/')
        .map(|part| match part.strip_prefix(':') {
            Some(trimmed) => format!("{{{trimmed}}}"),
            None => part.to_string(),
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// All routes registered by `Api::register` are documented in the OpenAPI specification.
#[test]
fn test_open_api_coverage() {
    let routes = extract_routes_from_register();
    assert!(!routes.is_empty(), "no routes found in Register function");

    let builder = OpenApiBuilder::new(OpenApiOptions::default());
    let mut open_api_paths = HashSet::new();
    for (path, item) in builder.get_all_path_definitions() {
        for (method, op) in [
            ("GET", &item.get),
            ("POST", &item.post),
            ("PUT", &item.put),
            ("DELETE", &item.delete),
            ("OPTIONS", &item.options),
        ] {
            if op.is_some() {
                open_api_paths.insert(format!("{path}:{method}"));
            }
        }
    }

    let ignored_routes: HashSet<&str> = [
        "/*path:OPTIONS",          // Wildcard OPTIONS handler.
        "/openapi.yaml:GET",       // Self-referential endpoint.
        "/notifications/live:GET", // SSE endpoint (version-specific).
    ]
    .into_iter()
    .collect();

    let mut missing_routes = Vec::new();
    for route in &routes {
        let key = format!(
            "{}:{}",
            normalize_path_for_open_api(&route.path),
            route.method
        );
        if ignored_routes.contains(key.as_str()) {
            continue;
        }
        if !open_api_paths.contains(&key) {
            missing_routes.push(key);
        }
    }
    assert!(
        missing_routes.is_empty(),
        "The following routes are registered but not documented in OpenAPI spec:\n{}",
        missing_routes.join("\n")
    );
}

/// The OpenAPI spec does not document routes that are not registered.
#[test]
fn test_open_api_has_no_extra_routes() {
    let routes = extract_routes_from_register();
    assert!(!routes.is_empty(), "no routes found in Register function");

    let registered_routes: HashSet<String> = routes
        .iter()
        .map(|r| format!("{}:{}", normalize_path_for_open_api(&r.path), r.method))
        .collect();

    let builder = OpenApiBuilder::new(OpenApiOptions::default());
    let mut extra_routes = Vec::new();
    for (path, item) in builder.get_all_path_definitions() {
        for (method, op) in [
            ("GET", &item.get),
            ("POST", &item.post),
            ("PUT", &item.put),
            ("DELETE", &item.delete),
            ("OPTIONS", &item.options),
        ] {
            if op.is_some() {
                let key = format!("{path}:{method}");
                if !registered_routes.contains(&key) {
                    extra_routes.push(key);
                }
            }
        }
    }
    assert!(
        extra_routes.is_empty(),
        "The following routes are documented in OpenAPI but not registered:\n{}",
        extra_routes.join("\n")
    );
}
