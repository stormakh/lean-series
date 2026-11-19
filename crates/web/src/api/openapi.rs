//! Port of `web/api/v1/openapi*.go`: the OpenAPI 3.1 and 3.2 documents of the HTTP API, built on the
//! first request for each version and kept as YAML text.

pub mod emit;
mod examples;
pub mod model;
mod paths;
mod schemas;

use std::borrow::Cow;
use std::sync::OnceLock;

use http::{Request, Response};

use self::emit::Node;
use self::model::{MapBuilder, PathItem};

/// OpenAPI 3.1.0 is the default version with broader compatibility.
const OPENAPI_VERSION_31: &str = "3.1.0";
/// OpenAPI 3.2.0 supports advanced features like itemSchema for SSE streams.
const OPENAPI_VERSION_32: &str = "3.2.0";

/// Go `defaultSearchLimit` (search.go).
pub(crate) const DEFAULT_SEARCH_LIMIT: i64 = 100;
/// Go `defaultSearchBatchSize` (search.go).
pub(crate) const DEFAULT_SEARCH_BATCH_SIZE: i64 = 100;
/// Go `FuzzAlgorithms()` (search.go).
pub(crate) const FUZZ_ALGORITHMS: &[&str] = &["subsequence", "jarowinkler"];

/// Go `OpenAPIOptions`.
#[derive(Clone, Debug, Default)]
pub struct OpenApiOptions {
    /// Filters which paths to include in the spec. If empty, all paths are included. Paths are matched by
    /// prefix (e.g., "/query" matches "/query" and "/query_range").
    pub include_paths: Vec<String>,
    /// The external URL of the Prometheus server (e.g., "http://prometheus.example.com:9090").
    pub external_url: String,
    /// The API version to include in the spec. If empty, defaults to "0.0.1-undefined".
    pub version: String,
    /// The operator-configured cap for the search API "limit" parameter. Zero means no cap.
    pub max_search_limit: i64,
}

/// Go `OpenAPIBuilder`. Go renders both documents in `NewOpenAPIBuilder`; this builds each one on its first
/// request.
pub struct OpenApiBuilder {
    pub opts: OpenApiOptions,
    cached_yaml_31: OnceLock<Vec<u8>>,
    cached_yaml_32: OnceLock<Vec<u8>>,
}

impl OpenApiBuilder {
    /// Go `NewOpenAPIBuilder`.
    pub fn new(opts: OpenApiOptions) -> OpenApiBuilder {
        OpenApiBuilder {
            opts,
            cached_yaml_31: OnceLock::new(),
            cached_yaml_32: OnceLock::new(),
        }
    }

    /// The YAML document of an OpenAPI version ("3.1.0" or "3.2.0").
    pub fn yaml(&self, version: &str) -> &[u8] {
        let cell = if version == OPENAPI_VERSION_32 {
            &self.cached_yaml_32
        } else {
            &self.cached_yaml_31
        };
        cell.get_or_init(|| {
            let v = if version == OPENAPI_VERSION_32 {
                OPENAPI_VERSION_32
            } else {
                OPENAPI_VERSION_31
            };
            emit::render(&self.build_document(v)).into_bytes()
        })
    }

    /// Go `ServeOpenAPI`: OpenAPI 3.1.0 by default, 3.2.0 with `?openapi_version=3.2`.
    pub fn serve_openapi(&self, req: &Request) -> Response {
        let version = match req.query_get("openapi_version").as_deref() {
            Some("3.2") | Some("3.2.0") => OPENAPI_VERSION_32,
            _ => OPENAPI_VERSION_31,
        };
        let mut resp = Response::text(
            200,
            "application/yaml; charset=utf-8",
            self.yaml(version).to_vec(),
        );
        resp.headers.push((
            "Cache-Control".to_string(),
            "no-cache, no-store, must-revalidate".to_string(),
        ));
        resp
    }

    /// Go `searchDefaultLimit`: the default of the search "limit" parameter, capped by `max_search_limit`
    /// when it is smaller.
    pub(crate) fn search_default_limit(&self) -> i64 {
        if self.opts.max_search_limit > 0 && self.opts.max_search_limit < DEFAULT_SEARCH_LIMIT {
            return self.opts.max_search_limit;
        }
        DEFAULT_SEARCH_LIMIT
    }

    /// Go `shouldIncludePath`.
    pub fn should_include_path(&self, path: &str) -> bool {
        if self.opts.include_paths.is_empty() {
            return true;
        }
        self.opts
            .include_paths
            .iter()
            .any(|include| path.starts_with(include.as_str()) || path == include)
    }

    /// Go `shouldIncludePathForVersion`: OpenAPI 3.1 leaves out `/notifications/live`, which uses the
    /// 3.2-only `itemSchema`.
    fn should_include_path_for_version(&self, path: &str, version: &str) -> bool {
        if !self.should_include_path(path) {
            return false;
        }
        !(version == OPENAPI_VERSION_31 && path == "/notifications/live")
    }

    /// Go `buildDocument`.
    fn build_document(&self, version: &str) -> Node {
        let mut m = MapBuilder::default();
        m.put("openapi", Node::from(version.to_string()));
        m.put("info", self.build_info());
        m.put(
            "servers",
            Node::Seq(vec![Node::Map(vec![(
                Cow::Borrowed("url"),
                Node::from(self.build_server_url()),
            )])]),
        );
        let paths = self
            .get_all_path_definitions()
            .into_iter()
            .filter(|(p, _)| self.should_include_path_for_version(p, version))
            .map(|(p, item)| (Cow::Borrowed(p), item.to_node()))
            .collect();
        m.put("paths", Node::Map(paths));
        let schemas = self
            .build_component_schemas()
            .into_iter()
            .map(|(k, s)| (Cow::Borrowed(k), s.to_node()))
            .collect();
        m.put(
            "components",
            Node::Map(vec![(Cow::Borrowed("schemas"), Node::Map(schemas))]),
        );
        m.put("tags", build_tags(version));
        m.node()
    }

    /// Go `buildInfo`.
    fn build_info(&self) -> Node {
        let version = if self.opts.version.is_empty() {
            "0.0.1-undefined".to_string()
        } else {
            self.opts.version.clone()
        };
        let mut contact = MapBuilder::default();
        contact.put("name", Node::from("Prometheus Community"));
        contact.put("url", Node::from("https://prometheus.io/community/"));
        let mut m = MapBuilder::default();
        m.put("title", Node::from("Prometheus API"));
        m.put(
            "description",
            Node::from(
                "Prometheus is an Open-Source monitoring system with a dimensional data model, flexible query language, efficient time series database and modern alerting approach.",
            ),
        );
        m.put("contact", contact.node());
        m.put("version", Node::from(version));
        m.node()
    }

    /// Go `buildServers`: the external URL (always set by the server) with `/api/v1` appended to its path.
    fn build_server_url(&self) -> String {
        if !self.opts.external_url.is_empty()
            && let Ok(mut u) = ::yaml::url::parse(&self.opts.external_url)
        {
            u.path = path_join(&u.path, "/api/v1");
            u.raw_path.clear();
            return u.to_url_string();
        }
        "/api/v1".to_string()
    }

    /// Go `getAllPathDefinitions`.
    pub fn get_all_path_definitions(&self) -> Vec<(&'static str, PathItem)> {
        vec![
            // Query endpoints.
            ("/query", self.query_path()),
            ("/query_range", self.query_range_path()),
            ("/query_cost", self.query_cost_path()),
            ("/query_range_cost", self.query_range_cost_path()),
            ("/query_exemplars", self.query_exemplars_path()),
            ("/format_query", self.format_query_path()),
            ("/parse_query", self.parse_query_path()),
            // Label endpoints.
            ("/labels", self.labels_path()),
            ("/label/{name}/values", self.label_values_path()),
            ("/search/metric_names", self.search_metric_names_path()),
            ("/search/label_names", self.search_label_names_path()),
            ("/search/label_values", self.search_label_values_path()),
            // Series endpoints.
            ("/series", self.series_path()),
            // Metadata endpoints.
            ("/metadata", self.metadata_path()),
            // Target endpoints.
            ("/scrape_pools", self.scrape_pools_path()),
            ("/scrape_pools/config", self.scrape_pool_config_path()),
            ("/targets", self.targets_path()),
            ("/targets/metadata", self.targets_metadata_path()),
            ("/targets/relabel_steps", self.targets_relabel_steps_path()),
            // Rules and alerts endpoints.
            ("/rules", self.rules_path()),
            ("/alerts", self.alerts_path()),
            ("/alertmanagers", self.alertmanagers_path()),
            // Status endpoints.
            ("/status/config", self.status_config_path()),
            ("/status/runtimeinfo", self.status_runtime_info_path()),
            ("/status/buildinfo", self.status_build_info_path()),
            ("/status/flags", self.status_flags_path()),
            ("/status/tsdb", self.status_tsdb_path()),
            ("/status/tsdb/blocks", self.status_tsdb_blocks_path()),
            ("/status/walreplay", self.status_wal_replay_path()),
            ("/status/self_metrics", self.status_self_metrics_path()),
            // Admin endpoints.
            ("/admin/tsdb/delete_series", self.admin_delete_series_path()),
            (
                "/admin/tsdb/clean_tombstones",
                self.admin_clean_tombstones_path(),
            ),
            ("/admin/tsdb/snapshot", self.admin_snapshot_path()),
            // Remote endpoints.
            ("/read", self.remote_read_path()),
            ("/write", self.remote_write_path()),
            ("/otlp/v1/metrics", self.otlp_write_path()),
            // Notifications endpoints.
            ("/notifications", self.notifications_path()),
            ("/notifications/live", self.notifications_live_path()),
            // Features endpoint.
            ("/features", self.features_path()),
        ]
    }
}

/// Go `buildTags`. Tag summary is an OpenAPI 3.2 feature, left out of 3.1.
fn build_tags(version: &str) -> Node {
    const TAGS: [(&str, &str, &str); 13] = [
        ("query", "Query", "Query and evaluate PromQL expressions."),
        (
            "metadata",
            "Metadata",
            "Retrieve metric metadata such as type and unit.",
        ),
        ("labels", "Labels", "Query label names and values."),
        ("series", "Series", "Query and manage time series."),
        (
            "targets",
            "Targets",
            "Retrieve target and scrape pool information.",
        ),
        ("rules", "Rules", "Query recording and alerting rules."),
        (
            "alerts",
            "Alerts",
            "Query active alerts and alertmanager discovery.",
        ),
        (
            "status",
            "Status",
            "Retrieve server status and configuration.",
        ),
        (
            "admin",
            "Admin",
            "Administrative operations for TSDB management.",
        ),
        ("features", "Features", "Query enabled features."),
        (
            "remote",
            "Remote Storage",
            "Remote read and write endpoints.",
        ),
        ("otlp", "OTLP", "OpenTelemetry Protocol metrics ingestion."),
        (
            "notifications",
            "Notifications",
            "Server notifications and events.",
        ),
    ];
    Node::Seq(
        TAGS.iter()
            .map(|(name, summary, description)| {
                let mut m = MapBuilder::default();
                m.put("name", Node::from(*name));
                if version == OPENAPI_VERSION_32 {
                    m.put("summary", Node::from(*summary));
                }
                m.put("description", Node::from(*description));
                m.node()
            })
            .collect(),
    )
}

/// Go `path.Join(base, elem)` for a non-empty `elem`.
fn path_join(base: &str, elem: &str) -> String {
    let joined = if base.is_empty() {
        elem.to_string()
    } else {
        format!("{base}/{elem}")
    };
    path_clean(&joined)
}

/// Go `path.Clean`.
fn path_clean(p: &str) -> String {
    if p.is_empty() {
        return ".".to_string();
    }
    let rooted = p.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                if parts.last().is_some_and(|l| *l != "..") {
                    parts.pop();
                } else if !rooted {
                    parts.push("..");
                }
            }
            s => parts.push(s),
        }
    }
    let body = parts.join("/");
    match (rooted, body.is_empty()) {
        (true, _) => format!("/{body}"),
        (false, true) => ".".to_string(),
        (false, false) => body,
    }
}
