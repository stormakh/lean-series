//! Port of `web/api/v1/search.go`: the streaming search endpoints (NDJSON). (Stub: being ported.)

use http::{Request, Response};

use super::Api;

/// Go `searchMetadataCache`.
#[derive(Default)]
pub struct SearchMetadataCache {}

pub fn search_metric_names(_api: &Api, _req: Request) -> Response {
    crate::route::not_found()
}

pub fn search_label_names(_api: &Api, _req: Request) -> Response {
    crate::route::not_found()
}

pub fn search_label_values(_api: &Api, _req: Request) -> Response {
    crate::route::not_found()
}
