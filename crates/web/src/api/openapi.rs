//! Port of `web/api/v1/openapi*.go`: the OpenAPI document of the API. (Stub: being ported.)

use http::{Request, Response};

/// Go `OpenAPIOptions`.
#[derive(Clone, Debug, Default)]
pub struct OpenApiOptions {}

/// Go `OpenAPIBuilder`.
pub struct OpenApiBuilder {
    pub opts: OpenApiOptions,
}

impl OpenApiBuilder {
    /// Go `NewOpenAPIBuilder`.
    pub fn new(opts: OpenApiOptions) -> OpenApiBuilder {
        OpenApiBuilder { opts }
    }

    /// Go `ServeOpenAPI`.
    pub fn serve_openapi(&self, _req: &Request) -> Response {
        crate::route::not_found()
    }
}
