//! Port of `web/api/testhelpers/request.go`: request builders that return a [`Response`] for chained
//! assertions.

use http::{Body, Request, VecStreamWriter};
use json::Json;

use super::ApiWrapper;

/// Go `Response`: an HTTP response with its body parsed as JSON (when it is JSON).
pub struct Response {
    pub status_code: u16,
    pub body: String,
    pub json: Option<Json>,
    pub method: String,
    /// The request path (without the query string).
    pub path: String,
    /// The raw query string of the request.
    pub query: String,
    pub request_headers: Vec<(String, String)>,
    pub request_body: Vec<u8>,
    pub response_headers: Vec<(String, String)>,
}

/// Go `url.Values.Encode`: sorted by key, keys and values query-escaped.
fn encode(params: &[&str]) -> String {
    assert!(params.len() % 2 == 0, "params must be key-value pairs");
    let mut pairs: Vec<(&str, &str)> = params.chunks(2).map(|p| (p[0], p[1])).collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    pairs
        .iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                yaml::url::query_escape(k),
                yaml::url::query_escape(v)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// Go `GET`: query parameters are key-value pairs.
pub fn get(api: &ApiWrapper, path: &str, query_params: &[&str]) -> Response {
    let req = Request {
        method: "GET".to_string(),
        path: path.to_string(),
        query: encode(query_params),
        ..Default::default()
    };
    execute_request(api, req)
}

/// Go `POST`: body parameters are key-value pairs sent as a form.
pub fn post(api: &ApiWrapper, path: &str, body_params: &[&str]) -> Response {
    let req = Request {
        method: "POST".to_string(),
        path: path.to_string(),
        headers: vec![(
            "Content-Type".to_string(),
            "application/x-www-form-urlencoded".to_string(),
        )],
        body: encode(body_params).into_bytes(),
        ..Default::default()
    };
    execute_request(api, req)
}

fn header<'a>(h: &'a [(String, String)], name: &str) -> &'a str {
    h.iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
        .unwrap_or("")
}

/// Go `executeRequest`.
fn execute_request(api: &ApiWrapper, req: Request) -> Response {
    let (method, path, query, request_headers, request_body) = (
        req.method.clone(),
        req.path.clone(),
        req.query.clone(),
        req.headers.clone(),
        req.body.clone(),
    );
    let resp = api.handler.serve(req);
    let body = match resp.body {
        Body::Bytes(b) => b,
        Body::Stream(f) => {
            let mut w = VecStreamWriter(Vec::new());
            f(&mut w).expect("streaming the response body");
            w.0
        }
        Body::File(..) => panic!("unexpected file body"),
    };
    let body = String::from_utf8(body).expect("response body is UTF-8");
    let json = if header(&resp.headers, "Content-Type").contains("application/json") {
        // If JSON parsing fails, leave JSON as None (tests can handle non-JSON responses).
        match Json::parse(&body) {
            Ok(j @ Json::Obj(_)) => Some(j),
            _ => None,
        }
    } else {
        None
    };
    Response {
        status_code: resp.status,
        body,
        json,
        method,
        path,
        query,
        request_headers,
        request_body,
        response_headers: resp.headers,
    }
}

impl Response {
    pub fn response_header(&self, name: &str) -> &str {
        header(&self.response_headers, name)
    }
    pub fn request_header(&self, name: &str) -> &str {
        header(&self.request_headers, name)
    }
}
