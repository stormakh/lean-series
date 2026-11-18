//! Port of discovery/xds/client_test.go.
mod xds_common;

use std::time::Duration;

use discovery::CancelToken;
use discovery::xds::{DiscoveryRequest, DiscoveryResponse, HttpResourceClient, HttpResourceClientConfig, PROTOCOL_V3, ProtocolVersion, ResourceClient, make_xds_resource_http_endpoint_url};
use xds_common::{TestServer, create_test_http_server, sd_conf};

fn test_http_resource_config() -> HttpResourceClientConfig {
    let mut http_client_config = net::HttpClientConfig::default();
    http_client_config.tls_config.insecure_skip_verify = true;
    HttpResourceClientConfig {
        http_client_config,
        resource_type: "monitoring".to_string(),
        // Some known type.
        resource_type_url: "type.googleapis.com/envoy.service.discovery.v3.DiscoveryRequest".to_string(),
        server: "http://localhost".to_string(),
        client_id: "test-id".to_string(),
        ..Default::default()
    }
}

fn url_must_parse(s: &str) -> yaml::url::GoUrl {
    yaml::url::parse(s).unwrap()
}

#[test]
fn test_make_xds_resource_http_endpoint_empty_server_url_scheme() {
    let r = make_xds_resource_http_endpoint_url(PROTOCOL_V3, Some(url_must_parse("127.0.0.1")), "monitoring");
    assert_eq!(r.err().as_deref(), Some("invalid xDS server URL"));
}

#[test]
fn test_make_xds_resource_http_endpoint_empty_server_url_host() {
    let r = make_xds_resource_http_endpoint_url(PROTOCOL_V3, Some(url_must_parse("grpc://127.0.0.1")), "monitoring");
    let err = r.expect_err("expected an error");
    assert!(err.contains("must be either 'http' or 'https'"), "{err}");
}

#[test]
fn test_make_xds_resource_http_endpoint() {
    let u = make_xds_resource_http_endpoint_url(PROTOCOL_V3, Some(url_must_parse("http://127.0.0.1:5000")), "monitoring").unwrap();
    assert_eq!(u.to_url_string(), "http://127.0.0.1:5000/v3/discovery:monitoring");
}

#[test]
fn test_create_new_http_resource_client() {
    let c = HttpResourceClientConfig {
        http_client_config: sd_conf().http_client_config,
        name: "test".to_string(),
        extra_query_params: Some(vec![("param1".to_string(), "v1".to_string())]),
        timeout: Duration::from_secs(60),
        resource_type: "monitoring".to_string(),
        resource_type_url: "type.googleapis.com/envoy.service.discovery.v3.DiscoveryRequest".to_string(),
        server: "http://127.0.0.1:5000".to_string(),
        client_id: "client".to_string(),
    };
    let client = HttpResourceClient::new(c, PROTOCOL_V3).unwrap();
    assert_eq!(client.endpoint, "http://127.0.0.1:5000/v3/discovery:monitoring?param1=v1");
    assert_eq!(client.client.timeout, Some(Duration::from_secs(60)));
}

/// Go `createTestHTTPResourceClient`.
fn create_test_http_resource_client(
    mut conf: HttpResourceClientConfig,
    protocol_version: ProtocolVersion,
    responder: impl Fn(&DiscoveryRequest) -> Result<Option<DiscoveryResponse>, String> + Send + Sync + 'static,
) -> (HttpResourceClient, TestServer) {
    let (type_url, client_id) = (conf.resource_type_url.clone(), conf.client_id.clone());
    let s = create_test_http_server(move |request| {
        assert_eq!(type_url, request.type_url);
        assert_eq!(client_id, request.node.as_ref().map(|n| n.id.as_str()).unwrap_or(""));
        responder(request)
    });
    conf.server = s.url.clone();
    let client = HttpResourceClient::new(conf, protocol_version).unwrap();
    (client, s)
}

#[test]
fn test_http_resource_client_fetch_empty_response() {
    let (mut client, s) = create_test_http_resource_client(test_http_resource_config(), PROTOCOL_V3, |_| Ok(None));
    let res = client.fetch(&CancelToken::new());
    assert_eq!(res, Ok(None));
    s.close();
}

#[test]
fn test_http_resource_client_fetch_full_response() {
    let (mut client, s) = create_test_http_resource_client(test_http_resource_config(), PROTOCOL_V3, |request| {
        if request.version_info == "1" {
            return Ok(None);
        }
        Ok(Some(DiscoveryResponse { type_url: request.type_url.clone(), version_info: "1".to_string(), nonce: "abc".to_string(), resources: Vec::new(), ..Default::default() }))
    });

    let res = client.fetch(&CancelToken::new()).unwrap();
    let res = res.expect("response expected");
    assert_eq!(client.resource_type_url(), res.type_url);
    assert!(res.resources.is_empty());
    assert_eq!(client.latest_nonce, "abc", "Nonce not cached");
    assert_eq!(client.latest_version, "1", "Version not cached");

    let res = client.fetch(&CancelToken::new());
    assert_eq!(res, Ok(None), "Update not expected");
    s.close();
}

#[test]
fn test_http_resource_client_server_error() {
    let (mut client, s) = create_test_http_resource_client(test_http_resource_config(), PROTOCOL_V3, |_| Err("server error".to_string()));
    let res = client.fetch(&CancelToken::new());
    assert!(res.is_err(), "{res:?}");
    s.close();
}
