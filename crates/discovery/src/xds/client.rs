//! Port of `discovery/xds/client.go`: the xDS REST-JSON polling client.
//!
//! Differs from Go: a request in flight is not aborted when the discoverer is cancelled; it ends at the client
//! timeout (fetch_timeout + 15s).
use std::time::Duration;

use net::HttpClientConfig;
use yaml::url::GoUrl;

use super::pb::{DiscoveryRequest, DiscoveryResponse, Message, Node};
use super::{PROTOCOL_V3, ProtocolVersion, ResourceClient};
use crate::cancel::CancelToken;

/// Go `version.PrometheusUserAgent()`.
pub const USER_AGENT: &str = "Prometheus/3.16.0";

/// Go `HTTPResourceClientConfig`.
#[derive(Clone, Debug, Default)]
pub struct HttpResourceClientConfig {
    pub http_client_config: HttpClientConfig,
    pub name: String,
    /// Extra query parameters of the request URL (Go `url.Values`; `None` is a nil map, which keeps the query of
    /// the server URL).
    pub extra_query_params: Option<Vec<(String, String)>>,
    /// The timeout of one fetch request (zero: none).
    pub timeout: Duration,
    /// The xDS resource type, e.g. "monitoringassignments", used in the request path.
    pub resource_type: String,
    /// The type URL of the resource, e.g. "type.googleapis.com/kuma.observability.v1.MonitoringAssignment".
    pub resource_type_url: String,
    /// The xDS management server.
    pub server: String,
    /// Identifies the client to the management server.
    pub client_id: String,
}

/// Go `HTTPResourceClient`.
pub struct HttpResourceClient {
    pub client: net::Client,
    pub config: HttpResourceClientConfig,
    /// The full xDS HTTP endpoint.
    pub endpoint: String,
    pub latest_version: String,
    pub latest_nonce: String,
}

impl HttpResourceClient {
    /// Go `NewHTTPResourceClient`.
    pub fn new(conf: HttpResourceClientConfig, protocol_version: ProtocolVersion) -> Result<HttpResourceClient, String> {
        if protocol_version != PROTOCOL_V3 {
            return Err("only the v3 protocol is supported".to_string());
        }
        if conf.server.is_empty() {
            return Err("empty xDS server".to_string());
        }
        let server_url = yaml::url::parse(&conf.server)?;
        let mut endpoint_url = make_xds_resource_http_endpoint_url(protocol_version, Some(server_url), &conf.resource_type)?;
        if let Some(q) = &conf.extra_query_params {
            endpoint_url.raw_query = yaml::url::encode_query(q);
        }
        let opts = net::HttpClientOptions::default().with_idle_conn_timeout(conf.timeout);
        let mut client = net::new_client_from_config(&conf.http_client_config, &opts).map_err(|e| e.to_string())?;
        // Go: a zero client timeout means no timeout.
        client.timeout = if conf.timeout.is_zero() { None } else { Some(conf.timeout) };
        Ok(HttpResourceClient { client, endpoint: endpoint_url.to_url_string(), config: conf, latest_version: String::new(), latest_nonce: String::new() })
    }
}

/// Go `makeXDSResourceHTTPEndpointURL`.
pub fn make_xds_resource_http_endpoint_url(protocol_version: ProtocolVersion, server_url: Option<GoUrl>, resource_type: &str) -> Result<GoUrl, String> {
    let Some(mut u) = server_url else {
        return Err("empty xDS server URL".to_string());
    };
    if u.scheme.is_empty() || u.host.is_empty() {
        return Err("invalid xDS server URL".to_string());
    }
    if u.scheme != "http" && u.scheme != "https" {
        return Err("invalid xDS server URL protocol. must be either 'http' or 'https'".to_string());
    }
    u.path = path_join(&[&u.path, protocol_version.0, &format!("discovery:{resource_type}")]);
    Ok(u)
}

/// Go `path.Join`.
fn path_join(elems: &[&str]) -> String {
    let parts: Vec<&str> = elems.iter().copied().filter(|e| !e.is_empty()).collect();
    if parts.is_empty() {
        return String::new();
    }
    net::config::clean_path(&parts.join("/"))
}

impl ResourceClient for HttpResourceClient {
    fn resource_type_url(&self) -> &str {
        &self.config.resource_type_url
    }
    fn server(&self) -> &str {
        &self.config.server
    }
    fn id(&self) -> &str {
        &self.config.client_id
    }
    fn close(&mut self) {
        self.client.transport.close_idle_connections();
    }

    /// Go `Fetch`: the latest state of the resources, or `None` when the local version is current.
    fn fetch(&mut self, _ctx: &CancelToken) -> Result<Option<DiscoveryResponse>, String> {
        let req = DiscoveryRequest {
            version_info: self.latest_version.clone(),
            response_nonce: self.latest_nonce.clone(),
            type_url: self.config.resource_type_url.clone(),
            resource_names: Vec::new(),
            node: Some(Node { id: self.config.client_id.clone() }),
        };
        let body = req.to_proto_json()?;
        let mut request = net::Request::new("POST", &self.endpoint, body.into_bytes()).map_err(|e| e.to_string())?;
        request.header.add("User-Agent", USER_AGENT);
        request.header.add("Content-Type", "application/json");
        request.header.add("Accept", "application/json");
        let resp = self.client.do_request(request).map_err(|e| e.to_string())?;
        if resp.status == 304 {
            // Empty response, already have the latest.
            return Ok(None);
        }
        if resp.status != 200 {
            return Err(format!("non 200 status '{}' response during xDS fetch", resp.status));
        }
        let res = DiscoveryResponse::from_proto_json(&resp.body)?;
        // Cache the latest nonce + version info.
        self.latest_nonce = res.nonce.clone();
        self.latest_version = res.version_info.clone();
        Ok(Some(res))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
