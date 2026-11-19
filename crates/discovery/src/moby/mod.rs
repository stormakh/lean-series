//! Port of `discovery/moby`: Docker (`docker_sd_configs`) and Docker Swarm (`dockerswarm_sd_configs`) service
//! discovery, with the subset of the Docker Engine API client they use (`client`, `types`).
//!
//! Differs from Go: a request in flight is not aborted when the discoverer is cancelled; it ends at the client
//! timeout (the refresh interval, as in Go).
use std::time::Duration;

use crate::cancel::CancelToken;

pub mod client;
pub mod docker;
pub mod dockerswarm;
mod network;
mod nodes;
mod services;
mod tasks;
pub mod types;

pub use docker::{DockerDiscovery, DockerMetrics, DockerSdConfig, default_docker_sd_config};
pub use dockerswarm::{Discovery, DockerSwarmSdConfig, DockerswarmMetrics, Filter, default_docker_swarm_sd_config};

/// The Docker client both discoverers build (Go: `client.New(WithHost, [WithHTTPClient, WithScheme,
/// WithHTTPHeaders], WithTimeout)` after `url.Parse(conf.Host)`). `what` is the name in the setup error
/// ("docker" or "docker swarm").
fn new_client(host: &str, http_client_config: &net::HttpClientConfig, refresh_interval: yaml::Duration, what: &str) -> Result<client::Client, String> {
    let host_url = yaml::url::parse(host)?;
    let mut opts =
        client::ClientOptions { host: host.to_string(), http_client: None, scheme: String::new(), http_headers: Vec::new(), timeout: Duration::ZERO };
    // There are other protocols than HTTP supported by the Docker daemon, like unix, which are not supported by
    // the HTTP client. Passing HTTP client options to the Docker client makes those non-HTTP requests fail.
    if host_url.scheme == "http" || host_url.scheme == "https" {
        let rt = net::new_round_tripper_from_config(http_client_config, &net::HttpClientOptions::default()).map_err(|e| e.to_string())?;
        opts.http_client = Some(net::Client::new(rt));
        opts.scheme = host_url.scheme.clone();
        opts.http_headers = vec![("User-Agent".to_string(), crate::http::USER_AGENT.to_string())];
    }
    // Go `WithTimeout(refresh_interval)` (a negative duration means no timeout, as Go's client ignores it).
    opts.timeout = if refresh_interval.0 > 0 { refresh_interval.to_std() } else { Duration::ZERO };
    client::Client::new(opts).map_err(|e| format!("error setting up {what} client: {e}"))
}

/// Go `client.ContainerList(ctx, ContainerListOptions{Filters: f})`.
fn container_list(c: &mut client::Client, ctx: &CancelToken, filters: &client::Filters) -> Result<Vec<types::ContainerSummary>, String> {
    let r = c.get(ctx, "/containers/json", &query(filters))?;
    types::container_list(&types::parse_body(&r.body)?)
}

/// Go `client.NetworkList(ctx, NetworkListOptions{})`.
fn network_list(c: &mut client::Client, ctx: &CancelToken) -> Result<Vec<types::NetworkSummary>, String> {
    let r = c.get(ctx, "/networks", &[])?;
    types::network_list(&types::parse_body(&r.body)?)
}

/// Go `client.NodeList`.
fn node_list(c: &mut client::Client, ctx: &CancelToken, filters: &client::Filters) -> Result<Vec<types::Node>, String> {
    let r = c.get(ctx, "/nodes", &query(filters))?;
    types::node_list(&types::parse_body(&r.body)?)
}

/// Go `client.ServiceList`.
fn service_list(c: &mut client::Client, ctx: &CancelToken, filters: &client::Filters) -> Result<Vec<types::Service>, String> {
    let r = c.get(ctx, "/services", &query(filters))?;
    types::service_list(&types::parse_body(&r.body)?)
}

/// Go `client.TaskList`.
fn task_list(c: &mut client::Client, ctx: &CancelToken, filters: &client::Filters) -> Result<Vec<types::Task>, String> {
    let r = c.get(ctx, "/tasks", &query(filters))?;
    types::task_list(&types::parse_body(&r.body)?)
}

fn query(filters: &client::Filters) -> Vec<(String, String)> {
    let mut q = Vec::new();
    filters.update_url_values(&mut q);
    q
}
