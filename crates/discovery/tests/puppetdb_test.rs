//! Port of discovery/puppetdb/puppetdb_test.go.
mod common;

use std::sync::Arc;

use common::{Req, Resp, TestServer, ls, new_options};
use discovery::puppetdb::{Discovery, SdConfig};
use discovery::{CancelToken, Group};

/// Go `mockServer`: serves `fixtures/<query>.json` for the query in the JSON request body.
fn mock_server() -> TestServer {
    TestServer::new(|r: &Req| {
        let Ok(v) = json::Json::parse_bytes(&r.body) else {
            return Resp::new(400, Vec::new());
        };
        let query = v.get("query").and_then(|q| q.as_str()).unwrap_or("").to_string();
        let p = common::upstream(&format!("discovery/puppetdb/fixtures/{query}.json"));
        match std::fs::read(&p) {
            Ok(b) => Resp::new(200, b).header("Content-Type", "application/json"),
            Err(_) => Resp::not_found(),
        }
    })
}

fn config(url: &str, query: &str, include_parameters: bool) -> SdConfig {
    SdConfig {
        http_client_config: net::HttpClientConfig::default_config(),
        url: url.to_string(),
        query: query.to_string(),
        port: 80,
        include_parameters,
        refresh_interval: yaml::Duration::from_secs(30),
    }
}

fn new_discovery(cfg: &SdConfig) -> Discovery {
    let (mut opts, _reg) = new_options(cfg);
    opts.set_name = "puppetdb".to_string();
    Discovery::new(cfg, opts).unwrap()
}

#[test]
fn test_puppet_slash_in_url() {
    let tests = [
        ("https://puppetserver", "https://puppetserver/pdb/query/v4"),
        ("https://puppetserver/", "https://puppetserver/pdb/query/v4"),
        ("http://puppetserver:8080/", "http://puppetserver:8080/pdb/query/v4"),
        ("http://puppetserver:8080", "http://puppetserver:8080/pdb/query/v4"),
    ];
    for (server_url, api_url) in tests {
        // This is not a valid PuppetDB query, but it is used by the mock.
        let cfg = config(server_url, "vhosts", false);
        let mut d = new_discovery(&cfg);
        assert_eq!(api_url, d.url());
    }
}

#[test]
fn test_puppet_db_refresh() {
    let ts = mock_server();
    let cfg = config(&ts.url, "vhosts", false);
    let mut d = new_discovery(&cfg);
    let tgs = d.refresh(&CancelToken::new()).unwrap();
    let expected = vec![Arc::new(Group {
        targets: vec![ls(&[
            ("__address__", "edinburgh.example.com:80"),
            ("__meta_puppetdb_query", "vhosts"),
            ("__meta_puppetdb_certname", "edinburgh.example.com"),
            ("__meta_puppetdb_environment", "prod"),
            ("__meta_puppetdb_exported", "false"),
            ("__meta_puppetdb_file", "/etc/puppetlabs/code/environments/prod/modules/upstream/apache/manifests/init.pp"),
            ("__meta_puppetdb_resource", "49af83866dc5a1518968b68e58a25319107afe11"),
            ("__meta_puppetdb_tags", ",roles::hypervisor,apache,apache::vhost,class,default-ssl,profile_hypervisor,vhost,profile_apache,hypervisor,__node_regexp__edinburgh,roles,node,"),
            ("__meta_puppetdb_title", "default-ssl"),
            ("__meta_puppetdb_type", "Apache::Vhost"),
        ])],
        labels: Default::default(),
        source: format!("{}/pdb/query/v4?query=vhosts", ts.url),
    })];
    assert_eq!(expected, tgs);
}

#[test]
fn test_puppet_db_refresh_with_parameters() {
    let ts = mock_server();
    let cfg = config(&ts.url, "vhosts", true);
    let mut d = new_discovery(&cfg);
    let tgs = d.refresh(&CancelToken::new()).unwrap();
    let expected = vec![Arc::new(Group {
        targets: vec![ls(&[
            ("__address__", "edinburgh.example.com:80"),
            ("__meta_puppetdb_query", "vhosts"),
            ("__meta_puppetdb_certname", "edinburgh.example.com"),
            ("__meta_puppetdb_environment", "prod"),
            ("__meta_puppetdb_exported", "false"),
            ("__meta_puppetdb_file", "/etc/puppetlabs/code/environments/prod/modules/upstream/apache/manifests/init.pp"),
            ("__meta_puppetdb_parameter_access_log", "true"),
            ("__meta_puppetdb_parameter_access_log_file", "ssl_access_log"),
            ("__meta_puppetdb_parameter_buckets", "0,2,5"),
            ("__meta_puppetdb_parameter_coordinates", "60.13464726551357,-2.0513768021728893"),
            ("__meta_puppetdb_parameter_docroot", "/var/www/html"),
            ("__meta_puppetdb_parameter_ensure", "absent"),
            ("__meta_puppetdb_parameter_labels_alias", "edinburgh"),
            ("__meta_puppetdb_parameter_options", "Indexes,FollowSymLinks,MultiViews"),
            ("__meta_puppetdb_parameter_pi", "3.141592653589793"),
            ("__meta_puppetdb_parameter_port", "22"),
            ("__meta_puppetdb_resource", "49af83866dc5a1518968b68e58a25319107afe11"),
            ("__meta_puppetdb_tags", ",roles::hypervisor,apache,apache::vhost,class,default-ssl,profile_hypervisor,vhost,profile_apache,hypervisor,__node_regexp__edinburgh,roles,node,"),
            ("__meta_puppetdb_title", "default-ssl"),
            ("__meta_puppetdb_type", "Apache::Vhost"),
        ])],
        labels: Default::default(),
        source: format!("{}/pdb/query/v4?query=vhosts", ts.url),
    })];
    assert_eq!(expected, tgs);
}

#[test]
fn test_puppet_db_invalid_code() {
    let ts = TestServer::new(|_: &Req| Resp::new(400, Vec::new()));
    let cfg = config(&ts.url, "", false);
    let mut d = new_discovery(&cfg);
    let err = d.refresh(&CancelToken::new()).unwrap_err();
    assert_eq!("server returned HTTP status 400 Bad Request", err);
}

#[test]
fn test_puppet_db_invalid_format() {
    // Go's ResponseWriter sniffs "text/plain; charset=utf-8" for "{}\n" (TestServer does the same).
    let ts = TestServer::new(|_: &Req| Resp::new(200, "{}\n"));
    let cfg = config(&ts.url, "", false);
    let mut d = new_discovery(&cfg);
    let err = d.refresh(&CancelToken::new()).unwrap_err();
    assert_eq!("unsupported content type text/plain; charset=utf-8", err);
}
