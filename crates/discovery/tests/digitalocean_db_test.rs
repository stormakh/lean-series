//! Port of discovery/digitalocean/digitalocean_db_test.go.
mod common;
mod digitalocean_mock;

use common::{Req, Resp, TestServer, ls};
use digitalocean_mock::{SdMock, go_raw_strings};
use discovery::CancelToken;
use discovery::digitalocean::{
    DATABASES_ROLE, GoHttpClient, Role, default_sd_config, new_refresher,
};
use discovery::refresh::Refresher;

#[test]
fn test_digital_ocean_db_refresh() {
    let mut mock = SdMock::new();
    mock.handle_databases_list();

    let mut cfg = default_sd_config();
    cfg.role = Role(DATABASES_ROLE.to_string());
    cfg.port = 9273;
    // Go `mock.Server.Client()`: a plain HTTP client.
    cfg.http_client = Some(GoHttpClient::default());

    let mut d = new_refresher(&cfg).unwrap();

    // Inject the mock URL into the godo client since we can't easily change it via HTTPClient.
    d.client().base_url = yaml::url::parse(&mock.endpoint()).unwrap();

    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());

    let tg = &tgs[0];
    assert_eq!(2, tg.targets.len());

    let expected_targets = [
        ls(&[
            (
                "__address__",
                "do-fra1-pg-trading-001-do-user-XXXXX-0.f.db.ondigitalocean.com:9273",
            ),
            (
                "__meta_digitalocean_db_id",
                "9cc10173-e9ea-4176-9dbc-a4cee4c4ff30",
            ),
            ("__meta_digitalocean_db_name", "do-fra1-pg-trading-001"),
            ("__meta_digitalocean_db_engine", "pg"),
            ("__meta_digitalocean_db_version", "16"),
            ("__meta_digitalocean_db_status", "online"),
            ("__meta_digitalocean_db_region", "fra1"),
            ("__meta_digitalocean_db_size", "db-s-1vcpu-2gb"),
            ("__meta_digitalocean_db_num_nodes", "1"),
            (
                "__meta_digitalocean_db_host",
                "do-fra1-pg-trading-001-do-user-XXXXX-0.f.db.ondigitalocean.com",
            ),
            ("__meta_digitalocean_db_tag_prod", "true"),
            ("__meta_digitalocean_db_tag_market", "true"),
        ]),
        ls(&[
            ("__address__", "private-db-host:9273"),
            (
                "__meta_digitalocean_db_id",
                "a0b1c2d3-e4f5-4677-8899-001122334455",
            ),
            ("__meta_digitalocean_db_name", "private-db"),
            ("__meta_digitalocean_db_engine", "mysql"),
            ("__meta_digitalocean_db_version", "8"),
            ("__meta_digitalocean_db_status", "online"),
            ("__meta_digitalocean_db_region", "nyc1"),
            ("__meta_digitalocean_db_size", "db-s-2vcpu-4gb"),
            ("__meta_digitalocean_db_num_nodes", "2"),
            ("__meta_digitalocean_db_host", "public-db-host"),
            ("__meta_digitalocean_db_private_host", "private-db-host"),
        ]),
    ];

    for (i, expected) in expected_targets.iter().enumerate() {
        assert_eq!(expected, &tg.targets[i]);
    }
    mock.shutdown_server();
}

#[test]
fn test_digital_ocean_db_refresh_pagination() {
    // The handler bodies are the raw strings of the Go test file.
    let bodies = go_raw_strings("discovery/digitalocean/digitalocean_db_test.go");
    let ts = TestServer::new(move |r: &Req| match r.query_get("page") {
        "1" | "" => Resp::new(200, bodies[0].clone()),
        "2" => Resp::new(200, bodies[1].clone()),
        _ => Resp::new(200, bodies[2].clone()),
    });

    let mut cfg = default_sd_config();
    cfg.role = Role(DATABASES_ROLE.to_string());
    cfg.http_client = Some(GoHttpClient::default());

    let mut d = new_refresher(&cfg).unwrap();
    d.client().base_url = yaml::url::parse(&ts.url).unwrap();

    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());
    assert_eq!(2, tgs[0].targets.len());
}
