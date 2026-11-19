//! Port of discovery/ovhcloud/dedicated_server_test.go.
mod common;

use common::{Req, Resp, TestServer, ls, read_upstream};
use discovery::CancelToken;
use discovery::ovhcloud::{SdConfig, new_refresher};
use discovery::refresh::Refresher;

const OVHCLOUD_APPLICATION_KEY_TEST: &str = "TDPKJdwZwAQPwKX2";
const OVHCLOUD_APPLICATION_SECRET_TEST: &str = "9ufkBmLaTQ9nz5yMUlg79taH0GNnzDjk";
const OVHCLOUD_CONSUMER_KEY_TEST: &str = "5mBuy6SUQcRw2ZUxg0cG68BoDKpED4KY";

/// Go `MockDedicatedAPI`.
fn mock_dedicated_api(r: &Req) -> Resp {
    if r.header("X-Ovh-Application") != OVHCLOUD_APPLICATION_KEY_TEST {
        return Resp::error(400, "bad application key");
    }
    let file = match r.path.as_str() {
        "/dedicated/server" => Some("dedicated_servers.json"),
        "/dedicated/server/abcde" => Some("dedicated_servers_details.json"),
        "/dedicated/server/abcde/ips" => Some("dedicated_servers_abcde_ips.json"),
        _ => None,
    };
    let body = file.map(|f| read_upstream(&format!("discovery/ovhcloud/testdata/dedicated_server/{f}"))).unwrap_or_default();
    Resp::json(body)
}

#[test]
fn test_ovhcloud_dedicated_server_refresh() {
    let mock = TestServer::new(mock_dedicated_api);
    let cfg_string = format!(
        "\n---\nservice: dedicated_server\nendpoint: {}\napplication_key: {OVHCLOUD_APPLICATION_KEY_TEST}\napplication_secret: {OVHCLOUD_APPLICATION_SECRET_TEST}\nconsumer_key: {OVHCLOUD_CONSUMER_KEY_TEST}",
        mock.url
    );
    let cfg: SdConfig = yaml::unmarshal_strict(&cfg_string).unwrap();
    let mut d = new_refresher(&cfg, discovery::Logger::nop()).unwrap();
    let target_groups = d.refresh(&CancelToken::new()).unwrap();

    assert_eq!(target_groups.len(), 1);
    let target_group = &target_groups[0];
    assert_eq!(target_group.targets.len(), 1);

    for (i, lbls) in [ls(&[
        ("__address__", "1.2.3.4"),
        ("__meta_ovhcloud_dedicated_server_commercial_range", "Advance-1 Gen 2"),
        ("__meta_ovhcloud_dedicated_server_datacenter", "gra3"),
        ("__meta_ovhcloud_dedicated_server_ipv4", "1.2.3.4"),
        ("__meta_ovhcloud_dedicated_server_ipv6", ""),
        ("__meta_ovhcloud_dedicated_server_link_speed", "123"),
        ("__meta_ovhcloud_dedicated_server_name", "abcde"),
        ("__meta_ovhcloud_dedicated_server_no_intervention", "false"),
        ("__meta_ovhcloud_dedicated_server_os", "debian11_64"),
        ("__meta_ovhcloud_dedicated_server_rack", "TESTRACK"),
        ("__meta_ovhcloud_dedicated_server_reverse", "abcde-rev"),
        ("__meta_ovhcloud_dedicated_server_server_id", "1234"),
        ("__meta_ovhcloud_dedicated_server_state", "test"),
        ("__meta_ovhcloud_dedicated_server_support_level", "pro"),
        ("instance", "abcde"),
    ])]
    .iter()
    .enumerate()
    {
        assert_eq!(&target_group.targets[i], lbls, "item {i}");
    }
}
