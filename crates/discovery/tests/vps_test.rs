//! Port of discovery/ovhcloud/vps_test.go.
mod common;

use common::{Req, Resp, TestServer, ls, read_upstream};
use discovery::CancelToken;
use discovery::ovhcloud::{SdConfig, new_refresher};
use discovery::refresh::Refresher;

const OVHCLOUD_APPLICATION_KEY_TEST: &str = "TDPKJdwZwAQPwKX2";
const OVHCLOUD_APPLICATION_SECRET_TEST: &str = "9ufkBmLaTQ9nz5yMUlg79taH0GNnzDjk";
const OVHCLOUD_CONSUMER_KEY_TEST: &str = "5mBuy6SUQcRw2ZUxg0cG68BoDKpED4KY";

/// Go `MockVpsAPI`.
fn mock_vps_api(r: &Req) -> Resp {
    if r.header("X-Ovh-Application") != OVHCLOUD_APPLICATION_KEY_TEST {
        return Resp::error(400, "bad application key");
    }
    let file = match r.path.as_str() {
        "/vps" => Some("vps.json"),
        "/vps/abc" => Some("vps_details.json"),
        "/vps/abc/ips" => Some("vps_abc_ips.json"),
        _ => None,
    };
    let body = file.map(|f| read_upstream(&format!("discovery/ovhcloud/testdata/vps/{f}"))).unwrap_or_default();
    Resp::json(body)
}

#[test]
fn test_ovh_cloud_vps_refresh() {
    let mock = TestServer::new(mock_vps_api);
    let cfg_string = format!(
        "\n---\nservice: vps\nendpoint: {}\napplication_key: {OVHCLOUD_APPLICATION_KEY_TEST}\napplication_secret: {OVHCLOUD_APPLICATION_SECRET_TEST}\nconsumer_key: {OVHCLOUD_CONSUMER_KEY_TEST}",
        mock.url
    );
    let cfg: SdConfig = yaml::unmarshal_strict(&cfg_string).unwrap();

    let mut d = new_refresher(&cfg, discovery::Logger::nop()).unwrap();
    let target_groups = d.refresh(&CancelToken::new()).unwrap();

    assert_eq!(target_groups.len(), 1);
    let target_group = &target_groups[0];
    assert_eq!(target_group.targets.len(), 1);
    for (i, lbls) in [ls(&[
        ("__address__", "192.0.2.1"),
        ("__meta_ovhcloud_vps_ipv4", "192.0.2.1"),
        ("__meta_ovhcloud_vps_ipv6", ""),
        ("__meta_ovhcloud_vps_cluster", "cluster_test"),
        ("__meta_ovhcloud_vps_datacenter", "[]"),
        ("__meta_ovhcloud_vps_disk", "40"),
        ("__meta_ovhcloud_vps_display_name", "abc"),
        ("__meta_ovhcloud_vps_maximum_additional_ip", "16"),
        ("__meta_ovhcloud_vps_memory", "2048"),
        ("__meta_ovhcloud_vps_memory_limit", "2048"),
        ("__meta_ovhcloud_vps_model_name", "vps-value-1-2-40"),
        ("__meta_ovhcloud_vps_name", "abc"),
        ("__meta_ovhcloud_vps_netboot_mode", "local"),
        ("__meta_ovhcloud_vps_offer", "VPS abc"),
        ("__meta_ovhcloud_vps_offer_type", "ssd"),
        ("__meta_ovhcloud_vps_state", "running"),
        ("__meta_ovhcloud_vps_vcore", "1"),
        ("__meta_ovhcloud_vps_model_vcore", "1"),
        ("__meta_ovhcloud_vps_version", "2019v1"),
        ("__meta_ovhcloud_vps_zone", "zone"),
        ("instance", "abc"),
    ])]
    .iter()
    .enumerate()
    {
        assert_eq!(&target_group.targets[i], lbls, "item {i}");
    }
}
