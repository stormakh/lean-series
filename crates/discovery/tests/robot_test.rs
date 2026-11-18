//! Port of discovery/hetzner/robot_test.go.
mod common;
mod hetzner_mock;

use common::ls;
use discovery::CancelToken;
use discovery::hetzner::{default_sd_config, new_robot_discovery};
use hetzner_mock::{ROBOT_TEST_PASSWORD, ROBOT_TEST_USERNAME, SdMock};

/// Go `robotSDTestSuite.SetupTest`.
fn setup_test() -> SdMock {
    let mock = SdMock::new();
    mock.handle_robot_servers();
    mock
}

#[test]
fn test_robot_sd_refresh() {
    let mock = setup_test();
    let mut cfg = default_sd_config();
    cfg.http_client_config.basic_auth = Some(net::BasicAuth { username: ROBOT_TEST_USERNAME.to_string(), password: yaml::Secret::new(ROBOT_TEST_PASSWORD), ..Default::default() });
    cfg.robot_endpoint = mock.endpoint();

    let mut d = new_robot_discovery(&cfg).unwrap();
    let target_groups = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, target_groups.len());

    let target_group = &target_groups[0];
    assert_eq!(2, target_group.targets.len(), "targetGroup.targets");

    let expected = [
        ls(&[
            ("__address__", "123.123.123.123:80"),
            ("__meta_hetzner_role", "robot"),
            ("__meta_hetzner_server_id", "321"),
            ("__meta_hetzner_server_name", "server1"),
            ("__meta_hetzner_server_status", "ready"),
            ("__meta_hetzner_public_ipv4", "123.123.123.123"),
            ("__meta_hetzner_public_ipv6_network", "2a01:4f8:111:4221::/64"),
            ("__meta_hetzner_datacenter", "nbg1-dc1"),
            ("__meta_hetzner_robot_datacenter", "nbg1-dc1"),
            ("__meta_hetzner_robot_product", "DS 3000"),
            ("__meta_hetzner_robot_cancelled", "false"),
        ]),
        ls(&[
            ("__address__", "123.123.123.124:80"),
            ("__meta_hetzner_role", "robot"),
            ("__meta_hetzner_server_id", "421"),
            ("__meta_hetzner_server_name", "server2"),
            ("__meta_hetzner_server_status", "in process"),
            ("__meta_hetzner_public_ipv4", "123.123.123.124"),
            ("__meta_hetzner_datacenter", "fsn1-dc10"),
            ("__meta_hetzner_robot_datacenter", "fsn1-dc10"),
            ("__meta_hetzner_robot_product", "X5"),
            ("__meta_hetzner_robot_cancelled", "true"),
        ]),
    ];
    for (i, label_set) in expected.iter().enumerate() {
        assert_eq!(label_set, &target_group.targets[i], "item {i}");
    }
}

#[test]
fn test_robot_sd_refresh_handle_error() {
    let mock = setup_test();
    let mut cfg = default_sd_config();
    cfg.robot_endpoint = mock.endpoint();

    let mut d = new_robot_discovery(&cfg).unwrap();
    let (target_groups, err) = match d.refresh(&CancelToken::new()) {
        Ok(t) => (t, None),
        Err(e) => (Vec::new(), Some(e)),
    };
    assert_eq!(Some("non 2xx status '401' response during hetzner service discovery with role robot".to_string()), err);
    assert!(target_groups.is_empty());
}
