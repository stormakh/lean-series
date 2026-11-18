//! Port of discovery/hetzner/hcloud_test.go.
mod common;
mod hetzner_mock;

use common::ls;
use discovery::CancelToken;
use discovery::hetzner::{default_sd_config, new_hcloud_discovery};
use hetzner_mock::{HCLOUD_TEST_TOKEN, SdMock};

/// Go `hcloudSDTestSuite.SetupTest`.
fn setup_test() -> SdMock {
    let mock = SdMock::new();
    mock.handle_hcloud_servers();
    mock.handle_hcloud_networks();
    mock
}

#[test]
fn test_h_cloud_sd_refresh() {
    let mock = setup_test();
    let mut cfg = default_sd_config();
    cfg.http_client_config.bearer_token = yaml::Secret::new(HCLOUD_TEST_TOKEN);
    cfg.hcloud_endpoint = mock.endpoint();

    let mut d = new_hcloud_discovery(&cfg).unwrap();
    let target_groups = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, target_groups.len());

    let target_group = &target_groups[0];
    assert_eq!(3, target_group.targets.len(), "targetGroup.targets");

    let expected = [
        ls(&[
            ("__address__", "1.2.3.4:80"),
            ("__meta_hetzner_role", "hcloud"),
            ("__meta_hetzner_server_id", "42"),
            ("__meta_hetzner_server_name", "my-server"),
            ("__meta_hetzner_server_status", "running"),
            ("__meta_hetzner_public_ipv4", "1.2.3.4"),
            ("__meta_hetzner_public_ipv6_network", "2001:db8::/64"),
            ("__meta_hetzner_hcloud_image_name", "ubuntu-20.04"),
            ("__meta_hetzner_hcloud_image_description", "Ubuntu 20.04 Standard 64 bit"),
            ("__meta_hetzner_hcloud_image_os_flavor", "ubuntu"),
            ("__meta_hetzner_hcloud_image_os_version", "20.04"),
            ("__meta_hetzner_hcloud_location", "fsn1"),
            ("__meta_hetzner_hcloud_location_network_zone", "eu-central"),
            ("__meta_hetzner_hcloud_datacenter_location", "fsn1"),
            ("__meta_hetzner_hcloud_datacenter_location_network_zone", "eu-central"),
            ("__meta_hetzner_hcloud_cpu_cores", "1"),
            ("__meta_hetzner_hcloud_cpu_type", "shared"),
            ("__meta_hetzner_hcloud_memory_size_gb", "1"),
            ("__meta_hetzner_hcloud_disk_size_gb", "25"),
            ("__meta_hetzner_hcloud_server_type", "cx11"),
            ("__meta_hetzner_hcloud_private_ipv4_mynet", "10.0.0.2"),
            ("__meta_hetzner_hcloud_labelpresent_my_key", "true"),
            ("__meta_hetzner_hcloud_label_my_key", "my-value"),
        ]),
        ls(&[
            ("__address__", "1.2.3.5:80"),
            ("__meta_hetzner_role", "hcloud"),
            ("__meta_hetzner_server_id", "44"),
            ("__meta_hetzner_server_name", "another-server"),
            ("__meta_hetzner_server_status", "stopped"),
            ("__meta_hetzner_public_ipv4", "1.2.3.5"),
            ("__meta_hetzner_public_ipv6_network", "2001:db9::/64"),
            ("__meta_hetzner_hcloud_image_name", "ubuntu-20.04"),
            ("__meta_hetzner_hcloud_image_description", "Ubuntu 20.04 Standard 64 bit"),
            ("__meta_hetzner_hcloud_image_os_flavor", "ubuntu"),
            ("__meta_hetzner_hcloud_image_os_version", "20.04"),
            ("__meta_hetzner_hcloud_location", "fsn1"),
            ("__meta_hetzner_hcloud_location_network_zone", "eu-central"),
            ("__meta_hetzner_hcloud_datacenter_location", "fsn1"),
            ("__meta_hetzner_hcloud_datacenter_location_network_zone", "eu-central"),
            ("__meta_hetzner_hcloud_cpu_cores", "2"),
            ("__meta_hetzner_hcloud_cpu_type", "shared"),
            ("__meta_hetzner_hcloud_memory_size_gb", "1"),
            ("__meta_hetzner_hcloud_disk_size_gb", "50"),
            ("__meta_hetzner_hcloud_server_type", "cpx11"),
            ("__meta_hetzner_hcloud_labelpresent_key", "true"),
            ("__meta_hetzner_hcloud_labelpresent_other_key", "true"),
            ("__meta_hetzner_hcloud_label_key", ""),
            ("__meta_hetzner_hcloud_label_other_key", "value"),
        ]),
        ls(&[
            ("__address__", "1.2.3.6:80"),
            ("__meta_hetzner_role", "hcloud"),
            ("__meta_hetzner_server_id", "36"),
            ("__meta_hetzner_server_name", "deleted-image-server"),
            ("__meta_hetzner_server_status", "stopped"),
            ("__meta_hetzner_public_ipv4", "1.2.3.6"),
            ("__meta_hetzner_public_ipv6_network", "2001:db7::/64"),
            ("__meta_hetzner_hcloud_location", "fsn1"),
            ("__meta_hetzner_hcloud_location_network_zone", "eu-central"),
            ("__meta_hetzner_hcloud_datacenter_location", "fsn1"),
            ("__meta_hetzner_hcloud_datacenter_location_network_zone", "eu-central"),
            ("__meta_hetzner_hcloud_cpu_cores", "2"),
            ("__meta_hetzner_hcloud_cpu_type", "shared"),
            ("__meta_hetzner_hcloud_memory_size_gb", "1"),
            ("__meta_hetzner_hcloud_disk_size_gb", "50"),
            ("__meta_hetzner_hcloud_server_type", "cpx11"),
        ]),
    ];
    for (i, label_set) in expected.iter().enumerate() {
        assert_eq!(label_set, &target_group.targets[i], "item {i}");
    }
}
