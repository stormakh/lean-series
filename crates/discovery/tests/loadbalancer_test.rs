//! Port of discovery/openstack/loadbalancer_test.go.
mod cloud_mock;
mod openstack_mock;

use discovery::openstack::{Role, SdConfig, new_refresher};
use discovery::refresh::Refresher;
use discovery::{CancelToken, Logger};
use model::labelset::LabelSet;
use openstack_mock::SdMock;

/// Go `OpenstackSDLoadBalancerTestSuite.SetupTest`.
fn setup() -> SdMock {
    let m = SdMock::new();
    m.handle_load_balancer_list_successfully();
    m.handle_listeners_list_successfully();
    m.handle_floating_ip_list_successfully();
    m.handle_versions_successfully();
    m.handle_auth_successfully();
    m
}

/// Go `openstackAuthSuccess`.
fn openstack_auth_success(m: &SdMock) -> Result<Box<dyn Refresher>, String> {
    let conf = SdConfig {
        identity_endpoint: m.endpoint(),
        password: "test".into(),
        username: "test".to_string(),
        domain_name: "12345".to_string(),
        region: "RegionOne".to_string(),
        role: Role::new("loadbalancer"),
        ..Default::default()
    };
    new_refresher(&conf, Logger::nop())
}

#[test]
fn test_openstack_sd_load_balancer_refresh() {
    let mock = setup();
    let mut instance = openstack_auth_success(&mock).unwrap();
    let tgs = instance.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());
    let tg = &tgs[0];
    assert_eq!(4, tg.targets.len());

    let expected = [
        LabelSet::from_pairs(&[
            ("__address__", "10.0.0.32:9273"),
            ("__meta_openstack_loadbalancer_id", "ef079b0c-e610-4dfb-b1aa-b49f07ac48e5"),
            ("__meta_openstack_loadbalancer_name", "lb1"),
            ("__meta_openstack_loadbalancer_operating_status", "ONLINE"),
            ("__meta_openstack_loadbalancer_provisioning_status", "ACTIVE"),
            ("__meta_openstack_loadbalancer_availability_zone", "az1"),
            ("__meta_openstack_loadbalancer_floating_ip", "192.168.1.2"),
            ("__meta_openstack_loadbalancer_vip", "10.0.0.32"),
            ("__meta_openstack_loadbalancer_provider", "amphora"),
            ("__meta_openstack_loadbalancer_tags", "tag1,tag2"),
            ("__meta_openstack_project_id", "fcad67a6189847c4aecfa3c81a05783b"),
        ]),
        LabelSet::from_pairs(&[
            ("__address__", "10.0.2.78:8080"),
            ("__meta_openstack_loadbalancer_id", "d92c471e-8d3e-4b9f-b2b5-9c72a9e3ef54"),
            ("__meta_openstack_loadbalancer_name", "lb3"),
            ("__meta_openstack_loadbalancer_operating_status", "ONLINE"),
            ("__meta_openstack_loadbalancer_provisioning_status", "ACTIVE"),
            ("__meta_openstack_loadbalancer_availability_zone", "az3"),
            ("__meta_openstack_loadbalancer_floating_ip", "192.168.3.4"),
            ("__meta_openstack_loadbalancer_vip", "10.0.2.78"),
            ("__meta_openstack_loadbalancer_provider", "amphora"),
            ("__meta_openstack_loadbalancer_tags", "tag5,tag6"),
            ("__meta_openstack_project_id", "ac57f03dba1a4fdebff3e67201bc7a85"),
        ]),
        LabelSet::from_pairs(&[
            ("__address__", "10.0.3.99:9090"),
            ("__meta_openstack_loadbalancer_id", "f5c7e918-df38-4a5a-a7d4-d9c27ab2cf67"),
            ("__meta_openstack_loadbalancer_name", "lb4"),
            ("__meta_openstack_loadbalancer_operating_status", "ONLINE"),
            ("__meta_openstack_loadbalancer_provisioning_status", "ACTIVE"),
            ("__meta_openstack_loadbalancer_availability_zone", "az1"),
            ("__meta_openstack_loadbalancer_floating_ip", "192.168.4.5"),
            ("__meta_openstack_loadbalancer_vip", "10.0.3.99"),
            ("__meta_openstack_loadbalancer_provider", "amphora"),
            ("__meta_openstack_project_id", "fa8c372dfe4d4c92b0c4e3a2d9b3c9fa"),
        ]),
        LabelSet::from_pairs(&[
            ("__address__", "10.0.4.88:9876"),
            ("__meta_openstack_loadbalancer_id", "e83a6d92-7a3e-4567-94b3-20c83b32a75e"),
            ("__meta_openstack_loadbalancer_name", "lb5"),
            ("__meta_openstack_loadbalancer_operating_status", "ONLINE"),
            ("__meta_openstack_loadbalancer_provisioning_status", "ACTIVE"),
            ("__meta_openstack_loadbalancer_availability_zone", "az4"),
            ("__meta_openstack_loadbalancer_vip", "10.0.4.88"),
            ("__meta_openstack_loadbalancer_provider", "amphora"),
            ("__meta_openstack_project_id", "a5d3b2e1e6f34cd9a5f7c2f01a6b8e29"),
        ]),
    ];
    for (i, lbls) in expected.iter().enumerate() {
        assert_eq!(lbls, &tg.targets[i], "item {i}");
    }
}

#[test]
fn test_openstack_sd_load_balancer_refresh_with_done_context() {
    let mock = setup();
    let mut loadbalancer = openstack_auth_success(&mock).unwrap();
    let ctx = CancelToken::new();
    ctx.cancel();
    let err = loadbalancer.refresh(&ctx).unwrap_err();
    assert!(err.contains("context canceled"), "{err:?} doesn't contain \"context canceled\"");
}
