//! Port of discovery/openstack/instance_test.go.
use super::{cloud_mock, openstack_mock};

use discovery::openstack::{Role, SdConfig, new_refresher};
use discovery::refresh::Refresher;
use discovery::{CancelToken, Logger};
use model::labelset::LabelSet;
use openstack_mock::SdMock;

/// Go `OpenstackSDInstanceTestSuite.SetupTest`.
fn setup() -> SdMock {
    let m = SdMock::new();
    m.handle_server_list_successfully();
    m.handle_floating_ip_list_successfully();
    m.handle_ports_list_successfully();
    m.handle_versions_successfully();
    m.handle_auth_successfully();
    m
}

/// Go `OpenstackSDInstanceTestSuite.openstackAuthSuccess`.
fn openstack_auth_success(m: &SdMock) -> Result<Box<dyn Refresher>, String> {
    let conf = SdConfig {
        identity_endpoint: m.endpoint(),
        password: "test".into(),
        username: "test".to_string(),
        domain_name: "12345".to_string(),
        region: "RegionOne".to_string(),
        role: Role::new("instance"),
        all_tenants: true,
        ..Default::default()
    };
    new_refresher(&conf, Logger::nop())
}

#[test]
fn test_openstack_sd_instance_refresh() {
    let mock = setup();
    let mut instance = openstack_auth_success(&mock).unwrap();
    let tgs = instance.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());
    let tg = &tgs[0];
    assert_eq!(6, tg.targets.len());

    let expected = [
        LabelSet::from_pairs(&[
            ("__address__", "10.0.0.32:0"),
            ("__meta_openstack_instance_flavor", "1"),
            ("__meta_openstack_instance_id", "ef079b0c-e610-4dfb-b1aa-b49f07ac48e5"),
            ("__meta_openstack_instance_image", "f90f6034-2570-4974-8351-6b49732ef2eb"),
            ("__meta_openstack_instance_status", "ACTIVE"),
            ("__meta_openstack_instance_name", "herp"),
            ("__meta_openstack_private_ip", "10.0.0.32"),
            ("__meta_openstack_public_ip", "10.10.10.2"),
            ("__meta_openstack_address_pool", "private"),
            ("__meta_openstack_project_id", "fcad67a6189847c4aecfa3c81a05783b"),
            ("__meta_openstack_user_id", "9349aff8be7545ac9d2f1d00999a23cd"),
        ]),
        LabelSet::from_pairs(&[
            ("__address__", "10.0.0.31:0"),
            ("__meta_openstack_instance_flavor", "m1.medium"),
            ("__meta_openstack_instance_id", "9e5476bd-a4ec-4653-93d6-72c93aa682ba"),
            ("__meta_openstack_instance_image", "f90f6034-2570-4974-8351-6b49732ef2eb"),
            ("__meta_openstack_instance_status", "ACTIVE"),
            ("__meta_openstack_instance_name", "derp"),
            ("__meta_openstack_private_ip", "10.0.0.31"),
            ("__meta_openstack_address_pool", "private"),
            ("__meta_openstack_project_id", "fcad67a6189847c4aecfa3c81a05783b"),
            ("__meta_openstack_user_id", "9349aff8be7545ac9d2f1d00999a23cd"),
        ]),
        LabelSet::from_pairs(&[
            ("__address__", "10.0.0.33:0"),
            ("__meta_openstack_instance_flavor", "m1.small"),
            ("__meta_openstack_instance_id", "9e5476bd-a4ec-4653-93d6-72c93aa682bb"),
            ("__meta_openstack_instance_status", "ACTIVE"),
            ("__meta_openstack_instance_name", "merp"),
            ("__meta_openstack_private_ip", "10.0.0.33"),
            ("__meta_openstack_address_pool", "private"),
            ("__meta_openstack_tag_env", "prod"),
            ("__meta_openstack_project_id", "fcad67a6189847c4aecfa3c81a05783b"),
            ("__meta_openstack_user_id", "9349aff8be7545ac9d2f1d00999a23cd"),
        ]),
        LabelSet::from_pairs(&[
            ("__address__", "10.0.0.34:0"),
            ("__meta_openstack_instance_flavor", "m1.small"),
            ("__meta_openstack_instance_id", "9e5476bd-a4ec-4653-93d6-72c93aa682bb"),
            ("__meta_openstack_instance_status", "ACTIVE"),
            ("__meta_openstack_instance_name", "merp"),
            ("__meta_openstack_private_ip", "10.0.0.34"),
            ("__meta_openstack_address_pool", "private"),
            ("__meta_openstack_tag_env", "prod"),
            ("__meta_openstack_public_ip", "10.10.10.4"),
            ("__meta_openstack_project_id", "fcad67a6189847c4aecfa3c81a05783b"),
            ("__meta_openstack_user_id", "9349aff8be7545ac9d2f1d00999a23cd"),
        ]),
        LabelSet::from_pairs(&[
            ("__address__", "10.0.0.33:0"),
            ("__meta_openstack_instance_flavor", "m1.small"),
            ("__meta_openstack_instance_id", "87caf8ed-d92a-41f6-9dcd-d1399e39899f"),
            ("__meta_openstack_instance_status", "ACTIVE"),
            ("__meta_openstack_instance_name", "merp-project2"),
            ("__meta_openstack_private_ip", "10.0.0.33"),
            ("__meta_openstack_address_pool", "private"),
            ("__meta_openstack_tag_env", "prod"),
            ("__meta_openstack_project_id", "b78fef2305934dbbbeb9a10b4c326f7a"),
            ("__meta_openstack_user_id", "9349aff8be7545ac9d2f1d00999a23cd"),
        ]),
        LabelSet::from_pairs(&[
            ("__address__", "10.0.0.34:0"),
            ("__meta_openstack_instance_flavor", "m1.small"),
            ("__meta_openstack_instance_id", "87caf8ed-d92a-41f6-9dcd-d1399e39899f"),
            ("__meta_openstack_instance_status", "ACTIVE"),
            ("__meta_openstack_instance_name", "merp-project2"),
            ("__meta_openstack_private_ip", "10.0.0.34"),
            ("__meta_openstack_address_pool", "private"),
            ("__meta_openstack_tag_env", "prod"),
            ("__meta_openstack_public_ip", "10.10.10.24"),
            ("__meta_openstack_project_id", "b78fef2305934dbbbeb9a10b4c326f7a"),
            ("__meta_openstack_user_id", "9349aff8be7545ac9d2f1d00999a23cd"),
        ]),
    ];
    for (i, lbls) in expected.iter().enumerate() {
        assert_eq!(lbls, &tg.targets[i], "item {i}");
    }
}

#[test]
fn test_openstack_sd_instance_refresh_with_done_context() {
    // Go uses the hypervisor suite here.
    let mock = SdMock::new();
    mock.handle_hypervisor_list_successfully();
    mock.handle_versions_successfully();
    mock.handle_auth_successfully();
    let conf = SdConfig {
        identity_endpoint: mock.endpoint(),
        password: "test".into(),
        username: "test".to_string(),
        domain_name: "12345".to_string(),
        region: "RegionOne".to_string(),
        role: Role::new("hypervisor"),
        ..Default::default()
    };
    let mut hypervisor = new_refresher(&conf, Logger::nop()).unwrap();
    let ctx = CancelToken::new();
    ctx.cancel();
    let err = hypervisor.refresh(&ctx).unwrap_err();
    assert!(err.contains("context canceled"), "{err:?} doesn't contain \"context canceled\"");
}
