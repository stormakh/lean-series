//! Port of discovery/openstack/hypervisor_test.go.
mod cloud_mock;
mod openstack_mock;

use discovery::openstack::{Role, SdConfig, new_refresher};
use discovery::refresh::Refresher;
use discovery::{CancelToken, Logger};
use openstack_mock::SdMock;

/// Go `OpenstackSDHypervisorTestSuite.SetupTest`.
fn setup() -> SdMock {
    let m = SdMock::new();
    m.handle_hypervisor_list_successfully();
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
        role: Role::new("hypervisor"),
        ..Default::default()
    };
    new_refresher(&conf, Logger::nop())
}

#[test]
fn test_openstack_sd_hypervisor_refresh() {
    let mock = setup();
    let mut hypervisor = openstack_auth_success(&mock).unwrap();
    let tgs = hypervisor.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());
    let tg = &tgs[0];
    assert_eq!(2, tg.targets.len());

    for (l, v) in [
        ("__address__", "172.16.70.14:0"),
        ("__meta_openstack_hypervisor_hostname", "nc14.cloud.com"),
        ("__meta_openstack_hypervisor_type", "QEMU"),
        ("__meta_openstack_hypervisor_host_ip", "172.16.70.14"),
        ("__meta_openstack_hypervisor_state", "up"),
        ("__meta_openstack_hypervisor_status", "enabled"),
        ("__meta_openstack_hypervisor_id", "1"),
    ] {
        assert_eq!(Some(v), tg.targets[0].get(l).map(String::as_str), "{l}");
    }

    for (l, v) in [
        ("__address__", "172.16.70.13:0"),
        ("__meta_openstack_hypervisor_hostname", "cc13.cloud.com"),
        ("__meta_openstack_hypervisor_type", "QEMU"),
        ("__meta_openstack_hypervisor_host_ip", "172.16.70.13"),
        ("__meta_openstack_hypervisor_state", "up"),
        ("__meta_openstack_hypervisor_status", "enabled"),
        ("__meta_openstack_hypervisor_id", "721"),
    ] {
        assert_eq!(Some(v), tg.targets[1].get(l).map(String::as_str), "{l}");
    }
}

#[test]
fn test_openstack_sd_hypervisor_refresh_with_done_context() {
    let mock = setup();
    let mut hypervisor = openstack_auth_success(&mock).unwrap();
    let ctx = CancelToken::new();
    ctx.cancel();
    let err = hypervisor.refresh(&ctx).unwrap_err();
    assert!(err.contains("context canceled"), "{err:?} doesn't contain \"context canceled\"");
}
