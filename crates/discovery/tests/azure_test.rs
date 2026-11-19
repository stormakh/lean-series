//! Port of discovery/azure/azure_test.go. Go builds the SDK clients on fake transports; here the same data is
//! served as Azure Resource Manager responses (the wire JSON, with nextLink paging) by a local mock server, so
//! the request, paging and parse path is tested too.
mod cloud_mock;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use cloud_mock::{MockRequest, MockResponse, MockServer};
use discovery::azure::arm::{
    ArmClients, ArmInterface, ArmScaleSet, ArmVirtualMachine, HardwareProfile, InterfaceProperties,
    IpConfiguration, IpConfigurationProperties, NetworkInterfaceReference, NetworkProfile, OsDisk,
    OsProfile, StorageProfile, VmProperties,
};
use discovery::azure::identity::{
    AccessToken, CredError, IdentityHttp, IdentityOptions, TokenCredential,
};
use discovery::azure::{
    AUTH_METHOD_MANAGED_IDENTITY, AzureClient, CloudConfiguration, Discovery, SdConfig, USER_AGENT,
    VirtualMachine, default_sd_config, map_from_vm, map_from_vm_scale_set_vm,
    new_azure_resource_from_id, new_credential,
};
use discovery::{CancelToken, Config, DiscovererOptions, Group, Logger, Registry};
use json::Json;
use model::labelset::LabelSet;

const DEFAULT_MOCK_NETWORK_ID: &str = "/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Network/networkInterfaces/{networkInterfaceName}";

fn s(v: &str) -> Option<String> {
    Some(v.to_string())
}

fn props(
    os_type: Option<&str>,
    with_os_disk: bool,
    nics: Vec<Option<NetworkInterfaceReference>>,
) -> VmProperties {
    VmProperties {
        os_profile: Some(OsProfile {
            computer_name: s("computer_name"),
        }),
        storage_profile: Some(StorageProfile {
            os_disk: if with_os_disk {
                Some(OsDisk {
                    os_type: os_type.map(|t| t.to_string()),
                })
            } else {
                None
            },
        }),
        network_profile: Some(NetworkProfile {
            network_interfaces: nics,
        }),
        hardware_profile: Some(HardwareProfile { vm_size: s("size") }),
    }
}

fn prometheus_tag() -> BTreeMap<String, Option<String>> {
    BTreeMap::from([("prometheus".to_string(), Some(String::new()))])
}

#[test]
fn test_map_from_vm_with_empty_tags() {
    let test_vm = ArmVirtualMachine {
        id: s("test"),
        name: s("name"),
        type_: s("type"),
        location: s("westeurope"),
        tags: None,
        properties: Some(props(Some("Linux"), true, vec![])),
        ..Default::default()
    };
    let expected = VirtualMachine {
        id: "test".into(),
        name: "name".into(),
        computer_name: "computer_name".into(),
        type_: "type".into(),
        location: "westeurope".into(),
        os_type: "Linux".into(),
        tags: BTreeMap::new(),
        network_interfaces: vec![],
        size: "size".into(),
        ..Default::default()
    };
    assert_eq!(expected, map_from_vm(&test_vm));
}

/// Go `azfake.TokenCredential`.
struct FakeCredential;

impl TokenCredential for FakeCredential {
    fn get_token(&self, _ctx: &CancelToken, _scopes: &[String]) -> Result<AccessToken, CredError> {
        Ok(AccessToken {
            token: "fake_token".to_string(),
            expires_on: SystemTime::now() + Duration::from_secs(3600),
            refresh_on: None,
        })
    }
}

/// The fake ARM server: list pages per operation and one network interface for every NIC request.
struct FakeArm {
    vm_pages: Vec<Vec<ArmVirtualMachine>>,
    vmss_pages: Vec<Vec<ArmScaleSet>>,
    vmssvm_pages: Vec<Vec<ArmVirtualMachine>>,
    interface: ArmInterface,
}

const SUB_PATH: &str = "/subscriptions/fake-subscription-id";

fn page_response<T>(
    base: &str,
    path: &str,
    req: &MockRequest,
    pages: &[Vec<T>],
    to_json: impl Fn(&T) -> Json,
) -> MockResponse {
    let i: usize = req
        .param("$skiptoken")
        .and_then(|t| t.parse().ok())
        .unwrap_or(0);
    let mut body = Json::obj();
    if i + 1 < pages.len() {
        body.set(
            "nextLink",
            format!("{base}{path}?api-version=2026-04-01&%24skiptoken={}", i + 1),
        );
    }
    let value: Vec<Json> = pages
        .get(i)
        .map(|p| p.iter().map(&to_json).collect())
        .unwrap_or_default();
    body.set("value", Json::Arr(value));
    MockResponse::json(body.stringify())
}

fn serve(fake: FakeArm) -> MockServer {
    let fake = Arc::new(fake);
    let base = Arc::new(std::sync::Mutex::new(String::new()));
    let b2 = base.clone();
    let srv = MockServer::from_fn(move |req| {
        let base = b2.lock().unwrap().clone();
        if req.method != "GET"
            || req.header("Authorization") != "Bearer fake_token"
            || req.header("Accept-Encoding") != "gzip"
        {
            return MockResponse::status(400, "bad request");
        }
        let api = req.param("api-version").unwrap_or_default();
        let ua = req.header("User-Agent");
        let p = req.path.as_str();
        let vms = format!("{SUB_PATH}/providers/Microsoft.Compute/virtualMachines");
        let vmss = format!("{SUB_PATH}/providers/Microsoft.Compute/virtualMachineScaleSets");
        let vmssvm = format!(
            "{SUB_PATH}/resourceGroups/%7BresourceGroup%7D/providers/Microsoft.Compute/virtualMachineScaleSets/vmScaleSet1/virtualMachines"
        );
        let nic = format!(
            "{SUB_PATH}/resourceGroups/%7BresourceGroupName%7D/providers/Microsoft.Network/networkInterfaces/%7BnetworkInterfaceName%7D"
        );
        let vmss_nic = format!(
            "{SUB_PATH}/resourceGroups/%7BresourceGroupName%7D/providers/microsoft.Compute/virtualMachineScaleSets/vmScaleSet1/virtualMachines/123/networkInterfaces/%7BnetworkInterfaceName%7D"
        );
        let compute_ua = ua.starts_with("Prometheus/3.16.0 azsdk-go-armcompute/v8.4.0 (");
        let network_ua = ua.starts_with("Prometheus/3.16.0 azsdk-go-v8/v8.0.0 (");
        if p == vms && api == "2026-04-01" && compute_ua {
            page_response(&base, p, req, &fake.vm_pages, |v| v.to_json())
        } else if p == vmss && api == "2026-04-01" && compute_ua {
            page_response(&base, p, req, &fake.vmss_pages, |v| v.to_json())
        } else if p == vmssvm && api == "2026-04-01" && compute_ua {
            page_response(&base, p, req, &fake.vmssvm_pages, |v| v.to_json())
        } else if network_ua
            && ((p == nic
                && req.query
                    == "%24expand=IPConfigurations%2FPublicIPAddress&api-version=2025-03-01")
                || (p == vmss_nic
                    && req.query
                        == "%24expand=IPConfigurations%2FPublicIPAddress&api-version=2018-10-01"))
        {
            MockResponse::json(fake.interface.to_json().stringify())
        } else {
            MockResponse::new(
                404,
                "application/json",
                r#"{"error":{"code":"NotFound","message":"unexpected request"}}"#,
            )
        }
    });
    *base.lock().unwrap() = srv.url.clone();
    srv
}

/// Go `createMockAzureClient`: the SDK clients with a fake credential, sending to the fake server.
fn create_mock_azure_client(srv: &MockServer) -> AzureClient {
    let http = net::new_client_from_config(
        &net::HttpClientConfig::default_config(),
        &net::HttpClientOptions::default(),
    )
    .unwrap();
    let arm = ArmClients::new(
        "fake-subscription-id",
        &srv.url,
        "https://management.core.windows.net/",
        Arc::new(FakeCredential),
        http,
        USER_AGENT,
        true,
    );
    AzureClient {
        arm,
        logger: Logger::nop(),
    }
}

fn new_discovery(cfg: &SdConfig) -> Discovery {
    let reg = Registry::new();
    let refresh_metrics = discovery::new_refresh_metrics(&reg);
    let metrics = cfg.new_discoverer_metrics(&reg, refresh_metrics);
    metrics.register().unwrap();
    Discovery::new(
        cfg,
        DiscovererOptions {
            logger: Logger::nop(),
            metrics: Some(metrics),
            set_name: "azure".to_string(),
            ..Default::default()
        },
    )
    .unwrap()
}

fn primary_nic(ip: &str) -> ArmInterface {
    ArmInterface {
        name: s(DEFAULT_MOCK_NETWORK_ID),
        id: s(DEFAULT_MOCK_NETWORK_ID),
        properties: Some(InterfaceProperties {
            primary: Some(true),
            ip_configurations: vec![Some(IpConfiguration {
                properties: Some(IpConfigurationProperties {
                    private_ip_address: s(ip),
                    public_ip_address: None,
                }),
            })],
        }),
    }
}

#[test]
fn test_vm_to_label_set() {
    let id = "/subscriptions/00000000-0000-0000-0000-000000000000/test";
    let nics = vec![Some(NetworkInterfaceReference {
        id: s(DEFAULT_MOCK_NETWORK_ID),
        primary: Some(true),
    })];
    let test_vm = ArmVirtualMachine {
        id: s(id),
        name: s("name"),
        type_: s("type"),
        location: s("westeurope"),
        tags: None,
        properties: Some(props(Some("Linux"), true, nics)),
        ..Default::default()
    };
    let expected = VirtualMachine {
        id: id.into(),
        name: "name".into(),
        computer_name: "computer_name".into(),
        type_: "type".into(),
        location: "westeurope".into(),
        os_type: "Linux".into(),
        tags: BTreeMap::new(),
        network_interfaces: vec![DEFAULT_MOCK_NETWORK_ID.into()],
        size: "size".into(),
        ..Default::default()
    };
    let actual = map_from_vm(&test_vm);
    assert_eq!(expected, actual);

    // adapted: Go builds the Discovery struct by hand with a 5-entry cache and no metrics; NewDiscovery is used
    // here (5000 entries, metrics registered), which changes nothing for one NIC.
    let mut d = new_discovery(&default_sd_config());
    let srv = serve(FakeArm {
        vm_pages: vec![],
        vmss_pages: vec![],
        vmssvm_pages: vec![],
        interface: primary_nic("10.20.30.40"),
    });
    let client = create_mock_azure_client(&srv);
    let label_set = d
        .refresher()
        .vm_to_label_set(&CancelToken::new(), &client, &actual)
        .unwrap()
        .unwrap();
    assert_eq!(11, label_set.0.len());
}

#[test]
fn test_vm_to_label_set_with_nil_tag_value() {
    // A tag with a JSON null value, as returned by the ARM API, is a None that is still in the tags.
    let id = "/subscriptions/00000000-0000-0000-0000-000000000000/test";
    let nics = vec![Some(NetworkInterfaceReference {
        id: s(DEFAULT_MOCK_NETWORK_ID),
        primary: Some(true),
    })];
    let tags = BTreeMap::from([
        ("CostCenter".to_string(), None),
        ("env".to_string(), s("prod")),
    ]);
    let test_vm = ArmVirtualMachine {
        id: s(id),
        name: s("name"),
        type_: s("type"),
        location: s("westeurope"),
        tags: Some(tags),
        properties: Some(props(Some("Linux"), true, nics)),
        ..Default::default()
    };
    let mut d = new_discovery(&default_sd_config());
    let srv = serve(FakeArm {
        vm_pages: vec![],
        vmss_pages: vec![],
        vmssvm_pages: vec![],
        interface: primary_nic("10.20.30.40"),
    });
    let client = create_mock_azure_client(&srv);
    let label_set = d
        .refresher()
        .vm_to_label_set(&CancelToken::new(), &client, &map_from_vm(&test_vm))
        .unwrap()
        .unwrap();
    // The tag with a value is kept, the one with a nil value is skipped.
    assert_eq!(
        Some(&"prod".to_string()),
        label_set.0.get("__meta_azure_machine_tag_env")
    );
    assert!(
        !label_set
            .0
            .contains_key("__meta_azure_machine_tag_CostCenter")
    );
}

#[test]
fn test_map_from_vm_with_empty_os_type() {
    let test_vm = ArmVirtualMachine {
        id: s("test"),
        name: s("name"),
        type_: s("type"),
        location: s("westeurope"),
        tags: None,
        properties: Some(props(None, true, vec![])),
        ..Default::default()
    };
    let expected = VirtualMachine {
        id: "test".into(),
        name: "name".into(),
        computer_name: "computer_name".into(),
        type_: "type".into(),
        location: "westeurope".into(),
        tags: BTreeMap::new(),
        network_interfaces: vec![],
        size: "size".into(),
        ..Default::default()
    };
    assert_eq!(expected, map_from_vm(&test_vm));
}

#[test]
fn test_map_from_vm_with_tags() {
    let test_vm = ArmVirtualMachine {
        id: s("test"),
        name: s("name"),
        type_: s("type"),
        location: s("westeurope"),
        tags: Some(prometheus_tag()),
        properties: Some(props(Some("Linux"), true, vec![])),
        ..Default::default()
    };
    let expected = VirtualMachine {
        id: "test".into(),
        name: "name".into(),
        computer_name: "computer_name".into(),
        type_: "type".into(),
        location: "westeurope".into(),
        os_type: "Linux".into(),
        tags: prometheus_tag(),
        network_interfaces: vec![],
        size: "size".into(),
        ..Default::default()
    };
    assert_eq!(expected, map_from_vm(&test_vm));
}

fn scale_set_vm(
    tags: Option<BTreeMap<String, Option<String>>>,
    os_type: Option<&str>,
    with_os_disk: bool,
) -> ArmVirtualMachine {
    ArmVirtualMachine {
        id: s("test"),
        name: s("name"),
        type_: s("type"),
        instance_id: s("123"),
        location: s("westeurope"),
        tags,
        properties: Some(props(os_type, with_os_disk, vec![])),
    }
}

fn expected_scale_set_vm(os_type: &str, tags: BTreeMap<String, Option<String>>) -> VirtualMachine {
    VirtualMachine {
        id: "test".into(),
        name: "name".into(),
        computer_name: "computer_name".into(),
        type_: "type".into(),
        location: "westeurope".into(),
        os_type: os_type.into(),
        tags,
        network_interfaces: vec![],
        scale_set: "testSet".into(),
        instance_id: "123".into(),
        size: "size".into(),
    }
}

#[test]
fn test_map_from_vm_scale_set_vm_with_empty_tags() {
    let test_vm = scale_set_vm(None, Some("Linux"), true);
    assert_eq!(
        expected_scale_set_vm("Linux", BTreeMap::new()),
        map_from_vm_scale_set_vm(&test_vm, "testSet")
    );
}

#[test]
fn test_map_from_vm_scale_set_vm_with_empty_os_type() {
    // Go: StorageProfile without an OSDisk.
    let test_vm = scale_set_vm(None, None, false);
    assert_eq!(
        expected_scale_set_vm("", BTreeMap::new()),
        map_from_vm_scale_set_vm(&test_vm, "testSet")
    );
}

#[test]
fn test_map_from_vm_scale_set_vm_with_tags() {
    let test_vm = scale_set_vm(Some(prometheus_tag()), Some("Linux"), true);
    assert_eq!(
        expected_scale_set_vm("Linux", prometheus_tag()),
        map_from_vm_scale_set_vm(&test_vm, "testSet")
    );
}

#[test]
fn test_new_azure_resource_from_id() {
    for (id, name, group) in [
        (
            "/subscriptions/SUBSCRIPTION_ID/resourceGroups/group/providers/PROVIDER/TYPE/name",
            "name",
            "group",
        ),
        (
            "/subscriptions/SUBSCRIPTION_ID/resourceGroups/group/providers/PROVIDER/TYPE/name/TYPE/h",
            "h",
            "group",
        ),
    ] {
        let actual = new_azure_resource_from_id(id, &Logger::nop()).unwrap();
        assert_eq!(name, actual.name);
        assert_eq!(group, actual.resource_group_name);
    }
}

#[test]
fn test_new_credential_managed_identity() {
    // System-assigned managed identity (empty client ID) creates a valid credential, and so does a
    // user-assigned one (non-empty client ID).
    let opts = IdentityOptions {
        authority_host: CloudConfiguration::azure_public().active_directory_authority_host,
        imds_endpoint: discovery::azure::identity::IMDS_ENDPOINT.to_string(),
        allow_http: false,
        http: IdentityHttp::new("").unwrap(),
    };
    let mut cfg = SdConfig {
        authentication_method: AUTH_METHOD_MANAGED_IDENTITY.to_string(),
        client_id: String::new(),
        ..Default::default()
    };
    assert!(new_credential(&cfg, &opts).is_ok());
    cfg.client_id = "00000000-0000-0000-0000-000000000000".to_string();
    assert!(new_credential(&cfg, &opts).is_ok());
}

fn default_vm_with_id_and_name(id: &str, name: &str) -> ArmVirtualMachine {
    ArmVirtualMachine {
        id: s(id),
        name: s(name),
        type_: s("Microsoft.Compute/virtualMachines"),
        location: s("australiaeast"),
        instance_id: None,
        properties: Some(props(
            Some("Linux"),
            true,
            vec![Some(NetworkInterfaceReference {
                id: s(DEFAULT_MOCK_NETWORK_ID),
                primary: None,
            })],
        )),
        tags: Some(prometheus_tag()),
    }
}

fn default_vmss_vm_with_id_and_name(id: &str, name: &str) -> ArmVirtualMachine {
    ArmVirtualMachine {
        instance_id: s("123"),
        ..default_vm_with_id_and_name(id, name)
    }
}

#[test]
fn test_azure_refresh() {
    let p = "/subscriptions/00000000-0000-0000-0000-00000000000/resourceGroups/{resourceGroup}/providers/Microsoft.Compute";
    let vm_pages = vec![
        vec![
            default_vm_with_id_and_name(&format!("{p}/virtualMachine/vm1"), "vm1"),
            default_vm_with_id_and_name(&format!("{p}/virtualMachine/vm2"), "vm2"),
        ],
        vec![
            default_vm_with_id_and_name(&format!("{p}/virtualMachine/vm3"), "vm3"),
            default_vm_with_id_and_name(&format!("{p}/virtualMachine/vm4"), "vm4"),
        ],
    ];
    let vmss_pages = vec![vec![ArmScaleSet {
        id: s(&format!("{p}/virtualMachineScaleSets/vmScaleSet1")),
        name: s("vmScaleSet1"),
        location: s("australiaeast"),
        type_: s("Microsoft.Compute/virtualMachineScaleSets"),
    }]];
    let vmssvm_pages = vec![vec![
        default_vmss_vm_with_id_and_name(
            &format!("{p}/virtualMachineScaleSets/vmScaleSet1/virtualMachines/vmScaleSet1_vm1"),
            "vmScaleSet1_vm1",
        ),
        default_vmss_vm_with_id_and_name(
            &format!("{p}/virtualMachineScaleSets/vmScaleSet1/virtualMachines/vmScaleSet1_vm2"),
            "vmScaleSet1_vm2",
        ),
    ]];
    let interface = ArmInterface {
        id: s(DEFAULT_MOCK_NETWORK_ID),
        name: None,
        ..primary_nic("10.0.0.1")
    };

    let target = |id: &str, name: &str, scale_set: Option<&str>| {
        let mut l = vec![
            ("__address__", "10.0.0.1:80"),
            ("__meta_azure_machine_computer_name", "computer_name"),
            ("__meta_azure_machine_id", id),
            ("__meta_azure_machine_location", "australiaeast"),
            ("__meta_azure_machine_name", name),
            ("__meta_azure_machine_os_type", "Linux"),
            ("__meta_azure_machine_private_ip", "10.0.0.1"),
            ("__meta_azure_machine_resource_group", "{resourceGroup}"),
            ("__meta_azure_machine_size", "size"),
            ("__meta_azure_machine_tag_prometheus", ""),
            ("__meta_azure_subscription_id", ""),
            ("__meta_azure_tenant_id", ""),
        ];
        if let Some(ss) = scale_set {
            l.push(("__meta_azure_machine_scale_set", ss));
        }
        LabelSet::from_pairs(&l)
    };
    let expected_tg = vec![Arc::new(Group {
        targets: vec![
            target(&format!("{p}/virtualMachine/vm1"), "vm1", None),
            target(&format!("{p}/virtualMachine/vm2"), "vm2", None),
            target(&format!("{p}/virtualMachine/vm3"), "vm3", None),
            target(&format!("{p}/virtualMachine/vm4"), "vm4", None),
            target(
                &format!("{p}/virtualMachineScaleSets/vmScaleSet1/virtualMachines/vmScaleSet1_vm1"),
                "vmScaleSet1_vm1",
                Some("vmScaleSet1"),
            ),
            target(
                &format!("{p}/virtualMachineScaleSets/vmScaleSet1/virtualMachines/vmScaleSet1_vm2"),
                "vmScaleSet1_vm2",
                Some("vmScaleSet1"),
            ),
        ],
        labels: LabelSet::new(),
        source: String::new(),
    })];

    // scenario: VMs, VMSS and VMSSVMs in Multiple Responses
    let srv = serve(FakeArm {
        vm_pages,
        vmss_pages,
        vmssvm_pages,
        interface,
    });
    let client = create_mock_azure_client(&srv);
    let mut sd = new_discovery(&default_sd_config());
    let mut tg = sd
        .refresher()
        .refresh_azure_client(&CancelToken::new(), &client)
        .unwrap();
    let g = Arc::make_mut(&mut tg[0]);
    g.targets.sort_by(|a, b| {
        a.0.get("__meta_azure_machine_id")
            .cmp(&b.0.get("__meta_azure_machine_id"))
    });
    assert_eq!(expected_tg, tg);
}
