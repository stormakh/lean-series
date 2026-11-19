//! Port of discovery/scaleway/instance_test.go.
//!
//! The config templates and inline response bodies are the raw string literals of the Go file, read from
//! `upstream/`; `testdata/` files are read from there too.
use discovery::CancelToken;
use discovery::refresh::Refresher;
use discovery::scaleway::{SdConfig, new_refresher};

use crate::common::{self, Req, Resp, TestServer, ls};

const TEST_PROJECT_ID: &str = "8feda53f-15f0-447f-badf-ebe32dad2fc0";
const TEST_SECRET_KEY: &str = "6d6579e5-a5b9-49fc-a35f-b4feb9b87301";
const TEST_ACCESS_KEY: &str = "SCW0W8NG6024YHRJ7723";
const INSTANCE_PRIVATE_IPV4_LABEL: &str = "__meta_scaleway_instance_private_ipv4";

/// Go `testSecretKeyFile` ("testdata/secret_key", relative to the package directory).
fn test_secret_key_file() -> String {
    common::upstream("discovery/scaleway/testdata/secret_key")
}

/// The raw string literals of instance_test.go, in order: config template, IPAM-test servers body, IPAM-test
/// ips body, config template, config template (secret_key_file).
fn raw_strings() -> Vec<String> {
    let src =
        String::from_utf8(common::read_upstream("discovery/scaleway/instance_test.go")).unwrap();
    src.split('`')
        .skip(1)
        .step_by(2)
        .map(str::to_string)
        .collect()
}

/// Go `fmt.Sprintf` with `%s` verbs only.
fn sprintf(tmpl: &str, args: &[&str]) -> String {
    let mut out = String::new();
    let mut parts = tmpl.split("%s");
    out.push_str(parts.next().unwrap());
    for (p, a) in parts.zip(args) {
        out.push_str(a);
        out.push_str(p);
    }
    out
}

/// Go `yaml.UnmarshalStrict(cfgString, &cfg)`.
fn load(cfg_string: &str) -> SdConfig {
    yaml::unmarshal_strict::<SdConfig>(cfg_string).unwrap()
}

/// Go `mockScalewayInstance`.
fn mock_scaleway_instance(r: &Req) -> Resp {
    if r.header("X-Auth-Token") != TEST_SECRET_KEY {
        return Resp::error(401, "bad token id");
    }
    if r.path != "/instance/v1/zones/fr-par-1/servers" {
        return Resp::error(404, "bad url");
    }
    match std::fs::read(common::upstream(
        "discovery/scaleway/testdata/instance.json",
    )) {
        Ok(b) => Resp::new(200, b).header("Content-Type", "application/json"),
        Err(e) => Resp::error(500, &e.to_string()),
    }
}

#[test]
fn test_scaleway_instance_refresh() {
    let mock = TestServer::new(mock_scaleway_instance);
    let cfg_string = sprintf(
        &raw_strings()[0],
        &[TEST_PROJECT_ID, TEST_SECRET_KEY, TEST_ACCESS_KEY, &mock.url],
    );
    let cfg = load(&cfg_string);

    let mut d = new_refresher(&cfg).unwrap();
    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());

    let tg = &tgs[0];
    assert_eq!(4, tg.targets.len());

    let expected = [
        ls(&[
            ("__address__", "10.70.60.57:80"),
            ("__meta_scaleway_instance_boot_type", "local"),
            ("__meta_scaleway_instance_hostname", "scw-nervous-shirley"),
            (
                "__meta_scaleway_instance_id",
                "93c18a61-b681-49d0-a1cc-62b43883ae89",
            ),
            ("__meta_scaleway_instance_image_arch", "x86_64"),
            (
                "__meta_scaleway_instance_image_id",
                "45a86b35-eca6-4055-9b34-ca69845da146",
            ),
            (
                "__meta_scaleway_instance_image_name",
                "Ubuntu 18.04 Bionic Beaver",
            ),
            ("__meta_scaleway_instance_location_cluster_id", "40"),
            ("__meta_scaleway_instance_location_hypervisor_id", "1601"),
            ("__meta_scaleway_instance_location_node_id", "29"),
            ("__meta_scaleway_instance_name", "scw-nervous-shirley"),
            (
                "__meta_scaleway_instance_organization_id",
                "cb334986-b054-4725-9d3a-40850fdc6015",
            ),
            ("__meta_scaleway_instance_private_ipv4", "10.70.60.57"),
            (
                "__meta_scaleway_instance_project_id",
                "cb334986-b054-4725-9d5a-30850fdc6015",
            ),
            (
                "__meta_scaleway_instance_public_ipv6",
                "2001:bc8:630:1e1c::1",
            ),
            ("__meta_scaleway_instance_region", "fr-par"),
            (
                "__meta_scaleway_instance_security_group_id",
                "a6a794b7-c05b-4b20-b9fe-e17a9bb85cf0",
            ),
            (
                "__meta_scaleway_instance_security_group_name",
                "Default security group",
            ),
            ("__meta_scaleway_instance_status", "running"),
            ("__meta_scaleway_instance_type", "DEV1-S"),
            ("__meta_scaleway_instance_zone", "fr-par-1"),
        ]),
        ls(&[
            ("__address__", "10.193.162.9:80"),
            ("__meta_scaleway_instance_boot_type", "local"),
            ("__meta_scaleway_instance_hostname", "scw-quizzical-feistel"),
            (
                "__meta_scaleway_instance_id",
                "5b6198b4-c677-41b5-9c05-04557264ae1f",
            ),
            ("__meta_scaleway_instance_image_arch", "x86_64"),
            (
                "__meta_scaleway_instance_image_id",
                "71733b74-260f-4d0d-8f18-f37dcfa56d6f",
            ),
            ("__meta_scaleway_instance_image_name", "Debian Buster"),
            ("__meta_scaleway_instance_location_cluster_id", "4"),
            ("__meta_scaleway_instance_location_hypervisor_id", "201"),
            ("__meta_scaleway_instance_location_node_id", "5"),
            ("__meta_scaleway_instance_name", "scw-quizzical-feistel"),
            (
                "__meta_scaleway_instance_organization_id",
                "cb334986-b054-4725-9d3a-40850fdc6015",
            ),
            ("__meta_scaleway_instance_private_ipv4", "10.193.162.9"),
            (
                "__meta_scaleway_instance_project_id",
                "cb334986-b054-4725-9d5a-30850fdc6015",
            ),
            ("__meta_scaleway_instance_public_ipv4", "151.115.45.127"),
            (
                "__meta_scaleway_instance_public_ipv6",
                "2001:bc8:1e00:6204::1",
            ),
            ("__meta_scaleway_instance_region", "fr-par"),
            (
                "__meta_scaleway_instance_security_group_id",
                "d35dd210-8392-44dc-8854-121e419a0f56",
            ),
            (
                "__meta_scaleway_instance_security_group_name",
                "Default security group",
            ),
            ("__meta_scaleway_instance_status", "running"),
            ("__meta_scaleway_instance_type", "DEV1-S"),
            ("__meta_scaleway_instance_zone", "fr-par-1"),
        ]),
        ls(&[
            ("__address__", "51.158.183.115:80"),
            ("__meta_scaleway_instance_boot_type", "local"),
            ("__meta_scaleway_instance_hostname", "routed-dualstack"),
            (
                "__meta_scaleway_instance_id",
                "4904366a-7e26-4b65-b97b-6392c761247a",
            ),
            ("__meta_scaleway_instance_image_arch", "x86_64"),
            (
                "__meta_scaleway_instance_image_id",
                "3e0a5b84-1d69-4993-8fa4-0d7df52d5160",
            ),
            (
                "__meta_scaleway_instance_image_name",
                "Ubuntu 22.04 Jammy Jellyfish",
            ),
            ("__meta_scaleway_instance_location_cluster_id", "19"),
            ("__meta_scaleway_instance_location_hypervisor_id", "1201"),
            ("__meta_scaleway_instance_location_node_id", "24"),
            ("__meta_scaleway_instance_name", "routed-dualstack"),
            (
                "__meta_scaleway_instance_organization_id",
                "20b3d507-96ac-454c-a795-bc731b46b12f",
            ),
            (
                "__meta_scaleway_instance_project_id",
                "20b3d507-96ac-454c-a795-bc731b46b12f",
            ),
            ("__meta_scaleway_instance_public_ipv4", "51.158.183.115"),
            (
                "__meta_scaleway_instance_public_ipv4_addresses",
                ",51.158.183.115,",
            ),
            (
                "__meta_scaleway_instance_public_ipv6_addresses",
                ",2001:bc8:1640:1568:dc00:ff:fe21:91b,",
            ),
            ("__meta_scaleway_instance_region", "nl-ams"),
            (
                "__meta_scaleway_instance_security_group_id",
                "984414da-9fc2-49c0-a925-fed6266fe092",
            ),
            (
                "__meta_scaleway_instance_security_group_name",
                "Default security group",
            ),
            ("__meta_scaleway_instance_status", "running"),
            ("__meta_scaleway_instance_type", "DEV1-S"),
            ("__meta_scaleway_instance_zone", "nl-ams-1"),
        ]),
        ls(&[
            ("__address__", "163.172.136.10:80"),
            ("__meta_scaleway_instance_boot_type", "local"),
            ("__meta_scaleway_instance_hostname", "multiple-ips"),
            (
                "__meta_scaleway_instance_id",
                "658abbf4-e6c6-4239-a483-3307763cf6e0",
            ),
            ("__meta_scaleway_instance_image_arch", "x86_64"),
            (
                "__meta_scaleway_instance_image_id",
                "f583f58c-1ea5-44ab-a1e6-2b2e7df32a86",
            ),
            (
                "__meta_scaleway_instance_image_name",
                "Ubuntu 24.04 Noble Numbat",
            ),
            ("__meta_scaleway_instance_location_cluster_id", "7"),
            ("__meta_scaleway_instance_location_hypervisor_id", "801"),
            ("__meta_scaleway_instance_location_node_id", "95"),
            ("__meta_scaleway_instance_name", "multiple-ips"),
            (
                "__meta_scaleway_instance_organization_id",
                "ee7bd9e1-9cbd-4724-b2f4-19e50f3cf38b",
            ),
            (
                "__meta_scaleway_instance_project_id",
                "ee7bd9e1-9cbd-4724-b2f4-19e50f3cf38b",
            ),
            ("__meta_scaleway_instance_public_ipv4", "163.172.136.10"),
            (
                "__meta_scaleway_instance_public_ipv4_addresses",
                ",163.172.136.10,212.47.248.223,51.15.231.134,",
            ),
            (
                "__meta_scaleway_instance_public_ipv6_addresses",
                ",2001:bc8:710:4a69:dc00:ff:fe58:40c1,2001:bc8:710:d::,2001:bc8:710:5417::,",
            ),
            ("__meta_scaleway_instance_region", "fr-par"),
            (
                "__meta_scaleway_instance_security_group_id",
                "0fe819c3-274d-472a-b3f5-ddb258d2d8bb",
            ),
            (
                "__meta_scaleway_instance_security_group_name",
                "Default security group",
            ),
            ("__meta_scaleway_instance_status", "running"),
            ("__meta_scaleway_instance_type", "PLAY2-PICO"),
            ("__meta_scaleway_instance_zone", "fr-par-1"),
        ]),
    ];
    for (i, lbls) in expected.iter().enumerate() {
        assert_eq!(lbls, &tg.targets[i], "item {i}");
    }
}

#[test]
fn test_scaleway_instance_refresh_ipam_private_nic() {
    let raw = raw_strings();
    let (servers_body, ips_body) = (raw[1].clone(), raw[2].clone());
    let mock = TestServer::new(move |r: &Req| {
        if r.header("X-Auth-Token") != TEST_SECRET_KEY {
            return Resp::error(401, "bad token id");
        }
        match r.path.as_str() {
            "/instance/v1/zones/fr-par-1/servers" => Resp::json(servers_body.clone()),
            "/ipam/v1/regions/fr-par/ips" => Resp::json(ips_body.clone()),
            _ => Resp::error(404, "bad url"),
        }
    });

    let cfg_string = sprintf(
        &raw[3],
        &[TEST_PROJECT_ID, TEST_SECRET_KEY, TEST_ACCESS_KEY, &mock.url],
    );
    let cfg = load(&cfg_string);

    let mut d = new_refresher(&cfg).unwrap();
    let tgs = d.refresh(&CancelToken::new()).unwrap();

    // adapted: Go asserts the IPAM query inside the handler; here the server records the requests.
    let ipam: Vec<Req> = mock
        .requests()
        .into_iter()
        .filter(|r| r.path == "/ipam/v1/regions/fr-par/ips")
        .collect();
    assert!(!ipam.is_empty());
    for r in &ipam {
        assert_eq!(
            "2d17a5a0-f8e5-4e7f-a029-f7c8d182af53",
            r.query_get("resource_ids")
        );
        assert_eq!("instance_private_nic", r.query_get("resource_types"));
    }

    assert_eq!(1, tgs.len());
    assert_eq!(1, tgs[0].targets.len());
    assert_eq!(
        Some(&"10.0.0.7:80".to_string()),
        tgs[0].targets[0].get("__address__")
    );
    assert_eq!(
        Some(&"10.0.0.7".to_string()),
        tgs[0].targets[0].get(INSTANCE_PRIVATE_IPV4_LABEL)
    );
}

#[test]
fn test_scaleway_instance_auth_token() {
    let mock = TestServer::new(mock_scaleway_instance);
    let key_file = test_secret_key_file();
    let cfg_string = sprintf(
        &raw_strings()[4],
        &[TEST_PROJECT_ID, &key_file, TEST_ACCESS_KEY, &mock.url],
    );
    let cfg = load(&cfg_string);

    let mut d = new_refresher(&cfg).unwrap();
    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());
}
