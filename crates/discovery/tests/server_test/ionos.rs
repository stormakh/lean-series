//! Port of discovery/ionos/server_test.go.
use crate::common::{Req, Resp, TestServer, ls, read_upstream};
use discovery::CancelToken;
use discovery::ionos::server::DEFAULT_IONOS_BASE_PATH;
use discovery::ionos::{ServerDiscovery, default_sd_config, new_server_discovery};
use model::labelset::LabelSet;

const IONOS_TEST_BEARER_TOKEN: &str = "jwt";
const IONOS_TEST_DATACENTER_ID: &str = "8feda53f-15f0-447f-badf-ebe32dad2fc0";

/// Go `mockIONOSServers`.
fn mock_ionos_servers(r: &Req) -> Resp {
    if r.header("Authorization") != format!("Bearer {IONOS_TEST_BEARER_TOKEN}") {
        return Resp::error(401, "bad token");
    }
    if r.path != format!("{DEFAULT_IONOS_BASE_PATH}/datacenters/{IONOS_TEST_DATACENTER_ID}/servers") {
        return Resp::error(404, &format!("bad url: {}", r.path));
    }
    Resp::json(read_upstream("discovery/ionos/testdata/servers.json"))
}

#[test]
fn test_ionos_server_refresh() {
    let mock = TestServer::new(mock_ionos_servers);
    let mut cfg = default_sd_config();
    cfg.datacenter_id = IONOS_TEST_DATACENTER_ID.to_string();
    cfg.http_client_config.bearer_token = yaml::Secret(IONOS_TEST_BEARER_TOKEN.to_string());
    cfg.ionos_endpoint = mock.url.clone();

    let mut d = new_server_discovery(&cfg, discovery::Logger::nop()).unwrap();
    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(tgs.len(), 1);
    let tg = &tgs[0];
    assert_eq!(tg.targets.len(), 3);

    for (i, lbls) in [
        ls(&[
            ("__address__", "85.215.243.177:80"),
            ("__meta_ionos_server_availability_zone", "ZONE_2"),
            ("__meta_ionos_server_boot_cdrom_id", "0e4d57f9-cd78-11e9-b88c-525400f64d8d"),
            ("__meta_ionos_server_cpu_family", "INTEL_SKYLAKE"),
            ("__meta_ionos_server_id", "b501942c-4e08-43e6-8ec1-00e59c64e0e4"),
            ("__meta_ionos_server_ip", ",85.215.243.177,185.56.150.9,85.215.238.118,"),
            ("__meta_ionos_server_nic_ip_metrics", ",85.215.243.177,"),
            ("__meta_ionos_server_nic_ip_unnamed", ",185.56.150.9,85.215.238.118,"),
            ("__meta_ionos_server_lifecycle", "AVAILABLE"),
            ("__meta_ionos_server_name", "prometheus-2"),
            ("__meta_ionos_server_servers_id", "8feda53f-15f0-447f-badf-ebe32dad2fc0/servers"),
            ("__meta_ionos_server_state", "RUNNING"),
            ("__meta_ionos_server_type", "ENTERPRISE"),
        ]),
        ls(&[
            ("__address__", "85.215.248.84:80"),
            ("__meta_ionos_server_availability_zone", "ZONE_1"),
            ("__meta_ionos_server_boot_cdrom_id", "0e4d57f9-cd78-11e9-b88c-525400f64d8d"),
            ("__meta_ionos_server_cpu_family", "INTEL_SKYLAKE"),
            ("__meta_ionos_server_id", "523415e6-ff8c-4dc0-86d3-09c256039b30"),
            ("__meta_ionos_server_ip", ",85.215.248.84,"),
            ("__meta_ionos_server_nic_ip_unnamed", ",85.215.248.84,"),
            ("__meta_ionos_server_lifecycle", "AVAILABLE"),
            ("__meta_ionos_server_name", "prometheus-1"),
            ("__meta_ionos_server_servers_id", "8feda53f-15f0-447f-badf-ebe32dad2fc0/servers"),
            ("__meta_ionos_server_state", "RUNNING"),
            ("__meta_ionos_server_type", "ENTERPRISE"),
        ]),
        // A server whose optional pointer fields (id, and everything under properties/metadata used for
        // labels) are null in the API response must not panic and must simply omit the corresponding labels.
        ls(&[
            ("__address__", "10.0.0.99:80"),
            ("__meta_ionos_server_ip", ",10.0.0.99,"),
            ("__meta_ionos_server_nic_ip_unnamed", ",10.0.0.99,"),
            ("__meta_ionos_server_servers_id", "8feda53f-15f0-447f-badf-ebe32dad2fc0/servers"),
        ]),
    ]
    .iter()
    .enumerate()
    {
        assert_eq!(&tg.targets[i], lbls, "item {i}");
    }
}

/// Go `ionosTestDiscovery`: a discovery pointed at a mock serving `body`. Returns the server too, which must
/// live as long as the discovery is used.
fn ionos_test_discovery(body: &str) -> (ServerDiscovery, TestServer) {
    let body = body.to_string();
    let mock = TestServer::new(move |_r: &Req| Resp::json(body.clone()));
    let mut cfg = default_sd_config();
    cfg.datacenter_id = IONOS_TEST_DATACENTER_ID.to_string();
    cfg.http_client_config.bearer_token = yaml::Secret(IONOS_TEST_BEARER_TOKEN.to_string());
    cfg.ionos_endpoint = mock.url.clone();
    (new_server_discovery(&cfg, discovery::Logger::nop()).unwrap(), mock)
}

/// Go `ionosServersBody`.
fn ionos_servers_body(items: &str) -> String {
    format!("{{\n\t\t\"id\": \"{IONOS_TEST_DATACENTER_ID}/servers\",\n\t\t\"type\": \"collection\",\n\t\t\"items\": [{items}]\n\t}}")
}

#[test]
fn test_ionos_server_refresh_nil_containers() {
    const SERVERS_ID: &str = "8feda53f-15f0-447f-badf-ebe32dad2fc0/servers";
    let cases: Vec<(&str, String, Vec<LabelSet>)> = vec![
        ("NoServerItems", format!("{{\"id\": \"{IONOS_TEST_DATACENTER_ID}/servers\", \"type\": \"collection\"}}"), vec![]),
        (
            "NilServerProperties",
            ionos_servers_body(
                r#"{
				"id": "srv-1",
				"type": "server",
				"properties": null,
				"entities": {"nics": {"items": [
					{"id": "nic-1", "type": "nic", "properties": {"ips": ["10.0.0.1"]}}
				]}}
			}"#,
            ),
            vec![ls(&[
                ("__address__", "10.0.0.1:80"),
                ("__meta_ionos_server_id", "srv-1"),
                ("__meta_ionos_server_ip", ",10.0.0.1,"),
                ("__meta_ionos_server_nic_ip_unnamed", ",10.0.0.1,"),
                ("__meta_ionos_server_servers_id", SERVERS_ID),
            ])],
        ),
        (
            "NoNICItems",
            ionos_servers_body(
                r#"{
				"id": "srv-2",
				"type": "server",
				"properties": {"name": "no-nics"},
				"entities": {"nics": {"id": "srv-2/nics", "type": "collection"}}
			}"#,
            ),
            vec![],
        ),
        (
            "NilNICProperties",
            ionos_servers_body(
                r#"{
				"id": "srv-3",
				"type": "server",
				"properties": {"name": "nic-without-properties"},
				"entities": {"nics": {"items": [
					{"id": "nic-3", "type": "nic", "properties": null}
				]}}
			}"#,
            ),
            vec![],
        ),
        (
            "NilNICIps",
            ionos_servers_body(
                r#"{
				"id": "srv-4",
				"type": "server",
				"properties": {"name": "nic-without-ips"},
				"entities": {"nics": {"items": [
					{"id": "nic-4", "type": "nic", "properties": {"name": "eth0"}}
				]}}
			}"#,
            ),
            vec![],
        ),
        (
            "NilBootCdromAndBootVolumeID",
            ionos_servers_body(
                r#"{
				"id": "srv-5",
				"type": "server",
				"properties": {"bootCdrom": {}, "bootVolume": {"id": null}},
				"entities": {"nics": {"items": [
					{"id": "nic-5", "type": "nic", "properties": {"ips": ["10.0.0.5"]}}
				]}}
			}"#,
            ),
            vec![ls(&[
                ("__address__", "10.0.0.5:80"),
                ("__meta_ionos_server_id", "srv-5"),
                ("__meta_ionos_server_ip", ",10.0.0.5,"),
                ("__meta_ionos_server_nic_ip_unnamed", ",10.0.0.5,"),
                ("__meta_ionos_server_servers_id", SERVERS_ID),
            ])],
        ),
        (
            "NoVolumeItems",
            ionos_servers_body(
                r#"{
				"id": "srv-6",
				"type": "server",
				"properties": {"name": "no-volume-items"},
				"entities": {
					"nics": {"items": [
						{"id": "nic-6", "type": "nic", "properties": {"ips": ["10.0.0.6"]}}
					]},
					"volumes": {"id": "srv-6/volumes", "type": "collection"}
				}
			}"#,
            ),
            vec![ls(&[
                ("__address__", "10.0.0.6:80"),
                ("__meta_ionos_server_id", "srv-6"),
                ("__meta_ionos_server_ip", ",10.0.0.6,"),
                ("__meta_ionos_server_name", "no-volume-items"),
                ("__meta_ionos_server_nic_ip_unnamed", ",10.0.0.6,"),
                ("__meta_ionos_server_servers_id", SERVERS_ID),
            ])],
        ),
        (
            "NilVolumeProperties",
            ionos_servers_body(
                r#"{
				"id": "srv-7",
				"type": "server",
				"properties": {"name": "volume-without-properties"},
				"entities": {
					"nics": {"items": [
						{"id": "nic-7", "type": "nic", "properties": {"ips": ["10.0.0.7"]}}
					]},
					"volumes": {"items": [{"id": "vol-7", "type": "volume", "properties": null}]}
				}
			}"#,
            ),
            vec![ls(&[
                ("__address__", "10.0.0.7:80"),
                ("__meta_ionos_server_id", "srv-7"),
                ("__meta_ionos_server_ip", ",10.0.0.7,"),
                ("__meta_ionos_server_name", "volume-without-properties"),
                ("__meta_ionos_server_nic_ip_unnamed", ",10.0.0.7,"),
                ("__meta_ionos_server_servers_id", SERVERS_ID),
            ])],
        ),
    ];
    for (name, body, expected) in cases {
        let (mut d, _mock) = ionos_test_discovery(&body);
        let tgs = d.refresh(&CancelToken::new()).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(tgs.len(), 1, "{name}");
        assert_eq!(tgs[0].targets, expected, "{name}");
    }
}
