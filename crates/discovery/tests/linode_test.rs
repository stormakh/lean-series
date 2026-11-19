//! Port of discovery/linode/linode_test.go and mock_test.go.
mod common;

use common::{Req, Resp, TestServer, new_options};
use discovery::CancelToken;
use discovery::linode::{Discovery, default_sd_config};
use model::labelset::LabelSet;

const TOKEN_ID: &str = "7b2c56dd51edd90952c1b94c472b94b176f20c5c777e376849edd8ad1c6c03bb";

/// Go `SDMock`: the Linode API mock.
struct SdMock {
    server: TestServer,
}

impl SdMock {
    /// Go `NewSDMock` + `Setup` + `SetupHandlers`.
    fn setup() -> SdMock {
        let handlers = ["/v4/account/events", "/v4/linode/instances", "/v4/networking/ips", "/v4/networking/ipv6/ranges"];
        let server = TestServer::new(move |r: &Req| {
            if !handlers.contains(&r.path.as_str()) {
                return Resp::not_found();
            }
            if r.header("Authorization") != format!("Bearer {TOKEN_ID}") {
                return Resp::new(401, "");
            }
            let region = json::Json::parse(r.header("X-Filter")).ok().and_then(|f| f.get("region").and_then(|v| v.as_str().map(str::to_string))).unwrap_or_default();
            let mut directory = "discovery/linode/testdata/no_region_filter".to_string();
            if !region.is_empty() {
                // Validate region filter matches test criteria.
                directory = format!("discovery/linode/testdata/{region}");
            }
            match std::fs::read(common::upstream(&format!("{directory}{}.json", r.path))) {
                Ok(response) => Resp::new(200, response).header("content-type", "application/json; charset=utf-8"),
                Err(_) => Resp::new(500, ""),
            }
        });
        SdMock { server }
    }

    /// Go `Endpoint`.
    fn endpoint(&self) -> String {
        format!("{}/", self.server.url)
    }
}

fn want(p: &[(&str, &str)]) -> LabelSet {
    common::ls(p)
}

#[test]
fn test_linode_sd_refresh() {
    let sdmock = SdMock::setup();

    let exporter1 = want(&[
        ("__address__", "45.33.82.151:80"),
        ("__meta_linode_instance_id", "26838044"),
        ("__meta_linode_instance_label", "prometheus-linode-sd-exporter-1"),
        ("__meta_linode_image", "linode/arch"),
        ("__meta_linode_private_ipv4", "192.168.170.51"),
        ("__meta_linode_public_ipv4", "45.33.82.151"),
        ("__meta_linode_public_ipv6", "2600:3c03::f03c:92ff:fe1a:1382"),
        ("__meta_linode_private_ipv4_rdns", ""),
        ("__meta_linode_public_ipv4_rdns", "li1028-151.members.linode.com"),
        ("__meta_linode_public_ipv6_rdns", ""),
        ("__meta_linode_region", "us-east"),
        ("__meta_linode_type", "g6-standard-2"),
        ("__meta_linode_status", "running"),
        ("__meta_linode_tags", ",monitoring,"),
        ("__meta_linode_group", ""),
        ("__meta_linode_gpus", "0"),
        ("__meta_linode_hypervisor", "kvm"),
        ("__meta_linode_backups", "disabled"),
        ("__meta_linode_specs_disk_bytes", "85899345920"),
        ("__meta_linode_specs_memory_bytes", "4294967296"),
        ("__meta_linode_specs_vcpus", "2"),
        ("__meta_linode_specs_transfer_bytes", "4194304000"),
        ("__meta_linode_extra_ips", ",96.126.108.16,192.168.201.25,"),
    ]);
    let exporter2 = want(&[
        ("__address__", "139.162.196.43:80"),
        ("__meta_linode_instance_id", "26848419"),
        ("__meta_linode_instance_label", "prometheus-linode-sd-exporter-2"),
        ("__meta_linode_image", "linode/debian10"),
        ("__meta_linode_private_ipv4", ""),
        ("__meta_linode_public_ipv4", "139.162.196.43"),
        ("__meta_linode_public_ipv6", "2a01:7e00::f03c:92ff:fe1a:9976"),
        ("__meta_linode_private_ipv4_rdns", ""),
        ("__meta_linode_public_ipv4_rdns", "li1359-43.members.linode.com"),
        ("__meta_linode_public_ipv6_rdns", ""),
        ("__meta_linode_region", "eu-west"),
        ("__meta_linode_type", "g6-standard-2"),
        ("__meta_linode_status", "running"),
        ("__meta_linode_tags", ",monitoring,"),
        ("__meta_linode_group", ""),
        ("__meta_linode_gpus", "0"),
        ("__meta_linode_hypervisor", "kvm"),
        ("__meta_linode_backups", "disabled"),
        ("__meta_linode_specs_disk_bytes", "85899345920"),
        ("__meta_linode_specs_memory_bytes", "4294967296"),
        ("__meta_linode_specs_vcpus", "2"),
        ("__meta_linode_specs_transfer_bytes", "4194304000"),
    ]);
    let exporter3 = want(&[
        ("__address__", "192.53.120.25:80"),
        ("__meta_linode_instance_id", "26837938"),
        ("__meta_linode_instance_label", "prometheus-linode-sd-exporter-3"),
        ("__meta_linode_image", "linode/ubuntu20.04"),
        ("__meta_linode_private_ipv4", ""),
        ("__meta_linode_public_ipv4", "192.53.120.25"),
        ("__meta_linode_public_ipv6", "2600:3c04::f03c:92ff:fe1a:fb68"),
        ("__meta_linode_private_ipv4_rdns", ""),
        ("__meta_linode_public_ipv4_rdns", "li2216-25.members.linode.com"),
        ("__meta_linode_public_ipv6_rdns", ""),
        ("__meta_linode_region", "ca-central"),
        ("__meta_linode_type", "g6-standard-1"),
        ("__meta_linode_status", "running"),
        ("__meta_linode_tags", ",monitoring,"),
        ("__meta_linode_group", ""),
        ("__meta_linode_gpus", "0"),
        ("__meta_linode_hypervisor", "kvm"),
        ("__meta_linode_backups", "disabled"),
        ("__meta_linode_specs_disk_bytes", "53687091200"),
        ("__meta_linode_specs_memory_bytes", "2147483648"),
        ("__meta_linode_specs_vcpus", "1"),
        ("__meta_linode_specs_transfer_bytes", "2097152000"),
        ("__meta_linode_ipv6_ranges", ",2600:3c04:e001:456::/64,"),
    ]);
    let exporter4 = want(&[
        ("__address__", "66.228.47.103:80"),
        ("__meta_linode_instance_id", "26837992"),
        ("__meta_linode_instance_label", "prometheus-linode-sd-exporter-4"),
        ("__meta_linode_image", "linode/ubuntu20.04"),
        ("__meta_linode_private_ipv4", "192.168.148.94"),
        ("__meta_linode_public_ipv4", "66.228.47.103"),
        ("__meta_linode_public_ipv6", "2600:3c03::f03c:92ff:fe1a:fb4c"),
        ("__meta_linode_private_ipv4_rdns", ""),
        ("__meta_linode_public_ipv4_rdns", "li328-103.members.linode.com"),
        ("__meta_linode_public_ipv6_rdns", ""),
        ("__meta_linode_region", "us-east"),
        ("__meta_linode_type", "g6-nanode-1"),
        ("__meta_linode_status", "running"),
        ("__meta_linode_tags", ",monitoring,"),
        ("__meta_linode_group", ""),
        ("__meta_linode_gpus", "0"),
        ("__meta_linode_hypervisor", "kvm"),
        ("__meta_linode_backups", "disabled"),
        ("__meta_linode_specs_disk_bytes", "26843545600"),
        ("__meta_linode_specs_memory_bytes", "1073741824"),
        ("__meta_linode_specs_vcpus", "1"),
        ("__meta_linode_specs_transfer_bytes", "1048576000"),
        ("__meta_linode_extra_ips", ",172.104.18.104,"),
        ("__meta_linode_ipv6_ranges", ",2600:3c03:e000:123::/64,"),
    ]);

    // name, region, target count, wanted label sets
    let tests: Vec<(&str, &str, usize, Vec<LabelSet>)> = vec![
        ("no_region", "", 4, vec![exporter1.clone(), exporter2, exporter3.clone(), exporter4.clone()]),
        ("us-east", "us-east", 2, vec![exporter1, exporter4]),
        ("us-central", "ca-central", 1, vec![exporter3]),
    ];

    for (name, region, target_count, want) in tests {
        let mut cfg = default_sd_config();
        if !region.is_empty() {
            cfg.region = region.to_string();
        }
        cfg.http_client_config.authorization = Some(net::Authorization { credentials: yaml::Secret::new(TOKEN_ID), type_: "Bearer".to_string(), ..Default::default() });

        let (mut opts, _reg) = new_options(&cfg);
        opts.set_name = "linode".to_string();
        let mut d = Discovery::new(&cfg, opts).unwrap();
        let endpoint = yaml::url::parse(&sdmock.endpoint()).unwrap();
        d.set_base_url(&endpoint.to_url_string());

        let tgs = d.refresh(&CancelToken::new()).unwrap();
        assert_eq!(1, tgs.len(), "{name}");
        let tg = &tgs[0];
        assert_eq!(target_count, tg.targets.len(), "{name}");

        for (i, lbls) in want.iter().enumerate() {
            assert_eq!(lbls, &tg.targets[i], "{name}: item {i}");
        }
    }
}
