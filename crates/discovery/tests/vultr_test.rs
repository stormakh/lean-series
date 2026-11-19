//! Port of discovery/vultr/vultr_test.go and mock_test.go.
mod common;

use common::{Req, Resp, TestServer, ls, new_options};
use discovery::CancelToken;
use discovery::vultr::{Discovery, default_sd_config};

const API_KEY: &str = "ABCBTDG35OTGH2UKCC3S6CNMDUPCN3ZWSGFQ";

/// The instance list the Go mock returns (`HandleInstanceList`).
const INSTANCE_LIST: &str = r#"
{
    "instances": [
        {
            "id": "dbdbd38c-9884-4c92-95fe-899e50dee717",
            "os": "Marketplace",
            "ram": 4096,
            "disk": 128,
            "main_ip": "149.28.234.27",
            "vcpu_count": 2,
            "region": "ewr",
            "plan": "vhf-2c-4gb",
            "date_created": "2022-03-10T02:25:30+00:00",
            "status": "active",
            "allowed_bandwidth": 3000,
            "netmask_v4": "255.255.254.0",
            "gateway_v4": "149.28.234.1",
            "power_status": "running",
            "server_status": "ok",
            "v6_network": "",
            "v6_main_ip": "",
            "v6_network_size": 0,
            "label": "np-2-eae38a19b0f3",
            "internal_ip": "10.1.96.5",
            "kvm": "https://my.vultr.com/subs/vps/novnc/api.php?data=djJ8Q2k2NlVpNUlZNE5kM3NZMWdBNzFKUkVadDBDU3ItTEJ8-yNWBdMn1AWPqX-EnpTVbA1qBeP0txBZbahSOh3UtHIL2p0fZKEdFJwFNjWtvBKC9BGvJkoJdWBoo7hF-6RzavdUKPD3KIU5xD_T6keV8X-PPukPgXMuSUaAqurAuJvwAWXcTcodBjaIH961xgbvfkFyqVl0ZgHf3ErZQE-EmpZ0Jc3uF1DO2McJ4UL8KUvHLwa-ZDUZ_AtWJjQWsjNkxjvOiA",
            "hostname": "np-2-eae38a19b0f3",
            "os_id": 426,
            "app_id": 0,
            "image_id": "vke-worker",
            "firewall_group_id": "",
            "features": ["backups"],
            "tags": ["tag1", "tag2", "tag3"]
        },
        {
            "id": "fccb117c-62f7-4b17-995d-a8e56dd30b33",
            "os": "Marketplace",
            "ram": 4096,
            "disk": 128,
            "main_ip": "45.63.1.222",
            "vcpu_count": 2,
            "region": "ewr",
            "plan": "vhf-2c-4gb",
            "date_created": "2022-03-10T02:25:32+00:00",
            "status": "active",
            "allowed_bandwidth": 3000,
            "netmask_v4": "255.255.254.0",
            "gateway_v4": "45.63.0.1",
            "power_status": "running",
            "server_status": "ok",
            "v6_network": "",
            "v6_main_ip": "",
            "v6_network_size": 0,
            "label": "np-2-fd0714b5fe42",
            "internal_ip": "10.1.96.6",
            "kvm": "https://my.vultr.com/subs/vps/novnc/api.php?data=djJ8R0tzbmI0VWVfSDR6VE51Q0FGUTBCamwyTFNzZDNkVTF8Zwm93t6L2BLTYSNmM0g0Ppf43r71aIbqLiJCx6_1NuCugUqIKz8POshH91vi6XXDorpb3hYTk8RrJWQbaFMak7XbbfMObzsrFxKAQUVGfbjTBIHCKftxNIXeQOp1fEIDa3p-vkfSxEpx6ZX8nChGYYSVuOjS1twV6oI_IKtHXKdFJiVFz0Unay_K3HuXW_H2wrYSjmAChloR-eA1jSmBXLccJQ",
            "hostname": "np-2-fd0714b5fe42",
            "os_id": 426,
            "app_id": 0,
            "image_id": "vke-worker",
            "firewall_group_id": "",
            "features": [],
            "tags": []
        },
        {
            "id": "c4e58767-f61b-4c5e-9bce-43761cc04868",
            "os": "Marketplace",
            "ram": 4096,
            "disk": 128,
            "main_ip": "149.28.237.151",
            "vcpu_count": 2,
            "region": "ewr",
            "plan": "vhf-2c-4gb",
            "date_created": "2022-03-10T02:25:33+00:00",
            "status": "active",
            "allowed_bandwidth": 3000,
            "netmask_v4": "255.255.254.0",
            "gateway_v4": "149.28.236.1",
            "power_status": "running",
            "server_status": "ok",
            "v6_network": "",
            "v6_main_ip": "",
            "v6_network_size": 0,
            "label": "np-2-d04e7829fa43",
            "internal_ip": "10.1.96.7",
            "kvm": "https://my.vultr.com/subs/vps/novnc/api.php?data=djJ8NFEzbFZmV2hOUWNrc2R4bndnckhFQTJ3ME1yWmxVeW98P-p-KeyNtP73wzVvuWZSOrV5PvQDiBmYyB3cp99btIy5nczmvX8DOGSChbEldk3idKNdFLQMIieWnpR5xU5K-dMYlL6n6oYv2u-rh8zxS6-8hQsSbL2OsoywNPoLpQ3xPkT9Rb-yPrZX4fDLbt8yPVKDHqHO0GB-TxVPemueovfkkUZMXSraAXgHtl1YgK2uLMQf1WBFVzgwnQ7MYcfMGGYFQw",
            "hostname": "np-2-d04e7829fa43",
            "os_id": 426,
            "app_id": 0,
            "image_id": "vke-worker",
            "firewall_group_id": "",
            "features": [],
            "tags": []
        }
    ],
    "meta": {
        "total": 3,
        "links": {
            "next": "",
            "prev": ""
        }
    }
}
"#;

/// Go `SDMock` with `HandleInstanceList`.
struct SdMock {
    server: TestServer,
}

impl SdMock {
    /// Go `NewSDMock` + `Setup` + `HandleInstanceList`.
    fn setup() -> SdMock {
        let server = TestServer::new(|r: &Req| {
            if r.path != "/v2/instances" {
                return Resp::not_found();
            }
            if r.header("Authorization") != format!("Bearer {API_KEY}") {
                return Resp::new(403, "");
            }
            Resp::new(200, INSTANCE_LIST).header("content-type", "application/json")
        });
        SdMock { server }
    }

    /// Go `Endpoint`.
    fn endpoint(&self) -> String {
        format!("{}/", self.server.url)
    }
}

#[test]
fn test_vultr_sd_refresh() {
    let sd_mock = SdMock::setup();

    let mut cfg = default_sd_config();
    cfg.http_client_config.bearer_token = yaml::Secret::new(API_KEY);

    let (mut opts, _reg) = new_options(&cfg);
    opts.set_name = "vultr".to_string();
    let mut d = Discovery::new(&cfg, opts).unwrap();
    let endpoint = yaml::url::parse(&sd_mock.endpoint()).unwrap();
    d.set_base_url(endpoint);

    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());
    let tg = &tgs[0];
    assert_eq!(3, tg.targets.len());

    let want = [
        ls(&[
            ("__address__", "149.28.234.27:80"),
            ("__meta_vultr_instance_id", "dbdbd38c-9884-4c92-95fe-899e50dee717"),
            ("__meta_vultr_instance_label", "np-2-eae38a19b0f3"),
            ("__meta_vultr_instance_os", "Marketplace"),
            ("__meta_vultr_instance_os_id", "426"),
            ("__meta_vultr_instance_region", "ewr"),
            ("__meta_vultr_instance_plan", "vhf-2c-4gb"),
            ("__meta_vultr_instance_main_ip", "149.28.234.27"),
            ("__meta_vultr_instance_internal_ip", "10.1.96.5"),
            ("__meta_vultr_instance_main_ipv6", ""),
            ("__meta_vultr_instance_features", ",backups,"),
            ("__meta_vultr_instance_tags", ",tag1,tag2,tag3,"),
            ("__meta_vultr_instance_hostname", "np-2-eae38a19b0f3"),
            ("__meta_vultr_instance_server_status", "ok"),
            ("__meta_vultr_instance_vcpu_count", "2"),
            ("__meta_vultr_instance_ram_mb", "4096"),
            ("__meta_vultr_instance_disk_gb", "128"),
            ("__meta_vultr_instance_allowed_bandwidth_gb", "3000"),
        ]),
        ls(&[
            ("__address__", "45.63.1.222:80"),
            ("__meta_vultr_instance_id", "fccb117c-62f7-4b17-995d-a8e56dd30b33"),
            ("__meta_vultr_instance_label", "np-2-fd0714b5fe42"),
            ("__meta_vultr_instance_os", "Marketplace"),
            ("__meta_vultr_instance_os_id", "426"),
            ("__meta_vultr_instance_region", "ewr"),
            ("__meta_vultr_instance_plan", "vhf-2c-4gb"),
            ("__meta_vultr_instance_main_ip", "45.63.1.222"),
            ("__meta_vultr_instance_internal_ip", "10.1.96.6"),
            ("__meta_vultr_instance_main_ipv6", ""),
            ("__meta_vultr_instance_hostname", "np-2-fd0714b5fe42"),
            ("__meta_vultr_instance_server_status", "ok"),
            ("__meta_vultr_instance_vcpu_count", "2"),
            ("__meta_vultr_instance_ram_mb", "4096"),
            ("__meta_vultr_instance_disk_gb", "128"),
            ("__meta_vultr_instance_allowed_bandwidth_gb", "3000"),
        ]),
        ls(&[
            ("__address__", "149.28.237.151:80"),
            ("__meta_vultr_instance_id", "c4e58767-f61b-4c5e-9bce-43761cc04868"),
            ("__meta_vultr_instance_label", "np-2-d04e7829fa43"),
            ("__meta_vultr_instance_os", "Marketplace"),
            ("__meta_vultr_instance_os_id", "426"),
            ("__meta_vultr_instance_region", "ewr"),
            ("__meta_vultr_instance_plan", "vhf-2c-4gb"),
            ("__meta_vultr_instance_main_ip", "149.28.237.151"),
            ("__meta_vultr_instance_internal_ip", "10.1.96.7"),
            ("__meta_vultr_instance_main_ipv6", ""),
            ("__meta_vultr_instance_hostname", "np-2-d04e7829fa43"),
            ("__meta_vultr_instance_server_status", "ok"),
            ("__meta_vultr_instance_vcpu_count", "2"),
            ("__meta_vultr_instance_ram_mb", "4096"),
            ("__meta_vultr_instance_disk_gb", "128"),
            ("__meta_vultr_instance_allowed_bandwidth_gb", "3000"),
        ]),
    ];
    for (i, k) in want.iter().enumerate() {
        assert_eq!(k, &tg.targets[i], "item {i}");
    }
}
