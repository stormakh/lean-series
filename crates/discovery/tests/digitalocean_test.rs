//! Port of discovery/digitalocean/digitalocean_test.go.
mod common;
mod digitalocean_mock;

use common::ls;
use digitalocean_mock::{SdMock, TOKEN_ID};
use discovery::digitalocean::{default_sd_config, new_refresher};
use discovery::refresh::Refresher;
use discovery::{CancelToken, Config, DiscovererMetrics};

/// Go `DigitalOceanSDTestSuite.SetupTest`.
fn setup_test() -> SdMock {
    let mock = SdMock::new();
    mock.handle_droplets_list();
    mock
}

#[test]
fn test_digital_ocean_sd_refresh() {
    let mut sdmock = setup_test();

    let mut cfg = default_sd_config();
    cfg.http_client_config.bearer_token = yaml::Secret::new(TOKEN_ID);

    let reg = discovery::Registry::new();
    let refresh_metrics = discovery::new_refresh_metrics(&reg);
    let metrics = cfg.new_discoverer_metrics(&reg, refresh_metrics.clone());
    metrics.register().unwrap();

    let mut d = new_refresher(&cfg).unwrap();
    d.client().base_url = yaml::url::parse(&sdmock.endpoint()).unwrap();

    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());

    let tg = &tgs[0];
    assert_eq!(4, tg.targets.len());

    let expected = [
        ls(&[
            ("__address__", "104.236.32.182:80"),
            ("__meta_digitalocean_droplet_id", "3164444"),
            ("__meta_digitalocean_droplet_name", "example.com"),
            ("__meta_digitalocean_image", "ubuntu-16-04-x64"),
            ("__meta_digitalocean_image_name", "14.04 x64"),
            ("__meta_digitalocean_private_ipv4", ""),
            ("__meta_digitalocean_public_ipv4", "104.236.32.182"),
            ("__meta_digitalocean_public_ipv6", "2604:A880:0800:0010:0000:0000:02DD:4001"),
            ("__meta_digitalocean_region", "nyc3"),
            ("__meta_digitalocean_size", "s-1vcpu-1gb"),
            ("__meta_digitalocean_status", "active"),
            ("__meta_digitalocean_vpc", "f9b0769c-e118-42fb-a0c4-fed15ef69662"),
            ("__meta_digitalocean_features", ",backups,ipv6,virtio,"),
        ]),
        ls(&[
            ("__address__", "104.131.186.241:80"),
            ("__meta_digitalocean_droplet_id", "3164494"),
            ("__meta_digitalocean_droplet_name", "prometheus"),
            ("__meta_digitalocean_image", "ubuntu-16-04-x64"),
            ("__meta_digitalocean_image_name", "14.04 x64"),
            ("__meta_digitalocean_private_ipv4", ""),
            ("__meta_digitalocean_public_ipv4", "104.131.186.241"),
            ("__meta_digitalocean_public_ipv6", ""),
            ("__meta_digitalocean_region", "nyc3"),
            ("__meta_digitalocean_size", "s-1vcpu-1gb"),
            ("__meta_digitalocean_status", "active"),
            ("__meta_digitalocean_vpc", "f9b0769c-e118-42fb-a0c4-fed15ef69662"),
            ("__meta_digitalocean_tags", ",monitor,"),
            ("__meta_digitalocean_features", ",virtio,"),
        ]),
        ls(&[
            ("__address__", "167.172.111.118:80"),
            ("__meta_digitalocean_droplet_id", "175072239"),
            ("__meta_digitalocean_droplet_name", "prometheus-demo-old"),
            ("__meta_digitalocean_image", "ubuntu-18-04-x64"),
            ("__meta_digitalocean_image_name", "18.04.3 (LTS) x64"),
            ("__meta_digitalocean_private_ipv4", "10.135.64.211"),
            ("__meta_digitalocean_public_ipv4", "167.172.111.118"),
            ("__meta_digitalocean_public_ipv6", ""),
            ("__meta_digitalocean_region", "fra1"),
            ("__meta_digitalocean_size", "s-1vcpu-1gb"),
            ("__meta_digitalocean_status", "off"),
            ("__meta_digitalocean_vpc", "953d698c-dc84-11e8-80bc-3cfdfea9fba1"),
            ("__meta_digitalocean_features", ",ipv6,private_networking,"),
        ]),
        ls(&[
            ("__address__", "138.65.56.69:80"),
            ("__meta_digitalocean_droplet_id", "176011507"),
            ("__meta_digitalocean_droplet_name", "prometheus-demo"),
            ("__meta_digitalocean_image", "ubuntu-18-04-x64"),
            ("__meta_digitalocean_image_name", "18.04.3 (LTS) x64"),
            ("__meta_digitalocean_private_ipv4", "10.135.64.212"),
            ("__meta_digitalocean_public_ipv4", "138.65.56.69"),
            ("__meta_digitalocean_public_ipv6", "2a03:b0c0:3:f0::cf2:4"),
            ("__meta_digitalocean_region", "fra1"),
            ("__meta_digitalocean_size", "s-1vcpu-1gb"),
            ("__meta_digitalocean_status", "active"),
            ("__meta_digitalocean_vpc", "953d698c-dc84-11e8-80bc-3cfdfea9fba1"),
            ("__meta_digitalocean_features", ",ipv6,private_networking,"),
        ]),
    ];
    for (i, lbls) in expected.iter().enumerate() {
        assert_eq!(lbls, &tg.targets[i], "item {i}");
    }

    metrics.unregister();
    refresh_metrics.unregister();
    sdmock.shutdown_server();
}
