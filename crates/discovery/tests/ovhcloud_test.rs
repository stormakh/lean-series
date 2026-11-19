//! Port of discovery/ovhcloud/ovhcloud_test.go.
mod common;

use discovery::Config;
use discovery::ovhcloud::{SdConfig, create_client, parse_ip_list};

pub const OVHCLOUD_APPLICATION_KEY_TEST: &str = "TDPKJdwZwAQPwKX2";
pub const OVHCLOUD_APPLICATION_SECRET_TEST: &str = "9ufkBmLaTQ9nz5yMUlg79taH0GNnzDjk";
pub const OVHCLOUD_CONSUMER_KEY_TEST: &str = "5mBuy6SUQcRw2ZUxg0cG68BoDKpED4KY";

const MOCK_URL: &str = "https://localhost:1234";

fn get_mock_conf(service: &str) -> (SdConfig, Result<(), yaml::Error>) {
    let conf_string = format!(
        "\nendpoint: {MOCK_URL}\napplication_key: {OVHCLOUD_APPLICATION_KEY_TEST}\napplication_secret: {OVHCLOUD_APPLICATION_SECRET_TEST}\nconsumer_key: {OVHCLOUD_CONSUMER_KEY_TEST}\nrefresh_interval: 1m\nservice: {service}\n"
    );
    get_mock_conf_from_string(&conf_string)
}

fn get_mock_conf_from_string(conf_string: &str) -> (SdConfig, Result<(), yaml::Error>) {
    let mut conf = SdConfig::default();
    let err = yaml::unmarshal_into(conf_string, &mut conf, true);
    (conf, err)
}

#[test]
fn test_error_init_client() {
    let conf_string = format!("\nendpoint: {MOCK_URL}\n\n");
    let (conf, _) = get_mock_conf_from_string(&conf_string);
    let err = create_client(&conf).err().expect("an error");
    assert!(err.contains("missing authentication information"), "{err}");
}

#[test]
fn test_parse_i_ps() {
    let test_cases: &[(&str, &[&str], Option<&str>)] = &[
        ("Parse IPv4 failed.", &["A.b"], Some("could not parse IP addresses from list")),
        ("Parse unspecified failed.", &["0.0.0.0"], Some("could not parse IP addresses from list")),
        ("Parse void IP failed.", &[""], Some("could not parse IP addresses from list")),
        ("Parse IPv6 ok.", &["2001:0db8:0000:0000:0000:0000:0000:0001"], None),
        ("Parse IPv6 failed.", &["bbb:cccc:1111"], Some("could not parse IP addresses from list")),
        ("Parse IPv4 bad mask.", &["192.0.2.1/23"], Some("could not parse IP addresses from list")),
        ("Parse IPv4 ok.", &["192.0.2.1/32"], None),
    ];
    for (name, input, want) in test_cases {
        let got = parse_ip_list(input).err();
        assert_eq!(got.as_deref(), *want, "{name}");
    }
}

#[test]
fn test_discoverer() {
    let (conf, _) = get_mock_conf("vps");
    let (opts, _reg) = common::new_options(&conf);
    let r = conf.new_discoverer(discovery::DiscovererOptions { set_name: String::new(), ..opts });
    assert!(r.is_ok(), "{:?}", r.err());
}
