//! Port of cmd/promtool/sd_test.go.

use std::collections::BTreeMap;

use config::ScrapeConfig;
use discovery::Group;
use model::labels::{ValidationScheme, from_strings};
use model::relabel::{self, Action, Regexp};
use promtool::sd::{SdCheckResult, get_sd_check_result};

#[test]
fn test_sd_check_result() {
    let mut target = BTreeMap::new();
    target.insert("__address__".to_string(), "localhost:8080".to_string());
    target.insert("foo".to_string(), "bar".to_string());
    let target_groups = vec![Group { targets: vec![target.into_iter().collect()], ..Default::default() }];

    let reg = Regexp::new("(.*)").expect("regexp");
    let scrape_config = ScrapeConfig {
        scrape_interval: yaml::Duration::from_secs(60),
        scrape_timeout: yaml::Duration::from_secs(10),
        relabel_configs: vec![relabel::Config {
            source_labels: Some(vec!["foo".to_string()]),
            action: Action::Replace,
            target_label: "newfoo".to_string(),
            regex: reg,
            replacement: "$1".to_string(),
            name_validation_scheme: ValidationScheme::Utf8,
            ..Default::default()
        }],
        ..Default::default()
    };

    let expected = vec![SdCheckResult {
        discovered_labels: from_strings(&[
            "__address__",
            "localhost:8080",
            "__always_scrape_classic_histograms__",
            "false",
            "__convert_classic_histograms_to_nhcb__",
            "false",
            "__scrape_interval__",
            "1m",
            "__scrape_native_histograms__",
            "false",
            "__scrape_timeout__",
            "10s",
            "foo",
            "bar",
        ]),
        labels: from_strings(&[
            "__address__",
            "localhost:8080",
            "__always_scrape_classic_histograms__",
            "false",
            "__convert_classic_histograms_to_nhcb__",
            "false",
            "__scrape_interval__",
            "1m",
            "__scrape_native_histograms__",
            "false",
            "__scrape_timeout__",
            "10s",
            "foo",
            "bar",
            "instance",
            "localhost:8080",
            "newfoo",
            "bar",
        ]),
        error: None,
    }];
    assert_eq!(expected, get_sd_check_result(&target_groups, &scrape_config));
}
