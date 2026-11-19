//! Port of `cmd/prometheus/features_test.go`.

mod common;

use std::collections::BTreeMap;

use common::*;
use server::features::{FeatureOptions, set_feature_list_options};

type Features = BTreeMap<String, BTreeMap<String, bool>>;

fn to_features(j: &json::Json) -> Features {
    let mut out = Features::new();
    for (cat, m) in j.as_obj().expect("object") {
        let mut inner = BTreeMap::new();
        for (k, v) in m.as_obj().expect("object") {
            inner.insert(k.clone(), v.as_bool().expect("bool"));
        }
        out.insert(cat.clone(), inner);
    }
    out
}

#[test]
fn test_features_api() {
    let tmp_dir = TempDir::new();
    let config_file = tmp_dir.join("prometheus.yml");
    std::fs::write(&config_file, b"").unwrap();

    let port = random_port();
    let _prom =
        prometheus_command_with_logging(&config_file, port, &[format!("--storage.tsdb.path={}", tmp_dir.path().display())]);

    let base_url = format!("http://127.0.0.1:{port}");

    wait_for_prometheus_ready(port);

    // Fetch features from the API.
    let resp = get(&format!("{base_url}/api/v1/features")).unwrap();
    assert_eq!(200, resp.status);

    // Parse API response.
    let api_response = json::Json::parse(&resp.body).unwrap();
    assert_eq!(Some("success"), api_response.get("status").and_then(|s| s.as_str()));
    let data = to_features(api_response.get("data").expect("data"));

    // adapted: Go's -update-features flag (rewriting the golden file) is not ported; the golden file is upstream's.
    let golden_path = cmd_dir().join("testdata/features.json");

    // Load golden file.
    let golden_data = std::fs::read_to_string(&golden_path)
        .unwrap_or_else(|e| panic!("Failed to read golden file {}: {e}", golden_path.display()));
    let mut expected_features = to_features(&json::Json::parse(&golden_data).unwrap());

    // The labels implementation depends on build tags (stringlabels, slicelabels, or dedupelabels).
    // We need to update the expected features to match the current build.
    if let Some(prometheus_features) = expected_features.get_mut("prometheus") {
        // Remove all label implementation features from expected.
        prometheus_features.remove("stringlabels");
        prometheus_features.remove("slicelabels");
        prometheus_features.remove("dedupelabels");
        // Add the current implementation.
        if let Some(actual_prometheus) = data.get("prometheus") {
            for imp in ["stringlabels", "slicelabels", "dedupelabels"] {
                if actual_prometheus.get(imp).copied().unwrap_or(false) {
                    prometheus_features.insert(imp.to_string(), true);
                }
            }
        }
    }

    // Compare the features data with the golden file.
    assert_eq!(expected_features, data, "Features mismatch.");
}

/// TestSetFeatureListOptions_MetadataWALRecords is a regression test: enabling
/// metadata-wal-records used to only flip the scrape and web AppendMetadata
/// options, never tsdb.EnableMetadataWALRecords.
#[test]
fn test_set_feature_list_options_metadata_wal_records() {
    let mut c = FeatureOptions::default();
    set_feature_list_options(&mut c, &["metadata-wal-records".to_string()], &discovery::Logger::nop()).unwrap();

    assert!(c.scrape_append_metadata);
    assert!(c.web_append_metadata);
    assert!(c.tsdb_enable_metadata_wal_records);
    assert!(c.agent_enable_metadata_wal_records);
}

#[test]
fn test_set_feature_list_options_unquoted_utf8_names() {
    for enabled in [false, true] {
        let mut c = FeatureOptions::default();
        let feature_list: Vec<String> =
            if enabled { vec!["promql-unquoted-utf8-names".to_string()] } else { Vec::new() };
        set_feature_list_options(&mut c, &feature_list, &discovery::Logger::nop()).unwrap();
        assert_eq!(enabled, c.parser_options().enable_unquoted_utf8_names);
    }
}
