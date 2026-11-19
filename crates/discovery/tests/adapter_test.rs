//! Port of `documentation/examples/custom-sd/adapter/adapter_test.go`.

use std::collections::BTreeMap;
use std::sync::Arc;

use discovery::adapter::{Adapter, CustomSd, generate_target_groups};
use discovery::manager::TargetSets;
use discovery::{CancelToken, Group, Logger, Registry, SdMetrics, new_refresh_metrics, register_sd_metrics};
use model::labelset::{ADDRESS_LABEL, LabelSet};

fn group(source: &str, addrs: &[&str], labels: &[(&str, &str)]) -> Arc<Group> {
    Arc::new(Group {
        source: source.to_string(),
        targets: addrs.iter().map(|a| LabelSet::from_pairs(&[(ADDRESS_LABEL, a)])).collect(),
        labels: LabelSet::from_pairs(labels),
    })
}

fn sd(targets: &[&str], labels: &[(&str, &str)]) -> CustomSd {
    CustomSd {
        targets: targets.iter().map(|t| t.to_string()).collect(),
        labels: labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
    }
}

/// Checks that the target is correctly generated, also when the target is empty.
#[test]
fn test_generate_target_groups() {
    let l1 = [("__meta_test_label", "label_test_1")];
    let l2 = [("__meta_test_label", "label_test_2")];
    let test_cases: Vec<(&str, TargetSets, BTreeMap<String, CustomSd>)> = vec![
        (
            "Empty targetGroup",
            BTreeMap::from([("customSD".to_string(), vec![group("Consul", &[], &[]), group("Kubernetes", &[], &[])])]),
            BTreeMap::from([
                ("customSD:Consul:0000000000000000".to_string(), sd(&[], &[])),
                ("customSD:Kubernetes:0000000000000000".to_string(), sd(&[], &[])),
            ]),
        ),
        (
            "targetGroup filled",
            BTreeMap::from([(
                "customSD".to_string(),
                vec![group("Azure", &["host1", "host2"], &l1), group("Openshift", &["host3", "host4"], &l2)],
            )]),
            BTreeMap::from([
                ("customSD:Azure:282a007a18fadbbb".to_string(), sd(&["host1", "host2"], &l1)),
                ("customSD:Openshift:281c007a18ea2ad0".to_string(), sd(&["host3", "host4"], &l2)),
            ]),
        ),
        (
            "Mixed between empty targetGroup and targetGroup filled",
            BTreeMap::from([(
                "customSD".to_string(),
                vec![group("GCE", &["host1", "host2"], &l1), group("Kubernetes", &[], &l2)],
            )]),
            BTreeMap::from([
                ("customSD:GCE:282a007a18fadbbb".to_string(), sd(&["host1", "host2"], &l1)),
                ("customSD:Kubernetes:282e007a18fad483".to_string(), sd(&[], &l2)),
            ]),
        ),
        (
            "Disordered Ips in Alibaba's application management system",
            BTreeMap::from([
                ("cart".to_string(), vec![group("alibaba", &["192.168.1.55", "192.168.1.44"], &l1)]),
                ("buy".to_string(), vec![group("alibaba", &["192.168.1.22", "192.168.1.33"], &l1)]),
            ]),
            BTreeMap::from([
                ("buy:alibaba:21c0d97a1e27e6fe".to_string(), sd(&["192.168.1.22", "192.168.1.33"], &l1)),
                ("cart:alibaba:1112e97a13b159fa".to_string(), sd(&["192.168.1.44", "192.168.1.55"], &l1)),
            ]),
        ),
    ];
    for (title, target_group, expected) in test_cases {
        let result = generate_target_groups(&target_group);
        assert_eq!(expected, result, "{title}");
    }
}

/// Checks the adapter can write a file to disk.
#[test]
fn test_write_output() {
    let ctx = CancelToken::new();
    let dir = std::env::temp_dir();
    let path = dir.join(format!("sd_adapter_test{}", std::process::id()));
    std::fs::write(&path, b"").expect("create temp file");
    struct Remove(std::path::PathBuf);
    impl Drop for Remove {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let _cleanup = Remove(path.clone());

    let reg = Registry::new();
    let refresh_metrics = new_refresh_metrics(&reg);
    let mechanism_metrics = register_sd_metrics(&reg, refresh_metrics.clone()).expect("RegisterSDMetrics");
    let sd_metrics = SdMetrics { mechanism_metrics, refresh_manager: refresh_metrics };

    let adapter = Adapter::new(&ctx, path.to_str().unwrap(), "test_sd", None, Logger::nop(), &sd_metrics, &reg).expect("NewAdapter");
    adapter.write_output().expect("writeOutput");
    // adapted: Go only checks the error; the Rust test also checks the file holds the empty JSON array.
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "[]");
}
