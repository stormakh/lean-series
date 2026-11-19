// Port of notifier/alertmanager_test.go.

use std::sync::Arc;

use config::{default_alertmanager_config, AlertmanagerConfig, ALERTMANAGER_API_VERSION_V2};
use discovery::metrics::Registry;
use discovery::{Group, Logger};
use model::labelset::{LabelSet, ADDRESS_LABEL};
use notifier::{alertmanager_from_group, post_path, AlertMetrics, AlertmanagerSet, Options, DEFAULT_MAX_BATCH_SIZE};

#[test]
fn test_post_path() {
    let cases = [("", "/api/v2/alerts"), ("/", "/api/v2/alerts"), ("/prefix", "/prefix/api/v2/alerts"), ("/prefix//", "/prefix/api/v2/alerts"), ("prefix//", "/prefix/api/v2/alerts")];
    for (input, out) in cases {
        assert_eq!(out, post_path(input, ALERTMANAGER_API_VERSION_V2));
    }
}

pub fn make_input_target_group() -> Group {
    Group {
        targets: vec![LabelSet::from_pairs(&[(ADDRESS_LABEL, "1.1.1.1:9090"), ("notcommon1", "label")])],
        labels: LabelSet::from_pairs(&[("common", "label")]),
        source: "testsource".to_string(),
    }
}

#[test]
fn test_label_set_not_reused() {
    let tg = make_input_target_group();
    let res = alertmanager_from_group(&tg, &AlertmanagerConfig::default());
    assert!(res.is_ok());
    // Target modified during alertmanager extraction.
    assert_eq!(tg, make_input_target_group());
}

/// Verifies that sync properly manages the send loop lifecycle: starts loops for new Alertmanagers, stops
/// loops of removed ones, keeps loops still in use and does not stop loops that were just created.
#[test]
fn test_alertmanager_set_sync() {
    let reg = Registry::new();
    let metrics = Arc::new(AlertMetrics::new(Some(&reg), Box::new(|| 0.0)));
    let opts = Arc::new(Options { queue_capacity: 10, max_batch_size: DEFAULT_MAX_BATCH_SIZE, ..Default::default() });
    let cfg = default_alertmanager_config();

    let ams = AlertmanagerSet::new(cfg, opts, Logger::nop(), metrics).expect("new set");

    struct Cleanup<'a>(&'a AlertmanagerSet);
    impl Drop for Cleanup<'_> {
        fn drop(&mut self) {
            self.0.sync(&[]);
            if !std::thread::panicking() {
                assert!(self.0.send_loops().is_empty(), "All sendloops should be cleaned up");
            }
        }
    }
    let _cleanup = Cleanup(&ams);

    let tg = |addrs: &[&str]| vec![Arc::new(Group { targets: addrs.iter().map(|a| LabelSet::from_pairs(&[(ADDRESS_LABEL, a)])).collect(), ..Default::default() })];
    const AM1: &str = "http://am1.example.com:9093/api/v2/alerts";
    const AM2: &str = "http://am2.example.com:9093/api/v2/alerts";
    const AM3: &str = "http://am3.example.com:9093/api/v2/alerts";

    // First sync: add AM1 and AM2.
    ams.sync(&tg(&["am1.example.com:9093", "am2.example.com:9093"]));
    let loops = ams.send_loops();
    assert_eq!(loops.len(), 2, "AM1 and AM2 sendloops should be created");
    assert!(loops.contains_key(AM1), "AM1 sendloop should be created");
    assert!(loops.contains_key(AM2), "AM2 sendloop should be created");
    let am2_loop = loops[AM2].clone();

    // Second sync: keep AM2, remove AM1, add AM3.
    ams.sync(&tg(&["am2.example.com:9093", "am3.example.com:9093"]));
    let loops = ams.send_loops();
    assert_eq!(loops.len(), 2);
    assert!(!loops.contains_key(AM1), "AM1 sendloop should be removed");
    assert!(loops.contains_key(AM2), "AM2 sendloop should be kept");
    assert!(loops.contains_key(AM3), "AM3 sendloop should be created");
    assert!(Arc::ptr_eq(&am2_loop, &loops[AM2]), "AM2 sendloop should not be recreated");
    let am3_loop = loops[AM3].clone();

    // Third sync: keep only AM3, remove AM2.
    ams.sync(&tg(&["am3.example.com:9093"]));
    let loops = ams.send_loops();
    assert_eq!(loops.len(), 1);
    assert!(!loops.contains_key(AM2), "AM2 sendloop should be removed");
    assert!(loops.contains_key(AM3), "AM3 sendloop should be kept");
    assert!(Arc::ptr_eq(&am3_loop, &loops[AM3]), "AM3 sendloop should not be recreated");
}
