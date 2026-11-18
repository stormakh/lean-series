//! Ported from prometheus/common v0.72.1 config/config_test.go (the YAML parts; JSON marshaling of these
//! types is not used by Prometheus and not ported).
use std::collections::BTreeMap;

use net::config::ProxyHeader;
use yaml::Secret;

#[derive(Default)]
struct Tmp {
    s: Secret,
}
yaml::yaml_struct!(Tmp, "config.tmp", [(s: "s")]);
yaml::impl_plain_struct!(Tmp);

/// MarshalSecretValue is a global: tests that marshal secrets take this lock (Go runs them one after another).
static SECRET_FLAG: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn test_json_marshal_secret() {
    let _g = SECRET_FLAG.lock().unwrap_or_else(|p| p.into_inner());
    // only the "true value in YAML" case is a YAML case; the others test encoding/json
    yaml::MARSHAL_SECRET_VALUE.store(true, std::sync::atomic::Ordering::SeqCst);
    let c = yaml::marshal(&Tmp { s: Secret::new("test") }).unwrap();
    yaml::MARSHAL_SECRET_VALUE.store(false, std::sync::atomic::Ordering::SeqCst);
    assert_eq!("s: test\n", c, "Secret not marshaled correctly");
    // with MarshalSecretValue false the secret is hidden
    assert_eq!("s: <secret>\n", yaml::marshal(&Tmp { s: Secret::new("test") }).unwrap());
}

fn ph(pairs: &[(&str, &[&str])]) -> ProxyHeader {
    pairs.iter().map(|(k, vs)| (k.to_string(), vs.iter().map(|v| Secret::new(*v)).collect())).collect()
}

// adapted: Go's http.Header keeps keys with empty or nil value lists; net::Header holds (key, value) pairs, so
// the check is on the values per key.
#[test]
fn test_header_http_header() {
    let pc = net::config::ProxyConfig { proxy_connect_header: ph(&[("single", &["v1"]), ("multi", &["v1", "v2"]), ("empty", &[]), ("nil", &[])]), ..Default::default() };
    let h = pc.get_proxy_connect_header();
    assert_eq!(h.values("single"), vec!["v1"]);
    assert_eq!(h.values("multi"), vec!["v1", "v2"]);
    assert!(h.values("empty").is_empty());
    assert!(h.values("nil").is_empty());
    let pc = net::config::ProxyConfig::default();
    assert!(pc.get_proxy_connect_header().is_empty());
}

#[test]
fn test_header_yaml_unmarshal() {
    let cases: [(&str, &str, Option<ProxyHeader>); 5] = [
        ("void", "", None),
        ("simple", "single:\n- a\n", Some(ph(&[("single", &["a"])]))),
        ("multi", "multi:\n- a\n- b\n", Some(ph(&[("multi", &["a", "b"])]))),
        ("empty", "{}", Some(ProxyHeader::new())),
        ("empty value", "empty:\n", Some(ph(&[("empty", &[])]))),
    ];
    for (name, input, expected) in cases {
        let actual: ProxyHeader = yaml::unmarshal(input).unwrap_or_else(|e| panic!("error unmarshaling {input}: {e}"));
        assert_eq!(actual, expected.unwrap_or_default(), "{name}");
    }
}

#[test]
fn test_header_yaml_marshal() {
    let _g = SECRET_FLAG.lock().unwrap_or_else(|p| p.into_inner());
    let cases: [(&str, ProxyHeader, &str); 4] = [
        ("void", BTreeMap::new(), "{}\n"),
        ("simple", ph(&[("single", &["a"])]), "single:\n- <secret>\n"),
        ("multi", ph(&[("multi", &["a", "b"])]), "multi:\n- <secret>\n- <secret>\n"),
        ("empty", ph(&[("empty", &[])]), "empty: []\n"),
    ];
    for (name, input, expected) in cases {
        assert_eq!(yaml::marshal(&input).unwrap(), expected, "{name}");
    }
}

// adapted: JSON input decoded with the YAML decoder (JSON is YAML).
#[test]
fn test_header_json_unmarshal() {
    let cases: [(&str, &str, ProxyHeader); 5] = [
        ("void", "null", ProxyHeader::new()),
        ("simple", r#"{"single": ["a"]}"#, ph(&[("single", &["a"])])),
        ("multi", r#"{"multi": ["a", "b"]}"#, ph(&[("multi", &["a", "b"])])),
        ("empty", "{}", ProxyHeader::new()),
        ("empty value", r#"{"empty":null}"#, ph(&[("empty", &[])])),
    ];
    for (name, input, expected) in cases {
        let actual: ProxyHeader = yaml::unmarshal(input).unwrap_or_else(|e| panic!("error unmarshaling {input}: {e}"));
        assert_eq!(actual, expected, "{name}");
    }
}
