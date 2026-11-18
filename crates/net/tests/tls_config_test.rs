//! Ported from prometheus/common v0.72.1 config/tls_config_test.go (JSON marshal of TLSVersion not ported;
//! the .json fixtures are decoded strictly with the YAML decoder, as Go's DisallowUnknownFields does).
mod common;

use common::setup;
use net::config::{TLS10, TLS11, TLS12, TLS13, TlsConfig, TlsVersion};
use net::tls::{TlsClientConfig, new_tls_config};

fn load_tls_config(filename: &str) -> Result<TlsClientConfig, String> {
    let content = std::fs::read_to_string(filename).map_err(|e| e.to_string())?;
    let cfg: TlsConfig = if filename.ends_with(".yml") || filename.ends_with(".json") {
        yaml::unmarshal_strict(&content).map_err(|e| e.to_string())?
    } else {
        return Err(format!("Unknown extension: {filename}"));
    };
    new_tls_config(&cfg, None)
}

#[test]
fn test_valid_tls_config() {
    setup();
    // (file, insecure, min, max)
    let cases: [(&str, bool, u16, u16); 10] = [
        ("tls_config.empty.good.json", false, 0, 0),
        ("tls_config.insecure.good.json", true, 0, 0),
        ("tls_config.tlsversion.good.json", false, TLS11, 0),
        ("tls_config.max_version.good.json", false, 0, TLS12),
        ("tls_config.empty.good.yml", false, 0, 0),
        ("tls_config.insecure.good.yml", true, 0, 0),
        ("tls_config.tlsversion.good.yml", false, TLS11, 0),
        ("tls_config.max_version.good.yml", false, 0, TLS12),
        ("tls_config.max_and_min_version.good.yml", false, TLS11, TLS12),
        ("tls_config.max_and_min_version_same.good.yml", false, TLS12, TLS12),
    ];
    for (f, insecure, min, max) in cases {
        let got = load_tls_config(&format!("testdata/{f}")).unwrap_or_else(|e| panic!("Error parsing {f}: {e}"));
        assert_eq!((got.insecure_skip_verify, got.min_version, got.max_version), (insecure, min, max), "{f}: unexpected config result");
        assert!(got.root_cas.is_none() && got.server_name.is_empty() && got.client_cert.is_none(), "{f}");
    }
}

#[test]
fn test_invalid_tls_config() {
    setup();
    let e = load_tls_config("testdata/tls_config.max_and_min_version.bad.yml").expect_err("Expected error with config but got none");
    assert!(e.contains("tls_config.max_version must be greater than or equal to tls_config.min_version if both are specified"), "{e}");
}

#[test]
fn test_tls_version_stringer() {
    assert_eq!("TLS13", TlsVersion(TLS13).go_string());
}

#[test]
fn test_tls_version_stringer_unknown() {
    assert_eq!("39321", TlsVersion(0x9999).go_string());
}

#[test]
fn test_tls_version_marshal_yaml() {
    assert_eq!(yaml::marshal(&TlsVersion(TLS13)).unwrap(), "TLS13\n");
    assert_eq!(yaml::marshal(&TlsVersion(TLS10)).unwrap(), "TLS10\n");
    assert_eq!(yaml::marshal(&TlsVersion(999)).unwrap_err(), "unknown TLS version: 999");
}
