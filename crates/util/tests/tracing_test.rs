//! Port of upstream/tracing/tracing_test.go.
//!
//! adapted: Go reads the global `otel.GetTracerProvider()`; here each manager owns its provider
//! (`Manager::tracer_provider`), because Rust tests run in parallel threads of one process.
use config::{TRACING_CLIENT_GRPC, TRACING_CLIENT_HTTP, TracingClientType, TracingConfig};
use std::collections::BTreeMap;
use util::logging::Logger;
use util::tracing::{Manager, TracerProvider};

/// How the server applies the `tracing` section: Go `getClient` loads the TLS config when not insecure.
fn apply(m: &mut Manager<TracingConfig>, cfg: &TracingConfig) -> Result<(), String> {
    let tls_set = cfg.tls_config != net::TlsConfig::default();
    m.apply_config(cfg, &cfg.endpoint, tls_set, || {
        if !cfg.insecure {
            net::tls::new_tls_config(&cfg.tls_config, None).map(|_| ())?;
        }
        Ok(())
    })
}

fn client(t: &str) -> TracingClientType {
    TracingClientType(t.to_string())
}

#[test]
fn test_installing_new_tracer_provider() {
    let mut m = Manager::new(Logger::nop());
    let tp_before = m.tracer_provider();

    let cfg = TracingConfig { endpoint: "localhost:1234".to_string(), client_type: client(TRACING_CLIENT_GRPC), ..Default::default() };

    assert!(apply(&mut m, &cfg).is_ok());
    assert_ne!(tp_before, m.tracer_provider());
}

#[test]
fn test_reinstalling_tracer_provider() {
    let mut m = Manager::new(Logger::nop());
    let cfg = TracingConfig {
        endpoint: "localhost:1234".to_string(),
        client_type: client(TRACING_CLIENT_GRPC),
        headers: BTreeMap::from([("foo".to_string(), "bar".to_string())]),
        ..Default::default()
    };

    assert!(apply(&mut m, &cfg).is_ok());
    let tp_first_config = m.tracer_provider();

    // Trying to apply the same config should not reinstall provider.
    assert!(apply(&mut m, &cfg).is_ok());
    assert_eq!(tp_first_config, m.tracer_provider());

    let mut cfg2 = TracingConfig {
        endpoint: "localhost:1234".to_string(),
        client_type: client(TRACING_CLIENT_HTTP),
        headers: BTreeMap::from([("bar".to_string(), "foo".to_string())]),
        ..Default::default()
    };

    assert!(apply(&mut m, &cfg2).is_ok());
    assert_ne!(tp_first_config, m.tracer_provider());
    let tp_second_config = m.tracer_provider();

    // Setting previously unset option should reinstall provider.
    cfg2.compression = "gzip".to_string();
    assert!(apply(&mut m, &cfg2).is_ok());
    assert_ne!(tp_second_config, m.tracer_provider());
}

#[test]
fn test_reinstalling_tracer_provider_with_tls() {
    let mut m = Manager::new(Logger::nop());
    let ca_file = format!("{}/../../upstream/tracing/testdata/ca.cer", env!("CARGO_MANIFEST_DIR"));
    let cfg = TracingConfig {
        endpoint: "localhost:1234".to_string(),
        client_type: client(TRACING_CLIENT_GRPC),
        tls_config: net::TlsConfig { ca_file, ..Default::default() },
        ..Default::default()
    };

    let r = apply(&mut m, &cfg);
    assert!(r.is_ok(), "{r:?}");
    let tp_first_config = m.tracer_provider();

    // Trying to apply the same config with TLS should reinstall provider.
    assert!(apply(&mut m, &cfg).is_ok());
    assert_ne!(tp_first_config, m.tracer_provider());
}

#[test]
fn test_uninstalling_tracer_provider() {
    let mut m = Manager::new(Logger::nop());
    let cfg = TracingConfig { endpoint: "localhost:1234".to_string(), client_type: client(TRACING_CLIENT_GRPC), ..Default::default() };

    assert!(apply(&mut m, &cfg).is_ok());
    assert_ne!(TracerProvider::Noop, m.tracer_provider());

    // Uninstall by passing empty config.
    let cfg2 = TracingConfig::default();

    assert!(apply(&mut m, &cfg2).is_ok());
    // Make sure we get a no-op tracer provider after uninstallation.
    assert_eq!(TracerProvider::Noop, m.tracer_provider());
}

#[test]
fn test_installing_tracer_provider_http_insecure() {
    let mut m = Manager::new(Logger::nop());
    let cfg = TracingConfig {
        endpoint: "localhost:4318".to_string(),
        client_type: client(TRACING_CLIENT_HTTP),
        insecure: true,
        ..Default::default()
    };
    assert!(apply(&mut m, &cfg).is_ok());
}

#[test]
fn test_tracer_provider_shutdown() {
    let mut m = Manager::new(Logger::nop());
    let cfg = TracingConfig { endpoint: "localhost:1234".to_string(), client_type: client(TRACING_CLIENT_GRPC), ..Default::default() };

    assert!(apply(&mut m, &cfg).is_ok());
    m.stop();

    // Check if we closed the done channel.
    assert!(m.is_done());
}
