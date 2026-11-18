//! Differential check against prometheus/common v0.72.1: each input in testdata/go_compat was loaded by Go
//! `config.LoadHTTPConfig` and printed with `HTTPClientConfig.String()` (YAML) or as "ERR: <error>".
//! Generated with bench/tmp/w0-net/gen_expected.sh (not a ported upstream test).

#[test]
fn go_compat_load_http_config() {
    let dir = format!("{}/tests/testdata/go_compat", env!("CARGO_MANIFEST_DIR"));
    let expected = std::fs::read_to_string(format!("{dir}/expected.txt")).unwrap();
    let mut got = String::new();
    for line in expected.lines() {
        if let Some(n) = line.strip_prefix("=== ") {
            let text = std::fs::read_to_string(format!("{dir}/{n}")).unwrap();
            got.push_str(&format!("=== {n}\n"));
            match net::load_http_config(&text) {
                Err(e) => got.push_str(&format!("ERR: {e}\n")),
                Ok(c) => got.push_str(&c.to_yaml_string()),
            }
        }
    }
    let (e, g): (Vec<&str>, Vec<&str>) = (expected.lines().collect(), got.lines().collect());
    for (i, (a, b)) in e.iter().zip(g.iter()).enumerate() {
        assert_eq!(a, b, "line {}", i + 1);
    }
    assert_eq!(e.len(), g.len());
}
