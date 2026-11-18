//! Differential check against go.yaml.in/yaml/v2 v2.4.4: each input in testdata/go_compat was decoded into
//! `interface{}` by Go (`Unmarshal` and `UnmarshalStrict`) and marshaled back; the expected files hold Go's
//! output ("ERR: <error>" or the YAML text). Generated with bench/tmp/w0-net/gocmp (not a ported upstream test).

fn dir() -> String {
    format!("{}/tests/testdata/go_compat", env!("CARGO_MANIFEST_DIR"))
}

fn run(expected_file: &str, strict: bool) {
    let expected = std::fs::read_to_string(format!("{}/{}", dir(), expected_file)).unwrap();
    let mut got = String::new();
    let mut names = Vec::new();
    for line in expected.lines() {
        if let Some(n) = line.strip_prefix("=== in/") {
            names.push(n.to_string());
        }
    }
    assert!(names.len() > 20);
    for n in names {
        let text = std::fs::read_to_string(format!("{}/{}", dir(), n)).unwrap();
        got.push_str(&format!("=== in/{n}\n"));
        let r: Result<yaml::Value, _> = if strict { yaml::unmarshal_strict(&text) } else { yaml::unmarshal(&text) };
        match r {
            Err(e) => got.push_str(&format!("ERR: {e}\n")),
            Ok(v) => got.push_str(&yaml::marshal(&v).unwrap()),
        }
    }
    let (e, g): (Vec<&str>, Vec<&str>) = (expected.lines().collect(), got.lines().collect());
    for (i, (a, b)) in e.iter().zip(g.iter()).enumerate() {
        assert_eq!(a, b, "line {}", i + 1);
    }
    assert_eq!(e.len(), g.len());
}

#[test]
fn go_compat_unmarshal_marshal() {
    run("expected.txt", false);
}

#[test]
fn go_compat_unmarshal_strict_marshal() {
    run("expected_strict.txt", true);
}
