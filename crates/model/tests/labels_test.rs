// Port of model/labels/labels_test.go (stringlabels build).

use std::collections::HashMap;

use model::labels::{
    compare, empty_labels, equal, from_map, from_strings, size_of_labels, Builder, Label, Labels,
    ScratchBuilder, ValidationScheme, METRIC_NAME,
};

fn s254() -> String {
    "x".repeat(254)
}
fn s255() -> String {
    "x".repeat(255)
}

fn test_case_labels() -> Vec<Labels> {
    vec![
        from_strings(&["t1", "t1", "t2", "t2"]),
        Labels::default(),
        from_strings(&["service.name", "t1", "whatever\\whatever", "t2"]),
        from_strings(&["aaa", "111", "xx", &s254()]),
        from_strings(&["aaa", "111", "xx", &s255()]),
        from_strings(&[
            "__name__",
            "kube_pod_container_status_last_terminated_exitcode",
            "cluster",
            "prod-af-north-0",
            " container",
            "prometheus",
            "instance",
            "kube-state-metrics-0:kube-state-metrics:ksm",
            "job",
            "kube-state-metrics/kube-state-metrics",
            " namespace",
            "observability-prometheus",
            "pod",
            "observability-prometheus-0",
            "uid",
            "d3ec90b2-4975-4607-b45d-b9ad64bb417e",
        ]),
    ]
}

// Values from labels_stringlabels_test.go; they must line up with test_case_labels.
const EXPECTED_SIZE_OF_LABELS: [u64; 6] = [12, 0, 37, 266, 270, 309];
const EXPECTED_BYTE_SIZE: [u64; 6] = [12, 0, 37, 266, 270, 309];

#[test]
fn test_labels_string() {
    let expected = vec![
        "{t1=\"t1\", t2=\"t2\"}".to_string(),
        "{}".to_string(),
        r#"{"service.name"="t1", "whatever\\whatever"="t2"}"#.to_string(),
        format!(r#"{{aaa="111", xx="{}"}}"#, s254()),
        format!(r#"{{aaa="111", xx="{}"}}"#, s255()),
        r#"{" container"="prometheus", " namespace"="observability-prometheus", __name__="kube_pod_container_status_last_terminated_exitcode", cluster="prod-af-north-0", instance="kube-state-metrics-0:kube-state-metrics:ksm", job="kube-state-metrics/kube-state-metrics", pod="observability-prometheus-0", uid="d3ec90b2-4975-4607-b45d-b9ad64bb417e"}"#.to_string(),
    ];
    let cases = test_case_labels();
    assert_eq!(expected.len(), cases.len());
    for (i, c) in expected.iter().enumerate() {
        assert_eq!(*c, cases[i].to_string());
    }
}

#[test]
fn test_size_of_labels() {
    let cases = test_case_labels();
    assert_eq!(EXPECTED_SIZE_OF_LABELS.len(), cases.len());
    for (i, c) in EXPECTED_SIZE_OF_LABELS.iter().enumerate() {
        let mut total = 0;
        cases[i].range(|n, v| total += size_of_labels(n, v, 1));
        assert_eq!(*c, total, "unexpected size for test case {}: {}", i, cases[i]);
    }
}

#[test]
fn test_byte_size() {
    let cases = test_case_labels();
    assert_eq!(EXPECTED_BYTE_SIZE.len(), cases.len());
    for (i, c) in EXPECTED_BYTE_SIZE.iter().enumerate() {
        assert_eq!(*c, cases[i].byte_size(), "unexpected size for test case {}: {}", i, cases[i]);
    }
}

#[test]
fn test_labels_match_labels() {
    let labels = from_strings(&[
        "__name__",
        "ALERTS",
        "alertname",
        "HTTPRequestRateLow",
        "alertstate",
        "pending",
        "instance",
        "0",
        "job",
        "app-server",
        "severity",
        "critical",
    ]);
    let tests: Vec<(Vec<&str>, bool, Labels)> = vec![
        (
            vec!["__name__", "alertname", "alertstate", "instance"],
            true,
            from_strings(&["__name__", "ALERTS", "alertname", "HTTPRequestRateLow", "alertstate", "pending", "instance", "0"]),
        ),
        (
            vec!["__name__", "alertname", "alertstate", "instance"],
            false,
            from_strings(&["job", "app-server", "severity", "critical"]),
        ),
        (
            vec!["alertname", "alertstate", "instance"],
            true,
            from_strings(&["alertname", "HTTPRequestRateLow", "alertstate", "pending", "instance", "0"]),
        ),
        (
            vec!["alertname", "alertstate", "instance"],
            false,
            from_strings(&["job", "app-server", "severity", "critical"]),
        ),
    ];
    for (i, (names, on, expected)) in tests.iter().enumerate() {
        let got = labels.match_labels(*on, names);
        assert!(equal(expected, &got), "unexpected labelset for test case {}", i);
    }
}

#[test]
fn test_labels_has_duplicate_label_names() {
    let mut m = HashMap::new();
    m.insert("__name__", "up");
    m.insert("hostname", "localhost");
    let cases: Vec<(Labels, bool, &str)> = vec![
        (from_map(m), false, ""),
        (from_strings(&["__name__", "up", "hostname", "localhost", "hostname", "127.0.0.1"]), true, "hostname"),
    ];
    for (i, (input, dup, name)) in cases.iter().enumerate() {
        let r = input.has_duplicate_label_names();
        assert_eq!(*dup, r.is_some(), "test {}: incorrect duplicate bool", i);
        assert_eq!(*name, r.unwrap_or(""), "test {}: incorrect label name", i);
    }
}

#[test]
fn test_labels_without_empty() {
    let tests: Vec<(Labels, Labels)> = vec![
        (from_strings(&["foo", "", "bar", ""]), empty_labels()),
        (from_strings(&["foo", "", "bar", "", "baz", ""]), empty_labels()),
        (
            from_strings(&["__name__", "test", "hostname", "localhost", "job", "check"]),
            from_strings(&["__name__", "test", "hostname", "localhost", "job", "check"]),
        ),
        (
            from_strings(&["__name__", "test", "hostname", "localhost", "bar", "", "job", "check"]),
            from_strings(&["__name__", "test", "hostname", "localhost", "job", "check"]),
        ),
        (
            from_strings(&["__name__", "test", "foo", "", "hostname", "localhost", "bar", "", "job", "check"]),
            from_strings(&["__name__", "test", "hostname", "localhost", "job", "check"]),
        ),
        (
            from_strings(&["__name__", "test", "foo", "", "baz", "", "hostname", "localhost", "bar", "", "job", "check"]),
            from_strings(&["__name__", "test", "hostname", "localhost", "job", "check"]),
        ),
    ];
    for (input, expected) in tests {
        assert!(equal(&expected, &input.without_empty()));
    }
}

#[test]
fn test_labels_is_valid() {
    let tests: Vec<(Labels, bool)> = vec![
        (from_strings(&["__name__", "test", "hostname", "localhost", "job", "check"]), true),
        (from_strings(&["__name__", "test:ms", "hostname_123", "localhost", "_job", "check"]), true),
        (from_strings(&["__name__", "test-ms"]), false),
        (from_strings(&["__name__", "0zz"]), false),
        (from_strings(&["abc:xyz", "invalid"]), false),
        (from_strings(&["123abc", "invalid"]), false),
        (from_strings(&["中文abc", "invalid"]), false),
    ];
    for (input, expected) in tests {
        assert_eq!(expected, input.is_valid(ValidationScheme::Legacy), "{}", input);
    }
    // adapted: the Go cases {"invalid": "aa\xe2"} and {"invalid": "\xF7\xBF\xBF\xBF"} expect IsValid to reject
    // values that are not valid UTF-8. Rust label strings cannot hold such bytes: they are rejected when the bytes
    // are converted to a label string, which is the same rejection.
    assert!(std::str::from_utf8(std::hint::black_box(b"aa\xe2")).is_err());
    assert!(std::str::from_utf8(std::hint::black_box(b"\xF7\xBF\xBF\xBF")).is_err());
}

#[test]
fn test_labels_validation_modes() {
    let tests: Vec<(Labels, ValidationScheme, bool)> = vec![
        (from_strings(&["__name__", "test.metric", "hostname", "localhost", "job", "check"]), ValidationScheme::Utf8, true),
        (from_strings(&["__name__", "test", "host.name", "localhost", "team/name", "platform"]), ValidationScheme::Utf8, true),
        (
            from_strings(&["__name__", "test.utf8.metric", "hostname", "localhost", "job", "check"]),
            ValidationScheme::Legacy,
            false,
        ),
        (from_strings(&["__name__", "test", "host.name", "localhost", "job", "check"]), ValidationScheme::Legacy, false),
    ];
    for (input, mode, expected) in tests {
        assert_eq!(expected, input.is_valid(mode));
    }
    // adapted: the Go case with the label name "\xc5 bad utf8" (invalid UTF-8) expects false under UTF-8
    // validation. Such a name cannot be a Rust label string; it is rejected at conversion.
    assert!(std::str::from_utf8(std::hint::black_box(b"\xc5 bad utf8")).is_err());
}

#[test]
fn test_labels_equal() {
    let labels = from_strings(&["aaa", "111", "bbb", "222"]);
    let tests: Vec<(Labels, bool)> = vec![
        (from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]), false),
        (from_strings(&["aaa", "111", "bar", "222"]), false),
        (from_strings(&["aaa", "111", "bbb", "233"]), false),
        (from_strings(&["aaa", "111", "bbb", "222"]), true),
    ];
    for (i, (compared, expected)) in tests.iter().enumerate() {
        assert_eq!(*expected, equal(&labels, compared), "unexpected comparison result for test case {}", i);
    }
}

#[test]
fn test_labels_from_strings() {
    let labels = from_strings(&["aaa", "111", "bbb", "222"]);
    let mut x = 0;
    labels.range(|n, v| {
        match x {
            0 => assert_eq!(Label::new("aaa", "111"), Label::new(n, v), "unexpected value"),
            1 => assert_eq!(Label::new("bbb", "222"), Label::new(n, v), "unexpected value"),
            _ => panic!("unexpected labelset value {}: {}={}", x, n, v),
        }
        x += 1;
    });
    assert!(std::panic::catch_unwind(|| from_strings(&["aaa", "111", "bbb"])).is_err());
}

#[test]
fn test_labels_compare() {
    let labels = from_strings(&["aaa", "111", "bbb", "222"]);
    let tests: Vec<(Labels, i32)> = vec![
        (from_strings(&["aaa", "110", "bbb", "222"]), 1),
        (from_strings(&["aaa", "111", "bbb", "233"]), -1),
        (from_strings(&["aaa", "111", "bar", "222"]), 1),
        (from_strings(&["aaa", "111", "bbc", "222"]), -1),
        (from_strings(&["aaa", "111", "bb", "222"]), 1),
        (from_strings(&["aaa", "111", "bbbb", "222"]), -1),
        (from_strings(&["aaa", "111"]), 1),
        (from_strings(&["aaa", "111", "bbb", "222", "ccc", "333", "ddd", "444"]), -2),
        (from_strings(&["aaa", "111", "bbb", "222"]), 0),
        (empty_labels(), 1),
    ];
    for (i, (compared, expected)) in tests.iter().enumerate() {
        let got = compare(&labels, compared);
        assert_eq!(expected.signum(), got.signum(), "unexpected comparison result for test case {}", i);
        let got = compare(compared, &labels);
        assert_eq!(-expected.signum(), got.signum(), "unexpected comparison result for reverse test case {}", i);
    }
}

#[test]
fn test_labels_has() {
    let tests = [("foo", false), ("aaa", true)];
    let labels_set = from_strings(&["aaa", "111", "bbb", "222"]);
    for (i, (input, expected)) in tests.iter().enumerate() {
        assert_eq!(*expected, labels_set.has(input), "unexpected comparison result for test case {}", i);
    }
}

#[test]
fn test_labels_get() {
    assert!(from_strings(&["aaa", "111", "bbb", "222"]).get("foo").is_empty());
    assert_eq!("111", from_strings(&["aaaa", "111", "bbb", "222"]).get("aaaa"));
    assert_eq!("222", from_strings(&["aaaa", "111", "bbb", "222"]).get("bbb"));
}

#[test]
fn test_labels_drop_metric_name() {
    assert!(equal(
        &from_strings(&["aaa", "111", "bbb", "222"]),
        &from_strings(&["aaa", "111", "bbb", "222"]).drop_metric_name()
    ));
    assert!(equal(&from_strings(&["aaa", "111"]), &from_strings(&[METRIC_NAME, "myname", "aaa", "111"]).drop_metric_name()));

    let original = from_strings(&["__aaa__", "111", METRIC_NAME, "myname", "bbb", "222"]);
    let check = original.copy();
    assert!(equal(&from_strings(&["__aaa__", "111", "bbb", "222"]), &check.drop_metric_name()));
    assert!(equal(&original, &check));
}

#[test]
fn test_labels_drop_reserved() {
    let should_drop = |n: &str| n == METRIC_NAME || n == "__something__";
    assert!(equal(
        &from_strings(&["aaa", "111", "bbb", "222"]),
        &from_strings(&["aaa", "111", "bbb", "222"]).drop_reserved(should_drop)
    ));
    assert!(equal(
        &from_strings(&["aaa", "111"]),
        &from_strings(&[METRIC_NAME, "myname", "aaa", "111"]).drop_reserved(should_drop)
    ));
    assert!(equal(
        &from_strings(&["aaa", "111"]),
        &from_strings(&[METRIC_NAME, "myname", "__something__", "counter", "aaa", "111"]).drop_reserved(should_drop)
    ));

    let original = from_strings(&["__aaa__", "111", METRIC_NAME, "myname", "bbb", "222"]);
    let check = original.copy();
    assert!(equal(&from_strings(&["__aaa__", "111", "bbb", "222"]), &check.drop_reserved(should_drop)));
    assert!(equal(&original, &check));
}

#[test]
fn test_labels_copy() {
    assert_eq!(from_strings(&["aaa", "111", "bbb", "222"]), from_strings(&["aaa", "111", "bbb", "222"]).copy());
}

#[test]
fn test_labels_map() {
    let mut want = HashMap::new();
    want.insert("aaa".to_string(), "111".to_string());
    want.insert("bbb".to_string(), "222".to_string());
    assert_eq!(want, from_strings(&["aaa", "111", "bbb", "222"]).map());
}

fn bytes_of(l: &Labels) -> Vec<u8> {
    let mut b = Vec::new();
    l.bytes(&mut b);
    b
}

#[test]
fn test_labels_bytes_with_labels() {
    let mut b = Vec::new();
    from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]).bytes_with_labels(&mut b, &["aaa", "bbb"]);
    assert_eq!(bytes_of(&from_strings(&["aaa", "111", "bbb", "222"])), b);
    let empty: [&str; 0] = [];
    from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]).bytes_with_labels(&mut b, &empty);
    assert_eq!(bytes_of(&from_strings::<&str>(&[])), b);
}

#[test]
fn test_labels_bytes_without_labels() {
    let mut b = Vec::new();
    from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]).bytes_without_labels(&mut b, &["bbb", "ccc"]);
    assert_eq!(bytes_of(&from_strings(&["aaa", "111"])), b);
    from_strings(&[METRIC_NAME, "333", "aaa", "111", "bbb", "222"]).bytes_without_labels(&mut b, &["bbb"]);
    assert_eq!(bytes_of(&from_strings(&[METRIC_NAME, "333", "aaa", "111"])), b);
    from_strings(&[METRIC_NAME, "333", "aaa", "111", "bbb", "222"]).bytes_without_labels(&mut b, &[METRIC_NAME, "bbb"]);
    assert_eq!(bytes_of(&from_strings(&["aaa", "111"])), b);
}

struct BuilderCase {
    base: Labels,
    del: Vec<&'static str>,
    keep: Vec<&'static str>,
    set: Vec<(&'static str, &'static str)>,
    want: Labels,
}

fn bc(
    base: Labels,
    del: Vec<&'static str>,
    keep: Vec<&'static str>,
    set: Vec<(&'static str, &'static str)>,
    want: Labels,
) -> BuilderCase {
    BuilderCase { base, del, keep, set, want }
}

#[test]
fn test_builder() {
    let mut reuse_builder = Builder::new(empty_labels());
    let cases = vec![
        bc(from_strings(&["aaa", "111"]), vec![], vec![], vec![], from_strings(&["aaa", "111"])),
        bc(
            empty_labels(),
            vec![],
            vec![],
            vec![("aaa", "444"), ("bbb", "555"), ("ccc", "666")],
            from_strings(&["aaa", "444", "bbb", "555", "ccc", "666"]),
        ),
        bc(
            from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]),
            vec![],
            vec![],
            vec![("aaa", "444"), ("bbb", "555"), ("ccc", "666")],
            from_strings(&["aaa", "444", "bbb", "555", "ccc", "666"]),
        ),
        bc(
            from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]),
            vec!["bbb"],
            vec![],
            vec![],
            from_strings(&["aaa", "111", "ccc", "333"]),
        ),
        bc(
            Labels::default(),
            vec!["bbb"],
            vec![],
            vec![("aaa", "111"), ("bbb", "222"), ("ccc", "333")],
            from_strings(&["aaa", "111", "ccc", "333"]),
        ),
        bc(from_strings(&["aaa", "111"]), vec![], vec![], vec![("bbb", "222")], from_strings(&["aaa", "111", "bbb", "222"])),
        bc(
            from_strings(&["aaa", "111"]),
            vec![],
            vec![],
            vec![("bbb", "222"), ("bbb", "333")],
            from_strings(&["aaa", "111", "bbb", "333"]),
        ),
        bc(
            from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]),
            vec!["bbb"],
            vec![],
            vec![("ddd", "444")],
            from_strings(&["aaa", "111", "ccc", "333", "ddd", "444"]),
        ),
        // Blank value is interpreted as delete.
        bc(from_strings(&["aaa", "111", "bbb", "", "ccc", "333"]), vec![], vec![], vec![], from_strings(&["aaa", "111", "ccc", "333"])),
        bc(
            from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]),
            vec![],
            vec![],
            vec![("bbb", "")],
            from_strings(&["aaa", "111", "ccc", "333"]),
        ),
        bc(
            from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]),
            vec![],
            vec!["bbb"],
            vec![],
            from_strings(&["bbb", "222"]),
        ),
        bc(
            from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]),
            vec![],
            vec!["aaa", "ccc"],
            vec![],
            from_strings(&["aaa", "111", "ccc", "333"]),
        ),
        bc(
            from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"]),
            vec!["bbb"],
            vec!["aaa", "ddd"],
            vec![("ddd", "444")],
            from_strings(&["aaa", "111", "ddd", "444"]),
        ),
    ];
    for (i, tcase) in cases.iter().enumerate() {
        let test = |b: &mut Builder| {
            for (n, v) in &tcase.set {
                b.set(n, v);
            }
            if !tcase.keep.is_empty() {
                b.keep(&tcase.keep);
            }
            b.del_many(&tcase.del);
            assert!(equal(&tcase.want, &b.labels()), "case {}", i);

            // Check what happens when we call Range and mutate the builder.
            b.range_mut(|b, n, _| {
                if n == "aaa" || n == "bbb" {
                    b.del(n);
                }
            });
            let mut want = Vec::new();
            tcase.want.bytes_without_labels(&mut want, &["aaa", "bbb"]);
            assert_eq!(want, bytes_of(&b.labels()), "case {}", i);
        };
        // NewBuilder
        test(&mut Builder::new(tcase.base.clone()));
        // NewBuilderWithSymbolTable (a no-op symbol table in the stringlabels build), then Reset.
        let mut b = Builder::new(empty_labels());
        b.reset(tcase.base.clone());
        test(&mut b);
        // reuseBuilder
        reuse_builder.reset(tcase.base.clone());
        test(&mut reuse_builder);
    }
    // set_after_del
    let mut b = Builder::new(from_strings(&["aaa", "111"]));
    b.del("bbb");
    b.set("bbb", "222");
    assert_eq!(from_strings(&["aaa", "111", "bbb", "222"]), b.labels());
    assert_eq!("222", b.get("bbb"));
}

#[test]
fn test_scratch_builder() {
    let cases: Vec<(Vec<(&str, &str)>, Labels)> = vec![
        (vec![], empty_labels()),
        (vec![("aaa", "111")], from_strings(&["aaa", "111"])),
        (vec![("aaa", "111"), ("bbb", "222"), ("ccc", "333")], from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"])),
        (vec![("bbb", "222"), ("aaa", "111"), ("ccc", "333")], from_strings(&["aaa", "111", "bbb", "222", "ccc", "333"])),
        (vec![("ddd", "444")], from_strings(&["ddd", "444"])),
    ];
    for (add, want) in cases {
        let mut b = ScratchBuilder::new(add.len());
        for (n, v) in &add {
            b.add(n, v);
        }
        b.sort();
        assert!(equal(&want, &b.labels()));
        b.assign(want.clone());
        assert!(equal(&want, &b.labels()));
    }
}

#[test]
fn test_labels_hash() {
    let lbls = from_strings(&["foo", "bar", "baz", "qux"]);
    let (hash1, hash2) = (lbls.hash(), lbls.hash());
    assert_eq!(hash1, hash2);
    assert_ne!(lbls.hash(), from_strings(&["foo", "bar"]).hash(), "different labels match.");
}

fn yaml_eq(a: &str, b: &str) {
    let ya = yaml_rust2::YamlLoader::load_from_str(a).unwrap();
    let yb = yaml_rust2::YamlLoader::load_from_str(b).unwrap();
    assert_eq!(ya, yb, "YAML {:?} != {:?}", a, b);
}

fn json_eq(a: &str, b: &str) {
    assert_eq!(json::Json::parse(a).unwrap(), json::Json::parse(b).unwrap(), "JSON {:?} != {:?}", a, b);
}

#[test]
fn test_marshaling() {
    let lbls = from_strings(&["aaa", "111", "bbb", "2222", "ccc", "33333"]);
    let expected_json = "{\"aaa\":\"111\",\"bbb\":\"2222\",\"ccc\":\"33333\"}";
    let b = lbls.marshal_json();
    json_eq(expected_json, &b);

    let got_j = Labels::unmarshal_json(&b).unwrap();
    assert_eq!(lbls, got_j);

    let expected_yaml = "aaa: \"111\"\nbbb: \"2222\"\nccc: \"33333\"\n";
    let b = lbls.marshal_yaml();
    yaml_eq(expected_yaml, &b);

    let got_y = Labels::unmarshal_yaml(&b).unwrap();
    assert_eq!(lbls, got_y);

    // Now in a struct with a tag: `a_labels,omitempty`. The struct encoding is written out by hand here, as the
    // reflection-based encoding of Go has no Rust counterpart; the labels part is the code under test.
    let mut b = String::from("{\"a_labels\":");
    lbls.write_json(&mut b);
    b.push('}');
    let expected_json_from_struct = format!("{{\"a_labels\":{}}}", expected_json);
    json_eq(&expected_json_from_struct, &b);
    let v = json::Json::parse(&b).unwrap();
    let got_fj = Labels::from_json(v.get("a_labels").unwrap()).unwrap();
    assert_eq!(lbls, got_fj);

    let mut h = yaml_rust2::yaml::Hash::new();
    h.insert(yaml_rust2::Yaml::String("a_labels".to_string()), lbls.to_yaml());
    let mut b = String::new();
    yaml_rust2::YamlEmitter::new(&mut b).dump(&yaml_rust2::Yaml::Hash(h)).unwrap();
    let expected_yaml_from_struct = "a_labels:\n  aaa: \"111\"\n  bbb: \"2222\"\n  ccc: \"33333\"\n";
    yaml_eq(expected_yaml_from_struct, &b);
    let y = &yaml_rust2::YamlLoader::load_from_str(&b).unwrap()[0];
    let got_fy = Labels::from_yaml(&y["a_labels"]).unwrap();
    assert_eq!(lbls, got_fy);
}

// Port of schema/labels_test.go (package schema; the ledger puts it in this file).

mod schema_tests {
    use model::labels::{from_strings, Builder, ScratchBuilder, METRIC_NAME};
    use model::metadata::MetricType;
    use model::schema::{Metadata, METRIC_TYPE_LABEL, METRIC_UNIT_LABEL};

    fn meta(name: &str, typ: MetricType, unit: &str) -> Metadata {
        Metadata { name: name.to_string(), typ: typ.as_str().to_string(), unit: unit.to_string() }
    }

    #[test]
    fn test_metadata() {
        let test_meta = meta("metric_total", MetricType::Counter, "seconds");
        for (empty_name, empty_type, empty_unit) in
            [(false, false, false), (true, false, false), (false, true, false), (false, false, true), (true, true, true)]
        {
            let mut expected_meta = Metadata::default();
            let mut lb = ScratchBuilder::new(0);
            lb.add("foo", "bar");
            if !empty_name {
                lb.add(METRIC_NAME, &test_meta.name);
                expected_meta.name = test_meta.name.clone();
            }
            if !empty_type {
                lb.add(METRIC_TYPE_LABEL, &test_meta.typ);
                expected_meta.typ = test_meta.typ.clone();
            } else {
                expected_meta.typ = MetricType::Unknown.as_str().to_string();
            }
            if !empty_unit {
                lb.add(METRIC_UNIT_LABEL, &test_meta.unit);
                expected_meta.unit = test_meta.unit.clone();
            }
            lb.sort();
            let expected_labels = lb.labels();

            // From labels to Metadata.
            assert_eq!(expected_meta, Metadata::from_labels(&expected_labels));
            // Empty methods.
            assert_eq!(empty_name, expected_meta.is_empty_for(METRIC_NAME));
            assert_eq!(empty_type, expected_meta.is_empty_for(METRIC_TYPE_LABEL));
            assert_eq!(empty_type, expected_meta.is_type_empty());
            assert_eq!(empty_unit, expected_meta.is_empty_for(METRIC_UNIT_LABEL));
            // From Metadata to labels for various builders.
            let mut slb = ScratchBuilder::new(0);
            slb.add("foo", "bar");
            expected_meta.add_to_labels(&mut slb);
            slb.sort();
            assert_eq!(expected_labels, slb.labels());

            let mut lb = Builder::new(from_strings(&["foo", "bar"]));
            expected_meta.set_to_labels(&mut lb);
            assert_eq!(expected_labels, lb.labels());
        }
    }

    #[test]
    fn test_ignore_overridden_metadata_labels_scratch_builder() {
        // PROM-39 specifies that metadata labels should be sourced primarily from the metadata structures.
        // However, the original labels should be preserved IF the metadata structure does not set or support
        // certain information.
        let incoming = from_strings(&[
            METRIC_NAME,
            "different_name",
            METRIC_TYPE_LABEL,
            MetricType::Summary.as_str(),
            METRIC_UNIT_LABEL,
            "MB",
            "foo",
            "bar",
        ]);
        let cases = vec![
            (Metadata::default(), incoming.clone()),
            (
                meta("metric_total", MetricType::Counter, "seconds"),
                from_strings(&[METRIC_NAME, "metric_total", METRIC_TYPE_LABEL, "counter", METRIC_UNIT_LABEL, "seconds", "foo", "bar"]),
            ),
            (
                meta("metric_total", MetricType::Counter, ""),
                from_strings(&[METRIC_NAME, "metric_total", METRIC_TYPE_LABEL, "counter", METRIC_UNIT_LABEL, "MB", "foo", "bar"]),
            ),
            (
                meta("", MetricType::Counter, "seconds"),
                from_strings(&[METRIC_NAME, "different_name", METRIC_TYPE_LABEL, "counter", METRIC_UNIT_LABEL, "seconds", "foo", "bar"]),
            ),
            (
                meta("metric_total", MetricType::Unknown, "seconds"),
                from_strings(&[METRIC_NAME, "metric_total", METRIC_TYPE_LABEL, "summary", METRIC_UNIT_LABEL, "seconds", "foo", "bar"]),
            ),
        ];
        for (high_prio_meta, expected) in cases {
            let mut lb = ScratchBuilder::new(0);
            high_prio_meta.add_to_labels(&mut lb);
            {
                let mut wrapped = high_prio_meta.new_ignore_overridden_metadata_label_scratch_builder(&mut lb);
                incoming.range(|n, v| wrapped.add(n, v));
            }
            lb.sort();
            assert_eq!(expected, lb.labels(), "{:?}", high_prio_meta);
        }
    }
}
