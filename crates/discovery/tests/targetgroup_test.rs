//! Port of discovery/targetgroup/targetgroup_test.go.
use discovery::Group;
use model::labelset::LabelSet;

fn ls(p: &[(&str, &str)]) -> LabelSet {
    LabelSet::from_pairs(p)
}

#[test]
fn test_target_group_strict_json_unmarshal() {
    let tests: Vec<(&str, Option<&str>, Group)> = vec![
        (r#"{"labels": {},"targets": []}"#, None, Group { targets: vec![], labels: LabelSet::new(), source: String::new() }),
        (
            r#"{"labels": {"my":"label"},"targets": ["localhost:9090","localhost:9091"]}"#,
            None,
            Group { targets: vec![ls(&[("__address__", "localhost:9090")]), ls(&[("__address__", "localhost:9091")])], labels: ls(&[("my", "label")]), source: String::new() },
        ),
        (r#"{"label": {},"targets": []}"#, Some("json: unknown field \"label\""), Group::default()),
        (r#"{"labels": {},"target": []}"#, Some("json: unknown field \"target\""), Group::default()),
    ];
    for (json, expected_reply, expected_group) in tests {
        let (reply, tg) = match Group::unmarshal_json(json) {
            Ok(g) => (None, g),
            Err(e) => (Some(e), Group::default()),
        };
        assert_eq!(expected_reply.map(|s| s.to_string()), reply);
        assert_eq!(expected_group, tg);
    }
}

/// testify `JSONEq`: equal as JSON values (object key order ignored).
fn json_eq(a: &str, b: &str) {
    fn norm(v: json::Json) -> json::Json {
        match v {
            json::Json::Obj(mut o) => {
                o.sort_by(|x, y| x.0.cmp(&y.0));
                json::Json::Obj(o.into_iter().map(|(k, v)| (k, norm(v))).collect())
            }
            json::Json::Arr(a) => json::Json::Arr(a.into_iter().map(norm).collect()),
            v => v,
        }
    }
    assert_eq!(norm(json::Json::parse(a).unwrap()), norm(json::Json::parse(b).unwrap()));
}

#[test]
fn test_target_group_json_marshal() {
    let tests = vec![
        // labels should be omitted if empty.
        (Group::default(), r#"{"targets": []}"#),
        // targets only exposes addresses.
        (
            Group { targets: vec![ls(&[("__address__", "localhost:9090")]), ls(&[("__address__", "localhost:9091")])], labels: ls(&[("foo", "bar"), ("bar", "baz")]), source: String::new() },
            r#"{"targets": ["localhost:9090", "localhost:9091"], "labels": {"bar": "baz", "foo": "bar"}}"#,
        ),
    ];
    for (group, expected_json) in tests {
        let actual = group.marshal_json();
        json_eq(expected_json, &actual);
    }
}

#[test]
fn test_target_group_yaml_marshal() {
    let tests = vec![
        // labels should be omitted if empty.
        (Group::default(), "targets: []\n"),
        // targets only exposes addresses.
        (
            Group { targets: vec![ls(&[("__address__", "localhost:9090")]), ls(&[("__address__", "localhost:9091")])], labels: ls(&[("foo", "bar"), ("bar", "baz")]), source: String::new() },
            "targets:\n- localhost:9090\n- localhost:9091\nlabels:\n  bar: baz\n  foo: bar\n",
        ),
    ];
    for (group, expected_yaml) in tests {
        let actual = yaml::marshal(&group);
        assert_eq!(Ok(expected_yaml.to_string()), actual);
    }
}

#[test]
fn test_target_group_yaml_unmarshal() {
    let tests: Vec<(&str, Group, Option<yaml::Error>)> = vec![
        // empty target group.
        ("labels:\ntargets:\n", Group { targets: vec![], ..Default::default() }, None),
        // brackets syntax.
        (
            "labels:\n  my:  label\ntargets:\n  ['localhost:9090', 'localhost:9191']",
            Group { targets: vec![ls(&[("__address__", "localhost:9090")]), ls(&[("__address__", "localhost:9191")])], labels: ls(&[("my", "label")]), source: String::new() },
            None,
        ),
        // incorrect syntax.
        ("labels:\ntargets:\n  'localhost:9090'", Group::default(), Some(yaml::Error::Type(vec!["line 3: cannot unmarshal !!str `localho...` into []string".to_string()]))),
    ];
    for (text, expected_group, expected_reply) in tests {
        let mut tg = Group::default();
        let actual = yaml::unmarshal_into(text, &mut tg, false).err();
        assert_eq!(expected_reply, actual);
        assert_eq!(expected_group, tg);
    }
}

#[test]
fn test_string() {
    // String() should return only the source, regardless of other attributes.
    let group1 = Group {
        targets: vec![ls(&[("__address__", "localhost:9090")]), ls(&[("__address__", "localhost:9091")])],
        source: "<source>".to_string(),
        labels: ls(&[("foo", "bar"), ("bar", "baz")]),
    };
    let group2 = Group { targets: vec![], source: "<source>".to_string(), labels: LabelSet::new() };
    assert_eq!("<source>", group1.to_string());
    assert_eq!("<source>", group2.to_string());
    assert_eq!(group1.to_string(), group2.to_string());
}
