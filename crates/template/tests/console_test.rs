//! Console templates (html/template with the Prometheus functions, as `web.go` expands them). The templates
//! are the Prometheus 2.53 consoles and console libraries (Apache-2.0); the expected outputs were produced by
//! upstream `template.ExpandHTML` (Go 1.26.7), see bench/tmp/w3-rules/template/goprom/main.go.

use std::collections::BTreeMap;
use std::sync::Arc;

use template::{Expander, GoUrl, QueryFunc, StrKind, StructType, Type, Value};

fn dir() -> String {
    format!("{}/tests/testdata/consoles", env!("CARGO_MANIFEST_DIR"))
}

fn data(path: &str) -> Value {
    let string_t = Type::String(StrKind::String);
    let values_t = Type::Map(Arc::new(Type::slice_of(string_t.clone())), Some(Arc::from("url.Values")));
    let st = Arc::new(StructType {
        name: "struct { RawParams url.Values; Params map[string]string; Path string; ExternalLabels map[string]string }"
            .to_string(),
        fields: vec![
            ("RawParams".to_string(), values_t.clone()),
            ("Params".to_string(), Type::map_of(string_t.clone())),
            ("Path".to_string(), string_t.clone()),
            ("ExternalLabels".to_string(), Type::map_of(string_t.clone())),
        ],
    });
    let mut raw: BTreeMap<String, Value> = BTreeMap::new();
    raw.insert("instance".into(), Value::slice(string_t.clone(), vec![Value::string("localhost:9090")]));
    raw.insert("x".into(), Value::slice(string_t.clone(), vec![Value::string("<y>"), Value::string("z")]));
    let raw = Value::Map(Arc::new(template::value::MapValue { ty: values_t, entries: raw }));
    Value::new_struct(
        st,
        vec![
            raw,
            Value::string_map([("instance", "localhost:9090"), ("x", "<y>")]),
            Value::string(path),
            Value::string_map([("cluster", "c1")]),
        ],
    )
}

#[test]
fn console_templates_match_go() {
    let query_func: QueryFunc = Arc::new(|_, _, _| {
        Ok(vec![
            promql::Sample {
                metric: model::labels::from_strings(&["__name__", "up", "instance", "localhost:9090", "job", "prometheus"]),
                f: 1.0,
                ..Default::default()
            },
            promql::Sample {
                metric: model::labels::from_strings(&["__name__", "up", "instance", "b:9100", "job", "node", "device", "sda"]),
                f: 0.5,
                ..Default::default()
            },
        ])
    });
    let ext_url: GoUrl = yaml::url::parse("http://localhost:9090/prom/").unwrap();
    let defs = "{{$rawParams := .RawParams }}{{$params := .Params}}{{$path := .Path}}{{$externalLabels := .ExternalLabels}}";
    let libs = vec![format!("{}/menu.lib", dir()), format!("{}/prom.lib", dir())];
    for name in ["prometheus.html", "prometheus-overview.html", "node.html", "node-overview.html", "node-disk.html"] {
        let text = std::fs::read_to_string(format!("{}/{}", dir(), name)).unwrap();
        let want = std::fs::read_to_string(format!("{}/{}.out", dir(), name)).unwrap();
        let exp = Expander::new(
            storage::Context::default(),
            &format!("{defs}{text}"),
            &format!("__console_{name}"),
            data(name),
            1353755652000,
            Some(query_func.clone()),
            Some(&ext_url),
            None,
        );
        let got = match exp.expand_html(&libs) {
            Ok(s) => s,
            Err(e) => format!("ERROR: {e}"),
        };
        if got != want {
            let line = got.lines().zip(want.lines()).position(|(a, b)| a != b);
            panic!(
                "{name}: output differs at line {:?}\n got: {:?}\nwant: {:?}",
                line.map(|l| l + 1),
                line.and_then(|l| got.lines().nth(l)),
                line.and_then(|l| want.lines().nth(l))
            );
        }
    }
}
