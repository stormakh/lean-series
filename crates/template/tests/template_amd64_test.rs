//! Port of upstream/template/template_amd64_test.go.
//!
//! Some test cases rely upon architecture-specific behaviors with respect to numerical conversions (Go's
//! float-to-int conversion of out-of-range values on amd64); the port implements the amd64 behavior.

use std::sync::Arc;

use template::{Expander, GoUrl, IntKind, QueryFunc, UintKind, Value};

struct Scenario {
    text: &'static str,
    output: &'static str,
    input: Value,
}

fn test_template_expansion_scenarios(scenarios: Vec<Scenario>) {
    let ext_url: GoUrl = yaml::url::parse("http://testhost:9090/path/prefix").expect("url");
    for s in scenarios {
        let query_func: QueryFunc = Arc::new(|_, _, _| Ok(promql::Vector::new()));
        let expander = Expander::new(
            storage::Context::default(),
            s.text,
            "test",
            s.input.clone(),
            1353755652000,
            Some(query_func),
            Some(&ext_url),
            None,
        );
        match expander.expand() {
            Ok(r) => assert_eq!(r, s.output, "{}", s.text),
            Err(e) => panic!("{}: unexpected error {}", s.text, e),
        }
    }
}

#[test]
fn test_template_expansion_amd64() {
    test_template_expansion_scenarios(vec![
        // HumanizeDuration - MaxInt64.
        Scenario {
            text: "{{ humanizeDuration . }}",
            input: Value::Int(i64::MAX, IntKind::Int),
            output: "-106751991167300d -15h -30m -8s",
        },
        // HumanizeDuration - MaxUint64.
        Scenario {
            text: "{{ humanizeDuration . }}",
            input: Value::Uint(u64::MAX, UintKind::Uint),
            output: "-106751991167300d -15h -30m -8s",
        },
    ]);
}
