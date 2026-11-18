// Port of promql/parser/features_test.go.

use std::collections::HashMap;
use std::sync::Arc;

use promql::features::{self, Collector, Registry};
use promql::parser::*;

#[test]
fn test_register_features_custom_functions() {
    let funcs: FunctionMap = [
        (
            "custom_func".to_string(),
            Arc::new(Function::new("custom_func", vec![ValueType::Matrix], 0, ValueType::Vector, false)),
        ),
        (
            "experimental_func".to_string(),
            Arc::new(Function::new("experimental_func", vec![ValueType::Matrix], 0, ValueType::Vector, true)),
        ),
    ]
    .into_iter()
    .collect();

    let r = Registry::new();
    Parser::new(Options { functions: Some(funcs), ..Default::default() }).register_features(&r);
    let want: HashMap<String, bool> =
        [("custom_func".to_string(), true), ("experimental_func".to_string(), false)].into_iter().collect();
    assert_eq!(want, r.get()[features::PROMQL_FUNCTIONS]);

    let r = Registry::new();
    Parser::new(Options::default()).register_features(&r);
    assert!(!r.get()[features::PROMQL_FUNCTIONS].contains_key("custom_func"));
    assert!(r.get()[features::PROMQL_FUNCTIONS].contains_key("rate"));
}
