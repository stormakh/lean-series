//! Deep recursion: Go grows goroutine stacks, so a recursive template runs into Go's depth limit, not a
//! stack overflow. The port must give the same error instead of crashing.

use template::Value;
use template::text::Template;

#[test]
fn recursive_template_hits_max_depth() {
    let t = Template::new("t0");
    t.parse(r#"{{define "a"}}{{template "a" .}}{{end}}{{template "a" .}}"#).unwrap();
    let err = t.execute(&Value::Invalid).unwrap_err();
    // Go 1.26.7 text/template.
    assert_eq!(err, r#"template: t0:1:25: executing "a" at <{{template "a" .}}>: exceeded maximum template depth (100000)"#);
}

#[test]
fn deeply_nested_parentheses_parse() {
    // Go's maxStackDepth is 10000.
    let depth = 10000;
    let text = format!("{{{{{}1{}}}}}", "(".repeat(depth), ")".repeat(depth));
    let t = Template::new("t0");
    t.parse(&text).unwrap();
    assert_eq!(t.execute(&Value::Invalid).unwrap(), "1");

    let depth = 10001;
    let text = format!("{{{{{}1{}}}}}", "(".repeat(depth), ")".repeat(depth));
    let t = Template::new("t0");
    assert_eq!(t.parse(&text).err().unwrap(), "template: t0:1: max expression depth exceeded");
}
