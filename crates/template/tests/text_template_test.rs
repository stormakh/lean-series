//! text/template behavior checked against real Go output (Go 1.26.7, text/template): printing, control
//! structures, builtins, fmt verbs, and parse and execution error texts. The expected strings were produced
//! by bench/tmp/w3-rules/template/gotext/main.go.

use std::collections::BTreeMap;
use std::sync::Arc;

use template::gotime::GoTime;
use template::text::Template;
use template::{Func, IntKind, StrKind, StructType, Type, UintKind, Value};

fn item_type() -> Arc<StructType> {
    // Go: type item struct { Name string; Count int; Tags []string; Inner *item; Any any }. The recursive
    // pointer field uses a struct type of the same name.
    let inner = Arc::new(StructType { name: "main.item".into(), fields: vec![("Name".into(), Type::String(StrKind::String))] });
    Arc::new(StructType {
        name: "main.item".into(),
        fields: vec![
            ("Name".into(), Type::String(StrKind::String)),
            ("Count".into(), Type::Int(IntKind::Int)),
            ("Tags".into(), Type::slice_of(Type::String(StrKind::String))),
            ("Inner".into(), Type::ptr_to(Type::Struct(inner))),
            ("Any".into(), Type::Interface),
        ],
    })
}

fn item(name: &str, count: i64, tags: &[&str], inner: Option<Value>) -> Value {
    let t = item_type();
    let inner_t = t.fields[3].1.clone();
    Value::ptr(Value::new_struct(
        t,
        vec![
            Value::string(name),
            Value::int(count),
            Value::slice(Type::String(StrKind::String), tags.iter().map(|s| Value::string(*s)).collect()),
            inner.unwrap_or(Value::Nil(inner_t)),
            Value::Invalid,
        ],
    ))
}

fn data() -> Value {
    let mut m: BTreeMap<String, Value> = BTreeMap::new();
    let s = |v: &str| Value::string(v);
    let strs = |v: &[&str]| Value::slice(Type::String(StrKind::String), v.iter().map(|x| Value::string(*x)).collect());
    m.insert("s".into(), s("hello"));
    m.insert("n".into(), Value::int(3));
    m.insert("neg".into(), Value::int(-2));
    m.insert("u".into(), Value::Uint(7, UintKind::Uint));
    m.insert("f".into(), Value::Float(2.5));
    m.insert("b".into(), Value::Bool(true));
    m.insert("nil".into(), Value::Invalid);
    m.insert("list".into(), strs(&["a", "b", "c"]));
    m.insert("ints".into(), Value::slice(Type::Int(IntKind::Int), vec![Value::int(3), Value::int(1), Value::int(2)]));
    m.insert("empty".into(), strs(&[]));
    m.insert("m".into(), Value::string_map([("x", "1"), ("y", "2")]));
    let mut mi: BTreeMap<String, Value> = BTreeMap::new();
    mi.insert("k".into(), Value::int(1));
    mi.insert("z".into(), Value::Invalid);
    m.insert("mi".into(), Value::map(Type::Interface, mi));
    m.insert("item".into(), item("it", 2, &["t1"], None));
    m.insert("deep".into(), item("d", 0, &[], Some(item("in", 0, &[], None))));
    m.insert("nilp".into(), Value::Nil(Type::ptr_to(Type::Struct(item_type()))));
    m.insert(
        "add".into(),
        Value::Func(Func::new(vec![Type::Int(IntKind::Int), Type::Int(IntKind::Int)], Type::Int(IntKind::Int), false, |a| {
            let (Value::Int(x, _), Value::Int(y, _)) = (&a[0], &a[1]) else { return Err("bad args".into()) };
            Ok(Value::int(x + y))
        })),
    );
    m.insert("fail".into(), Value::Func(Func::new(vec![], Type::String(StrKind::String), true, |_| Err("boom".into()))));
    m.insert("dur".into(), Value::duration(90_000_000_000));
    // time.Date(2020, 2, 29, 13, 4, 5, 123000000, time.UTC)
    m.insert("tm".into(), Value::Time(GoTime { sec: 1582981445, nsec: 123_000_000 }));
    m.insert("html".into(), s("<a href='x'>&</a>"));
    m.insert("cplx".into(), Value::Complex(1.0, 2.0));
    Value::map(Type::Interface, m)
}

#[test]
fn text_template_matches_go() {
    let cases: &[(&str, &str, &str, &str)] = &[
        (r###"{{.s}} {{.n}} {{.u}} {{.f}} {{.b}} {{.nil}} {{.list}} {{.m}} {{.mi}}"###, r###""###, r###"hello 3 7 2.5 true <no value> [a b c] map[x:1 y:2] map[k:1 z:<nil>]"###, r###""###),
        (r###"{{.item}} {{.item.Name}} {{.deep.Inner.Name}} {{.item.Tags}} {{.nilp}}"###, r###""###, r###"{it 2 [t1] <nil> <nil>} it in [t1] <nil>"###, r###""###),
        (r###"{{.deep.Inner.Inner}}|{{.item.Any}}|{{.item.Inner}}"###, r###""###, r###"<nil>|<no value>|<nil>"###, r###""###),
        (r###"{{.nilp.Name}}"###, r###""###, r###""###, r###"template: t0:1:7: executing "t0" at <.nilp.Name>: nil pointer evaluating interface {}.Name"###),
        (r###"{{.mi.z.q}}"###, r###""###, r###""###, r###"template: t0:1:5: executing "t0" at <.mi.z.q>: nil pointer evaluating interface {}.q"###),
        (r###"{{.missing}}|{{.m.missing}}|{{.mi.missing}}"###, r###""###, r###"<no value>|<no value>|<no value>"###, r###""###),
        (r###"{{.missing}}|{{.m.missing}}|{{.mi.missing}}"###, r###"missingkey=zero"###, r###"<no value>||<no value>"###, r###""###),
        (r###"{{.m.missing}}"###, r###"missingkey=error"###, r###""###, r###"template: t0:1:4: executing "t0" at <.m.missing>: map has no entry for key "missing""###),
        (r###"{{if .b}}yes{{else}}no{{end}} {{if .nil}}yes{{else if .n}}n{{else}}no{{end}}"###, r###""###, r###"yes n"###, r###""###),
        (r###"{{with .s}}[{{.}}]{{end}}{{with .nil}}x{{else with .n}}{{.}}{{end}}"###, r###""###, r###"[hello]3"###, r###""###),
        (r###"{{range $i, $e := .list}}{{$i}}={{$e}};{{end}}{{range .empty}}x{{else}}empty{{end}}"###, r###""###, r###"0=a;1=b;2=c;empty"###, r###""###),
        (r###"{{range $k, $v := .m}}{{$k}}:{{$v}} {{end}}"###, r###""###, r###"x:1 y:2 "###, r###""###),
        (r###"{{range 4}}{{.}}{{end}} {{range $i := 3}}{{$i}}{{end}}"###, r###""###, r###"0123 012"###, r###""###),
        (r###"{{range .ints}}{{if eq . 1}}{{continue}}{{end}}{{if eq . 2}}{{break}}{{end}}{{.}}{{end}}"###, r###""###, r###"3"###, r###""###),
        (r###"{{$x := 1}}{{range .list}}{{$x = 2}}{{end}}{{$x}}"###, r###""###, r###"2"###, r###""###),
        (r###"{{define "a"}}A{{.}}{{end}}{{template "a" .n}} {{block "b" .s}}B{{.}}{{end}}"###, r###""###, r###"A3 Bhello"###, r###""###),
        (r###"{{and 1 0 2}} {{and 1 2}} {{or 0 "" "x"}} {{or 0 ""}} {{not .b}} {{not 0}}"###, r###""###, r###"0 2 x  false true"###, r###""###),
        (r###"{{len .list}} {{len .m}} {{len .s}} {{index .list 1}} {{index .m "x"}} {{index .mi "k"}} {{slice .list 1 2}} {{slice .s 1 3}}"###, r###""###, r###"3 2 5 b 1 1 [b] el"###, r###""###),
        (r###"{{print 1 2 "a" "b" 3}}|{{println 1 "a"}}|{{printf "%d-%s-%v-%5.2f-%x-%q" 1 "a" true 3.14159 255 "q"}}"###, r###""###, r###"1 2ab3|1 a
|1-a-true- 3.14-ff-"q""###, r###""###),
        (r###"{{html .html}} {{js .html}} {{urlquery .html}} {{.html | html}}"###, r###""###, r###"&lt;a href=&#39;x&#39;&gt;&amp;&lt;/a&gt; \u003Ca href\u003D\'x\'\u003E\u0026\u003C/a\u003E %3Ca+href%3D%27x%27%3E%26%3C%2Fa%3E &lt;a href=&#39;x&#39;&gt;&amp;&lt;/a&gt;"###, r###""###),
        (r###"{{eq 1 1}} {{eq 1 2 1}} {{ne "a" "b"}} {{lt 1 2}} {{le 2 2}} {{gt 3 2}} {{ge 1 2}} {{eq .u 7}} {{lt .neg .u}}"###, r###""###, r###"true true true true true true false true true"###, r###""###),
        (r###"{{eq .s 1}}"###, r###""###, r###""###, r###"template: t0:1:2: executing "t0" at <eq .s 1>: error calling eq: incompatible types for comparison: string and int"###),
        (r###"{{lt true false}}"###, r###""###, r###""###, r###"template: t0:1:2: executing "t0" at <lt true false>: error calling lt: invalid type for comparison"###),
        (r###"{{call .add 1 2}} {{call .fail}}"###, r###""###, r###"3 "###, r###"template: t0:1:20: executing "t0" at <call .fail>: error calling call: boom"###),
        (r###"{{call .s}}"###, r###""###, r###""###, r###"template: t0:1:2: executing "t0" at <call .s>: error calling call: non-function .s of type string"###),
        (r###"{{.dur}} {{.dur.Seconds}} {{.tm}} {{.tm.Year}} {{.tm.Month}} {{.tm.Weekday}} {{.tm.Format "Jan _2 03:04:05.000 PM -07:00 MST Monday 002"}} {{.tm.Unix}}"###, r###""###, r###"1m30s 90 2020-02-29 13:04:05.123 +0000 UTC 2020 February Saturday Feb 29 01:04:05.123 PM +00:00 UTC Saturday 060 1582981445"###, r###""###),
        (r###"{{1}} {{1.5}} {{1e3}} {{0x10}} {{'a'}} {{-3}} {{1_000}} {{0b101}} {{0o17}} {{.cplx}} {{2i}}"###, r###""###, r###"1 1.5 1000 16 97 -3 1000 5 15 (1+2i) (0+2i)"###, r###""###),
        (r###"{{printf "%v %T %T %T" nil .item .m .list}}"###, r###""###, r###"<nil> *main.item map[string]string []string"###, r###""###),
        (r###"{{- " trim " -}}  x  {{- 1 -}}"###, r###""###, r###" trim x1"###, r###""###),
        (r###"{{/* comment */}}a{{- /* c2 */ -}} b"###, r###""###, r###"ab"###, r###""###),
        (r###"{{"a" | printf "%s-%s" "b"}}"###, r###""###, r###"b-a"###, r###""###),
        (r###"{{.s.x}}"###, r###""###, r###""###, r###"template: t0:1:4: executing "t0" at <.s.x>: can't evaluate field x in type interface {}"###),
        (r###"{{.n.x}}"###, r###""###, r###""###, r###"template: t0:1:4: executing "t0" at <.n.x>: can't evaluate field x in type interface {}"###),
        (r###"{{.item.nope}}"###, r###""###, r###""###, r###"template: t0:1:7: executing "t0" at <.item.nope>: can't evaluate field nope in type interface {}"###),
        (r###"{{.item.Name 1}}"###, r###""###, r###""###, r###"template: t0:1:7: executing "t0" at <.item.Name>: Name has arguments but cannot be invoked as function"###),
        (r###"{{template "nope"}}"###, r###""###, r###""###, r###"template: t0:1:11: executing "t0" at <{{template "nope"}}>: template "nope" not defined"###),
        (r###"{{index .list 5}}"###, r###""###, r###""###, r###"template: t0:1:2: executing "t0" at <index .list 5>: error calling index: index out of range: 5"###),
        (r###"{{index .s 1}}"###, r###""###, r###"101"###, r###""###),
        (r###"{{nope}}"###, r###""###, r###""###, r###"template: t0:1: function "nope" not defined"###),
        (r###"{{$x}}"###, r###""###, r###""###, r###"template: t0:1: undefined variable "$x""###),
        (r###"{{if}}x{{end}}"###, r###""###, r###""###, r###"template: t0:1: missing value for if"###),
        (r###"{{range .list}}"###, r###""###, r###""###, r###"template: t0:1: unexpected EOF"###),
        (r###"{{end}}"###, r###""###, r###""###, r###"template: t0:1: unexpected {{end}}"###),
        (r###"{{else}}"###, r###""###, r###""###, r###"template: t0:1: unexpected {{else}}"###),
        (r###"{{"unterminated}}"###, r###""###, r###""###, r###"template: t0:1: unterminated quoted string"###),
        (r###"{{1 | 2}}"###, r###""###, r###""###, r###"template: t0:1: non executable command in pipeline stage 2"###),
        (r###"{{(1}}"###, r###""###, r###""###, r###"template: t0:1: unclosed left paren"###),
        (r###"{{break}}"###, r###""###, r###""###, r###"template: t0:1: {{break}} outside {{range}}"###),
        (r###"{{define "x"}}a{{end}}{{define "x"}}b{{end}}"###, r###""###, r###""###, r###"template: t0:1: template: multiple definition of template "x""###),
        (r###"{{printf "%d %d" 1}} {{printf "%d" 1 2}} {{printf "%!" 1}} {{printf "%z" 1}}"###, r###""###, r###"1 %!d(MISSING) 1%!(EXTRA int=2) %!!(int=1) %!z(int=1)"###, r###""###),
        (r###"{{.f | printf "%.1f"}} {{printf "%08.3f|%-8d|%+d|% d|%x|%X|%o|%b|%c|%U|%e|%E|%g|%G" 1.5 42 5 5 "hi" 255 8 5 65 9731 1234.5678 0.00012 1e21 1e-7}}"###, r###""###, r###"2.5 0001.500|42      |+5| 5|6869|FF|10|101|A|U+2603|1.234568e+03|1.200000E-04|1e+21|1E-07"###, r###""###),
        (r###"{{printf "%6.2s|%-6s|%q|%#q|%x|% x|%T" "abcdef" "ab" "a\tb" "back" "hey" "hey" .dur}}"###, r###""###, r###"    ab|ab    |"a\tb"|`back`|686579|68 65 79|time.Duration"###, r###""###),
        (r###"{{printf "%v|%+v|%d|%s" .item .item .ints .list}}"###, r###""###, r###"&{it 2 [t1] <nil> <nil>}|&{Name:it Count:2 Tags:[t1] Inner:<nil> Any:<nil>}|[3 1 2]|[a b c]"###, r###""###),
        (r###"{{printf "%t %v %08b" true false 5}}"###, r###""###, r###"true false 00000101"###, r###""###),
        (r###"{{12345678901234567890}}"###, r###""###, r###""###, r###"template: t0:1:2: executing "t0" at <12345678901234567890>: 12345678901234567890 overflows int"###),
        (r###"{{nil}}"###, r###""###, r###""###, r###"template: t0:1:2: executing "t0" at <nil>: nil is not a command"###),
        (r###"{{.s | len}} {{"abc" | slice 1}}"###, r###""###, r###"5 "###, r###"template: t0:1:23: executing "t0" at <slice 1>: error calling slice: can't slice item of type int"###),
        (r###"line1
{{.s}}
{{.nope.x}}"###, r###"missingkey=error"###, r###"line1
hello
"###, r###"template: t0:3:7: executing "t0" at <.nope.x>: map has no entry for key "nope""###),
        (r###"{{with $x := .n}}{{$x}}{{end}}"###, r###""###, r###"3"###, r###""###),
        (r###"{{range $i, $e := 3}}{{end}}"###, r###""###, r###""###, r###"template: t0:1:18: executing "t0" at <3>: can't use 3 to iterate over more than one variable"###),
        (r###"{{.fail}}"###, r###""###, r###""###, r###"template: t0:1:2: executing "t0" at <{{.fail}}>: can't print {{.fail}} of type func() (string, error)"###),
        (r###"{{.add 1 2}}"###, r###""###, r###""###, r###"template: t0:1:2: executing "t0" at <.add>: add is not a method but has arguments"###),
    ];
    let mut failures = Vec::new();
    for (text, opt, want_out, want_err) in cases {
        let t = Template::new("t0");
        if !opt.is_empty() {
            t.option(&[*opt]).unwrap();
        }
        let (out, err) = match t.parse(text) {
            Err(e) => (String::new(), e),
            Ok(t) => {
                let mut out = Vec::new();
                match t.execute_to(&mut out, &data()) {
                    Ok(()) => (String::from_utf8_lossy(&out).into_owned(), String::new()),
                    Err(e) => (String::from_utf8_lossy(&out).into_owned(), e),
                }
            }
        };
        if out != *want_out || err != *want_err {
            failures.push(format!("{text}\n  got:  {out:?} / {err:?}\n  want: {want_out:?} / {want_err:?}"));
        }
    }
    assert!(failures.is_empty(), "{} failures:\n{}", failures.len(), failures.join("\n"));
}
