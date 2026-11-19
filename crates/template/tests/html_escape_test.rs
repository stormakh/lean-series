//! html/template contextual escaping checked against real Go output (Go 1.26.7, html/template).
//! The expected strings were produced by bench/tmp/w3-rules/template/gohtml/main.go.

use std::collections::BTreeMap;

use template::html::Template;
use template::{IntKind, StrKind, Type, Value};

fn data() -> Value {
    let mut m: BTreeMap<String, Value> = BTreeMap::new();
    let s = |v: &str| Value::string(v);
    m.insert("s".into(), s("<script>alert('x')</script> O'Reilly & \"friends\""));
    m.insert("url".into(), s("http://a.b/c?d=e f&g=<h>"));
    m.insert("url2".into(), s("/img/b.png"));
    m.insert("js".into(), s("javascript:alert(1)"));
    m.insert("q".into(), s("a b&c=d/\u{e9}"));
    m.insert("n".into(), Value::Int(42, IntKind::Int));
    m.insert("f".into(), Value::Float(1.5));
    m.insert("big".into(), Value::Float(1e21));
    m.insert("tiny".into(), Value::Float(1e-7));
    m.insert("b".into(), Value::Bool(true));
    m.insert("nil".into(), Value::Invalid);
    m.insert("list".into(), Value::slice(Type::String(StrKind::String), vec![s("a"), s("<b>")]));
    let mut inner: BTreeMap<String, Value> = BTreeMap::new();
    inner.insert("k".into(), s("v<"));
    inner.insert("n".into(), Value::Int(1, IntKind::Int));
    m.insert("m".into(), Value::map(Type::Interface, inner));
    m.insert("color".into(), s("red"));
    m.insert("bad".into(), s("expression(alert(1))"));
    m.insert("attr".into(), s("checked"));
    m.insert("onattr".into(), s("onclick"));
    m.insert("html".into(), Value::String("<b>bold</b> & co".into(), StrKind::Html));
    m.insert("jsv".into(), Value::String("alert(1)".into(), StrKind::Js));
    m.insert("jsstr".into(), Value::String("a\\x\"b".into(), StrKind::JsStr));
    m.insert("safeurl".into(), Value::String("javascript:ok()".into(), StrKind::Url));
    m.insert("ls".into(), s("line\u{2028}sep"));
    m.insert("empty".into(), s(""));
    Value::map(Type::Interface, m)
}

#[test]
fn html_escaping_matches_go() {
    let cases: &[(&str, &str, &str)] = &[
        (r###"<p>{{.s}}</p>"###, r###"<p>&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt; O&#39;Reilly &amp; &#34;friends&#34;</p>"###, r###""###),
        (r###"<a href="{{.url}}">x</a>"###, r###"<a href="http://a.b/c?d=e%20f&amp;g=%3ch%3e">x</a>"###, r###""###),
        (r###"<a href="/search?q={{.q}}">x</a>"###, r###"<a href="/search?q=a%20b%26c%3dd%2f%c3%a9">x</a>"###, r###""###),
        (r###"<a href="{{.js}}">x</a>"###, r###"<a href="#ZgotmplZ">x</a>"###, r###""###),
        (r###"<a href="{{.safeurl}}">x</a>"###, r###"<a href="javascript:ok%28%29">x</a>"###, r###""###),
        (r###"<a title={{.s}}>x</a>"###, r###"<a title=&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt;&#32;O&#39;Reilly&#32;&amp;&#32;&#34;friends&#34;>x</a>"###, r###""###),
        (r###"<a title='{{.s}}'>x</a>"###, r###"<a title='&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt; O&#39;Reilly &amp; &#34;friends&#34;'>x</a>"###, r###""###),
        (r###"<a title="{{.empty}}" alt={{.empty}}>x</a>"###, r###"<a title="" alt=ZgotmplZ>x</a>"###, r###""###),
        (r###"<script>var a = {{.s}}; var b = {{.n}}; var c = {{.f}}; var d = {{.list}}; var e = {{.m}}; var g = {{.big}}; var h = {{.tiny}}; var i = {{.b}};</script>"###, r###"<script>var a = "\u003cscript\u003ealert('x')\u003c/script\u003e O'Reilly \u0026 \"friends\""; var b =  42 ; var c =  1.5 ; var d = ["a","\u003cb\u003e"]; var e = {"k":"v\u003c","n":1}; var g =  1e+21 ; var h =  1e-7 ; var i =  true ;</script>"###, r###""###),
        (r###"<script>var s = "{{.s}}";</script>"###, r###"<script>var s = "\u003cscript\u003ealert(\u0027x\u0027)\u003c\/script\u003e O\u0027Reilly \u0026 \u0022friends\u0022";</script>"###, r###""###),
        (r###"<script>var s = '{{.s}}';</script>"###, r###"<script>var s = '\u003cscript\u003ealert(\u0027x\u0027)\u003c\/script\u003e O\u0027Reilly \u0026 \u0022friends\u0022';</script>"###, r###""###),
        (r###"<script>var r = /{{.s}}/;</script>"###, r###"<script>var r = /\u003cscript\u003ealert\(\u0027x\u0027\)\u003c\/script\u003e O\u0027Reilly \u0026 \u0022friends\u0022/;</script>"###, r###""###),
        (r###"<script>var t = `{{.s}}`;</script>"###, r###"<script>var t = `\u003cscript\u003ealert(\u0027x\u0027)\u003c\/script\u003e O\u0027Reilly \u0026 \u0022friends\u0022`;</script>"###, r###""###),
        (r###"<script>var x = {{.nil}}; var y = {{.jsv}}; var z = {{.jsstr}}; var w = "{{.jsstr}}";</script>"###, r###"<script>var x =  null ; var y = alert(1); var z = "a\x"b"; var w = "a\x\u0022b";</script>"###, r###""###),
        (r###"<script>var l = "{{.ls}}"; var m = {{.ls}};</script>"###, r###"<script>var l = "line\u2028sep"; var m = "line\u2028sep";</script>"###, r###""###),
        (r###"<style>p { color: {{.color}}; }</style>"###, r###"<style>p { color: red; }</style>"###, r###""###),
        (r###"<style>p { color: {{.bad}}; }</style>"###, r###"<style>p { color: ZgotmplZ; }</style>"###, r###""###),
        (r###"<p style="color: {{.color}}">x</p>"###, r###"<p style="color: red">x</p>"###, r###""###),
        (r###"<div style="background: url('{{.url}}')">x</div>"###, r###"<div style="background: url('http://a.b/c?d=e%20f&amp;g=%3ch%3e')">x</div>"###, r###""###),
        (r###"<div style="background: url({{.url}})">x</div>"###, r###"<div style="background: url(http://a.b/c?d=e%20f&amp;g=%3ch%3e)">x</div>"###, r###""###),
        (r###"<style>p { font-family: "{{.s}}"; }</style>"###, r###"<style>p { font-family: "\3cscript\3e alert\28\27x\27\29\3c\2fscript\3e  O\27Reilly \26  \22 friends\22 "; }</style>"###, r###""###),
        (r###"<img srcset="{{.url}} 1x, {{.url2}} 2x">"###, r###"<img srcset="#ZgotmplZ 1x, /img/b.png 2x">"###, r###""###),
        (r###"<img srcset="{{.js}}">"###, r###"<img srcset="#ZgotmplZ">"###, r###""###),
        (r###"<textarea>{{.s}}</textarea>"###, r###"<textarea>&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt; O&#39;Reilly &amp; &#34;friends&#34;</textarea>"###, r###""###),
        (r###"<title>{{.s}} {{.html}}</title>"###, r###"<title>&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt; O&#39;Reilly &amp; &#34;friends&#34; &lt;b&gt;bold&lt;/b&gt; & co</title>"###, r###""###),
        (r###"<!-- {{.s}} -->x"###, r###"x"###, r###""###),
        (r###"<a onclick="f({{.s}})">x</a>"###, r###"<a onclick="f(&#34;\u003cscript\u003ealert(&#39;x&#39;)\u003c/script\u003e O&#39;Reilly \u0026 \&#34;friends\&#34;&#34;)">x</a>"###, r###""###),
        (r###"<a onclick='f("{{.s}}")'>x</a>"###, r###"<a onclick='f("\u003cscript\u003ealert(\u0027x\u0027)\u003c\/script\u003e O\u0027Reilly \u0026 \u0022friends\u0022")'>x</a>"###, r###""###),
        (r###"<input {{.attr}}="v">"###, r###"<input checked="v">"###, r###""###),
        (r###"<input {{.onattr}}="v">"###, r###"<input ZgotmplZ="v">"###, r###""###),
        (r###"<p>{{.html}}</p><a title="{{.html}}">x</a><a title={{.html}}>y</a>"###, r###"<p><b>bold</b> & co</p><a title="bold & co">x</a><a title=bold&#32;&&#32;co>y</a>"###, r###""###),
        (r###"{{if .b}}<a href="{{.url}}">{{else}}<a href="/x">{{end}}x</a>"###, r###"<a href="http://a.b/c?d=e%20f&amp;g=%3ch%3e">x</a>"###, r###""###),
        (r###"<ul>{{range .list}}<li>{{.}}</li>{{end}}</ul>"###, r###"<ul><li>a</li><li>&lt;b&gt;</li></ul>"###, r###""###),
        (r###"{{define "t"}}<b>{{.}}</b>{{end}}<p>{{template "t" .s}}</p><a title="{{template "t" .s}}">x</a>"###, r###"<p><b>&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt; O&#39;Reilly &amp; &#34;friends&#34;</b></p><a title="<b>&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt; O&#39;Reilly &amp; &#34;friends&#34;</b>">x</a>"###, r###""###),
        (r###"<a href="?q={{.q}}&r={{.s}}">x</a>"###, r###"<a href="?q=a%20b%26c%3dd%2f%c3%a9&r=%3cscript%3ealert%28%27x%27%29%3c%2fscript%3e%20O%27Reilly%20%26%20%22friends%22">x</a>"###, r###""###),
        (r###"<a href="{{.url}}#{{.s}}">x</a>"###, r###"<a href="http://a.b/c?d=e%20f&amp;g=%3ch%3e#%3cscript%3ealert%28%27x%27%29%3c%2fscript%3e%20O%27Reilly%20%26%20%22friends%22">x</a>"###, r###""###),
        (r###"<meta http-equiv="refresh" content="0;url={{.url}}">"###, r###"<meta http-equiv="refresh" content="0;url=http://a.b/c?d=e f&amp;g=&lt;h&gt;">"###, r###""###),
        (r###"<script type="text/template">{{.s}}</script>"###, r###"<script type="text/template">&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt; O&#39;Reilly &amp; &#34;friends&#34;</script>"###, r###""###),
        (r###"a < b {{.s}} <!DOCTYPE html>"###, r###"a &lt; b &lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt; O&#39;Reilly &amp; &#34;friends&#34; <!DOCTYPE html>"###, r###""###),
        (r###"<p>{{.s | html}}</p>"###, r###"<p>&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt; O&#39;Reilly &amp; &#34;friends&#34;</p>"###, r###""###),
        (r###"<p>{{html .s .n}}</p>"###, r###"<p>&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt; O&#39;Reilly &amp; &#34;friends&#34;42</p>"###, r###""###),
        (r###"<a href="/x?{{.q | urlquery}}">x</a>"###, r###"<a href="/x?a&#43;b%26c%3Dd%2F%C3%A9">x</a>"###, r###""###),
        (r###"{{.list}} {{.m}} {{.n}} {{.f}} {{.nil}} {{.b}}"###, r###"[a &lt;b&gt;] map[k:v&lt; n:1] 42 1.5  true"###, r###""###),
        (r###"{{printf "%q" .s}}"###, r###"&#34;&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt; O&#39;Reilly &amp; \&#34;friends\&#34;&#34;"###, r###""###),
        (r###"<script>{{range .list}}x = {{.}};{{end}}</script>"###, r###"<script>x = "a";x = "\u003cb\u003e";</script>"###, r###""###),
        (r###"<a href="{{.url}}"###, r###""###, r###"html/template:t0: ends in a non-text context: {stateURL delimDoubleQuote urlPartNone jsCtxRegexp [] attrURL elementNone <nil>}"###),
        (r###"{{if .b}}<a>{{else}}<b{{end}}"###, r###""###, r###"html/template:t0:1:5: {{if}} branches end in different contexts: {stateText delimNone urlPartNone jsCtxRegexp [] attrNone elementNone <nil>}, {stateTag delimNone urlPartNone jsCtxRegexp [] attrNone elementNone <nil>}"###),
        (r###"<script>var x = 1 / {{.n}} / 2</script>"###, r###"<script>var x = 1 /  42  / 2</script>"###, r###""###),
        (r###"<script>{{.s | urlquery | html}}</script>"###, r###""###, r###"html/template:t0:1:10: predefined escaper "urlquery" disallowed in template"###),
        (r###"<a title={{.s | html}}>x</a>"###, r###""###, r###"html/template:t0:1:11: predefined escaper "html" disallowed in template"###),
        (r###"{{template "nosuch"}}"###, r###""###, r###"html/template:t0:1:11: no such template "nosuch""###),
        (r###"<a href="{{if .b}}/x?{{else}}/y{{end}}{{.q}}">"###, r###""###, r###"html/template:t0:1:40: {{.q}} appears in an ambiguous context within a URL"###),
        (r###"<a b="c{{.s}}d"###, r###""###, r###"html/template:t0: ends in a non-text context: {stateAttr delimDoubleQuote urlPartNone jsCtxRegexp [] attrNone elementNone <nil>}"###),
        (r###"<script>var a = `${ {{.s}} }`;</script>"###, r###"<script>var a = `${ "\u003cscript\u003ealert('x')\u003c/script\u003e O'Reilly \u0026 \"friends\"" }`;</script>"###, r###""###),
        (r###"<script>var a = "\"###, r###""###, r###"html/template:t0: unfinished escape sequence in JS string: "\\""###),
        (r###"<a {{.s}}>"###, r###"<a ZgotmplZ>"###, r###""###),
    ];
    let mut failures = Vec::new();
    for (text, want_out, want_err) in cases {
        let t = Template::new("t0");
        let r = t.parse(text).and_then(|t| t.execute(&data()));
        let (out, err) = match r {
            Ok(out) => (out, String::new()),
            Err(e) => (String::new(), e),
        };
        if out != *want_out || err != *want_err {
            failures.push(format!("{text}\n  got:  {out:?} / {err:?}\n  want: {want_out:?} / {want_err:?}"));
        }
    }
    assert!(failures.is_empty(), "{} failures:\n{}", failures.len(), failures.join("\n"));
}
