//! Port of Go `html/template` content handling and escapers: `content.go`, `html.go`, `js.go`, `css.go`,
//! `url.go`, `attr.go`, and the `encoding/json` marshaling that `jsValEscaper` uses.

use std::fmt::Write;

use super::context::{Context, Delim, Element, JsCtx, State, is_in_tag};
use super::transition::transition;
use crate::gofmt::{append_float, sprint};
use crate::methods::stringer;
use crate::value::{IntKind, Kind, StrKind, Type, Value, from_slot};

/// Go `contentType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContentType {
    Plain,
    Css,
    Html,
    HtmlAttr,
    Js,
    JsStr,
    Url,
    Srcset,
    Unsafe,
}

/// The output of a filter that rejected its input.
pub const FILTER_FAILSAFE: &str = "ZgotmplZ";

/// Go `indirect` (content.go): follows pointers.
fn indirect_all(v: &Value) -> Value {
    let mut v = v.clone();
    while let Value::Ptr(inner) | Value::Iface(inner) = v {
        v = (*inner).clone();
    }
    v
}

/// Go `indirectToStringerOrError`.
fn indirect_to_stringer(v: &Value) -> Value {
    let mut v = v.clone();
    loop {
        if stringer(&v).is_some() {
            return v;
        }
        match v {
            Value::Ptr(inner) | Value::Iface(inner) => v = (*inner).clone(),
            _ => return v,
        }
    }
}

/// Go `stringify`.
pub fn stringify(args: &[Value]) -> (String, ContentType) {
    if args.len() == 1 {
        if let Value::String(s, k) = indirect_all(&args[0]) {
            let t = match k {
                StrKind::String => ContentType::Plain,
                StrKind::Css => ContentType::Css,
                StrKind::Html => ContentType::Html,
                StrKind::HtmlAttr => ContentType::HtmlAttr,
                StrKind::Js => ContentType::Js,
                StrKind::JsStr => ContentType::JsStr,
                StrKind::Url => ContentType::Url,
                StrKind::Srcset => ContentType::Srcset,
            };
            return (s.to_string(), t);
        }
    }
    let a: Vec<Value> = args.iter().filter(|a| a.is_valid()).map(indirect_to_stringer).collect();
    (sprint(&a), ContentType::Plain)
}

/// Go `evalArgs` (html/template): one string as is, else `fmt.Sprint` of the arguments.
pub fn eval_args(args: &[Value]) -> String {
    if args.len() == 1 {
        if let Value::String(s, StrKind::String) = &args[0] {
            return s.to_string();
        }
    }
    let a: Vec<Value> = args.iter().map(indirect_to_stringer).collect();
    sprint(&a)
}

// ---------------------------------------------------------------------------------------------------------------
// html.go

fn html_table(r: char, table: u8) -> Option<&'static str> {
    // table: 0 = htmlReplacementTable, 1 = htmlNormReplacementTable, 2 = htmlNospaceReplacementTable,
    // 3 = htmlNospaceNormReplacementTable.
    let nospace = table >= 2;
    let norm = table % 2 == 1;
    Some(match r {
        '\0' => {
            if nospace {
                "&#xfffd;"
            } else {
                "\u{FFFD}"
            }
        }
        '"' => "&#34;",
        '&' if !norm => "&amp;",
        '\'' => "&#39;",
        '+' => "&#43;",
        '<' => "&lt;",
        '>' => "&gt;",
        '\t' if nospace => "&#9;",
        '\n' if nospace => "&#10;",
        '\x0b' if nospace => "&#11;",
        '\x0c' if nospace => "&#12;",
        '\r' if nospace => "&#13;",
        ' ' if nospace => "&#32;",
        '=' if nospace => "&#61;",
        '`' if nospace => "&#96;",
        _ => return None,
    })
}

/// Go `htmlReplacer`.
fn html_replacer(s: &str, table: u8, bad_runes: bool) -> String {
    let table_len = if table >= 2 { '`' as u32 + 1 } else { '>' as u32 + 1 };
    let mut b = String::new();
    let mut written = 0;
    for (i, r) in s.char_indices() {
        let w = r.len_utf8();
        if (r as u32) < table_len {
            if let Some(repl) = html_table(r, table) {
                b.push_str(&s[written..i]);
                b.push_str(repl);
                written = i + w;
            }
        } else if bad_runes {
            // No-op: IE does not allow these ranges in unquoted attrs.
        } else if (0xfdd0..=0xfdef).contains(&(r as u32)) || (0xfff0..=0xffff).contains(&(r as u32)) {
            b.push_str(&s[written..i]);
            let _ = write!(b, "&#x{:x};", r as u32);
            written = i + w;
        }
    }
    if written == 0 {
        return s.to_string();
    }
    b.push_str(&s[written..]);
    b
}

/// Go `htmlNospaceEscaper`.
pub fn html_nospace_escaper(args: &[Value]) -> String {
    let (s, t) = stringify(args);
    if s.is_empty() {
        return FILTER_FAILSAFE.to_string();
    }
    if t == ContentType::Html {
        return html_replacer(&strip_tags(&s), 3, false);
    }
    html_replacer(&s, 2, false)
}

/// Go `attrEscaper`.
pub fn attr_escaper(args: &[Value]) -> String {
    let (s, t) = stringify(args);
    if t == ContentType::Html {
        return html_replacer(&strip_tags(&s), 1, true);
    }
    html_replacer(&s, 0, true)
}

/// Go `rcdataEscaper`.
pub fn rcdata_escaper(args: &[Value]) -> String {
    let (s, t) = stringify(args);
    if t == ContentType::Html {
        return html_replacer(&s, 1, true);
    }
    html_replacer(&s, 0, true)
}

/// Go `htmlEscaper`.
pub fn html_escaper(args: &[Value]) -> String {
    let (s, t) = stringify(args);
    if t == ContentType::Html {
        return s;
    }
    html_replacer(&s, 0, true)
}

fn delim_ends(d: Delim) -> &'static [u8] {
    match d {
        Delim::DoubleQuote => b"\"",
        Delim::SingleQuote => b"'",
        Delim::SpaceOrTagEnd => b" \t\n\x0c\r>",
        Delim::None => b"",
    }
}

/// Go `stripTags`.
pub fn strip_tags(html: &str) -> String {
    let mut b: Vec<u8> = Vec::new();
    let s = html.as_bytes();
    let mut c = Context::default();
    let mut i = 0;
    let mut all_text = true;
    while i != s.len() {
        if c.delim == Delim::None {
            let mut st = c.state;
            if c.element != Element::None && !is_in_tag(st) {
                st = State::Rcdata;
            }
            let mut cc = c.clone();
            cc.state = st;
            let (d, nread) = transition(cc, &s[i..]);
            let i1 = i + nread;
            if c.state == State::Text || c.state == State::Rcdata {
                let mut j = i1;
                if d.state != c.state {
                    for j1 in (i..j).rev() {
                        if s[j1] == b'<' {
                            j = j1;
                            break;
                        }
                    }
                }
                b.extend_from_slice(&s[i..j]);
            } else {
                all_text = false;
            }
            c = d;
            i = i1;
            continue;
        }
        let Some(off) = s[i..].iter().position(|x| delim_ends(c.delim).contains(x)) else { break };
        let mut i1 = i + off;
        if c.delim != Delim::SpaceOrTagEnd {
            i1 += 1;
        }
        c = Context { state: State::Tag, element: c.element, ..Default::default() };
        i = i1;
    }
    if all_text {
        return html.to_string();
    } else if c.state == State::Text || c.state == State::Rcdata {
        b.extend_from_slice(&s[i..]);
    }
    String::from_utf8_lossy(&b).into_owned()
}

/// Go `htmlNameFilter`.
pub fn html_name_filter(args: &[Value]) -> String {
    let (s, t) = stringify(args);
    if t == ContentType::HtmlAttr {
        return s;
    }
    if s.is_empty() {
        return FILTER_FAILSAFE.to_string();
    }
    let s = s.to_lowercase();
    if attr_type(&s) != ContentType::Plain {
        return FILTER_FAILSAFE.to_string();
    }
    if !s.chars().all(|r| r.is_ascii_digit() || r.is_ascii_lowercase()) {
        return FILTER_FAILSAFE.to_string();
    }
    s
}

/// Go `commentEscaper`.
pub fn comment_escaper(_args: &[Value]) -> String {
    String::new()
}

// ---------------------------------------------------------------------------------------------------------------
// js.go

const JS_WHITESPACE: &[char] = &[
    '\x0c', '\n', '\r', '\t', '\x0b', ' ', '\u{a0}', '\u{1680}', '\u{2000}', '\u{2001}', '\u{2002}', '\u{2003}',
    '\u{2004}', '\u{2005}', '\u{2006}', '\u{2007}', '\u{2008}', '\u{2009}', '\u{200a}', '\u{2028}', '\u{2029}',
    '\u{202f}', '\u{205f}', '\u{3000}', '\u{feff}',
];

/// Go `nextJSCtx`.
pub fn next_js_ctx(s: &[u8], preceding: JsCtx) -> JsCtx {
    let st = String::from_utf8_lossy(s);
    let s = st.trim_end_matches(JS_WHITESPACE).as_bytes();
    if s.is_empty() {
        return preceding;
    }
    let n = s.len();
    let c = s[n - 1];
    match c {
        b'+' | b'-' => {
            let mut start = n - 1;
            while start > 0 && s[start - 1] == c {
                start -= 1;
            }
            if (n - start) & 1 == 1 {
                return JsCtx::Regexp;
            }
            JsCtx::DivOp
        }
        b'.' => {
            if n != 1 && s[n - 2].is_ascii_digit() {
                return JsCtx::DivOp;
            }
            JsCtx::Regexp
        }
        b',' | b'<' | b'>' | b'=' | b'*' | b'%' | b'&' | b'|' | b'^' | b'?' | b'!' | b'~' | b'(' | b'[' | b':' | b';'
        | b'{' | b'}' => JsCtx::Regexp,
        _ => {
            let mut j = n;
            while j > 0 && is_js_ident_part(s[j - 1] as char) {
                j -= 1;
            }
            if matches!(
                &s[j..],
                b"break"
                    | b"case"
                    | b"continue"
                    | b"delete"
                    | b"do"
                    | b"else"
                    | b"finally"
                    | b"in"
                    | b"instanceof"
                    | b"return"
                    | b"throw"
                    | b"try"
                    | b"typeof"
                    | b"void"
            ) {
                return JsCtx::Regexp;
            }
            JsCtx::DivOp
        }
    }
}

/// Go `isJSIdentPart`.
pub fn is_js_ident_part(r: char) -> bool {
    r == '$' || r.is_ascii_digit() || r.is_ascii_uppercase() || r == '_' || r.is_ascii_lowercase()
}

/// Go `isJSType`.
pub fn is_js_type(mime_type: &str) -> bool {
    let m = mime_type.split(';').next().unwrap_or("").to_lowercase();
    matches!(
        m.trim(),
        "" | "application/ecmascript"
            | "application/javascript"
            | "application/json"
            | "application/ld+json"
            | "application/x-ecmascript"
            | "application/x-javascript"
            | "module"
            | "text/ecmascript"
            | "text/javascript"
            | "text/javascript1.0"
            | "text/javascript1.1"
            | "text/javascript1.2"
            | "text/javascript1.3"
            | "text/javascript1.4"
            | "text/javascript1.5"
            | "text/jscript"
            | "text/livescript"
            | "text/x-ecmascript"
            | "text/x-javascript"
    )
}

/// Go `indirectToJSONMarshaler`: follows pointers until a value that marshals itself (time.Time).
fn indirect_to_json_marshaler(v: &Value) -> Value {
    let mut v = v.clone();
    loop {
        match v {
            Value::Ptr(inner) if !matches!(*inner, Value::Time(_)) => v = (*inner).clone(),
            Value::Iface(inner) => v = (*inner).clone(),
            _ => return v,
        }
    }
}

/// Go `jsValEscaper`.
pub fn js_val_escaper(args: &[Value]) -> String {
    let a = if args.len() == 1 {
        let a = indirect_to_json_marshaler(&args[0]);
        match &a {
            Value::String(t, StrKind::Js) => return t.to_string(),
            Value::String(t, StrKind::JsStr) => return format!("\"{}\"", t),
            Value::Time(_) => a.clone(),
            Value::Ptr(p) if matches!(**p, Value::Time(_)) => a.clone(),
            // *FloatHistogram has String with a pointer receiver; the struct value marshals as a struct.
            Value::Histogram(_) => a.clone(),
            _ => match stringer(&a) {
                Some(s) => Value::string(s),
                None => a.clone(),
            },
        }
    } else {
        let v: Vec<Value> = args.iter().map(indirect_to_json_marshaler).collect();
        Value::string(sprint(&v))
    };
    let b = match json_marshal(&a) {
        Ok(b) => b,
        Err(err_str) => {
            let mut e = script_tag_re_replace(&err_str);
            e = e.replace("*/", "* /");
            e = e.replace("<!--", "\\x3C!--");
            return format!(" /* {} */null ", e);
        }
    };
    if b.is_empty() {
        return " null ".to_string();
    }
    let first = b.chars().next().unwrap_or(' ');
    let last = b.chars().next_back().unwrap_or(' ');
    let pad = is_js_ident_part(first) || is_js_ident_part(last);
    let mut buf = String::new();
    if pad {
        buf.push(' ');
    }
    let mut written = 0;
    for (i, r) in b.char_indices() {
        let repl = match r {
            '\u{2028}' => "\\u2028",
            '\u{2029}' => "\\u2029",
            _ => continue,
        };
        buf.push_str(&b[written..i]);
        buf.push_str(repl);
        written = i + r.len_utf8();
    }
    if !buf.is_empty() {
        buf.push_str(&b[written..]);
        if pad {
            buf.push(' ');
        }
        return buf;
    }
    b
}

/// `(?i)<(/?)script` replaced by `\x3C${1}script`.
fn script_tag_re_replace(s: &str) -> String {
    let lower = s.to_ascii_lowercase();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    let b = s.as_bytes();
    while i < b.len() {
        if b[i] == b'<' {
            let rest = &lower[i + 1..];
            if rest.starts_with("script") {
                out.push_str("\\x3Cscript");
                i += 7;
                continue;
            }
            if rest.starts_with("/script") {
                out.push_str("\\x3C/script");
                i += 8;
                continue;
            }
        }
        let ch = s[i..].chars().next().unwrap_or('\u{FFFD}');
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

fn low_unicode(r: u32) -> Option<String> {
    Some(match r {
        7 => "\\u0007".to_string(),
        8 => "\\u0008".to_string(),
        9 => "\\t".to_string(),
        10 => "\\n".to_string(),
        11 => "\\u000b".to_string(),
        12 => "\\f".to_string(),
        13 => "\\r".to_string(),
        0..=0x1f => format!("\\u{:04x}", r),
        _ => return None,
    })
}

#[derive(Clone, Copy)]
enum JsTable {
    Str,
    BqStr,
    StrNorm,
    Regexp,
}

fn js_table(r: char, t: JsTable) -> Option<&'static str> {
    let common = match r {
        '"' => Some("\\u0022"),
        '&' => Some("\\u0026"),
        '\'' => Some("\\u0027"),
        '+' => Some("\\u002b"),
        '/' => Some("\\/"),
        '<' => Some("\\u003c"),
        '>' => Some("\\u003e"),
        _ => None,
    };
    match t {
        JsTable::Str => common.or(match r {
            '`' => Some("\\u0060"),
            '\\' => Some("\\\\"),
            _ => None,
        }),
        JsTable::BqStr => common.or(match r {
            '`' => Some("\\u0060"),
            '\\' => Some("\\\\"),
            '$' => Some("\\u0024"),
            '{' => Some("\\u007b"),
            '}' => Some("\\u007d"),
            _ => None,
        }),
        JsTable::StrNorm => common.or(match r {
            '`' => Some("\\u0060"),
            _ => None,
        }),
        JsTable::Regexp => match r {
            '"' => Some("\\u0022"),
            '$' => Some("\\$"),
            '&' => Some("\\u0026"),
            '\'' => Some("\\u0027"),
            '(' => Some("\\("),
            ')' => Some("\\)"),
            '*' => Some("\\*"),
            '+' => Some("\\u002b"),
            '-' => Some("\\-"),
            '.' => Some("\\."),
            '/' => Some("\\/"),
            '<' => Some("\\u003c"),
            '>' => Some("\\u003e"),
            '?' => Some("\\?"),
            '[' => Some("\\["),
            '\\' => Some("\\\\"),
            ']' => Some("\\]"),
            '^' => Some("\\^"),
            '{' => Some("\\{"),
            '|' => Some("\\|"),
            '}' => Some("\\}"),
            _ => None,
        },
    }
}

/// Go `replace` (js.go).
fn replace(s: &str, t: JsTable) -> String {
    let mut b = String::new();
    let mut written = 0;
    for (i, r) in s.char_indices() {
        let repl: String = if let Some(x) = low_unicode(r as u32) {
            x
        } else if let Some(x) = js_table(r, t) {
            x.to_string()
        } else if r == '\u{2028}' {
            "\\u2028".to_string()
        } else if r == '\u{2029}' {
            "\\u2029".to_string()
        } else {
            continue;
        };
        b.push_str(&s[written..i]);
        b.push_str(&repl);
        written = i + r.len_utf8();
    }
    if written == 0 {
        return s.to_string();
    }
    b.push_str(&s[written..]);
    b
}

/// Go `jsStrEscaper`.
pub fn js_str_escaper(args: &[Value]) -> String {
    let (s, t) = stringify(args);
    if t == ContentType::JsStr {
        return replace(&s, JsTable::StrNorm);
    }
    replace(&s, JsTable::Str)
}

/// Go `jsTmplLitEscaper`.
pub fn js_tmpl_lit_escaper(args: &[Value]) -> String {
    let (s, _) = stringify(args);
    replace(&s, JsTable::BqStr)
}

/// Go `jsRegexpEscaper`.
pub fn js_regexp_escaper(args: &[Value]) -> String {
    let (s, _) = stringify(args);
    let s = replace(&s, JsTable::Regexp);
    if s.is_empty() {
        return "(?:)".to_string();
    }
    s
}

// ---------------------------------------------------------------------------------------------------------------
// encoding/json

/// Go `json.Marshal` for template values. The error is Go's error text.
pub fn json_marshal(v: &Value) -> Result<String, String> {
    let mut out = String::new();
    json_value(&mut out, v)?;
    Ok(out)
}

fn json_string(out: &mut String, s: &str) {
    const HEX: &[u8] = b"0123456789abcdef";
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '<' | '>' | '&' => {
                out.push_str("\\u00");
                out.push(HEX[(c as usize) >> 4] as char);
                out.push(HEX[(c as usize) & 0xF] as char);
            }
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => {
                out.push_str("\\u00");
                out.push(HEX[(c as usize) >> 4] as char);
                out.push(HEX[(c as usize) & 0xF] as char);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn json_float(out: &mut String, f: f64) -> Result<(), String> {
    if f.is_nan() || f.is_infinite() {
        let mut s = String::new();
        append_float(&mut s, f, b'g', -1);
        return Err(format!("json: unsupported value: {}", s));
    }
    let abs = f.abs();
    let fmt = if abs != 0.0 && (abs < 1e-6 || abs >= 1e21) { b'e' } else { b'f' };
    let mut b = String::new();
    append_float(&mut b, f, fmt, -1);
    if fmt == b'e' {
        let n = b.len();
        let bytes = b.as_bytes();
        if n >= 4 && bytes[n - 4] == b'e' && bytes[n - 3] == b'-' && bytes[n - 2] == b'0' {
            let last = bytes[n - 1] as char;
            b.truncate(n - 2);
            b.push(last);
        }
    }
    out.push_str(&b);
    Ok(())
}

fn json_value(out: &mut String, v: &Value) -> Result<(), String> {
    match v {
        Value::Invalid => out.push_str("null"),
        Value::Nil(_) => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Int(i, _) => {
            let _ = write!(out, "{}", i);
        }
        Value::Uint(u, _) => {
            let _ = write!(out, "{}", u);
        }
        Value::Float(f) => json_float(out, *f)?,
        Value::Complex(..) => return Err("json: unsupported type: complex128".to_string()),
        Value::String(s, _) => json_string(out, s),
        Value::Slice(s) => {
            if let Some(Type::Uint(crate::value::UintKind::Uint8)) = s.ty.elem() {
                let bytes: Vec<u8> = s.items.iter().map(|v| if let Value::Uint(u, _) = v { *u as u8 } else { 0 }).collect();
                out.push('"');
                out.push_str(&base64(&bytes));
                out.push('"');
                return Ok(());
            }
            let elem = s.ty.elem().cloned().unwrap_or(Type::Interface);
            out.push('[');
            for (i, item) in s.items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                json_value(out, &from_slot(item, &elem))?;
            }
            out.push(']');
        }
        Value::Map(m) => {
            let elem = m.ty.elem().cloned().unwrap_or(Type::Interface);
            out.push('{');
            for (i, (k, item)) in m.entries.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                json_string(out, k);
                out.push(':');
                json_value(out, &from_slot(item, &elem))?;
            }
            out.push('}');
        }
        Value::Struct(s) => {
            out.push('{');
            let mut first = true;
            for ((name, ty), item) in s.ty.fields.iter().zip(s.fields.iter()) {
                if !name.starts_with(|c: char| c.is_uppercase()) {
                    continue;
                }
                if !first {
                    out.push(',');
                }
                first = false;
                json_string(out, name);
                out.push(':');
                json_value(out, &from_slot(item, ty))?;
            }
            out.push('}');
        }
        Value::Ptr(inner) | Value::Iface(inner) => json_value(out, inner)?,
        Value::Time(t) => {
            out.push('"');
            out.push_str(&t.format("2006-01-02T15:04:05.999999999Z07:00"));
            out.push('"');
        }
        Value::Histogram(h) => {
            out.push_str("{\"CounterResetHint\":");
            let _ = write!(out, "{}", h.counter_reset_hint as u8);
            let _ = write!(out, ",\"Schema\":{}", h.schema);
            out.push_str(",\"ZeroThreshold\":");
            json_float(out, h.zero_threshold)?;
            out.push_str(",\"ZeroCount\":");
            json_float(out, h.zero_count)?;
            out.push_str(",\"Count\":");
            json_float(out, h.count)?;
            out.push_str(",\"Sum\":");
            json_float(out, h.sum)?;
            let spans = |out: &mut String, spans: &[model::histogram::Span]| {
                if spans.is_empty() {
                    out.push_str("null");
                    return;
                }
                out.push('[');
                for (i, s) in spans.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    let _ = write!(out, "{{\"Offset\":{},\"Length\":{}}}", s.offset, s.length);
                }
                out.push(']');
            };
            out.push_str(",\"PositiveSpans\":");
            spans(out, &h.positive_spans);
            out.push_str(",\"NegativeSpans\":");
            spans(out, &h.negative_spans);
            let floats = |out: &mut String, v: &[f64]| -> Result<(), String> {
                if v.is_empty() {
                    out.push_str("null");
                    return Ok(());
                }
                out.push('[');
                for (i, f) in v.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    json_float(out, *f)?;
                }
                out.push(']');
                Ok(())
            };
            out.push_str(",\"PositiveBuckets\":");
            floats(out, &h.positive_buckets)?;
            out.push_str(",\"NegativeBuckets\":");
            floats(out, &h.negative_buckets)?;
            out.push_str(",\"CustomValues\":");
            match &h.custom_values {
                Some(cv) => {
                    out.push('[');
                    for (i, f) in cv.iter().enumerate() {
                        if i > 0 {
                            out.push(',');
                        }
                        json_float(out, *f)?;
                    }
                    out.push(']');
                }
                None => out.push_str("null"),
            }
            out.push('}');
        }
        Value::Func(f) => {
            return Err(format!("json: unsupported type: {}", Type::Func(f.sig.clone())));
        }
    }
    Ok(())
}

fn base64(b: &[u8]) -> String {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(b.len().div_ceil(3) * 4);
    for chunk in b.chunks(3) {
        let n = match chunk.len() {
            3 => (chunk[0] as u32) << 16 | (chunk[1] as u32) << 8 | chunk[2] as u32,
            2 => (chunk[0] as u32) << 16 | (chunk[1] as u32) << 8,
            _ => (chunk[0] as u32) << 16,
        };
        out.push(T[(n >> 18) as usize & 63] as char);
        out.push(T[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { T[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { T[n as usize & 63] as char } else { '=' });
    }
    out
}

// ---------------------------------------------------------------------------------------------------------------
// css.go

/// Go `endsWithCSSKeyword`.
pub fn ends_with_css_keyword(b: &[u8], kw: &str) -> bool {
    if b.len() < kw.len() {
        return false;
    }
    let i = b.len() - kw.len();
    if i != 0 {
        let prefix = String::from_utf8_lossy(&b[..i]);
        if let Some(r) = prefix.chars().next_back() {
            if is_css_nmchar(r) {
                return false;
            }
        }
    }
    b[i..].to_ascii_lowercase() == kw.as_bytes()
}

/// Go `isCSSNmchar`.
pub fn is_css_nmchar(r: char) -> bool {
    let r = r as u32;
    (b'a' as u32..=b'z' as u32).contains(&r)
        || (b'A' as u32..=b'Z' as u32).contains(&r)
        || (b'0' as u32..=b'9' as u32).contains(&r)
        || r == b'-' as u32
        || r == b'_' as u32
        || (0x80..=0xd7ff).contains(&r)
        || (0xe000..=0xfffd).contains(&r)
        || (0x10000..=0x10ffff).contains(&r)
}

fn is_hex(c: u8) -> bool {
    c.is_ascii_hexdigit()
}

fn hex_decode(s: &[u8]) -> u32 {
    let mut n = 0u32;
    for &c in s {
        n <<= 4;
        n |= match c {
            b'0'..=b'9' => (c - b'0') as u32,
            b'a'..=b'f' => (c - b'a') as u32 + 10,
            _ => (c - b'A') as u32 + 10,
        };
    }
    n
}

fn skip_css_space(c: &[u8]) -> &[u8] {
    if c.is_empty() {
        return c;
    }
    match c[0] {
        b'\t' | b'\n' | b'\x0c' | b' ' => &c[1..],
        b'\r' => {
            if c.len() >= 2 && c[1] == b'\n' {
                &c[2..]
            } else {
                &c[1..]
            }
        }
        _ => c,
    }
}

fn is_css_space(b: u8) -> bool {
    matches!(b, b'\t' | b'\n' | b'\x0c' | b'\r' | b' ')
}

/// Go `decodeCSS`.
pub fn decode_css(s: &[u8]) -> Vec<u8> {
    if !s.contains(&b'\\') {
        return s.to_vec();
    }
    let mut b = Vec::with_capacity(s.len());
    let mut s = s;
    while !s.is_empty() {
        let i = s.iter().position(|&c| c == b'\\').unwrap_or(s.len());
        b.extend_from_slice(&s[..i]);
        s = &s[i..];
        if s.len() < 2 {
            break;
        }
        if is_hex(s[1]) {
            let mut j = 2;
            while j < s.len() && j < 7 && is_hex(s[j]) {
                j += 1;
            }
            let mut r = hex_decode(&s[1..j]);
            if r > 0x10FFFF {
                r /= 16;
                j -= 1;
            }
            let ch = char::from_u32(r).unwrap_or('\u{FFFD}');
            let mut buf = [0u8; 4];
            b.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
            s = skip_css_space(&s[j..]);
        } else {
            let rest = String::from_utf8_lossy(&s[1..]);
            let n = rest.chars().next().map_or(1, |c| c.len_utf8()).min(s.len() - 1);
            b.extend_from_slice(&s[1..1 + n]);
            s = &s[1 + n..];
        }
    }
    b
}

fn css_replacement(r: char) -> Option<&'static str> {
    Some(match r {
        '\0' => "\\0",
        '\t' => "\\9",
        '\n' => "\\a",
        '\x0c' => "\\c",
        '\r' => "\\d",
        '"' => "\\22",
        '&' => "\\26",
        '\'' => "\\27",
        '(' => "\\28",
        ')' => "\\29",
        '+' => "\\2b",
        '/' => "\\2f",
        ':' => "\\3a",
        ';' => "\\3b",
        '<' => "\\3c",
        '>' => "\\3e",
        '\\' => "\\\\",
        '{' => "\\7b",
        '}' => "\\7d",
        _ => return None,
    })
}

/// Go `cssEscaper`.
pub fn css_escaper(args: &[Value]) -> String {
    let (s, _) = stringify(args);
    let mut b = String::new();
    let mut written = 0;
    let sb = s.as_bytes();
    for (i, r) in s.char_indices() {
        let Some(repl) = css_replacement(r) else { continue };
        b.push_str(&s[written..i]);
        b.push_str(repl);
        written = i + r.len_utf8();
        if repl != "\\\\" && (written == s.len() || is_hex(sb[written]) || is_css_space(sb[written])) {
            b.push(' ');
        }
    }
    if written == 0 {
        return s;
    }
    b.push_str(&s[written..]);
    b
}

/// Go `cssValueFilter`.
pub fn css_value_filter(args: &[Value]) -> String {
    let (s, t) = stringify(args);
    if t == ContentType::Css {
        return s;
    }
    let b = decode_css(s.as_bytes());
    let mut id: Vec<u8> = Vec::with_capacity(64);
    for (i, &c) in b.iter().enumerate() {
        match c {
            0 | b'"' | b'\'' | b'(' | b')' | b'/' | b';' | b'@' | b'[' | b'\\' | b']' | b'`' | b'{' | b'}' | b'<'
            | b'>' => return FILTER_FAILSAFE.to_string(),
            b'-' => {
                if i != 0 && b[i - 1] == b'-' {
                    return FILTER_FAILSAFE.to_string();
                }
            }
            _ => {
                if c < 0x80 && is_css_nmchar(c as char) {
                    id.push(c);
                }
            }
        }
    }
    let id = id.to_ascii_lowercase();
    let contains = |hay: &[u8], needle: &[u8]| hay.windows(needle.len()).any(|w| w == needle);
    if contains(&id, b"expression") || contains(&id, b"mozbinding") {
        return FILTER_FAILSAFE.to_string();
    }
    String::from_utf8_lossy(&b).into_owned()
}

// ---------------------------------------------------------------------------------------------------------------
// url.go

/// Go `urlFilter`.
pub fn url_filter(args: &[Value]) -> String {
    let (s, t) = stringify(args);
    if t == ContentType::Url {
        return s;
    }
    if !is_safe_url(&s) {
        return format!("#{}", FILTER_FAILSAFE);
    }
    s
}

fn is_safe_url(s: &str) -> bool {
    if let Some((protocol, _)) = s.split_once(':') {
        if !protocol.contains('/')
            && !protocol.eq_ignore_ascii_case("http")
            && !protocol.eq_ignore_ascii_case("https")
            && !protocol.eq_ignore_ascii_case("mailto")
        {
            return false;
        }
    }
    true
}

/// Go `urlEscaper`.
pub fn url_escaper(args: &[Value]) -> String {
    url_processor(false, args)
}

/// Go `urlNormalizer`.
pub fn url_normalizer(args: &[Value]) -> String {
    url_processor(true, args)
}

fn url_processor(mut norm: bool, args: &[Value]) -> String {
    let (s, t) = stringify(args);
    if t == ContentType::Url {
        norm = true;
    }
    let mut b = String::new();
    if process_url_onto(&s, norm, &mut b) {
        return b;
    }
    s
}

fn process_url_onto(s: &str, norm: bool, b: &mut String) -> bool {
    let sb = s.as_bytes();
    let mut written = 0;
    let n = sb.len();
    let mut i = 0;
    while i < n {
        let c = sb[i];
        let keep = match c {
            b'!' | b'#' | b'$' | b'&' | b'*' | b'+' | b',' | b'/' | b':' | b';' | b'=' | b'?' | b'@' | b'[' | b']' => norm,
            b'-' | b'.' | b'_' | b'~' => true,
            b'%' => norm && i + 2 < n && is_hex(sb[i + 1]) && is_hex(sb[i + 2]),
            _ => c.is_ascii_alphanumeric(),
        };
        if !keep {
            b.push_str(&String::from_utf8_lossy(&sb[written..i]));
            let _ = write!(b, "%{:02x}", c);
            written = i + 1;
        }
        i += 1;
    }
    b.push_str(&String::from_utf8_lossy(&sb[written..]));
    written != 0
}

/// Go `srcsetFilterAndEscaper`.
pub fn srcset_filter_and_escaper(args: &[Value]) -> String {
    let (s, t) = stringify(args);
    match t {
        ContentType::Srcset => return s,
        ContentType::Url => {
            let mut b = String::new();
            let s = if process_url_onto(&s, true, &mut b) { b } else { s };
            return s.replace(',', "%2c");
        }
        _ => {}
    }
    let mut b = String::new();
    let mut written = 0;
    let sb = s.as_bytes();
    for i in 0..sb.len() {
        if sb[i] == b',' {
            filter_srcset_element(&s, written, i, &mut b);
            b.push(',');
            written = i + 1;
        }
    }
    filter_srcset_element(&s, written, sb.len(), &mut b);
    b
}

fn is_html_space(c: u8) -> bool {
    matches!(c, b'\t' | b'\n' | b'\x0c' | b'\r' | b' ')
}

fn is_html_space_or_ascii_alnum(c: u8) -> bool {
    is_html_space(c) || c.is_ascii_alphanumeric()
}

fn filter_srcset_element(s: &str, left: usize, right: usize, b: &mut String) {
    let sb = s.as_bytes();
    let mut start = left;
    while start < right && is_html_space(sb[start]) {
        start += 1;
    }
    let mut end = right;
    for i in start..right {
        if is_html_space(sb[i]) {
            end = i;
            break;
        }
    }
    let url = &s[start..end];
    if is_safe_url(url) {
        let metadata_ok = (end..right).all(|i| is_html_space_or_ascii_alnum(sb[i]));
        if metadata_ok {
            b.push_str(&s[left..start]);
            let mut u = String::new();
            if process_url_onto(url, true, &mut u) {
                b.push_str(&u);
            } else {
                b.push_str(url);
            }
            b.push_str(&s[end..right]);
            return;
        }
    }
    b.push('#');
    b.push_str(FILTER_FAILSAFE);
}

// ---------------------------------------------------------------------------------------------------------------
// attr.go

fn attr_type_map(name: &str) -> Option<ContentType> {
    use ContentType::*;
    Some(match name {
        "accept" => Plain,
        "accept-charset" => Unsafe,
        "action" => Url,
        "alt" => Plain,
        "archive" => Url,
        "async" => Unsafe,
        "autocomplete" => Plain,
        "autofocus" => Plain,
        "autoplay" => Plain,
        "background" => Url,
        "border" => Plain,
        "checked" => Plain,
        "cite" => Url,
        "challenge" => Unsafe,
        "charset" => Unsafe,
        "class" => Plain,
        "classid" => Url,
        "codebase" => Url,
        "cols" => Plain,
        "colspan" => Plain,
        "content" => Unsafe,
        "contenteditable" => Plain,
        "contextmenu" => Plain,
        "controls" => Plain,
        "coords" => Plain,
        "crossorigin" => Unsafe,
        "data" => Url,
        "datetime" => Plain,
        "default" => Plain,
        "defer" => Unsafe,
        "dir" => Plain,
        "dirname" => Plain,
        "disabled" => Plain,
        "draggable" => Plain,
        "dropzone" => Plain,
        "enctype" => Unsafe,
        "for" => Plain,
        "form" => Unsafe,
        "formaction" => Url,
        "formenctype" => Unsafe,
        "formmethod" => Unsafe,
        "formnovalidate" => Unsafe,
        "formtarget" => Plain,
        "headers" => Plain,
        "height" => Plain,
        "hidden" => Plain,
        "high" => Plain,
        "href" => Url,
        "hreflang" => Plain,
        "http-equiv" => Unsafe,
        "icon" => Url,
        "id" => Plain,
        "ismap" => Plain,
        "keytype" => Unsafe,
        "kind" => Plain,
        "label" => Plain,
        "lang" => Plain,
        "language" => Unsafe,
        "list" => Plain,
        "longdesc" => Url,
        "loop" => Plain,
        "low" => Plain,
        "manifest" => Url,
        "max" => Plain,
        "maxlength" => Plain,
        "media" => Plain,
        "mediagroup" => Plain,
        "method" => Unsafe,
        "min" => Plain,
        "multiple" => Plain,
        "name" => Plain,
        "novalidate" => Unsafe,
        "open" => Plain,
        "optimum" => Plain,
        "pattern" => Unsafe,
        "placeholder" => Plain,
        "poster" => Url,
        "profile" => Url,
        "preload" => Plain,
        "pubdate" => Plain,
        "radiogroup" => Plain,
        "readonly" => Plain,
        "rel" => Unsafe,
        "required" => Plain,
        "reversed" => Plain,
        "rows" => Plain,
        "rowspan" => Plain,
        "sandbox" => Unsafe,
        "spellcheck" => Plain,
        "scope" => Plain,
        "scoped" => Plain,
        "seamless" => Plain,
        "selected" => Plain,
        "shape" => Plain,
        "size" => Plain,
        "sizes" => Plain,
        "span" => Plain,
        "src" => Url,
        "srcdoc" => Html,
        "srclang" => Plain,
        "srcset" => Srcset,
        "start" => Plain,
        "step" => Plain,
        "style" => Css,
        "tabindex" => Plain,
        "target" => Plain,
        "title" => Plain,
        "type" => Unsafe,
        "usemap" => Url,
        "value" => Unsafe,
        "width" => Plain,
        "wrap" => Plain,
        "xmlns" => Url,
        _ => return None,
    })
}

/// Go `attrType`.
pub fn attr_type(name: &str) -> ContentType {
    let mut name = name;
    if let Some(rest) = name.strip_prefix("data-") {
        name = rest;
    } else if let Some((prefix, short)) = name.split_once(':') {
        if prefix == "xmlns" {
            return ContentType::Url;
        }
        name = short;
    }
    if let Some(t) = attr_type_map(name) {
        return t;
    }
    if name.starts_with("on") {
        return ContentType::Js;
    }
    if name.contains("src") || name.contains("uri") || name.contains("url") {
        return ContentType::Url;
    }
    ContentType::Plain
}

#[allow(dead_code)]
fn _kinds(v: &Value) -> bool {
    v.kind() == Kind::Ptr || matches!(v, Value::Int(_, IntKind::Duration))
}
