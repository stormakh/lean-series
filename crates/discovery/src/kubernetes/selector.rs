//! Kubernetes label selectors (k8s.io/apimachinery `labels.Parse`) and field selectors (`fields.ParseSelector`):
//! parsing with the same errors, and label matching.
use std::sync::OnceLock;

use super::api::{StrMap, map_get};

/// A field selector term.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldTerm {
    pub field: String,
    pub value: String,
    pub not: bool,
}

/// Go `fields.UnescapeValue`.
fn unescape_field_value(s: &str) -> Result<String, String> {
    if !s.contains(['\\', ',', '=']) {
        return Ok(s.to_string());
    }
    let mut v = String::with_capacity(s.len());
    let mut in_slash = false;
    for c in s.chars() {
        if in_slash {
            match c {
                '\\' | ',' | '=' => v.push(c),
                _ => return Err(format!("invalid field selector: invalid escape sequence: \\{c}")),
            }
            in_slash = false;
            continue;
        }
        match c {
            '\\' => in_slash = true,
            ',' | '=' => return Err(format!("invalid field selector: unescaped character in value: {}", c as u32)),
            _ => v.push(c),
        }
    }
    if in_slash {
        return Err("invalid field selector: invalid escape sequence: \\".to_string());
    }
    Ok(v)
}

/// Go `fields.ParseSelector`.
pub fn parse_field_selector(selector: &str) -> Result<Vec<FieldTerm>, String> {
    let mut parts: Vec<&str> = Vec::new();
    if !selector.is_empty() {
        let mut start = 0;
        let mut in_slash = false;
        for (i, c) in selector.char_indices() {
            if in_slash {
                in_slash = false;
            } else if c == '\\' {
                in_slash = true;
            } else if c == ',' {
                parts.push(&selector[start..i]);
                start = i + 1;
            }
        }
        parts.push(&selector[start..]);
    }
    parts.sort();
    let mut out = Vec::new();
    for part in parts {
        if part.is_empty() {
            continue;
        }
        let mut split = None;
        'outer: for (i, _) in part.char_indices() {
            for op in ["!=", "==", "="] {
                if part[i..].starts_with(op) {
                    split = Some((&part[..i], op, &part[i + op.len()..]));
                    break 'outer;
                }
            }
        }
        let Some((lhs, op, rhs)) = split else {
            return Err(format!("invalid selector: '{selector}'; can't understand '{part}'"));
        };
        let value = unescape_field_value(rhs)?;
        out.push(FieldTerm { field: lhs.to_string(), value, not: op == "!=" });
    }
    Ok(out)
}

/// A label selector operator (Go `selection.Operator`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    In,
    NotIn,
    Equals,
    DoubleEquals,
    NotEquals,
    Exists,
    DoesNotExist,
    GreaterThan,
    LessThan,
}

/// One requirement of a label selector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Requirement {
    pub key: String,
    pub op: Op,
    pub values: Vec<String>,
}

impl Requirement {
    /// Go `Requirement.Matches`.
    pub fn matches(&self, ls: &StrMap) -> bool {
        let v = map_get(ls, &self.key);
        match self.op {
            Op::In | Op::Equals | Op::DoubleEquals => v.is_some_and(|v| self.values.iter().any(|x| x == v)),
            Op::NotIn | Op::NotEquals => v.is_none_or(|v| !self.values.iter().any(|x| x == v)),
            Op::Exists => v.is_some(),
            Op::DoesNotExist => v.is_none(),
            Op::GreaterThan | Op::LessThan => {
                let (Some(Ok(l)), [r]) = (v.map(|v| v.parse::<i64>()), self.values.as_slice()) else {
                    return false;
                };
                let Ok(r) = r.parse::<i64>() else { return false };
                (self.op == Op::GreaterThan && l > r) || (self.op == Op::LessThan && l < r)
            }
        }
    }
}

/// A parsed label selector (all requirements must match; empty matches everything).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LabelSelector(pub Vec<Requirement>);

impl LabelSelector {
    pub fn matches(&self, ls: &StrMap) -> bool {
        self.0.iter().all(|r| r.matches(ls))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tok {
    Error,
    End,
    ClosedPar,
    Comma,
    DoesNotExist,
    DoubleEquals,
    Equals,
    GreaterThan,
    Identifier,
    In,
    LessThan,
    NotEquals,
    NotIn,
    OpenPar,
}

fn string_to_token(s: &str) -> Option<Tok> {
    Some(match s {
        ")" => Tok::ClosedPar,
        "," => Tok::Comma,
        "!" => Tok::DoesNotExist,
        "==" => Tok::DoubleEquals,
        "=" => Tok::Equals,
        ">" => Tok::GreaterThan,
        "in" => Tok::In,
        "<" => Tok::LessThan,
        "!=" => Tok::NotEquals,
        "notin" => Tok::NotIn,
        "(" => Tok::OpenPar,
        _ => return None,
    })
}

fn is_ws(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\r' | b'\n')
}

fn is_special(c: u8) -> bool {
    matches!(c, b'=' | b'!' | b'(' | b')' | b',' | b'>' | b'<')
}

fn lex(s: &str) -> Vec<(Tok, String)> {
    let b = s.as_bytes();
    let mut pos = 0;
    let mut out = Vec::new();
    loop {
        while pos < b.len() && is_ws(b[pos]) {
            pos += 1;
        }
        // Go's lexer stops at a 0 byte as at the end
        if pos >= b.len() || b[pos] == 0 {
            out.push((Tok::End, String::new()));
            return out;
        }
        if is_special(b[pos]) {
            let mut buf: Vec<u8> = Vec::new();
            let mut last: Option<(Tok, String)> = None;
            while pos < b.len() && b[pos] != 0 && is_special(b[pos]) {
                buf.push(b[pos]);
                pos += 1;
                let bs = String::from_utf8_lossy(&buf).to_string();
                if let Some(t) = string_to_token(&bs) {
                    last = Some((t, bs));
                } else if last.is_some() {
                    pos -= 1;
                    break;
                }
            }
            match last {
                Some(l) => out.push(l),
                None => out.push((Tok::Error, format!("error expected: keyword found '{}'", String::from_utf8_lossy(&buf)))),
            }
        } else {
            let st = pos;
            while pos < b.len() && b[pos] != 0 && !is_special(b[pos]) && !is_ws(b[pos]) {
                pos += 1;
            }
            let lit = String::from_utf8_lossy(&b[st..pos]).to_string();
            match string_to_token(&lit) {
                Some(t) => out.push((t, lit)),
                None => out.push((Tok::Identifier, lit)),
            }
        }
    }
}

struct Parser {
    items: Vec<(Tok, String)>,
    pos: usize,
}

const BINARY_OPERATORS: &str = "in, notin, =, ==, !=, gt, lt";

fn re(pat: &'static str, cell: &'static OnceLock<regex::Regex>) -> &'static regex::Regex {
    cell.get_or_init(|| regex::Regex::new(pat).expect("valid regex"))
}

fn label_key_re() -> &'static regex::Regex {
    static R: OnceLock<regex::Regex> = OnceLock::new();
    re("^([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]$", &R)
}

fn label_value_re() -> &'static regex::Regex {
    static R: OnceLock<regex::Regex> = OnceLock::new();
    re("^(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?$", &R)
}

fn dns_subdomain_re() -> &'static regex::Regex {
    static R: OnceLock<regex::Regex> = OnceLock::new();
    re("^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$", &R)
}

const LABEL_KEY_FMT: &str = "([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]";
const LABEL_KEY_ERR: &str = "must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character";

fn regex_error(msg: &str, re: &str, examples: &[&str]) -> String {
    let mut m = format!("{msg} (e.g. ");
    for (i, e) in examples.iter().enumerate() {
        if i > 0 {
            m.push_str(" or ");
        }
        m.push_str(&format!("'{e}', "));
    }
    m.push_str(&format!("regex used for validation is '{re}')"));
    m
}

/// Go `content.IsLabelKey`.
fn is_label_key(value: &str) -> Vec<String> {
    let mut errs = Vec::new();
    let parts: Vec<&str> = value.split('/').collect();
    let name = match parts.len() {
        1 => parts[0],
        2 => {
            let prefix = parts[0];
            if prefix.is_empty() {
                errs.push("prefix part must be non-empty".to_string());
            } else {
                if prefix.len() > 253 {
                    errs.push("prefix part must be no more than 253 bytes".to_string());
                }
                if !dns_subdomain_re().is_match(prefix) {
                    errs.push(
                        "prefix part ".to_string()
                            + &regex_error(
                                "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character",
                                "[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*",
                                &["example.com"],
                            ),
                    );
                }
            }
            parts[1]
        }
        _ => {
            errs.push(format!(
                "a valid label key {} with an optional DNS subdomain prefix and '/' (e.g. 'example.com/MyName')",
                regex_error(LABEL_KEY_ERR, LABEL_KEY_FMT, &["MyName", "my.name", "123-abc"])
            ));
            return errs;
        }
    };
    if name.is_empty() {
        errs.push("name part must be non-empty".to_string());
    } else if name.len() > 63 {
        errs.push("name part must be no more than 63 bytes".to_string());
    }
    if !label_key_re().is_match(name) {
        errs.push("name part ".to_string() + &regex_error(LABEL_KEY_ERR, LABEL_KEY_FMT, &["MyName", "my.name", "123-abc"]));
    }
    errs
}

/// Go `content.IsLabelValue`.
fn is_label_value(value: &str) -> Vec<String> {
    let mut errs = Vec::new();
    if value.len() > 63 {
        errs.push("must be no more than 63 bytes".to_string());
    }
    if !label_value_re().is_match(value) {
        errs.push(regex_error(
            "a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character",
            "(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?",
            &["MyValue", "my_value", "12345"],
        ));
    }
    errs
}

/// Go `field.Invalid(path, value, detail).Error()`.
fn invalid(path: &str, value: &str, detail: &str) -> String {
    format!("{path}: Invalid value: {}: {detail}", yaml::go_quote(value))
}

fn validate_label_key(k: &str, path: &str) -> Result<(), String> {
    let errs = is_label_key(k);
    if errs.is_empty() { Ok(()) } else { Err(invalid(path, k, &errs.join("; "))) }
}

/// Go `field.ErrorList.ToAggregate().Error()`.
fn aggregate(errs: Vec<String>) -> Result<(), String> {
    match errs.len() {
        0 => Ok(()),
        1 => Err(errs.into_iter().next().unwrap_or_default()),
        _ => Err(format!("[{}]", errs.join(", "))),
    }
}

fn json_list(v: &[String]) -> String {
    format!("[{}]", v.iter().map(|s| json::quote(s)).collect::<Vec<_>>().join(","))
}

/// Go `labels.NewRequirement` (the parser path is empty).
fn new_requirement(key: &str, op: Op, vals: Vec<String>) -> Result<Requirement, String> {
    let mut errs = Vec::new();
    if let Err(e) = validate_label_key(key, "key") {
        errs.push(e);
    }
    match op {
        Op::In | Op::NotIn if vals.is_empty() => errs.push(format!("values: Invalid value: {}: for 'in', 'notin' operators, values set can't be empty", json_list(&vals))),
        Op::Equals | Op::DoubleEquals | Op::NotEquals if vals.len() != 1 => errs.push(format!("values: Invalid value: {}: exact-match compatibility requires one single value", json_list(&vals))),
        Op::Exists | Op::DoesNotExist if !vals.is_empty() => errs.push(format!("values: Invalid value: {}: values set must be empty for exists and does not exist", json_list(&vals))),
        Op::GreaterThan | Op::LessThan => {
            if vals.len() != 1 {
                errs.push(format!("values: Invalid value: {}: for 'Gt', 'Lt' operators, exactly one value is required", json_list(&vals)));
            }
            for (i, v) in vals.iter().enumerate() {
                if v.parse::<i64>().is_err() {
                    errs.push(invalid(&format!("values[{i}]"), v, "for 'Gt', 'Lt' operators, the value must be an integer"));
                }
            }
        }
        _ => {}
    }
    for (i, v) in vals.iter().enumerate() {
        let e = is_label_value(v);
        if !e.is_empty() {
            errs.push(invalid(&format!("values[{i}][{key}]"), v, &e.join("; ")));
        }
    }
    aggregate(errs)?;
    Ok(Requirement { key: key.to_string(), op, values: vals })
}

impl Parser {
    fn tok(&self, i: usize, values: bool) -> (Tok, String) {
        let (mut t, l) = self.items[i].clone();
        if values && matches!(t, Tok::In | Tok::NotIn) {
            t = Tok::Identifier;
        }
        (t, l)
    }
    fn lookahead(&self, values: bool) -> (Tok, String) {
        self.tok(self.pos, values)
    }
    fn consume(&mut self, values: bool) -> (Tok, String) {
        self.pos += 1;
        self.tok(self.pos - 1, values)
    }

    fn parse(&mut self) -> Result<Vec<Requirement>, String> {
        let mut reqs = Vec::new();
        loop {
            let (t, lit) = self.lookahead(true);
            match t {
                Tok::Identifier | Tok::DoesNotExist => {
                    let r = self.parse_requirement().map_err(|e| format!("unable to parse requirement: {e}"))?;
                    reqs.push(r);
                    let (t, l) = self.consume(true);
                    match t {
                        Tok::End => return Ok(reqs),
                        Tok::Comma => {
                            let (t2, l2) = self.lookahead(true);
                            if t2 != Tok::Identifier && t2 != Tok::DoesNotExist {
                                return Err(format!("found '{l2}', expected: identifier after ','"));
                            }
                        }
                        _ => return Err(format!("found '{l}', expected: ',' or 'end of string'")),
                    }
                }
                Tok::End => return Ok(reqs),
                _ => return Err(format!("found '{lit}', expected: !, identifier, or 'end of string'")),
            }
        }
    }

    fn parse_requirement(&mut self) -> Result<Requirement, String> {
        let (key, op) = self.parse_key_and_infer_operator()?;
        if let Some(op @ (Op::Exists | Op::DoesNotExist)) = op {
            return new_requirement(&key, op, Vec::new());
        }
        let op = self.parse_operator()?;
        let values = match op {
            Op::In | Op::NotIn => self.parse_values()?,
            _ => self.parse_exact_value()?,
        };
        let mut v: Vec<String> = values.into_iter().collect();
        v.sort();
        v.dedup();
        new_requirement(&key, op, v)
    }

    fn parse_key_and_infer_operator(&mut self) -> Result<(String, Option<Op>), String> {
        let mut op = None;
        let (mut t, mut lit) = self.consume(true);
        if t == Tok::DoesNotExist {
            op = Some(Op::DoesNotExist);
            (t, lit) = self.consume(true);
        }
        if t != Tok::Identifier {
            return Err(format!("found '{lit}', expected: identifier"));
        }
        validate_label_key(&lit, "")?;
        let (t, _) = self.lookahead(true);
        if (t == Tok::End || t == Tok::Comma) && op != Some(Op::DoesNotExist) {
            op = Some(Op::Exists);
        }
        Ok((lit, op))
    }

    fn parse_operator(&mut self) -> Result<Op, String> {
        let (t, lit) = self.consume(false);
        Ok(match t {
            Tok::In => Op::In,
            Tok::Equals => Op::Equals,
            Tok::DoubleEquals => Op::DoubleEquals,
            Tok::GreaterThan => Op::GreaterThan,
            Tok::LessThan => Op::LessThan,
            Tok::NotIn => Op::NotIn,
            Tok::NotEquals => Op::NotEquals,
            _ => return Err(format!("found '{lit}', expected: {BINARY_OPERATORS}")),
        })
    }

    fn parse_values(&mut self) -> Result<Vec<String>, String> {
        let (t, lit) = self.consume(true);
        if t != Tok::OpenPar {
            return Err(format!("found '{lit}' expected: '('"));
        }
        let (t, lit) = self.lookahead(true);
        match t {
            Tok::Identifier | Tok::Comma => {
                let s = self.parse_identifiers_list()?;
                if self.consume(true).0 != Tok::ClosedPar {
                    return Err(format!("found '{lit}', expected: ')'"));
                }
                Ok(s)
            }
            Tok::ClosedPar => {
                self.consume(true);
                Ok(vec![String::new()])
            }
            _ => Err(format!("found '{lit}', expected: ',', ')' or identifier")),
        }
    }

    fn parse_identifiers_list(&mut self) -> Result<Vec<String>, String> {
        let mut s: Vec<String> = Vec::new();
        let insert = |s: &mut Vec<String>, v: String| {
            if !s.contains(&v) {
                s.push(v);
            }
        };
        loop {
            let (t, lit) = self.consume(true);
            match t {
                Tok::Identifier => {
                    insert(&mut s, lit);
                    let (t2, l2) = self.lookahead(true);
                    match t2 {
                        Tok::Comma => continue,
                        Tok::ClosedPar => return Ok(s),
                        _ => return Err(format!("found '{l2}', expected: ',' or ')'")),
                    }
                }
                Tok::Comma => {
                    if s.is_empty() {
                        insert(&mut s, String::new());
                    }
                    let (t2, _) = self.lookahead(true);
                    if t2 == Tok::ClosedPar {
                        insert(&mut s, String::new());
                        return Ok(s);
                    }
                    if t2 == Tok::Comma {
                        insert(&mut s, String::new());
                    }
                }
                _ => return Err(format!("found '{lit}', expected: ',', or identifier")),
            }
        }
    }

    fn parse_exact_value(&mut self) -> Result<Vec<String>, String> {
        let (t, _) = self.lookahead(true);
        if t == Tok::End || t == Tok::Comma {
            return Ok(vec![String::new()]);
        }
        let (t, lit) = self.consume(true);
        if t == Tok::Identifier {
            return Ok(vec![lit]);
        }
        Err(format!("found '{lit}', expected: identifier"))
    }
}

/// Go `labels.Parse`.
pub fn parse_label_selector(selector: &str) -> Result<LabelSelector, String> {
    let mut p = Parser { items: lex(selector), pos: 0 };
    let mut reqs = p.parse()?;
    reqs.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(LabelSelector(reqs))
}
