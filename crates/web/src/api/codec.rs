//! Port of `web/api/v1/codec.go` and the Accept header parser it uses (`github.com/munnerz/goautoneg`).

use std::collections::HashMap;

use super::Response;

/// Encodes API responses (Go `Codec`).
pub trait Codec: Send + Sync {
    /// The MIME type this codec emits.
    fn content_type(&self) -> MimeType;
    /// Whether this codec can encode `resp`.
    fn can_encode(&self, resp: &Response) -> bool;
    /// Encodes `resp`.
    fn encode(&self, resp: &Response) -> Result<Vec<u8>, String>;
}

/// Go `MIMEType`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MimeType {
    pub typ: String,
    pub sub_type: String,
}

impl MimeType {
    pub fn new(typ: &str, sub_type: &str) -> MimeType {
        MimeType { typ: typ.to_string(), sub_type: sub_type.to_string() }
    }

    /// Go `MIMEType.Satisfies`.
    pub fn satisfies(&self, accept: &Accept) -> bool {
        if accept.typ == "*" && accept.sub_type == "*" {
            return true;
        }
        if accept.typ == self.typ && accept.sub_type == "*" {
            return true;
        }
        accept.typ == self.typ && accept.sub_type == self.sub_type
    }
}

impl std::fmt::Display for MimeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.typ, self.sub_type)
    }
}

/// One clause of an Accept header (goautoneg `Accept`).
#[derive(Clone, Debug, PartialEq)]
pub struct Accept {
    pub typ: String,
    pub sub_type: String,
    pub q: f64,
    pub params: HashMap<String, String>,
}

impl Accept {
    pub fn new(typ: &str, sub_type: &str) -> Accept {
        Accept { typ: typ.to_string(), sub_type: sub_type.to_string(), q: 1.0, params: HashMap::new() }
    }
}

fn next_split<'a>(s: &'a str, sep: char) -> (&'a str, &'a str) {
    match s.find(sep) {
        Some(i) => (&s[..i], &s[i + 1..]),
        None => (s, ""),
    }
}

fn trim_spaces(s: &str) -> &str {
    s.trim_matches(' ')
}

fn accept_less(a: &Accept, b: &Accept) -> bool {
    if a.q > b.q {
        return true;
    }
    if a.typ != "*" && b.typ == "*" {
        return true;
    }
    if a.sub_type != "*" && b.sub_type == "*" {
        return true;
    }
    false
}

/// goautoneg `ParseAccept`: the clauses of an Accept header, best first.
pub fn parse_accept(header: &str) -> Vec<Accept> {
    let mut out: Vec<Accept> = Vec::new();
    let mut remaining = header;
    while !remaining.is_empty() {
        let (part, rest) = next_split(remaining, ',');
        remaining = rest;
        let part = trim_spaces(part);
        let mut a = Accept { typ: String::new(), sub_type: String::new(), q: 1.0, params: HashMap::new() };
        let (sp, mut remaining_part) = next_split(part, ';');
        let (sp0, sp_rem) = next_split(sp, '/');
        a.typ = trim_spaces(sp0).to_string();
        if sp_rem.is_empty() {
            if a.typ == "*" {
                a.sub_type = "*".to_string();
            } else {
                continue;
            }
        } else {
            let (sp1, rest) = next_split(sp_rem, '/');
            if !rest.is_empty() {
                continue;
            }
            a.sub_type = trim_spaces(sp1).to_string();
        }
        while !remaining_part.is_empty() {
            let (p, rest) = next_split(remaining_part, ';');
            remaining_part = rest;
            let (p0, prem) = next_split(p, '=');
            if prem.is_empty() {
                continue;
            }
            let (p1, prem2) = next_split(prem, '=');
            if !prem2.is_empty() {
                continue;
            }
            let token = trim_spaces(p0);
            if token == "q" {
                // strconv.ParseFloat(sp1, 32): float32 precision; errors give 0.
                a.q = match crate::gotime::go_parse_float(p1) {
                    Some(v) if (v as f32).is_finite() => v as f32 as f64,
                    _ => 0.0,
                };
            } else {
                a.params.insert(token.to_string(), trim_spaces(p1).to_string());
            }
        }
        out.push(a);
    }
    // sort.Sort: insertion sort for short inputs (Go uses it below 13 elements).
    for i in 1..out.len() {
        let mut j = i;
        while j > 0 && accept_less(&out[j], &out[j - 1]) {
            out.swap(j, j - 1);
            j -= 1;
        }
    }
    out
}
