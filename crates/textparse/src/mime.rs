//! Port of Go `mime.ParseMediaType` (media type and parameters, RFC 2231 continuations), with Go's error texts.

use std::collections::HashMap;

pub const ERR_NO_MEDIA_TYPE: &str = "mime: no media type";
pub const ERR_NO_SLASH_AFTER_FIRST_TOKEN: &str = "mime: expected slash after first token";
pub const ERR_NO_TOKEN_AFTER_SLASH: &str = "mime: expected token after slash";
pub const ERR_UNEXPECTED_CONTENT_AFTER_MEDIA_SUBTYPE: &str = "mime: unexpected content after media subtype";
pub const ERR_INVALID_MEDIA_PARAMETER: &str = "mime: invalid media parameter";
pub const ERR_DUPLICATE_PARAM_NAME: &str = "mime: duplicate parameter name";

fn is_tspecial(c: u8) -> bool {
    matches!(c, b'(' | b')' | b'<' | b'>' | b'@' | b',' | b';' | b':' | b'\\' | b'"' | b'/' | b'[' | b']' | b'?' | b'=')
}

fn is_token_char(c: u8) -> bool {
    c.is_ascii_alphanumeric()
        || matches!(c, b'!' | b'#' | b'$' | b'%' | b'&' | b'\'' | b'*' | b'+' | b'-' | b'.' | b'^' | b'_' | b'`' | b'{' | b'|' | b'}' | b'~')
}

fn consume_token(v: &str) -> (&str, &str) {
    for (i, c) in v.bytes().enumerate() {
        if !is_token_char(c) {
            return (&v[..i], &v[i..]);
        }
    }
    (v, "")
}

fn consume_value(v: &str) -> (String, &str) {
    if v.is_empty() {
        return (String::new(), v);
    }
    if !v.starts_with('"') {
        let (t, r) = consume_token(v);
        return (t.to_string(), r);
    }
    let b = v.as_bytes();
    let mut buf = Vec::new();
    let mut i = 1;
    while i < b.len() {
        let r = b[i];
        if r == b'"' {
            return (String::from_utf8_lossy(&buf).into_owned(), &v[i + 1..]);
        }
        if r == b'\\' && i + 1 < b.len() && is_tspecial(b[i + 1]) {
            buf.push(b[i + 1]);
            i += 2;
            continue;
        }
        if r == b'\r' || r == b'\n' {
            return (String::new(), v);
        }
        buf.push(r);
        i += 1;
    }
    (String::new(), v)
}

fn consume_media_param(v: &str) -> (String, String, &str) {
    let rest = v.trim_start();
    let Some(rest) = rest.strip_prefix(';') else { return (String::new(), String::new(), v) };
    let rest = rest.trim_start();
    let (param, rest) = consume_token(rest);
    let param = param.to_ascii_lowercase();
    if param.is_empty() {
        return (String::new(), String::new(), v);
    }
    let rest = rest.trim_start();
    let Some(rest) = rest.strip_prefix('=') else { return (String::new(), String::new(), v) };
    let rest = rest.trim_start();
    let (value, rest2) = consume_value(rest);
    if value.is_empty() && rest2.len() == rest.len() {
        return (String::new(), String::new(), v);
    }
    (param, value, rest2)
}

fn check_media_type_disposition(s: &str) -> Result<(), &'static str> {
    let (typ, rest) = consume_token(s);
    if typ.is_empty() {
        return Err(ERR_NO_MEDIA_TYPE);
    }
    if rest.is_empty() {
        return Ok(());
    }
    let Some(rest) = rest.strip_prefix('/') else { return Err(ERR_NO_SLASH_AFTER_FIRST_TOKEN) };
    let (subtype, rest) = consume_token(rest);
    if subtype.is_empty() {
        return Err(ERR_NO_TOKEN_AFTER_SLASH);
    }
    if !rest.is_empty() {
        return Err(ERR_UNEXPECTED_CONTENT_AFTER_MEDIA_SUBTYPE);
    }
    Ok(())
}

fn unhex(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => 0,
    }
}

fn percent_hex_unescape(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let mut i = 0;
    let mut percents = 0;
    while i < b.len() {
        if b[i] != b'%' {
            i += 1;
            continue;
        }
        percents += 1;
        if i + 2 >= b.len() || !b[i + 1].is_ascii_hexdigit() || !b[i + 2].is_ascii_hexdigit() {
            return None;
        }
        i += 3;
    }
    if percents == 0 {
        return Some(s.to_string());
    }
    let mut t = Vec::with_capacity(b.len() - 2 * percents);
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            t.push(unhex(b[i + 1]) << 4 | unhex(b[i + 2]));
            i += 3;
        } else {
            t.push(b[i]);
            i += 1;
        }
    }
    Some(String::from_utf8_lossy(&t).into_owned())
}

fn decode_2231_enc(v: &str) -> Option<String> {
    let (charset, v) = v.split_once('\'')?;
    let (_, ext) = v.split_once('\'')?;
    match charset.to_ascii_lowercase().as_str() {
        "us-ascii" | "utf-8" => {}
        _ => return None,
    }
    percent_hex_unescape(ext)
}

/// Go `mime.ParseMediaType`. On a parameter error Go returns the media type too; this returns it in the error.
#[allow(clippy::type_complexity)]
pub fn parse_media_type(v: &str) -> Result<(String, HashMap<String, String>), (String, &'static str)> {
    let base = v.split_once(';').map_or(v, |(b, _)| b);
    let mediatype = model::unicode::str_to_lower(base).trim().to_string();
    check_media_type_disposition(&mediatype).map_err(|e| (String::new(), e))?;
    let mut params: HashMap<String, String> = HashMap::new();
    let mut continuation: HashMap<String, HashMap<String, String>> = HashMap::new();
    let mut v = &v[base.len()..];
    while !v.is_empty() {
        v = v.trim_start();
        if v.is_empty() {
            break;
        }
        let (key, value, rest) = consume_media_param(v);
        if key.is_empty() {
            if rest.trim() == ";" {
                break;
            }
            return Err((mediatype, ERR_INVALID_MEDIA_PARAMETER));
        }
        let pmap = if let Some((base_name, _)) = key.split_once('*') {
            continuation.entry(base_name.to_string()).or_default()
        } else {
            &mut params
        };
        if let Some(old) = pmap.get(&key) {
            if *old != value {
                return Err((String::new(), ERR_DUPLICATE_PARAM_NAME));
            }
        }
        pmap.insert(key, value);
        v = rest;
    }
    for (key, piece_map) in continuation {
        let single = format!("{key}*");
        if let Some(v) = piece_map.get(&single) {
            if let Some(d) = decode_2231_enc(v) {
                params.insert(key, d);
            }
            continue;
        }
        let mut buf = String::new();
        let mut valid = false;
        let mut n = 0;
        loop {
            let simple = format!("{key}*{n}");
            if let Some(v) = piece_map.get(&simple) {
                valid = true;
                buf.push_str(v);
                n += 1;
                continue;
            }
            let encoded = format!("{simple}*");
            let Some(v) = piece_map.get(&encoded) else { break };
            valid = true;
            if n == 0 {
                if let Some(d) = decode_2231_enc(v) {
                    buf.push_str(&d);
                }
            } else if let Some(d) = percent_hex_unescape(v) {
                buf.push_str(&d);
            }
            n += 1;
        }
        if valid {
            params.insert(key, buf);
        }
    }
    Ok((mediatype, params))
}
