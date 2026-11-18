//! Go net/url (Go 1.26): Parse, String, Redacted, Hostname, Port, ResolveReference, query escaping and parsing.
//! Prometheus configs keep URLs in this form, and their text (escaping, redaction) is visible to users.

const ENC_PATH: u8 = 1;
const ENC_PATH_SEGMENT: u8 = 2;
const ENC_HOST: u8 = 4;
const ENC_ZONE: u8 = 8;
const ENC_USER_PASSWORD: u8 = 16;
const ENC_QUERY_COMPONENT: u8 = 32;
const ENC_FRAGMENT: u8 = 64;

fn should_escape(c: u8, mode: u8) -> bool {
    if c.is_ascii_alphanumeric() {
        return false;
    }
    if mode == ENC_HOST || mode == ENC_ZONE {
        if matches!(c, b'!' | b'$' | b'&' | b'\'' | b'(' | b')' | b'*' | b'+' | b',' | b';' | b'=' | b':' | b'[' | b']' | b'<' | b'>' | b'"') {
            return false;
        }
    }
    match c {
        b'-' | b'_' | b'.' | b'~' => return false,
        b'$' | b'&' | b'+' | b',' | b'/' | b':' | b';' | b'=' | b'?' | b'@' => match mode {
            ENC_PATH => return c == b'?',
            ENC_PATH_SEGMENT => return c == b'/' || c == b';' || c == b',' || c == b'?',
            ENC_USER_PASSWORD => return c == b'@' || c == b'/' || c == b'?' || c == b':',
            ENC_QUERY_COMPONENT => return true,
            ENC_FRAGMENT => return false,
            _ => {}
        },
        _ => {}
    }
    if mode == ENC_FRAGMENT && matches!(c, b'!' | b'(' | b')' | b'*') {
        return false;
    }
    true
}

fn ishex(c: u8) -> bool {
    c.is_ascii_hexdigit()
}
fn unhex(c: u8) -> u8 {
    9 * (c >> 6) + (c & 15)
}

/// Go `strconv.Quote` for ASCII-ish error text.
fn quote(s: &str) -> String {
    crate::decode::go_quote(s)
}

fn unescape(s: &str, mode: u8) -> Result<String, String> {
    let b = s.as_bytes();
    let mut n = 0;
    let mut has_plus = false;
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                n += 1;
                if i + 2 >= b.len() || !ishex(b[i + 1]) || !ishex(b[i + 2]) {
                    let rest = &b[i..];
                    let rest = &rest[..rest.len().min(3)];
                    return Err(format!("invalid URL escape {}", quote(&String::from_utf8_lossy(rest))));
                }
                if mode == ENC_HOST && unhex(b[i + 1]) < 8 && &b[i..i + 3] != b"%25" {
                    return Err(format!("invalid URL escape {}", quote(&String::from_utf8_lossy(&b[i..i + 3]))));
                }
                if mode == ENC_ZONE {
                    let v = unhex(b[i + 1]) << 4 | unhex(b[i + 2]);
                    if &b[i..i + 3] != b"%25" && v != b' ' && should_escape(v, ENC_HOST) {
                        return Err(format!("invalid URL escape {}", quote(&String::from_utf8_lossy(&b[i..i + 3]))));
                    }
                }
                i += 3;
            }
            b'+' => {
                has_plus = mode == ENC_QUERY_COMPONENT;
                i += 1;
            }
            c => {
                if (mode == ENC_HOST || mode == ENC_ZONE) && c < 0x80 && should_escape(c, mode) {
                    return Err(format!("invalid character {} in host name", quote(&(c as char).to_string())));
                }
                i += 1;
            }
        }
    }
    if n == 0 && !has_plus {
        return Ok(s.to_string());
    }
    let mut t: Vec<u8> = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' => {
                t.push(unhex(b[i + 1]) << 4 | unhex(b[i + 2]));
                i += 2;
            }
            b'+' => t.push(if mode == ENC_QUERY_COMPONENT { b' ' } else { b'+' }),
            c => t.push(c),
        }
        i += 1;
    }
    Ok(String::from_utf8_lossy(&t).into_owned())
}

fn escape(s: &str, mode: u8) -> String {
    let mut out = String::with_capacity(s.len());
    for &c in s.as_bytes() {
        if c == b' ' && mode == ENC_QUERY_COMPONENT {
            out.push('+');
        } else if should_escape(c, mode) {
            out.push('%');
            out.push(b"0123456789ABCDEF"[(c >> 4) as usize] as char);
            out.push(b"0123456789ABCDEF"[(c & 15) as usize] as char);
        } else {
            out.push(c as char);
        }
    }
    out
}

/// Go `url.QueryEscape`.
pub fn query_escape(s: &str) -> String {
    escape(s, ENC_QUERY_COMPONENT)
}
/// Go `url.PathEscape`.
pub fn path_escape(s: &str) -> String {
    escape(s, ENC_PATH_SEGMENT)
}
/// Go `url.QueryUnescape`.
pub fn query_unescape(s: &str) -> Result<String, String> {
    unescape(s, ENC_QUERY_COMPONENT)
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Userinfo {
    pub username: String,
    pub password: String,
    pub password_set: bool,
}
impl Userinfo {
    pub fn to_url_string(&self) -> String {
        let mut s = escape(&self.username, ENC_USER_PASSWORD);
        if self.password_set {
            s.push(':');
            s.push_str(&escape(&self.password, ENC_USER_PASSWORD));
        }
        s
    }
    pub fn password(&self) -> Option<&str> {
        if self.password_set { Some(&self.password) } else { None }
    }
}

/// Go `url.URL`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GoUrl {
    pub scheme: String,
    pub opaque: String,
    pub user: Option<Userinfo>,
    pub host: String,
    pub path: String,
    pub fragment: String,
    pub raw_query: String,
    pub raw_path: String,
    pub raw_fragment: String,
    pub force_query: bool,
    pub omit_host: bool,
}

fn get_scheme(raw: &str) -> Result<(String, &str), String> {
    for (i, c) in raw.bytes().enumerate() {
        match c {
            b'a'..=b'z' | b'A'..=b'Z' => {}
            b'0'..=b'9' | b'+' | b'-' | b'.' => {
                if i == 0 {
                    return Ok((String::new(), raw));
                }
            }
            b':' => {
                if i == 0 {
                    return Err("missing protocol scheme".to_string());
                }
                return Ok((raw[..i].to_string(), &raw[i + 1..]));
            }
            _ => return Ok((String::new(), raw)),
        }
    }
    Ok((String::new(), raw))
}

fn valid_optional_port(port: &str) -> bool {
    if port.is_empty() {
        return true;
    }
    let b = port.as_bytes();
    if b[0] != b':' {
        return false;
    }
    b[1..].iter().all(|c| c.is_ascii_digit())
}

fn valid_userinfo(s: &str) -> bool {
    s.chars().all(|r| r.is_ascii_alphanumeric() || matches!(r, '-' | '.' | '_' | ':' | '~' | '!' | '$' | '&' | '\'' | '(' | ')' | '*' | '+' | ',' | ';' | '=' | '%' | '@'))
}

fn parse_host(scheme: &str, host: &str) -> Result<String, String> {
    if let Some(open) = host.rfind('[') {
        if open > 0 {
            return Err("invalid IP-literal".to_string());
        }
        let close = match host.rfind(']') {
            Some(c) => c,
            None => return Err("missing ']' in host".to_string()),
        };
        let colon_port = &host[close + 1..];
        if !valid_optional_port(colon_port) {
            return Err(format!("invalid port {} after host", quote(colon_port)));
        }
        let unescaped_port = unescape(colon_port, ENC_HOST)?;
        let hostname = &host[1..close];
        let unescaped_host = match hostname.find("%25") {
            Some(z) => unescape(&hostname[..z], ENC_HOST)? + &unescape(&hostname[z..], ENC_ZONE)?,
            None => unescape(hostname, ENC_HOST)?,
        };
        let addr_part = unescaped_host.split('%').next().unwrap_or("");
        match addr_part.parse::<std::net::IpAddr>() {
            Ok(std::net::IpAddr::V4(_)) => return Err("invalid IP-literal".to_string()),
            Ok(_) => {}
            Err(_) => return Err(format!("invalid host: ParseAddr({}): unable to parse IP", quote(&unescaped_host))),
        }
        return Ok(format!("[{unescaped_host}]{unescaped_port}"));
    } else if let Some(mut i) = host.find(':') {
        let last = host.rfind(':').unwrap_or(i);
        if last != i && scheme != "http" && scheme != "https" {
            i = last;
        }
        let colon_port = &host[i..];
        if !valid_optional_port(colon_port) {
            return Err(format!("invalid port {} after host", quote(colon_port)));
        }
    }
    unescape(host, ENC_HOST)
}

fn parse_authority(scheme: &str, authority: &str) -> Result<(Option<Userinfo>, String), String> {
    let i = authority.rfind('@');
    let host = match i {
        None => parse_host(scheme, authority)?,
        Some(i) => parse_host(scheme, &authority[i + 1..])?,
    };
    let i = match i {
        None => return Ok((None, host)),
        Some(i) => i,
    };
    let userinfo = &authority[..i];
    if !valid_userinfo(userinfo) {
        return Err("net/url: invalid userinfo".to_string());
    }
    let user = match userinfo.split_once(':') {
        None => Userinfo { username: unescape(userinfo, ENC_USER_PASSWORD)?, password: String::new(), password_set: false },
        Some((u, p)) => Userinfo { username: unescape(u, ENC_USER_PASSWORD)?, password: unescape(p, ENC_USER_PASSWORD)?, password_set: true },
    };
    Ok((Some(user), host))
}

fn parse_inner(raw: &str, via_request: bool) -> Result<GoUrl, String> {
    if raw.bytes().any(|b| b < b' ' || b == 0x7f) {
        return Err("net/url: invalid control character in URL".to_string());
    }
    if raw.is_empty() && via_request {
        return Err("empty url".to_string());
    }
    let mut u = GoUrl::default();
    if raw == "*" {
        u.path = "*".to_string();
        return Ok(u);
    }
    let (scheme, rest) = get_scheme(raw)?;
    u.scheme = scheme.to_ascii_lowercase();
    let mut rest = rest.to_string();
    if rest.ends_with('?') && rest.matches('?').count() == 1 {
        u.force_query = true;
        rest.pop();
    } else if let Some((r, q)) = rest.clone().split_once('?') {
        rest = r.to_string();
        u.raw_query = q.to_string();
    }
    if !rest.starts_with('/') {
        if !u.scheme.is_empty() {
            u.opaque = rest;
            return Ok(u);
        }
        if via_request {
            return Err("invalid URI for request".to_string());
        }
        let segment = rest.split('/').next().unwrap_or("");
        if segment.contains(':') {
            return Err("first path segment in URL cannot contain colon".to_string());
        }
    }
    if (!u.scheme.is_empty() || !via_request && !rest.starts_with("///")) && rest.starts_with("//") {
        let mut authority = rest[2..].to_string();
        let mut path = String::new();
        if let Some(i) = authority.find('/') {
            path = authority[i..].to_string();
            authority.truncate(i);
        }
        let (user, host) = parse_authority(&u.scheme, &authority)?;
        u.user = user;
        u.host = host;
        rest = path;
    } else if !u.scheme.is_empty() && rest.starts_with('/') {
        u.omit_host = true;
    }
    u.set_path(&rest)?;
    Ok(u)
}

/// Go `url.Parse`. Errors read `parse "<url>": <reason>`.
pub fn parse(raw: &str) -> Result<GoUrl, String> {
    let (u, frag) = match raw.split_once('#') {
        Some((u, f)) => (u, Some(f)),
        None => (raw, None),
    };
    let mut url = parse_inner(u, false).map_err(|e| format!("parse {}: {}", quote(u), e))?;
    if let Some(f) = frag {
        if !f.is_empty() {
            url.set_fragment(f).map_err(|e| format!("parse {}: {}", quote(raw), e))?;
        }
    }
    Ok(url)
}

/// Go `url.ParseRequestURI`.
pub fn parse_request_uri(raw: &str) -> Result<GoUrl, String> {
    parse_inner(raw, true).map_err(|e| format!("parse {}: {}", quote(raw), e))
}

fn valid_encoded(s: &str, mode: u8) -> bool {
    s.bytes().all(|c| matches!(c, b'!' | b'$' | b'&' | b'\'' | b'(' | b')' | b'*' | b'+' | b',' | b';' | b'=' | b':' | b'@' | b'[' | b']' | b'%') || !should_escape(c, mode))
}

fn split_host_port(host_port: &str) -> (&str, &str) {
    let mut host = host_port;
    let mut port = "";
    if let Some(colon) = host.rfind(':') {
        if valid_optional_port(&host[colon..]) {
            port = &host_port[colon + 1..];
            host = &host_port[..colon];
        }
    }
    if host.starts_with('[') && host.ends_with(']') && host.len() >= 2 {
        host = &host[1..host.len() - 1];
    }
    (host, port)
}

/// Go `resolvePath` (RFC 3986 dot-segment removal).
fn resolve_path(base: &str, r: &str) -> String {
    let full = if r.is_empty() {
        base.to_string()
    } else if !r.starts_with('/') {
        let i = base.rfind('/').map(|i| i + 1).unwrap_or(0);
        format!("{}{}", &base[..i], r)
    } else {
        r.to_string()
    };
    if full.is_empty() {
        return String::new();
    }
    let mut dst: Vec<u8> = vec![b'/'];
    let mut first = true;
    let mut elem = "";
    let mut parts = full.split('/').peekable();
    while let Some(e) = parts.next() {
        elem = e;
        match e {
            "." => {
                first = false;
                continue;
            }
            ".." => {
                match dst[1..].iter().rposition(|c| *c == b'/') {
                    Some(i) => dst.truncate(i + 1),
                    None => dst.truncate(1),
                }
                first = dst.len() == 1;
            }
            _ => {
                if !first {
                    dst.push(b'/');
                }
                dst.extend_from_slice(e.as_bytes());
                first = false;
            }
        }
    }
    if elem == "." || elem == ".." {
        dst.push(b'/');
    }
    let s = String::from_utf8_lossy(&dst).into_owned();
    if s.len() > 1 && s.as_bytes()[1] == b'/' { s[1..].to_string() } else { s }
}

impl GoUrl {
    fn set_path(&mut self, p: &str) -> Result<(), String> {
        let path = unescape(p, ENC_PATH)?;
        self.raw_path = if escape(&path, ENC_PATH) == p { String::new() } else { p.to_string() };
        self.path = path;
        Ok(())
    }
    fn set_fragment(&mut self, f: &str) -> Result<(), String> {
        let frag = unescape(f, ENC_FRAGMENT)?;
        self.raw_fragment = if escape(&frag, ENC_FRAGMENT) == f { String::new() } else { f.to_string() };
        self.fragment = frag;
        Ok(())
    }
    pub fn escaped_path(&self) -> String {
        if !self.raw_path.is_empty() && valid_encoded(&self.raw_path, ENC_PATH) {
            if let Ok(p) = unescape(&self.raw_path, ENC_PATH) {
                if p == self.path {
                    return self.raw_path.clone();
                }
            }
        }
        if self.path == "*" {
            return "*".to_string();
        }
        escape(&self.path, ENC_PATH)
    }
    pub fn escaped_fragment(&self) -> String {
        if !self.raw_fragment.is_empty() && valid_encoded(&self.raw_fragment, ENC_FRAGMENT) {
            if let Ok(f) = unescape(&self.raw_fragment, ENC_FRAGMENT) {
                if f == self.fragment {
                    return self.raw_fragment.clone();
                }
            }
        }
        escape(&self.fragment, ENC_FRAGMENT)
    }

    /// Go `URL.String`.
    pub fn to_url_string(&self) -> String {
        let mut buf = String::new();
        if !self.scheme.is_empty() {
            buf.push_str(&self.scheme);
            buf.push(':');
        }
        if !self.opaque.is_empty() {
            buf.push_str(&self.opaque);
        } else {
            if (!self.scheme.is_empty() || !self.host.is_empty() || self.user.is_some()) && !(self.omit_host && self.host.is_empty() && self.user.is_none()) {
                if !self.host.is_empty() || !self.path.is_empty() || self.user.is_some() {
                    buf.push_str("//");
                }
                if let Some(ui) = &self.user {
                    buf.push_str(&ui.to_url_string());
                    buf.push('@');
                }
                if !self.host.is_empty() {
                    buf.push_str(&escape(&self.host, ENC_HOST));
                }
            }
            let path = self.escaped_path();
            if !path.is_empty() && !path.starts_with('/') && !self.host.is_empty() {
                buf.push('/');
            }
            if buf.is_empty() {
                let segment = path.split('/').next().unwrap_or("");
                if segment.contains(':') {
                    buf.push_str("./");
                }
            }
            buf.push_str(&path);
        }
        if self.force_query || !self.raw_query.is_empty() {
            buf.push('?');
            buf.push_str(&self.raw_query);
        }
        if !self.fragment.is_empty() {
            buf.push('#');
            buf.push_str(&self.escaped_fragment());
        }
        buf
    }

    /// Go `URL.Redacted`: the password replaced by "xxxxx".
    pub fn redacted(&self) -> String {
        let mut ru = self.clone();
        if let Some(u) = ru.user.as_mut() {
            if u.password_set {
                u.password = "xxxxx".to_string();
            }
        }
        ru.to_url_string()
    }

    pub fn hostname(&self) -> &str {
        split_host_port(&self.host).0
    }
    pub fn port(&self) -> &str {
        split_host_port(&self.host).1
    }
    pub fn is_abs(&self) -> bool {
        !self.scheme.is_empty()
    }

    /// Go `URL.RequestURI`.
    pub fn request_uri(&self) -> String {
        let mut result = self.opaque.clone();
        if result.is_empty() {
            result = self.escaped_path();
            if result.is_empty() {
                result = "/".to_string();
            }
        } else if result.starts_with("//") {
            result = format!("{}:{}", self.scheme, result);
        }
        if self.force_query || !self.raw_query.is_empty() {
            result.push('?');
            result.push_str(&self.raw_query);
        }
        result
    }

    /// Go `URL.ResolveReference`.
    pub fn resolve_reference(&self, r: &GoUrl) -> GoUrl {
        let mut url = r.clone();
        if r.scheme.is_empty() {
            url.scheme = self.scheme.clone();
        }
        if !r.scheme.is_empty() || !r.host.is_empty() || r.user.is_some() {
            let p = resolve_path(&r.escaped_path(), "");
            let _ = url.set_path(&p);
            return url;
        }
        if !r.opaque.is_empty() {
            url.user = None;
            url.host.clear();
            url.path.clear();
            return url;
        }
        if r.path.is_empty() && !r.force_query && r.raw_query.is_empty() {
            url.raw_query = self.raw_query.clone();
            if r.fragment.is_empty() {
                url.fragment = self.fragment.clone();
                url.raw_fragment = self.raw_fragment.clone();
            }
        }
        if r.path.is_empty() && !self.opaque.is_empty() {
            url.opaque = self.opaque.clone();
            url.user = None;
            url.host.clear();
            url.path.clear();
            return url;
        }
        url.host = self.host.clone();
        url.user = self.user.clone();
        let p = resolve_path(&self.escaped_path(), &r.escaped_path());
        let _ = url.set_path(&p);
        url
    }

    /// Go `URL.Parse(ref)`.
    pub fn join(&self, r: &str) -> Result<GoUrl, String> {
        Ok(self.resolve_reference(&parse(r)?))
    }

    /// Go `URL.Query()`, in order of appearance (Go keeps a map; callers wanting Go's sorted `Encode` sort).
    pub fn query_pairs(&self) -> Vec<(String, String)> {
        parse_query(&self.raw_query).0
    }
}

impl std::fmt::Display for GoUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_url_string())
    }
}

/// Go `url.ParseQuery`: pairs in order and the first error, if any.
pub fn parse_query(query: &str) -> (Vec<(String, String)>, Option<String>) {
    let mut out = Vec::new();
    let mut err = None;
    for key in query.split('&') {
        if key.contains(';') {
            err = Some("invalid semicolon separator in query".to_string());
            continue;
        }
        if key.is_empty() {
            continue;
        }
        let (k, v) = key.split_once('=').unwrap_or((key, ""));
        let k = match query_unescape(k) {
            Ok(k) => k,
            Err(e) => {
                err.get_or_insert(e);
                continue;
            }
        };
        let v = match query_unescape(v) {
            Ok(v) => v,
            Err(e) => {
                err.get_or_insert(e);
                continue;
            }
        };
        out.push((k, v));
    }
    (out, err)
}

/// Go `url.Values.Encode`: keys sorted, values in order.
pub fn encode_query(pairs: &[(String, String)]) -> String {
    let mut idx: Vec<usize> = (0..pairs.len()).collect();
    idx.sort_by(|a, b| pairs[*a].0.cmp(&pairs[*b].0));
    let mut buf = String::new();
    for i in idx {
        if !buf.is_empty() {
            buf.push('&');
        }
        buf.push_str(&query_escape(&pairs[i].0));
        buf.push('=');
        buf.push_str(&query_escape(&pairs[i].1));
    }
    buf
}
