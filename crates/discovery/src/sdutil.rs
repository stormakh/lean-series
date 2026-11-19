//! Small helpers shared by the SD providers (Go standard library behavior they all use).

/// Go `net.JoinHostPort`: "host:port", with brackets when the host contains a colon (IPv6).
pub fn join_host_port(host: &str, port: impl std::fmt::Display) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// The tag label value most providers build: `sep + strings.Join(tags, sep) + sep`.
pub fn join_tags<S: AsRef<str>>(tags: &[S], sep: &str) -> String {
    let mut s = String::from(sep);
    for (i, t) in tags.iter().enumerate() {
        if i > 0 {
            s.push_str(sep);
        }
        s.push_str(t.as_ref());
    }
    s.push_str(sep);
    s
}

/// Go `strutil.SanitizeLabelName`: every rune that is not `[a-zA-Z0-9_]` becomes `_`.
pub fn sanitize_label_name(s: &str) -> String {
    s.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect()
}

/// Sends a GET request with the given headers and returns the response (any status).
pub fn get(client: &net::Client, url: &str, headers: &[(&str, &str)], timeout: Option<std::time::Duration>) -> Result<net::Response, net::Error> {
    let mut req = net::Request::get(url)?;
    for (k, v) in headers {
        req.header.set(k, v);
    }
    req.timeout = timeout;
    client.do_request(req)
}
