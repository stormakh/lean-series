//! Proxy selection: a fixed proxy_url (Go http.ProxyURL), proxy_url with no_proxy, or the environment
//! (golang.org/x/net/http/httpproxy: HTTP_PROXY, HTTPS_PROXY, NO_PROXY; localhost and loopback never proxied).
use std::net::IpAddr;
use std::sync::Arc;

use yaml::url::GoUrl;

use crate::config::ProxyConfig;

/// The proxy for a request URL (None = connect directly).
pub type ProxyFunc = Arc<dyn Fn(&GoUrl) -> Result<Option<GoUrl>, String> + Send + Sync>;

pub fn from_config(c: &ProxyConfig) -> Option<ProxyFunc> {
    if c.proxy_from_environment {
        let cfg = Config::from_environment();
        return Some(Arc::new(move |u: &GoUrl| cfg.proxy_for_url(u)));
    }
    let pu = c.proxy_url.0.as_ref()?;
    let s = pu.to_url_string();
    if s.is_empty() {
        return None;
    }
    if c.no_proxy.is_empty() {
        let pu = pu.clone();
        return Some(Arc::new(move |_: &GoUrl| Ok(Some(pu.clone()))));
    }
    let cfg = Config::new(&s, &s, &c.no_proxy, false);
    Some(Arc::new(move |u: &GoUrl| cfg.proxy_for_url(u)))
}

enum Matcher {
    All,
    Cidr(IpAddr, u8),
    Ip(IpAddr, String),
    Domain { host: String, port: String, match_host: bool },
}

fn split_host_port(addr: &str) -> Option<(String, String)> {
    if let Some(rest) = addr.strip_prefix('[') {
        let end = rest.find(']')?;
        let host = &rest[..end];
        let after = &rest[end + 1..];
        let port = after.strip_prefix(':')?;
        return Some((host.to_string(), port.to_string()));
    }
    let i = addr.rfind(':')?;
    let host = &addr[..i];
    if host.contains(':') {
        return None; // too many colons
    }
    Some((host.to_string(), addr[i + 1..].to_string()))
}

fn parse_cidr(s: &str) -> Option<(IpAddr, u8)> {
    let (ip, bits) = s.split_once('/')?;
    let ip: IpAddr = ip.parse().ok()?;
    let bits: u8 = bits.parse().ok()?;
    let max = if ip.is_ipv4() { 32 } else { 128 };
    if bits > max {
        return None;
    }
    Some((ip, bits))
}

fn cidr_contains(net: IpAddr, bits: u8, ip: IpAddr) -> bool {
    match (net, ip) {
        (IpAddr::V4(n), IpAddr::V4(i)) => {
            let mask = if bits == 0 { 0 } else { u32::MAX << (32 - bits) };
            u32::from(n) & mask == u32::from(i) & mask
        }
        (IpAddr::V6(n), IpAddr::V6(i)) => {
            let mask = if bits == 0 { 0 } else { u128::MAX << (128 - bits) };
            u128::from(n) & mask == u128::from(i) & mask
        }
        // Go's IPNet.Contains matches an IPv4 address against a v4-mapped network and back
        (IpAddr::V4(n), IpAddr::V6(i)) => i.to_ipv4_mapped().is_some_and(|i| cidr_contains(IpAddr::V4(n), bits, IpAddr::V4(i))),
        (IpAddr::V6(_), IpAddr::V4(_)) => false,
    }
}

fn ip_eq(a: IpAddr, b: IpAddr) -> bool {
    let norm = |x: IpAddr| match x {
        IpAddr::V6(v) => v.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(x),
        v => v,
    };
    norm(a) == norm(b)
}

pub struct Config {
    http_proxy: Option<GoUrl>,
    https_proxy: Option<GoUrl>,
    cgi: bool,
    ip_matchers: Vec<Matcher>,
    domain_matchers: Vec<Matcher>,
}

fn parse_proxy(p: &str) -> Option<GoUrl> {
    if p.is_empty() {
        return None;
    }
    match yaml::url::parse(p) {
        Ok(u) if !u.scheme.is_empty() && !u.host.is_empty() => Some(u),
        r => match yaml::url::parse(&format!("http://{p}")) {
            Ok(u) => Some(u),
            Err(_) => r.ok(),
        },
    }
}

fn env_any(names: &[&str]) -> String {
    for n in names {
        if let Ok(v) = std::env::var(n)
            && !v.is_empty() {
                return v;
            }
    }
    String::new()
}

impl Config {
    pub fn from_environment() -> Config {
        Config::new(
            &env_any(&["http_proxy", "HTTP_PROXY"]),
            &env_any(&["https_proxy", "HTTPS_PROXY"]),
            &env_any(&["no_proxy", "NO_PROXY"]),
            std::env::var("REQUEST_METHOD").map(|v| !v.is_empty()).unwrap_or(false),
        )
    }

    pub fn new(http_proxy: &str, https_proxy: &str, no_proxy: &str, cgi: bool) -> Config {
        let mut c = Config { http_proxy: parse_proxy(http_proxy), https_proxy: parse_proxy(https_proxy), cgi, ip_matchers: Vec::new(), domain_matchers: Vec::new() };
        for p in no_proxy.split(',') {
            let p = p.trim().to_lowercase();
            if p.is_empty() {
                continue;
            }
            if p == "*" {
                c.ip_matchers = vec![Matcher::All];
                c.domain_matchers = vec![Matcher::All];
                return c;
            }
            if let Some((ip, bits)) = parse_cidr(&p) {
                c.ip_matchers.push(Matcher::Cidr(ip, bits));
                continue;
            }
            let (mut phost, pport) = match split_host_port(&p) {
                Some((h, port)) => {
                    if h.is_empty() {
                        continue;
                    }
                    (h, port)
                }
                None => (p.clone(), String::new()),
            };
            if phost.starts_with('[') && phost.ends_with(']') {
                phost = phost[1..phost.len() - 1].to_string();
            }
            if let Ok(ip) = phost.parse::<IpAddr>() {
                c.ip_matchers.push(Matcher::Ip(ip, pport));
                continue;
            }
            if phost.is_empty() {
                continue;
            }
            if let Some(rest) = phost.strip_prefix("*.") {
                phost = format!(".{rest}");
            }
            let mut match_host = false;
            if !phost.starts_with('.') {
                match_host = true;
                phost = format!(".{phost}");
            }
            c.domain_matchers.push(Matcher::Domain { host: phost, port: pport, match_host });
        }
        c
    }

    pub fn proxy_for_url(&self, u: &GoUrl) -> Result<Option<GoUrl>, String> {
        let proxy = if u.scheme == "https" {
            self.https_proxy.as_ref()
        } else if u.scheme == "http" {
            if self.http_proxy.is_some() && self.cgi {
                return Err("refusing to use HTTP_PROXY value in CGI environment; see golang.org/s/cgihttpproxy".to_string());
            }
            self.http_proxy.as_ref()
        } else {
            None
        };
        let Some(proxy) = proxy else { return Ok(None) };
        let port = match u.port() {
            "" => match u.scheme.as_str() {
                "http" => "80",
                "https" => "443",
                "socks5" => "1080",
                _ => "",
            },
            p => p,
        };
        if !self.use_proxy(u.hostname(), port) {
            return Ok(None);
        }
        Ok(Some(proxy.clone()))
    }

    fn use_proxy(&self, host: &str, port: &str) -> bool {
        if host == "localhost" {
            return false;
        }
        let ip: Option<IpAddr> = host.parse().ok();
        if let Some(ip) = ip
            && (ip.is_loopback() || matches!(ip, IpAddr::V6(v) if v.to_ipv4_mapped().is_some_and(|m| m.is_loopback()))) {
                return false;
            }
        let addr = host.trim().to_lowercase();
        if let Some(ip) = ip {
            for m in &self.ip_matchers {
                let hit = match m {
                    Matcher::All => true,
                    Matcher::Cidr(n, b) => cidr_contains(*n, *b, ip),
                    Matcher::Ip(mip, mport) => ip_eq(*mip, ip) && (mport.is_empty() || mport == port),
                    _ => false,
                };
                if hit {
                    return false;
                }
            }
        }
        for m in &self.domain_matchers {
            let hit = match m {
                Matcher::All => true,
                Matcher::Domain { host: mh, port: mp, match_host } => {
                    ip.is_none() && (addr.ends_with(mh.as_str()) || (*match_host && addr == mh[1..])) && (mp.is_empty() || mp == port)
                }
                _ => false,
            };
            if hit {
                return false;
            }
        }
        true
    }
}
