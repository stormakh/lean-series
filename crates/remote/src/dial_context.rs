//! Port of `storage/remote/dial_context.go`: the dialer for `round_robin_dns: true`. It resolves the host and
//! dials one of its addresses at random, so connections spread over all of them. Plug it into the client with
//! `net::HttpClientOptions::with_dial(new_dial_context_with_round_robin_dns().dial_context_fn())`.
use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// A dial function: network, "host:port" and the deadline. `net::transport::DialFunc` when `C` is `TcpStream`.
pub type DialContextFunc<C> = Arc<dyn Fn(&str, &str, Option<Instant>) -> io::Result<C> + Send + Sync>;

/// Go `hostResolver`.
pub trait HostResolver: Send + Sync {
    fn lookup_host(&self, host: &str) -> Result<Vec<String>, String>;
}

/// Go `net.DefaultResolver.LookupHost`: the addresses of `host` as strings (an IP literal is returned as is).
pub struct DefaultResolver;

impl HostResolver for DefaultResolver {
    fn lookup_host(&self, host: &str) -> Result<Vec<String>, String> {
        let addrs = (host, 0u16).to_socket_addrs().map_err(|e| format!("lookup {host}: {e}"))?;
        let mut out: Vec<String> = Vec::new();
        for a in addrs {
            let s = a.ip().to_string();
            if !out.contains(&s) {
                out.push(s);
            }
        }
        Ok(out)
    }
}

/// A small seeded random source (splitmix64) for the address choice (Go `*rand.Rand`).
pub struct Rand(u64);

impl Rand {
    pub fn new(seed: i64) -> Rand {
        Rand(seed as u64)
    }
    /// Go `Intn`: a value in [0, n).
    pub fn intn(&mut self, n: usize) -> usize {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^= z >> 31;
        (z % n as u64) as usize
    }
}

/// Go `dialContextWithRoundRobinDNS`.
pub struct DialContextWithRoundRobinDns<C = TcpStream> {
    pub dial_context: DialContextFunc<C>,
    pub resolver: Arc<dyn HostResolver>,
    pub rand: Arc<Mutex<Rand>>,
}

/// The default dialer (Go `http.DefaultTransport.DialContext`): TCP to the first address that answers.
fn default_dial(_network: &str, addr: &str, deadline: Option<Instant>) -> io::Result<TcpStream> {
    let mut last = io::Error::new(io::ErrorKind::AddrNotAvailable, "no address");
    for a in addr.to_socket_addrs()? {
        let r = match deadline {
            Some(d) => {
                let left = d.saturating_duration_since(Instant::now());
                if left.is_zero() {
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "i/o timeout"));
                }
                TcpStream::connect_timeout(&a, left)
            }
            None => TcpStream::connect(a),
        };
        match r {
            Ok(s) => {
                let _ = s.set_nodelay(true);
                return Ok(s);
            }
            Err(e) => last = e,
        }
    }
    Err(last)
}

/// Go `newDialContextWithRoundRobinDNS`.
pub fn new_dial_context_with_round_robin_dns() -> DialContextWithRoundRobinDns<TcpStream> {
    let seed = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0);
    DialContextWithRoundRobinDns { dial_context: Arc::new(default_dial), resolver: Arc::new(DefaultResolver), rand: Arc::new(Mutex::new(Rand::new(seed))) }
}

impl<C: 'static> DialContextWithRoundRobinDns<C> {
    /// Go `dialContextFn`: when `addr` has a port and the host resolves, dial a random address of it, else
    /// dial `addr` unchanged.
    pub fn dial_context_fn(&self) -> DialContextFunc<C> {
        let (dial, resolver, rand) = (self.dial_context.clone(), self.resolver.clone(), self.rand.clone());
        Arc::new(move |network: &str, addr: &str, deadline: Option<Instant>| {
            let Ok((host, port)) = split_host_port(addr) else {
                return dial(network, addr, deadline);
            };
            let addrs = match resolver.lookup_host(&host) {
                Ok(a) if !a.is_empty() => a,
                _ => return dial(network, addr, deadline),
            };
            let i = rand.lock().unwrap_or_else(|e| e.into_inner()).intn(addrs.len());
            let random_addr = join_host_port(&addrs[i], &port);
            dial(network, &random_addr, deadline)
        })
    }
}

/// Go `net.SplitHostPort`.
pub fn split_host_port(hostport: &str) -> Result<(String, String), String> {
    let addr_err = |why: &str| Err(format!("address {hostport}: {why}"));
    let Some(i) = hostport.rfind(':') else {
        return addr_err("missing port in address");
    };
    let (host, j, k);
    if hostport.starts_with('[') {
        let Some(end) = hostport.find(']') else {
            return addr_err("missing ']' in address");
        };
        if end + 1 == hostport.len() {
            return addr_err("missing port in address");
        }
        if end + 1 != i {
            return if hostport.as_bytes()[end + 1] == b':' { addr_err("too many colons in address") } else { addr_err("missing port in address") };
        }
        host = &hostport[1..end];
        (j, k) = (1, end + 1);
    } else {
        host = &hostport[..i];
        if host.contains(':') {
            return addr_err("too many colons in address");
        }
        (j, k) = (0, 0);
    }
    if hostport[j..].contains('[') {
        return addr_err("unexpected '[' in address");
    }
    if hostport[k..].contains(']') {
        return addr_err("unexpected ']' in address");
    }
    Ok((host.to_string(), hostport[i + 1..].to_string()))
}

/// Go `net.JoinHostPort`.
pub fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') { format!("[{host}]:{port}") } else { format!("{host}:{port}") }
}
