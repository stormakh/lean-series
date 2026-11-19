//! A ZooKeeper client: the parts of github.com/go-zookeeper/zk v1.0.4 that Prometheus uses, with the same
//! wire protocol and behavior. `connect` resolves the servers (DNS host provider), then a connection thread
//! dials the servers in turn, does the session handshake (protocol version 0, last zxid, session timeout,
//! session id and password), and runs a send thread (requests and pings every third of the session timeout)
//! and the receive loop (responses, watch events). On reconnect the session and its watches are kept
//! (`setWatches`); when the session expired, the watches get an `EventNotWatching` event.
//!
//! Requests: `get`/`get_w`, `children`/`children_w`, `exists`/`exists_w`, `close`. A watch fires once.
//!
//! Differs from Go: the DNS lookup has no 3 second limit (it uses the system resolver), and its error text
//! is "lookup <host>: no such host" without the resolver address.
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io::{Read, Write};
use std::net::{IpAddr, Shutdown, SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, mpsc};
use std::time::{Duration, Instant};

use crate::log::Logger;

/// Go `protocolVersion`.
pub const PROTOCOL_VERSION: i32 = 0;
/// Go `DefaultPort`.
pub const DEFAULT_PORT: u16 = 2181;

const BUFFER_SIZE: usize = 1536 * 1024;
const EVENT_CHAN_SIZE: usize = 6;
const SEND_CHAN_SIZE: usize = 16;

pub const OP_NOTIFY: i32 = 0;
pub const OP_CREATE: i32 = 1;
pub const OP_DELETE: i32 = 2;
pub const OP_EXISTS: i32 = 3;
pub const OP_GET_DATA: i32 = 4;
pub const OP_SET_DATA: i32 = 5;
pub const OP_GET_ACL: i32 = 6;
pub const OP_SET_ACL: i32 = 7;
pub const OP_GET_CHILDREN: i32 = 8;
pub const OP_SYNC: i32 = 9;
pub const OP_PING: i32 = 11;
pub const OP_GET_CHILDREN2: i32 = 12;
pub const OP_CHECK: i32 = 13;
pub const OP_MULTI: i32 = 14;
pub const OP_CLOSE: i32 = -11;
pub const OP_SET_AUTH: i32 = 100;
pub const OP_SET_WATCHES: i32 = 101;

/// Go `zk.EventType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EventType(pub i32);

impl EventType {
    pub const NODE_CREATED: EventType = EventType(1);
    pub const NODE_DELETED: EventType = EventType(2);
    pub const NODE_DATA_CHANGED: EventType = EventType(3);
    pub const NODE_CHILDREN_CHANGED: EventType = EventType(4);
    pub const SESSION: EventType = EventType(-1);
    pub const NOT_WATCHING: EventType = EventType(-2);
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self.0 {
            1 => "EventNodeCreated",
            2 => "EventNodeDeleted",
            3 => "EventNodeDataChanged",
            4 => "EventNodeChildrenChanged",
            -1 => "EventSession",
            -2 => "EventNotWatching",
            _ => "Unknown",
        })
    }
}

/// Go `zk.State`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct State(pub i32);

impl State {
    pub const UNKNOWN: State = State(-1);
    pub const DISCONNECTED: State = State(0);
    pub const CONNECTING: State = State(1);
    pub const SYNC_CONNECTED: State = State(3);
    pub const AUTH_FAILED: State = State(4);
    pub const CONNECTED_READ_ONLY: State = State(5);
    pub const SASL_AUTHENTICATED: State = State(6);
    pub const EXPIRED: State = State(-112);
    pub const CONNECTED: State = State(100);
    pub const HAS_SESSION: State = State(101);
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self.0 {
            -1 => "StateUnknown",
            0 => "StateDisconnected",
            5 => "StateConnectedReadOnly",
            6 => "StateSaslAuthenticated",
            -112 => "StateExpired",
            4 => "StateAuthFailed",
            1 => "StateConnecting",
            100 => "StateConnected",
            101 => "StateHasSession",
            3 => "StateSyncConnected",
            _ => "Unknown",
        })
    }
}

/// The errors of the client (Go's `zk.Err*` values; `Other` holds I/O error texts).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    ConnectionClosed,
    Unknown,
    ApiError,
    NoNode,
    NoAuth,
    BadVersion,
    NoChildrenForEphemerals,
    NodeExists,
    NotEmpty,
    SessionExpired,
    InvalidAcl,
    InvalidFlags,
    AuthFailed,
    Closing,
    Nothing,
    SessionMoved,
    ReconfigDisabled,
    BadArguments,
    NoServer,
    InvalidPath,
    ShortBuffer,
    /// A server error code without a name (Go "unknown error: %v").
    Code(i32),
    /// Go `io.EOF`.
    Eof,
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Error::ConnectionClosed => "zk: connection closed",
            Error::Unknown => "zk: unknown error",
            Error::ApiError => "zk: api error",
            Error::NoNode => "zk: node does not exist",
            Error::NoAuth => "zk: not authenticated",
            Error::BadVersion => "zk: version conflict",
            Error::NoChildrenForEphemerals => "zk: ephemeral nodes may not have children",
            Error::NodeExists => "zk: node already exists",
            Error::NotEmpty => "zk: node has children",
            Error::SessionExpired => "zk: session has been expired by the server",
            Error::InvalidAcl => "zk: invalid ACL specified",
            Error::InvalidFlags => "zk: invalid flags specified",
            Error::AuthFailed => "zk: client authentication failed",
            Error::Closing => "zk: zookeeper is closing",
            Error::Nothing => "zk: no server responses to process",
            Error::SessionMoved => "zk: session moved to another server, so operation is ignored",
            Error::ReconfigDisabled => "attempts to perform a reconfiguration operation when reconfiguration feature is disabled",
            Error::BadArguments => "invalid arguments",
            Error::NoServer => "zk: could not connect to a server",
            Error::InvalidPath => "zk: invalid path",
            Error::ShortBuffer => "zk: buffer too small",
            Error::Code(c) => return write!(f, "unknown error: {c}"),
            Error::Eof => "EOF",
            Error::Other(s) => s,
        };
        f.write_str(s)
    }
}

impl std::error::Error for Error {}

impl Error {
    /// Go `ErrCode.toError` (None for 0).
    pub fn from_code(code: i32) -> Option<Error> {
        Some(match code {
            0 => return None,
            -100 => Error::ApiError,
            -101 => Error::NoNode,
            -102 => Error::NoAuth,
            -103 => Error::BadVersion,
            -108 => Error::NoChildrenForEphemerals,
            -110 => Error::NodeExists,
            -111 => Error::NotEmpty,
            -112 => Error::SessionExpired,
            -114 => Error::InvalidAcl,
            -115 => Error::AuthFailed,
            -116 => Error::Closing,
            -117 => Error::Nothing,
            -118 => Error::SessionMoved,
            -123 => Error::ReconfigDisabled,
            -8 => Error::BadArguments,
            c => Error::Code(c),
        })
    }
}

/// Go `zk.Event`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub type_: EventType,
    pub state: State,
    pub path: String,
    pub err: Option<Error>,
    pub server: String,
}

/// Go's `%+v` of the struct.
impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{Type:{} State:{} Path:{} Err:", self.type_, self.state, self.path)?;
        match &self.err {
            Some(e) => write!(f, "{e}")?,
            None => f.write_str("<nil>")?,
        }
        write!(f, " Server:{}}}", self.server)
    }
}

/// Go `zk.Stat`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stat {
    pub czxid: i64,
    pub mzxid: i64,
    pub ctime: i64,
    pub mtime: i64,
    pub version: i32,
    pub cversion: i32,
    pub aversion: i32,
    pub ephemeral_owner: i64,
    pub data_length: i32,
    pub num_children: i32,
    pub pzxid: i64,
}

// ---------------- packet encoding (jute) ----------------

/// Big-endian packet writer (Go `encodePacket`).
#[derive(Default)]
pub struct Enc(pub Vec<u8>);

impl Enc {
    pub fn i32(&mut self, v: i32) -> &mut Enc {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }
    pub fn i64(&mut self, v: i64) -> &mut Enc {
        self.0.extend_from_slice(&v.to_be_bytes());
        self
    }
    pub fn bool(&mut self, v: bool) -> &mut Enc {
        self.0.push(v as u8);
        self
    }
    pub fn string(&mut self, s: &str) -> &mut Enc {
        self.i32(s.len() as i32);
        self.0.extend_from_slice(s.as_bytes());
        self
    }
    /// A `[]byte`: a nil slice is length -1.
    pub fn bytes(&mut self, b: Option<&[u8]>) -> &mut Enc {
        match b {
            None => self.i32(-1),
            Some(b) => {
                self.i32(b.len() as i32);
                self.0.extend_from_slice(b);
                self
            }
        }
    }
    pub fn strings(&mut self, v: &[String]) -> &mut Enc {
        self.i32(v.len() as i32);
        for s in v {
            self.string(s);
        }
        self
    }
}

/// Big-endian packet reader (Go `decodePacket`); a short buffer is `ErrShortBuffer`.
pub struct Dec<'a> {
    b: &'a [u8],
    p: usize,
}

impl<'a> Dec<'a> {
    pub fn new(b: &'a [u8]) -> Dec<'a> {
        Dec { b, p: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        if self.b.len() - self.p < n {
            return Err(Error::ShortBuffer);
        }
        let s = &self.b[self.p..self.p + n];
        self.p += n;
        Ok(s)
    }
    pub fn i32(&mut self) -> Result<i32, Error> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn i64(&mut self) -> Result<i64, Error> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn bool(&mut self) -> Result<bool, Error> {
        Ok(self.take(1)?[0] != 0)
    }
    pub fn string(&mut self) -> Result<String, Error> {
        let n = self.i32()? as u32 as usize;
        Ok(String::from_utf8_lossy(self.take(n)?).into_owned())
    }
    /// A `[]byte`; a negative length is nil.
    pub fn bytes(&mut self) -> Result<Option<Vec<u8>>, Error> {
        let n = self.i32()?;
        if n < 0 {
            return Ok(None);
        }
        Ok(Some(self.take(n as usize)?.to_vec()))
    }
    pub fn strings(&mut self) -> Result<Vec<String>, Error> {
        let n = self.i32()? as u32 as usize;
        let mut v = Vec::with_capacity(n.min(1024));
        for _ in 0..n {
            v.push(self.string()?);
        }
        Ok(v)
    }
    pub fn stat(&mut self) -> Result<Stat, Error> {
        Ok(Stat {
            czxid: self.i64()?,
            mzxid: self.i64()?,
            ctime: self.i64()?,
            mtime: self.i64()?,
            version: self.i32()?,
            cversion: self.i32()?,
            aversion: self.i32()?,
            ephemeral_owner: self.i64()?,
            data_length: self.i32()?,
            num_children: self.i32()?,
            pzxid: self.i64()?,
        })
    }
}

/// A decoded response body.
#[derive(Clone, Debug)]
pub enum Body {
    Empty,
    Data(Vec<u8>, Stat),
    Children(Vec<String>, Stat),
    Stat(Stat),
}

fn decode_empty(_: &[u8]) -> Result<Body, Error> {
    Ok(Body::Empty)
}
fn decode_data(b: &[u8]) -> Result<Body, Error> {
    let mut d = Dec::new(b);
    let data = d.bytes()?.unwrap_or_default();
    Ok(Body::Data(data, d.stat()?))
}
fn decode_children2(b: &[u8]) -> Result<Body, Error> {
    let mut d = Dec::new(b);
    let c = d.strings()?;
    Ok(Body::Children(c, d.stat()?))
}
fn decode_stat(b: &[u8]) -> Result<Body, Error> {
    Ok(Body::Stat(Dec::new(b).stat()?))
}

// ---------------- logging ----------------

/// An argument of a go-zookeeper log line.
pub enum Arg {
    Str(String),
    Val(String),
}

/// Go `treecache.ZookeeperLogger`: `Printf(s, args...)` logs `s` unformatted at info level with `args` as slog
/// attributes, paired as slog does (a string is a key; a lone string or a non-string is "!BADKEY").
#[derive(Clone, Default)]
pub struct ZookeeperLogger {
    pub logger: Logger,
}

impl ZookeeperLogger {
    pub fn printf(&self, s: &str, args: &[Arg]) {
        let mut kv: Vec<(&str, &str)> = Vec::new();
        let mut i = 0;
        while i < args.len() {
            match &args[i] {
                Arg::Str(k) if i + 1 < args.len() => {
                    let v = match &args[i + 1] {
                        Arg::Str(v) | Arg::Val(v) => v.as_str(),
                    };
                    kv.push((k.as_str(), v));
                    i += 2;
                }
                Arg::Str(v) | Arg::Val(v) => {
                    kv.push(("!BADKEY", v.as_str()));
                    i += 1;
                }
            }
        }
        self.logger.info(s, &kv);
    }
}

// ---------------- servers ----------------

/// Go `FormatServers`: adds the default port to servers without one.
pub fn format_servers(servers: &[String]) -> Vec<String> {
    servers.iter().map(|a| if a.contains(':') { a.clone() } else { format!("{a}:{DEFAULT_PORT}") }).collect()
}

fn rand_u64() -> u64 {
    let mut b = [0u8; 8];
    crypto::random_bytes(&mut b);
    u64::from_le_bytes(b)
}

/// Go `stringShuffle`.
fn string_shuffle(s: &mut [String]) {
    for i in (1..s.len()).rev() {
        let j = (rand_u64() % (i as u64 + 1)) as usize;
        s.swap(i, j);
    }
}

/// Go `net.SplitHostPort`.
pub fn split_host_port(hostport: &str) -> Result<(String, String), String> {
    let addr_err = |why: &str| Err(format!("address {hostport}: {why}"));
    let Some(i) = hostport.rfind(':') else { return addr_err("missing port in address") };
    let (host, j, k);
    if hostport.starts_with('[') {
        let Some(end) = hostport.find(']') else { return addr_err("missing ']' in address") };
        if end + 1 == hostport.len() {
            return addr_err("missing port in address");
        } else if end + 1 != i {
            if hostport.as_bytes()[end + 1] == b':' {
                return addr_err("too many colons in address");
            }
            return addr_err("missing port in address");
        }
        host = &hostport[1..end];
        j = 1;
        k = end + 1;
    } else {
        host = &hostport[..i];
        if host.contains(':') {
            return addr_err("too many colons in address");
        }
        j = 0;
        k = 0;
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
fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') { format!("[{host}]:{port}") } else { format!("{host}:{port}") }
}

/// Go `net.Resolver.LookupHost` (through the system resolver).
fn lookup_host(host: &str) -> Result<Vec<String>, String> {
    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(vec![ip.to_string()]);
    }
    if host.is_empty() {
        return Err(format!("lookup {host}: no such host"));
    }
    let addrs = (host, 0u16).to_socket_addrs().map_err(|_| format!("lookup {host}: no such host"))?;
    let mut out: Vec<String> = Vec::new();
    for a in addrs {
        let s = a.ip().to_string();
        if !out.contains(&s) {
            out.push(s);
        }
    }
    if out.is_empty() {
        return Err(format!("lookup {host}: no such host"));
    }
    Ok(out)
}

/// Go `DNSHostProvider`.
struct DnsHostProvider {
    servers: Vec<String>,
    curr: isize,
    last: isize,
}

impl DnsHostProvider {
    /// Go `Init`.
    fn init(servers: &[String]) -> Result<DnsHostProvider, String> {
        let mut found = Vec::new();
        for server in servers {
            let (host, port) = split_host_port(server)?;
            for addr in lookup_host(&host)? {
                found.push(join_host_port(&addr, &port));
            }
        }
        if found.is_empty() {
            let q: Vec<String> = servers.iter().map(|s| yaml::go_quote(s)).collect();
            return Err(format!("No hosts found for addresses [{}]", q.join(" ")));
        }
        string_shuffle(&mut found);
        Ok(DnsHostProvider { servers: found, curr: -1, last: -1 })
    }

    /// Go `Next`: the next server, and whether all servers were tried since the last `connected`.
    fn next(&mut self) -> (String, bool) {
        self.curr = (self.curr + 1) % self.servers.len() as isize;
        let retry_start = self.curr == self.last;
        if self.last == -1 {
            self.last = 0;
        }
        (self.servers[self.curr as usize].clone(), retry_start)
    }

    fn connected(&mut self) {
        self.last = self.curr;
    }
}

/// Go `validatePath`.
pub fn validate_path(path: &str, is_sequential: bool) -> Result<(), Error> {
    if path.is_empty() || !path.starts_with('/') {
        return Err(Error::InvalidPath);
    }
    if path.len() == 1 {
        return Ok(());
    }
    if !is_sequential && path.ends_with('/') {
        return Err(Error::InvalidPath);
    }
    let chars: Vec<(usize, char)> = path.char_indices().collect();
    for (ci, &(i, r)) in chars.iter().enumerate().skip(1) {
        match r {
            '\u{0}' => return Err(Error::InvalidPath),
            '/' => {
                if chars[ci - 1].1 == '/' {
                    return Err(Error::InvalidPath);
                }
            }
            '.' => {
                let mut last = chars[ci - 1].1;
                if last == '.' {
                    last = if ci >= 2 { chars[ci - 2].1 } else { '\u{FFFD}' };
                }
                if last == '/' {
                    if i + 1 == path.len() {
                        return Err(Error::InvalidPath);
                    }
                    // Go reads the next rune at path[i+w] with `w` still the width of the previous rune: the
                    // rune after the '.', except on the first rune where w is 0 and it reads the '.' again
                    // (so Go accepts "/./a").
                    let next = if ci == 1 { Some('.') } else { chars.get(ci + 1).map(|c| c.1) };
                    if next == Some('/') {
                        return Err(Error::InvalidPath);
                    }
                }
            }
            '\u{1}'..='\u{1f}' | '\u{7f}'..='\u{9f}' | '\u{f000}'..='\u{f8ff}' | '\u{fff0}'..='\u{fffe}' => return Err(Error::InvalidPath),
            _ => {}
        }
    }
    Ok(())
}

// ---------------- the connection ----------------

/// A watch callback: called once with the event that fired the watch (or `EventNotWatching`).
pub type WatchFn = Box<dyn FnOnce(Event) + Send>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum WatchType {
    Data,
    Exist,
    Child,
}

/// The watch a request sets when it succeeds (Go's `recvFunc`).
enum WatchReq {
    None,
    Data(String, WatchFn),
    Child(String, WatchFn),
    /// Go `ExistsW`: a data watch when the node exists, an exist watch when it does not.
    Exists(String, WatchFn),
}

struct Request {
    xid: i32,
    opcode: i32,
    pkt: Vec<u8>,
    decode: fn(&[u8]) -> Result<Body, Error>,
    watch: WatchReq,
}

struct Response {
    zxid: i64,
    result: Result<Body, Error>,
}

struct St {
    state: State,
    session_id: i64,
    passwd: Option<Vec<u8>>,
    last_zxid: i64,
    session_timeout_ms: i32,
    recv_timeout: Duration,
    ping_interval: Duration,
    server: String,
    conn: Option<TcpStream>,
    /// Go `sendChan`.
    send_queue: VecDeque<Request>,
    /// Go `requests`: sent, waiting for the response.
    requests: HashMap<i32, Request>,
    /// Responses not yet taken by the caller, by xid (the first response of a request wins).
    responses: HashMap<i32, Response>,
    watchers: HashMap<(String, WatchType), Vec<WatchFn>>,
    /// Go `closeChan` closed: the send loop of the current connection stops.
    close_chan: bool,
    /// Go `shouldQuit` closed.
    should_quit: bool,
}

struct Inner {
    st: Mutex<St>,
    cv: Condvar,
    xid: AtomicU32,
    closed: AtomicBool,
    connect_timeout: Duration,
    logger: ZookeeperLogger,
    log_info: bool,
    host_provider: Mutex<DnsHostProvider>,
    event_chan: Mutex<Option<mpsc::SyncSender<Event>>>,
}

/// Go `zk.Conn`. Clones share the connection.
#[derive(Clone)]
pub struct Conn(Arc<Inner>);

fn nz(d: Duration) -> Option<Duration> {
    if d.is_zero() { Some(Duration::from_nanos(1)) } else { Some(d) }
}

fn io_text(s: &TcpStream, op: &str, e: &std::io::Error) -> String {
    let local = s.local_addr().map(|a| a.to_string()).unwrap_or_default();
    let peer = s.peer_addr().map(|a| a.to_string()).unwrap_or_default();
    format!("{op} tcp {local}->{peer}: {}", net::types::go_io_error(e))
}

/// Go `io.ReadFull` on the connection.
fn read_full(s: &mut TcpStream, buf: &mut [u8]) -> Result<(), Error> {
    let mut n = 0;
    while n < buf.len() {
        match s.read(&mut buf[n..]) {
            Ok(0) => return Err(if n == 0 { Error::Eof } else { Error::Other("unexpected EOF".to_string()) }),
            Ok(k) => n += k,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(Error::Other(io_text(s, "read", &e))),
        }
    }
    Ok(())
}

fn write_all(s: &mut TcpStream, buf: &[u8], timeout: Duration) -> Result<(), Error> {
    let _ = s.set_write_timeout(nz(timeout));
    let r = s.write_all(buf).map_err(|e| Error::Other(io_text(s, "write", &e)));
    let _ = s.set_write_timeout(None);
    r
}

/// Frames a packet: 4-byte length, then the header and the body.
fn frame(xid: i32, opcode: i32, pkt: &[u8]) -> Vec<u8> {
    let mut e = Enc(Vec::with_capacity(12 + pkt.len()));
    e.i32((8 + pkt.len()) as i32).i32(xid).i32(opcode);
    e.0.extend_from_slice(pkt);
    e.0
}

/// Go `zk.Connect`: resolves the servers and starts connecting in the background. The connection is not
/// established when this returns. The events channel gets session and watch events (dropped when full).
pub fn connect(servers: &[String], session_timeout: Duration, logger: Logger) -> Result<(Conn, mpsc::Receiver<Event>), String> {
    if servers.is_empty() {
        return Err("zk: server list must not be empty".to_string());
    }
    let mut srvs = format_servers(servers);
    string_shuffle(&mut srvs);
    let (etx, erx) = mpsc::sync_channel(EVENT_CHAN_SIZE);
    let hp = DnsHostProvider::init(&srvs)?;
    let mut st = St {
        state: State::DISCONNECTED,
        session_id: 0,
        passwd: Some(vec![0u8; 16]),
        last_zxid: 0,
        session_timeout_ms: 0,
        recv_timeout: Duration::ZERO,
        ping_interval: Duration::ZERO,
        server: String::new(),
        conn: None,
        send_queue: VecDeque::new(),
        requests: HashMap::new(),
        responses: HashMap::new(),
        watchers: HashMap::new(),
        close_chan: false,
        should_quit: false,
    };
    set_timeouts(&mut st, session_timeout.as_millis() as i32);
    let inner = Arc::new(Inner {
        st: Mutex::new(st),
        cv: Condvar::new(),
        xid: AtomicU32::new(0),
        closed: AtomicBool::new(false),
        connect_timeout: Duration::from_secs(1),
        logger: ZookeeperLogger { logger },
        log_info: true,
        host_provider: Mutex::new(hp),
        event_chan: Mutex::new(Some(etx)),
    });
    let c = Conn(inner);
    let c2 = c.clone();
    std::thread::Builder::new()
        .name("zk-conn".to_string())
        .spawn(move || {
            c2.run_loop();
            c2.flush_requests(&Error::Closing);
            c2.invalidate_watches(&Error::Closing);
            c2.0.event_chan.lock().unwrap_or_else(|e| e.into_inner()).take();
        })
        .map_err(|e| e.to_string())?;
    Ok((c, erx))
}

/// Go `setTimeouts`.
fn set_timeouts(st: &mut St, session_timeout_ms: i32) {
    st.session_timeout_ms = session_timeout_ms;
    let session_timeout = Duration::from_millis(session_timeout_ms.max(0) as u64);
    st.recv_timeout = session_timeout * 2 / 3;
    st.ping_interval = st.recv_timeout / 2;
}

impl Conn {
    fn lock(&self) -> MutexGuard<'_, St> {
        self.0.st.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn wait<'a>(&self, g: MutexGuard<'a, St>) -> MutexGuard<'a, St> {
        self.0.cv.wait(g).unwrap_or_else(|e| e.into_inner())
    }

    fn wait_timeout<'a>(&self, g: MutexGuard<'a, St>, d: Duration) -> MutexGuard<'a, St> {
        self.0.cv.wait_timeout(g, d).unwrap_or_else(|e| e.into_inner()).0
    }

    /// Go `State`.
    pub fn state(&self) -> State {
        self.lock().state
    }

    /// Go `SessionID`.
    pub fn session_id(&self) -> i64 {
        self.lock().session_id
    }

    /// Go `Server`: the current or last server.
    pub fn server(&self) -> String {
        self.lock().server.clone()
    }

    fn printf(&self, s: &str, args: &[Arg]) {
        self.0.logger.printf(s, args);
    }

    /// Go `sendEvent`: never blocks.
    fn send_event(&self, ev: Event) {
        if let Some(tx) = self.0.event_chan.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
            let _ = tx.try_send(ev);
        }
    }

    /// Go `setState`.
    fn set_state(&self, state: State) {
        let server = {
            let mut st = self.lock();
            st.state = state;
            st.server.clone()
        };
        self.send_event(Event { type_: EventType::SESSION, state, path: String::new(), err: None, server });
    }

    fn deliver(&self, st: &mut St, xid: i32, r: Response) {
        st.responses.entry(xid).or_insert(r);
        self.0.cv.notify_all();
    }

    fn next_xid(&self) -> i32 {
        (self.0.xid.fetch_add(1, Ordering::SeqCst).wrapping_add(1) & 0x7fff_ffff) as i32
    }

    /// Go `connect`: dials the servers in turn; after a full round without success it waits a second.
    fn connect(&self) -> Result<(), Error> {
        loop {
            let (server, retry_start) = self.0.host_provider.lock().unwrap_or_else(|e| e.into_inner()).next();
            self.lock().server = server.clone();
            self.set_state(State::CONNECTING);
            if retry_start {
                self.flush_unsent_requests(&Error::NoServer);
                let deadline = Instant::now() + Duration::from_secs(1);
                let mut st = self.lock();
                loop {
                    if st.should_quit {
                        drop(st);
                        self.set_state(State::DISCONNECTED);
                        self.flush_unsent_requests(&Error::Closing);
                        return Err(Error::Closing);
                    }
                    let now = Instant::now();
                    if now >= deadline {
                        break;
                    }
                    st = self.wait_timeout(st, deadline - now);
                }
            }
            match dial(&server, self.0.connect_timeout) {
                Ok(s) => {
                    self.lock().conn = Some(s);
                    self.set_state(State::CONNECTED);
                    if self.0.log_info {
                        self.printf("connected to %s", &[Arg::Str(server)]);
                    }
                    return Ok(());
                }
                Err(e) => self.printf("failed to connect to %s: %v", &[Arg::Str(server), Arg::Val(e)]),
            }
        }
    }

    /// Go `loop`: connects, authenticates and runs the send and receive loops until `close`.
    fn run_loop(&self) {
        loop {
            if self.connect().is_err() {
                return;
            }
            let auth = self.authenticate();
            match &auth {
                Err(Error::SessionExpired) => {
                    self.printf("authentication failed: %s", &[Arg::Val(Error::SessionExpired.to_string())]);
                    self.invalidate_watches(&Error::SessionExpired);
                }
                Err(e) => {
                    self.printf("authentication failed: %s", &[Arg::Val(e.to_string())]);
                    if let Some(c) = self.lock().conn.take() {
                        let _ = c.shutdown(Shutdown::Both);
                    }
                }
                Ok(()) => {
                    let (id, timeout) = {
                        let st = self.lock();
                        (st.session_id, st.session_timeout_ms)
                    };
                    if self.0.log_info {
                        self.printf("authenticated: id=%d, timeout=%d", &[Arg::Val(id.to_string()), Arg::Val(timeout.to_string())]);
                    }
                    self.0.host_provider.lock().unwrap_or_else(|e| e.into_inner()).connected();
                    let conn = {
                        let mut st = self.lock();
                        st.close_chan = false;
                        st.conn.as_ref().and_then(|c| c.try_clone().ok())
                    };
                    if let Some(conn) = conn {
                        self.run_connection(conn);
                    }
                }
            }
            self.set_state(State::DISCONNECTED);
            if self.lock().should_quit {
                self.flush_requests(&Error::Closing);
                return;
            }
            let err = if auth == Err(Error::SessionExpired) { Error::SessionExpired } else { Error::ConnectionClosed };
            self.flush_requests(&err);
        }
    }

    /// The send loop on its own thread and the receive loop on this one, until either ends.
    fn run_connection(&self, conn: TcpStream) {
        let send_conn = conn.try_clone();
        let c = self.clone();
        let sender = std::thread::Builder::new().name("zk-send".to_string()).spawn(move || {
            let Ok(mut w) = send_conn else { return };
            if c.0.log_info {
                // Go resendZkAuth: no credentials are ever added here.
                c.printf("re-submitting `%d` credentials after reconnect", &[Arg::Val("0".to_string())]);
            }
            let r = c.send_loop(&mut w);
            if r.is_err() || c.0.log_info {
                let e = match &r {
                    Ok(()) => "<nil>".to_string(),
                    Err(e) => e.to_string(),
                };
                c.printf("send loop terminated: %v", &[Arg::Val(e)]);
            }
            // causes the receive loop to end
            let _ = w.shutdown(Shutdown::Both);
        });
        self.send_set_watches();
        let mut r = conn;
        let err = self.recv_loop(&mut r);
        if err != Error::Eof || self.0.log_info {
            self.printf("recv loop terminated: %v", &[Arg::Val(err.to_string())]);
        }
        {
            let mut st = self.lock();
            st.close_chan = true;
            self.0.cv.notify_all();
        }
        let _ = r.shutdown(Shutdown::Both);
        if let Ok(h) = sender {
            let _ = h.join();
        }
    }

    /// Go `authenticate`: the connect request and response.
    fn authenticate(&self) -> Result<(), Error> {
        let (mut conn, req, recv_timeout) = {
            let st = self.lock();
            let Some(conn) = st.conn.as_ref().and_then(|c| c.try_clone().ok()) else { return Err(Error::ConnectionClosed) };
            let mut e = Enc::default();
            e.i32(PROTOCOL_VERSION).i64(st.last_zxid).i32(st.session_timeout_ms).i64(st.session_id).bytes(st.passwd.as_deref());
            (conn, e.0, st.recv_timeout)
        };
        let mut buf = (req.len() as i32).to_be_bytes().to_vec();
        buf.extend_from_slice(&req);
        write_all(&mut conn, &buf, recv_timeout * 10)?;

        let _ = conn.set_read_timeout(nz(recv_timeout * 10));
        let mut lb = [0u8; 4];
        let r = read_full(&mut conn, &mut lb);
        let _ = conn.set_read_timeout(None);
        r?;
        let blen = u32::from_be_bytes(lb) as usize;
        let mut body = vec![0u8; blen];
        read_full(&mut conn, &mut body)?;
        let mut d = Dec::new(&body);
        let _protocol_version = d.i32()?;
        let timeout = d.i32()?;
        let session_id = d.i64()?;
        let passwd = d.bytes()?;
        if session_id == 0 {
            {
                let mut st = self.lock();
                st.session_id = 0;
                st.passwd = Some(vec![0u8; 16]);
                st.last_zxid = 0;
            }
            self.set_state(State::EXPIRED);
            return Err(Error::SessionExpired);
        }
        {
            let mut st = self.lock();
            st.session_id = session_id;
            set_timeouts(&mut st, timeout);
            st.passwd = passwd;
        }
        self.set_state(State::HAS_SESSION);
        Ok(())
    }

    /// Go `sendData`: registers the request and writes it.
    fn send_data(&self, w: &mut TcpStream, req: Request) -> Result<(), Error> {
        let buf = frame(req.xid, req.opcode, &req.pkt);
        let (xid, recv_timeout) = {
            let mut st = self.lock();
            if buf.len() > BUFFER_SIZE {
                self.deliver(&mut st, req.xid, Response { zxid: -1, result: Err(Error::ShortBuffer) });
                return Ok(());
            }
            if st.close_chan {
                self.deliver(&mut st, req.xid, Response { zxid: -1, result: Err(Error::ConnectionClosed) });
                return Err(Error::ConnectionClosed);
            }
            let xid = req.xid;
            st.requests.insert(xid, req);
            (xid, st.recv_timeout)
        };
        if let Err(e) = write_all(w, &buf, recv_timeout) {
            let mut st = self.lock();
            st.requests.remove(&xid);
            self.deliver(&mut st, xid, Response { zxid: -1, result: Err(e.clone()) });
            drop(st);
            let _ = w.shutdown(Shutdown::Both);
            return Err(e);
        }
        Ok(())
    }

    /// Go `sendLoop`: queued requests, and a ping every ping interval.
    fn send_loop(&self, w: &mut TcpStream) -> Result<(), Error> {
        let (interval, recv_timeout) = {
            let st = self.lock();
            (st.ping_interval.max(Duration::from_millis(1)), st.recv_timeout)
        };
        let mut next_ping = Instant::now() + interval;
        loop {
            let mut st = self.lock();
            let req = loop {
                if st.close_chan {
                    return Ok(());
                }
                if let Some(r) = st.send_queue.pop_front() {
                    self.0.cv.notify_all();
                    break Some(r);
                }
                let now = Instant::now();
                if now >= next_ping {
                    break None;
                }
                st = self.wait_timeout(st, next_ping - now);
            };
            drop(st);
            match req {
                Some(r) => self.send_data(w, r)?,
                None => {
                    let now = Instant::now();
                    while next_ping <= now {
                        next_ping += interval;
                    }
                    let buf = frame(-2, OP_PING, &[]);
                    if let Err(e) = write_all(w, &buf, recv_timeout) {
                        let _ = w.shutdown(Shutdown::Both);
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Go `recvLoop`: reads packets until an error (or the response to `close`, which ends with EOF).
    fn recv_loop(&self, r: &mut TcpStream) -> Error {
        loop {
            let recv_timeout = self.lock().recv_timeout;
            let _ = r.set_read_timeout(nz(recv_timeout));
            let mut lb = [0u8; 4];
            if let Err(e) = read_full(r, &mut lb) {
                return Error::Other(format!("failed to read from connection: {e}"));
            }
            let blen = u32::from_be_bytes(lb) as usize;
            let mut buf = vec![0u8; blen];
            let res = read_full(r, &mut buf);
            let _ = r.set_read_timeout(None);
            if let Err(e) = res {
                return e;
            }
            let mut d = Dec::new(&buf);
            let (xid, zxid, code) = match (d.i32(), d.i64(), d.i32()) {
                (Ok(x), Ok(z), Ok(c)) => (x, z, c),
                _ => return Error::ShortBuffer,
            };
            let body = &buf[16..];
            if xid == -1 {
                let mut d = Dec::new(body);
                let ev = match (d.i32(), d.i32(), d.string()) {
                    (Ok(t), Ok(s), Ok(p)) => Event { type_: EventType(t), state: State(s), path: p, err: None, server: String::new() },
                    _ => return Error::ShortBuffer,
                };
                self.send_event(ev.clone());
                self.notify_watches(&ev);
            } else if xid == -2 {
                // ping response
            } else if xid < 0 {
                self.printf("Xid < 0 (%d) but not ping or watcher event", &[Arg::Val(xid.to_string())]);
            } else {
                let mut st = self.lock();
                if zxid > 0 {
                    st.last_zxid = zxid;
                }
                let Some(req) = st.requests.remove(&xid) else {
                    drop(st);
                    self.printf("Response for unknown request with xid %d", &[Arg::Val(xid.to_string())]);
                    continue;
                };
                let result = match Error::from_code(code) {
                    Some(e) => Err(e),
                    None => (req.decode)(body),
                };
                // Go's recvFunc: the watch is added together with the response.
                match req.watch {
                    WatchReq::None => {}
                    WatchReq::Data(p, f) => {
                        if result.is_ok() {
                            st.watchers.entry((p, WatchType::Data)).or_default().push(f);
                        }
                    }
                    WatchReq::Child(p, f) => {
                        if result.is_ok() {
                            st.watchers.entry((p, WatchType::Child)).or_default().push(f);
                        }
                    }
                    WatchReq::Exists(p, f) => match &result {
                        Ok(_) => st.watchers.entry((p, WatchType::Data)).or_default().push(f),
                        Err(Error::NoNode) => st.watchers.entry((p, WatchType::Exist)).or_default().push(f),
                        Err(_) => {}
                    },
                }
                self.deliver(&mut st, xid, Response { zxid, result });
                if req.opcode == OP_CLOSE {
                    return Error::Eof;
                }
            }
        }
    }

    /// Go `flushUnsentRequests`.
    fn flush_unsent_requests(&self, err: &Error) {
        let mut st = self.lock();
        while let Some(r) = st.send_queue.pop_front() {
            self.deliver(&mut st, r.xid, Response { zxid: -1, result: Err(err.clone()) });
        }
    }

    /// Go `flushRequests`.
    fn flush_requests(&self, err: &Error) {
        let mut st = self.lock();
        let xids: Vec<i32> = st.requests.keys().copied().collect();
        for xid in xids {
            st.requests.remove(&xid);
            self.deliver(&mut st, xid, Response { zxid: -1, result: Err(err.clone()) });
        }
    }

    /// Go `notifyWatches`.
    fn notify_watches(&self, ev: &Event) {
        let types: &[WatchType] = match ev.type_ {
            EventType::NODE_CREATED => &[WatchType::Exist],
            EventType::NODE_DATA_CHANGED => &[WatchType::Exist, WatchType::Data],
            EventType::NODE_CHILDREN_CHANGED => &[WatchType::Child],
            EventType::NODE_DELETED => &[WatchType::Exist, WatchType::Data, WatchType::Child],
            _ => &[],
        };
        let mut st = self.lock();
        for t in types {
            if let Some(ws) = st.watchers.remove(&(ev.path.clone(), *t)) {
                for f in ws {
                    f(ev.clone());
                }
            }
        }
    }

    /// Go `invalidateWatches`: every watch gets `EventNotWatching` with `err`.
    fn invalidate_watches(&self, err: &Error) {
        let mut st = self.lock();
        for ((path, _), ws) in std::mem::take(&mut st.watchers) {
            let ev = Event { type_: EventType::NOT_WATCHING, state: State::DISCONNECTED, path, err: Some(err.clone()), server: String::new() };
            self.send_event(ev.clone());
            for f in ws {
                f(ev.clone());
            }
        }
    }

    /// Go `sendSetWatches`: re-sets the watches on a new connection of the session, in packets of at most
    /// 128 KiB.
    fn send_set_watches(&self) {
        let reqs = {
            let st = self.lock();
            if st.watchers.is_empty() {
                return;
            }
            let limit = 128 * 1024;
            let mut reqs: Vec<[Vec<String>; 3]> = Vec::new();
            let mut cur: Option<[Vec<String>; 3]> = None;
            let mut size = 0;
            let mut n = 0;
            for ((path, t), ws) in &st.watchers {
                if ws.is_empty() {
                    continue;
                }
                let addl = 4 + path.len();
                if cur.is_none() || size + addl > limit {
                    if let Some(r) = cur.take() {
                        reqs.push(r);
                    }
                    size = 28;
                    cur = Some(Default::default());
                }
                size += addl;
                let r = cur.as_mut().unwrap();
                match t {
                    WatchType::Data => r[0].push(path.clone()),
                    WatchType::Exist => r[1].push(path.clone()),
                    WatchType::Child => r[2].push(path.clone()),
                }
                n += 1;
            }
            if n == 0 {
                return;
            }
            if let Some(r) = cur {
                reqs.push(r);
            }
            let zxid = st.last_zxid;
            reqs.into_iter()
                .map(|[data, exist, child]| {
                    let mut e = Enc::default();
                    e.i64(zxid).strings(&data).strings(&exist).strings(&child);
                    e.0
                })
                .collect::<Vec<_>>()
        };
        let c = self.clone();
        let _ = std::thread::Builder::new().name("zk-setwatches".to_string()).spawn(move || {
            for pkt in reqs {
                if let (_, Err(e)) = c.request(OP_SET_WATCHES, pkt, decode_empty, WatchReq::None) {
                    c.printf("Failed to set previous watches: %v", &[Arg::Val(e.to_string())]);
                    break;
                }
            }
        });
    }

    /// Go `queueRequest`: puts the request on the send queue (16 entries); returns its xid.
    fn queue_request(&self, opcode: i32, pkt: Vec<u8>, decode: fn(&[u8]) -> Result<Body, Error>, watch: WatchReq) -> i32 {
        let xid = self.next_xid();
        let rq = Request { xid, opcode, pkt, decode, watch };
        let mut st = self.lock();
        if opcode == OP_CLOSE {
            // always attempt to send close ops
            let deadline = Instant::now() + self.0.connect_timeout * 2;
            while st.send_queue.len() >= SEND_CHAN_SIZE {
                let now = Instant::now();
                if now >= deadline {
                    drop(st);
                    self.printf("gave up trying to send opClose to server", &[]);
                    let mut st = self.lock();
                    self.deliver(&mut st, xid, Response { zxid: -1, result: Err(Error::ConnectionClosed) });
                    return xid;
                }
                st = self.wait_timeout(st, deadline - now);
            }
            st.send_queue.push_back(rq);
            self.0.cv.notify_all();
            return xid;
        }
        loop {
            if st.should_quit {
                self.deliver(&mut st, xid, Response { zxid: -1, result: Err(Error::ConnectionClosed) });
                return xid;
            }
            if st.send_queue.len() < SEND_CHAN_SIZE {
                st.send_queue.push_back(rq);
                self.0.cv.notify_all();
                return xid;
            }
            st = self.wait(st);
        }
    }

    /// Go `request`: queues the request and waits for its response (or `close`).
    fn request(&self, opcode: i32, pkt: Vec<u8>, decode: fn(&[u8]) -> Result<Body, Error>, watch: WatchReq) -> (i64, Result<Body, Error>) {
        let xid = self.queue_request(opcode, pkt, decode, watch);
        let mut st = self.lock();
        loop {
            if let Some(r) = st.responses.remove(&xid) {
                return (r.zxid, r.result);
            }
            if st.should_quit {
                return (-1, Err(Error::ConnectionClosed));
            }
            st = self.wait(st);
        }
    }

    /// Go `Close`: stops the connection; sends a close request and waits for it at most one second.
    pub fn close(&self) {
        if self.0.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        {
            let mut st = self.lock();
            st.should_quit = true;
            self.0.cv.notify_all();
        }
        let xid = self.queue_request(OP_CLOSE, Vec::new(), decode_empty, WatchReq::None);
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut st = self.lock();
        loop {
            if st.responses.remove(&xid).is_some() {
                return;
            }
            let now = Instant::now();
            if now >= deadline {
                return;
            }
            st = self.wait_timeout(st, deadline - now);
        }
    }

    fn path_watch_request(path: &str, watch: bool) -> Vec<u8> {
        let mut e = Enc::default();
        e.string(path).bool(watch);
        e.0
    }

    /// Go `Children`.
    pub fn children(&self, path: &str) -> Result<(Vec<String>, Stat), Error> {
        validate_path(path, false)?;
        match self.request(OP_GET_CHILDREN2, Self::path_watch_request(path, false), decode_children2, WatchReq::None).1? {
            Body::Children(c, s) => Ok((c, s)),
            _ => Err(Error::ShortBuffer),
        }
    }

    /// Go `ChildrenW` with a callback for the watch.
    pub fn children_w_fn(&self, path: &str, watch: WatchFn) -> Result<(Vec<String>, Stat), Error> {
        validate_path(path, false)?;
        match self.request(OP_GET_CHILDREN2, Self::path_watch_request(path, true), decode_children2, WatchReq::Child(path.to_string(), watch)).1? {
            Body::Children(c, s) => Ok((c, s)),
            _ => Err(Error::ShortBuffer),
        }
    }

    /// Go `ChildrenW`: the children and a channel that gets the watch event.
    pub fn children_w(&self, path: &str) -> Result<(Vec<String>, Stat, mpsc::Receiver<Event>), Error> {
        let (f, rx) = watch_channel();
        let (c, s) = self.children_w_fn(path, f)?;
        Ok((c, s, rx))
    }

    /// Go `Get`.
    pub fn get(&self, path: &str) -> Result<(Vec<u8>, Stat), Error> {
        validate_path(path, false)?;
        match self.request(OP_GET_DATA, Self::path_watch_request(path, false), decode_data, WatchReq::None).1? {
            Body::Data(d, s) => Ok((d, s)),
            _ => Err(Error::ShortBuffer),
        }
    }

    /// Go `GetW` with a callback for the watch.
    pub fn get_w_fn(&self, path: &str, watch: WatchFn) -> Result<(Vec<u8>, Stat), Error> {
        validate_path(path, false)?;
        match self.request(OP_GET_DATA, Self::path_watch_request(path, true), decode_data, WatchReq::Data(path.to_string(), watch)).1? {
            Body::Data(d, s) => Ok((d, s)),
            _ => Err(Error::ShortBuffer),
        }
    }

    /// Go `GetW`: the data and a channel that gets the watch event.
    pub fn get_w(&self, path: &str) -> Result<(Vec<u8>, Stat, mpsc::Receiver<Event>), Error> {
        let (f, rx) = watch_channel();
        let (d, s) = self.get_w_fn(path, f)?;
        Ok((d, s, rx))
    }

    /// Go `Exists`.
    pub fn exists(&self, path: &str) -> Result<(bool, Stat), Error> {
        validate_path(path, false)?;
        match self.request(OP_EXISTS, Self::path_watch_request(path, false), decode_stat, WatchReq::None).1 {
            Ok(Body::Stat(s)) => Ok((true, s)),
            Ok(_) => Err(Error::ShortBuffer),
            Err(Error::NoNode) => Ok((false, Stat::default())),
            Err(e) => Err(e),
        }
    }

    /// Go `ExistsW`: whether the node exists, and a channel that gets the watch event.
    pub fn exists_w(&self, path: &str) -> Result<(bool, Stat, mpsc::Receiver<Event>), Error> {
        validate_path(path, false)?;
        let (f, rx) = watch_channel();
        match self.request(OP_EXISTS, Self::path_watch_request(path, true), decode_stat, WatchReq::Exists(path.to_string(), f)).1 {
            Ok(Body::Stat(s)) => Ok((true, s, rx)),
            Ok(_) => Err(Error::ShortBuffer),
            Err(Error::NoNode) => Ok((false, Stat::default(), rx)),
            Err(e) => Err(e),
        }
    }
}

/// A watch that sends its event on a channel of one (Go `make(chan Event, 1)`), then closes it.
fn watch_channel() -> (WatchFn, mpsc::Receiver<Event>) {
    let (tx, rx) = mpsc::sync_channel(1);
    (
        Box::new(move |ev| {
            let _ = tx.try_send(ev);
        }),
        rx,
    )
}

/// Go `net.DialTimeout("tcp", server, timeout)`.
fn dial(server: &str, timeout: Duration) -> Result<TcpStream, String> {
    let addr: SocketAddr = match server.parse() {
        Ok(a) => a,
        Err(_) => match server.to_socket_addrs().ok().and_then(|mut a| a.next()) {
            Some(a) => a,
            None => return Err(format!("dial tcp: lookup {server}: no such host")),
        },
    };
    TcpStream::connect_timeout(&addr, timeout).map_err(|e| {
        let why = net::types::go_io_error(&e);
        if why == "i/o timeout" { format!("dial tcp {server}: {why}") } else { format!("dial tcp {server}: connect: {why}") }
    })
}
