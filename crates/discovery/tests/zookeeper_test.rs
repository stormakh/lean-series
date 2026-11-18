//! Port of discovery/zookeeper/zookeeper_test.go, plus an end-to-end check of the ZooKeeper client and tree
//! cache against a small fake ZooKeeper server (not in the ledger).
use std::collections::{BTreeMap, HashSet};
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use discovery::zookeeper::{new_discovery, parse_nerve_member, parse_serverset_member};
use discovery::{CancelToken, Discoverer, Logger, Recv};
use model::labelset::LabelSet;

// TestNewDiscoveryError can fail if the DNS resolver mistakenly resolves the domain below.
// See https://github.com/prometheus/prometheus/issues/16191 for a precedent.
#[test]
fn test_new_discovery_error() {
    let r = new_discovery(&["unreachable.invalid".to_string()], Duration::from_secs(1), &["/".to_string()], Logger::nop(), Box::new(|_: &[u8], _: &str| Ok(LabelSet::new())));
    assert!(r.is_err());
}

fn ls(p: &[(&str, &str)]) -> LabelSet {
    LabelSet::from_pairs(p)
}

/// Extra (not in the ledger): member parsing and Go's JSON error texts.
#[test]
fn extra_parse_members() {
    let data = br#"{"serviceEndpoint":{"host":"10.0.0.1","port":8080},"additionalEndpoints":{"admin-http":{"host":"10.0.0.1","port":9990}},"status":"ALIVE","shard":3}"#;
    let got = parse_serverset_member(data, "/s/m").unwrap();
    let want = ls(&[
        ("__meta_serverset_path", "/s/m"),
        ("__address__", "10.0.0.1:8080"),
        ("__meta_serverset_endpoint_host", "10.0.0.1"),
        ("__meta_serverset_endpoint_port", "8080"),
        ("__meta_serverset_endpoint_host_admin_http", "10.0.0.1"),
        ("__meta_serverset_endpoint_port_admin_http", "9990"),
        ("__meta_serverset_status", "ALIVE"),
        ("__meta_serverset_shard", "3"),
    ]);
    assert_eq!(want, got);

    let got = parse_nerve_member(br#"{"host":"::1","port":80,"name":"web"}"#, "/n/m").unwrap();
    let want = ls(&[
        ("__meta_nerve_path", "/n/m"),
        ("__address__", "[::1]:80"),
        ("__meta_nerve_endpoint_host", "::1"),
        ("__meta_nerve_endpoint_port", "80"),
        ("__meta_nerve_endpoint_name", "web"),
    ]);
    assert_eq!(want, got);

    // Texts checked against Go's encoding/json.
    assert_eq!(
        Err("error unmarshaling serverset member \"/p\": json: cannot unmarshal string into Go struct field serversetEndpoint.AdditionalEndpoints.Port of type int".to_string()),
        parse_serverset_member(br#"{"additionalEndpoints":{"x":{"port":"1"}}}"#, "/p")
    );
    assert_eq!(
        Err("error unmarshaling serverset member \"/p\": json: cannot unmarshal number 1.5 into Go struct field serversetEndpoint.ServiceEndpoint.Port of type int".to_string()),
        parse_serverset_member(br#"{"serviceEndpoint":{"host":"h","port":1.5}}"#, "/p")
    );
    assert_eq!(
        Err("error unmarshaling serverset member \"/p\": json: cannot unmarshal bool into Go struct field serversetMember.shard of type int".to_string()),
        parse_serverset_member(br#"{"shard":true}"#, "/p")
    );
    assert_eq!(
        Err("error unmarshaling nerve member \"/p\": json: cannot unmarshal array into Go value of type zookeeper.nerveMember".to_string()),
        parse_nerve_member(b"[]", "/p")
    );
}

// ---------------- a fake ZooKeeper server ----------------

const SESSION_ID: i64 = 0x1234_5678;
const PASSWD: [u8; 16] = [7; 16];

#[derive(Default)]
struct ZkState {
    nodes: BTreeMap<String, Vec<u8>>,
    data_watches: HashSet<String>,
    child_watches: HashSet<String>,
    conns: Vec<Arc<Mutex<TcpStream>>>,
    /// (opcode, path) of every request.
    ops: Vec<(i32, String)>,
    /// The bodies of the connect requests.
    connects: Vec<Vec<u8>>,
    /// The bodies of the setWatches requests.
    set_watches: Vec<Vec<u8>>,
    zxid: i64,
}

struct FakeZk {
    addr: String,
    st: Arc<Mutex<ZkState>>,
}

fn put_i32(b: &mut Vec<u8>, v: i32) {
    b.extend_from_slice(&v.to_be_bytes());
}
fn put_i64(b: &mut Vec<u8>, v: i64) {
    b.extend_from_slice(&v.to_be_bytes());
}
fn put_str(b: &mut Vec<u8>, s: &[u8]) {
    put_i32(b, s.len() as i32);
    b.extend_from_slice(s);
}
fn get_i32(b: &[u8], p: &mut usize) -> i32 {
    let v = i32::from_be_bytes(b[*p..*p + 4].try_into().unwrap());
    *p += 4;
    v
}
fn get_i64(b: &[u8], p: &mut usize) -> i64 {
    let v = i64::from_be_bytes(b[*p..*p + 8].try_into().unwrap());
    *p += 8;
    v
}
fn get_str(b: &[u8], p: &mut usize) -> String {
    let n = get_i32(b, p) as usize;
    let s = String::from_utf8(b[*p..*p + n].to_vec()).unwrap();
    *p += n;
    s
}
fn get_strs(b: &[u8], p: &mut usize) -> Vec<String> {
    let n = get_i32(b, p);
    (0..n).map(|_| get_str(b, p)).collect()
}

fn read_packet(s: &mut TcpStream) -> Option<Vec<u8>> {
    let mut l = [0u8; 4];
    s.read_exact(&mut l).ok()?;
    let mut b = vec![0u8; u32::from_be_bytes(l) as usize];
    s.read_exact(&mut b).ok()?;
    Some(b)
}

fn send_packet(w: &Mutex<TcpStream>, body: &[u8]) {
    let mut p = Vec::new();
    put_i32(&mut p, body.len() as i32);
    p.extend_from_slice(body);
    let _ = w.lock().unwrap().write_all(&p);
}

fn parent(path: &str) -> String {
    match path.rfind('/') {
        Some(0) => "/".to_string(),
        Some(i) => path[..i].to_string(),
        None => String::new(),
    }
}

impl ZkState {
    fn children(&self, path: &str) -> Vec<String> {
        self.nodes.keys().filter(|k| *k != path && parent(k) == path).map(|k| k[k.rfind('/').unwrap() + 1..].to_string()).collect()
    }

    /// Sends a watch event (xid -1) on every connection.
    fn notify(&self, typ: i32, path: &str) {
        let mut b = Vec::new();
        put_i32(&mut b, -1);
        put_i64(&mut b, -1);
        put_i32(&mut b, 0);
        put_i32(&mut b, typ);
        put_i32(&mut b, 3); // StateSyncConnected
        put_str(&mut b, path.as_bytes());
        for c in &self.conns {
            send_packet(c, &b);
        }
    }
}

impl FakeZk {
    fn start(nodes: &[(&str, &[u8])]) -> FakeZk {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let st = Arc::new(Mutex::new(ZkState { nodes: nodes.iter().map(|(k, v)| (k.to_string(), v.to_vec())).collect(), ..Default::default() }));
        let st2 = st.clone();
        std::thread::spawn(move || {
            for c in l.incoming() {
                let Ok(c) = c else { return };
                let st = st2.clone();
                std::thread::spawn(move || serve(c, st));
            }
        });
        FakeZk { addr, st }
    }

    fn set(&self, path: &str, data: &[u8]) {
        let mut st = self.st.lock().unwrap();
        let created = st.nodes.insert(path.to_string(), data.to_vec()).is_none();
        if st.data_watches.remove(path) {
            st.notify(3, path); // EventNodeDataChanged
        }
        let p = parent(path);
        if created && st.child_watches.remove(&p) {
            st.notify(4, &p); // EventNodeChildrenChanged
        }
    }

    fn delete(&self, path: &str) {
        let mut st = self.st.lock().unwrap();
        st.nodes.remove(path);
        let d = st.data_watches.remove(path);
        let c = st.child_watches.remove(path);
        if d || c {
            st.notify(2, path); // EventNodeDeleted
        }
        let p = parent(path);
        if st.child_watches.remove(&p) {
            st.notify(4, &p);
        }
    }

    /// Drops every client connection (the session stays).
    fn kill_conns(&self) {
        let mut st = self.st.lock().unwrap();
        for c in st.conns.drain(..) {
            let _ = c.lock().unwrap().shutdown(Shutdown::Both);
        }
    }

    fn ops(&self) -> Vec<(i32, String)> {
        self.st.lock().unwrap().ops.clone()
    }
}

fn serve(mut c: TcpStream, st: Arc<Mutex<ZkState>>) {
    // The connect handshake.
    let Some(req) = read_packet(&mut c) else { return };
    let w = Arc::new(Mutex::new(c.try_clone().unwrap()));
    {
        let mut s = st.lock().unwrap();
        s.connects.push(req);
        s.conns.push(w.clone());
    }
    let mut resp = Vec::new();
    put_i32(&mut resp, 0); // protocol version
    put_i32(&mut resp, 1500); // negotiated session timeout (ms)
    put_i64(&mut resp, SESSION_ID);
    put_str(&mut resp, &PASSWD);
    send_packet(&w, &resp);

    while let Some(b) = read_packet(&mut c) {
        let mut p = 0;
        let xid = get_i32(&b, &mut p);
        let op = get_i32(&b, &mut p);
        let mut s = st.lock().unwrap();
        s.zxid += 1;
        let zxid = s.zxid;
        let mut out = Vec::new();
        put_i32(&mut out, xid);
        put_i64(&mut out, zxid);
        match op {
            4 | 12 | 3 => {
                let path = get_str(&b, &mut p);
                let watch = b[p] != 0;
                s.ops.push((op, path.clone()));
                if !s.nodes.contains_key(&path) {
                    put_i32(&mut out, -101); // ZNONODE
                } else {
                    put_i32(&mut out, 0);
                    if op == 4 {
                        let d = s.nodes[&path].clone();
                        put_str(&mut out, &d);
                    } else if op == 12 {
                        let ch = s.children(&path);
                        put_i32(&mut out, ch.len() as i32);
                        for c in &ch {
                            put_str(&mut out, c.as_bytes());
                        }
                    }
                    out.extend_from_slice(&[0u8; 68]); // Stat
                    if watch {
                        if op == 12 {
                            s.child_watches.insert(path);
                        } else {
                            s.data_watches.insert(path);
                        }
                    }
                }
            }
            11 => {
                s.ops.push((op, String::new()));
                out = Vec::new();
                put_i32(&mut out, -2);
                put_i64(&mut out, zxid);
                put_i32(&mut out, 0);
            }
            101 => {
                s.ops.push((op, String::new()));
                s.set_watches.push(b[p..].to_vec());
                put_i32(&mut out, 0);
            }
            -11 => {
                s.ops.push((op, String::new()));
                put_i32(&mut out, 0);
                drop(s);
                send_packet(&w, &out);
                let _ = c.shutdown(Shutdown::Both);
                return;
            }
            _ => {
                s.ops.push((op, String::new()));
                put_i32(&mut out, -6); // ZUNIMPLEMENTED
            }
        }
        drop(s);
        send_packet(&w, &out);
    }
}

fn wait_for(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Extra (not in the ledger): serverset discovery against the fake server: handshake bytes, initial read,
/// watch events for a new member, a deleted member and changed data, pings, reconnect with the same session
/// and `setWatches`, and the close request.
#[test]
fn extra_serverset_against_fake_zookeeper() {
    let m0 = br#"{"serviceEndpoint":{"host":"10.0.0.1","port":8080},"status":"ALIVE"}"#;
    let m1 = br#"{"serviceEndpoint":{"host":"10.0.0.2","port":8080},"status":"ALIVE"}"#;
    let zk = FakeZk::start(&[("/services", b""), ("/services/member_0000", m0)]);
    let mut d = new_discovery(std::slice::from_ref(&zk.addr), Duration::from_secs(10), &["/services".to_string()], Logger::nop(), Box::new(parse_serverset_member)).unwrap();

    let ctx = CancelToken::new();
    let (tx, rx) = discovery::channel();
    let ctx2 = ctx.clone();
    let runner = std::thread::spawn(move || d.run(&ctx2, tx));
    let next = || match rx.recv_timeout(Duration::from_secs(10)) {
        Recv::Update(u) => {
            assert_eq!(1, u.len());
            (u[0].source.clone(), u[0].targets.clone())
        }
        other => panic!("no update: {other:?}"),
    };
    let target = |path: &str, host: &str| {
        ls(&[
            ("__meta_serverset_path", path),
            ("__address__", &format!("{host}:8080")),
            ("__meta_serverset_endpoint_host", host),
            ("__meta_serverset_endpoint_port", "8080"),
            ("__meta_serverset_status", "ALIVE"),
            ("__meta_serverset_shard", "0"),
        ])
    };

    // The parent node has no member data: a group without targets, as upstream.
    assert_eq!(("/services".to_string(), vec![]), next());
    assert_eq!(("/services/member_0000".to_string(), vec![target("/services/member_0000", "10.0.0.1")]), next());

    // The connect request: protocol version 0, last zxid 0, the session timeout, no session, empty password.
    {
        let st = zk.st.lock().unwrap();
        let mut want = Vec::new();
        put_i32(&mut want, 0);
        put_i64(&mut want, 0);
        put_i32(&mut want, 10_000);
        put_i64(&mut want, 0);
        put_str(&mut want, &[0u8; 16]);
        assert_eq!(want, st.connects[0]);
    }
    wait_for("the initial reads", || zk.ops().len() >= 4);
    assert_eq!(&zk.ops()[..4], &[(4, "/services".to_string()), (12, "/services".to_string()), (4, "/services/member_0000".to_string()), (12, "/services/member_0000".to_string())]);

    // A new member.
    zk.set("/services/member_0001", m1);
    assert_eq!(("/services/member_0001".to_string(), vec![target("/services/member_0001", "10.0.0.2")]), next());

    // A deleted member.
    zk.delete("/services/member_0000");
    assert_eq!(("/services/member_0000".to_string(), vec![]), next());

    // Pings every third of the negotiated session timeout (1.5s).
    wait_for("a ping", || zk.ops().iter().any(|(op, _)| *op == 11));

    // The connection drops: the client reconnects with its session and sets its watches again.
    zk.kill_conns();
    wait_for("a second connect", || zk.st.lock().unwrap().connects.len() >= 2);
    {
        let st = zk.st.lock().unwrap();
        let c = &st.connects[1];
        let mut p = 0;
        assert_eq!(0, get_i32(c, &mut p));
        assert!(get_i64(c, &mut p) > 0, "last zxid seen");
        assert_eq!(1500, get_i32(c, &mut p));
        assert_eq!(SESSION_ID, get_i64(c, &mut p));
        assert_eq!(&PASSWD[..], &c[p + 4..]);
    }
    wait_for("setWatches", || !zk.st.lock().unwrap().set_watches.is_empty());
    {
        let st = zk.st.lock().unwrap();
        let b = &st.set_watches[0];
        let mut p = 8;
        let mut data = get_strs(b, &mut p);
        let exist = get_strs(b, &mut p);
        let mut child = get_strs(b, &mut p);
        data.sort();
        data.dedup();
        child.sort();
        child.dedup();
        assert!(data.contains(&"/services".to_string()) && data.contains(&"/services/member_0001".to_string()), "{data:?}");
        assert!(exist.is_empty());
        assert!(child.contains(&"/services".to_string()) && child.contains(&"/services/member_0001".to_string()), "{child:?}");
    }

    // Watches still work on the new connection.
    let m1b = br#"{"serviceEndpoint":{"host":"10.0.0.3","port":8080},"status":"ALIVE"}"#;
    zk.set("/services/member_0001", m1b);
    assert_eq!(("/services/member_0001".to_string(), vec![target("/services/member_0001", "10.0.0.3")]), next());

    // Cancel: the discoverer stops its tree cache and closes the session.
    ctx.cancel();
    runner.join().unwrap();
    wait_for("the close request", || zk.ops().iter().any(|(op, _)| *op == -11));
}

/// Extra (not in the ledger): go-zookeeper's path validation, with results taken from the Go code.
#[test]
fn extra_validate_path() {
    let cases = [
        ("/", true),
        ("", false),
        ("a", false),
        ("/a/", false),
        ("//a", false),
        ("/a//b", false),
        ("/./a", true),
        ("/a/./b", false),
        ("/a/../b", false),
        ("/a/.", false),
        ("/a/..", false),
        ("/.", false),
        ("/..", false),
        ("/a.b", true),
        ("/a\x01", false),
        ("/a/.b", true),
        ("/../a", false),
        ("/services/m_1", true),
    ];
    for (p, ok) in cases {
        assert_eq!(ok, discovery::zookeeper::zk::validate_path(p, false).is_ok(), "{p:?}");
    }
}
