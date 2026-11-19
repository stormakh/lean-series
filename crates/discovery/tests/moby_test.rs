//! Port of discovery/moby/moby_test.go.
mod common;

use std::os::unix::net::UnixListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use common::new_options;
use discovery::moby::{Discovery, DockerDiscovery, DockerSdConfig, DockerSwarmSdConfig};
use discovery::{CancelToken, Group};

// The refresh interval doubles as the request timeout, so it is kept well below the deadlines a refresh is
// measured against.
const REFRESH_INTERVAL: &str = "100ms";
const MAX_REFRESH_DURATION: Duration = Duration::from_secs(2);
const HANG_DEADLINE: Duration = Duration::from_secs(10);

type RefreshFn = Box<dyn FnMut(&CancelToken) -> Result<Vec<Arc<Group>>, String> + Send>;
type Conns = Arc<Mutex<Vec<Box<dyn Send>>>>;
type HostFn = fn() -> (String, UnresponsiveDaemon);
type NewFn = fn(&str) -> RefreshFn;

/// A daemon that accepts connections without ever writing to them, simulating a wedged Docker daemon. The
/// connections are held open until it is dropped, so that a client without a request timeout blocks rather than
/// seeing the socket close.
struct UnresponsiveDaemon {
    addr: String,
    closed: Arc<AtomicBool>,
    conns: Conns,
    wake: Box<dyn Fn() + Send>,
    thread: Option<std::thread::JoinHandle<()>>,
    dir: Option<std::path::PathBuf>,
}

impl UnresponsiveDaemon {
    fn tcp() -> UnresponsiveDaemon {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap().to_string();
        let (closed, conns): (Arc<AtomicBool>, Conns) = (Arc::default(), Arc::default());
        let (c2, k2) = (closed.clone(), conns.clone());
        let thread = std::thread::spawn(move || {
            for c in l.incoming() {
                let Ok(c) = c else { return };
                // Accepting races with the cleanup, which must not leave a connection behind.
                if c2.load(Ordering::SeqCst) {
                    return;
                }
                k2.lock().unwrap().push(Box::new(c));
            }
        });
        let a2 = addr.clone();
        UnresponsiveDaemon { addr, closed, conns, wake: Box::new(move || drop(std::net::TcpStream::connect(&a2))), thread: Some(thread), dir: None }
    }

    fn unix() -> UnresponsiveDaemon {
        // The socket path must stay under the 108 byte cap on unix addresses, so it is in the system temp
        // directory as in Go (os.MkdirTemp("", "moby")).
        static N: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!("moby{}-{}", std::process::id(), N.fetch_add(1, Ordering::SeqCst)));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("d.sock");
        let l = UnixListener::bind(&path).unwrap();
        let (closed, conns): (Arc<AtomicBool>, Conns) = (Arc::default(), Arc::default());
        let (c2, k2) = (closed.clone(), conns.clone());
        let thread = std::thread::spawn(move || {
            for c in l.incoming() {
                let Ok(c) = c else { return };
                if c2.load(Ordering::SeqCst) {
                    return;
                }
                k2.lock().unwrap().push(Box::new(c));
            }
        });
        let p2 = path.clone();
        UnresponsiveDaemon { addr: path.to_string_lossy().into_owned(), closed, conns, wake: Box::new(move || drop(std::os::unix::net::UnixStream::connect(&p2))), thread: Some(thread), dir: Some(dir) }
    }
}

impl Drop for UnresponsiveDaemon {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
        (self.wake)();
        if let Some(t) = self.thread.take() {
            t.join().unwrap();
        }
        self.conns.lock().unwrap().clear();
        if let Some(d) = &self.dir {
            std::fs::remove_dir_all(d).unwrap();
        }
    }
}

fn new_docker(host: &str) -> RefreshFn {
    let cfg: DockerSdConfig = yaml::unmarshal(&format!("\n---\nhost: {host}\nrefresh_interval: {REFRESH_INTERVAL}\n")).unwrap();
    let (mut opts, _reg) = new_options(&cfg);
    opts.set_name = String::new();
    let mut d = DockerDiscovery::new(&cfg, opts).unwrap();
    Box::new(move |ctx| d.refresh(ctx))
}

fn new_dockerswarm(host: &str) -> RefreshFn {
    let cfg: DockerSwarmSdConfig = yaml::unmarshal(&format!("\n---\nhost: {host}\nrole: nodes\nrefresh_interval: {REFRESH_INTERVAL}\n")).unwrap();
    let (mut opts, _reg) = new_options(&cfg);
    opts.set_name = String::new();
    let mut d = Discovery::new(&cfg, opts).unwrap();
    Box::new(move |ctx| d.refresh(ctx))
}

/// A Docker daemon which accepts the connection but never answers cannot block discovery forever. The http
/// case also guards the option ordering the timeout relies on.
#[test]
fn test_refresh_times_out_on_unresponsive_daemon() {
    let hosts: [(&str, HostFn); 2] = [
        ("unix", || {
            let d = UnresponsiveDaemon::unix();
            (format!("unix://{}", d.addr), d)
        }),
        ("http", || {
            let d = UnresponsiveDaemon::tcp();
            (format!("http://{}", d.addr), d)
        }),
    ];
    let sds: [(&str, NewFn); 2] = [("docker", new_docker), ("dockerswarm", new_dockerswarm)];
    let mut subtests = Vec::new();
    for (host_name, host) in hosts {
        for (sd_name, new_discovery) in sds {
            let name = format!("{sd_name}/{host_name}");
            subtests.push(std::thread::spawn(move || {
                let (host, _daemon) = host();
                let mut d = new_discovery(&host);

                // The context deliberately carries no deadline, so that a refresh returning at all proves the
                // client bounded it.
                let (tx, rx) = mpsc::channel();
                let start = Instant::now();
                std::thread::spawn(move || {
                    let _ = tx.send(d(&CancelToken::new()));
                });

                match rx.recv_timeout(HANG_DEADLINE) {
                    Ok(res) => {
                        let err = res.expect_err(&format!("{name}: refresh must fail"));
                        // adapted: Go checks errors.As(err, &net.Error) && Timeout(); errors are strings here,
                        // so the check is Go's text for a client timeout.
                        assert!(err.contains("context deadline exceeded (Client.Timeout exceeded while awaiting headers)"), "{name}: expected a timeout error, got: {err}");
                        assert!(start.elapsed() < MAX_REFRESH_DURATION, "{name}: refresh returned, but too late for the timeout to still be derived from refresh_interval");
                    }
                    Err(_) => panic!("{name}: refresh did not return: the Docker client has no request timeout on this transport"),
                }
            }));
        }
    }
    for t in subtests {
        t.join().unwrap();
    }
}
