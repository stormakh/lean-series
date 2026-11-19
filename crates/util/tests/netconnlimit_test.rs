//! Port of upstream/util/netconnlimit/netconnlimit_test.go.
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use util::netconnlimit::{SharedLimitListener, SharedSemaphore};

#[test]
fn test_shared_limit_listener_concurrency() {
    struct Tc {
        name: &'static str,
        sem_capacity: usize,
        conn_count: usize,
        expected: i64, // Expected number of connections processed simultaneously.
    }
    let test_cases = [
        Tc { name: "Single connection allowed", sem_capacity: 1, conn_count: 3, expected: 1 },
        Tc { name: "Two connections allowed", sem_capacity: 2, conn_count: 3, expected: 2 },
        Tc { name: "Three connections allowed", sem_capacity: 3, conn_count: 3, expected: 3 },
    ];

    for tc in test_cases {
        let sem = SharedSemaphore::new(tc.sem_capacity);
        let listener = TcpListener::bind("127.0.0.1:0").expect("failed to create listener");
        let addr = listener.local_addr().expect("addr");

        let limited_listener = Arc::new(SharedLimitListener::new(listener, sem.clone()));

        let active_conn_count = Arc::new(Mutex::new(0i64));
        let mut wg = Vec::new();

        // Accept connections.
        for _ in 0..tc.conn_count {
            let l = limited_listener.clone();
            let active = active_conn_count.clone();
            let (name, expected) = (tc.name, tc.expected);
            wg.push(std::thread::spawn(move || {
                let conn = l.accept().unwrap_or_else(|e| panic!("{name}: failed to accept connection: {e}"));

                // Simulate work and track the active connection count.
                {
                    let mut n = active.lock().unwrap();
                    *n += 1;
                    assert!(*n <= expected, "{name}: too many simultaneous connections");
                }

                std::thread::sleep(Duration::from_millis(100));

                *active.lock().unwrap() -= 1;
                drop(conn);
            }));
        }

        // Create clients that attempt to connect to the listener.
        let mut clients = Vec::new();
        for _ in 0..tc.conn_count {
            clients.push(std::thread::spawn(move || {
                let mut conn = TcpStream::connect(addr).expect("failed to connect to listener");
                let _ = conn.write_all(b"hello");
            }));
        }

        for t in wg {
            assert!(t.join().is_ok(), "{}", tc.name);
        }
        for c in clients {
            let _ = c.join();
        }

        // Ensure all connections are released and semaphore is empty.
        assert!(sem.is_empty(), "{}", tc.name);
        limited_listener.close().expect("close");
    }
}

#[test]
fn test_shared_limit_listener_close() {
    let sem = SharedSemaphore::new(2);
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to create listener");

    let limited_listener = SharedLimitListener::new(listener, sem);

    // Close the listener and ensure it does not accept new connections.
    limited_listener.close().expect("failed to close listener");

    let conn = limited_listener.accept();
    assert!(conn.is_err(), "expected error on accept after listener closed");
}
