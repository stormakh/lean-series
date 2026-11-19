//! Port of `storage/remote/dial_context_test.go`.
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use remote::dial_context::{DialContextWithRoundRobinDns, HostResolver, Rand, join_host_port};

const TEST_NETWORK: &str = "tcp";
const TEST_ADDR_WITHOUT_PORT: &str = "this-is-my-addr.without-port";
const TEST_ADDR_WITH_PORT: &str = "this-is-my-addr.without-port:123";
const TEST_PORT: &str = "123";
const IP1: &str = "1.2.3.4";
const IP2: &str = "5.6.7.8";
const IP3: &str = "9.0.1.2";
const RAND_SEED: i64 = 123456789;

fn test_lookup_result() -> Vec<String> {
    vec![IP1.to_string(), IP2.to_string(), IP3.to_string()]
}
fn test_lookup_result_with_port() -> Vec<String> {
    vec![join_host_port(IP1, TEST_PORT), join_host_port(IP2, TEST_PORT), join_host_port(IP3, TEST_PORT)]
}

/// The testify mock: accepts calls only for the acceptable addresses (any other address panics, as an
/// unexpected mock call does in Go) and counts them.
struct MockDialContext {
    acceptable: Vec<String>,
    addr_frequency: Mutex<HashMap<String, usize>>,
}

impl MockDialContext {
    fn new(acceptable: Vec<String>) -> Arc<MockDialContext> {
        Arc::new(MockDialContext { acceptable, addr_frequency: Mutex::new(HashMap::new()) })
    }
    fn dial_context(&self, _network: &str, addr: &str, _deadline: Option<Instant>) -> io::Result<()> {
        let mut f = self.addr_frequency.lock().unwrap();
        assert!(self.acceptable.iter().any(|a| a == addr), "mock: unexpected call dialContext with addr {addr}");
        *f.entry(addr.to_string()).or_default() += 1;
        Ok(())
    }
    fn get_count(&self, addr: &str) -> usize {
        *self.addr_frequency.lock().unwrap().get(addr).unwrap_or(&0)
    }
}

struct MockedLookupHost {
    with_err: bool,
    result: Vec<String>,
}

impl HostResolver for MockedLookupHost {
    fn lookup_host(&self, _host: &str) -> Result<Vec<String>, String> {
        if self.with_err {
            return Err("this is a mocked error".to_string());
        }
        Ok(self.result.clone())
    }
}

fn create_dial_context_with_round_robin_dns(mdc: &Arc<MockDialContext>, resolver: MockedLookupHost, r: Rand) -> DialContextWithRoundRobinDns<()> {
    let m = mdc.clone();
    DialContextWithRoundRobinDns {
        dial_context: Arc::new(move |n: &str, a: &str, d: Option<Instant>| m.dial_context(n, a, d)),
        resolver: Arc::new(resolver),
        rand: Arc::new(Mutex::new(r)),
    }
}

fn now_unix() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
}

type Setup = Box<dyn Fn() -> (Arc<MockDialContext>, DialContextWithRoundRobinDns<()>)>;
type Check = Box<dyn Fn(&MockDialContext)>;

#[test]
fn test_dial_context_with_random_connections() {
    let number_of_runs = 2 * test_lookup_result().len();
    let cases: Vec<(&str, &str, Setup, Check)> = vec![
        (
            "if address contains no port call default DealContext",
            TEST_ADDR_WITHOUT_PORT,
            Box::new(|| {
                let mdc = MockDialContext::new(vec![TEST_ADDR_WITHOUT_PORT.to_string()]);
                let dc = create_dial_context_with_round_robin_dns(&mdc, MockedLookupHost { with_err: false, result: vec![] }, Rand::new(now_unix()));
                (mdc, dc)
            }),
            Box::new(move |mdc| assert_eq!(number_of_runs, mdc.get_count(TEST_ADDR_WITHOUT_PORT))),
        ),
        (
            "if lookup host returns error call default DealContext",
            TEST_ADDR_WITH_PORT,
            Box::new(|| {
                let mdc = MockDialContext::new(vec![TEST_ADDR_WITH_PORT.to_string()]);
                let dc = create_dial_context_with_round_robin_dns(&mdc, MockedLookupHost { with_err: true, result: vec![] }, Rand::new(now_unix()));
                (mdc, dc)
            }),
            Box::new(move |mdc| assert_eq!(number_of_runs, mdc.get_count(TEST_ADDR_WITH_PORT))),
        ),
        (
            "if lookup returns no addresses call default DealContext",
            TEST_ADDR_WITH_PORT,
            Box::new(|| {
                let mdc = MockDialContext::new(vec![TEST_ADDR_WITH_PORT.to_string()]);
                let dc = create_dial_context_with_round_robin_dns(&mdc, MockedLookupHost { with_err: false, result: vec![] }, Rand::new(now_unix()));
                (mdc, dc)
            }),
            Box::new(move |mdc| assert_eq!(number_of_runs, mdc.get_count(TEST_ADDR_WITH_PORT))),
        ),
        (
            "if lookup host is successful, shuffle results",
            TEST_ADDR_WITH_PORT,
            Box::new(|| {
                let mdc = MockDialContext::new(test_lookup_result_with_port());
                let dc = create_dial_context_with_round_robin_dns(&mdc, MockedLookupHost { with_err: false, result: test_lookup_result() }, Rand::new(RAND_SEED));
                (mdc, dc)
            }),
            // we ensure that not all runs will choose the first element of the lookup
            Box::new(move |mdc| assert_ne!(number_of_runs, mdc.get_count(&test_lookup_result_with_port()[0]))),
        ),
    ];

    for (name, addr, setup, check) in cases {
        eprintln!("=== RUN {name}");
        let (mdc, dc) = setup();
        for _ in 0..number_of_runs {
            let r = dc.dial_context_fn()(TEST_NETWORK, addr, None);
            assert!(r.is_ok(), "{name}: {:?}", r.err());
        }
        check(&mdc);
    }
}
