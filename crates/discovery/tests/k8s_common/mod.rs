//! The test harness of discovery/kubernetes (Go kubernetes_test.go helpers): a discovery on the fake client,
//! `K8sDiscoveryTest::run` (Go `k8sDiscoveryTest.Run`) and target group comparison.
#![allow(dead_code)]
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use discovery::chan::{Recv, channel};
use discovery::kubernetes::api::{Object, StrMap};
use discovery::kubernetes::fake::FakeClient;
use discovery::kubernetes::{AttachMetadataConfig, Discovery, KubernetesMetrics, NamespaceDiscovery, Role, RoleSelector};
use discovery::{CancelToken, DiscovererMetrics, Group, Logger, Registry};
use model::labelset::LabelSet;

/// A label set from pairs.
pub fn ls(p: &[(&str, &str)]) -> LabelSet {
    LabelSet::from_pairs(p)
}

/// A Go `map[string]string` from pairs.
pub fn smap(p: &[(&str, &str)]) -> StrMap {
    p.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

/// A target group.
pub fn group(targets: Vec<LabelSet>, labels: LabelSet, source: &str) -> Group {
    Group { targets, labels, source: source.to_string() }
}

/// A group with only a source (Go `{Source: ...}`).
pub fn empty_group(source: &str) -> Group {
    Group { source: source.to_string(), ..Default::default() }
}

/// The expected result map (Go `map[string]*targetgroup.Group`).
pub fn expect(groups: Vec<Group>) -> BTreeMap<String, Group> {
    groups.into_iter().map(|g| (g.source.clone(), g)).collect()
}

/// Go `makeDiscoveryWithVersion`.
pub fn make_discovery(role: &str, ns: NamespaceDiscovery, objects: Vec<Object>) -> (Discovery, FakeClient) {
    let client = FakeClient::new(objects);
    let reg = Registry::new();
    let metrics = KubernetesMetrics::new(&reg);
    metrics.register().unwrap();
    let d = Discovery::with_client(Arc::new(client.clone()), Role::new(role), Logger::nop(), ns, RoleSelector::default(), "own-ns".to_string(), AttachMetadataConfig::default(), metrics);
    (d, client)
}

/// Go `makeDiscoveryWithMetadata`.
pub fn make_discovery_with_metadata(role: &str, ns: NamespaceDiscovery, attach: AttachMetadataConfig, objects: Vec<Object>) -> (Discovery, FakeClient) {
    let (mut d, c) = make_discovery(role, ns, objects);
    d.attach_metadata = attach;
    (d, c)
}

/// Go `k8sDiscoveryTest`.
pub struct K8sDiscoveryTest {
    pub discovery: Arc<Discovery>,
    pub before_run: Option<Box<dyn FnOnce()>>,
    pub after_start: Option<Box<dyn FnOnce()>>,
    pub expected_max_items: usize,
    pub expected_res: Option<BTreeMap<String, Group>>,
}

impl K8sDiscoveryTest {
    pub fn new(d: Discovery) -> K8sDiscoveryTest {
        K8sDiscoveryTest { discovery: Arc::new(d), before_run: None, after_start: None, expected_max_items: 0, expected_res: None }
    }
    pub fn before_run(mut self, f: impl FnOnce() + 'static) -> Self {
        self.before_run = Some(Box::new(f));
        self
    }
    pub fn after_start(mut self, f: impl FnOnce() + 'static) -> Self {
        self.after_start = Some(Box::new(f));
        self
    }
    pub fn expected_max_items(mut self, n: usize) -> Self {
        self.expected_max_items = n;
        self
    }
    pub fn expected_res(mut self, r: BTreeMap<String, Group>) -> Self {
        self.expected_res = Some(r);
        self
    }

    /// Go `k8sDiscoveryTest.Run`.
    pub fn run(self) {
        let ctx = CancelToken::new();
        let (tx, rx) = channel();
        if let Some(f) = self.before_run {
            f();
        }
        let d = self.discovery.clone();
        let c2 = ctx.clone();
        let th = std::thread::spawn(move || d.run(&c2, tx));

        // Wait until the discoverers are set and stable.
        let mut last = 0;
        loop {
            let l = self.discovery.discoverers().len();
            if l > 0 && l == last {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
            last = l;
        }

        let max = self.expected_max_items;
        let c3 = ctx.clone();
        let reader = std::thread::spawn(move || read_result_with_timeout(&c3, rx, max, Duration::from_secs(1)));

        let deadline = Instant::now() + Duration::from_secs(60);
        while !self.discovery.has_synced() {
            assert!(Instant::now() < deadline, "discoverer failed to sync");
            std::thread::sleep(Duration::from_millis(100));
        }
        if let Some(f) = self.after_start {
            f();
        }
        match self.expected_res {
            Some(expected) => {
                let res = reader.join().unwrap();
                require_target_groups(&expected, &res);
                ctx.cancel();
            }
            None => {
                ctx.cancel();
                reader.join().unwrap();
            }
        }
        th.join().unwrap();
    }
}

/// Go `readResultWithTimeout`: merges target groups by source until `max` sources or the timeout.
pub fn read_result_with_timeout(ctx: &CancelToken, rx: discovery::Receiver, max: usize, stop_after: Duration) -> BTreeMap<String, Group> {
    let mut res = BTreeMap::new();
    let deadline = Instant::now() + stop_after;
    loop {
        let now = Instant::now();
        if now >= deadline || ctx.is_cancelled() {
            break;
        }
        match rx.recv_timeout((deadline - now).min(Duration::from_millis(50))) {
            Recv::Update(tgs) => {
                for tg in tgs {
                    res.insert(tg.source.clone(), (*tg).clone());
                }
                if res.len() == max {
                    break;
                }
            }
            Recv::Timeout => {}
            Recv::Closed | Recv::Cancelled => break,
        }
    }
    res
}

/// Go `requireTargetGroups`: equal sources, targets (in order) and labels.
pub fn require_target_groups(expected: &BTreeMap<String, Group>, res: &BTreeMap<String, Group>) {
    assert_eq!(expected, res, "\nexpected: {expected:#?}\n     got: {res:#?}");
}
