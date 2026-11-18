//! Port of discovery/xds/xds_test.go (the polling tests; `createTestHTTPServer` and `sdConf` are in
//! xds_common). Go's `goleak.VerifyTestMain` has no Rust counterpart: every thread a test starts is joined.
mod xds_common;

use std::sync::Arc;
use std::time::Duration;

use discovery::xds::{Any, DiscoveryResponse, FetchDiscovery, PROTOCOL_V3, ProtocolVersion, ResourceClient, ResourceParser, SdConfig, XdsMetrics};
use discovery::{CancelToken, Config, DiscovererMetrics, Group, Logger, Recv, Registry, Sender};
use model::labelset::LabelSet;

type FetchFn = Box<dyn FnMut(&CancelToken) -> Result<Option<DiscoveryResponse>, String> + Send>;

/// Go `testResourceClient`.
struct TestResourceClient {
    resource_type_url: String,
    server: String,
    #[allow(dead_code)]
    protocol_version: Option<ProtocolVersion>,
    fetch: FetchFn,
}

impl ResourceClient for TestResourceClient {
    fn resource_type_url(&self) -> &str {
        &self.resource_type_url
    }
    fn server(&self) -> &str {
        &self.server
    }
    fn fetch(&mut self, ctx: &CancelToken) -> Result<Option<DiscoveryResponse>, String> {
        (self.fetch)(ctx)
    }
    fn id(&self) -> &str {
        "test-client"
    }
    fn close(&mut self) {}
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Go `constantResourceParser`.
fn constant_resource_parser(targets: Vec<LabelSet>, err: Option<String>) -> ResourceParser {
    Box::new(move |_: &[Any], _: &str| match &err {
        Some(e) => Err(e.clone()),
        None => Ok(targets.clone()),
    })
}

fn xds_metrics(reg: &Arc<Registry>) -> (Arc<XdsMetrics>, Arc<dyn DiscovererMetrics>, Arc<discovery::RefreshMetricsVecs>) {
    let refresh_metrics = discovery::new_refresh_metrics(reg);
    let metrics = SdConfig::default().new_discoverer_metrics(reg, refresh_metrics.clone());
    metrics.register().unwrap();
    let m = discovery::downcast_metrics::<XdsMetrics>(&Some(metrics.clone())).expect("invalid discovery metrics type");
    (m, metrics, refresh_metrics)
}

/// Runs `pd.poll` on another thread (Go's channel has a buffer of 1, this one is a rendezvous) and returns what
/// the receiver got: the update, or `Closed` when poll sent nothing.
fn poll_and_recv(pd: &mut FetchDiscovery, ctx: &CancelToken) -> Recv {
    let (tx, rx) = discovery::channel();
    std::thread::scope(|sc| {
        sc.spawn(move || {
            pd.poll(ctx, &tx);
            drop::<Sender>(tx);
        });
        rx.recv()
    })
}

fn update(r: Recv) -> Vec<Arc<Group>> {
    match r {
        Recv::Update(g) => g,
        other => panic!("expected an update, got {other:?}"),
    }
}

#[test]
fn test_polling_refresh_skip_update() {
    let rc = TestResourceClient { resource_type_url: String::new(), server: String::new(), protocol_version: None, fetch: Box::new(|_| Ok(None)) };

    let reg = Registry::new();
    let (xds_metrics, metrics, refresh_metrics) = xds_metrics(&reg);

    let mut pd = FetchDiscovery {
        client: Box::new(rc),
        source: String::new(),
        refresh_interval: Duration::ZERO,
        parse_resources: constant_resource_parser(Vec::new(), None),
        logger: Logger::nop(),
        metrics: xds_metrics,
    };

    let ctx = CancelToken::new();
    let c2 = ctx.clone();
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(1));
        c2.cancel();
    });

    let (tx, rx) = discovery::channel_with_cancel(&ctx);
    pd.poll(&ctx, &tx);
    match rx.recv() {
        Recv::Cancelled => {}
        Recv::Update(_) => panic!("no update expected"),
        other => panic!("unexpected {other:?}"),
    }
    canceller.join().unwrap();

    metrics.unregister();
    refresh_metrics.unregister();
}

#[test]
fn test_polling_refresh_attaches_group_metadata() {
    let server = "http://198.161.2.0";
    let source = "test";
    let rc = TestResourceClient {
        resource_type_url: String::new(),
        server: server.to_string(),
        protocol_version: Some(PROTOCOL_V3),
        fetch: Box::new(|_| Ok(Some(DiscoveryResponse::default()))),
    };

    let reg = Registry::new();
    let (xds_metrics, metrics, refresh_metrics) = xds_metrics(&reg);

    let mut pd = FetchDiscovery {
        source: source.to_string(),
        client: Box::new(rc),
        logger: Logger::nop(),
        refresh_interval: Duration::ZERO,
        parse_resources: constant_resource_parser(
            vec![
                LabelSet::from_pairs(&[("__meta_custom_xds_label", "a-value"), ("__address__", "10.1.4.32:9090"), ("instance", "prometheus-01")]),
                LabelSet::from_pairs(&[("__meta_custom_xds_label", "a-value"), ("__address__", "10.1.5.32:9090"), ("instance", "prometheus-02")]),
            ],
            None,
        ),
        metrics: xds_metrics,
    };
    let groups = update(poll_and_recv(&mut pd, &CancelToken::new()));
    assert_eq!(groups.len(), 1);

    let group = &groups[0];
    assert_eq!(source, group.source);
    assert_eq!(group.targets.len(), 2);

    let target2 = &group.targets[1];
    assert!(target2.contains_key("__meta_custom_xds_label"));
    assert_eq!(target2.get("__meta_custom_xds_label").map(String::as_str), Some("a-value"));

    metrics.unregister();
    refresh_metrics.unregister();
}

#[test]
fn test_polling_disappearing_targets() {
    let server = "http://198.161.2.0";
    let source = "test";
    let rc = TestResourceClient {
        resource_type_url: String::new(),
        server: server.to_string(),
        protocol_version: Some(PROTOCOL_V3),
        fetch: Box::new(|_| Ok(Some(DiscoveryResponse::default()))),
    };

    // On the first poll, send back two targets. On the next, send just one.
    let mut counter = 0;
    let parser: ResourceParser = Box::new(move |_: &[Any], _: &str| {
        counter += 1;
        if counter == 1 {
            return Ok(vec![
                LabelSet::from_pairs(&[("__meta_custom_xds_label", "a-value"), ("__address__", "10.1.4.32:9090"), ("instance", "prometheus-01")]),
                LabelSet::from_pairs(&[("__meta_custom_xds_label", "a-value"), ("__address__", "10.1.5.32:9090"), ("instance", "prometheus-02")]),
            ]);
        }
        Ok(vec![LabelSet::from_pairs(&[("__meta_custom_xds_label", "a-value"), ("__address__", "10.1.4.32:9090"), ("instance", "prometheus-01")])])
    });

    let reg = Registry::new();
    let (xds_metrics, metrics, refresh_metrics) = xds_metrics(&reg);

    let mut pd = FetchDiscovery { source: source.to_string(), client: Box::new(rc), logger: Logger::nop(), refresh_interval: Duration::ZERO, parse_resources: parser, metrics: xds_metrics };

    let groups = update(poll_and_recv(&mut pd, &CancelToken::new()));
    assert_eq!(groups.len(), 1);
    assert_eq!(source, groups[0].source);
    assert_eq!(groups[0].targets.len(), 2);

    let groups = update(poll_and_recv(&mut pd, &CancelToken::new()));
    assert_eq!(groups.len(), 1);
    assert_eq!(source, groups[0].source);
    assert_eq!(groups[0].targets.len(), 1);

    metrics.unregister();
    refresh_metrics.unregister();
}
