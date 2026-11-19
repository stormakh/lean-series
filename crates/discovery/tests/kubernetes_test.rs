//! Port of discovery/kubernetes/kubernetes_test.go (the tests; the helpers are in k8s_common).
mod k8s_common;

use std::sync::Arc;
use std::time::Duration;

use discovery::CancelToken;
use discovery::kubernetes::api::{Node, Object, ObjectMeta};
use discovery::kubernetes::client::ApiError;
use discovery::kubernetes::informer::Event;
use discovery::kubernetes::{NamespaceDiscovery, Role, node_name, retry_on_error};
use k8s_common::*;

#[test]
fn test_retry_on_error() {
    for success_at in [1, 2, 3] {
        let mut called = 0;
        let f = || {
            called += 1;
            if called >= success_at { Ok(()) } else { Err("dummy".to_string()) }
        };
        retry_on_error(&CancelToken::new(), Duration::ZERO, f);
        assert_eq!(success_at, called);
    }
}

#[test]
fn test_failures_count_metric() {
    let tests = [(Role::NODE, 1.0), (Role::POD, 1.0), (Role::SERVICE, 1.0), (Role::ENDPOINT, 3.0), (Role::ENDPOINT_SLICE, 3.0), (Role::INGRESS, 1.0)];
    let handles: Vec<_> = tests
        .into_iter()
        .map(|(role, min_failed_watches)| {
            std::thread::spawn(move || {
                let (n, c) = make_discovery(role, NamespaceDiscovery::default(), vec![]);
                let metrics = n.metrics.clone();
                // The counter is initialized and no failures at the beginning.
                assert_eq!(0.0, metrics.failures());
                // Simulate an error on watch requests.
                c.prepend_watch_error(ApiError::unauthorized("unauthorized"));
                // Start the discovery.
                K8sDiscoveryTest::new(n).run();
                // At least the errors of the initial watches should be caught (watches are retried on errors).
                assert!(metrics.failures() >= min_failed_watches, "{role}: {} < {min_failed_watches}", metrics.failures());
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn test_node_name() {
    let node = Arc::new(Object::Node(Node { meta: ObjectMeta { name: "foo".to_string(), ..Default::default() }, ..Default::default() }));
    assert_eq!("foo", node_name(&Event::Add(&node)).unwrap());
    assert_eq!("bar", node_name(&Event::Delete { key: "bar", obj: &node, tombstone: true }).unwrap());
}
