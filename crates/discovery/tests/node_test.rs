//! Port of discovery/kubernetes/node_test.go.
mod k8s_common;

use discovery::kubernetes::api::{Condition, Kind, Node, NodeAddress, Object, ObjectMeta, StrMap};
use discovery::kubernetes::{NamespaceDiscovery, Role};
use k8s_common::*;
use model::labelset::LabelSet;

fn make_node(name: &str, address: &str, provider_id: &str, labels: StrMap, annotations: StrMap, conditions: Vec<Condition>) -> Object {
    Object::Node(Node {
        meta: ObjectMeta { name: name.to_string(), labels, annotations, ..Default::default() },
        provider_id: provider_id.to_string(),
        addresses: vec![NodeAddress { type_: "InternalIP".to_string(), address: address.to_string() }],
        kubelet_port: 10250,
        conditions,
    })
}

fn make_enumerated_node(i: usize) -> Object {
    make_node(&format!("test{i}"), "1.2.3.4", &format!("aws:///de-west-3a/i-{i}"), smap(&[]), smap(&[]), Vec::new())
}

fn cond(t: &str, s: &str) -> Condition {
    Condition { type_: t.to_string(), status: s.to_string() }
}

#[test]
fn test_node_discovery_before_start() {
    let (n, c) = make_discovery(Role::NODE, NamespaceDiscovery::default(), vec![]);
    K8sDiscoveryTest::new(n)
        .before_run(move || {
            let obj = make_node("test", "1.2.3.4", "aws:///nl-north-7b/i-03149834983492827", smap(&[("test-label", "testvalue")]), smap(&[("test-annotation", "testannotationvalue")]), Vec::new());
            c.create("", obj).unwrap();
        })
        .expected_max_items(1)
        .expected_res(expect(vec![group(
            vec![ls(&[("__address__", "1.2.3.4:10250"), ("instance", "test"), ("__meta_kubernetes_node_address_InternalIP", "1.2.3.4")])],
            ls(&[
                ("__meta_kubernetes_node_name", "test"),
                ("__meta_kubernetes_node_provider_id", "aws:///nl-north-7b/i-03149834983492827"),
                ("__meta_kubernetes_node_label_test_label", "testvalue"),
                ("__meta_kubernetes_node_labelpresent_test_label", "true"),
                ("__meta_kubernetes_node_annotation_test_annotation", "testannotationvalue"),
                ("__meta_kubernetes_node_annotationpresent_test_annotation", "true"),
            ]),
            "node/test",
        )]))
        .run();
}

#[test]
fn test_node_discovery_add() {
    let (n, c) = make_discovery(Role::NODE, NamespaceDiscovery::default(), vec![]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            c.create("", make_enumerated_node(1)).unwrap();
        })
        .expected_max_items(1)
        .expected_res(expect(vec![group(
            vec![ls(&[("__address__", "1.2.3.4:10250"), ("instance", "test1"), ("__meta_kubernetes_node_address_InternalIP", "1.2.3.4")])],
            ls(&[("__meta_kubernetes_node_name", "test1"), ("__meta_kubernetes_node_provider_id", "aws:///de-west-3a/i-1")]),
            "node/test1",
        )]))
        .run();
}

#[test]
fn test_node_discovery_delete() {
    let obj = make_enumerated_node(0);
    let (n, c) = make_discovery(Role::NODE, NamespaceDiscovery::default(), vec![obj]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            c.delete(Kind::Node, "", "test0").unwrap();
        })
        .expected_max_items(2)
        .expected_res(expect(vec![empty_group("node/test0")]))
        .run();
}

#[test]
fn test_node_discovery_update() {
    let (n, c) = make_discovery(Role::NODE, NamespaceDiscovery::default(), vec![]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            c.create("", make_enumerated_node(0)).unwrap();
            let obj2 = make_node("test0", "1.2.3.4", "aws:///fr-south-1c/i-49508290343823952", smap(&[("Unschedulable", "true")]), smap(&[]), Vec::new());
            c.update("", obj2).unwrap();
        })
        .expected_max_items(2)
        .expected_res(expect(vec![group(
            vec![ls(&[("__address__", "1.2.3.4:10250"), ("instance", "test0"), ("__meta_kubernetes_node_address_InternalIP", "1.2.3.4")])],
            ls(&[
                ("__meta_kubernetes_node_label_Unschedulable", "true"),
                ("__meta_kubernetes_node_labelpresent_Unschedulable", "true"),
                ("__meta_kubernetes_node_name", "test0"),
                ("__meta_kubernetes_node_provider_id", "aws:///fr-south-1c/i-49508290343823952"),
            ]),
            "node/test0",
        )]))
        .run();
}

#[test]
fn test_node_discovery_conditions() {
    let tests: Vec<(&str, Vec<Condition>, LabelSet)> = vec![
        (
            "node ready true",
            vec![cond("Ready", "True")],
            ls(&[("__meta_kubernetes_node_name", "test"), ("__meta_kubernetes_node_provider_id", "aws:///test-zone/i-test"), ("__meta_kubernetes_node_condition_ready", "true")]),
        ),
        (
            "node ready false",
            vec![cond("Ready", "False")],
            ls(&[("__meta_kubernetes_node_name", "test"), ("__meta_kubernetes_node_provider_id", "aws:///test-zone/i-test"), ("__meta_kubernetes_node_condition_ready", "false")]),
        ),
        (
            "node ready unknown",
            vec![cond("Ready", "Unknown")],
            ls(&[("__meta_kubernetes_node_name", "test"), ("__meta_kubernetes_node_provider_id", "aws:///test-zone/i-test"), ("__meta_kubernetes_node_condition_ready", "unknown")]),
        ),
        ("node no conditions", Vec::new(), ls(&[("__meta_kubernetes_node_name", "test"), ("__meta_kubernetes_node_provider_id", "aws:///test-zone/i-test")])),
        (
            "node multiple conditions",
            vec![cond("MemoryPressure", "False"), cond("Ready", "True"), cond("DiskPressure", "False")],
            ls(&[
                ("__meta_kubernetes_node_name", "test"),
                ("__meta_kubernetes_node_provider_id", "aws:///test-zone/i-test"),
                ("__meta_kubernetes_node_condition_memorypressure", "false"),
                ("__meta_kubernetes_node_condition_ready", "true"),
                ("__meta_kubernetes_node_condition_diskpressure", "false"),
            ]),
        ),
    ];
    for (name, conditions, expected) in tests {
        eprintln!("subtest {name}");
        let (n, c) = make_discovery(Role::NODE, NamespaceDiscovery::default(), vec![]);
        K8sDiscoveryTest::new(n)
            .before_run(move || {
                c.create("", make_node("test", "1.2.3.4", "aws:///test-zone/i-test", smap(&[]), smap(&[]), conditions)).unwrap();
            })
            .expected_max_items(1)
            .expected_res(expect(vec![group(
                vec![ls(&[("__address__", "1.2.3.4:10250"), ("instance", "test"), ("__meta_kubernetes_node_address_InternalIP", "1.2.3.4")])],
                expected,
                "node/test",
            )]))
            .run();
    }
}
