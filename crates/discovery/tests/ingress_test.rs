//! Port of discovery/kubernetes/ingress_test.go.
mod k8s_common;

use std::collections::BTreeMap;

use discovery::Group;
use discovery::kubernetes::api::{Ingress, IngressRule, Object, ObjectMeta, StrMap};
use discovery::kubernetes::{AttachMetadataConfig, NamespaceDiscovery, Role};
use k8s_common::*;

#[allow(dead_code)] // Go also never uses TLSWildcard.
#[derive(Clone, Copy, PartialEq)]
enum TlsMode {
    No,
    Yes,
    Mixed,
    Wildcard,
}

fn make_ingress(namespace: &str, tls: TlsMode) -> Object {
    let mut ret = Ingress {
        meta: ObjectMeta {
            name: "testingress".to_string(),
            namespace: namespace.to_string(),
            labels: smap(&[("test/label", "testvalue")]),
            annotations: smap(&[("test/annotation", "testannotationvalue")]),
            ..Default::default()
        },
        ingress_class_name: Some("testclass".to_string()),
        tls_hosts: Vec::new(),
        rules: vec![
            IngressRule { host: "example.com".to_string(), http_paths: Some(vec!["/".to_string(), "/foo".to_string()]) },
            // No backend config, ignored
            IngressRule { host: "nobackend.example.com".to_string(), http_paths: Some(Vec::new()) },
            // Go `Paths: []v1.HTTPIngressPath{{}}`: one path with an empty Path.
            IngressRule { host: "test.example.com".to_string(), http_paths: Some(vec![String::new()]) },
        ],
    };
    let hosts = |h: &[&str]| vec![h.iter().map(|s| s.to_string()).collect::<Vec<_>>()];
    match tls {
        TlsMode::Yes => ret.tls_hosts = hosts(&["example.com", "test.example.com"]),
        TlsMode::Mixed => ret.tls_hosts = hosts(&["example.com"]),
        TlsMode::Wildcard => ret.tls_hosts = hosts(&["*.example.com"]),
        TlsMode::No => {}
    }
    Object::Ingress(ret)
}

fn expected_target_groups(ns: &str, tls: TlsMode) -> BTreeMap<String, Group> {
    let (mut scheme1, mut scheme2) = ("http", "http");
    match tls {
        TlsMode::Yes => {
            scheme1 = "https";
            scheme2 = "https";
        }
        TlsMode::Mixed => scheme1 = "https",
        TlsMode::Wildcard => scheme2 = "https",
        TlsMode::No => {}
    }
    let key = format!("ingress/{ns}/testingress");
    expect(vec![group(
        vec![
            ls(&[("__meta_kubernetes_ingress_scheme", scheme1), ("__meta_kubernetes_ingress_host", "example.com"), ("__meta_kubernetes_ingress_path", "/"), ("__address__", "example.com")]),
            ls(&[("__meta_kubernetes_ingress_scheme", scheme1), ("__meta_kubernetes_ingress_host", "example.com"), ("__meta_kubernetes_ingress_path", "/foo"), ("__address__", "example.com")]),
            ls(&[("__meta_kubernetes_ingress_scheme", scheme2), ("__meta_kubernetes_ingress_host", "test.example.com"), ("__address__", "test.example.com"), ("__meta_kubernetes_ingress_path", "/")]),
        ],
        ls(&[
            ("__meta_kubernetes_ingress_name", "testingress"),
            ("__meta_kubernetes_namespace", ns),
            ("__meta_kubernetes_ingress_label_test_label", "testvalue"),
            ("__meta_kubernetes_ingress_labelpresent_test_label", "true"),
            ("__meta_kubernetes_ingress_annotation_test_annotation", "testannotationvalue"),
            ("__meta_kubernetes_ingress_annotationpresent_test_annotation", "true"),
            ("__meta_kubernetes_ingress_class_name", "testclass"),
        ]),
        &key,
    )])
}

/// Go `m[k] = v` on a map.
fn set(m: &mut StrMap, k: &str, v: &str) {
    match m.iter_mut().find(|(mk, _)| mk == k) {
        Some(e) => e.1 = v.to_string(),
        None => m.push((k.to_string(), v.to_string())),
    }
}

/// Go `makeNamespace` (endpointslice_test.go).
fn make_namespace(name: &str, labels: StrMap, annotations: StrMap) -> ObjectMeta {
    ObjectMeta { name: name.to_string(), labels, annotations, ..Default::default() }
}

fn names(n: &[&str]) -> NamespaceDiscovery {
    NamespaceDiscovery { names: n.iter().map(|s| s.to_string()).collect(), ..Default::default() }
}

fn run_add(tls: TlsMode) {
    let (n, c) = make_discovery(Role::INGRESS, names(&["default"]), vec![]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            let _ = c.create("default", make_ingress("default", tls));
        })
        .expected_max_items(1)
        .expected_res(expected_target_groups("default", tls))
        .run();
}

#[test]
fn test_ingress_discovery_add() {
    run_add(TlsMode::No);
}

#[test]
fn test_ingress_discovery_add_tls() {
    run_add(TlsMode::Yes);
}

#[test]
fn test_ingress_discovery_add_mixed() {
    run_add(TlsMode::Mixed);
}

#[test]
fn test_ingress_discovery_namespaces() {
    let (n, c) = make_discovery(Role::INGRESS, names(&["ns1", "ns2"]), vec![]);
    let mut expected = expected_target_groups("ns1", TlsMode::No);
    expected.extend(expected_target_groups("ns2", TlsMode::No));
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            for ns in ["ns1", "ns2"] {
                let _ = c.create(ns, make_ingress(ns, TlsMode::No));
            }
        })
        .expected_max_items(2)
        .expected_res(expected)
        .run();
}

#[test]
fn test_ingress_discovery_own_namespace() {
    let (n, c) = make_discovery(Role::INGRESS, NamespaceDiscovery { include_own_namespace: true, ..Default::default() }, vec![]);
    let expected = expected_target_groups("own-ns", TlsMode::No);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            for ns in ["own-ns", "non-own-ns"] {
                let _ = c.create(ns, make_ingress(ns, TlsMode::No));
            }
        })
        .expected_max_items(1)
        .expected_res(expected)
        .run();
}

fn ingress_targets() -> Vec<model::labelset::LabelSet> {
    vec![
        ls(&[("__address__", "example.com"), ("__meta_kubernetes_ingress_host", "example.com"), ("__meta_kubernetes_ingress_path", "/"), ("__meta_kubernetes_ingress_scheme", "http")]),
        ls(&[("__address__", "example.com"), ("__meta_kubernetes_ingress_host", "example.com"), ("__meta_kubernetes_ingress_path", "/foo"), ("__meta_kubernetes_ingress_scheme", "http")]),
        ls(&[("__address__", "test.example.com"), ("__meta_kubernetes_ingress_host", "test.example.com"), ("__meta_kubernetes_ingress_path", "/"), ("__meta_kubernetes_ingress_scheme", "http")]),
    ]
}

#[test]
fn test_ingress_discovery_with_namespace_metadata() {
    let ns = "test-ns";
    let ns_labels = smap(&[("service", "web"), ("layer", "frontend")]);
    let ns_annotations = smap(&[("contact", "platform"), ("release", "v5.6.7")]);

    let (n, _c) = make_discovery_with_metadata(
        Role::INGRESS,
        NamespaceDiscovery::default(),
        AttachMetadataConfig { namespace: true, ..Default::default() },
        vec![Object::Namespace(make_namespace(ns, ns_labels, ns_annotations)), make_ingress(ns, TlsMode::No)],
    );
    K8sDiscoveryTest::new(n)
        .expected_max_items(1)
        .expected_res(expect(vec![group(
            ingress_targets(),
            ls(&[
                ("__meta_kubernetes_namespace", ns),
                ("__meta_kubernetes_namespace_annotation_contact", "platform"),
                ("__meta_kubernetes_namespace_annotationpresent_contact", "true"),
                ("__meta_kubernetes_namespace_annotation_release", "v5.6.7"),
                ("__meta_kubernetes_namespace_annotationpresent_release", "true"),
                ("__meta_kubernetes_namespace_label_service", "web"),
                ("__meta_kubernetes_namespace_labelpresent_service", "true"),
                ("__meta_kubernetes_namespace_label_layer", "frontend"),
                ("__meta_kubernetes_namespace_labelpresent_layer", "true"),
                ("__meta_kubernetes_ingress_name", "testingress"),
                ("__meta_kubernetes_ingress_label_test_label", "testvalue"),
                ("__meta_kubernetes_ingress_labelpresent_test_label", "true"),
                ("__meta_kubernetes_ingress_annotation_test_annotation", "testannotationvalue"),
                ("__meta_kubernetes_ingress_annotationpresent_test_annotation", "true"),
                ("__meta_kubernetes_ingress_class_name", "testclass"),
            ]),
            &format!("ingress/{ns}/testingress"),
        )]))
        .run();
}

#[test]
fn test_ingress_discovery_with_updated_namespace_metadata() {
    let ns = "test-ns";
    let ns_labels = smap(&[("component", "database"), ("layer", "backend")]);
    let ns_annotations = smap(&[("contact", "dba"), ("release", "v6.7.8")]);

    let mut namespace = make_namespace(ns, ns_labels, ns_annotations);
    let (n, c) = make_discovery_with_metadata(
        Role::INGRESS,
        NamespaceDiscovery::default(),
        AttachMetadataConfig { namespace: true, ..Default::default() },
        vec![Object::Namespace(namespace.clone()), make_ingress(ns, TlsMode::No)],
    );
    K8sDiscoveryTest::new(n)
        .expected_max_items(2)
        .after_start(move || {
            set(&mut namespace.labels, "component", "cache");
            set(&mut namespace.labels, "region", "us-central");
            set(&mut namespace.annotations, "contact", "sre");
            set(&mut namespace.annotations, "monitoring", "enabled");
            let _ = c.update("", Object::Namespace(namespace));
        })
        .expected_res(expect(vec![group(
            ingress_targets(),
            ls(&[
                ("__meta_kubernetes_namespace", ns),
                ("__meta_kubernetes_namespace_annotation_contact", "sre"),
                ("__meta_kubernetes_namespace_annotationpresent_contact", "true"),
                ("__meta_kubernetes_namespace_annotation_release", "v6.7.8"),
                ("__meta_kubernetes_namespace_annotationpresent_release", "true"),
                ("__meta_kubernetes_namespace_annotation_monitoring", "enabled"),
                ("__meta_kubernetes_namespace_annotationpresent_monitoring", "true"),
                ("__meta_kubernetes_namespace_label_component", "cache"),
                ("__meta_kubernetes_namespace_labelpresent_component", "true"),
                ("__meta_kubernetes_namespace_label_layer", "backend"),
                ("__meta_kubernetes_namespace_labelpresent_layer", "true"),
                ("__meta_kubernetes_namespace_label_region", "us-central"),
                ("__meta_kubernetes_namespace_labelpresent_region", "true"),
                ("__meta_kubernetes_ingress_name", "testingress"),
                ("__meta_kubernetes_ingress_label_test_label", "testvalue"),
                ("__meta_kubernetes_ingress_labelpresent_test_label", "true"),
                ("__meta_kubernetes_ingress_annotation_test_annotation", "testannotationvalue"),
                ("__meta_kubernetes_ingress_annotationpresent_test_annotation", "true"),
                ("__meta_kubernetes_ingress_class_name", "testclass"),
            ]),
            &format!("ingress/{ns}/testingress"),
        )]))
        .run();
}
