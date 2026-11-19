//! Port of discovery/kubernetes/service_test.go.
mod k8s_common;

use discovery::kubernetes::api::{Kind, Object, ObjectMeta, Service, ServicePort, ServiceSpec, StrMap};
use discovery::kubernetes::{AttachMetadataConfig, NamespaceDiscovery, Role};
use k8s_common::*;

fn port(name: &str, protocol: &str, port: i32) -> ServicePort {
    ServicePort { name: name.to_string(), protocol: protocol.to_string(), port }
}

fn make_multi_port_service() -> Object {
    Object::Service(Service {
        meta: ObjectMeta {
            name: "testservice".to_string(),
            namespace: "default".to_string(),
            labels: smap(&[("test-label", "testvalue")]),
            annotations: smap(&[("test-annotation", "testannotationvalue")]),
            ..Default::default()
        },
        spec: ServiceSpec {
            ports: vec![port("testport0", "TCP", 30900), port("testport1", "UDP", 30901)],
            type_: "ClusterIP".to_string(),
            cluster_ip: "10.0.0.1".to_string(),
            ..Default::default()
        },
        ..Default::default()
    })
}

fn make_service(namespace: &str) -> Object {
    Object::Service(Service {
        meta: ObjectMeta { name: "testservice".to_string(), namespace: namespace.to_string(), ..Default::default() },
        spec: ServiceSpec { ports: vec![port("testport", "TCP", 30900)], type_: "ClusterIP".to_string(), cluster_ip: "10.0.0.1".to_string(), ..Default::default() },
        ..Default::default()
    })
}

fn make_external_service() -> Object {
    Object::Service(Service {
        meta: ObjectMeta { name: "testservice-external".to_string(), namespace: "default".to_string(), ..Default::default() },
        spec: ServiceSpec { ports: vec![port("testport", "TCP", 31900)], type_: "ExternalName".to_string(), external_name: "FooExternalName".to_string(), ..Default::default() },
        ..Default::default()
    })
}

fn make_load_balancer_service() -> Object {
    Object::Service(Service {
        meta: ObjectMeta { name: "testservice-loadbalancer".to_string(), namespace: "default".to_string(), ..Default::default() },
        spec: ServiceSpec {
            ports: vec![port("testport", "TCP", 31900)],
            type_: "LoadBalancer".to_string(),
            load_balancer_ip: "127.0.0.1".to_string(),
            cluster_ip: "10.0.0.1".to_string(),
            ..Default::default()
        },
        ..Default::default()
    })
}

fn make_load_balancer_service_from_ingress() -> Object {
    Object::Service(Service {
        meta: ObjectMeta { name: "testservice-lb-ingress".to_string(), namespace: "default".to_string(), ..Default::default() },
        spec: ServiceSpec { ports: vec![port("testport", "TCP", 31900)], type_: "LoadBalancer".to_string(), cluster_ip: "10.0.0.1".to_string(), ..Default::default() },
        load_balancer_ingress_ips: vec!["192.168.1.15".to_string(), "2001:db8::1".to_string()],
    })
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

fn ns_of(o: &Object) -> String {
    match o {
        Object::Service(s) => s.meta.namespace.clone(),
        _ => unreachable!(),
    }
}

/// The ClusterIP target of `make_service(ns)`.
fn cluster_ip_group(ns: &str) -> discovery::Group {
    let source = format!("svc/{ns}/testservice");
    let addr = format!("testservice.{ns}.svc:30900");
    group(
        vec![ls(&[
            ("__meta_kubernetes_service_port_protocol", "TCP"),
            ("__address__", &addr),
            ("__meta_kubernetes_service_type", "ClusterIP"),
            ("__meta_kubernetes_service_cluster_ip", "10.0.0.1"),
            ("__meta_kubernetes_service_port_name", "testport"),
            ("__meta_kubernetes_service_port_number", "30900"),
        ])],
        ls(&[("__meta_kubernetes_service_name", "testservice"), ("__meta_kubernetes_namespace", ns)]),
        &source,
    )
}

#[test]
fn test_service_discovery_add() {
    let (n, c) = make_discovery(Role::SERVICE, NamespaceDiscovery::default(), vec![]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            for obj in [make_service("default"), make_external_service(), make_load_balancer_service(), make_load_balancer_service_from_ingress()] {
                let ns = ns_of(&obj);
                let _ = c.create(&ns, obj);
            }
        })
        .expected_max_items(4)
        .expected_res(expect(vec![
            group(
                vec![ls(&[
                    ("__meta_kubernetes_service_port_protocol", "TCP"),
                    ("__address__", "testservice.default.svc:30900"),
                    ("__meta_kubernetes_service_type", "ClusterIP"),
                    ("__meta_kubernetes_service_cluster_ip", "10.0.0.1"),
                    ("__meta_kubernetes_service_port_name", "testport"),
                    ("__meta_kubernetes_service_port_number", "30900"),
                ])],
                ls(&[("__meta_kubernetes_service_name", "testservice"), ("__meta_kubernetes_namespace", "default")]),
                "svc/default/testservice",
            ),
            group(
                vec![ls(&[
                    ("__meta_kubernetes_service_port_protocol", "TCP"),
                    ("__address__", "testservice-external.default.svc:31900"),
                    ("__meta_kubernetes_service_type", "ExternalName"),
                    ("__meta_kubernetes_service_port_name", "testport"),
                    ("__meta_kubernetes_service_port_number", "31900"),
                    ("__meta_kubernetes_service_external_name", "FooExternalName"),
                ])],
                ls(&[("__meta_kubernetes_service_name", "testservice-external"), ("__meta_kubernetes_namespace", "default")]),
                "svc/default/testservice-external",
            ),
            group(
                vec![ls(&[
                    ("__meta_kubernetes_service_port_protocol", "TCP"),
                    ("__address__", "testservice-loadbalancer.default.svc:31900"),
                    ("__meta_kubernetes_service_type", "LoadBalancer"),
                    ("__meta_kubernetes_service_port_name", "testport"),
                    ("__meta_kubernetes_service_port_number", "31900"),
                    ("__meta_kubernetes_service_cluster_ip", "10.0.0.1"),
                    ("__meta_kubernetes_service_loadbalancer_ip", "127.0.0.1"),
                ])],
                ls(&[("__meta_kubernetes_service_name", "testservice-loadbalancer"), ("__meta_kubernetes_namespace", "default")]),
                "svc/default/testservice-loadbalancer",
            ),
            group(
                vec![ls(&[
                    ("__meta_kubernetes_service_port_protocol", "TCP"),
                    ("__address__", "testservice-lb-ingress.default.svc:31900"),
                    ("__meta_kubernetes_service_type", "LoadBalancer"),
                    ("__meta_kubernetes_service_port_name", "testport"),
                    ("__meta_kubernetes_service_port_number", "31900"),
                    ("__meta_kubernetes_service_cluster_ip", "10.0.0.1"),
                    ("__meta_kubernetes_service_loadbalancer_ip", "192.168.1.15,2001:db8::1"),
                ])],
                ls(&[("__meta_kubernetes_service_name", "testservice-lb-ingress"), ("__meta_kubernetes_namespace", "default")]),
                "svc/default/testservice-lb-ingress",
            ),
        ]))
        .run();
}

#[test]
fn test_service_discovery_delete() {
    let (n, c) = make_discovery(Role::SERVICE, NamespaceDiscovery::default(), vec![make_service("default")]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            let _ = c.delete(Kind::Service, "default", "testservice");
        })
        .expected_max_items(2)
        .expected_res(expect(vec![empty_group("svc/default/testservice")]))
        .run();
}

#[test]
fn test_service_discovery_update() {
    let (n, c) = make_discovery(Role::SERVICE, NamespaceDiscovery::default(), vec![make_service("default")]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            let _ = c.update("default", make_multi_port_service());
        })
        .expected_max_items(2)
        .expected_res(expect(vec![group(
            vec![
                ls(&[
                    ("__meta_kubernetes_service_port_protocol", "TCP"),
                    ("__address__", "testservice.default.svc:30900"),
                    ("__meta_kubernetes_service_type", "ClusterIP"),
                    ("__meta_kubernetes_service_cluster_ip", "10.0.0.1"),
                    ("__meta_kubernetes_service_port_name", "testport0"),
                    ("__meta_kubernetes_service_port_number", "30900"),
                ]),
                ls(&[
                    ("__meta_kubernetes_service_port_protocol", "UDP"),
                    ("__address__", "testservice.default.svc:30901"),
                    ("__meta_kubernetes_service_type", "ClusterIP"),
                    ("__meta_kubernetes_service_cluster_ip", "10.0.0.1"),
                    ("__meta_kubernetes_service_port_name", "testport1"),
                    ("__meta_kubernetes_service_port_number", "30901"),
                ]),
            ],
            ls(&[
                ("__meta_kubernetes_service_name", "testservice"),
                ("__meta_kubernetes_namespace", "default"),
                ("__meta_kubernetes_service_label_test_label", "testvalue"),
                ("__meta_kubernetes_service_labelpresent_test_label", "true"),
                ("__meta_kubernetes_service_annotation_test_annotation", "testannotationvalue"),
                ("__meta_kubernetes_service_annotationpresent_test_annotation", "true"),
            ]),
            "svc/default/testservice",
        )]))
        .run();
}

#[test]
fn test_service_discovery_namespaces() {
    let (n, c) = make_discovery(Role::SERVICE, NamespaceDiscovery { names: vec!["ns1".to_string(), "ns2".to_string()], ..Default::default() }, vec![]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            for ns in ["ns1", "ns2"] {
                let _ = c.create(ns, make_service(ns));
            }
        })
        .expected_max_items(2)
        .expected_res(expect(vec![cluster_ip_group("ns1"), cluster_ip_group("ns2")]))
        .run();
}

#[test]
fn test_service_discovery_own_namespace() {
    let (n, c) = make_discovery(Role::SERVICE, NamespaceDiscovery { include_own_namespace: true, ..Default::default() }, vec![]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            for ns in ["own-ns", "non-own-ns"] {
                let _ = c.create(ns, make_service(ns));
            }
        })
        .expected_max_items(1)
        .expected_res(expect(vec![cluster_ip_group("own-ns")]))
        .run();
}

#[test]
fn test_service_discovery_all_namespaces() {
    let (n, c) = make_discovery(Role::SERVICE, NamespaceDiscovery::default(), vec![]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            for ns in ["own-ns", "non-own-ns"] {
                let _ = c.create(ns, make_service(ns));
            }
        })
        .expected_max_items(2)
        .expected_res(expect(vec![cluster_ip_group("own-ns"), cluster_ip_group("non-own-ns")]))
        .run();
}

#[test]
fn test_service_discovery_with_namespace_metadata() {
    let ns = "test-ns";
    let ns_labels = smap(&[("environment", "production"), ("team", "backend")]);
    let ns_annotations = smap(&[("owner", "platform"), ("version", "v1.2.3")]);

    let (n, _c) = make_discovery_with_metadata(
        Role::SERVICE,
        NamespaceDiscovery::default(),
        AttachMetadataConfig { namespace: true, ..Default::default() },
        vec![Object::Namespace(make_namespace(ns, ns_labels, ns_annotations)), make_service(ns)],
    );
    K8sDiscoveryTest::new(n)
        .expected_max_items(1)
        .expected_res(expect(vec![group(
            vec![ls(&[
                ("__address__", "testservice.test-ns.svc:30900"),
                ("__meta_kubernetes_service_cluster_ip", "10.0.0.1"),
                ("__meta_kubernetes_service_port_name", "testport"),
                ("__meta_kubernetes_service_port_number", "30900"),
                ("__meta_kubernetes_service_port_protocol", "TCP"),
                ("__meta_kubernetes_service_type", "ClusterIP"),
            ])],
            ls(&[
                ("__meta_kubernetes_namespace", ns),
                ("__meta_kubernetes_namespace_annotation_owner", "platform"),
                ("__meta_kubernetes_namespace_annotationpresent_owner", "true"),
                ("__meta_kubernetes_namespace_annotation_version", "v1.2.3"),
                ("__meta_kubernetes_namespace_annotationpresent_version", "true"),
                ("__meta_kubernetes_namespace_label_environment", "production"),
                ("__meta_kubernetes_namespace_labelpresent_environment", "true"),
                ("__meta_kubernetes_namespace_label_team", "backend"),
                ("__meta_kubernetes_namespace_labelpresent_team", "true"),
                ("__meta_kubernetes_service_name", "testservice"),
            ]),
            &format!("svc/{ns}/testservice"),
        )]))
        .run();
}

#[test]
fn test_service_discovery_with_updated_namespace_metadata() {
    let ns = "test-ns";
    let ns_labels = smap(&[("environment", "development"), ("team", "frontend")]);
    let ns_annotations = smap(&[("owner", "devops"), ("version", "v2.1.0")]);

    let mut namespace = make_namespace(ns, ns_labels, ns_annotations);
    let (n, c) = make_discovery_with_metadata(
        Role::SERVICE,
        NamespaceDiscovery::default(),
        AttachMetadataConfig { namespace: true, ..Default::default() },
        vec![Object::Namespace(namespace.clone()), make_service(ns)],
    );
    K8sDiscoveryTest::new(n)
        .expected_max_items(2)
        .after_start(move || {
            set(&mut namespace.labels, "environment", "staging");
            set(&mut namespace.labels, "region", "us-west");
            set(&mut namespace.annotations, "owner", "sre");
            set(&mut namespace.annotations, "cost-center", "engineering");
            let _ = c.update("", Object::Namespace(namespace));
        })
        .expected_res(expect(vec![group(
            vec![ls(&[
                ("__address__", "testservice.test-ns.svc:30900"),
                ("__meta_kubernetes_service_cluster_ip", "10.0.0.1"),
                ("__meta_kubernetes_service_port_name", "testport"),
                ("__meta_kubernetes_service_port_number", "30900"),
                ("__meta_kubernetes_service_port_protocol", "TCP"),
                ("__meta_kubernetes_service_type", "ClusterIP"),
            ])],
            ls(&[
                ("__meta_kubernetes_namespace", ns),
                ("__meta_kubernetes_namespace_annotation_owner", "sre"),
                ("__meta_kubernetes_namespace_annotationpresent_owner", "true"),
                ("__meta_kubernetes_namespace_annotation_version", "v2.1.0"),
                ("__meta_kubernetes_namespace_annotationpresent_version", "true"),
                ("__meta_kubernetes_namespace_annotation_cost_center", "engineering"),
                ("__meta_kubernetes_namespace_annotationpresent_cost_center", "true"),
                ("__meta_kubernetes_namespace_label_environment", "staging"),
                ("__meta_kubernetes_namespace_labelpresent_environment", "true"),
                ("__meta_kubernetes_namespace_label_team", "frontend"),
                ("__meta_kubernetes_namespace_labelpresent_team", "true"),
                ("__meta_kubernetes_namespace_label_region", "us-west"),
                ("__meta_kubernetes_namespace_labelpresent_region", "true"),
                ("__meta_kubernetes_service_name", "testservice"),
            ]),
            &format!("svc/{ns}/testservice"),
        )]))
        .run();
}
