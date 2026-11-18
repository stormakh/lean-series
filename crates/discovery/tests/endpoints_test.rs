//! Port of discovery/kubernetes/endpoints_test.go.
mod k8s_common;

use discovery::kubernetes::api::{
    Condition, Container, ContainerPort, EndpointAddress, EndpointPort, EndpointSubset, Endpoints, Kind, Node, NodeAddress, Object, ObjectMeta, ObjectReference, Pod, PodSpec, PodStatus, Service, StrMap,
};
use discovery::kubernetes::{AttachMetadataConfig, NamespaceDiscovery, Role};
use k8s_common::*;
use model::labelset::LabelSet;

fn s(v: &str) -> String {
    v.to_string()
}

fn ep_port(name: &str, port: i32) -> EndpointPort {
    EndpointPort { name: s(name), port, protocol: s("TCP") }
}

fn addr(ip: &str) -> EndpointAddress {
    EndpointAddress { ip: s(ip), ..Default::default() }
}

fn pod_ref_addr(ip: &str, name: &str, ns: &str) -> EndpointAddress {
    EndpointAddress { ip: s(ip), target_ref: Some(ObjectReference { kind: s("Pod"), name: s(name), namespace: s(ns) }), ..Default::default() }
}

fn container(name: &str, image: &str, port_name: &str, port: i32) -> Container {
    Container { name: s(name), image: s(image), ports: vec![ContainerPort { name: s(port_name), container_port: port, protocol: s("TCP") }] }
}

fn meta(name: &str, ns: &str) -> ObjectMeta {
    ObjectMeta { name: s(name), namespace: s(ns), ..Default::default() }
}

/// Go `makeEndpoints`.
fn make_endpoints(namespace: &str) -> Endpoints {
    Endpoints {
        meta: ObjectMeta { name: s("testendpoints"), namespace: s(namespace), annotations: smap(&[("test.annotation", "test")]), ..Default::default() },
        subsets: vec![
            EndpointSubset {
                addresses: vec![EndpointAddress { ip: s("1.2.3.4"), hostname: s("testendpoint1"), node_name: Some(s("foobar")), ..Default::default() }],
                ports: vec![ep_port("testport", 9000)],
                ..Default::default()
            },
            EndpointSubset { addresses: vec![addr("2.3.4.5")], not_ready_addresses: vec![addr("2.3.4.5")], ports: vec![ep_port("testport", 9001)] },
            EndpointSubset {
                addresses: vec![EndpointAddress { ip: s("6.7.8.9"), target_ref: Some(ObjectReference { kind: s("Node"), name: s("barbaz"), ..Default::default() }), ..Default::default() }],
                ports: vec![ep_port("testport", 9002)],
                ..Default::default()
            },
        ],
    }
}

/// Go `makeNode` (node_test.go).
fn make_node(name: &str, address: &str, provider_id: &str, labels: StrMap, annotations: StrMap, conditions: Vec<Condition>) -> Node {
    Node {
        meta: ObjectMeta { name: s(name), labels, annotations, ..Default::default() },
        provider_id: s(provider_id),
        addresses: vec![NodeAddress { type_: s("InternalIP"), address: s(address) }],
        kubelet_port: 10250,
        conditions,
    }
}

/// Go `makeNamespace` (endpointslice_test.go).
fn make_namespace(name: &str, labels: StrMap, annotations: StrMap) -> ObjectMeta {
    ObjectMeta { name: s(name), labels, annotations, ..Default::default() }
}

fn test_service(ns: &str, labels: &[(&str, &str)]) -> Service {
    Service { meta: ObjectMeta { name: s("testendpoints"), namespace: s(ns), labels: smap(labels), ..Default::default() }, ..Default::default() }
}

/// The four targets of `make_endpoints`; `node1`/`node2` add node metadata labels to the first and last target.
fn make_endpoints_targets(node1: &[(&str, &str)], node2: &[(&str, &str)]) -> Vec<LabelSet> {
    let mut t1 = vec![
        ("__address__", "1.2.3.4:9000"),
        ("__meta_kubernetes_endpoint_hostname", "testendpoint1"),
        ("__meta_kubernetes_endpoint_node_name", "foobar"),
        ("__meta_kubernetes_endpoint_port_name", "testport"),
        ("__meta_kubernetes_endpoint_port_protocol", "TCP"),
        ("__meta_kubernetes_endpoint_ready", "true"),
    ];
    t1.extend_from_slice(node1);
    let mut t4 = vec![
        ("__address__", "6.7.8.9:9002"),
        ("__meta_kubernetes_endpoint_address_target_kind", "Node"),
        ("__meta_kubernetes_endpoint_address_target_name", "barbaz"),
        ("__meta_kubernetes_endpoint_port_name", "testport"),
        ("__meta_kubernetes_endpoint_port_protocol", "TCP"),
        ("__meta_kubernetes_endpoint_ready", "true"),
    ];
    t4.extend_from_slice(node2);
    vec![
        ls(&t1),
        ls(&[("__address__", "2.3.4.5:9001"), ("__meta_kubernetes_endpoint_port_name", "testport"), ("__meta_kubernetes_endpoint_port_protocol", "TCP"), ("__meta_kubernetes_endpoint_ready", "true")]),
        ls(&[("__address__", "2.3.4.5:9001"), ("__meta_kubernetes_endpoint_port_name", "testport"), ("__meta_kubernetes_endpoint_port_protocol", "TCP"), ("__meta_kubernetes_endpoint_ready", "false")]),
        ls(&t4),
    ]
}

#[test]
fn test_endpoints_discovery_before_run() {
    let (n, c) = make_discovery(Role::ENDPOINT, NamespaceDiscovery::default(), vec![]);
    K8sDiscoveryTest::new(n)
        .before_run(move || {
            let obj = make_endpoints("default");
            let ns = obj.meta.namespace.clone();
            c.create(&ns, Object::Endpoints(obj)).unwrap();
        })
        .expected_max_items(1)
        .expected_res(expect(vec![group(
            make_endpoints_targets(&[], &[]),
            ls(&[
                ("__meta_kubernetes_namespace", "default"),
                ("__meta_kubernetes_endpoints_name", "testendpoints"),
                ("__meta_kubernetes_endpoints_annotation_test_annotation", "test"),
                ("__meta_kubernetes_endpoints_annotationpresent_test_annotation", "true"),
            ]),
            "endpoints/default/testendpoints",
        )]))
        .run();
}

#[test]
fn test_endpoints_discovery_add() {
    let obj = Pod {
        meta: ObjectMeta { name: s("testpod"), namespace: s("default"), uid: s("deadbeef"), ..Default::default() },
        spec: PodSpec { node_name: s("testnode"), containers: vec![container("c1", "c1:latest", "mainport", 9000), container("c2", "c2:latest", "sideport", 9001)], ..Default::default() },
        status: PodStatus { host_ip: s("2.3.4.5"), pod_ip: s("1.2.3.4"), ..Default::default() },
    };
    let (n, c) = make_discovery(Role::ENDPOINT, NamespaceDiscovery::default(), vec![Object::Pod(obj)]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            let obj = Endpoints {
                meta: meta("testendpoints", "default"),
                subsets: vec![EndpointSubset { addresses: vec![pod_ref_addr("4.3.2.1", "testpod", "default")], ports: vec![ep_port("testport", 9000)], ..Default::default() }],
            };
            let _ = c.create("default", Object::Endpoints(obj));
        })
        .expected_max_items(1)
        .expected_res(expect(vec![group(
            vec![
                ls(&[
                    ("__address__", "4.3.2.1:9000"),
                    ("__meta_kubernetes_endpoint_port_name", "testport"),
                    ("__meta_kubernetes_endpoint_port_protocol", "TCP"),
                    ("__meta_kubernetes_endpoint_ready", "true"),
                    ("__meta_kubernetes_endpoint_address_target_kind", "Pod"),
                    ("__meta_kubernetes_endpoint_address_target_name", "testpod"),
                    ("__meta_kubernetes_pod_name", "testpod"),
                    ("__meta_kubernetes_pod_ip", "1.2.3.4"),
                    ("__meta_kubernetes_pod_ready", "unknown"),
                    ("__meta_kubernetes_pod_phase", ""),
                    ("__meta_kubernetes_pod_node_name", "testnode"),
                    ("__meta_kubernetes_pod_host_ip", "2.3.4.5"),
                    ("__meta_kubernetes_pod_container_name", "c1"),
                    ("__meta_kubernetes_pod_container_image", "c1:latest"),
                    ("__meta_kubernetes_pod_container_port_name", "mainport"),
                    ("__meta_kubernetes_pod_container_port_number", "9000"),
                    ("__meta_kubernetes_pod_container_port_protocol", "TCP"),
                    ("__meta_kubernetes_pod_uid", "deadbeef"),
                    ("__meta_kubernetes_pod_container_init", "false"),
                ]),
                ls(&[
                    ("__address__", "1.2.3.4:9001"),
                    ("__meta_kubernetes_pod_name", "testpod"),
                    ("__meta_kubernetes_pod_ip", "1.2.3.4"),
                    ("__meta_kubernetes_pod_ready", "unknown"),
                    ("__meta_kubernetes_pod_phase", ""),
                    ("__meta_kubernetes_pod_node_name", "testnode"),
                    ("__meta_kubernetes_pod_host_ip", "2.3.4.5"),
                    ("__meta_kubernetes_pod_container_name", "c2"),
                    ("__meta_kubernetes_pod_container_image", "c2:latest"),
                    ("__meta_kubernetes_pod_container_port_name", "sideport"),
                    ("__meta_kubernetes_pod_container_port_number", "9001"),
                    ("__meta_kubernetes_pod_container_port_protocol", "TCP"),
                    ("__meta_kubernetes_pod_uid", "deadbeef"),
                    ("__meta_kubernetes_pod_container_init", "false"),
                ]),
            ],
            ls(&[("__meta_kubernetes_endpoints_name", "testendpoints"), ("__meta_kubernetes_namespace", "default")]),
            "endpoints/default/testendpoints",
        )]))
        .run();
}

#[test]
fn test_endpoints_discovery_delete() {
    let (n, c) = make_discovery(Role::ENDPOINT, NamespaceDiscovery::default(), vec![Object::Endpoints(make_endpoints("default"))]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            let obj = make_endpoints("default");
            let _ = c.delete(Kind::Endpoints, &obj.meta.namespace, &obj.meta.name);
        })
        .expected_max_items(2)
        .expected_res(expect(vec![empty_group("endpoints/default/testendpoints")]))
        .run();
}

#[test]
fn test_endpoints_discovery_update() {
    let (n, c) = make_discovery(Role::ENDPOINT, NamespaceDiscovery::default(), vec![Object::Endpoints(make_endpoints("default"))]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            let obj = Endpoints {
                meta: meta("testendpoints", "default"),
                subsets: vec![
                    EndpointSubset { addresses: vec![addr("1.2.3.4")], ports: vec![ep_port("testport", 9000)], ..Default::default() },
                    EndpointSubset { addresses: vec![addr("2.3.4.5")], ports: vec![ep_port("testport", 9001)], ..Default::default() },
                ],
            };
            let _ = c.update("default", Object::Endpoints(obj));
        })
        .expected_max_items(2)
        .expected_res(expect(vec![group(
            vec![
                ls(&[("__address__", "1.2.3.4:9000"), ("__meta_kubernetes_endpoint_port_name", "testport"), ("__meta_kubernetes_endpoint_port_protocol", "TCP"), ("__meta_kubernetes_endpoint_ready", "true")]),
                ls(&[("__address__", "2.3.4.5:9001"), ("__meta_kubernetes_endpoint_port_name", "testport"), ("__meta_kubernetes_endpoint_port_protocol", "TCP"), ("__meta_kubernetes_endpoint_ready", "true")]),
            ],
            ls(&[("__meta_kubernetes_namespace", "default"), ("__meta_kubernetes_endpoints_name", "testendpoints")]),
            "endpoints/default/testendpoints",
        )]))
        .run();
}

#[test]
fn test_endpoints_discovery_empty_subsets() {
    let (n, c) = make_discovery(Role::ENDPOINT, NamespaceDiscovery::default(), vec![Object::Endpoints(make_endpoints("default"))]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            let obj = Endpoints { meta: meta("testendpoints", "default"), subsets: vec![] };
            let _ = c.update("default", Object::Endpoints(obj));
        })
        .expected_max_items(2)
        .expected_res(expect(vec![group(
            vec![],
            ls(&[("__meta_kubernetes_namespace", "default"), ("__meta_kubernetes_endpoints_name", "testendpoints")]),
            "endpoints/default/testendpoints",
        )]))
        .run();
}

#[test]
fn test_endpoints_discovery_with_service() {
    let (n, c) = make_discovery(Role::ENDPOINT, NamespaceDiscovery::default(), vec![Object::Endpoints(make_endpoints("default"))]);
    K8sDiscoveryTest::new(n)
        .before_run(move || {
            let _ = c.create("default", Object::Service(test_service("default", &[("app/name", "test")])));
        })
        .expected_max_items(1)
        .expected_res(expect(vec![group(
            make_endpoints_targets(&[], &[]),
            ls(&[
                ("__meta_kubernetes_namespace", "default"),
                ("__meta_kubernetes_endpoints_name", "testendpoints"),
                ("__meta_kubernetes_service_label_app_name", "test"),
                ("__meta_kubernetes_service_labelpresent_app_name", "true"),
                ("__meta_kubernetes_service_name", "testendpoints"),
                ("__meta_kubernetes_endpoints_annotation_test_annotation", "test"),
                ("__meta_kubernetes_endpoints_annotationpresent_test_annotation", "true"),
            ]),
            "endpoints/default/testendpoints",
        )]))
        .run();
}

#[test]
fn test_endpoints_discovery_with_service_update() {
    let (n, c) = make_discovery(Role::ENDPOINT, NamespaceDiscovery::default(), vec![Object::Endpoints(make_endpoints("default"))]);
    let c2 = c.clone();
    K8sDiscoveryTest::new(n)
        .before_run(move || {
            let _ = c.create("default", Object::Service(test_service("default", &[("app/name", "test")])));
        })
        .after_start(move || {
            let _ = c2.update("default", Object::Service(test_service("default", &[("app/name", "svc"), ("component", "testing")])));
        })
        .expected_max_items(2)
        .expected_res(expect(vec![group(
            make_endpoints_targets(&[], &[]),
            ls(&[
                ("__meta_kubernetes_namespace", "default"),
                ("__meta_kubernetes_endpoints_name", "testendpoints"),
                ("__meta_kubernetes_service_label_app_name", "svc"),
                ("__meta_kubernetes_service_labelpresent_app_name", "true"),
                ("__meta_kubernetes_service_name", "testendpoints"),
                ("__meta_kubernetes_service_label_component", "testing"),
                ("__meta_kubernetes_service_labelpresent_component", "true"),
                ("__meta_kubernetes_endpoints_annotation_test_annotation", "test"),
                ("__meta_kubernetes_endpoints_annotationpresent_test_annotation", "true"),
            ]),
            "endpoints/default/testendpoints",
        )]))
        .run();
}

fn node_metadata_labels() -> LabelSet {
    ls(&[
        ("__meta_kubernetes_namespace", "default"),
        ("__meta_kubernetes_endpoints_name", "testendpoints"),
        ("__meta_kubernetes_service_label_app_name", "test"),
        ("__meta_kubernetes_service_labelpresent_app_name", "true"),
        ("__meta_kubernetes_service_name", "testendpoints"),
        ("__meta_kubernetes_endpoints_annotation_test_annotation", "test"),
        ("__meta_kubernetes_endpoints_annotationpresent_test_annotation", "true"),
    ])
}

const NODE1_LABELS: &[(&str, &str)] = &[("__meta_kubernetes_node_label_az", "us-east1"), ("__meta_kubernetes_node_labelpresent_az", "true"), ("__meta_kubernetes_node_name", "foobar")];
const NODE2_LABELS: &[(&str, &str)] = &[("__meta_kubernetes_node_label_az", "us-west2"), ("__meta_kubernetes_node_labelpresent_az", "true"), ("__meta_kubernetes_node_name", "barbaz")];

#[test]
fn test_endpoints_discovery_with_node_metadata() {
    let metadata_config = AttachMetadataConfig { node: true, ..Default::default() };
    let node1 = make_node("foobar", "", "", smap(&[("az", "us-east1")]), StrMap::new(), vec![]);
    let node2 = make_node("barbaz", "", "", smap(&[("az", "us-west2")]), StrMap::new(), vec![]);
    let svc = test_service("default", &[("app/name", "test")]);
    let (n, _c) = make_discovery_with_metadata(
        Role::ENDPOINT,
        NamespaceDiscovery::default(),
        metadata_config,
        vec![Object::Endpoints(make_endpoints("default")), Object::Service(svc), Object::Node(node1), Object::Node(node2)],
    );
    K8sDiscoveryTest::new(n)
        .expected_max_items(1)
        .expected_res(expect(vec![group(make_endpoints_targets(NODE1_LABELS, NODE2_LABELS), node_metadata_labels(), "endpoints/default/testendpoints")]))
        .run();
}

#[test]
fn test_endpoints_discovery_with_updated_node_metadata() {
    let node1 = make_node("foobar", "", "", smap(&[("az", "us-east1")]), StrMap::new(), vec![]);
    let node2 = make_node("barbaz", "", "", smap(&[("az", "us-west2")]), StrMap::new(), vec![]);
    let metadata_config = AttachMetadataConfig { node: true, ..Default::default() };
    let svc = test_service("default", &[("app/name", "test")]);
    let (n, c) = make_discovery_with_metadata(
        Role::ENDPOINT,
        NamespaceDiscovery::default(),
        metadata_config,
        vec![Object::Endpoints(make_endpoints("default")), Object::Node(node1.clone()), Object::Node(node2), Object::Service(svc)],
    );
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            let mut node1 = node1;
            node1.meta.labels = smap(&[("az", "eu-central1")]);
            let _ = c.update("", Object::Node(node1));
        })
        .expected_max_items(2)
        .expected_res(expect(vec![group(make_endpoints_targets(NODE1_LABELS, NODE2_LABELS), node_metadata_labels(), "endpoints/default/testendpoints")]))
        .run();
}

#[test]
fn test_endpoints_discovery_namespaces() {
    let mut ep_one = make_endpoints("default");
    ep_one.meta.namespace = s("ns1");
    let objs = vec![
        Object::Endpoints(ep_one),
        Object::Endpoints(Endpoints {
            meta: meta("testendpoints", "ns2"),
            subsets: vec![EndpointSubset { addresses: vec![pod_ref_addr("4.3.2.1", "testpod", "ns2")], ports: vec![ep_port("testport", 9000)], ..Default::default() }],
        }),
        Object::Service(test_service("ns1", &[("app", "app1")])),
        Object::Pod(Pod {
            meta: ObjectMeta { name: s("testpod"), namespace: s("ns2"), uid: s("deadbeef"), ..Default::default() },
            spec: PodSpec { node_name: s("testnode"), containers: vec![container("c1", "c1:latest", "mainport", 9000)], ..Default::default() },
            status: PodStatus { host_ip: s("2.3.4.5"), pod_ip: s("4.3.2.1"), ..Default::default() },
        }),
    ];
    let (n, _c) = make_discovery(Role::ENDPOINT, NamespaceDiscovery { names: vec![s("ns1"), s("ns2")], ..Default::default() }, objs);
    K8sDiscoveryTest::new(n)
        .expected_max_items(2)
        .expected_res(expect(vec![
            group(
                make_endpoints_targets(&[], &[]),
                ls(&[
                    ("__meta_kubernetes_namespace", "ns1"),
                    ("__meta_kubernetes_endpoints_name", "testendpoints"),
                    ("__meta_kubernetes_endpoints_annotation_test_annotation", "test"),
                    ("__meta_kubernetes_endpoints_annotationpresent_test_annotation", "true"),
                    ("__meta_kubernetes_service_label_app", "app1"),
                    ("__meta_kubernetes_service_labelpresent_app", "true"),
                    ("__meta_kubernetes_service_name", "testendpoints"),
                ]),
                "endpoints/ns1/testendpoints",
            ),
            group(
                vec![ls(&[
                    ("__address__", "4.3.2.1:9000"),
                    ("__meta_kubernetes_endpoint_port_name", "testport"),
                    ("__meta_kubernetes_endpoint_port_protocol", "TCP"),
                    ("__meta_kubernetes_endpoint_ready", "true"),
                    ("__meta_kubernetes_endpoint_address_target_kind", "Pod"),
                    ("__meta_kubernetes_endpoint_address_target_name", "testpod"),
                    ("__meta_kubernetes_pod_name", "testpod"),
                    ("__meta_kubernetes_pod_ip", "4.3.2.1"),
                    ("__meta_kubernetes_pod_ready", "unknown"),
                    ("__meta_kubernetes_pod_phase", ""),
                    ("__meta_kubernetes_pod_node_name", "testnode"),
                    ("__meta_kubernetes_pod_host_ip", "2.3.4.5"),
                    ("__meta_kubernetes_pod_container_name", "c1"),
                    ("__meta_kubernetes_pod_container_image", "c1:latest"),
                    ("__meta_kubernetes_pod_container_port_name", "mainport"),
                    ("__meta_kubernetes_pod_container_port_number", "9000"),
                    ("__meta_kubernetes_pod_container_port_protocol", "TCP"),
                    ("__meta_kubernetes_pod_uid", "deadbeef"),
                    ("__meta_kubernetes_pod_container_init", "false"),
                ])],
                ls(&[("__meta_kubernetes_namespace", "ns2"), ("__meta_kubernetes_endpoints_name", "testendpoints")]),
                "endpoints/ns2/testendpoints",
            ),
        ]))
        .run();
}

fn p1_pod(ns: &str, status: PodStatus) -> Pod {
    Pod {
        meta: ObjectMeta { name: s("testpod"), namespace: s(ns), uid: s("deadbeef"), ..Default::default() },
        spec: PodSpec { node_name: s("testnode"), containers: vec![container("p1", "p1:latest", "mainport", 9000)], ..Default::default() },
        status,
    }
}

#[test]
fn test_endpoints_discovery_own_namespace() {
    let mut ep_one = make_endpoints("default");
    ep_one.meta.namespace = s("own-ns");
    let mut ep_two = make_endpoints("default");
    ep_two.meta.namespace = s("non-own-ns");
    let st = PodStatus { host_ip: s("2.3.4.5"), pod_ip: s("4.3.2.1"), ..Default::default() };
    let pod_one = p1_pod("own-ns", st.clone());
    let mut pod_two = pod_one.clone();
    pod_two.meta.namespace = s("non-own-ns");
    let objs = vec![Object::Endpoints(ep_one), Object::Endpoints(ep_two), Object::Pod(pod_one), Object::Pod(pod_two)];
    let (n, _c) = make_discovery(Role::ENDPOINT, NamespaceDiscovery { include_own_namespace: true, ..Default::default() }, objs);
    K8sDiscoveryTest::new(n)
        .expected_max_items(1)
        .expected_res(expect(vec![group(
            make_endpoints_targets(&[], &[]),
            ls(&[
                ("__meta_kubernetes_namespace", "own-ns"),
                ("__meta_kubernetes_endpoints_name", "testendpoints"),
                ("__meta_kubernetes_endpoints_annotation_test_annotation", "test"),
                ("__meta_kubernetes_endpoints_annotationpresent_test_annotation", "true"),
            ]),
            "endpoints/own-ns/testendpoints",
        )]))
        .run();
}

#[test]
fn test_endpoints_discovery_empty_pod_status() {
    let mut ep = make_endpoints("default");
    ep.meta.namespace = s("ns");
    let pod = p1_pod("ns", PodStatus::default());
    let objs = vec![Object::Endpoints(ep), Object::Pod(pod)];
    let (n, _c) = make_discovery(Role::ENDPOINT, NamespaceDiscovery { include_own_namespace: true, ..Default::default() }, objs);
    K8sDiscoveryTest::new(n).expected_max_items(0).expected_res(expect(vec![])).run();
}

/// Makes sure that Endpoints discovery detects underlying Pods changes.
/// See https://github.com/prometheus/prometheus/issues/11305 for more details.
#[test]
fn test_endpoints_discovery_update_pod() {
    let mut pod = Pod {
        meta: ObjectMeta { name: s("testpod"), namespace: s("default"), uid: s("deadbeef"), ..Default::default() },
        spec: PodSpec { node_name: s("testnode"), containers: vec![container("c1", "c1:latest", "mainport", 9000)], ..Default::default() },
        status: PodStatus {
            // Pod is in Pending phase when discovered for first time.
            phase: s("Pending"),
            conditions: vec![Condition { type_: s("Ready"), status: s("False") }],
            host_ip: s("2.3.4.5"),
            pod_ip: s("4.3.2.1"),
            ..Default::default()
        },
    };
    let objs = vec![
        Object::Endpoints(Endpoints {
            meta: meta("testendpoints", "default"),
            subsets: vec![EndpointSubset {
                // The Pending Pod may be included because the Endpoints was created manually.
                // Or because the corresponding service has ".spec.publishNotReadyAddresses: true".
                addresses: vec![pod_ref_addr("4.3.2.1", "testpod", "default")],
                ports: vec![ep_port("mainport", 9000)],
                ..Default::default()
            }],
        }),
        Object::Pod(pod.clone()),
    ];
    let (n, c) = make_discovery(Role::ENDPOINT, NamespaceDiscovery::default(), objs);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            // the Pod becomes Ready.
            pod.status.phase = s("Running");
            pod.status.conditions = vec![Condition { type_: s("Ready"), status: s("True") }];
            let ns = pod.meta.namespace.clone();
            let _ = c.update(&ns, Object::Pod(pod));
        })
        .expected_max_items(2)
        .expected_res(expect(vec![group(
            vec![ls(&[
                ("__address__", "4.3.2.1:9000"),
                ("__meta_kubernetes_endpoint_port_name", "mainport"),
                ("__meta_kubernetes_endpoint_port_protocol", "TCP"),
                ("__meta_kubernetes_endpoint_ready", "true"),
                ("__meta_kubernetes_endpoint_address_target_kind", "Pod"),
                ("__meta_kubernetes_endpoint_address_target_name", "testpod"),
                ("__meta_kubernetes_pod_name", "testpod"),
                ("__meta_kubernetes_pod_ip", "4.3.2.1"),
                ("__meta_kubernetes_pod_ready", "true"),
                ("__meta_kubernetes_pod_phase", "Running"),
                ("__meta_kubernetes_pod_node_name", "testnode"),
                ("__meta_kubernetes_pod_host_ip", "2.3.4.5"),
                ("__meta_kubernetes_pod_container_name", "c1"),
                ("__meta_kubernetes_pod_container_image", "c1:latest"),
                ("__meta_kubernetes_pod_container_port_name", "mainport"),
                ("__meta_kubernetes_pod_container_port_number", "9000"),
                ("__meta_kubernetes_pod_container_port_protocol", "TCP"),
                ("__meta_kubernetes_pod_uid", "deadbeef"),
                ("__meta_kubernetes_pod_container_init", "false"),
            ])],
            ls(&[("__meta_kubernetes_namespace", "default"), ("__meta_kubernetes_endpoints_name", "testendpoints")]),
            "endpoints/default/testendpoints",
        )]))
        .run();
}

#[test]
fn test_endpoints_discovery_sidecar_container() {
    let objs = vec![
        Object::Endpoints(Endpoints {
            meta: meta("testsidecar", "default"),
            subsets: vec![EndpointSubset {
                addresses: vec![pod_ref_addr("4.3.2.1", "testpod", "default")],
                ports: vec![ep_port("testport", 9000), ep_port("initport", 9111)],
                ..Default::default()
            }],
        }),
        Object::Pod(Pod {
            meta: ObjectMeta { name: s("testpod"), namespace: s("default"), uid: s("deadbeef"), ..Default::default() },
            spec: PodSpec {
                node_name: s("testnode"),
                init_containers: vec![container("ic1", "ic1:latest", "initport", 1111), container("ic2", "ic2:latest", "initport", 9111)],
                containers: vec![container("c1", "c1:latest", "mainport", 9000)],
            },
            status: PodStatus { host_ip: s("2.3.4.5"), pod_ip: s("4.3.2.1"), ..Default::default() },
        }),
    ];
    let (n, _c) = make_discovery(Role::ENDPOINT, NamespaceDiscovery::default(), objs);
    K8sDiscoveryTest::new(n)
        .expected_max_items(1)
        .expected_res(expect(vec![group(
            vec![
                ls(&[
                    ("__address__", "4.3.2.1:9000"),
                    ("__meta_kubernetes_endpoint_address_target_kind", "Pod"),
                    ("__meta_kubernetes_endpoint_address_target_name", "testpod"),
                    ("__meta_kubernetes_endpoint_port_name", "testport"),
                    ("__meta_kubernetes_endpoint_port_protocol", "TCP"),
                    ("__meta_kubernetes_endpoint_ready", "true"),
                    ("__meta_kubernetes_pod_container_image", "c1:latest"),
                    ("__meta_kubernetes_pod_container_name", "c1"),
                    ("__meta_kubernetes_pod_container_port_name", "mainport"),
                    ("__meta_kubernetes_pod_container_port_number", "9000"),
                    ("__meta_kubernetes_pod_container_port_protocol", "TCP"),
                    ("__meta_kubernetes_pod_host_ip", "2.3.4.5"),
                    ("__meta_kubernetes_pod_ip", "4.3.2.1"),
                    ("__meta_kubernetes_pod_name", "testpod"),
                    ("__meta_kubernetes_pod_node_name", "testnode"),
                    ("__meta_kubernetes_pod_phase", ""),
                    ("__meta_kubernetes_pod_ready", "unknown"),
                    ("__meta_kubernetes_pod_uid", "deadbeef"),
                    ("__meta_kubernetes_pod_container_init", "false"),
                ]),
                ls(&[
                    ("__address__", "4.3.2.1:9111"),
                    ("__meta_kubernetes_endpoint_address_target_kind", "Pod"),
                    ("__meta_kubernetes_endpoint_address_target_name", "testpod"),
                    ("__meta_kubernetes_endpoint_port_name", "initport"),
                    ("__meta_kubernetes_endpoint_port_protocol", "TCP"),
                    ("__meta_kubernetes_endpoint_ready", "true"),
                    ("__meta_kubernetes_pod_container_image", "ic2:latest"),
                    ("__meta_kubernetes_pod_container_name", "ic2"),
                    ("__meta_kubernetes_pod_container_port_name", "initport"),
                    ("__meta_kubernetes_pod_container_port_number", "9111"),
                    ("__meta_kubernetes_pod_container_port_protocol", "TCP"),
                    ("__meta_kubernetes_pod_host_ip", "2.3.4.5"),
                    ("__meta_kubernetes_pod_ip", "4.3.2.1"),
                    ("__meta_kubernetes_pod_name", "testpod"),
                    ("__meta_kubernetes_pod_node_name", "testnode"),
                    ("__meta_kubernetes_pod_phase", ""),
                    ("__meta_kubernetes_pod_ready", "unknown"),
                    ("__meta_kubernetes_pod_uid", "deadbeef"),
                    ("__meta_kubernetes_pod_container_init", "true"),
                ]),
                ls(&[
                    ("__address__", "4.3.2.1:1111"),
                    ("__meta_kubernetes_pod_container_image", "ic1:latest"),
                    ("__meta_kubernetes_pod_container_name", "ic1"),
                    ("__meta_kubernetes_pod_container_port_name", "initport"),
                    ("__meta_kubernetes_pod_container_port_number", "1111"),
                    ("__meta_kubernetes_pod_container_port_protocol", "TCP"),
                    ("__meta_kubernetes_pod_host_ip", "2.3.4.5"),
                    ("__meta_kubernetes_pod_ip", "4.3.2.1"),
                    ("__meta_kubernetes_pod_name", "testpod"),
                    ("__meta_kubernetes_pod_node_name", "testnode"),
                    ("__meta_kubernetes_pod_phase", ""),
                    ("__meta_kubernetes_pod_ready", "unknown"),
                    ("__meta_kubernetes_pod_uid", "deadbeef"),
                    ("__meta_kubernetes_pod_container_init", "true"),
                ]),
            ],
            ls(&[("__meta_kubernetes_endpoints_name", "testsidecar"), ("__meta_kubernetes_namespace", "default")]),
            "endpoints/default/testsidecar",
        )]))
        .run();
}

#[test]
fn test_endpoints_discovery_with_namespace_metadata() {
    let ns = "test-ns";
    let ns_labels = smap(&[("environment", "production"), ("team", "backend")]);
    let ns_annotations = smap(&[("owner", "platform"), ("version", "v1.2.3")]);
    let (n, _c) = make_discovery_with_metadata(
        Role::ENDPOINT,
        NamespaceDiscovery::default(),
        AttachMetadataConfig { namespace: true, ..Default::default() },
        vec![Object::Namespace(make_namespace(ns, ns_labels, ns_annotations)), Object::Endpoints(make_endpoints(ns))],
    );
    let source = format!("endpoints/{ns}/testendpoints");
    K8sDiscoveryTest::new(n)
        .expected_max_items(1)
        .expected_res(expect(vec![group(
            make_endpoints_targets(&[], &[]),
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
                ("__meta_kubernetes_endpoints_name", "testendpoints"),
                ("__meta_kubernetes_endpoints_annotation_test_annotation", "test"),
                ("__meta_kubernetes_endpoints_annotationpresent_test_annotation", "true"),
            ]),
            &source,
        )]))
        .run();
}

#[test]
fn test_endpoints_discovery_with_updated_namespace_metadata() {
    let ns = "test-ns";
    let ns_labels = smap(&[("environment", "development"), ("team", "frontend")]);
    let ns_annotations = smap(&[("owner", "devops"), ("version", "v2.1.0")]);
    let namespace = make_namespace(ns, ns_labels, ns_annotations);
    let (n, c) = make_discovery_with_metadata(
        Role::ENDPOINT,
        NamespaceDiscovery::default(),
        AttachMetadataConfig { namespace: true, ..Default::default() },
        vec![Object::Namespace(namespace.clone()), Object::Endpoints(make_endpoints(ns))],
    );
    let source = format!("endpoints/{ns}/testendpoints");
    K8sDiscoveryTest::new(n)
        .expected_max_items(2)
        .after_start(move || {
            // Go mutates the namespace's maps in place.
            let mut namespace = namespace;
            namespace.labels = smap(&[("environment", "staging"), ("team", "frontend"), ("region", "us-west")]);
            namespace.annotations = smap(&[("owner", "sre"), ("version", "v2.1.0"), ("cost-center", "engineering")]);
            let _ = c.update("", Object::Namespace(namespace));
        })
        .expected_res(expect(vec![group(
            make_endpoints_targets(&[], &[]),
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
                ("__meta_kubernetes_endpoints_name", "testendpoints"),
                ("__meta_kubernetes_endpoints_annotation_test_annotation", "test"),
                ("__meta_kubernetes_endpoints_annotationpresent_test_annotation", "true"),
            ]),
            &source,
        )]))
        .run();
}
