//! Port of discovery/kubernetes/endpointslice_test.go.
mod k8s_common;

use discovery::kubernetes::api::{
    Container, ContainerPort, Endpoint, EndpointConditions, EndpointSlice as ApiEndpointSlice, EndpointSlicePort, Kind, Node, NodeAddress, Object,
    ObjectMeta, ObjectReference, Pod, PodSpec, PodStatus, Service, ServiceSpec, StrMap,
};
use discovery::kubernetes::endpointslice::EndpointSlice;
use discovery::kubernetes::{AttachMetadataConfig, NamespaceDiscovery, Role};
use k8s_common::*;
use model::labelset::LabelSet;

type Pairs = &'static [(&'static str, &'static str)];

fn s(v: &str) -> Option<String> {
    Some(v.to_string())
}

fn names(n: &[&str]) -> NamespaceDiscovery {
    NamespaceDiscovery { names: n.iter().map(|s| s.to_string()).collect(), ..Default::default() }
}

/// Go `makeEndpointSliceV1`.
fn make_endpoint_slice_v1(namespace: &str) -> ApiEndpointSlice {
    ApiEndpointSlice {
        meta: ObjectMeta {
            name: "testendpoints".to_string(),
            namespace: namespace.to_string(),
            labels: smap(&[("kubernetes.io/service-name", "testendpoints")]),
            annotations: smap(&[("test.annotation", "test")]),
            ..Default::default()
        },
        address_type: "IPv4".to_string(),
        ports: vec![EndpointSlicePort { name: s("testport"), port: Some(9000), protocol: s("TCP"), app_protocol: s("http") }],
        endpoints: vec![
            Endpoint {
                addresses: vec!["1.2.3.4".to_string()],
                conditions: EndpointConditions { ready: Some(true), serving: Some(true), terminating: Some(false) },
                hostname: s("testendpoint1"),
                target_ref: Some(ObjectReference::default()),
                node_name: s("foobar"),
                deprecated_topology: smap(&[("topology", "value")]),
                zone: s("us-east-1a"),
            },
            Endpoint {
                addresses: vec!["2.3.4.5".to_string()],
                conditions: EndpointConditions { ready: Some(true), serving: Some(true), terminating: Some(false) },
                zone: s("us-east-1b"),
                ..Default::default()
            },
            Endpoint {
                addresses: vec!["3.4.5.6".to_string()],
                conditions: EndpointConditions { ready: Some(false), serving: Some(true), terminating: Some(true) },
                zone: s("us-east-1c"),
                ..Default::default()
            },
            Endpoint {
                addresses: vec!["4.5.6.7".to_string()],
                conditions: EndpointConditions { ready: Some(true), serving: Some(true), terminating: Some(false) },
                target_ref: Some(ObjectReference { kind: "Node".to_string(), name: "barbaz".to_string(), ..Default::default() }),
                zone: s("us-east-1a"),
                ..Default::default()
            },
        ],
    }
}

/// Go `makeNamespace`.
fn make_namespace(name: &str, labels: StrMap, annotations: StrMap) -> Object {
    Object::Namespace(ObjectMeta { name: name.to_string(), labels, annotations, ..Default::default() })
}

/// Go `makeNode` (copied from node_test.go).
fn make_node(name: &str, address: &str, provider_id: &str, labels: StrMap, annotations: StrMap) -> Node {
    Node {
        meta: ObjectMeta { name: name.to_string(), labels, annotations, ..Default::default() },
        provider_id: provider_id.to_string(),
        addresses: vec![NodeAddress { type_: "InternalIP".to_string(), address: address.to_string() }],
        kubelet_port: 10250,
        conditions: Vec::new(),
    }
}

fn container(name: &str, image: &str, port_name: &str, port: i32) -> Container {
    Container {
        name: name.to_string(),
        image: image.to_string(),
        ports: vec![ContainerPort { name: port_name.to_string(), container_port: port, protocol: "TCP".to_string() }],
    }
}

fn pod(namespace: &str, containers: Vec<Container>, init_containers: Vec<Container>, host_ip: &str, pod_ip: &str) -> Pod {
    Pod {
        meta: ObjectMeta { name: "testpod".to_string(), namespace: namespace.to_string(), uid: "deadbeef".to_string(), ..Default::default() },
        spec: PodSpec { node_name: "testnode".to_string(), containers, init_containers },
        status: PodStatus { host_ip: host_ip.to_string(), pod_ip: pod_ip.to_string(), ..Default::default() },
    }
}

fn service(name: &str, namespace: &str, labels: StrMap) -> Object {
    Object::Service(Service { meta: ObjectMeta { name: name.to_string(), namespace: namespace.to_string(), labels, ..Default::default() }, ..Default::default() })
}

fn pod_ref(namespace: &str) -> Option<ObjectReference> {
    Some(ObjectReference { kind: "Pod".to_string(), name: "testpod".to_string(), namespace: namespace.to_string() })
}

/// A label set from a base and extra pairs.
fn lse(base: Pairs, extra: &[(&str, &str)]) -> LabelSet {
    let mut v: Vec<(&str, &str)> = base.to_vec();
    v.extend_from_slice(extra);
    ls(&v)
}

// The targets of makeEndpointSliceV1, as they are written in every test of the Go file.
const T1: Pairs = &[
    ("__address__", "1.2.3.4:9000"),
    ("__meta_kubernetes_endpointslice_address_target_kind", ""),
    ("__meta_kubernetes_endpointslice_address_target_name", ""),
    ("__meta_kubernetes_endpointslice_endpoint_conditions_ready", "true"),
    ("__meta_kubernetes_endpointslice_endpoint_conditions_serving", "true"),
    ("__meta_kubernetes_endpointslice_endpoint_conditions_terminating", "false"),
    ("__meta_kubernetes_endpointslice_endpoint_hostname", "testendpoint1"),
    ("__meta_kubernetes_endpointslice_endpoint_node_name", "foobar"),
    ("__meta_kubernetes_endpointslice_endpoint_topology_present_topology", "true"),
    ("__meta_kubernetes_endpointslice_endpoint_topology_topology", "value"),
    ("__meta_kubernetes_endpointslice_endpoint_zone", "us-east-1a"),
    ("__meta_kubernetes_endpointslice_port", "9000"),
    ("__meta_kubernetes_endpointslice_port_app_protocol", "http"),
    ("__meta_kubernetes_endpointslice_port_name", "testport"),
    ("__meta_kubernetes_endpointslice_port_protocol", "TCP"),
];
const T2: Pairs = &[
    ("__address__", "2.3.4.5:9000"),
    ("__meta_kubernetes_endpointslice_endpoint_conditions_ready", "true"),
    ("__meta_kubernetes_endpointslice_endpoint_conditions_serving", "true"),
    ("__meta_kubernetes_endpointslice_endpoint_conditions_terminating", "false"),
    ("__meta_kubernetes_endpointslice_endpoint_zone", "us-east-1b"),
    ("__meta_kubernetes_endpointslice_port", "9000"),
    ("__meta_kubernetes_endpointslice_port_app_protocol", "http"),
    ("__meta_kubernetes_endpointslice_port_name", "testport"),
    ("__meta_kubernetes_endpointslice_port_protocol", "TCP"),
];
const T3: Pairs = &[
    ("__address__", "3.4.5.6:9000"),
    ("__meta_kubernetes_endpointslice_endpoint_conditions_ready", "false"),
    ("__meta_kubernetes_endpointslice_endpoint_conditions_serving", "true"),
    ("__meta_kubernetes_endpointslice_endpoint_conditions_terminating", "true"),
    ("__meta_kubernetes_endpointslice_endpoint_zone", "us-east-1c"),
    ("__meta_kubernetes_endpointslice_port", "9000"),
    ("__meta_kubernetes_endpointslice_port_app_protocol", "http"),
    ("__meta_kubernetes_endpointslice_port_name", "testport"),
    ("__meta_kubernetes_endpointslice_port_protocol", "TCP"),
];
const T4: Pairs = &[
    ("__address__", "4.5.6.7:9000"),
    ("__meta_kubernetes_endpointslice_address_target_kind", "Node"),
    ("__meta_kubernetes_endpointslice_address_target_name", "barbaz"),
    ("__meta_kubernetes_endpointslice_endpoint_conditions_ready", "true"),
    ("__meta_kubernetes_endpointslice_endpoint_conditions_serving", "true"),
    ("__meta_kubernetes_endpointslice_endpoint_conditions_terminating", "false"),
    ("__meta_kubernetes_endpointslice_endpoint_zone", "us-east-1a"),
    ("__meta_kubernetes_endpointslice_port", "9000"),
    ("__meta_kubernetes_endpointslice_port_app_protocol", "http"),
    ("__meta_kubernetes_endpointslice_port_name", "testport"),
    ("__meta_kubernetes_endpointslice_port_protocol", "TCP"),
];

fn four_targets() -> Vec<LabelSet> {
    vec![lse(T1, &[]), lse(T2, &[]), lse(T3, &[]), lse(T4, &[])]
}

// The group labels of makeEndpointSliceV1 (without the namespace label).
const SLICE_LABELS: Pairs = &[
    ("__meta_kubernetes_endpointslice_address_type", "IPv4"),
    ("__meta_kubernetes_endpointslice_name", "testendpoints"),
    ("__meta_kubernetes_endpointslice_label_kubernetes_io_service_name", "testendpoints"),
    ("__meta_kubernetes_endpointslice_labelpresent_kubernetes_io_service_name", "true"),
    ("__meta_kubernetes_endpointslice_annotation_test_annotation", "test"),
    ("__meta_kubernetes_endpointslice_annotationpresent_test_annotation", "true"),
];

const SVC_APP_NAME_TEST: Pairs = &[
    ("__meta_kubernetes_service_label_app_name", "test"),
    ("__meta_kubernetes_service_labelpresent_app_name", "true"),
    ("__meta_kubernetes_service_name", "testendpoints"),
];

fn slice_labels(ns: &str, extra: &[(&str, &str)]) -> LabelSet {
    let mut v = vec![("__meta_kubernetes_namespace", ns)];
    v.extend_from_slice(extra);
    lse(SLICE_LABELS, &v)
}

fn plain_slice_labels(ns: &str, name: &str) -> LabelSet {
    ls(&[("__meta_kubernetes_endpointslice_address_type", "IPv4"), ("__meta_kubernetes_endpointslice_name", name), ("__meta_kubernetes_namespace", ns)])
}

#[test]
fn test_endpoint_slice_discovery_before_run() {
    let (n, c) = make_discovery(Role::ENDPOINT_SLICE, names(&["default"]), vec![]);
    K8sDiscoveryTest::new(n)
        .before_run(move || {
            let obj = make_endpoint_slice_v1("default");
            let ns = obj.meta.namespace.clone();
            let _ = c.create(&ns, Object::EndpointSlice(obj));
        })
        .expected_max_items(1)
        .expected_res(expect(vec![group(four_targets(), slice_labels("default", &[]), "endpointslice/default/testendpoints")]))
        .run();
}

#[test]
fn test_endpoint_slice_discovery_add() {
    let obj = pod("default", vec![container("c1", "c1:latest", "mainport", 9000), container("c2", "c2:latest", "sideport", 9001)], vec![], "2.3.4.5", "1.2.3.4");
    let (n, c) = make_discovery(Role::ENDPOINT_SLICE, names(&["default"]), vec![Object::Pod(obj)]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            let obj = ApiEndpointSlice {
                meta: ObjectMeta { name: "testendpoints".to_string(), namespace: "default".to_string(), ..Default::default() },
                address_type: "IPv4".to_string(),
                ports: vec![EndpointSlicePort { name: s("testport"), port: Some(9000), protocol: s("TCP"), ..Default::default() }],
                endpoints: vec![Endpoint {
                    addresses: vec!["4.3.2.1".to_string()],
                    target_ref: pod_ref("default"),
                    conditions: EndpointConditions { ready: Some(false), ..Default::default() },
                    ..Default::default()
                }],
            };
            let _ = c.create("default", Object::EndpointSlice(obj));
        })
        .expected_max_items(1)
        .expected_res(expect(vec![group(
            vec![
                ls(&[
                    ("__address__", "4.3.2.1:9000"),
                    ("__meta_kubernetes_endpointslice_address_target_kind", "Pod"),
                    ("__meta_kubernetes_endpointslice_address_target_name", "testpod"),
                    ("__meta_kubernetes_endpointslice_endpoint_conditions_ready", "false"),
                    ("__meta_kubernetes_endpointslice_port", "9000"),
                    ("__meta_kubernetes_endpointslice_port_name", "testport"),
                    ("__meta_kubernetes_endpointslice_port_protocol", "TCP"),
                    ("__meta_kubernetes_pod_container_name", "c1"),
                    ("__meta_kubernetes_pod_container_image", "c1:latest"),
                    ("__meta_kubernetes_pod_container_port_name", "mainport"),
                    ("__meta_kubernetes_pod_container_port_number", "9000"),
                    ("__meta_kubernetes_pod_container_port_protocol", "TCP"),
                    ("__meta_kubernetes_pod_host_ip", "2.3.4.5"),
                    ("__meta_kubernetes_pod_ip", "1.2.3.4"),
                    ("__meta_kubernetes_pod_name", "testpod"),
                    ("__meta_kubernetes_pod_node_name", "testnode"),
                    ("__meta_kubernetes_pod_phase", ""),
                    ("__meta_kubernetes_pod_ready", "unknown"),
                    ("__meta_kubernetes_pod_uid", "deadbeef"),
                    ("__meta_kubernetes_pod_container_init", "false"),
                ]),
                ls(&[
                    ("__address__", "1.2.3.4:9001"),
                    ("__meta_kubernetes_pod_container_name", "c2"),
                    ("__meta_kubernetes_pod_container_image", "c2:latest"),
                    ("__meta_kubernetes_pod_container_port_name", "sideport"),
                    ("__meta_kubernetes_pod_container_port_number", "9001"),
                    ("__meta_kubernetes_pod_container_port_protocol", "TCP"),
                    ("__meta_kubernetes_pod_host_ip", "2.3.4.5"),
                    ("__meta_kubernetes_pod_ip", "1.2.3.4"),
                    ("__meta_kubernetes_pod_name", "testpod"),
                    ("__meta_kubernetes_pod_node_name", "testnode"),
                    ("__meta_kubernetes_pod_phase", ""),
                    ("__meta_kubernetes_pod_ready", "unknown"),
                    ("__meta_kubernetes_pod_uid", "deadbeef"),
                    ("__meta_kubernetes_pod_container_init", "false"),
                ]),
            ],
            plain_slice_labels("default", "testendpoints"),
            "endpointslice/default/testendpoints",
        )]))
        .run();
}

#[test]
fn test_endpoint_slice_discovery_delete() {
    let (n, c) = make_discovery(Role::ENDPOINT_SLICE, names(&["default"]), vec![Object::EndpointSlice(make_endpoint_slice_v1("default"))]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            let obj = make_endpoint_slice_v1("default");
            let _ = c.delete(Kind::EndpointSlice, &obj.meta.namespace, &obj.meta.name);
        })
        .expected_max_items(2)
        .expected_res(expect(vec![empty_group("endpointslice/default/testendpoints")]))
        .run();
}

#[test]
fn test_endpoint_slice_discovery_update() {
    let (n, c) = make_discovery(Role::ENDPOINT_SLICE, names(&["default"]), vec![Object::EndpointSlice(make_endpoint_slice_v1("default"))]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            let mut obj = make_endpoint_slice_v1("default");
            obj.meta.labels = StrMap::new();
            obj.meta.annotations = StrMap::new();
            obj.endpoints.truncate(2);
            let _ = c.update("default", Object::EndpointSlice(obj));
        })
        .expected_max_items(2)
        .expected_res(expect(vec![group(vec![lse(T1, &[]), lse(T2, &[])], plain_slice_labels("default", "testendpoints"), "endpointslice/default/testendpoints")]))
        .run();
}

#[test]
fn test_endpoint_slice_discovery_empty_endpoints() {
    let (n, c) = make_discovery(Role::ENDPOINT_SLICE, names(&["default"]), vec![Object::EndpointSlice(make_endpoint_slice_v1("default"))]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            let mut obj = make_endpoint_slice_v1("default");
            obj.endpoints = Vec::new();
            let _ = c.update("default", Object::EndpointSlice(obj));
        })
        .expected_max_items(2)
        .expected_res(expect(vec![group(vec![], slice_labels("default", &[]), "endpointslice/default/testendpoints")]))
        .run();
}

#[test]
fn test_endpoint_slice_discovery_with_service() {
    let (n, c) = make_discovery(Role::ENDPOINT_SLICE, names(&["default"]), vec![Object::EndpointSlice(make_endpoint_slice_v1("default"))]);
    K8sDiscoveryTest::new(n)
        .before_run(move || {
            let _ = c.create("default", service("testendpoints", "default", smap(&[("app/name", "test")])));
        })
        .expected_max_items(1)
        .expected_res(expect(vec![group(four_targets(), slice_labels("default", SVC_APP_NAME_TEST), "endpointslice/default/testendpoints")]))
        .run();
}

#[test]
fn test_endpoint_slice_discovery_with_service_update() {
    let (n, c) = make_discovery(Role::ENDPOINT_SLICE, names(&["default"]), vec![Object::EndpointSlice(make_endpoint_slice_v1("default"))]);
    let c2 = c.clone();
    K8sDiscoveryTest::new(n)
        .before_run(move || {
            let _ = c.create("default", service("testendpoints", "default", smap(&[("app/name", "test")])));
        })
        .after_start(move || {
            let _ = c2.update("default", service("testendpoints", "default", smap(&[("app/name", "svc"), ("component", "testing")])));
        })
        .expected_max_items(2)
        .expected_res(expect(vec![group(
            four_targets(),
            slice_labels(
                "default",
                &[
                    ("__meta_kubernetes_service_label_app_name", "svc"),
                    ("__meta_kubernetes_service_label_component", "testing"),
                    ("__meta_kubernetes_service_labelpresent_app_name", "true"),
                    ("__meta_kubernetes_service_labelpresent_component", "true"),
                    ("__meta_kubernetes_service_name", "testendpoints"),
                ],
            ),
            "endpointslice/default/testendpoints",
        )]))
        .run();
}

const NODE_FOOBAR_EAST: Pairs = &[("__meta_kubernetes_node_label_az", "us-east1"), ("__meta_kubernetes_node_labelpresent_az", "true"), ("__meta_kubernetes_node_name", "foobar")];
const NODE_BARBAZ_WEST: Pairs = &[("__meta_kubernetes_node_label_az", "us-west2"), ("__meta_kubernetes_node_labelpresent_az", "true"), ("__meta_kubernetes_node_name", "barbaz")];

fn node_metadata_targets() -> Vec<LabelSet> {
    vec![lse(T1, NODE_FOOBAR_EAST), lse(T2, &[]), lse(T3, &[]), lse(T4, NODE_BARBAZ_WEST)]
}

#[test]
fn test_endpoints_slices_discovery_with_node_metadata() {
    let metadata_config = AttachMetadataConfig { node: true, ..Default::default() };
    let node_labels1 = smap(&[("az", "us-east1")]);
    let node_labels2 = smap(&[("az", "us-west2")]);
    let svc = service("testendpoints", "default", smap(&[("app/name", "test")]));
    let objs = vec![
        Object::EndpointSlice(make_endpoint_slice_v1("default")),
        Object::Node(make_node("foobar", "", "", node_labels1, StrMap::new())),
        Object::Node(make_node("barbaz", "", "", node_labels2, StrMap::new())),
        svc,
    ];
    let (n, _c) = make_discovery_with_metadata(Role::ENDPOINT_SLICE, NamespaceDiscovery::default(), metadata_config, objs);
    K8sDiscoveryTest::new(n)
        .expected_max_items(1)
        .expected_res(expect(vec![group(node_metadata_targets(), slice_labels("default", SVC_APP_NAME_TEST), "endpointslice/default/testendpoints")]))
        .run();
}

#[test]
fn test_endpoints_slices_discovery_with_updated_node_metadata() {
    let metadata_config = AttachMetadataConfig { node: true, ..Default::default() };
    let node_labels1 = smap(&[("az", "us-east1")]);
    let node_labels2 = smap(&[("az", "us-west2")]);
    let svc = service("testendpoints", "default", smap(&[("app/name", "test")]));
    let mut node1 = make_node("foobar", "", "", node_labels1, StrMap::new());
    let node2 = make_node("barbaz", "", "", node_labels2, StrMap::new());
    let objs = vec![Object::EndpointSlice(make_endpoint_slice_v1("default")), Object::Node(node1.clone()), Object::Node(node2), svc];
    let (n, c) = make_discovery_with_metadata(Role::ENDPOINT_SLICE, NamespaceDiscovery::default(), metadata_config, objs);
    K8sDiscoveryTest::new(n)
        .expected_max_items(2)
        .after_start(move || {
            set(&mut node1.meta.labels, "az", "us-central1");
            let _ = c.update("", Object::Node(node1));
        })
        // The first endpoint's TargetRef has no kind, so the node index does not have "foobar" and the node update
        // does not requeue the slice (as in Go).
        .expected_res(expect(vec![group(node_metadata_targets(), slice_labels("default", SVC_APP_NAME_TEST), "endpointslice/default/testendpoints")]))
        .run();
}

/// Go `m[k] = v` on a map.
fn set(m: &mut StrMap, k: &str, v: &str) {
    match m.iter_mut().find(|(mk, _)| mk == k) {
        Some(e) => e.1 = v.to_string(),
        None => m.push((k.to_string(), v.to_string())),
    }
}

#[test]
fn test_endpoint_slice_discovery_namespaces() {
    let mut ep_one = make_endpoint_slice_v1("default");
    ep_one.meta.namespace = "ns1".to_string();
    let objs = vec![
        Object::EndpointSlice(ep_one),
        Object::EndpointSlice(ApiEndpointSlice {
            meta: ObjectMeta { name: "testendpoints".to_string(), namespace: "ns2".to_string(), ..Default::default() },
            address_type: "IPv4".to_string(),
            ports: vec![EndpointSlicePort { name: s("testport"), port: Some(9000), protocol: s("TCP"), ..Default::default() }],
            endpoints: vec![Endpoint { addresses: vec!["4.3.2.1".to_string()], target_ref: pod_ref("ns2"), ..Default::default() }],
        }),
        service("testendpoints", "ns1", smap(&[("app", "app1")])),
        Object::Pod(pod("ns2", vec![container("c1", "c1:latest", "mainport", 9000)], vec![], "2.3.4.5", "4.3.2.1")),
    ];
    let (n, _c) = make_discovery(Role::ENDPOINT_SLICE, names(&["ns1", "ns2"]), objs);
    K8sDiscoveryTest::new(n)
        .expected_max_items(2)
        .expected_res(expect(vec![
            group(
                four_targets(),
                slice_labels(
                    "ns1",
                    &[("__meta_kubernetes_service_label_app", "app1"), ("__meta_kubernetes_service_labelpresent_app", "true"), ("__meta_kubernetes_service_name", "testendpoints")],
                ),
                "endpointslice/ns1/testendpoints",
            ),
            group(
                vec![ls(&[
                    ("__address__", "4.3.2.1:9000"),
                    ("__meta_kubernetes_endpointslice_address_target_kind", "Pod"),
                    ("__meta_kubernetes_endpointslice_address_target_name", "testpod"),
                    ("__meta_kubernetes_endpointslice_port", "9000"),
                    ("__meta_kubernetes_endpointslice_port_name", "testport"),
                    ("__meta_kubernetes_endpointslice_port_protocol", "TCP"),
                    ("__meta_kubernetes_pod_container_name", "c1"),
                    ("__meta_kubernetes_pod_container_image", "c1:latest"),
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
                ])],
                plain_slice_labels("ns2", "testendpoints"),
                "endpointslice/ns2/testendpoints",
            ),
        ]))
        .run();
}

#[test]
fn test_endpoint_slice_discovery_own_namespace() {
    let mut ep_one = make_endpoint_slice_v1("default");
    ep_one.meta.namespace = "own-ns".to_string();
    let mut ep_two = make_endpoint_slice_v1("default");
    ep_two.meta.namespace = "non-own-ns".to_string();
    let pod_one = pod("own-ns", vec![container("p1", "p1:latest", "mainport", 9000)], vec![], "2.3.4.5", "4.3.2.1");
    let mut pod_two = pod_one.clone();
    pod_two.meta.namespace = "non-own-ns".to_string();
    let objs = vec![Object::EndpointSlice(ep_one), Object::EndpointSlice(ep_two), Object::Pod(pod_one), Object::Pod(pod_two)];
    let (n, _c) = make_discovery(Role::ENDPOINT_SLICE, NamespaceDiscovery { include_own_namespace: true, ..Default::default() }, objs);
    K8sDiscoveryTest::new(n)
        .expected_max_items(1)
        .expected_res(expect(vec![group(four_targets(), slice_labels("own-ns", &[]), "endpointslice/own-ns/testendpoints")]))
        .run();
}

#[test]
fn test_endpoint_slice_discovery_empty_pod_status() {
    let mut ep = make_endpoint_slice_v1("default");
    ep.meta.namespace = "ns".to_string();
    let mut p = pod("ns", vec![container("p1", "p1:latest", "mainport", 9000)], vec![], "", "");
    p.status = PodStatus::default();
    let objs = vec![Object::EndpointSlice(ep), Object::Pod(p)];
    // Go uses RoleEndpoint here, not RoleEndpointSlice.
    let (n, _c) = make_discovery(Role::ENDPOINT, NamespaceDiscovery { include_own_namespace: true, ..Default::default() }, objs);
    K8sDiscoveryTest::new(n).expected_max_items(0).expected_res(expect(vec![])).run();
}

/// TestEndpointSliceInfIndexersCount makes sure that RoleEndpointSlice discovery sets up indexing for the main Kube
/// informer only when needed.
#[test]
fn test_endpoint_slice_inf_indexers_count() {
    for (name, with_node_metadata) in [("with node metadata", true), ("without node metadata", false)] {
        eprintln!("subtest {name}");
        // service indexer is enabled by default
        let mut main_inf_indexers_count = 1;
        let n = if with_node_metadata {
            main_inf_indexers_count += 1;
            make_discovery_with_metadata(Role::ENDPOINT_SLICE, NamespaceDiscovery::default(), AttachMetadataConfig { node: true, ..Default::default() }, vec![]).0
        } else {
            make_discovery(Role::ENDPOINT_SLICE, NamespaceDiscovery::default(), vec![]).0
        };
        let t = K8sDiscoveryTest::new(n);
        let d = t.discovery.clone();
        t.after_start(move || {
            let ds = d.discoverers();
            assert_eq!(ds.len(), 1);
            let es = ds[0].as_any().downcast_ref::<EndpointSlice>().expect("discoverer is an EndpointSlice");
            assert_eq!(es.endpoint_slice_informer().indexer_count(), main_inf_indexers_count);
        })
        .run();
    }
}

#[test]
fn test_endpoint_slice_discovery_sidecar_container() {
    let objs = vec![
        Object::EndpointSlice(ApiEndpointSlice {
            meta: ObjectMeta { name: "testsidecar".to_string(), namespace: "default".to_string(), ..Default::default() },
            address_type: "IPv4".to_string(),
            ports: vec![
                EndpointSlicePort { name: s("testport"), port: Some(9000), protocol: s("TCP"), ..Default::default() },
                EndpointSlicePort { name: s("initport"), port: Some(9111), protocol: s("TCP"), ..Default::default() },
            ],
            endpoints: vec![Endpoint { addresses: vec!["4.3.2.1".to_string()], target_ref: pod_ref("default"), ..Default::default() }],
        }),
        Object::Pod(pod(
            "default",
            vec![container("c1", "c1:latest", "mainport", 9000)],
            vec![container("ic1", "ic1:latest", "initport", 1111), container("ic2", "ic2:latest", "initport", 9111)],
            "2.3.4.5",
            "4.3.2.1",
        )),
    ];
    let (n, _c) = make_discovery(Role::ENDPOINT_SLICE, NamespaceDiscovery::default(), objs);
    K8sDiscoveryTest::new(n)
        .expected_max_items(1)
        .expected_res(expect(vec![group(
            vec![
                ls(&[
                    ("__address__", "4.3.2.1:9000"),
                    ("__meta_kubernetes_endpointslice_address_target_kind", "Pod"),
                    ("__meta_kubernetes_endpointslice_address_target_name", "testpod"),
                    ("__meta_kubernetes_endpointslice_port", "9000"),
                    ("__meta_kubernetes_endpointslice_port_name", "testport"),
                    ("__meta_kubernetes_endpointslice_port_protocol", "TCP"),
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
                    ("__meta_kubernetes_endpointslice_address_target_kind", "Pod"),
                    ("__meta_kubernetes_endpointslice_address_target_name", "testpod"),
                    ("__meta_kubernetes_endpointslice_port", "9111"),
                    ("__meta_kubernetes_endpointslice_port_name", "initport"),
                    ("__meta_kubernetes_endpointslice_port_protocol", "TCP"),
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
            plain_slice_labels("default", "testsidecar"),
            "endpointslice/default/testsidecar",
        )]))
        .run();
}

#[test]
fn test_endpoints_slices_discovery_with_namespace_metadata() {
    let ns = "test-ns";
    let ns_labels = smap(&[("service", "web"), ("layer", "frontend")]);
    let ns_annotations = smap(&[("contact", "platform"), ("release", "v5.6.7")]);
    let svc = service("testendpoints", ns, smap(&[("app/name", "test")]));
    let objs = vec![make_namespace(ns, ns_labels, ns_annotations), svc, Object::EndpointSlice(make_endpoint_slice_v1(ns))];
    let (n, _c) = make_discovery_with_metadata(Role::ENDPOINT_SLICE, NamespaceDiscovery::default(), AttachMetadataConfig { namespace: true, ..Default::default() }, objs);
    K8sDiscoveryTest::new(n)
        .expected_max_items(1)
        .expected_res(expect(vec![group(
            four_targets(),
            slice_labels(
                ns,
                &[
                    ("__meta_kubernetes_namespace_annotation_contact", "platform"),
                    ("__meta_kubernetes_namespace_annotationpresent_contact", "true"),
                    ("__meta_kubernetes_namespace_annotation_release", "v5.6.7"),
                    ("__meta_kubernetes_namespace_annotationpresent_release", "true"),
                    ("__meta_kubernetes_namespace_label_service", "web"),
                    ("__meta_kubernetes_namespace_labelpresent_service", "true"),
                    ("__meta_kubernetes_namespace_label_layer", "frontend"),
                    ("__meta_kubernetes_namespace_labelpresent_layer", "true"),
                    ("__meta_kubernetes_service_label_app_name", "test"),
                    ("__meta_kubernetes_service_labelpresent_app_name", "true"),
                    ("__meta_kubernetes_service_name", "testendpoints"),
                ],
            ),
            &format!("endpointslice/{ns}/testendpoints"),
        )]))
        .run();
}

#[test]
fn test_endpoints_slices_discovery_with_updated_namespace_metadata() {
    let ns = "test-ns";
    let ns_labels = smap(&[("component", "database"), ("layer", "backend")]);
    let ns_annotations = smap(&[("contact", "dba"), ("release", "v6.7.8")]);
    let metadata_config = AttachMetadataConfig { namespace: true, ..Default::default() };
    let svc = service("testendpoints", ns, smap(&[("app/name", "test")]));
    let namespace = ObjectMeta { name: ns.to_string(), labels: ns_labels, annotations: ns_annotations, ..Default::default() };
    let (n, c) = make_discovery_with_metadata(
        Role::ENDPOINT_SLICE,
        NamespaceDiscovery::default(),
        metadata_config,
        vec![Object::Namespace(namespace.clone()), svc, Object::EndpointSlice(make_endpoint_slice_v1(ns))],
    );
    let mut namespace = namespace;
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
            four_targets(),
            slice_labels(
                ns,
                &[
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
                    ("__meta_kubernetes_service_label_app_name", "test"),
                    ("__meta_kubernetes_service_labelpresent_app_name", "true"),
                    ("__meta_kubernetes_service_name", "testendpoints"),
                ],
            ),
            &format!("endpointslice/{ns}/testendpoints"),
        )]))
        .run();
}

/// Go `makeDualStackService`.
fn make_dual_stack_service(name: &str, namespace: &str, policy: &str, families: &[&str]) -> Object {
    Object::Service(Service {
        meta: ObjectMeta { name: name.to_string(), namespace: namespace.to_string(), ..Default::default() },
        spec: ServiceSpec { ip_family_policy: s(policy), ip_families: families.iter().map(|f| f.to_string()).collect(), ..Default::default() },
        ..Default::default()
    })
}

/// Go `makeDualStackEndpointSlice`.
fn make_dual_stack_endpoint_slice(name: &str, namespace: &str, svc_name: &str, addr_type: &str, addr: &str) -> Object {
    Object::EndpointSlice(ApiEndpointSlice {
        meta: ObjectMeta { name: name.to_string(), namespace: namespace.to_string(), labels: smap(&[("kubernetes.io/service-name", svc_name)]), ..Default::default() },
        address_type: addr_type.to_string(),
        ports: vec![EndpointSlicePort { name: s("http"), port: Some(9000), protocol: s("TCP"), ..Default::default() }],
        endpoints: vec![Endpoint { addresses: vec![addr.to_string()], conditions: EndpointConditions { ready: Some(true), ..Default::default() }, ..Default::default() }],
    })
}

/// Go `dualStackGroupLabels`.
fn dual_stack_group_labels(namespace: &str, addr_type: &str, slice_name: &str, svc_name: &str) -> LabelSet {
    ls(&[
        ("__meta_kubernetes_namespace", namespace),
        ("__meta_kubernetes_endpointslice_address_type", addr_type),
        ("__meta_kubernetes_endpointslice_name", slice_name),
        ("__meta_kubernetes_endpointslice_label_kubernetes_io_service_name", svc_name),
        ("__meta_kubernetes_endpointslice_labelpresent_kubernetes_io_service_name", "true"),
        ("__meta_kubernetes_service_name", svc_name),
    ])
}

/// Go `dualStackTarget`.
fn dual_stack_target(addr: &str) -> LabelSet {
    ls(&[
        ("__address__", addr),
        ("__meta_kubernetes_endpointslice_endpoint_conditions_ready", "true"),
        ("__meta_kubernetes_endpointslice_port", "9000"),
        ("__meta_kubernetes_endpointslice_port_name", "http"),
        ("__meta_kubernetes_endpointslice_port_protocol", "TCP"),
    ])
}

/// TestEndpointSliceDiscoveryDualStackPreferredSkipsSecondarySlice verifies that when a service has
/// ipFamilyPolicy=PreferDualStack with IPv4 as the primary family, only the IPv4 EndpointSlice generates targets.
/// The IPv6 (secondary) EndpointSlice produces an empty target group.
#[test]
fn test_endpoint_slice_discovery_dual_stack_preferred_skips_secondary_slice() {
    let svc = make_dual_stack_service("testsvc", "default", "PreferDualStack", &["IPv4", "IPv6"]);
    let eps_ipv4 = make_dual_stack_endpoint_slice("testsvc-ipv4", "default", "testsvc", "IPv4", "1.2.3.4");
    let eps_ipv6 = make_dual_stack_endpoint_slice("testsvc-ipv6", "default", "testsvc", "IPv6", "fd00::1");
    let (n, _c) = make_discovery(Role::ENDPOINT_SLICE, names(&["default"]), vec![svc, eps_ipv4, eps_ipv6]);
    K8sDiscoveryTest::new(n)
        .expected_max_items(2)
        .expected_res(expect(vec![
            group(vec![dual_stack_target("1.2.3.4:9000")], dual_stack_group_labels("default", "IPv4", "testsvc-ipv4", "testsvc"), "endpointslice/default/testsvc-ipv4"),
            group(vec![], dual_stack_group_labels("default", "IPv6", "testsvc-ipv6", "testsvc"), "endpointslice/default/testsvc-ipv6"),
        ]))
        .run();
}

/// TestEndpointSliceDiscoveryDualStackRequiredSkipsSecondarySlice verifies the same deduplication behaviour for
/// ipFamilyPolicy=RequireDualStack.
#[test]
fn test_endpoint_slice_discovery_dual_stack_required_skips_secondary_slice() {
    let svc = make_dual_stack_service("testsvc", "default", "RequireDualStack", &["IPv4", "IPv6"]);
    let eps_ipv4 = make_dual_stack_endpoint_slice("testsvc-ipv4", "default", "testsvc", "IPv4", "1.2.3.4");
    let eps_ipv6 = make_dual_stack_endpoint_slice("testsvc-ipv6", "default", "testsvc", "IPv6", "fd00::1");
    let (n, _c) = make_discovery(Role::ENDPOINT_SLICE, names(&["default"]), vec![svc, eps_ipv4, eps_ipv6]);
    K8sDiscoveryTest::new(n)
        .expected_max_items(2)
        .expected_res(expect(vec![
            group(vec![dual_stack_target("1.2.3.4:9000")], dual_stack_group_labels("default", "IPv4", "testsvc-ipv4", "testsvc"), "endpointslice/default/testsvc-ipv4"),
            group(vec![], dual_stack_group_labels("default", "IPv6", "testsvc-ipv6", "testsvc"), "endpointslice/default/testsvc-ipv6"),
        ]))
        .run();
}

#[test]
fn test_endpoint_slice_discovery_single_stack_i_pv4_unaffected() {
    let svc = make_dual_stack_service("testsvc", "default", "SingleStack", &["IPv4"]);
    let eps_ipv4 = make_dual_stack_endpoint_slice("testsvc-ipv4", "default", "testsvc", "IPv4", "1.2.3.4");
    let (n, _c) = make_discovery(Role::ENDPOINT_SLICE, names(&["default"]), vec![svc, eps_ipv4]);
    K8sDiscoveryTest::new(n)
        .expected_max_items(1)
        .expected_res(expect(vec![group(
            vec![dual_stack_target("1.2.3.4:9000")],
            dual_stack_group_labels("default", "IPv4", "testsvc-ipv4", "testsvc"),
            "endpointslice/default/testsvc-ipv4",
        )]))
        .run();
}

/// TestEndpointSliceDiscoverySingleStackIPv6Unaffected verifies that a single-stack IPv6 service is not affected by
/// the dual-stack deduplication logic and still generates targets.
#[test]
fn test_endpoint_slice_discovery_single_stack_i_pv6_unaffected() {
    let svc = make_dual_stack_service("testsvc", "default", "SingleStack", &["IPv6"]);
    let eps_ipv6 = make_dual_stack_endpoint_slice("testsvc-ipv6", "default", "testsvc", "IPv6", "fd00::1");
    let (n, _c) = make_discovery(Role::ENDPOINT_SLICE, names(&["default"]), vec![svc, eps_ipv6]);
    K8sDiscoveryTest::new(n)
        .expected_max_items(1)
        .expected_res(expect(vec![group(
            vec![dual_stack_target("[fd00::1]:9000")],
            dual_stack_group_labels("default", "IPv6", "testsvc-ipv6", "testsvc"),
            "endpointslice/default/testsvc-ipv6",
        )]))
        .run();
}

/// TestEndpointSliceDiscoveryDualStackPreferredIPv6Primary verifies that when IPv6 is the primary family of a
/// dual-stack service, the IPv6 EndpointSlice generates targets and the IPv4 (secondary) EndpointSlice produces an
/// empty target group.
#[test]
fn test_endpoint_slice_discovery_dual_stack_preferred_i_pv6_primary() {
    let svc = make_dual_stack_service("testsvc", "default", "PreferDualStack", &["IPv6", "IPv4"]);
    let eps_ipv6 = make_dual_stack_endpoint_slice("testsvc-ipv6", "default", "testsvc", "IPv6", "fd00::1");
    let eps_ipv4 = make_dual_stack_endpoint_slice("testsvc-ipv4", "default", "testsvc", "IPv4", "1.2.3.4");
    let (n, _c) = make_discovery(Role::ENDPOINT_SLICE, names(&["default"]), vec![svc, eps_ipv6, eps_ipv4]);
    K8sDiscoveryTest::new(n)
        .expected_max_items(2)
        .expected_res(expect(vec![
            group(vec![dual_stack_target("[fd00::1]:9000")], dual_stack_group_labels("default", "IPv6", "testsvc-ipv6", "testsvc"), "endpointslice/default/testsvc-ipv6"),
            group(vec![], dual_stack_group_labels("default", "IPv4", "testsvc-ipv4", "testsvc"), "endpointslice/default/testsvc-ipv4"),
        ]))
        .run();
}
