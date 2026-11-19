//! Port of discovery/kubernetes/pod_test.go.
mod k8s_common;

use std::collections::BTreeMap;

use discovery::Group;
use discovery::kubernetes::api::{Condition, Container, ContainerPort, ContainerStatus, Kind, Node, NodeAddress, Object, ObjectMeta, OwnerReference, Pod, PodSpec, PodStatus, StrMap};
use discovery::kubernetes::{AttachMetadataConfig, NamespaceDiscovery, PodMetadataConfig, ResourceSelector, Role, RoleSelector};
use k8s_common::*;

fn owner(kind: &str, name: &str, uid: &str) -> OwnerReference {
    OwnerReference { kind: kind.to_string(), name: name.to_string(), uid: uid.to_string(), controller: Some(true) }
}

fn port(name: &str, protocol: &str, p: i32) -> ContainerPort {
    ContainerPort { name: name.to_string(), protocol: protocol.to_string(), container_port: p }
}

fn container(name: &str, image: &str, ports: Vec<ContainerPort>) -> Container {
    Container { name: name.to_string(), image: image.to_string(), ports }
}

fn status(name: &str, id: &str) -> ContainerStatus {
    ContainerStatus { name: name.to_string(), container_id: id.to_string() }
}

fn cond(t: &str, s: &str) -> Condition {
    Condition { type_: t.to_string(), status: s.to_string() }
}

fn make_multi_port_pods() -> Pod {
    Pod {
        meta: ObjectMeta {
            name: "testpod".to_string(),
            namespace: "default".to_string(),
            labels: smap(&[("test/label", "testvalue")]),
            annotations: smap(&[("test/annotation", "testannotationvalue")]),
            uid: "abc123".to_string(),
            owner_references: vec![owner("testcontrollerkind", "testcontrollername", "")],
            ..Default::default()
        },
        spec: PodSpec {
            node_name: "testnode".to_string(),
            containers: vec![
                container("testcontainer0", "testcontainer0:latest", vec![port("testport0", "TCP", 9000), port("testport1", "UDP", 9001)]),
                container("testcontainer1", "testcontainer1:latest", vec![]),
            ],
            ..Default::default()
        },
        status: PodStatus {
            pod_ip: "1.2.3.4".to_string(),
            host_ip: "2.3.4.5".to_string(),
            phase: "Running".to_string(),
            conditions: vec![cond("Ready", "True")],
            container_statuses: vec![status("testcontainer0", "docker://a1b2c3d4e5f6"), status("testcontainer1", "containerd://6f5e4d3c2b1a")],
            ..Default::default()
        },
    }
}

fn make_pods(namespace: &str) -> Pod {
    Pod {
        meta: ObjectMeta { name: "testpod".to_string(), namespace: namespace.to_string(), uid: "abc123".to_string(), ..Default::default() },
        spec: PodSpec { node_name: "testnode".to_string(), containers: vec![container("testcontainer", "testcontainer:latest", vec![port("testport", "TCP", 9000)])], ..Default::default() },
        status: PodStatus {
            pod_ip: "1.2.3.4".to_string(),
            host_ip: "2.3.4.5".to_string(),
            phase: "Running".to_string(),
            conditions: vec![cond("Ready", "True")],
            container_statuses: vec![status("testcontainer", "docker://a1b2c3d4e5f6")],
            ..Default::default()
        },
    }
}

fn make_init_container_pods() -> Pod {
    Pod {
        meta: ObjectMeta { name: "testpod".to_string(), namespace: "default".to_string(), uid: "abc123".to_string(), ..Default::default() },
        spec: PodSpec {
            node_name: "testnode".to_string(),
            containers: vec![container("testcontainer", "testcontainer:latest", vec![port("testport", "TCP", 9000)])],
            init_containers: vec![container("initcontainer", "initcontainer:latest", vec![])],
        },
        status: PodStatus {
            pod_ip: "1.2.3.4".to_string(),
            host_ip: "2.3.4.5".to_string(),
            phase: "Pending".to_string(),
            conditions: vec![cond("Ready", "False")],
            container_statuses: vec![status("testcontainer", "docker://a1b2c3d4e5f6")],
            init_container_statuses: vec![status("initcontainer", "containerd://6f5e4d3c2b1a")],
        },
    }
}

/// Go `makeNode` (node_test.go).
fn make_node(name: &str, address: &str, provider_id: &str, labels: StrMap, annotations: StrMap, conditions: Vec<Condition>) -> Object {
    Object::Node(Node {
        meta: ObjectMeta { name: name.to_string(), labels, annotations, ..Default::default() },
        provider_id: provider_id.to_string(),
        addresses: vec![NodeAddress { type_: "InternalIP".to_string(), address: address.to_string() }],
        kubelet_port: 10250,
        conditions,
    })
}

/// Go `makeNamespace` (endpointslice_test.go).
fn make_namespace(name: &str, labels: StrMap, annotations: StrMap) -> ObjectMeta {
    ObjectMeta { name: name.to_string(), labels, annotations, ..Default::default() }
}

fn testcontainer_target(addr: &str) -> model::labelset::LabelSet {
    ls(&[
        ("__address__", addr),
        ("__meta_kubernetes_pod_container_name", "testcontainer"),
        ("__meta_kubernetes_pod_container_image", "testcontainer:latest"),
        ("__meta_kubernetes_pod_container_port_name", "testport"),
        ("__meta_kubernetes_pod_container_port_number", "9000"),
        ("__meta_kubernetes_pod_container_port_protocol", "TCP"),
        ("__meta_kubernetes_pod_container_init", "false"),
        ("__meta_kubernetes_pod_container_id", "docker://a1b2c3d4e5f6"),
    ])
}

fn expected_pod_target_groups(ns: &str) -> BTreeMap<String, Group> {
    let key = format!("pod/{ns}/testpod");
    expect(vec![group(
        vec![testcontainer_target("1.2.3.4:9000")],
        ls(&[
            ("__meta_kubernetes_pod_name", "testpod"),
            ("__meta_kubernetes_namespace", ns),
            ("__meta_kubernetes_pod_node_name", "testnode"),
            ("__meta_kubernetes_pod_ip", "1.2.3.4"),
            ("__meta_kubernetes_pod_host_ip", "2.3.4.5"),
            ("__meta_kubernetes_pod_ready", "true"),
            ("__meta_kubernetes_pod_phase", "Running"),
            ("__meta_kubernetes_pod_uid", "abc123"),
        ]),
        &key,
    )])
}

fn expected_pod_target_groups_with_node_meta(ns: &str, node_name: &str, node_labels: &StrMap) -> BTreeMap<String, Group> {
    let mut result = expected_pod_target_groups(ns);
    for tg in result.values_mut() {
        tg.labels.0.insert("__meta_kubernetes_node_name".to_string(), node_name.to_string());
        for (k, v) in node_labels {
            tg.labels.0.insert(format!("__meta_kubernetes_node_label_{k}"), v.clone());
            tg.labels.0.insert(format!("__meta_kubernetes_node_labelpresent_{k}"), "true".to_string());
        }
    }
    result
}

#[test]
fn test_pod_discovery_before_run() {
    let (n, c) = make_discovery(Role::POD, NamespaceDiscovery::default(), vec![]);
    K8sDiscoveryTest::new(n)
        .before_run(move || {
            let obj = make_multi_port_pods();
            let ns = obj.meta.namespace.clone();
            let _ = c.create(&ns, Object::Pod(obj));
        })
        .expected_max_items(1)
        .expected_res(expect(vec![group(
            vec![
                ls(&[
                    ("__address__", "1.2.3.4:9000"),
                    ("__meta_kubernetes_pod_container_name", "testcontainer0"),
                    ("__meta_kubernetes_pod_container_image", "testcontainer0:latest"),
                    ("__meta_kubernetes_pod_container_port_name", "testport0"),
                    ("__meta_kubernetes_pod_container_port_number", "9000"),
                    ("__meta_kubernetes_pod_container_port_protocol", "TCP"),
                    ("__meta_kubernetes_pod_container_init", "false"),
                    ("__meta_kubernetes_pod_container_id", "docker://a1b2c3d4e5f6"),
                ]),
                ls(&[
                    ("__address__", "1.2.3.4:9001"),
                    ("__meta_kubernetes_pod_container_name", "testcontainer0"),
                    ("__meta_kubernetes_pod_container_image", "testcontainer0:latest"),
                    ("__meta_kubernetes_pod_container_port_name", "testport1"),
                    ("__meta_kubernetes_pod_container_port_number", "9001"),
                    ("__meta_kubernetes_pod_container_port_protocol", "UDP"),
                    ("__meta_kubernetes_pod_container_init", "false"),
                    ("__meta_kubernetes_pod_container_id", "docker://a1b2c3d4e5f6"),
                ]),
                ls(&[
                    ("__address__", "1.2.3.4"),
                    ("__meta_kubernetes_pod_container_name", "testcontainer1"),
                    ("__meta_kubernetes_pod_container_image", "testcontainer1:latest"),
                    ("__meta_kubernetes_pod_container_init", "false"),
                    ("__meta_kubernetes_pod_container_id", "containerd://6f5e4d3c2b1a"),
                ]),
            ],
            ls(&[
                ("__meta_kubernetes_pod_name", "testpod"),
                ("__meta_kubernetes_namespace", "default"),
                ("__meta_kubernetes_pod_label_test_label", "testvalue"),
                ("__meta_kubernetes_pod_labelpresent_test_label", "true"),
                ("__meta_kubernetes_pod_annotation_test_annotation", "testannotationvalue"),
                ("__meta_kubernetes_pod_annotationpresent_test_annotation", "true"),
                ("__meta_kubernetes_pod_node_name", "testnode"),
                ("__meta_kubernetes_pod_ip", "1.2.3.4"),
                ("__meta_kubernetes_pod_host_ip", "2.3.4.5"),
                ("__meta_kubernetes_pod_ready", "true"),
                ("__meta_kubernetes_pod_phase", "Running"),
                ("__meta_kubernetes_pod_uid", "abc123"),
                ("__meta_kubernetes_pod_controller_kind", "testcontrollerkind"),
                ("__meta_kubernetes_pod_controller_name", "testcontrollername"),
            ]),
            "pod/default/testpod",
        )]))
        .run();
}

#[test]
fn test_pod_discovery_init_container() {
    let (n, c) = make_discovery(Role::POD, NamespaceDiscovery::default(), vec![]);

    let ns = "default";
    let key = format!("pod/{ns}/testpod");
    let mut expected = expected_pod_target_groups(ns);
    let g = expected.get_mut(&key).unwrap();
    g.targets.push(ls(&[
        ("__address__", "1.2.3.4"),
        ("__meta_kubernetes_pod_container_name", "initcontainer"),
        ("__meta_kubernetes_pod_container_image", "initcontainer:latest"),
        ("__meta_kubernetes_pod_container_init", "true"),
        ("__meta_kubernetes_pod_container_id", "containerd://6f5e4d3c2b1a"),
    ]));
    g.labels.0.insert("__meta_kubernetes_pod_phase".to_string(), "Pending".to_string());
    g.labels.0.insert("__meta_kubernetes_pod_ready".to_string(), "false".to_string());

    K8sDiscoveryTest::new(n)
        .before_run(move || {
            let obj = make_init_container_pods();
            let ns = obj.meta.namespace.clone();
            let _ = c.create(&ns, Object::Pod(obj));
        })
        .expected_max_items(1)
        .expected_res(expected)
        .run();
}

#[test]
fn test_pod_discovery_add() {
    let (n, c) = make_discovery(Role::POD, NamespaceDiscovery::default(), vec![]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            let obj = make_pods("default");
            let _ = c.create("default", Object::Pod(obj));
        })
        .expected_max_items(1)
        .expected_res(expected_pod_target_groups("default"))
        .run();
}

#[test]
fn test_pod_discovery_delete() {
    let obj = make_pods("default");
    let (n, c) = make_discovery(Role::POD, NamespaceDiscovery::default(), vec![Object::Pod(obj)]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            let obj = make_pods("default");
            let _ = c.delete(Kind::Pod, &obj.meta.namespace, &obj.meta.name);
        })
        .expected_max_items(2)
        .expected_res(expect(vec![empty_group("pod/default/testpod")]))
        .run();
}

#[test]
fn test_pod_discovery_update() {
    let obj = Pod {
        meta: ObjectMeta { name: "testpod".to_string(), namespace: "default".to_string(), uid: "xyz321".to_string(), ..Default::default() },
        spec: PodSpec { node_name: "testnode".to_string(), containers: vec![container("testcontainer", "testcontainer:latest", vec![port("testport", "TCP", 9000)])], ..Default::default() },
        status: PodStatus { pod_ip: "1.2.3.4".to_string(), host_ip: "2.3.4.5".to_string(), ..Default::default() },
    };
    let (n, c) = make_discovery(Role::POD, NamespaceDiscovery::default(), vec![Object::Pod(obj)]);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            let obj = make_pods("default");
            let _ = c.update("default", Object::Pod(obj));
        })
        .expected_max_items(2)
        .expected_res(expected_pod_target_groups("default"))
        .run();
}

#[test]
fn test_pod_discovery_update_empty_pod_ip() {
    let (n, c) = make_discovery(Role::POD, NamespaceDiscovery::default(), vec![]);
    let initial_pod = make_pods("default");
    let mut updated_pod = make_pods("default");
    updated_pod.status.pod_ip = String::new();

    let c2 = c.clone();
    K8sDiscoveryTest::new(n)
        .before_run(move || {
            let _ = c.create("default", Object::Pod(initial_pod));
        })
        .after_start(move || {
            let _ = c2.update("default", Object::Pod(updated_pod));
        })
        .expected_max_items(2)
        .expected_res(expect(vec![empty_group("pod/default/testpod")]))
        .run();
}

#[test]
fn test_pod_discovery_namespaces() {
    let (n, c) = make_discovery(Role::POD, NamespaceDiscovery { names: vec!["ns1".to_string(), "ns2".to_string()], ..Default::default() }, vec![]);

    let mut expected = expected_pod_target_groups("ns1");
    expected.extend(expected_pod_target_groups("ns2"));
    K8sDiscoveryTest::new(n)
        .before_run(move || {
            for ns in ["ns1", "ns2"] {
                let mut pod = make_pods("default");
                pod.meta.namespace = ns.to_string();
                let _ = c.create(ns, Object::Pod(pod));
            }
        })
        .expected_max_items(2)
        .expected_res(expected)
        .run();
}

#[test]
fn test_pod_discovery_own_namespace() {
    let (n, c) = make_discovery(Role::POD, NamespaceDiscovery { include_own_namespace: true, ..Default::default() }, vec![]);

    let expected = expected_pod_target_groups("own-ns");
    K8sDiscoveryTest::new(n)
        .before_run(move || {
            for ns in ["own-ns", "non-own-ns"] {
                let mut pod = make_pods("default");
                pod.meta.namespace = ns.to_string();
                let _ = c.create(ns, Object::Pod(pod));
            }
        })
        .expected_max_items(1)
        .expected_res(expected)
        .run();
}

#[test]
fn test_pod_discovery_with_node_metadata() {
    let attach_metadata = AttachMetadataConfig { node: true, ..Default::default() };
    let (n, c) = make_discovery_with_metadata(Role::POD, NamespaceDiscovery::default(), attach_metadata, vec![]);
    let node_lbls = smap(&[("l1", "v1")]);

    let expected = expected_pod_target_groups_with_node_meta("default", "testnode", &node_lbls);
    K8sDiscoveryTest::new(n)
        .after_start(move || {
            let nodes = make_node("testnode", "", "", node_lbls, smap(&[]), Vec::new());
            let _ = c.create("", nodes);

            let pods = make_pods("default");
            let _ = c.create("default", Object::Pod(pods));
        })
        .expected_max_items(2)
        .expected_res(expected)
        .run();
}

#[test]
fn test_pod_discovery_with_node_metadata_update_node() {
    let node_lbls = smap(&[("l2", "v2")]);
    let attach_metadata = AttachMetadataConfig { node: true, ..Default::default() };
    let (n, c) = make_discovery_with_metadata(Role::POD, NamespaceDiscovery::default(), attach_metadata, vec![]);

    let expected = expected_pod_target_groups_with_node_meta("default", "testnode", &node_lbls);
    let c2 = c.clone();
    K8sDiscoveryTest::new(n)
        .before_run(move || {
            let old_node_lbls = smap(&[("l1", "v1")]);
            let nodes = make_node("testnode", "", "", old_node_lbls, smap(&[]), Vec::new());
            let _ = c.create("", nodes);
        })
        .after_start(move || {
            let pods = make_pods("default");
            let _ = c2.create("default", Object::Pod(pods));

            let nodes = make_node("testnode", "", "", node_lbls, smap(&[]), Vec::new());
            let _ = c2.update("", nodes);
        })
        .expected_max_items(2)
        .expected_res(expected)
        .run();
}

#[test]
fn test_pod_discovery_with_namespace_metadata() {
    let ns = "test-ns";
    let ns_labels = smap(&[("app", "web"), ("tier", "frontend")]);
    let ns_annotations = smap(&[("maintainer", "devops"), ("build", "v3.4.5")]);

    let (n, _) = make_discovery_with_metadata(
        Role::POD,
        NamespaceDiscovery::default(),
        AttachMetadataConfig { namespace: true, ..Default::default() },
        vec![Object::Namespace(make_namespace(ns, ns_labels, ns_annotations)), Object::Pod(make_pods(ns))],
    );
    K8sDiscoveryTest::new(n)
        .expected_max_items(1)
        .expected_res(expect(vec![group(
            vec![testcontainer_target("1.2.3.4:9000")],
            ls(&[
                ("__meta_kubernetes_namespace", ns),
                ("__meta_kubernetes_namespace_annotation_maintainer", "devops"),
                ("__meta_kubernetes_namespace_annotationpresent_maintainer", "true"),
                ("__meta_kubernetes_namespace_annotation_build", "v3.4.5"),
                ("__meta_kubernetes_namespace_annotationpresent_build", "true"),
                ("__meta_kubernetes_namespace_label_app", "web"),
                ("__meta_kubernetes_namespace_labelpresent_app", "true"),
                ("__meta_kubernetes_namespace_label_tier", "frontend"),
                ("__meta_kubernetes_namespace_labelpresent_tier", "true"),
                ("__meta_kubernetes_pod_name", "testpod"),
                ("__meta_kubernetes_pod_ip", "1.2.3.4"),
                ("__meta_kubernetes_pod_ready", "true"),
                ("__meta_kubernetes_pod_phase", "Running"),
                ("__meta_kubernetes_pod_node_name", "testnode"),
                ("__meta_kubernetes_pod_host_ip", "2.3.4.5"),
                ("__meta_kubernetes_pod_uid", "abc123"),
            ]),
            &format!("pod/{ns}/testpod"),
        )]))
        .run();
}

/// Sets a key in a Go-like map (replaces an existing key in place, else appends).
fn map_set(m: &mut StrMap, k: &str, v: &str) {
    match m.iter_mut().find(|(mk, _)| mk == k) {
        Some(e) => e.1 = v.to_string(),
        None => m.push((k.to_string(), v.to_string())),
    }
}

#[test]
fn test_pod_discovery_with_updated_namespace_metadata() {
    let ns = "test-ns";
    let ns_labels = smap(&[("app", "api"), ("tier", "backend")]);
    let ns_annotations = smap(&[("maintainer", "platform"), ("build", "v4.5.6")]);

    let mut namespace = make_namespace(ns, ns_labels, ns_annotations);
    let (n, c) = make_discovery_with_metadata(
        Role::POD,
        NamespaceDiscovery::default(),
        AttachMetadataConfig { namespace: true, ..Default::default() },
        vec![Object::Namespace(namespace.clone()), Object::Pod(make_pods(ns))],
    );

    K8sDiscoveryTest::new(n)
        .after_start(move || {
            map_set(&mut namespace.labels, "app", "service");
            map_set(&mut namespace.labels, "zone", "us-east");
            map_set(&mut namespace.annotations, "maintainer", "sre");
            map_set(&mut namespace.annotations, "deployment", "canary");
            let _ = c.update("", Object::Namespace(namespace));
        })
        .expected_max_items(2)
        .expected_res(expect(vec![group(
            vec![testcontainer_target("1.2.3.4:9000")],
            ls(&[
                ("__meta_kubernetes_namespace", ns),
                ("__meta_kubernetes_namespace_annotation_maintainer", "sre"),
                ("__meta_kubernetes_namespace_annotationpresent_maintainer", "true"),
                ("__meta_kubernetes_namespace_annotation_build", "v4.5.6"),
                ("__meta_kubernetes_namespace_annotationpresent_build", "true"),
                ("__meta_kubernetes_namespace_annotation_deployment", "canary"),
                ("__meta_kubernetes_namespace_annotationpresent_deployment", "true"),
                ("__meta_kubernetes_namespace_label_app", "service"),
                ("__meta_kubernetes_namespace_labelpresent_app", "true"),
                ("__meta_kubernetes_namespace_label_tier", "backend"),
                ("__meta_kubernetes_namespace_labelpresent_tier", "true"),
                ("__meta_kubernetes_namespace_label_zone", "us-east"),
                ("__meta_kubernetes_namespace_labelpresent_zone", "true"),
                ("__meta_kubernetes_pod_name", "testpod"),
                ("__meta_kubernetes_pod_ip", "1.2.3.4"),
                ("__meta_kubernetes_pod_ready", "true"),
                ("__meta_kubernetes_pod_phase", "Running"),
                ("__meta_kubernetes_pod_node_name", "testnode"),
                ("__meta_kubernetes_pod_host_ip", "2.3.4.5"),
                ("__meta_kubernetes_pod_uid", "abc123"),
            ]),
            &format!("pod/{ns}/testpod"),
        )]))
        .run();
}

#[test]
fn test_pod_discovery_with_node_selector() {
    let worker_node = make_node("worker-node", "10.0.0.1", "", smap(&[("node-type", "worker")]), smap(&[]), Vec::new());
    let filtered_node = make_node("filtered-node", "10.0.0.2", "", smap(&[("node-type", "master")]), smap(&[]), Vec::new());

    let attach_metadata = AttachMetadataConfig {
        node: true, // necessary for node role selectors to work for pod role
        ..Default::default()
    };
    let (mut n, c) = make_discovery_with_metadata(Role::POD, NamespaceDiscovery::default(), attach_metadata, vec![worker_node, filtered_node]);
    n.selectors = RoleSelector { node: ResourceSelector { label: "node-type=worker".to_string(), ..Default::default() }, ..Default::default() };

    let mut pod_on_worker = make_pods("default");
    pod_on_worker.meta.name = "pod-on-worker".to_string();
    pod_on_worker.meta.uid = "worker-pod-123".to_string();
    pod_on_worker.spec.node_name = "worker-node".to_string();
    pod_on_worker.status.pod_ip = "192.168.1.1".to_string();

    let mut pod_on_filtered_node = make_pods("default");
    pod_on_filtered_node.meta.name = "pod-on-filtered-node".to_string();
    pod_on_filtered_node.meta.uid = "filtered-pod-456".to_string();
    pod_on_filtered_node.spec.node_name = "filtered-node".to_string();
    pod_on_filtered_node.status.pod_ip = "192.168.1.2".to_string();

    K8sDiscoveryTest::new(n)
        .before_run(move || {
            let _ = c.create("default", Object::Pod(pod_on_worker));
            let _ = c.create("default", Object::Pod(pod_on_filtered_node));
        })
        .expected_max_items(2)
        .expected_res(expect(vec![
            group(
                vec![testcontainer_target("192.168.1.1:9000")],
                ls(&[
                    ("__meta_kubernetes_namespace", "default"),
                    ("__meta_kubernetes_pod_name", "pod-on-worker"),
                    ("__meta_kubernetes_pod_ip", "192.168.1.1"),
                    ("__meta_kubernetes_pod_ready", "true"),
                    ("__meta_kubernetes_pod_phase", "Running"),
                    ("__meta_kubernetes_pod_node_name", "worker-node"),
                    ("__meta_kubernetes_pod_host_ip", "2.3.4.5"),
                    ("__meta_kubernetes_pod_uid", "worker-pod-123"),
                    ("__meta_kubernetes_node_name", "worker-node"),
                    ("__meta_kubernetes_node_label_node_type", "worker"),
                    ("__meta_kubernetes_node_labelpresent_node_type", "true"),
                ]),
                "pod/default/pod-on-worker",
            ),
            empty_group("pod/default/pod-on-filtered-node"),
        ]))
        .run();
}

fn make_replica_set(name: &str, namespace: &str, deployment_name: &str, deployment_uid: &str) -> ObjectMeta {
    ObjectMeta { name: name.to_string(), namespace: namespace.to_string(), uid: "rs123".to_string(), owner_references: vec![owner("Deployment", deployment_name, deployment_uid)], ..Default::default() }
}

/// Go `makeDeployment`: only the name and UID are used (Prometheus does not watch Deployments).
fn make_deployment(name: &str, namespace: &str) -> ObjectMeta {
    ObjectMeta { name: name.to_string(), namespace: namespace.to_string(), uid: "deploy123".to_string(), ..Default::default() }
}

fn make_job(name: &str, namespace: &str, cron_job_name: &str, cron_job_uid: &str) -> ObjectMeta {
    let mut job = ObjectMeta { name: name.to_string(), namespace: namespace.to_string(), uid: "job123".to_string(), ..Default::default() };
    if !cron_job_name.is_empty() {
        job.owner_references = vec![owner("CronJob", cron_job_name, cron_job_uid)];
    }
    job
}

/// Go `makeCronJob`: only the name and UID are used (Prometheus does not watch CronJobs).
fn make_cron_job(name: &str, namespace: &str) -> ObjectMeta {
    ObjectMeta { name: name.to_string(), namespace: namespace.to_string(), uid: "cronjob123".to_string(), ..Default::default() }
}

fn make_owned_pod(namespace: &str, kind: &str, owner_name: &str, owner_uid: &str) -> Pod {
    let mut p = make_pods(namespace);
    p.meta.uid = "pod123".to_string();
    p.meta.owner_references = vec![owner(kind, owner_name, owner_uid)];
    p
}

fn make_replica_set_owned_pod(namespace: &str, replica_set_name: &str, replica_set_uid: &str) -> Pod {
    make_owned_pod(namespace, "ReplicaSet", replica_set_name, replica_set_uid)
}

fn make_job_owned_pod(namespace: &str, job_name: &str, job_uid: &str) -> Pod {
    make_owned_pod(namespace, "Job", job_name, job_uid)
}

fn owned_pod_group(controller_kind: &str, controller_name: &str, extra: (&str, &str)) -> BTreeMap<String, Group> {
    expect(vec![group(
        vec![testcontainer_target("1.2.3.4:9000")],
        ls(&[
            ("__meta_kubernetes_pod_name", "testpod"),
            ("__meta_kubernetes_namespace", "default"),
            ("__meta_kubernetes_pod_node_name", "testnode"),
            ("__meta_kubernetes_pod_ip", "1.2.3.4"),
            ("__meta_kubernetes_pod_host_ip", "2.3.4.5"),
            ("__meta_kubernetes_pod_ready", "true"),
            ("__meta_kubernetes_pod_phase", "Running"),
            ("__meta_kubernetes_pod_uid", "pod123"),
            ("__meta_kubernetes_pod_controller_kind", controller_kind),
            ("__meta_kubernetes_pod_controller_name", controller_name),
            extra,
        ]),
        "pod/default/testpod",
    )])
}

fn pod_meta(deployment: bool, job: bool, cron_job: bool) -> AttachMetadataConfig {
    AttachMetadataConfig { pod_metadata: PodMetadataConfig { deployment, job, cron_job }, ..Default::default() }
}

// adapted: Go also adds the Deployment / CronJob objects to the fake clientset. Prometheus never lists or watches
// those kinds (the names come from the owner references of the ReplicaSet / Job), so the Rust fake has no such
// kinds and they are left out.

#[test]
fn test_pod_discovery_with_deployment() {
    let deployment = make_deployment("testdeployment", "default");
    let replica_set = make_replica_set("testdeployment-rs-abc", "default", &deployment.name, &deployment.uid);
    let pod = make_replica_set_owned_pod("default", &replica_set.name, &replica_set.uid);

    let (n, _) = make_discovery_with_metadata(Role::POD, NamespaceDiscovery::default(), pod_meta(true, false, false), vec![Object::Pod(pod), Object::ReplicaSet(replica_set)]);

    K8sDiscoveryTest::new(n)
        .expected_max_items(1)
        .expected_res(owned_pod_group("ReplicaSet", "testdeployment-rs-abc", ("__meta_kubernetes_pod_deployment_name", "testdeployment")))
        .run();
}

#[test]
fn test_pod_discovery_with_job() {
    let job = make_job("testjob", "default", "", "");
    let pod = make_job_owned_pod("default", &job.name, &job.uid);

    let (n, _) = make_discovery_with_metadata(Role::POD, NamespaceDiscovery::default(), pod_meta(false, true, false), vec![Object::Pod(pod), Object::Job(job)]);

    K8sDiscoveryTest::new(n).expected_max_items(1).expected_res(owned_pod_group("Job", "testjob", ("__meta_kubernetes_pod_job_name", "testjob"))).run();
}

#[test]
fn test_pod_discovery_with_cron_job() {
    let cron_job = make_cron_job("testcronjob", "default");
    let job = make_job("testcronjob-job-abc", "default", &cron_job.name, &cron_job.uid);
    let pod = make_job_owned_pod("default", &job.name, &job.uid);

    let (n, _) = make_discovery_with_metadata(Role::POD, NamespaceDiscovery::default(), pod_meta(false, true, false), vec![Object::Pod(pod), Object::Job(job)]);

    K8sDiscoveryTest::new(n)
        .expected_max_items(1)
        .expected_res(owned_pod_group("Job", "testcronjob-job-abc", ("__meta_kubernetes_pod_job_name", "testcronjob-job-abc")))
        .run();
}

#[test]
fn test_pod_discovery_with_deployment_update() {
    let mut deployment = make_deployment("testdeployment", "default");
    deployment.labels = smap(&[("version", "v1")]);

    let replica_set = make_replica_set("testdeployment-rs-abc", "default", &deployment.name, &deployment.uid);
    let pod = make_replica_set_owned_pod("default", &replica_set.name, &replica_set.uid);

    let (n, _c) = make_discovery_with_metadata(Role::POD, NamespaceDiscovery::default(), pod_meta(true, false, false), vec![Object::Pod(pod), Object::ReplicaSet(replica_set)]);

    K8sDiscoveryTest::new(n)
        .after_start(move || {
            // Update deployment with new labels.
            map_set(&mut deployment.labels, "version", "v2");
            map_set(&mut deployment.labels, "env", "prod");
            // adapted: Go updates the Deployment in the fake clientset; no informer watches Deployments, so the
            // update produces no event and the Rust fake has no Deployment kind.
            let _ = deployment;
        })
        .expected_max_items(2)
        .expected_res(owned_pod_group("ReplicaSet", "testdeployment-rs-abc", ("__meta_kubernetes_pod_deployment_name", "testdeployment")))
        .run();
}

#[test]
fn test_pod_discovery_with_job_update() {
    let mut job = make_job("testjob", "default", "", "");
    job.labels = smap(&[("batch", "v1")]);

    let pod = make_job_owned_pod("default", &job.name, &job.uid);

    let (n, c) = make_discovery_with_metadata(Role::POD, NamespaceDiscovery::default(), pod_meta(false, true, false), vec![Object::Pod(pod), Object::Job(job.clone())]);

    K8sDiscoveryTest::new(n)
        .after_start(move || {
            // Update job with new labels.
            map_set(&mut job.labels, "batch", "v2");
            map_set(&mut job.labels, "priority", "high");
            let ns = job.namespace.clone();
            let _ = c.update(&ns, Object::Job(job));
        })
        .expected_max_items(2)
        .expected_res(owned_pod_group("Job", "testjob", ("__meta_kubernetes_pod_job_name", "testjob")))
        .run();
}

#[test]
fn test_pod_discovery_with_cron_job_update() {
    let mut cron_job = make_cron_job("testcronjob", "default");
    cron_job.labels = smap(&[("schedule", "hourly")]);

    let job = make_job("testcronjob-job-abc", "default", &cron_job.name, &cron_job.uid);
    let pod = make_job_owned_pod("default", &job.name, &job.uid);

    let (n, _c) = make_discovery_with_metadata(Role::POD, NamespaceDiscovery::default(), pod_meta(false, false, true), vec![Object::Pod(pod), Object::Job(job)]);

    K8sDiscoveryTest::new(n)
        .after_start(move || {
            // Update cronjob with new labels.
            map_set(&mut cron_job.labels, "schedule", "daily");
            map_set(&mut cron_job.labels, "retention", "7d");
            // adapted: Go updates the CronJob in the fake clientset; no informer watches CronJobs, so the update
            // produces no event and the Rust fake has no CronJob kind.
            let _ = cron_job;
        })
        .expected_max_items(2)
        .expected_res(owned_pod_group("Job", "testcronjob-job-abc", ("__meta_kubernetes_pod_cronjob_name", "testcronjob")))
        .run();
}
