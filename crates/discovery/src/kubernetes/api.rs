//! The Kubernetes API objects that the discovery reads (k8s.io/api core/v1, discovery/v1, networking/v1, apps/v1,
//! batch/v1), with only the fields that Prometheus uses, and their JSON decoding.
//!
//! Decoding is lenient about types: a field of the wrong JSON type reads as its zero value (client-go fails the
//! whole object). Field names are matched case-sensitively, as client-go does.
use json::Json;

/// A Go `map[string]string` in JSON order (a repeated key keeps its last value).
pub type StrMap = Vec<(String, String)>;

/// Looks up a key in a `StrMap` (Go `m[k]` with the `ok` result).
pub fn map_get<'a>(m: &'a StrMap, k: &str) -> Option<&'a str> {
    m.iter().find(|(mk, _)| mk == k).map(|(_, v)| v.as_str())
}

/// metav1.OwnerReference (the fields used).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OwnerReference {
    pub kind: String,
    pub name: String,
    pub uid: String,
    pub controller: Option<bool>,
}

/// metav1.ObjectMeta (the fields used).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ObjectMeta {
    pub name: String,
    pub namespace: String,
    pub uid: String,
    pub resource_version: String,
    pub labels: StrMap,
    pub annotations: StrMap,
    pub owner_references: Vec<OwnerReference>,
}

/// core/v1 ObjectReference (the fields used).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ObjectReference {
    pub kind: String,
    pub namespace: String,
    pub name: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ContainerPort {
    pub name: String,
    pub container_port: i32,
    pub protocol: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Container {
    pub name: String,
    pub image: String,
    pub ports: Vec<ContainerPort>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PodSpec {
    pub node_name: String,
    pub containers: Vec<Container>,
    pub init_containers: Vec<Container>,
}

/// A condition of a pod or node: type and status.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Condition {
    pub type_: String,
    pub status: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ContainerStatus {
    pub name: String,
    pub container_id: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PodStatus {
    pub phase: String,
    pub conditions: Vec<Condition>,
    pub host_ip: String,
    pub pod_ip: String,
    pub container_statuses: Vec<ContainerStatus>,
    pub init_container_statuses: Vec<ContainerStatus>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Pod {
    pub meta: ObjectMeta,
    pub spec: PodSpec,
    pub status: PodStatus,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct NodeAddress {
    pub type_: String,
    pub address: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Node {
    pub meta: ObjectMeta,
    /// spec.providerID
    pub provider_id: String,
    pub conditions: Vec<Condition>,
    pub addresses: Vec<NodeAddress>,
    /// status.daemonEndpoints.kubeletEndpoint.Port
    pub kubelet_port: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ServicePort {
    pub name: String,
    pub protocol: String,
    pub port: i32,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ServiceSpec {
    pub ports: Vec<ServicePort>,
    pub cluster_ip: String,
    pub type_: String,
    pub external_name: String,
    pub load_balancer_ip: String,
    pub ip_families: Vec<String>,
    pub ip_family_policy: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Service {
    pub meta: ObjectMeta,
    pub spec: ServiceSpec,
    /// status.loadBalancer.ingress[].ip
    pub load_balancer_ingress_ips: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct EndpointAddress {
    pub ip: String,
    pub hostname: String,
    pub node_name: Option<String>,
    pub target_ref: Option<ObjectReference>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct EndpointPort {
    pub name: String,
    pub port: i32,
    pub protocol: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct EndpointSubset {
    pub addresses: Vec<EndpointAddress>,
    pub not_ready_addresses: Vec<EndpointAddress>,
    pub ports: Vec<EndpointPort>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Endpoints {
    pub meta: ObjectMeta,
    pub subsets: Vec<EndpointSubset>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct EndpointConditions {
    pub ready: Option<bool>,
    pub serving: Option<bool>,
    pub terminating: Option<bool>,
}

/// discovery/v1 Endpoint.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Endpoint {
    pub addresses: Vec<String>,
    pub conditions: EndpointConditions,
    pub hostname: Option<String>,
    pub target_ref: Option<ObjectReference>,
    pub deprecated_topology: StrMap,
    pub node_name: Option<String>,
    pub zone: Option<String>,
}

/// discovery/v1 EndpointPort.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EndpointSlicePort {
    pub name: Option<String>,
    pub protocol: Option<String>,
    pub port: Option<i32>,
    pub app_protocol: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct EndpointSlice {
    pub meta: ObjectMeta,
    pub address_type: String,
    pub endpoints: Vec<Endpoint>,
    pub ports: Vec<EndpointSlicePort>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct IngressRule {
    pub host: String,
    /// `http.paths[].path`; None when the rule has no `http` value.
    pub http_paths: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Ingress {
    pub meta: ObjectMeta,
    pub ingress_class_name: Option<String>,
    pub rules: Vec<IngressRule>,
    /// `tls[].hosts`
    pub tls_hosts: Vec<Vec<String>>,
}

/// One object of any watched kind. Namespaces, ReplicaSets and Jobs keep only their metadata.
#[derive(Clone, Debug, PartialEq)]
pub enum Object {
    Pod(Pod),
    Node(Node),
    Service(Service),
    Endpoints(Endpoints),
    EndpointSlice(EndpointSlice),
    Ingress(Ingress),
    Namespace(ObjectMeta),
    ReplicaSet(ObjectMeta),
    Job(ObjectMeta),
}

impl Object {
    pub fn meta(&self) -> &ObjectMeta {
        match self {
            Object::Pod(o) => &o.meta,
            Object::Node(o) => &o.meta,
            Object::Service(o) => &o.meta,
            Object::Endpoints(o) => &o.meta,
            Object::EndpointSlice(o) => &o.meta,
            Object::Ingress(o) => &o.meta,
            Object::Namespace(m) | Object::ReplicaSet(m) | Object::Job(m) => m,
        }
    }
    pub fn meta_mut(&mut self) -> &mut ObjectMeta {
        match self {
            Object::Pod(o) => &mut o.meta,
            Object::Node(o) => &mut o.meta,
            Object::Service(o) => &mut o.meta,
            Object::Endpoints(o) => &mut o.meta,
            Object::EndpointSlice(o) => &mut o.meta,
            Object::Ingress(o) => &mut o.meta,
            Object::Namespace(m) | Object::ReplicaSet(m) | Object::Job(m) => m,
        }
    }
    /// Go `cache.MetaNamespaceKeyFunc`: "namespace/name", or "name" for cluster objects.
    pub fn key(&self) -> String {
        let m = self.meta();
        if m.namespace.is_empty() { m.name.clone() } else { format!("{}/{}", m.namespace, m.name) }
    }
    pub fn kind(&self) -> Kind {
        match self {
            Object::Pod(_) => Kind::Pod,
            Object::Node(_) => Kind::Node,
            Object::Service(_) => Kind::Service,
            Object::Endpoints(_) => Kind::Endpoints,
            Object::EndpointSlice(_) => Kind::EndpointSlice,
            Object::Ingress(_) => Kind::Ingress,
            Object::Namespace(_) => Kind::Namespace,
            Object::ReplicaSet(_) => Kind::ReplicaSet,
            Object::Job(_) => Kind::Job,
        }
    }
}

/// A resource kind that the discovery lists and watches.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
    Pod,
    Node,
    Service,
    Endpoints,
    EndpointSlice,
    Ingress,
    Namespace,
    ReplicaSet,
    Job,
}

impl Kind {
    /// The API path prefix of the group version.
    pub fn group_version_path(self) -> &'static str {
        match self {
            Kind::Pod | Kind::Node | Kind::Service | Kind::Endpoints | Kind::Namespace => "/api/v1",
            Kind::EndpointSlice => "/apis/discovery.k8s.io/v1",
            Kind::Ingress => "/apis/networking.k8s.io/v1",
            Kind::ReplicaSet => "/apis/apps/v1",
            Kind::Job => "/apis/batch/v1",
        }
    }
    /// The resource name in URLs.
    pub fn resource(self) -> &'static str {
        match self {
            Kind::Pod => "pods",
            Kind::Node => "nodes",
            Kind::Service => "services",
            Kind::Endpoints => "endpoints",
            Kind::EndpointSlice => "endpointslices",
            Kind::Ingress => "ingresses",
            Kind::Namespace => "namespaces",
            Kind::ReplicaSet => "replicasets",
            Kind::Job => "jobs",
        }
    }
    pub fn namespaced(self) -> bool {
        !matches!(self, Kind::Node | Kind::Namespace)
    }
    /// The Go type, as in client-go messages ("*v1.Pod").
    pub fn type_description(self) -> &'static str {
        match self {
            Kind::Pod => "*v1.Pod",
            Kind::Node => "*v1.Node",
            Kind::Service => "*v1.Service",
            Kind::Endpoints => "*v1.Endpoints",
            Kind::EndpointSlice => "*v1.EndpointSlice",
            Kind::Ingress => "*v1.Ingress",
            Kind::Namespace => "*v1.Namespace",
            Kind::ReplicaSet => "*v1.ReplicaSet",
            Kind::Job => "*v1.Job",
        }
    }
    /// The URL path of the collection in a namespace ("" = all namespaces).
    pub fn path(self, namespace: &str) -> String {
        if self.namespaced() && !namespace.is_empty() {
            format!("{}/namespaces/{}/{}", self.group_version_path(), yaml::url::path_escape(namespace), self.resource())
        } else {
            format!("{}/{}", self.group_version_path(), self.resource())
        }
    }

    /// Decodes one object of this kind.
    pub fn decode(self, v: &Json) -> Object {
        match self {
            Kind::Pod => Object::Pod(decode_pod(v)),
            Kind::Node => Object::Node(decode_node(v)),
            Kind::Service => Object::Service(decode_service(v)),
            Kind::Endpoints => Object::Endpoints(decode_endpoints(v)),
            Kind::EndpointSlice => Object::EndpointSlice(decode_endpoint_slice(v)),
            Kind::Ingress => Object::Ingress(decode_ingress(v)),
            Kind::Namespace => Object::Namespace(decode_meta(v.get("metadata"))),
            Kind::ReplicaSet => Object::ReplicaSet(decode_meta(v.get("metadata"))),
            Kind::Job => Object::Job(decode_meta(v.get("metadata"))),
        }
    }
}

fn str_of(v: Option<&Json>, k: &str) -> String {
    v.and_then(|v| v.get(k)).and_then(|v| v.as_str()).unwrap_or("").to_string()
}

fn opt_str(v: &Json, k: &str) -> Option<String> {
    v.get(k).and_then(|v| v.as_str()).map(|s| s.to_string())
}

fn opt_bool(v: &Json, k: &str) -> Option<bool> {
    v.get(k).and_then(|v| v.as_bool())
}

fn int_of(v: Option<&Json>, k: &str) -> i32 {
    v.and_then(|v| v.get(k)).and_then(|v| v.as_i64()).unwrap_or(0) as i32
}

fn arr<'a>(v: Option<&'a Json>, k: &str) -> &'a [Json] {
    v.and_then(|v| v.get(k)).and_then(|v| v.as_arr()).map(|a| a.as_slice()).unwrap_or(&[])
}

fn strs(v: Option<&Json>, k: &str) -> Vec<String> {
    arr(v, k).iter().map(|s| s.as_str().unwrap_or("").to_string()).collect()
}

/// A JSON object of strings as a Go map (later duplicates replace earlier ones).
pub fn str_map(v: Option<&Json>) -> StrMap {
    let mut out: StrMap = Vec::new();
    if let Some(Json::Obj(o)) = v {
        for (k, v) in o {
            let v = v.as_str().unwrap_or("").to_string();
            match out.iter_mut().find(|(ok, _)| ok == k) {
                Some(e) => e.1 = v,
                None => out.push((k.clone(), v)),
            }
        }
    }
    out
}

pub fn decode_meta(m: Option<&Json>) -> ObjectMeta {
    ObjectMeta {
        name: str_of(m, "name"),
        namespace: str_of(m, "namespace"),
        uid: str_of(m, "uid"),
        resource_version: str_of(m, "resourceVersion"),
        labels: str_map(m.and_then(|m| m.get("labels"))),
        annotations: str_map(m.and_then(|m| m.get("annotations"))),
        owner_references: arr(m, "ownerReferences")
            .iter()
            .map(|r| OwnerReference { kind: str_of(Some(r), "kind"), name: str_of(Some(r), "name"), uid: str_of(Some(r), "uid"), controller: opt_bool(r, "controller") })
            .collect(),
    }
}

fn decode_object_ref(v: Option<&Json>) -> Option<ObjectReference> {
    let v = v.filter(|v| !v.is_null())?;
    Some(ObjectReference { kind: str_of(Some(v), "kind"), namespace: str_of(Some(v), "namespace"), name: str_of(Some(v), "name") })
}

fn decode_containers(v: Option<&Json>, k: &str) -> Vec<Container> {
    arr(v, k)
        .iter()
        .map(|c| Container {
            name: str_of(Some(c), "name"),
            image: str_of(Some(c), "image"),
            ports: arr(Some(c), "ports").iter().map(|p| ContainerPort { name: str_of(Some(p), "name"), container_port: int_of(Some(p), "containerPort"), protocol: str_of(Some(p), "protocol") }).collect(),
        })
        .collect()
}

fn decode_conditions(v: Option<&Json>) -> Vec<Condition> {
    arr(v, "conditions").iter().map(|c| Condition { type_: str_of(Some(c), "type"), status: str_of(Some(c), "status") }).collect()
}

fn decode_statuses(v: Option<&Json>, k: &str) -> Vec<ContainerStatus> {
    arr(v, k).iter().map(|c| ContainerStatus { name: str_of(Some(c), "name"), container_id: str_of(Some(c), "containerID") }).collect()
}

pub fn decode_pod(v: &Json) -> Pod {
    let spec = v.get("spec");
    let st = v.get("status");
    Pod {
        meta: decode_meta(v.get("metadata")),
        spec: PodSpec { node_name: str_of(spec, "nodeName"), containers: decode_containers(spec, "containers"), init_containers: decode_containers(spec, "initContainers") },
        status: PodStatus {
            phase: str_of(st, "phase"),
            conditions: decode_conditions(st),
            host_ip: str_of(st, "hostIP"),
            pod_ip: str_of(st, "podIP"),
            container_statuses: decode_statuses(st, "containerStatuses"),
            init_container_statuses: decode_statuses(st, "initContainerStatuses"),
        },
    }
}

pub fn decode_node(v: &Json) -> Node {
    let st = v.get("status");
    let kubelet = st.and_then(|s| s.get("daemonEndpoints")).and_then(|d| d.get("kubeletEndpoint"));
    Node {
        meta: decode_meta(v.get("metadata")),
        provider_id: str_of(v.get("spec"), "providerID"),
        conditions: decode_conditions(st),
        addresses: arr(st, "addresses").iter().map(|a| NodeAddress { type_: str_of(Some(a), "type"), address: str_of(Some(a), "address") }).collect(),
        kubelet_port: int_of(kubelet, "Port"),
    }
}

pub fn decode_service(v: &Json) -> Service {
    let spec = v.get("spec");
    let lb = v.get("status").and_then(|s| s.get("loadBalancer"));
    Service {
        meta: decode_meta(v.get("metadata")),
        spec: ServiceSpec {
            ports: arr(spec, "ports").iter().map(|p| ServicePort { name: str_of(Some(p), "name"), protocol: str_of(Some(p), "protocol"), port: int_of(Some(p), "port") }).collect(),
            cluster_ip: str_of(spec, "clusterIP"),
            type_: str_of(spec, "type"),
            external_name: str_of(spec, "externalName"),
            load_balancer_ip: str_of(spec, "loadBalancerIP"),
            ip_families: strs(spec, "ipFamilies"),
            ip_family_policy: spec.and_then(|s| opt_str(s, "ipFamilyPolicy")),
        },
        load_balancer_ingress_ips: arr(lb, "ingress").iter().map(|i| str_of(Some(i), "ip")).collect(),
    }
}

fn decode_addresses(v: Option<&Json>, k: &str) -> Vec<EndpointAddress> {
    arr(v, k)
        .iter()
        .map(|a| EndpointAddress { ip: str_of(Some(a), "ip"), hostname: str_of(Some(a), "hostname"), node_name: opt_str(a, "nodeName"), target_ref: decode_object_ref(a.get("targetRef")) })
        .collect()
}

pub fn decode_endpoints(v: &Json) -> Endpoints {
    Endpoints {
        meta: decode_meta(v.get("metadata")),
        subsets: arr(Some(v), "subsets")
            .iter()
            .map(|s| EndpointSubset {
                addresses: decode_addresses(Some(s), "addresses"),
                not_ready_addresses: decode_addresses(Some(s), "notReadyAddresses"),
                ports: arr(Some(s), "ports").iter().map(|p| EndpointPort { name: str_of(Some(p), "name"), port: int_of(Some(p), "port"), protocol: str_of(Some(p), "protocol") }).collect(),
            })
            .collect(),
    }
}

pub fn decode_endpoint_slice(v: &Json) -> EndpointSlice {
    EndpointSlice {
        meta: decode_meta(v.get("metadata")),
        address_type: str_of(Some(v), "addressType"),
        endpoints: arr(Some(v), "endpoints")
            .iter()
            .map(|e| {
                let c = e.get("conditions");
                Endpoint {
                    addresses: strs(Some(e), "addresses"),
                    conditions: EndpointConditions { ready: c.and_then(|c| opt_bool(c, "ready")), serving: c.and_then(|c| opt_bool(c, "serving")), terminating: c.and_then(|c| opt_bool(c, "terminating")) },
                    hostname: opt_str(e, "hostname"),
                    target_ref: decode_object_ref(e.get("targetRef")),
                    deprecated_topology: str_map(e.get("deprecatedTopology")),
                    node_name: opt_str(e, "nodeName"),
                    zone: opt_str(e, "zone"),
                }
            })
            .collect(),
        ports: arr(Some(v), "ports")
            .iter()
            .map(|p| EndpointSlicePort {
                name: opt_str(p, "name"),
                protocol: opt_str(p, "protocol"),
                port: p.get("port").and_then(|n| n.as_i64()).map(|n| n as i32),
                app_protocol: opt_str(p, "appProtocol"),
            })
            .collect(),
    }
}

pub fn decode_ingress(v: &Json) -> Ingress {
    let spec = v.get("spec");
    Ingress {
        meta: decode_meta(v.get("metadata")),
        ingress_class_name: spec.and_then(|s| opt_str(s, "ingressClassName")),
        rules: arr(spec, "rules")
            .iter()
            .map(|r| IngressRule {
                host: str_of(Some(r), "host"),
                http_paths: r.get("http").filter(|h| !h.is_null()).map(|h| arr(Some(h), "paths").iter().map(|p| str_of(Some(p), "path")).collect()),
            })
            .collect(),
        tls_hosts: arr(spec, "tls").iter().map(|t| strs(Some(t), "hosts")).collect(),
    }
}
