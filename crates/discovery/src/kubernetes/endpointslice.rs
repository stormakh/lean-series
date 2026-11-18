//! Port of `discovery/kubernetes/endpointslice.go` (discovery/v1 EndpointSlices).
use std::any::Any;
use std::sync::Arc;

use model::labelset::{ADDRESS_LABEL, LabelSet};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::CounterVec;

use super::api::{Endpoint, EndpointSlice as ApiEndpointSlice, EndpointSlicePort, Object, ObjectReference, Service as ApiService};
use super::endpoints::uncovered_container_targets;
use super::informer::{Event, Informer, NAMESPACE_INDEX, WorkQueue, split_meta_namespace_key};
use super::service::service_labels;
use super::*;

/// Go `disv1.LabelServiceName`.
pub const LABEL_SERVICE_NAME: &str = "kubernetes.io/service-name";

pub const ENDPOINT_SLICE_ADDRESS_TYPE_LABEL: &str = "__meta_kubernetes_endpointslice_address_type";
pub const ENDPOINT_SLICE_PORT_NAME_LABEL: &str = "__meta_kubernetes_endpointslice_port_name";
pub const ENDPOINT_SLICE_PORT_PROTOCOL_LABEL: &str = "__meta_kubernetes_endpointslice_port_protocol";
pub const ENDPOINT_SLICE_PORT_LABEL: &str = "__meta_kubernetes_endpointslice_port";
pub const ENDPOINT_SLICE_PORT_APP_PROTOCOL: &str = "__meta_kubernetes_endpointslice_port_app_protocol";
pub const ENDPOINT_SLICE_ENDPOINT_CONDITIONS_READY_LABEL: &str = "__meta_kubernetes_endpointslice_endpoint_conditions_ready";
pub const ENDPOINT_SLICE_ENDPOINT_CONDITIONS_SERVING_LABEL: &str = "__meta_kubernetes_endpointslice_endpoint_conditions_serving";
pub const ENDPOINT_SLICE_ENDPOINT_CONDITIONS_TERMINATING_LABEL: &str = "__meta_kubernetes_endpointslice_endpoint_conditions_terminating";
pub const ENDPOINT_SLICE_ENDPOINT_ZONE_LABEL: &str = "__meta_kubernetes_endpointslice_endpoint_zone";
pub const ENDPOINT_SLICE_ENDPOINT_HOSTNAME_LABEL: &str = "__meta_kubernetes_endpointslice_endpoint_hostname";
pub const ENDPOINT_SLICE_ENDPOINT_NODENAME_LABEL: &str = "__meta_kubernetes_endpointslice_endpoint_node_name";
pub const ENDPOINT_SLICE_ADDRESS_TARGET_KIND_LABEL: &str = "__meta_kubernetes_endpointslice_address_target_kind";
pub const ENDPOINT_SLICE_ADDRESS_TARGET_NAME_LABEL: &str = "__meta_kubernetes_endpointslice_address_target_name";
pub const ENDPOINT_SLICE_ENDPOINT_TOPOLOGY_LABEL_PREFIX: &str = "__meta_kubernetes_endpointslice_endpoint_topology_";
pub const ENDPOINT_SLICE_ENDPOINT_TOPOLOGY_LABEL_PRESENT_PREFIX: &str = "__meta_kubernetes_endpointslice_endpoint_topology_present_";

/// Go `EndpointSlice`: discovers new endpoint targets.
pub struct EndpointSlice {
    logger: Logger,
    endpoint_slice_inf: Arc<Informer>,
    service_inf: Arc<Informer>,
    pod_inf: Arc<Informer>,
    meta: MetadataInformers,
    queue: Arc<WorkQueue>,
}

fn enqueue_index(inf: &Informer, q: &WorkQueue, l: &Logger, index: &str, value: &str, msg: &str, what: &str) {
    match inf.by_index(index, value) {
        Ok(objs) => objs.iter().for_each(|o| q.add(o.key())),
        Err(e) => l.error(msg, &[(what, value), ("err", &e)]),
    }
}

impl EndpointSlice {
    /// Go `NewEndpointSlice`.
    pub fn new(logger: Logger, eps: Arc<Informer>, svc: Arc<Informer>, pod: Arc<Informer>, meta: MetadataInformers, event_count: &CounterVec) -> Arc<EndpointSlice> {
        let add = event_counter(event_count, Role::ENDPOINT_SLICE, METRIC_LABEL_ROLE_ADD);
        let update = event_counter(event_count, Role::ENDPOINT_SLICE, METRIC_LABEL_ROLE_UPDATE);
        let delete = event_counter(event_count, Role::ENDPOINT_SLICE, METRIC_LABEL_ROLE_DELETE);
        let svc_add = event_counter(event_count, Role::SERVICE, METRIC_LABEL_ROLE_ADD);
        let svc_update = event_counter(event_count, Role::SERVICE, METRIC_LABEL_ROLE_UPDATE);
        let svc_delete = event_counter(event_count, Role::SERVICE, METRIC_LABEL_ROLE_DELETE);
        let e = Arc::new(EndpointSlice { logger, endpoint_slice_inf: eps, service_inf: svc, pod_inf: pod, meta, queue: Arc::new(WorkQueue::new()) });

        let q = e.queue.clone();
        e.endpoint_slice_inf.add_event_handler(Arc::new(move |ev: &Event| {
            match ev {
                Event::Add(_) => add.inc(),
                Event::Update(..) => update.inc(),
                Event::Delete { .. } => delete.inc(),
            }
            q.add(ev.key());
        }));

        let (ei, q, l) = (e.endpoint_slice_inf.clone(), e.queue.clone(), e.logger.clone());
        e.service_inf.add_event_handler(Arc::new(move |ev: &Event| {
            let o = match ev {
                Event::Add(o) => {
                    svc_add.inc();
                    o
                }
                Event::Update(_, o) => {
                    svc_update.inc();
                    o
                }
                Event::Delete { obj, tombstone, .. } => {
                    svc_delete.inc();
                    if *tombstone {
                        l.error("converting to Service object failed", &[("err", "received unexpected object: tombstone")]);
                        return;
                    }
                    obj
                }
            };
            let m = o.meta();
            match ei.by_index(SERVICE_INDEX, &namespaced_name(&m.namespace, &m.name)) {
                Ok(objs) => objs.iter().for_each(|o| q.add(o.key())),
                Err(err) => l.error("getting endpoint slices by service name failed", &[("err", &err)]),
            }
        }));

        if let Some(node_inf) = &e.meta.node {
            let (ei, q, l) = (e.endpoint_slice_inf.clone(), e.queue.clone(), e.logger.clone());
            node_inf.add_event_handler(Arc::new(move |ev: &Event| {
                let name = match ev {
                    Event::Add(o) | Event::Update(_, o) => o.meta().name.clone(),
                    Event::Delete { key, .. } => key.to_string(),
                };
                enqueue_index(&ei, &q, &l, NODE_INDEX, &name, "Error getting endpoints for node", "node");
            }));
        }
        if let Some(ns_inf) = &e.meta.namespace {
            let (ei, q, l) = (e.endpoint_slice_inf.clone(), e.queue.clone(), e.logger.clone());
            ns_inf.add_event_handler(Arc::new(move |ev: &Event| {
                if let Event::Update(_, o) = ev {
                    enqueue_index(&ei, &q, &l, NAMESPACE_INDEX, &o.meta().name, "Error getting endpoints in namespace", "namespace");
                }
            }));
        }
        e
    }

    /// The main informer (Go `endpointSliceInf`).
    pub fn endpoint_slice_informer(&self) -> &Arc<Informer> {
        &self.endpoint_slice_inf
    }

    fn process(&self, key: &str) -> Process {
        let Ok((namespace, name)) = split_meta_namespace_key(key) else {
            self.logger.error("splitting key failed", &[("key", key)]);
            return Process::Skip;
        };
        let Some(o) = self.endpoint_slice_inf.get(key) else {
            return Process::Send(LabelSet::new(), Vec::new(), endpoint_slice_source_from_namespace_and_name(namespace, name));
        };
        let Object::EndpointSlice(es) = o.as_ref() else {
            self.logger.error("received unexpected object", &[("object", &format!("{o:?}"))]);
            return Process::Stop;
        };
        let (labels, targets, source) = self.build_endpoint_slice(es);
        Process::Send(labels, targets, source)
    }

    fn resolve_pod_ref(&self, r: Option<&ObjectReference>) -> Option<Arc<Object>> {
        let r = r?;
        if r.kind != "Pod" {
            return None;
        }
        self.pod_inf.get(&namespaced_name(&r.namespace, &r.name)).filter(|o| matches!(o.as_ref(), Object::Pod(_)))
    }

    /// Go `addServiceLabels`: merges the labels of the slice's service; returns the service.
    fn add_service_labels(&self, eps: &ApiEndpointSlice, labels: &mut LabelSet) -> Option<Arc<Object>> {
        let name = api::map_get(&eps.meta.labels, LABEL_SERVICE_NAME)?;
        let obj = self.service_inf.get(&namespaced_name(&eps.meta.namespace, name))?;
        let Object::Service(svc) = obj.as_ref() else { return None };
        *labels = labels.merge(&service_labels(svc));
        Some(obj)
    }

    /// Go `buildEndpointSlice`.
    pub fn build_endpoint_slice(&self, eps: &ApiEndpointSlice) -> (LabelSet, Vec<LabelSet>, String) {
        let source = endpoint_slice_source_from_namespace_and_name(&eps.meta.namespace, &eps.meta.name);
        let mut labels = LabelSet::new();
        labels.0.insert(NAMESPACE_LABEL.to_string(), eps.meta.namespace.clone());
        labels.0.insert(ENDPOINT_SLICE_ADDRESS_TYPE_LABEL.to_string(), eps.address_type.clone());
        add_object_meta_labels(&mut labels, &eps.meta, Role::ENDPOINT_SLICE);
        let svc = self.add_service_labels(eps, &mut labels);
        if let Some(ns_inf) = &self.meta.namespace {
            labels = add_namespace_labels(labels, ns_inf, &eps.meta.namespace);
        }
        let svc_ref = svc.as_ref().and_then(|o| if let Object::Service(s) = o.as_ref() { Some(s) } else { None });
        if non_primary_ip_family_slice(svc_ref, eps) {
            return (labels, Vec::new(), source);
        }

        let mut seen_pods: Vec<(String, Arc<Object>, Vec<EndpointSlicePort>)> = Vec::new();
        let mut targets: Vec<LabelSet> = Vec::new();

        let mut add = |addr: &str, ep: &Endpoint, port: &EndpointSlicePort| {
            let a = match port.port {
                Some(p) => join_host_port(addr, &format_port(p)),
                None => addr.to_string(),
            };
            let mut target = LabelSet::new();
            target.0.insert(ADDRESS_LABEL.to_string(), a);
            if let Some(n) = &port.name {
                target.0.insert(ENDPOINT_SLICE_PORT_NAME_LABEL.to_string(), n.clone());
            }
            if let Some(p) = &port.protocol {
                target.0.insert(ENDPOINT_SLICE_PORT_PROTOCOL_LABEL.to_string(), p.clone());
            }
            if let Some(p) = port.port {
                target.0.insert(ENDPOINT_SLICE_PORT_LABEL.to_string(), format_port(p));
            }
            if let Some(p) = &port.app_protocol {
                target.0.insert(ENDPOINT_SLICE_PORT_APP_PROTOCOL.to_string(), p.clone());
            }
            if let Some(b) = ep.conditions.ready {
                target.0.insert(ENDPOINT_SLICE_ENDPOINT_CONDITIONS_READY_LABEL.to_string(), b.to_string());
            }
            if let Some(b) = ep.conditions.serving {
                target.0.insert(ENDPOINT_SLICE_ENDPOINT_CONDITIONS_SERVING_LABEL.to_string(), b.to_string());
            }
            if let Some(b) = ep.conditions.terminating {
                target.0.insert(ENDPOINT_SLICE_ENDPOINT_CONDITIONS_TERMINATING_LABEL.to_string(), b.to_string());
            }
            if let Some(h) = &ep.hostname {
                target.0.insert(ENDPOINT_SLICE_ENDPOINT_HOSTNAME_LABEL.to_string(), h.clone());
            }
            if let Some(r) = &ep.target_ref {
                target.0.insert(ENDPOINT_SLICE_ADDRESS_TARGET_KIND_LABEL.to_string(), r.kind.clone());
                target.0.insert(ENDPOINT_SLICE_ADDRESS_TARGET_NAME_LABEL.to_string(), r.name.clone());
            }
            if let Some(n) = &ep.node_name {
                target.0.insert(ENDPOINT_SLICE_ENDPOINT_NODENAME_LABEL.to_string(), n.clone());
            }
            if let Some(z) = &ep.zone {
                target.0.insert(ENDPOINT_SLICE_ENDPOINT_ZONE_LABEL.to_string(), z.clone());
            }
            for (k, v) in &ep.deprecated_topology {
                let ln = crate::sanitize_label_name(k);
                target.0.insert(format!("{ENDPOINT_SLICE_ENDPOINT_TOPOLOGY_LABEL_PREFIX}{ln}"), v.clone());
                target.0.insert(format!("{ENDPOINT_SLICE_ENDPOINT_TOPOLOGY_LABEL_PRESENT_PREFIX}{ln}"), PRESENT_VALUE.to_string());
            }
            if let Some(node_inf) = &self.meta.node {
                match &ep.target_ref {
                    Some(r) if r.kind == "Node" => target = add_node_labels(target, node_inf, Some(&r.name)),
                    _ => target = add_node_labels(target, node_inf, ep.node_name.as_deref()),
                }
            }
            let Some(pod_obj) = self.resolve_pod_ref(ep.target_ref.as_ref()) else {
                // This target is not a Pod, so don't continue with Pod specific logic.
                targets.push(target);
                return;
            };
            let Object::Pod(pod) = pod_obj.as_ref() else { return };
            let s = namespaced_name(&pod.meta.namespace, &pod.meta.name);
            let idx = match seen_pods.iter().position(|(k, _, _)| *k == s) {
                Some(i) => i,
                None => {
                    seen_pods.push((s, pod_obj.clone(), Vec::new()));
                    seen_pods.len() - 1
                }
            };
            // Attach standard pod labels.
            target = target.merge(&pod_labels(pod, &self.meta));
            // Attach potential container port labels matching the endpoint port.
            let n = pod.spec.containers.len();
            for (i, c) in pod.spec.containers.iter().chain(pod.spec.init_containers.iter()).enumerate() {
                for cport in &c.ports {
                    let Some(p) = port.port else { continue };
                    if p == cport.container_port {
                        target.0.insert(POD_CONTAINER_NAME_LABEL.to_string(), c.name.clone());
                        target.0.insert(POD_CONTAINER_IMAGE_LABEL.to_string(), c.image.clone());
                        target.0.insert(POD_CONTAINER_PORT_NAME_LABEL.to_string(), cport.name.clone());
                        target.0.insert(POD_CONTAINER_PORT_NUMBER_LABEL.to_string(), format_port(p));
                        target.0.insert(POD_CONTAINER_PORT_PROTOCOL_LABEL.to_string(), cport.protocol.clone());
                        target.0.insert(POD_CONTAINER_IS_INIT.to_string(), (i >= n).to_string());
                        break;
                    }
                }
            }
            // Add service port so we know that we have already generated a target for it.
            seen_pods[idx].2.push(port.clone());
            targets.push(target);
        };

        for ep in &eps.endpoints {
            for port in &eps.ports {
                for addr in &ep.addresses {
                    add(addr, ep, port);
                }
            }
        }

        // For all seen pods, check all container ports. If they were not covered by one of the service
        // endpoints, generate targets for them.
        for (_, pod_obj, service_ports) in &seen_pods {
            let Object::Pod(pod) = pod_obj.as_ref() else { continue };
            // PodIP can be empty when a pod is starting or has been evicted.
            if pod.status.pod_ip.is_empty() {
                continue;
            }
            targets.extend(uncovered_container_targets(pod, &self.meta, |cp| service_ports.iter().any(|ep| ep.port == Some(cp))));
        }
        (labels, targets, source)
    }
}

impl RoleDiscoverer for EndpointSlice {
    fn run(&self, ctx: &CancelToken, up: Sender) {
        let mut syncs: Vec<&Arc<Informer>> = vec![&self.endpoint_slice_inf, &self.service_inf, &self.pod_inf];
        if let Some(i) = &self.meta.node {
            syncs.push(i);
        }
        if let Some(i) = &self.meta.namespace {
            syncs.push(i);
        }
        run_queue(ctx, &up, &self.queue, &syncs, &self.logger, "endpointslice", |k| self.process(k));
    }
    fn has_synced(&self) -> bool {
        self.endpoint_slice_inf.has_synced() && self.service_inf.has_synced() && self.pod_inf.has_synced()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub fn endpoint_slice_source_from_namespace_and_name(namespace: &str, name: &str) -> String {
    format!("endpointslice/{namespace}/{name}")
}

/// Go `nonPrimaryIPFamilySlice`: the secondary slice of a dual-stack service.
pub fn non_primary_ip_family_slice(svc: Option<&ApiService>, eps: &ApiEndpointSlice) -> bool {
    let Some(svc) = svc else { return false };
    let Some(policy) = &svc.spec.ip_family_policy else { return false };
    if policy != "PreferDualStack" && policy != "RequireDualStack" {
        return false;
    }
    match svc.spec.ip_families.first() {
        None => false,
        Some(f) => eps.address_type != *f,
    }
}
