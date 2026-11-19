//! Port of `discovery/kubernetes/endpoints.go` (the deprecated core/v1 Endpoints API).
use std::any::Any;
use std::sync::Arc;

use model::labelset::{ADDRESS_LABEL, LabelSet};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::CounterVec;

use super::api::{EndpointAddress, EndpointPort, Endpoints as ApiEndpoints, Object, ObjectReference, Pod as ApiPod};
use super::informer::{Event, Informer, NAMESPACE_INDEX, WorkQueue, split_meta_namespace_key};
use super::service::service_labels;
use super::*;

pub const ENDPOINT_NODE_NAME: &str = "__meta_kubernetes_endpoint_node_name";
pub const ENDPOINT_HOSTNAME: &str = "__meta_kubernetes_endpoint_hostname";
pub const ENDPOINT_READY_LABEL: &str = "__meta_kubernetes_endpoint_ready";
pub const ENDPOINT_PORT_NAME_LABEL: &str = "__meta_kubernetes_endpoint_port_name";
pub const ENDPOINT_PORT_PROTOCOL_LABEL: &str = "__meta_kubernetes_endpoint_port_protocol";
pub const ENDPOINT_ADDRESS_TARGET_KIND_LABEL: &str = "__meta_kubernetes_endpoint_address_target_kind";
pub const ENDPOINT_ADDRESS_TARGET_NAME_LABEL: &str = "__meta_kubernetes_endpoint_address_target_name";

/// Go `apiv1.EndpointsOverCapacity`.
pub const ENDPOINTS_OVER_CAPACITY: &str = "endpoints.kubernetes.io/over-capacity";

/// Go `Endpoints`: discovers new endpoint targets.
pub struct Endpoints {
    logger: Logger,
    endpoints_inf: Arc<Informer>,
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

impl Endpoints {
    /// Go `NewEndpoints`.
    pub fn new(logger: Logger, eps: Arc<Informer>, svc: Arc<Informer>, pod: Arc<Informer>, meta: MetadataInformers, event_count: &CounterVec) -> Arc<Endpoints> {
        let ep_add = event_counter(event_count, Role::ENDPOINT, METRIC_LABEL_ROLE_ADD);
        let ep_update = event_counter(event_count, Role::ENDPOINT, METRIC_LABEL_ROLE_UPDATE);
        let ep_delete = event_counter(event_count, Role::ENDPOINT, METRIC_LABEL_ROLE_DELETE);
        let svc_add = event_counter(event_count, Role::SERVICE, METRIC_LABEL_ROLE_ADD);
        let svc_update = event_counter(event_count, Role::SERVICE, METRIC_LABEL_ROLE_UPDATE);
        let svc_delete = event_counter(event_count, Role::SERVICE, METRIC_LABEL_ROLE_DELETE);
        let pod_update = event_counter(event_count, Role::POD, METRIC_LABEL_ROLE_UPDATE);
        let e = Arc::new(Endpoints { logger, endpoints_inf: eps, service_inf: svc, pod_inf: pod, meta, queue: Arc::new(WorkQueue::new()) });

        let q = e.queue.clone();
        e.endpoints_inf.add_event_handler(Arc::new(move |ev: &Event| {
            match ev {
                Event::Add(_) => ep_add.inc(),
                Event::Update(..) => ep_update.inc(),
                Event::Delete { .. } => ep_delete.inc(),
            }
            q.add(ev.key());
        }));

        let (ei, q, l) = (e.endpoints_inf.clone(), e.queue.clone(), e.logger.clone());
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
            if let Some(obj) = ei.get(&namespaced_name(&m.namespace, &m.name)) {
                q.add(obj.key());
            }
        }));

        let (ei, q, l) = (e.endpoints_inf.clone(), e.queue.clone(), e.logger.clone());
        e.pod_inf.add_event_handler(Arc::new(move |ev: &Event| {
            if let Event::Update(old, cur) = ev {
                pod_update.inc();
                let (Object::Pod(old), Object::Pod(cur)) = (old.as_ref(), cur.as_ref()) else { return };
                // the Pod's phase may change without an update of the Endpoints/Service
                // (https://github.com/prometheus/prometheus/issues/11305)
                if cur.status.phase != old.status.phase {
                    enqueue_index(&ei, &q, &l, POD_INDEX, &namespaced_name(&cur.meta.namespace, &cur.meta.name), "Error getting endpoints for pod", "pod");
                }
            }
        }));

        if let Some(node_inf) = &e.meta.node {
            let (ei, q, l) = (e.endpoints_inf.clone(), e.queue.clone(), e.logger.clone());
            node_inf.add_event_handler(Arc::new(move |ev: &Event| {
                let name = match ev {
                    Event::Add(o) | Event::Update(_, o) => o.meta().name.clone(),
                    Event::Delete { key, .. } => key.to_string(),
                };
                enqueue_index(&ei, &q, &l, NODE_INDEX, &name, "Error getting endpoints for node", "node");
            }));
        }
        if let Some(ns_inf) = &e.meta.namespace {
            let (ei, q, l) = (e.endpoints_inf.clone(), e.queue.clone(), e.logger.clone());
            ns_inf.add_event_handler(Arc::new(move |ev: &Event| {
                if let Event::Update(_, o) = ev {
                    enqueue_index(&ei, &q, &l, NAMESPACE_INDEX, &o.meta().name, "Error getting endpoints in namespace", "namespace");
                }
            }));
        }
        e
    }

    pub fn endpoints_informer(&self) -> &Arc<Informer> {
        &self.endpoints_inf
    }

    fn process(&self, key: &str) -> Process {
        let Ok((namespace, name)) = split_meta_namespace_key(key) else {
            self.logger.error("splitting key failed", &[("key", key)]);
            return Process::Skip;
        };
        let Some(o) = self.endpoints_inf.get(key) else {
            return Process::Send(LabelSet::new(), Vec::new(), endpoints_source_from_namespace_and_name(namespace, name));
        };
        let Object::Endpoints(eps) = o.as_ref() else {
            self.logger.error("converting to Endpoints object failed", &[("err", &format!("received unexpected object: {o:?}"))]);
            return Process::Skip;
        };
        let (labels, targets, source) = self.build_endpoints(eps);
        Process::Send(labels, targets, source)
    }

    fn resolve_pod_ref(&self, r: Option<&ObjectReference>) -> Option<Arc<Object>> {
        let r = r?;
        if r.kind != "Pod" {
            return None;
        }
        self.pod_inf.get(&namespaced_name(&r.namespace, &r.name)).filter(|o| matches!(o.as_ref(), Object::Pod(_)))
    }

    /// Go `buildEndpoints`.
    pub fn build_endpoints(&self, eps: &ApiEndpoints) -> (LabelSet, Vec<LabelSet>, String) {
        let source = endpoints_source_from_namespace_and_name(&eps.meta.namespace, &eps.meta.name);
        let mut labels = LabelSet::new();
        labels.0.insert(NAMESPACE_LABEL.to_string(), eps.meta.namespace.clone());
        if let Some(svc) = self.service_inf.get(&namespaced_name(&eps.meta.namespace, &eps.meta.name))
            && let Object::Service(svc) = svc.as_ref()
        {
            labels = labels.merge(&service_labels(svc));
        }
        add_object_meta_labels(&mut labels, &eps.meta, Role::ENDPOINT);
        if let Some(ns_inf) = &self.meta.namespace {
            labels = add_namespace_labels(labels, ns_inf, &eps.meta.namespace);
        }

        // the pods seen, in first-seen order (Go iterates a map), with the service ports covered
        let mut seen_pods: Vec<(String, Arc<Object>, Vec<EndpointPort>)> = Vec::new();
        let mut targets: Vec<LabelSet> = Vec::new();

        let mut add = |addr: &EndpointAddress, port: &EndpointPort, ready: &str| {
            let mut target = LabelSet::new();
            target.0.insert(ADDRESS_LABEL.to_string(), join_host_port(&addr.ip, &format_port(port.port)));
            target.0.insert(ENDPOINT_PORT_NAME_LABEL.to_string(), port.name.clone());
            target.0.insert(ENDPOINT_PORT_PROTOCOL_LABEL.to_string(), port.protocol.clone());
            target.0.insert(ENDPOINT_READY_LABEL.to_string(), ready.to_string());
            if let Some(r) = &addr.target_ref {
                target.0.insert(ENDPOINT_ADDRESS_TARGET_KIND_LABEL.to_string(), r.kind.clone());
                target.0.insert(ENDPOINT_ADDRESS_TARGET_NAME_LABEL.to_string(), r.name.clone());
            }
            if let Some(n) = &addr.node_name {
                target.0.insert(ENDPOINT_NODE_NAME.to_string(), n.clone());
            }
            if !addr.hostname.is_empty() {
                target.0.insert(ENDPOINT_HOSTNAME.to_string(), addr.hostname.clone());
            }
            if let Some(node_inf) = &self.meta.node {
                if let Some(n) = &addr.node_name {
                    target = add_node_labels(target, node_inf, Some(n));
                } else if let Some(r) = &addr.target_ref
                    && r.kind == "Node"
                {
                    target = add_node_labels(target, node_inf, Some(&r.name));
                }
            }
            let Some(pod_obj) = self.resolve_pod_ref(addr.target_ref.as_ref()) else {
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
                    if port.port != cport.container_port {
                        continue;
                    }
                    let ports = format_port(port.port);
                    target.0.insert(POD_CONTAINER_NAME_LABEL.to_string(), c.name.clone());
                    target.0.insert(POD_CONTAINER_IMAGE_LABEL.to_string(), c.image.clone());
                    target.0.insert(POD_CONTAINER_PORT_NAME_LABEL.to_string(), cport.name.clone());
                    target.0.insert(POD_CONTAINER_PORT_NUMBER_LABEL.to_string(), ports);
                    target.0.insert(POD_CONTAINER_PORT_PROTOCOL_LABEL.to_string(), port.protocol.clone());
                    target.0.insert(POD_CONTAINER_IS_INIT.to_string(), (i >= n).to_string());
                    // Go breaks only the inner loop: later containers may still match and overwrite
                    break;
                }
            }
            // Add service port so we know that we have already generated a target for it.
            seen_pods[idx].2.push(port.clone());
            targets.push(target);
        };

        for ss in &eps.subsets {
            for port in &ss.ports {
                for addr in &ss.addresses {
                    add(addr, port, "true");
                }
                // Although this generates the same target again, as it was generated in the loop above, it
                // causes the ready meta label to be overridden.
                for addr in &ss.not_ready_addresses {
                    add(addr, port, "false");
                }
            }
        }

        match api::map_get(&eps.meta.labels, ENDPOINTS_OVER_CAPACITY) {
            Some("truncated") => self.logger.warn("Number of endpoints in one Endpoints object exceeds 1000 and has been truncated, please use \"role: endpointslice\" instead", &[("endpoint", &eps.meta.name)]),
            Some("warning") => self.logger.warn("Number of endpoints in one Endpoints object exceeds 1000, please use \"role: endpointslice\" instead", &[("endpoint", &eps.meta.name)]),
            _ => {}
        }

        // For all seen pods, check all container ports. If they were not covered by one of the service
        // endpoints, generate targets for them.
        for (_, pod_obj, service_ports) in &seen_pods {
            let Object::Pod(pod) = pod_obj.as_ref() else { continue };
            // PodIP can be empty when a pod is starting or has been evicted.
            if pod.status.pod_ip.is_empty() {
                continue;
            }
            targets.extend(uncovered_container_targets(pod, &self.meta, |cp| service_ports.iter().any(|ep| ep.port == cp)));
        }
        (labels, targets, source)
    }
}

/// The targets of the container ports of a pod that no service endpoint covered.
pub(crate) fn uncovered_container_targets(pod: &ApiPod, meta: &MetadataInformers, seen: impl Fn(i32) -> bool) -> Vec<LabelSet> {
    let mut out = Vec::new();
    let n = pod.spec.containers.len();
    for (i, c) in pod.spec.containers.iter().chain(pod.spec.init_containers.iter()).enumerate() {
        for cport in &c.ports {
            if seen(cport.container_port) {
                continue;
            }
            let ports = format_port(cport.container_port);
            let mut t = LabelSet::new();
            t.0.insert(ADDRESS_LABEL.to_string(), join_host_port(&pod.status.pod_ip, &ports));
            t.0.insert(POD_CONTAINER_NAME_LABEL.to_string(), c.name.clone());
            t.0.insert(POD_CONTAINER_IMAGE_LABEL.to_string(), c.image.clone());
            t.0.insert(POD_CONTAINER_PORT_NAME_LABEL.to_string(), cport.name.clone());
            t.0.insert(POD_CONTAINER_PORT_NUMBER_LABEL.to_string(), ports);
            t.0.insert(POD_CONTAINER_PORT_PROTOCOL_LABEL.to_string(), cport.protocol.clone());
            t.0.insert(POD_CONTAINER_IS_INIT.to_string(), (i >= n).to_string());
            out.push(t.merge(&pod_labels(pod, meta)));
        }
    }
    out
}

impl RoleDiscoverer for Endpoints {
    fn run(&self, ctx: &CancelToken, up: Sender) {
        let mut syncs: Vec<&Arc<Informer>> = vec![&self.endpoints_inf, &self.service_inf, &self.pod_inf];
        if let Some(i) = &self.meta.node {
            syncs.push(i);
        }
        if let Some(i) = &self.meta.namespace {
            syncs.push(i);
        }
        run_queue(ctx, &up, &self.queue, &syncs, &self.logger, "endpoints", |k| self.process(k));
    }
    fn has_synced(&self) -> bool {
        self.endpoints_inf.has_synced() && self.service_inf.has_synced() && self.pod_inf.has_synced()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub fn endpoints_source_from_namespace_and_name(namespace: &str, name: &str) -> String {
    format!("endpoints/{namespace}/{name}")
}
