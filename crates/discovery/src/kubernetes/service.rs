//! Port of `discovery/kubernetes/service.go`.
use std::any::Any;
use std::sync::Arc;

use model::labelset::{ADDRESS_LABEL, LabelSet};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::CounterVec;

use super::api::{Object, Service as ApiService};
use super::informer::{Event, Informer, NAMESPACE_INDEX, WorkQueue, split_meta_namespace_key};
use super::*;

pub const SERVICE_PORT_NAME_LABEL: &str = "__meta_kubernetes_service_port_name";
pub const SERVICE_PORT_NUMBER_LABEL: &str = "__meta_kubernetes_service_port_number";
pub const SERVICE_PORT_PROTOCOL_LABEL: &str = "__meta_kubernetes_service_port_protocol";
pub const SERVICE_CLUSTER_IP_LABEL: &str = "__meta_kubernetes_service_cluster_ip";
pub const SERVICE_LOAD_BALANCER_IP: &str = "__meta_kubernetes_service_loadbalancer_ip";
pub const SERVICE_EXTERNAL_NAME_LABEL: &str = "__meta_kubernetes_service_external_name";
pub const SERVICE_TYPE: &str = "__meta_kubernetes_service_type";

/// Go `Service`: discovers Kubernetes services.
pub struct Service {
    logger: Logger,
    informer: Arc<Informer>,
    queue: Arc<WorkQueue>,
    namespace_inf: Option<Arc<Informer>>,
}

impl Service {
    /// Go `NewService`.
    pub fn new(logger: Logger, inf: Arc<Informer>, namespace: Option<Arc<Informer>>, event_count: &CounterVec) -> Arc<Service> {
        let add = event_counter(event_count, Role::SERVICE, METRIC_LABEL_ROLE_ADD);
        let update = event_counter(event_count, Role::SERVICE, METRIC_LABEL_ROLE_UPDATE);
        let delete = event_counter(event_count, Role::SERVICE, METRIC_LABEL_ROLE_DELETE);
        let s = Arc::new(Service { logger, informer: inf, queue: Arc::new(WorkQueue::new()), namespace_inf: namespace });
        let q = s.queue.clone();
        s.informer.add_event_handler(Arc::new(move |e: &Event| {
            match e {
                Event::Add(_) => add.inc(),
                Event::Update(..) => update.inc(),
                Event::Delete { .. } => delete.inc(),
            }
            q.add(e.key());
        }));
        if let Some(ns_inf) = &s.namespace_inf {
            let (inf, q, l) = (s.informer.clone(), s.queue.clone(), s.logger.clone());
            ns_inf.add_event_handler(Arc::new(move |e: &Event| {
                if let Event::Update(_, o) = e {
                    let ns = &o.meta().name;
                    match inf.by_index(NAMESPACE_INDEX, ns) {
                        Ok(objs) => objs.iter().for_each(|o| q.add(o.key())),
                        Err(err) => l.error("Error getting services in namespace", &[("namespace", ns), ("err", &err)]),
                    }
                }
            }));
        }
        s
    }

    pub fn informer(&self) -> &Arc<Informer> {
        &self.informer
    }

    fn process(&self, key: &str) -> Process {
        let Ok((namespace, name)) = split_meta_namespace_key(key) else { return Process::Skip };
        let Some(o) = self.informer.get(key) else {
            return Process::Send(LabelSet::new(), Vec::new(), service_source_from_namespace_and_name(namespace, name));
        };
        let Object::Service(svc) = o.as_ref() else {
            self.logger.error("converting to Service object failed", &[("err", &format!("received unexpected object: {o:?}"))]);
            return Process::Skip;
        };
        let (labels, targets, source) = self.build_service(svc);
        Process::Send(labels, targets, source)
    }

    /// Go `buildService`.
    pub fn build_service(&self, svc: &ApiService) -> (LabelSet, Vec<LabelSet>, String) {
        let source = service_source_from_namespace_and_name(&svc.meta.namespace, &svc.meta.name);
        let mut labels = service_labels(svc);
        if let Some(ns_inf) = &self.namespace_inf {
            labels = add_namespace_labels(labels, ns_inf, &svc.meta.namespace);
        }
        let mut targets = Vec::with_capacity(svc.spec.ports.len());
        for port in &svc.spec.ports {
            let p = port.port.to_string();
            let mut t = LabelSet::new();
            t.0.insert(ADDRESS_LABEL.to_string(), join_host_port(&format!("{}.{}.svc", svc.meta.name, svc.meta.namespace), &p));
            t.0.insert(SERVICE_PORT_NAME_LABEL.to_string(), port.name.clone());
            t.0.insert(SERVICE_PORT_NUMBER_LABEL.to_string(), p);
            t.0.insert(SERVICE_PORT_PROTOCOL_LABEL.to_string(), port.protocol.clone());
            t.0.insert(SERVICE_TYPE.to_string(), svc.spec.type_.clone());
            if svc.spec.type_ == "ExternalName" {
                t.0.insert(SERVICE_EXTERNAL_NAME_LABEL.to_string(), svc.spec.external_name.clone());
            } else {
                t.0.insert(SERVICE_CLUSTER_IP_LABEL.to_string(), svc.spec.cluster_ip.clone());
            }
            if svc.spec.type_ == "LoadBalancer" {
                t.0.insert(SERVICE_LOAD_BALANCER_IP.to_string(), load_balancer_ips(svc));
            }
            targets.push(t);
        }
        (labels, targets, source)
    }
}

impl RoleDiscoverer for Service {
    fn run(&self, ctx: &CancelToken, up: Sender) {
        let mut syncs: Vec<&Arc<Informer>> = vec![&self.informer];
        if let Some(i) = &self.namespace_inf {
            syncs.push(i);
        }
        run_queue(ctx, &up, &self.queue, &syncs, &self.logger, "service", |k| self.process(k));
    }
    fn has_synced(&self) -> bool {
        self.informer.has_synced()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub fn service_source_from_namespace_and_name(namespace: &str, name: &str) -> String {
    format!("svc/{namespace}/{name}")
}

/// Go `serviceLabels`.
pub fn service_labels(svc: &ApiService) -> LabelSet {
    let mut ls = LabelSet::new();
    ls.0.insert(NAMESPACE_LABEL.to_string(), svc.meta.namespace.clone());
    add_object_meta_labels(&mut ls, &svc.meta, Role::SERVICE);
    ls
}

/// Go `loadBalancerIPs`: the status ingress IPs joined with commas, else the deprecated spec.loadBalancerIP.
pub fn load_balancer_ips(svc: &ApiService) -> String {
    let ips: Vec<&str> = svc.load_balancer_ingress_ips.iter().filter(|ip| !ip.is_empty()).map(|s| s.as_str()).collect();
    if !ips.is_empty() {
        return ips.join(",");
    }
    svc.spec.load_balancer_ip.clone()
}
