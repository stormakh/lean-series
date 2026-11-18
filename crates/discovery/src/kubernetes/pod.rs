//! Port of `discovery/kubernetes/pod.go`.
use std::any::Any;
use std::sync::Arc;

use model::labelset::{ADDRESS_LABEL, LabelSet};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::CounterVec;

use super::api::{ContainerStatus, Object, Pod as ApiPod};
use super::informer::{Event, Informer, NAMESPACE_INDEX, WorkQueue, split_meta_namespace_key};
use super::*;

/// Go `Pod`: discovers pod targets.
pub struct Pod {
    pod_inf: Arc<Informer>,
    meta: MetadataInformers,
    logger: Logger,
    queue: Arc<WorkQueue>,
}

fn enqueue_by_index(inf: &Informer, q: &WorkQueue, logger: &Logger, index: &str, value: &str, what: &str) {
    match inf.by_index(index, value) {
        Ok(objs) => {
            for o in objs {
                q.add(o.key());
            }
        }
        Err(e) => logger.error(what, &[(index, value), ("err", &e)]),
    }
}

impl Pod {
    /// Go `NewPod`.
    pub fn new(logger: Logger, pods: Arc<Informer>, meta: MetadataInformers, event_count: &CounterVec) -> Arc<Pod> {
        let add = event_counter(event_count, Role::POD, METRIC_LABEL_ROLE_ADD);
        let delete = event_counter(event_count, Role::POD, METRIC_LABEL_ROLE_DELETE);
        let update = event_counter(event_count, Role::POD, METRIC_LABEL_ROLE_UPDATE);
        let p = Arc::new(Pod { pod_inf: pods, meta, logger, queue: Arc::new(WorkQueue::new()) });
        let q = p.queue.clone();
        p.pod_inf.add_event_handler(Arc::new(move |e: &Event| {
            match e {
                Event::Add(_) => add.inc(),
                Event::Update(..) => update.inc(),
                Event::Delete { .. } => delete.inc(),
            }
            q.add(e.key());
        }));
        if let Some(node_inf) = &p.meta.node {
            let (pi, q, l) = (p.pod_inf.clone(), p.queue.clone(), p.logger.clone());
            node_inf.add_event_handler(Arc::new(move |e: &Event| {
                let name = match e {
                    Event::Add(o) | Event::Update(_, o) => o.meta().name.clone(),
                    Event::Delete { key, .. } => key.to_string(),
                };
                enqueue_by_index(&pi, &q, &l, NODE_INDEX, &name, "Error getting pods for node");
            }));
        }
        if let Some(ns_inf) = &p.meta.namespace {
            let (pi, q, l) = (p.pod_inf.clone(), p.queue.clone(), p.logger.clone());
            ns_inf.add_event_handler(Arc::new(move |e: &Event| {
                // Creation and deletion trigger events for the resources within the namespace.
                if let Event::Update(_, o) = e {
                    enqueue_by_index(&pi, &q, &l, NAMESPACE_INDEX, &o.meta().name, "Error getting pods in namespace");
                }
            }));
        }
        if p.meta.deployment
            && let Some(rs_inf) = &p.meta.replica_set
        {
            let (pi, q, l) = (p.pod_inf.clone(), p.queue.clone(), p.logger.clone());
            rs_inf.add_event_handler(Arc::new(move |e: &Event| {
                let o = match e {
                    Event::Add(o) | Event::Update(_, o) | Event::Delete { obj: o, .. } => o,
                };
                let m = o.meta();
                enqueue_by_index(&pi, &q, &l, REPLICA_SET_INDEX, &namespaced_name(&m.namespace, &m.name), "Error getting pods for replicaset");
            }));
        }
        if (p.meta.job_metadata || p.meta.cron_job_metadata)
            && let Some(job_inf) = &p.meta.job
        {
            let (pi, q, l) = (p.pod_inf.clone(), p.queue.clone(), p.logger.clone());
            job_inf.add_event_handler(Arc::new(move |e: &Event| {
                let o = match e {
                    Event::Add(o) | Event::Update(_, o) | Event::Delete { obj: o, .. } => o,
                };
                let m = o.meta();
                enqueue_by_index(&pi, &q, &l, JOB_INDEX, &namespaced_name(&m.namespace, &m.name), "Error getting pods for job");
            }));
        }
        p
    }

    pub fn pod_informer(&self) -> &Arc<Informer> {
        &self.pod_inf
    }

    fn process(&self, key: &str) -> Process {
        let Ok((namespace, name)) = split_meta_namespace_key(key) else { return Process::Skip };
        let Some(o) = self.pod_inf.get(key) else {
            return Process::Send(LabelSet::new(), Vec::new(), pod_source_from_namespace_and_name(namespace, name));
        };
        let Object::Pod(pod) = o.as_ref() else {
            self.logger.error("converting to Pod object failed", &[("err", &format!("received unexpected object: {o:?}"))]);
            return Process::Skip;
        };
        let (labels, targets, source) = self.build_pod(pod);
        Process::Send(labels, targets, source)
    }

    fn find_pod_container_id(&self, statuses: &[ContainerStatus], name: &str) -> String {
        match statuses.iter().find(|s| s.name == name) {
            Some(s) => s.container_id.clone(),
            None => {
                self.logger.debug("cannot find container ID", &[("err", &format!("cannot find container with name {name}"))]);
                String::new()
            }
        }
    }

    /// Go `buildPod`.
    pub fn build_pod(&self, pod: &ApiPod) -> (LabelSet, Vec<LabelSet>, String) {
        let source = pod_source_from_namespace_and_name(&pod.meta.namespace, &pod.meta.name);
        // PodIP can be empty when a pod is starting or has been evicted.
        if pod.status.pod_ip.is_empty() {
            return (LabelSet::new(), Vec::new(), source);
        }
        // Filter out pods scheduled on nodes that are not in the node store, as these were filtered out by
        // node selectors.
        if let Some(node_inf) = &self.meta.node
            && node_inf.get(&pod.spec.node_name).is_none()
        {
            return (LabelSet::new(), Vec::new(), source);
        }
        let mut labels = pod_labels(pod, &self.meta);
        labels.0.insert(NAMESPACE_LABEL.to_string(), pod.meta.namespace.clone());
        if let Some(node_inf) = &self.meta.node {
            labels = add_node_labels(labels, node_inf, Some(&pod.spec.node_name));
        }
        if let Some(ns_inf) = &self.meta.namespace {
            labels = add_namespace_labels(labels, ns_inf, &pod.meta.namespace);
        }
        let mut targets = Vec::new();
        let n = pod.spec.containers.len();
        for (i, c) in pod.spec.containers.iter().chain(pod.spec.init_containers.iter()).enumerate() {
            let is_init = i >= n;
            let statuses = if is_init { &pod.status.init_container_statuses } else { &pod.status.container_statuses };
            let cid = self.find_pod_container_id(statuses, &c.name);
            // If no ports are defined for the container, create an anonymous target per container.
            if c.ports.is_empty() {
                let mut t = LabelSet::new();
                t.0.insert(ADDRESS_LABEL.to_string(), pod.status.pod_ip.clone());
                t.0.insert(POD_CONTAINER_NAME_LABEL.to_string(), c.name.clone());
                t.0.insert(POD_CONTAINER_ID_LABEL.to_string(), cid);
                t.0.insert(POD_CONTAINER_IMAGE_LABEL.to_string(), c.image.clone());
                t.0.insert(POD_CONTAINER_IS_INIT.to_string(), is_init.to_string());
                targets.push(t);
                continue;
            }
            // Otherwise create one target for each container/port combination.
            for port in &c.ports {
                let ports = format_port(port.container_port);
                let mut t = LabelSet::new();
                t.0.insert(ADDRESS_LABEL.to_string(), join_host_port(&pod.status.pod_ip, &ports));
                t.0.insert(POD_CONTAINER_NAME_LABEL.to_string(), c.name.clone());
                t.0.insert(POD_CONTAINER_ID_LABEL.to_string(), cid.clone());
                t.0.insert(POD_CONTAINER_IMAGE_LABEL.to_string(), c.image.clone());
                t.0.insert(POD_CONTAINER_PORT_NUMBER_LABEL.to_string(), ports);
                t.0.insert(POD_CONTAINER_PORT_NAME_LABEL.to_string(), port.name.clone());
                t.0.insert(POD_CONTAINER_PORT_PROTOCOL_LABEL.to_string(), port.protocol.clone());
                t.0.insert(POD_CONTAINER_IS_INIT.to_string(), is_init.to_string());
                targets.push(t);
            }
        }
        (labels, targets, source)
    }
}

impl RoleDiscoverer for Pod {
    fn run(&self, ctx: &CancelToken, up: Sender) {
        let mut syncs: Vec<&Arc<Informer>> = vec![&self.pod_inf];
        if let Some(i) = &self.meta.node {
            syncs.push(i);
        }
        if let Some(i) = &self.meta.namespace {
            syncs.push(i);
        }
        if self.meta.deployment
            && let Some(i) = &self.meta.replica_set
        {
            syncs.push(i);
        }
        if (self.meta.job_metadata || self.meta.cron_job_metadata)
            && let Some(i) = &self.meta.job
        {
            syncs.push(i);
        }
        run_queue(ctx, &up, &self.queue, &syncs, &self.logger, "pod", |k| self.process(k));
    }
    fn has_synced(&self) -> bool {
        self.pod_inf.has_synced()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub fn pod_source_from_namespace_and_name(namespace: &str, name: &str) -> String {
    format!("pod/{}", namespaced_name(namespace, name))
}
