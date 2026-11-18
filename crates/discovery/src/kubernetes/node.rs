//! Port of `discovery/kubernetes/node.go`.
use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use model::labelset::{ADDRESS_LABEL, LabelSet};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::CounterVec;

use super::api::{Node as ApiNode, Object};
use super::informer::{Event, Informer, WorkQueue, split_meta_namespace_key};
use super::{Process, Role, RoleDiscoverer, add_object_meta_labels, event_counter, join_host_port, run_queue};

pub const NODE_LEGACY_HOST_IP: &str = "LegacyHostIP";
pub const NODE_PROVIDER_ID_LABEL: &str = "__meta_kubernetes_node_provider_id";
pub const NODE_CONDITION_PREFIX: &str = "__meta_kubernetes_node_condition_";
pub const NODE_ADDRESS_PREFIX: &str = "__meta_kubernetes_node_address_";

/// Go `Node`: discovers Kubernetes nodes.
pub struct Node {
    logger: Logger,
    informer: Arc<Informer>,
    queue: Arc<WorkQueue>,
}

impl Node {
    /// Go `NewNode`.
    pub fn new(logger: Logger, inf: Arc<Informer>, event_count: &CounterVec) -> Arc<Node> {
        let add = event_counter(event_count, Role::NODE, super::METRIC_LABEL_ROLE_ADD);
        let update = event_counter(event_count, Role::NODE, super::METRIC_LABEL_ROLE_UPDATE);
        let delete = event_counter(event_count, Role::NODE, super::METRIC_LABEL_ROLE_DELETE);
        let n = Arc::new(Node { logger, informer: inf, queue: Arc::new(WorkQueue::new()) });
        let q = n.queue.clone();
        n.informer.add_event_handler(Arc::new(move |e: &Event| {
            match e {
                Event::Add(_) => add.inc(),
                Event::Update(..) => update.inc(),
                Event::Delete { .. } => delete.inc(),
            }
            q.add(e.key());
        }));
        n
    }

    pub fn informer(&self) -> &Arc<Informer> {
        &self.informer
    }

    fn process(&self, key: &str) -> Process {
        let Ok((_, name)) = split_meta_namespace_key(key) else { return Process::Skip };
        let Some(o) = self.informer.get(key) else {
            return Process::Send(LabelSet::new(), Vec::new(), node_source_from_name(name));
        };
        let Object::Node(node) = o.as_ref() else {
            self.logger.error("converting to Node object failed", &[("err", &format!("received unexpected object: {o:?}"))]);
            return Process::Skip;
        };
        match self.build_node(node) {
            Some((labels, targets, source)) => Process::Send(labels, targets, source),
            None => Process::Skip,
        }
    }

    /// Go `buildNode` (None when the node has no address).
    pub fn build_node(&self, node: &ApiNode) -> Option<(LabelSet, Vec<LabelSet>, String)> {
        let source = node_source_from_name(&node.meta.name);
        let labels = node_labels(node);
        let (addr, addr_map) = match node_address(node) {
            Ok(a) => a,
            Err(e) => {
                self.logger.warn("No node address found", &[("err", &e)]);
                return None;
            }
        };
        let addr = join_host_port(&addr, &node.kubelet_port.to_string());
        let mut t = LabelSet::new();
        t.0.insert(ADDRESS_LABEL.to_string(), addr);
        t.0.insert("instance".to_string(), node.meta.name.clone());
        for (ty, a) in &addr_map {
            let ln = crate::sanitize_label_name(&format!("{NODE_ADDRESS_PREFIX}{ty}"));
            t.0.insert(ln, a[0].clone());
        }
        Some((labels, vec![t], source))
    }
}

impl RoleDiscoverer for Node {
    fn run(&self, ctx: &CancelToken, up: Sender) {
        run_queue(ctx, &up, &self.queue, &[&self.informer], &self.logger, "node", |k| self.process(k));
    }
    fn has_synced(&self) -> bool {
        self.informer.has_synced()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub fn node_source_from_name(name: &str) -> String {
    format!("node/{name}")
}

/// Go `nodeLabels`.
pub fn node_labels(n: &ApiNode) -> LabelSet {
    let mut ls = LabelSet::new();
    ls.0.insert(NODE_PROVIDER_ID_LABEL.to_string(), n.provider_id.clone());
    for c in &n.conditions {
        let ct = c.type_.to_lowercase();
        ls.0.insert(format!("{NODE_CONDITION_PREFIX}{}", crate::sanitize_label_name(&ct)), c.status.to_lowercase());
    }
    add_object_meta_labels(&mut ls, &n.meta, Role::NODE);
    ls
}

/// Go `nodeAddress`: the address by priority InternalIP, InternalDNS, ExternalIP, ExternalDNS, LegacyHostIP,
/// Hostname, and all addresses by type.
pub fn node_address(node: &ApiNode) -> Result<(String, BTreeMap<String, Vec<String>>), String> {
    let mut m: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for a in &node.addresses {
        m.entry(a.type_.clone()).or_default().push(a.address.clone());
    }
    for t in ["InternalIP", "InternalDNS", "ExternalIP", "ExternalDNS", NODE_LEGACY_HOST_IP, "Hostname"] {
        if let Some(a) = m.get(t) {
            return Ok((a[0].clone(), m));
        }
    }
    Err("host address unknown".to_string())
}
