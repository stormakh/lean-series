//! Port of `discovery/kubernetes/ingress.go`.
use std::any::Any;
use std::sync::Arc;

use model::labelset::{ADDRESS_LABEL, LabelSet};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::CounterVec;

use super::api::{Ingress as ApiIngress, IngressRule, Object};
use super::informer::{Event, Informer, NAMESPACE_INDEX, WorkQueue, split_meta_namespace_key};
use super::*;

pub const INGRESS_SCHEME_LABEL: &str = "__meta_kubernetes_ingress_scheme";
pub const INGRESS_HOST_LABEL: &str = "__meta_kubernetes_ingress_host";
pub const INGRESS_PATH_LABEL: &str = "__meta_kubernetes_ingress_path";
pub const INGRESS_CLASS_NAME_LABEL: &str = "__meta_kubernetes_ingress_class_name";

/// Go `Ingress`: discovers Kubernetes ingresses.
pub struct Ingress {
    logger: Logger,
    informer: Arc<Informer>,
    queue: Arc<WorkQueue>,
    namespace_inf: Option<Arc<Informer>>,
}

impl Ingress {
    /// Go `NewIngress`.
    pub fn new(logger: Logger, inf: Arc<Informer>, namespace: Option<Arc<Informer>>, event_count: &CounterVec) -> Arc<Ingress> {
        let add = event_counter(event_count, Role::INGRESS, METRIC_LABEL_ROLE_ADD);
        let update = event_counter(event_count, Role::INGRESS, METRIC_LABEL_ROLE_UPDATE);
        let delete = event_counter(event_count, Role::INGRESS, METRIC_LABEL_ROLE_DELETE);
        let s = Arc::new(Ingress { logger, informer: inf, queue: Arc::new(WorkQueue::new()), namespace_inf: namespace });
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
                        Err(err) => l.error("Error getting ingresses in namespace", &[("namespace", ns), ("err", &err)]),
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
            return Process::Send(LabelSet::new(), Vec::new(), ingress_source_from_namespace_and_name(namespace, name));
        };
        let Object::Ingress(ing) = o.as_ref() else {
            self.logger.error("converting to Ingress object failed", &[("err", &format!("received unexpected object: {o:?}"))]);
            return Process::Skip;
        };
        let (labels, targets, source) = self.build_ingress(ing);
        Process::Send(labels, targets, source)
    }

    /// Go `buildIngress`.
    pub fn build_ingress(&self, ingress: &ApiIngress) -> (LabelSet, Vec<LabelSet>, String) {
        let source = ingress_source_from_namespace_and_name(&ingress.meta.namespace, &ingress.meta.name);
        let mut labels = ingress_labels(ingress);
        if let Some(ns_inf) = &self.namespace_inf {
            labels = add_namespace_labels(labels, ns_inf, &ingress.meta.namespace);
        }
        let tls: Vec<&String> = ingress.tls_hosts.iter().flatten().collect();
        let mut targets = Vec::new();
        for rule in &ingress.rules {
            let paths = paths_from_ingress_paths(rule_paths(rule));
            let scheme = if tls.iter().any(|p| matches_hostname_pattern(p, &rule.host)) { "https" } else { "http" };
            for path in paths {
                let mut t = LabelSet::new();
                t.0.insert(ADDRESS_LABEL.to_string(), rule.host.clone());
                t.0.insert(INGRESS_SCHEME_LABEL.to_string(), scheme.to_string());
                t.0.insert(INGRESS_HOST_LABEL.to_string(), rule.host.clone());
                t.0.insert(INGRESS_PATH_LABEL.to_string(), path);
                targets.push(t);
            }
        }
        (labels, targets, source)
    }
}

impl RoleDiscoverer for Ingress {
    fn run(&self, ctx: &CancelToken, up: Sender) {
        let mut syncs: Vec<&Arc<Informer>> = vec![&self.informer];
        if let Some(i) = &self.namespace_inf {
            syncs.push(i);
        }
        run_queue(ctx, &up, &self.queue, &syncs, &self.logger, "ingress", |k| self.process(k));
    }
    fn has_synced(&self) -> bool {
        self.informer.has_synced()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub fn ingress_source_from_namespace_and_name(namespace: &str, name: &str) -> String {
    format!("ingress/{namespace}/{name}")
}

/// Go `ingressLabels`.
pub fn ingress_labels(ingress: &ApiIngress) -> LabelSet {
    let mut ls = LabelSet::new();
    ls.0.insert(NAMESPACE_LABEL.to_string(), ingress.meta.namespace.clone());
    if let Some(cls) = &ingress.ingress_class_name {
        ls.0.insert(INGRESS_CLASS_NAME_LABEL.to_string(), cls.clone());
    }
    add_object_meta_labels(&mut ls, &ingress.meta, Role::INGRESS);
    ls
}

/// Go `pathsFromIngressPaths` (None is Go's nil slice).
pub fn paths_from_ingress_paths(paths: Option<&[String]>) -> Vec<String> {
    match paths {
        None => vec!["/".to_string()],
        Some(p) => p.iter().map(|p| if p.is_empty() { "/".to_string() } else { p.clone() }).collect(),
    }
}

/// Go `rulePaths`.
pub fn rule_paths(rule: &IngressRule) -> Option<&[String]> {
    rule.http_paths.as_deref()
}

/// Go `matchesHostnamePattern`.
pub fn matches_hostname_pattern(pattern: &str, host: &str) -> bool {
    if pattern == host {
        return true;
    }
    let pp: Vec<&str> = pattern.split('.').collect();
    let hp: Vec<&str> = host.split('.').collect();
    if pp[0] != "*" {
        return false;
    }
    if pp.len() != hp.len() {
        return false;
    }
    (1..pp.len()).all(|i| pp[i] == hp[i])
}
