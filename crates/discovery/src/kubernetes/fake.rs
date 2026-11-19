//! An in-process fake API that behaves like client-go's fake clientset (`k8s.io/client-go/kubernetes/fake` with
//! its object tracker): lists filter by label selector only, resource versions count per resource kind, a watch
//! from a list's resource version first gets the objects changed since, and watch events reach every watcher of
//! the namespace (and of all namespaces) in order. For tests.
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};

use super::api::{Kind, Object};
use super::client::{ApiError, Client, EventHandler, ListOptions, ObjectList, Stopper, WatchEvent};
use super::selector::parse_label_selector;

struct Watcher {
    id: u64,
    kind: Kind,
    namespace: String,
    events: VecDeque<WatchEvent>,
    stopped: bool,
}

/// (namespace, name) -> (resource version, object)
type Objects = BTreeMap<(String, String), (i64, Object)>;

#[derive(Default)]
struct State {
    objects: HashMap<Kind, Objects>,
    rvs: HashMap<Kind, i64>,
    watchers: Vec<Watcher>,
    next_id: u64,
    watch_error: Option<ApiError>,
}

struct Inner {
    st: Mutex<State>,
    cv: Condvar,
}

/// Go `fake.NewClientset`.
#[derive(Clone)]
pub struct FakeClient(Arc<Inner>);

impl FakeClient {
    /// A fake with the given objects (each in its own namespace).
    pub fn new(objects: Vec<Object>) -> FakeClient {
        let f = FakeClient(Arc::new(Inner { st: Mutex::new(State::default()), cv: Condvar::new() }));
        for o in objects {
            let ns = o.meta().namespace.clone();
            f.create(&ns, o).expect("add object");
        }
        f
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.0.st.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn broadcast(&self, st: &mut State, kind: Kind, ns: &str, ev: WatchEvent) {
        for w in st.watchers.iter_mut().filter(|w| w.kind == kind && (w.namespace == ns || w.namespace.is_empty())) {
            w.events.push_back(ev.clone());
        }
        self.0.cv.notify_all();
    }

    fn add(&self, ns: &str, mut o: Object, replace: bool) -> Result<(), ApiError> {
        let kind = o.kind();
        if o.meta().namespace.is_empty() {
            o.meta_mut().namespace = ns.to_string();
        }
        if ns != o.meta().namespace {
            return Err(ApiError::new(400, "BadRequest", format!("request namespace does not match object namespace, request: {:?} object: {:?}", ns, o.meta().namespace)));
        }
        let mut st = self.lock();
        let rv = st.rvs.get(&kind).copied().unwrap_or(1) + 1;
        let key = (o.meta().namespace.clone(), o.meta().name.clone());
        let exists = st.objects.get(&kind).is_some_and(|m| m.contains_key(&key));
        let name = key.1.clone();
        if exists && !replace {
            return Err(ApiError::new(409, "AlreadyExists", format!("{} {:?} already exists", kind.resource(), name)));
        }
        if !exists && replace {
            return Err(ApiError::new(404, "NotFound", format!("{} {:?} not found", kind.resource(), name)));
        }
        st.rvs.insert(kind, rv);
        st.objects.entry(kind).or_default().insert(key, (rv, o.clone()));
        let ev = if exists { WatchEvent::Modified(o) } else { WatchEvent::Added(o) };
        self.broadcast(&mut st, kind, ns, ev);
        Ok(())
    }

    /// Go `Create` in namespace `ns` ("" for cluster objects).
    pub fn create(&self, ns: &str, o: Object) -> Result<(), ApiError> {
        self.add(ns, o, false)
    }

    /// Go `Update`.
    pub fn update(&self, ns: &str, o: Object) -> Result<(), ApiError> {
        self.add(ns, o, true)
    }

    /// Go `Delete`.
    pub fn delete(&self, kind: Kind, ns: &str, name: &str) -> Result<(), ApiError> {
        let mut st = self.lock();
        let Some(m) = st.objects.get_mut(&kind) else {
            return Err(ApiError::new(404, "NotFound", format!("{} {:?} not found", kind.resource(), name)));
        };
        let Some((_, o)) = m.remove(&(ns.to_string(), name.to_string())) else {
            return Err(ApiError::new(404, "NotFound", format!("{} {:?} not found", kind.resource(), name)));
        };
        self.broadcast(&mut st, kind, ns, WatchEvent::Deleted(o));
        Ok(())
    }

    /// Go `PrependWatchReactor("*", ...)` returning an error: every later watch fails with `err`.
    pub fn prepend_watch_error(&self, err: ApiError) {
        self.lock().watch_error = Some(err);
    }
}

impl Client for FakeClient {
    fn list(&self, kind: Kind, namespace: &str, opts: &ListOptions, _stop: &Stopper) -> Result<ObjectList, ApiError> {
        let sel = if opts.label_selector.is_empty() { Default::default() } else { parse_label_selector(&opts.label_selector).map_err(|e| ApiError::new(400, "BadRequest", e))? };
        let st = self.lock();
        let rv = st.rvs.get(&kind).copied().unwrap_or(1);
        let items = st
            .objects
            .get(&kind)
            .map(|m| m.iter().filter(|((ns, _), _)| namespace.is_empty() || ns == namespace).map(|(_, (_, o))| o).filter(|o| sel.matches(&o.meta().labels)).cloned().collect())
            .unwrap_or_default();
        Ok(ObjectList { resource_version: rv.to_string(), continue_: String::new(), items })
    }

    fn watch(&self, kind: Kind, namespace: &str, opts: &ListOptions, stop: &Stopper, mut on_event: EventHandler) -> Result<(), ApiError> {
        let id = {
            let mut st = self.lock();
            if let Some(e) = &st.watch_error {
                return Err(e.clone());
            }
            let from: i64 = if opts.resource_version.is_empty() {
                0
            } else {
                opts.resource_version.parse().map_err(|_| ApiError::new(500, "InternalError", format!("invalid ResourceVersion {:?} in ListOptions, must be int64", opts.resource_version)))?
            };
            let events: VecDeque<WatchEvent> = st
                .objects
                .get(&kind)
                .map(|m| m.iter().filter(|((ns, _), (rv, _))| (namespace.is_empty() || ns == namespace) && from < *rv).map(|(_, (_, o))| WatchEvent::Added(o.clone())).collect())
                .unwrap_or_default();
            st.next_id += 1;
            let id = st.next_id;
            st.watchers.push(Watcher { id, kind, namespace: namespace.to_string(), events, stopped: false });
            id
        };
        let inner = self.0.clone();
        if !stop.set(Box::new(move || {
            let mut st = inner.st.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(w) = st.watchers.iter_mut().find(|w| w.id == id) {
                w.stopped = true;
            }
            inner.cv.notify_all();
        })) {
            self.lock().watchers.retain(|w| w.id != id);
            return Ok(());
        }
        loop {
            let ev = {
                let mut st = self.lock();
                loop {
                    let w = st.watchers.iter_mut().find(|w| w.id == id).expect("watcher");
                    if w.stopped {
                        st.watchers.retain(|w| w.id != id);
                        stop.clear();
                        return Ok(());
                    }
                    if let Some(ev) = w.events.pop_front() {
                        break ev;
                    }
                    st = self.0.cv.wait(st).unwrap_or_else(|e| e.into_inner());
                }
            };
            if !on_event(ev) {
                self.lock().watchers.retain(|w| w.id != id);
                stop.clear();
                return Ok(());
            }
        }
    }
}
