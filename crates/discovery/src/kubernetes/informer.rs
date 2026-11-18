//! A port of the parts of client-go `tools/cache` the discovery uses: a shared index informer (reflector with
//! list + watch, resource versions, bookmarks, relist on 410 Gone, backoff; a store with indexers; event
//! handlers) and the `util/workqueue` queue of keys.
//!
//! Each informer runs its reflector on one thread. The store is updated first, then the handlers are called on
//! the reflector thread (client-go calls them from one goroutine per handler); handlers only enqueue keys. A
//! relist replaces the store at once, then calls the handlers: deletes for objects that are gone first, then
//! adds or updates in list order (client-go's RealFIFO with atomic replace).
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::cancel::CancelToken;
use crate::log::Logger;

use super::api::{Kind, Object};
use super::client::{ApiError, Client, EventHandler, ListOptions, Stopper, WatchEvent};

/// An index function: the index values of an object (Go `cache.IndexFunc`).
pub type IndexFunc = fn(&Object) -> Vec<String>;

/// Go `cache.NamespaceIndex`.
pub const NAMESPACE_INDEX: &str = "namespace";

/// Go `cache.MetaNamespaceIndexFunc`.
pub fn meta_namespace_index_func(o: &Object) -> Vec<String> {
    vec![o.meta().namespace.clone()]
}

struct Index {
    name: &'static str,
    f: IndexFunc,
    values: HashMap<String, BTreeSet<String>>,
}

/// The objects of an informer by key, with indexes (Go `cache.Indexer`).
#[derive(Default)]
pub struct Store {
    items: HashMap<String, Arc<Object>>,
    indexes: Vec<Index>,
}

impl Store {
    fn unindex(&mut self, key: &str, o: &Object) {
        for ix in &mut self.indexes {
            for v in (ix.f)(o) {
                if let Some(s) = ix.values.get_mut(&v) {
                    s.remove(key);
                    if s.is_empty() {
                        ix.values.remove(&v);
                    }
                }
            }
        }
    }

    /// Adds or replaces an object; returns the old one.
    fn put(&mut self, key: String, o: Arc<Object>) -> Option<Arc<Object>> {
        let old = self.items.remove(&key);
        if let Some(old) = &old {
            self.unindex(&key, old);
        }
        for ix in &mut self.indexes {
            for v in (ix.f)(&o) {
                ix.values.entry(v).or_default().insert(key.clone());
            }
        }
        self.items.insert(key, o);
        old
    }

    fn remove(&mut self, key: &str) -> Option<Arc<Object>> {
        let old = self.items.remove(key)?;
        self.unindex(key, &old);
        Some(old)
    }

    pub fn get(&self, key: &str) -> Option<Arc<Object>> {
        self.items.get(key).cloned()
    }

    /// Go `Indexer.ByIndex` (objects sorted by key).
    pub fn by_index(&self, name: &str, value: &str) -> Result<Vec<Arc<Object>>, String> {
        let ix = self.indexes.iter().find(|i| i.name == name).ok_or_else(|| format!("Index with name {name} does not exist"))?;
        Ok(ix.values.get(value).map(|keys| keys.iter().filter_map(|k| self.items.get(k).cloned()).collect()).unwrap_or_default())
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
}

/// What a handler is told (Go `ResourceEventHandler` calls).
pub enum Event<'a> {
    Add(&'a Arc<Object>),
    Update(&'a Arc<Object>, &'a Arc<Object>),
    /// The key and the last known object; `tombstone` when the deletion was found by a relist (Go
    /// `cache.DeletedFinalStateUnknown`).
    Delete { key: &'a str, obj: &'a Arc<Object>, tombstone: bool },
}

impl Event<'_> {
    /// Go `cache.DeletionHandlingMetaNamespaceKeyFunc` of the event object.
    pub fn key(&self) -> String {
        match self {
            Event::Add(o) | Event::Update(_, o) => o.key(),
            Event::Delete { key, .. } => key.to_string(),
        }
    }
}

pub type Handler = Arc<dyn Fn(&Event) + Send + Sync>;

/// The reflector's resource version state.
#[derive(Default)]
struct RvState {
    last_sync: String,
    unavailable: bool,
    paginated_result: bool,
}

/// Go `wait.Backoff` with `DelayWithReset`: 800ms doubling to 30s, full jitter, reset after 2 quiet minutes.
struct Backoff {
    duration: Duration,
    steps: i32,
    last: Option<Instant>,
}

impl Backoff {
    const INIT: Duration = Duration::from_millis(800);
    const CAP: Duration = Duration::from_secs(30);
    const RESET: Duration = Duration::from_secs(120);

    fn new() -> Backoff {
        Backoff { duration: Self::INIT, steps: 38, last: None }
    }
    fn delay(&mut self) -> Duration {
        let now = Instant::now();
        if self.last.is_some_and(|l| now.duration_since(l) > Self::RESET) {
            *self = Backoff::new();
        }
        self.last = Some(now);
        let d = if self.steps < 1 {
            self.duration
        } else {
            self.steps -= 1;
            let d = self.duration;
            self.duration = self.duration.mul_f64(2.0);
            if self.duration > Self::CAP {
                self.duration = Self::CAP;
                self.steps = 0;
            }
            d
        };
        jitter(d, 1.0)
    }
}

fn rand_f64() -> f64 {
    let mut b = [0u8; 8];
    crypto::random_bytes(&mut b);
    (u64::from_le_bytes(b) >> 11) as f64 / (1u64 << 53) as f64
}

/// Go `wait.Jitter`.
fn jitter(d: Duration, f: f64) -> Duration {
    d + d.mul_f64(rand_f64() * f)
}

/// The watch timeout: random in [5m, 10m) (Go `minWatchTimeout`).
fn watch_timeout_seconds() -> i64 {
    (300.0 + rand_f64() * 300.0) as i64
}

#[derive(Default)]
struct WatchState {
    events: usize,
    besides_added: bool,
    err: Option<ApiError>,
}

/// A shared index informer for one resource kind in one namespace ("" = all).
pub struct Informer {
    client: Arc<dyn Client>,
    kind: Kind,
    namespace: String,
    label_selector: String,
    field_selector: String,
    store: RwLock<Store>,
    handlers: Mutex<Vec<Handler>>,
    synced: Mutex<bool>,
    synced_cv: Condvar,
    rv: Mutex<RvState>,
    stopper: Stopper,
    /// Called with every error that ends a list-and-watch (Go `SetWatchErrorHandler`).
    watch_error_handler: Box<dyn Fn(&ApiError) + Send + Sync>,
    logger: Logger,
}

impl Informer {
    /// Go `cache.NewSharedIndexInformer` with a list-watch of `kind` in `namespace` with the selectors.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Arc<dyn Client>,
        kind: Kind,
        namespace: &str,
        label_selector: &str,
        field_selector: &str,
        indexers: Vec<(&'static str, IndexFunc)>,
        watch_error_handler: Box<dyn Fn(&ApiError) + Send + Sync>,
        logger: Logger,
    ) -> Arc<Informer> {
        let store = Store { items: HashMap::new(), indexes: indexers.into_iter().map(|(name, f)| Index { name, f, values: HashMap::new() }).collect() };
        Arc::new(Informer {
            client,
            kind,
            namespace: namespace.to_string(),
            label_selector: label_selector.to_string(),
            field_selector: field_selector.to_string(),
            store: RwLock::new(store),
            handlers: Mutex::new(Vec::new()),
            synced: Mutex::new(false),
            synced_cv: Condvar::new(),
            rv: Mutex::new(RvState::default()),
            stopper: Stopper::new(),
            watch_error_handler,
            logger: logger.with(&[("component", "k8s_client_runtime")]),
        })
    }

    pub fn kind(&self) -> Kind {
        self.kind
    }

    pub fn add_event_handler(&self, h: Handler) {
        self.handlers.lock().unwrap_or_else(|e| e.into_inner()).push(h);
    }

    fn store(&self) -> std::sync::RwLockReadGuard<'_, Store> {
        self.store.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Go `GetStore().GetByKey`.
    pub fn get(&self, key: &str) -> Option<Arc<Object>> {
        self.store().get(key)
    }

    /// Go `GetIndexer().ByIndex`.
    pub fn by_index(&self, name: &str, value: &str) -> Result<Vec<Arc<Object>>, String> {
        self.store().by_index(name, value)
    }

    /// The number of indexers (Go `len(GetIndexer().GetIndexers())`).
    pub fn indexer_count(&self) -> usize {
        self.store().indexes.len()
    }

    /// The number of objects in the store.
    pub fn len(&self) -> usize {
        self.store().len()
    }
    pub fn is_empty(&self) -> bool {
        self.store().is_empty()
    }

    /// Go `HasSynced`: the first list has been stored and handed to the handlers.
    pub fn has_synced(&self) -> bool {
        *self.synced.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Go `cache.WaitForCacheSync` for this informer; false when `ctx` was cancelled first.
    pub fn wait_for_sync(self: &Arc<Self>, ctx: &CancelToken) -> bool {
        let weak = Arc::downgrade(self);
        ctx.on_cancel(Box::new(move || {
            if let Some(i) = weak.upgrade() {
                let _g = i.synced.lock().unwrap_or_else(|e| e.into_inner());
                i.synced_cv.notify_all();
            }
        }));
        let mut s = self.synced.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if *s {
                return true;
            }
            if ctx.is_cancelled() {
                return false;
            }
            s = self.synced_cv.wait(s).unwrap_or_else(|e| e.into_inner());
        }
    }

    fn dispatch(&self, events: &[Event]) {
        let hs: Vec<Handler> = self.handlers.lock().unwrap_or_else(|e| e.into_inner()).clone();
        for e in events {
            for h in &hs {
                h(e);
            }
        }
    }

    /// Adds or updates one object (watch ADDED/MODIFIED).
    fn upsert(&self, o: Object) {
        let key = o.key();
        let new = Arc::new(o);
        let old = self.store.write().unwrap_or_else(|e| e.into_inner()).put(key, new.clone());
        match old {
            Some(old) => self.dispatch(&[Event::Update(&old, &new)]),
            None => self.dispatch(&[Event::Add(&new)]),
        }
    }

    fn delete(&self, o: Object) {
        let key = o.key();
        self.store.write().unwrap_or_else(|e| e.into_inner()).remove(&key);
        let obj = Arc::new(o);
        self.dispatch(&[Event::Delete { key: &key, obj: &obj, tombstone: false }]);
    }

    /// Go `Replace` of the store with a list.
    fn replace(&self, items: Vec<Object>) {
        let new_keys: HashSet<String> = items.iter().map(|o| o.key()).collect();
        let mut deleted: Vec<(String, Arc<Object>)> = Vec::new();
        let mut changed: Vec<(Option<Arc<Object>>, Arc<Object>)> = Vec::with_capacity(items.len());
        {
            let mut st = self.store.write().unwrap_or_else(|e| e.into_inner());
            let mut gone: Vec<String> = st.items.keys().filter(|k| !new_keys.contains(*k)).cloned().collect();
            gone.sort();
            for k in gone {
                if let Some(o) = st.remove(&k) {
                    deleted.push((k, o));
                }
            }
            for o in items {
                let key = o.key();
                let new = Arc::new(o);
                let old = st.put(key, new.clone());
                changed.push((old, new));
            }
        }
        let mut events: Vec<Event> = Vec::with_capacity(deleted.len() + changed.len());
        for (k, o) in &deleted {
            events.push(Event::Delete { key: k, obj: o, tombstone: true });
        }
        for (old, new) in &changed {
            events.push(match old {
                Some(old) => Event::Update(old, new),
                None => Event::Add(new),
            });
        }
        self.dispatch(&events);
    }

    fn options(&self, rv: &str) -> ListOptions {
        ListOptions { label_selector: self.label_selector.clone(), field_selector: self.field_selector.clone(), resource_version: rv.to_string(), ..Default::default() }
    }

    fn rv(&self) -> std::sync::MutexGuard<'_, RvState> {
        self.rv.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Go `relistResourceVersion`.
    fn relist_resource_version(&self) -> String {
        let r = self.rv();
        if r.unavailable {
            return String::new();
        }
        if r.last_sync.is_empty() {
            return "0".to_string();
        }
        r.last_sync.clone()
    }

    /// Go `pager.List`: pages of 500 unless the resource version asks for a list from the watch cache.
    fn list_pages(&self, rv: &str) -> Result<(String, Vec<Object>, bool), ApiError> {
        let paginated = self.rv().paginated_result;
        let page_size = if paginated || rv.is_empty() || rv == "0" { 500 } else { 0 };
        let mut opts = self.options(rv);
        opts.limit = page_size;
        let mut items = Vec::new();
        let mut was_paginated = false;
        loop {
            let page = match self.client.list(self.kind, &self.namespace, &opts, &self.stopper) {
                Ok(p) => p,
                // an expired continue token: a full list (Go pager FullListIfExpired)
                Err(e) if e.is_expired() && !opts.continue_.is_empty() => {
                    let mut o = self.options("");
                    o.limit = 0;
                    let p = self.client.list(self.kind, &self.namespace, &o, &self.stopper)?;
                    return Ok((p.resource_version, p.items, false));
                }
                Err(e) => return Err(e),
            };
            items.extend(page.items);
            if page.continue_.is_empty() {
                return Ok((page.resource_version, items, was_paginated));
            }
            was_paginated = true;
            opts.continue_ = page.continue_;
            opts.resource_version = String::new();
        }
    }

    /// Go `Reflector.list`.
    fn list(&self, ctx: &CancelToken) -> Result<(), ApiError> {
        let rv = self.relist_resource_version();
        let mut r = self.list_pages(&rv);
        if let Err(e) = &r
            && (e.is_expired() || e.is_too_large_resource_version())
        {
            self.rv().unavailable = true;
            r = self.list_pages(&self.relist_resource_version());
        }
        if ctx.is_cancelled() {
            return Ok(());
        }
        let (list_rv, items, paginated) = r.map_err(|e| ApiError { message: format!("failed to list {}: {}", self.kind.type_description(), e.message), ..e })?;
        {
            let mut s = self.rv();
            if rv == "0" && paginated {
                s.paginated_result = true;
            }
            s.unavailable = false;
        }
        self.replace(items);
        self.rv().last_sync = list_rv;
        let mut s = self.synced.lock().unwrap_or_else(|e| e.into_inner());
        if !*s {
            *s = true;
            self.synced_cv.notify_all();
        }
        Ok(())
    }

    /// Go `Reflector.watch`: watches until an error ends it; Ok when a relist is due or on stop.
    fn watch(self: &Arc<Self>, ctx: &CancelToken, backoff: &mut Backoff) -> Result<(), ApiError> {
        loop {
            if ctx.is_cancelled() {
                return Ok(());
            }
            let start = Instant::now();
            let rv = self.rv().last_sync.clone();
            let mut opts = self.options(&rv);
            opts.timeout_seconds = Some(watch_timeout_seconds());
            opts.allow_watch_bookmarks = true;
            let propagate = !(rv.is_empty() || rv == "0");
            let ws = Arc::new(Mutex::new(WatchState::default()));
            let (me, ws2, stopper) = (self.clone(), ws.clone(), self.stopper.clone());
            let handler: EventHandler = Box::new(move |ev| {
                if stopper.is_stopped() {
                    return false;
                }
                let rv = ev.resource_version().to_string();
                let mut w = ws2.lock().unwrap_or_else(|e| e.into_inner());
                match ev {
                    WatchEvent::Error(e) => {
                        w.err = Some(e);
                        return false;
                    }
                    WatchEvent::Added(o) => {
                        drop(w);
                        me.upsert(o);
                        w = ws2.lock().unwrap_or_else(|e| e.into_inner());
                    }
                    WatchEvent::Modified(o) => {
                        w.besides_added = true;
                        drop(w);
                        me.upsert(o);
                        w = ws2.lock().unwrap_or_else(|e| e.into_inner());
                    }
                    WatchEvent::Deleted(o) => {
                        w.besides_added = true;
                        drop(w);
                        me.delete(o);
                        w = ws2.lock().unwrap_or_else(|e| e.into_inner());
                    }
                    WatchEvent::Bookmark(_) => w.besides_added = true,
                }
                if propagate || w.besides_added {
                    me.rv().last_sync = rv;
                }
                w.events += 1;
                true
            });
            if let Err(e) = self.client.watch(self.kind, &self.namespace, &opts, &self.stopper, handler) {
                if ctx.is_cancelled() {
                    return Ok(());
                }
                if e.is_watch_retriable() {
                    if ctx.wait_timeout(backoff.delay()) {
                        return Ok(());
                    }
                    continue;
                }
                return Err(e);
            }
            if ctx.is_cancelled() {
                return Ok(());
            }
            let w = std::mem::take(&mut *ws.lock().unwrap_or_else(|e| e.into_inner()));
            let err = match w.err {
                Some(e) => Some(e),
                None if w.events == 0 && start.elapsed() < Duration::from_secs(1) => {
                    Some(ApiError::new(0, "", format!("very short watch: {}: Unexpected watch close - watch lasted less than a second and no items received", self.kind.type_description())))
                }
                None => None,
            };
            let Some(e) = err else { continue };
            if e.is_expired() {
                return Ok(());
            }
            if e.is_too_many_requests() {
                if ctx.wait_timeout(backoff.delay()) {
                    return Ok(());
                }
                continue;
            }
            self.logger.info("Warning: watch ended with error", &[("type", self.kind.type_description()), ("err", &e.message)]);
            return Ok(());
        }
    }

    /// Go `Reflector.RunWithContext`: list and watch until `ctx` is cancelled, with backoff between attempts.
    pub fn run(self: &Arc<Self>, ctx: &CancelToken) {
        let stopper = self.stopper.clone();
        ctx.on_cancel(Box::new(move || stopper.stop()));
        let mut backoff = Backoff::new();
        loop {
            if ctx.is_cancelled() {
                return;
            }
            let r = self.list(ctx).and_then(|_| if ctx.is_cancelled() { Ok(()) } else { self.watch(ctx, &mut backoff) });
            if let Err(e) = r {
                if ctx.is_cancelled() {
                    return;
                }
                (self.watch_error_handler)(&e);
                self.logger.error("Failed to watch", &[("type", self.kind.type_description()), ("err", &e.message)]);
            }
            if ctx.wait_timeout(backoff.delay()) {
                return;
            }
        }
    }
}

/// Go `workqueue.Typed[string]`: a FIFO of keys where a key is queued at most once, and a key that is added
/// while it is being processed is queued again when its processing is done.
#[derive(Default)]
pub struct WorkQueue {
    m: Mutex<QueueState>,
    cv: Condvar,
}

#[derive(Default)]
struct QueueState {
    queue: VecDeque<String>,
    dirty: HashSet<String>,
    processing: HashSet<String>,
    shutting_down: bool,
}

impl WorkQueue {
    pub fn new() -> WorkQueue {
        WorkQueue::default()
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, QueueState> {
        self.m.lock().unwrap_or_else(|e| e.into_inner())
    }
    pub fn add(&self, key: String) {
        let mut s = self.lock();
        if s.shutting_down || s.dirty.contains(&key) {
            return;
        }
        s.dirty.insert(key.clone());
        if s.processing.contains(&key) {
            return;
        }
        s.queue.push_back(key);
        self.cv.notify_one();
    }
    /// Blocks for the next key; None after `shut_down` once the queue is empty.
    pub fn get(&self) -> Option<String> {
        let mut s = self.lock();
        while s.queue.is_empty() && !s.shutting_down {
            s = self.cv.wait(s).unwrap_or_else(|e| e.into_inner());
        }
        let key = s.queue.pop_front()?;
        s.processing.insert(key.clone());
        s.dirty.remove(&key);
        Some(key)
    }
    pub fn done(&self, key: &str) {
        let mut s = self.lock();
        s.processing.remove(key);
        if s.dirty.contains(key) {
            s.queue.push_back(key.to_string());
            self.cv.notify_one();
        }
    }
    pub fn shut_down(&self) {
        self.lock().shutting_down = true;
        self.cv.notify_all();
    }
    pub fn len(&self) -> usize {
        self.lock().queue.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Go `cache.SplitMetaNamespaceKey`.
pub fn split_meta_namespace_key(key: &str) -> Result<(&str, &str), String> {
    let parts: Vec<&str> = key.split('/').collect();
    match parts.len() {
        1 => Ok(("", parts[0])),
        2 => Ok((parts[0], parts[1])),
        _ => Err(format!("unexpected key format: {}", yaml::go_quote(key))),
    }
}
