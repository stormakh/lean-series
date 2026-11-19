//! Port of `notifier/alertmanagerset.go`: the Alertmanagers of one `alertmanagers` config entry, with their
//! HTTP client and send loops.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use config::AlertmanagerConfig;
use discovery::{Group, Logger};
use model::labels::Labels;

use crate::alert::{relabel_alerts, Alert};
use crate::alertmanager::{alertmanager_from_group, Alertmanager};
use crate::manager::Options;
use crate::metric::AlertMetrics;
use crate::send_loop::SendLoop;

/// The mutable part of a set (Go fields guarded by `alertmanagerSet.mtx`).
#[derive(Default)]
pub struct SetState {
    pub ams: Vec<Alertmanager>,
    pub dropped_ams: Vec<Alertmanager>,
    /// None after `apply_config` moved the loops to a new set (Go nil map).
    pub send_loops: Option<BTreeMap<String, Arc<SendLoop>>>,
}

/// Go `alertmanagerSet`.
pub struct AlertmanagerSet {
    pub cfg: Arc<AlertmanagerConfig>,
    client: Option<net::Client>,
    opts: Arc<Options>,
    metrics: Arc<AlertMetrics>,
    mtx: RwLock<SetState>,
    logger: Logger,
}

impl AlertmanagerSet {
    /// Go `newAlertmanagerSet`: builds the HTTP client from the config (with SigV4 when configured).
    pub fn new(cfg: AlertmanagerConfig, opts: Arc<Options>, logger: Logger, metrics: Arc<AlertMetrics>) -> Result<AlertmanagerSet, String> {
        let mut client = net::new_client_from_config(&cfg.http_client_config, &net::HttpClientOptions::default()).map_err(|e| e.msg)?;
        if let Some(sc) = &cfg.sigv4_config {
            let rt = net::SigV4RoundTripper::new(sc, client.transport.clone(), Default::default()).map_err(|e| e.msg)?;
            client.transport = Arc::new(rt);
        }
        Ok(AlertmanagerSet {
            cfg: Arc::new(cfg),
            client: Some(client),
            opts,
            metrics,
            mtx: RwLock::new(SetState { send_loops: Some(BTreeMap::new()), ..Default::default() }),
            logger,
        })
    }

    /// A set with the given Alertmanager URLs and (not started) send loops; Go's test helper
    /// `newTestAlertmanagerSet`. `client` None is Go's nil client (the default client is used).
    pub fn with_alertmanagers(cfg: AlertmanagerConfig, client: Option<net::Client>, opts: Arc<Options>, metrics: Arc<AlertMetrics>, urls: &[&str]) -> AlertmanagerSet {
        let cfg = Arc::new(cfg);
        let logger = Logger::nop();
        let ams: Vec<Alertmanager> = urls.iter().map(|u| Alertmanager::from_url(u)).collect();
        let mut loops = BTreeMap::new();
        for u in urls {
            loops.insert(u.to_string(), SendLoop::new(u, client.clone(), Some(cfg.clone()), opts.clone(), logger.clone(), metrics.clone()));
        }
        AlertmanagerSet { cfg, client, opts, metrics, mtx: RwLock::new(SetState { ams, dropped_ams: Vec::new(), send_loops: Some(loops) }), logger }
    }

    pub fn read(&self) -> RwLockReadGuard<'_, SetState> {
        self.mtx.read().unwrap_or_else(|e| e.into_inner())
    }

    pub fn write(&self) -> RwLockWriteGuard<'_, SetState> {
        self.mtx.write().unwrap_or_else(|e| e.into_inner())
    }

    /// The Alertmanagers in use.
    pub fn ams(&self) -> Vec<Alertmanager> {
        self.read().ams.clone()
    }

    /// The send loops by Alertmanager URL (empty when they were moved away).
    pub fn send_loops(&self) -> BTreeMap<String, Arc<SendLoop>> {
        self.read().send_loops.clone().unwrap_or_default()
    }

    /// Go `sync`: the deduplicated Alertmanagers of the target groups; starts send loops for new ones and
    /// stops the loops of removed ones.
    pub fn sync(&self, tgs: &[Arc<Group>]) {
        let mut all_ams = Vec::new();
        let mut all_dropped = Vec::new();
        for tg in tgs {
            match alertmanager_from_group(tg, &self.cfg) {
                Ok((ams, dropped)) => {
                    all_ams.extend(ams);
                    all_dropped.extend(dropped);
                }
                Err(e) => {
                    self.logger.error("Creating discovered Alertmanagers failed", &[("err", &e)]);
                }
            }
        }

        let mut st = self.write();
        let previous = std::mem::take(&mut st.ams);
        st.dropped_ams = all_dropped;

        // Deduplicate Alertmanagers and add send loops for new Alertmanagers.
        let mut seen: HashSet<String> = HashSet::new();
        for am in all_ams {
            if seen.insert(am.url_string()) {
                st.ams.push(am);
            }
        }
        let ams = st.ams.clone();
        self.add_send_loops(&mut st, &ams);

        // Clean up the Alertmanagers that are gone; avoid cleaning up what we just added.
        for am in &previous {
            if seen.insert(am.url_string()) {
                Self::clean_send_loops_locked(&mut st, std::slice::from_ref(am));
            }
        }
    }

    /// Go `configHash`: the config as YAML (Go hashes it with MD5; the text itself serves as the key here).
    pub fn config_hash(&self) -> Result<String, String> {
        yaml::marshal(&*self.cfg)
    }

    /// Go `send`: applies the set's alert relabeling and queues the alerts on every send loop.
    pub fn send(&self, alerts: &[Arc<Alert>]) {
        let st = self.write();
        let relabeled;
        let mut alerts = alerts;
        if !self.cfg.alert_relabel_configs.is_empty() {
            relabeled = relabel_alerts(&self.cfg.alert_relabel_configs, &Labels::empty(), alerts);
            if relabeled.is_empty() {
                return;
            }
            alerts = &relabeled;
        }
        if let Some(loops) = &st.send_loops {
            for l in loops.values() {
                l.add(alerts);
            }
        }
    }

    /// Go `addSendLoops`: creates and starts a send loop for each new Alertmanager. The caller holds the lock.
    fn add_send_loops(&self, st: &mut SetState, ams: &[Alertmanager]) {
        let loops = st.send_loops.get_or_insert_with(BTreeMap::new);
        for am in ams {
            let us = am.url_string();
            // Only add if the send loop doesn't already exist.
            if loops.contains_key(&us) {
                continue;
            }
            let l = SendLoop::new(&us, self.client.clone(), Some(self.cfg.clone()), self.opts.clone(), self.logger.with(&[("alertmanager", &us)]), self.metrics.clone());
            l.start();
            loops.insert(us, l);
        }
    }

    /// Go `cleanSendLoops` with the lock held by the caller: stops and removes the loops of these Alertmanagers.
    pub fn clean_send_loops_locked(st: &mut SetState, ams: &[Alertmanager]) {
        for am in ams {
            let us = am.url_string();
            if let Some(loops) = st.send_loops.as_mut()
                && let Some(l) = loops.remove(&us)
            {
                l.stop();
            }
        }
    }

    /// Go `cleanSendLoops(ams.ams...)`: stops the loops of all current Alertmanagers.
    pub fn clean_send_loops(&self) {
        let mut st = self.write();
        let ams = st.ams.clone();
        Self::clean_send_loops_locked(&mut st, &ams);
    }

    /// Go `startSendLoops(ams.ams)`: starts the loops of all current Alertmanagers. Panics when one is missing.
    pub fn start_send_loops(&self) {
        let st = self.read();
        for am in &st.ams {
            let us = am.url_string();
            match st.send_loops.as_ref().and_then(|l| l.get(&us)) {
                Some(l) => l.start(),
                None => panic!("send loop not found for {us}"),
            }
        }
    }

    /// Builds a new set for `apply_config` that reuses the state of an old one.
    pub(crate) fn adopt(&self, ams: Vec<Alertmanager>, dropped: Vec<Alertmanager>, loops: Option<BTreeMap<String, Arc<SendLoop>>>) {
        let mut st = self.write();
        st.ams = ams;
        st.dropped_ams = dropped;
        if loops.is_some() {
            st.send_loops = loops;
        }
    }
}
