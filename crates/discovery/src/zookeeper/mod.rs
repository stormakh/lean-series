//! Port of `discovery/zookeeper`: Twitter serversets (`serverset_sd_configs`) and AirBnB Nerve
//! (`nerve_sd_configs`) registered in ZooKeeper. A tree cache per path watches the member nodes; every
//! change of a member is sent as a group whose source is the member's path.
//!
//! `zk` is the ZooKeeper client (go-zookeeper/zk) and `treecache` the port of `util/treecache`.
pub mod treecache;
pub mod zk;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use model::labelset::{ADDRESS_LABEL, LabelSet};
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::marathon::gojson;
use crate::sdutil::{join_host_port, sanitize_label_name};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererOptions, SdConfigType};

use treecache::{ZookeeperTreeCache, ZookeeperTreeCacheEvent, new_zookeeper_tree_cache};

/// Go `zookeeper.ServersetSDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ServersetSdConfig {
    pub servers: Vec<String>,
    pub paths: Vec<String>,
    pub timeout: yaml::Duration,
}

/// Go `zookeeper.NerveSDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NerveSdConfig {
    pub servers: Vec<String>,
    pub paths: Vec<String>,
    pub timeout: yaml::Duration,
}

/// Go `DefaultServersetSDConfig`.
pub fn default_serverset_sd_config() -> ServersetSdConfig {
    ServersetSdConfig { timeout: yaml::Duration::from_secs(10), ..Default::default() }
}

/// Go `DefaultNerveSDConfig`.
pub fn default_nerve_sd_config() -> NerveSdConfig {
    NerveSdConfig { timeout: yaml::Duration::from_secs(10), ..Default::default() }
}

yaml::yaml_struct!(ServersetSdConfig, "zookeeper.plain", [
    (servers: "servers")
    (paths: "paths")
    (timeout: "timeout", omitempty)
]);

yaml::yaml_struct!(NerveSdConfig, "zookeeper.plain", [
    (servers: "servers")
    (paths: "paths")
    (timeout: "timeout", omitempty)
]);

/// The validation both configs share (`kind` is "serverset" or "nerve").
fn validate(kind: &str, servers: &[String], paths: &[String]) -> yaml::Result<()> {
    if servers.is_empty() {
        return Err(YError::Msg(format!("{kind} SD config must contain at least one Zookeeper server")));
    }
    if paths.is_empty() {
        return Err(YError::Msg(format!("{kind} SD config must contain at least one path")));
    }
    for path in paths {
        if !path.starts_with('/') {
            return Err(YError::Msg(format!("{kind} SD config paths must begin with '/': {path}")));
        }
    }
    Ok(())
}

impl Unmarshal for ServersetSdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_serverset_sd_config();
            d.inner_struct(n, self)?;
            validate("serverset", &self.servers, &self.paths)
        })
    }
    fn go_type() -> String {
        "zookeeper.plain".to_string()
    }
}

impl Unmarshal for NerveSdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_nerve_sd_config();
            d.inner_struct(n, self)?;
            validate("nerve", &self.servers, &self.paths)
        })
    }
    fn go_type() -> String {
        "zookeeper.plain".to_string()
    }
}

impl yaml::Marshal for ServersetSdConfig {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        yaml::Struct::fields_zero(self)
    }
}

impl yaml::Marshal for NerveSdConfig {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        yaml::Struct::fields_zero(self)
    }
}

impl SdConfigType for ServersetSdConfig {
    const GO_TYPE: &'static str = "zookeeper.ServersetSDConfig";
}

impl SdConfigType for NerveSdConfig {
    const GO_TYPE: &'static str = "zookeeper.NerveSDConfig";
}

impl Config for ServersetSdConfig {
    fn name(&self) -> &'static str {
        "serverset"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(new_serverset_discovery(self, opts.logger)?))
    }
}

impl Config for NerveSdConfig {
    fn name(&self) -> &'static str {
        "nerve"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(new_nerve_discovery(self, opts.logger)?))
    }
}

/// Parses the data of a member node at a path into its labels.
pub type ParseFn = Box<dyn Fn(&[u8], &str) -> Result<LabelSet, String> + Send>;

enum Update {
    Event(ZookeeperTreeCacheEvent),
    Cancel,
}

/// Go `zookeeper.Discovery`.
pub struct Discovery {
    conn: zk::Conn,
    sources: HashMap<String, Arc<Group>>,
    updates: mpsc::Receiver<Update>,
    updates_tx: mpsc::Sender<Update>,
    tree_caches: Vec<ZookeeperTreeCache>,
    parse: ParseFn,
    logger: Logger,
    stopped: bool,
}

/// Go `NewNerveDiscovery`.
pub fn new_nerve_discovery(conf: &NerveSdConfig, logger: Logger) -> Result<Discovery, String> {
    new_discovery(&conf.servers, conf.timeout.to_std(), &conf.paths, logger, Box::new(parse_nerve_member))
}

/// Go `NewServersetDiscovery`.
pub fn new_serverset_discovery(conf: &ServersetSdConfig, logger: Logger) -> Result<Discovery, String> {
    new_discovery(&conf.servers, conf.timeout.to_std(), &conf.paths, logger, Box::new(parse_serverset_member))
}

/// Go `NewDiscovery`: connects (in the background) and starts a tree cache per path.
pub fn new_discovery(srvs: &[String], timeout: Duration, paths: &[String], logger: Logger, pf: ParseFn) -> Result<Discovery, String> {
    let (conn, _events) = zk::connect(srvs, timeout, logger.clone())?;
    let (updates_tx, updates) = mpsc::channel();
    let mut tree_caches = Vec::with_capacity(paths.len());
    for path in paths {
        let tx = updates_tx.clone();
        let sink: treecache::EventSink = Box::new(move |ev| {
            let _ = tx.send(Update::Event(ev));
        });
        tree_caches.push(new_zookeeper_tree_cache(conn.clone(), path, sink, logger.clone()));
    }
    Ok(Discovery { conn, sources: HashMap::new(), updates, updates_tx, tree_caches, parse: pf, logger, stopped: false })
}

impl Discovery {
    /// The deferred part of Go's `Run`: stops the tree caches and closes the connection.
    fn shutdown(&mut self) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        for tc in &mut self.tree_caches {
            tc.stop();
        }
        self.conn.close();
    }

    /// The connection (for tests and diagnostics).
    pub fn conn(&self) -> &zk::Conn {
        &self.conn
    }

    pub fn logger(&self) -> &Logger {
        &self.logger
    }
}

impl Drop for Discovery {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl Discoverer for Discovery {
    fn run(&mut self, ctx: &CancelToken, ch: Sender) {
        let tx = self.updates_tx.clone();
        ctx.on_cancel(Box::new(move || {
            let _ = tx.send(Update::Cancel);
        }));
        while !ctx.is_cancelled() {
            let Ok(Update::Event(event)) = self.updates.recv() else { break };
            let mut tg = Group { source: event.path.clone(), ..Default::default() };
            match &event.data {
                Some(data) => match (self.parse)(data, &event.path) {
                    Ok(ls) => {
                        tg.targets = vec![ls];
                        self.sources.insert(event.path.clone(), Arc::new(tg.clone()));
                    }
                    Err(_) => {
                        self.sources.remove(&event.path);
                    }
                },
                None => {
                    self.sources.remove(&event.path);
                }
            }
            if !ch.send(vec![Arc::new(tg)]) {
                break;
            }
        }
        self.shutdown();
    }
}

pub const SERVERSET_STATUS_LABEL: &str = "__meta_serverset_status";
pub const SERVERSET_PATH_LABEL: &str = "__meta_serverset_path";
pub const SERVERSET_ENDPOINT_LABEL_PREFIX: &str = "__meta_serverset_endpoint";
pub const SERVERSET_SHARD_LABEL: &str = "__meta_serverset_shard";


/// Go `serversetMember`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ServersetMember {
    pub service_endpoint: ServersetEndpoint,
    pub additional_endpoints: BTreeMap<String, ServersetEndpoint>,
    pub status: String,
    pub shard: i64,
}
crate::go_json_struct!(ServersetMember, "zookeeper", "serversetMember", [
    service_endpoint: "ServiceEndpoint", additional_endpoints: "AdditionalEndpoints", status: "status", shard: "shard",
]);

/// Go `serversetEndpoint`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ServersetEndpoint {
    pub host: String,
    pub port: i64,
}
crate::go_json_struct!(ServersetEndpoint, "zookeeper", "serversetEndpoint", [host: "Host", port: "Port"]);

/// Go `parseServersetMember`.
pub fn parse_serverset_member(data: &[u8], path: &str) -> Result<LabelSet, String> {
    let mut member = ServersetMember::default();
    gojson::unmarshal(data, &mut member).map_err(|e| format!("error unmarshaling serverset member {}: {e}", yaml::go_quote(path)))?;
    let mut labels = LabelSet::new();
    labels.insert(SERVERSET_PATH_LABEL.to_string(), path.to_string());
    labels.insert(ADDRESS_LABEL.to_string(), join_host_port(&member.service_endpoint.host, member.service_endpoint.port));
    labels.insert(format!("{SERVERSET_ENDPOINT_LABEL_PREFIX}_host"), member.service_endpoint.host.clone());
    labels.insert(format!("{SERVERSET_ENDPOINT_LABEL_PREFIX}_port"), member.service_endpoint.port.to_string());
    for (name, endpoint) in &member.additional_endpoints {
        let clean = sanitize_label_name(name);
        labels.insert(format!("{SERVERSET_ENDPOINT_LABEL_PREFIX}_host_{clean}"), endpoint.host.clone());
        labels.insert(format!("{SERVERSET_ENDPOINT_LABEL_PREFIX}_port_{clean}"), endpoint.port.to_string());
    }
    labels.insert(SERVERSET_STATUS_LABEL.to_string(), member.status.clone());
    labels.insert(SERVERSET_SHARD_LABEL.to_string(), member.shard.to_string());
    Ok(labels)
}

pub const NERVE_PATH_LABEL: &str = "__meta_nerve_path";
pub const NERVE_ENDPOINT_LABEL_PREFIX: &str = "__meta_nerve_endpoint";

/// Go `nerveMember`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NerveMember {
    pub host: String,
    pub port: i64,
    pub name: String,
}
crate::go_json_struct!(NerveMember, "zookeeper", "nerveMember", [host: "host", port: "port", name: "name"]);

/// Go `parseNerveMember`.
pub fn parse_nerve_member(data: &[u8], path: &str) -> Result<LabelSet, String> {
    let mut member = NerveMember::default();
    gojson::unmarshal(data, &mut member).map_err(|e| format!("error unmarshaling nerve member {}: {e}", yaml::go_quote(path)))?;
    let mut labels = LabelSet::new();
    labels.insert(NERVE_PATH_LABEL.to_string(), path.to_string());
    labels.insert(ADDRESS_LABEL.to_string(), join_host_port(&member.host, member.port));
    labels.insert(format!("{NERVE_ENDPOINT_LABEL_PREFIX}_host"), member.host.clone());
    labels.insert(format!("{NERVE_ENDPOINT_LABEL_PREFIX}_port"), member.port.to_string());
    labels.insert(format!("{NERVE_ENDPOINT_LABEL_PREFIX}_name"), member.name.clone());
    Ok(labels)
}
