//! Port of `discovery/triton`: targets from the Triton (Joyent) cmon discovery endpoint, for containers or compute
//! nodes, refreshed at an interval.
//!
//! Differs from Go: a request in flight is not aborted when the discoverer is cancelled (a cancelled context is
//! checked before the request). Go's client has no timeout either.
use std::sync::Arc;
use std::time::Duration;

use model::labelset::{ADDRESS_LABEL, LabelSet};
use net::TlsConfig;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::metrics::Registry;
use crate::refresh::{self, Refresher};
use crate::targetgroup::Group;
use crate::{Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator, SdConfigType};

pub const TRITON_LABEL: &str = "__meta_triton_";
pub const TRITON_LABEL_GROUPS: &str = "__meta_triton_groups";
pub const TRITON_LABEL_MACHINE_ID: &str = "__meta_triton_machine_id";
pub const TRITON_LABEL_MACHINE_ALIAS: &str = "__meta_triton_machine_alias";
pub const TRITON_LABEL_MACHINE_BRAND: &str = "__meta_triton_machine_brand";
pub const TRITON_LABEL_MACHINE_IMAGE: &str = "__meta_triton_machine_image";
pub const TRITON_LABEL_SERVER_ID: &str = "__meta_triton_server_id";

/// Go `triton.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub account: String,
    pub role: String,
    pub dns_suffix: String,
    pub endpoint: String,
    pub groups: Vec<String>,
    pub port: i64,
    pub refresh_interval: yaml::Duration,
    pub tls_config: TlsConfig,
    pub version: i64,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig { role: "container".to_string(), port: 9163, refresh_interval: yaml::Duration::from_secs(60), version: 1, ..Default::default() }
}

yaml::yaml_struct!(SdConfig, "triton.plain", [
    (account: "account")
    (role: "role")
    (dns_suffix: "dns_suffix")
    (endpoint: "endpoint")
    (groups: "groups", omitempty)
    (port: "port")
    (refresh_interval: "refresh_interval", omitempty)
    (tls_config: "tls_config", omitempty)
    (version: "version")
]);

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            if self.role != "container" && self.role != "cn" {
                return Err(YError::Msg("triton SD configuration requires role to be 'container' or 'cn'".into()));
            }
            if self.account.is_empty() {
                return Err(YError::Msg("triton SD configuration requires an account".into()));
            }
            if self.dns_suffix.is_empty() {
                return Err(YError::Msg("triton SD configuration requires a dns_suffix".into()));
            }
            if self.endpoint.is_empty() {
                return Err(YError::Msg("triton SD configuration requires an endpoint".into()));
            }
            if self.refresh_interval.0 <= 0 {
                return Err(YError::Msg("triton SD configuration requires RefreshInterval to be a positive integer".into()));
            }
            Ok(())
        })
    }
    fn go_type() -> String {
        "triton.plain".to_string()
    }
}

impl yaml::Marshal for SdConfig {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        yaml::marshal_struct(self)
    }
    fn is_zero(&self) -> bool {
        yaml::Struct::fields_zero(self)
    }
}

impl SdConfigType for SdConfig {
    const GO_TYPE: &'static str = "triton.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "triton"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(&self, _reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
        Arc::new(TritonMetrics { refresh_metrics: rmi })
    }
    fn set_directory(&mut self, dir: &str) {
        self.tls_config.set_directory(dir);
    }
}

/// Go `tritonMetrics` (only the refresh metrics).
pub struct TritonMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
}

impl DiscovererMetrics for TritonMetrics {
    fn register(&self) -> Result<(), String> {
        Ok(())
    }
    fn unregister(&self) {}
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// The refresh state of the Triton discoverer (Go `Discovery` fields).
pub struct TritonRefresher {
    pub client: net::Client,
    pub interval: Duration,
    pub sd_config: SdConfig,
}

/// Go `triton.Discovery`.
pub struct Discovery {
    inner: refresh::Discovery<TritonRefresher>,
}

impl Discovery {
    /// Go `New`.
    pub fn new(conf: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<TritonMetrics>(&opts.metrics).ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let tls = net::tls::new_tls_config(&conf.tls_config, None)?;
        // Go: &http.Transport{TLSClientConfig: tls, DialContext: conntrack...}: no proxy, default pool settings.
        let transport = net::Transport::new(
            tls,
            None,
            net::Header::new(),
            net::TransportOptions { idle_conn_timeout: Duration::ZERO, max_idle_per_host: 2, ..Default::default() },
        )?;
        let client = net::Client::new(Arc::new(transport));
        let r = TritonRefresher { client, interval: conf.refresh_interval.to_std(), sd_config: conf.clone() };
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "triton",
            set_name: &opts.set_name,
            interval: conf.refresh_interval.to_std(),
            refresh_f: r,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(Discovery { inner })
    }

    /// Go `Discovery.refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.inner.refresher().refresh(ctx)
    }

    /// The refresh state (Go `d.client`, `d.interval`, `d.sdConfig`), for tests.
    pub fn state(&mut self) -> &mut TritonRefresher {
        self.inner.refresher()
    }
}

impl Discoverer for Discovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}

/// Go `encoding/json` field lookup: exact name first, then case-insensitive.
fn field<'a>(o: &'a json::Json, k: &str) -> Option<&'a json::Json> {
    crate::moby::types::get(o, k).filter(|v| !v.is_null())
}

fn json_str(o: &json::Json, k: &str, ty: &str) -> Result<String, String> {
    match field(o, k) {
        None => Ok(String::new()),
        Some(json::Json::Str(s)) => Ok(s.clone()),
        Some(v) => Err(format!("json: cannot unmarshal {} into Go struct field {ty}.{k} of type string", crate::moby::types::json_kind(v))),
    }
}

fn json_arr<'a>(o: &'a json::Json, k: &str, ty: &str, go_type: &str) -> Result<&'a [json::Json], String> {
    match field(o, k) {
        None => Ok(&[]),
        Some(json::Json::Arr(a)) => Ok(a),
        Some(v) => Err(format!("json: cannot unmarshal {} into Go struct field {ty}.{k} of type {go_type}", crate::moby::types::json_kind(v))),
    }
}

fn json_obj(v: &json::Json, ty: &str) -> Result<bool, String> {
    match v {
        json::Json::Null => Ok(false),
        json::Json::Obj(_) => Ok(true),
        other => Err(format!("json: cannot unmarshal {} into Go value of type {ty}", crate::moby::types::json_kind(other))),
    }
}

impl TritonRefresher {
    /// Go `processContainerResponse`.
    fn process_container_response(&self, data: &[u8], endpoint: &str) -> Result<Vec<Arc<Group>>, String> {
        let wrap = |e: String| format!("an error occurred unmarshaling the discovery response json: {e}");
        let mut tg = Group { source: endpoint.to_string(), ..Default::default() };
        let dr = json::Json::parse_bytes(data).map_err(|e| wrap(crate::moby::types::go_json_error(&e)))?;
        if json_obj(&dr, "triton.DiscoveryResponse").map_err(wrap)? {
            const TY: &str = "DiscoveryResponse.containers";
            for c in json_arr(&dr, "containers", "DiscoveryResponse", "[]struct { Groups []string \"json:\\\"groups\\\"\"; ServerUUID string \"json:\\\"server_uuid\\\"\"; VMAlias string \"json:\\\"vm_alias\\\"\"; VMBrand string \"json:\\\"vm_brand\\\"\"; VMImageUUID string \"json:\\\"vm_image_uuid\\\"\"; VMUUID string \"json:\\\"vm_uuid\\\"\" }").map_err(wrap)? {
                let is_obj = match c {
                    json::Json::Null => false,
                    json::Json::Obj(_) => true,
                    other => return Err(wrap(format!("json: cannot unmarshal {} into Go struct field {TY} of type struct", crate::moby::types::json_kind(other)))),
                };
                let s = |k: &str| if is_obj { json_str(c, k, TY) } else { Ok(String::new()) };
                let vm_uuid = s("vm_uuid").map_err(wrap)?;
                let mut labels = LabelSet::from_pairs(&[
                    (TRITON_LABEL_MACHINE_ID, &vm_uuid),
                    (TRITON_LABEL_MACHINE_ALIAS, &s("vm_alias").map_err(wrap)?),
                    (TRITON_LABEL_MACHINE_BRAND, &s("vm_brand").map_err(wrap)?),
                    (TRITON_LABEL_MACHINE_IMAGE, &s("vm_image_uuid").map_err(wrap)?),
                    (TRITON_LABEL_SERVER_ID, &s("server_uuid").map_err(wrap)?),
                ]);
                labels.insert(ADDRESS_LABEL.to_string(), format!("{vm_uuid}.{}:{}", self.sd_config.dns_suffix, self.sd_config.port));
                let mut groups: Vec<String> = Vec::new();
                if is_obj {
                    for g in json_arr(c, "groups", TY, "[]string").map_err(wrap)? {
                        match g {
                            json::Json::Str(g) => groups.push(g.clone()),
                            json::Json::Null => groups.push(String::new()),
                            other => return Err(wrap(format!("json: cannot unmarshal {} into Go struct field {TY}.groups of type string", crate::moby::types::json_kind(other)))),
                        }
                    }
                }
                if !groups.is_empty() {
                    labels.insert(TRITON_LABEL_GROUPS.to_string(), format!(",{},", groups.join(",")));
                }
                tg.targets.push(labels);
            }
        }
        Ok(vec![Arc::new(tg)])
    }

    /// Go `processComputeNodeResponse`.
    fn process_compute_node_response(&self, data: &[u8], endpoint: &str) -> Result<Vec<Arc<Group>>, String> {
        let wrap = |e: String| format!("an error occurred unmarshaling the compute node discovery response json: {e}");
        let mut tg = Group { source: endpoint.to_string(), ..Default::default() };
        let dr = json::Json::parse_bytes(data).map_err(|e| wrap(crate::moby::types::go_json_error(&e)))?;
        if json_obj(&dr, "triton.ComputeNodeDiscoveryResponse").map_err(wrap)? {
            const TY: &str = "ComputeNodeDiscoveryResponse.cns";
            for cn in json_arr(
                &dr,
                "cns",
                "ComputeNodeDiscoveryResponse",
                "[]struct { ServerUUID string \"json:\\\"server_uuid\\\"\"; ServerHostname string \"json:\\\"server_hostname\\\"\" }",
            )
            .map_err(wrap)?
            {
                let is_obj = match cn {
                    json::Json::Null => false,
                    json::Json::Obj(_) => true,
                    other => {
                        return Err(wrap(format!("json: cannot unmarshal {} into Go struct field {TY} of type struct", crate::moby::types::json_kind(other))));
                    }
                };
                let s = |k: &str| if is_obj { json_str(cn, k, TY) } else { Ok(String::new()) };
                let uuid = s("server_uuid").map_err(wrap)?;
                let mut labels = LabelSet::from_pairs(&[(TRITON_LABEL_MACHINE_ID, &uuid), (TRITON_LABEL_MACHINE_ALIAS, &s("server_hostname").map_err(wrap)?)]);
                labels.insert(ADDRESS_LABEL.to_string(), format!("{uuid}.{}:{}", self.sd_config.dns_suffix, self.sd_config.port));
                tg.targets.push(labels);
            }
        }
        Ok(vec![Arc::new(tg)])
    }
}

impl Refresher for TritonRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let c = &self.sd_config;
        let mut endpoint = match c.role.as_str() {
            "container" => format!("https://{}:{}/v{}/discover", c.endpoint, c.port, c.version),
            "cn" => format!("https://{}:{}/v{}/gz/discover", c.endpoint, c.port, c.version),
            r => return Err(format!("unknown role '{r}' in configuration")),
        };
        if !c.groups.is_empty() {
            let groups = yaml::url::query_escape(&c.groups.join(","));
            endpoint = format!("{endpoint}?groups={groups}");
        }
        let mut req = net::Request::new("GET", &endpoint, Vec::new()).map_err(|e| e.to_string())?;
        req.decompress = true;
        let wrap = |e: String| format!("an error occurred when requesting targets from the discovery endpoint: {e}");
        if ctx.is_cancelled() {
            return Err(wrap(format!("Get {}: context canceled", yaml::go_quote(&req.url.to_url_string()))));
        }
        let resp = self.client.do_request(req).map_err(|e| wrap(e.msg))?;
        match c.role.as_str() {
            "container" => self.process_container_response(&resp.body, &endpoint),
            "cn" => self.process_compute_node_response(&resp.body, &endpoint),
            r => Err(format!("unknown role '{r}' in configuration")),
        }
    }
}
