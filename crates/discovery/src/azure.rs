//! Port of `discovery/azure`: virtual machines and scale set VMs of a subscription (or one resource group) from
//! the Azure Resource Manager REST API, with the private IP of the primary network interface as the target.
//!
//! The Go SDK layers are ported in `arm` (requests, paging, errors, models) and `identity` (credentials).
//! Differences from Go:
//! - Labels of VMs are computed by a pool of at most 16 threads (Go: one goroutine per VM); targets keep the
//!   order of the list calls (Go: completion order); with several failing VMs the first in list order is reported.
//! - A missing `id`, `name`, `type`, `location` (or scale set VM `instanceId`, NIC reference `id`, `vmSize` in
//!   a present hardware profile) is taken as "" where Go panics.
//! - The ARM resource provider registration policy (retrying after registering a provider on a 409
//!   `MissingSubscriptionRegistration`) is not ported; the 409 is returned as an error.
//! - JSON syntax error texts of malformed API responses differ from Go's `encoding/json` texts.
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use model::labelset::{ADDRESS_LABEL, LabelSet};
use net::HttpClientConfig;
use yaml::{Decoder, Error as YError, Node, Unmarshal};

pub mod arm;
pub mod identity;

use arm::{ArmClients, ArmError, ArmInterface, ArmScaleSet, ArmVirtualMachine, ResourceId};
use identity::{IdentityHttp, IdentityOptions, ManagedIdentityCredential, TokenCredential};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::metrics::{CounterVec, MetricRegisterer, Opts, Registry};
use crate::refresh::{self, Refresher};
use crate::targetgroup::Group;
use crate::{
    Config, Discoverer, DiscovererMetrics, DiscovererOptions, RefreshMetricsInstantiator,
    SdConfigType,
};

/// Go `version.PrometheusUserAgent()` (the SDK telemetry application ID).
pub const USER_AGENT: &str = "Prometheus/3.16.0";

pub const AZURE_LABEL: &str = "__meta_azure_";
pub const AZURE_LABEL_SUBSCRIPTION_ID: &str = "__meta_azure_subscription_id";
pub const AZURE_LABEL_TENANT_ID: &str = "__meta_azure_tenant_id";
pub const AZURE_LABEL_MACHINE_ID: &str = "__meta_azure_machine_id";
pub const AZURE_LABEL_MACHINE_RESOURCE_GROUP: &str = "__meta_azure_machine_resource_group";
pub const AZURE_LABEL_MACHINE_NAME: &str = "__meta_azure_machine_name";
pub const AZURE_LABEL_MACHINE_COMPUTER_NAME: &str = "__meta_azure_machine_computer_name";
pub const AZURE_LABEL_MACHINE_OS_TYPE: &str = "__meta_azure_machine_os_type";
pub const AZURE_LABEL_MACHINE_LOCATION: &str = "__meta_azure_machine_location";
pub const AZURE_LABEL_MACHINE_PRIVATE_IP: &str = "__meta_azure_machine_private_ip";
pub const AZURE_LABEL_MACHINE_PUBLIC_IP: &str = "__meta_azure_machine_public_ip";
pub const AZURE_LABEL_MACHINE_TAG: &str = "__meta_azure_machine_tag_";
pub const AZURE_LABEL_MACHINE_SCALE_SET: &str = "__meta_azure_machine_scale_set";
pub const AZURE_LABEL_MACHINE_SIZE: &str = "__meta_azure_machine_size";

pub const AUTH_METHOD_OAUTH: &str = "OAuth";
pub const AUTH_METHOD_SDK: &str = "SDK";
pub const AUTH_METHOD_MANAGED_IDENTITY: &str = "ManagedIdentity";
pub const AUTH_METHOD_WORKLOAD_IDENTITY: &str = "WorkloadIdentity";

/// Go `azure.SDConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SdConfig {
    pub environment: String,
    pub port: i64,
    pub subscription_id: String,
    pub tenant_id: String,
    pub client_id: String,
    pub client_secret: yaml::Secret,
    pub refresh_interval: yaml::Duration,
    pub authentication_method: String,
    pub resource_group: String,
    pub http_client_config: HttpClientConfig,
}

/// Go `DefaultSDConfig`.
pub fn default_sd_config() -> SdConfig {
    SdConfig {
        port: 80,
        refresh_interval: yaml::Duration::from_secs(300),
        environment: "AzurePublicCloud".to_string(),
        authentication_method: AUTH_METHOD_OAUTH.to_string(),
        http_client_config: HttpClientConfig::default_config(),
        ..Default::default()
    }
}

yaml::yaml_struct!(SdConfig, "azure.plain", [
    (environment: "environment", omitempty)
    (port: "port")
    (subscription_id: "subscription_id")
    (tenant_id: "tenant_id", omitempty)
    (client_id: "client_id", omitempty)
    (client_secret: "client_secret", omitempty)
    (refresh_interval: "refresh_interval", omitempty)
    (authentication_method: "authentication_method", omitempty)
    (resource_group: "resource_group", omitempty)
    (inline http_client_config)
]);

fn validate_auth_param(param: &str, name: &str) -> Result<(), YError> {
    if param.is_empty() {
        return Err(YError::Msg(format!(
            "azure SD configuration requires a {name}"
        )));
    }
    Ok(())
}

impl Unmarshal for SdConfig {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            *self = default_sd_config();
            d.inner_struct(n, self)?;
            validate_auth_param(&self.subscription_id, "subscription_id")?;
            if self.authentication_method == AUTH_METHOD_OAUTH {
                validate_auth_param(&self.tenant_id, "tenant_id")?;
                validate_auth_param(&self.client_id, "client_id")?;
                validate_auth_param(&self.client_secret.0, "client_secret")?;
            }
            let m = self.authentication_method.as_str();
            if m != AUTH_METHOD_OAUTH
                && m != AUTH_METHOD_MANAGED_IDENTITY
                && m != AUTH_METHOD_SDK
                && m != AUTH_METHOD_WORKLOAD_IDENTITY
            {
                return Err(YError::Msg(format!(
                    "unknown authentication_type {}. Supported types are {}, {}, {} or {}",
                    yaml::go_quote(m),
                    yaml::go_quote(AUTH_METHOD_OAUTH),
                    yaml::go_quote(AUTH_METHOD_MANAGED_IDENTITY),
                    yaml::go_quote(AUTH_METHOD_SDK),
                    yaml::go_quote(AUTH_METHOD_WORKLOAD_IDENTITY)
                )));
            }
            self.http_client_config.validate().map_err(YError::Msg)
        })
    }
    fn go_type() -> String {
        "azure.plain".to_string()
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
    const GO_TYPE: &'static str = "azure.SDConfig";
}

impl Config for SdConfig {
    fn name(&self) -> &'static str {
        "azure"
    }
    fn new_discoverer(&self, opts: DiscovererOptions) -> Result<Box<dyn Discoverer>, String> {
        Ok(Box::new(Discovery::new(self, opts)?))
    }
    fn new_discoverer_metrics(
        &self,
        reg: &Arc<Registry>,
        rmi: Arc<dyn RefreshMetricsInstantiator>,
    ) -> Arc<dyn DiscovererMetrics> {
        AzureMetrics::new(reg, rmi)
    }
    fn set_directory(&mut self, dir: &str) {
        self.http_client_config.set_directory(dir);
    }
}

/// Go `cloud.Configuration` with the resource manager service (the values azcore's `arm/runtime` sets).
#[derive(Clone, Debug, PartialEq)]
pub struct CloudConfiguration {
    pub active_directory_authority_host: String,
    pub resource_manager_endpoint: String,
    pub resource_manager_audience: String,
}

impl CloudConfiguration {
    pub fn azure_public() -> CloudConfiguration {
        CloudConfiguration {
            active_directory_authority_host: "https://login.microsoftonline.com/".into(),
            resource_manager_endpoint: "https://management.azure.com".into(),
            resource_manager_audience: "https://management.core.windows.net/".into(),
        }
    }
    pub fn azure_china() -> CloudConfiguration {
        CloudConfiguration {
            active_directory_authority_host: "https://login.chinacloudapi.cn/".into(),
            resource_manager_endpoint: "https://management.chinacloudapi.cn".into(),
            resource_manager_audience: "https://management.core.chinacloudapi.cn/".into(),
        }
    }
    pub fn azure_government() -> CloudConfiguration {
        CloudConfiguration {
            active_directory_authority_host: "https://login.microsoftonline.us/".into(),
            resource_manager_endpoint: "https://management.usgovcloudapi.net".into(),
            resource_manager_audience: "https://management.core.usgovcloudapi.net/".into(),
        }
    }
}

/// Go `CloudConfigurationFromName`.
pub fn cloud_configuration_from_name(name: &str) -> Result<CloudConfiguration, String> {
    let name = name.to_uppercase();
    match name.as_str() {
        "AZURECHINACLOUD" => Ok(CloudConfiguration::azure_china()),
        "AZURECLOUD" | "AZUREGERMANCLOUD" | "AZUREPUBLICCLOUD" => {
            Ok(CloudConfiguration::azure_public())
        }
        "AZUREUSGOVERNMENT" | "AZUREUSGOVERNMENTCLOUD" => {
            Ok(CloudConfiguration::azure_government())
        }
        _ => Err(format!(
            "there is no cloud configuration matching the name {}",
            yaml::go_quote(&name)
        )),
    }
}

/// Go `azureMetrics`.
pub struct AzureMetrics {
    pub refresh_metrics: Arc<dyn RefreshMetricsInstantiator>,
    pub failures_count: Arc<CounterVec>,
    pub cache_hit_count: Arc<CounterVec>,
    registerer: MetricRegisterer,
}

impl AzureMetrics {
    pub fn new(reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<AzureMetrics> {
        let failures_count = CounterVec::new_counter(Opts::new(
            "prometheus_sd_azure_failures_total",
            "Number of Azure service discovery refresh failures.",
        ));
        let cache_hit_count = CounterVec::new_counter(Opts::new(
            "prometheus_sd_azure_cache_hit_total",
            "Number of cache hit during refresh.",
        ));
        let registerer = MetricRegisterer::new(
            reg.clone(),
            vec![failures_count.clone(), cache_hit_count.clone()],
        );
        Arc::new(AzureMetrics {
            refresh_metrics: rmi,
            failures_count,
            cache_hit_count,
            registerer,
        })
    }
}

impl DiscovererMetrics for AzureMetrics {
    fn register(&self) -> Result<(), String> {
        self.registerer.register_metrics()
    }
    fn unregister(&self) {
        self.registerer.unregister_metrics()
    }
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }
}

/// Go `virtualMachine`: an Azure virtual machine (which can also be created by a VMSS).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct VirtualMachine {
    pub id: String,
    pub name: String,
    pub computer_name: String,
    pub type_: String,
    pub location: String,
    pub os_type: String,
    pub scale_set: String,
    pub instance_id: String,
    pub tags: BTreeMap<String, Option<String>>,
    pub network_interfaces: Vec<String>,
    pub size: String,
}

fn map_common(vm: &ArmVirtualMachine, scale_set: &str, instance_id: String) -> VirtualMachine {
    let mut out = VirtualMachine {
        id: vm.id.clone().unwrap_or_default(),
        name: vm.name.clone().unwrap_or_default(),
        type_: vm.type_.clone().unwrap_or_default(),
        location: vm.location.clone().unwrap_or_default(),
        scale_set: scale_set.to_string(),
        instance_id,
        tags: vm.tags.clone().unwrap_or_default(),
        ..Default::default()
    };
    if let Some(p) = &vm.properties {
        if let Some(t) = p
            .storage_profile
            .as_ref()
            .and_then(|s| s.os_disk.as_ref())
            .and_then(|d| d.os_type.as_ref())
        {
            out.os_type = t.clone();
        }
        if let Some(n) = &p.network_profile {
            for r in &n.network_interfaces {
                out.network_interfaces
                    .push(r.as_ref().and_then(|r| r.id.clone()).unwrap_or_default());
            }
        }
        if let Some(c) = p.os_profile.as_ref().and_then(|o| o.computer_name.as_ref()) {
            out.computer_name = c.clone();
        }
        if let Some(h) = &p.hardware_profile {
            out.size = h.vm_size.clone().unwrap_or_default();
        }
    }
    out
}

/// Go `mapFromVM`.
pub fn map_from_vm(vm: &ArmVirtualMachine) -> VirtualMachine {
    map_common(vm, "", String::new())
}

/// Go `mapFromVMScaleSetVM`.
pub fn map_from_vm_scale_set_vm(vm: &ArmVirtualMachine, scale_set_name: &str) -> VirtualMachine {
    map_common(
        vm,
        scale_set_name,
        vm.instance_id.clone().unwrap_or_default(),
    )
}

/// Go `newAzureResourceFromID`.
pub fn new_azure_resource_from_id(id: &str, logger: &Logger) -> Result<ResourceId, String> {
    arm::parse_resource_id(id).map_err(|e| {
        let err = format!("invalid ID '{id}': {e}");
        logger.error("Failed to parse resource ID", &[("err", &err)]);
        err
    })
}

/// The error of a network interface lookup: Go `errorNotFound` for a 404, or another error.
#[derive(Clone, Debug, PartialEq)]
pub enum NicError {
    NotFound,
    Other(String),
}

impl std::fmt::Display for NicError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NicError::NotFound => f.write_str("network interface does not exist"),
            NicError::Other(e) => f.write_str(e),
        }
    }
}

/// Go `client`: the calls discovery makes.
pub trait Client: Sync {
    fn get_vms(
        &self,
        ctx: &CancelToken,
        resource_group: &str,
    ) -> Result<Vec<VirtualMachine>, String>;
    fn get_scale_sets(
        &self,
        ctx: &CancelToken,
        resource_group: &str,
    ) -> Result<Vec<ArmScaleSet>, String>;
    fn get_scale_set_vms(
        &self,
        ctx: &CancelToken,
        scale_set: &ArmScaleSet,
    ) -> Result<Vec<VirtualMachine>, String>;
    fn get_vm_network_interface_by_id(
        &self,
        ctx: &CancelToken,
        network_interface_id: &str,
    ) -> Result<ArmInterface, NicError>;
    fn get_vm_scale_set_vm_network_interface_by_id(
        &self,
        ctx: &CancelToken,
        network_interface_id: &str,
        scale_set_name: &str,
        instance_id: &str,
    ) -> Result<ArmInterface, NicError>;
}

/// Go `azureClient`: the ARM SDK clients.
pub struct AzureClient {
    pub arm: ArmClients,
    pub logger: Logger,
}

fn nic_result(id: &str, r: Result<ArmInterface, ArmError>) -> Result<ArmInterface, NicError> {
    r.map_err(|e| {
        if e.status == Some(404) {
            NicError::NotFound
        } else {
            NicError::Other(format!("failed to retrieve Interface {id} with error: {e}"))
        }
    })
}

impl Client for AzureClient {
    fn get_vms(
        &self,
        ctx: &CancelToken,
        resource_group: &str,
    ) -> Result<Vec<VirtualMachine>, String> {
        let vms = self
            .arm
            .list_vms(ctx, resource_group)
            .map_err(|e| format!("could not list virtual machines: {e}"))?;
        Ok(vms.iter().map(map_from_vm).collect())
    }
    fn get_scale_sets(
        &self,
        ctx: &CancelToken,
        resource_group: &str,
    ) -> Result<Vec<ArmScaleSet>, String> {
        self.arm
            .list_scale_sets(ctx, resource_group)
            .map_err(|e| format!("could not list virtual machine scale sets: {e}"))
    }
    fn get_scale_set_vms(
        &self,
        ctx: &CancelToken,
        scale_set: &ArmScaleSet,
    ) -> Result<Vec<VirtualMachine>, String> {
        let r = new_azure_resource_from_id(scale_set.id.as_deref().unwrap_or(""), &self.logger)
            .map_err(|e| format!("could not parse scale set ID: {e}"))?;
        let name = scale_set.name.as_deref().unwrap_or("");
        let vms = self
            .arm
            .list_scale_set_vms(ctx, &r.resource_group_name, name)
            .map_err(|e| format!("could not list virtual machine scale set vms: {e}"))?;
        Ok(vms
            .iter()
            .map(|v| map_from_vm_scale_set_vm(v, name))
            .collect())
    }
    fn get_vm_network_interface_by_id(
        &self,
        ctx: &CancelToken,
        network_interface_id: &str,
    ) -> Result<ArmInterface, NicError> {
        let r = new_azure_resource_from_id(network_interface_id, &self.logger)
            .map_err(|e| NicError::Other(format!("could not parse network interface ID: {e}")))?;
        nic_result(
            network_interface_id,
            self.arm
                .get_interface_by_name(ctx, &r.resource_group_name, &r.name),
        )
    }
    fn get_vm_scale_set_vm_network_interface_by_id(
        &self,
        ctx: &CancelToken,
        network_interface_id: &str,
        scale_set_name: &str,
        instance_id: &str,
    ) -> Result<ArmInterface, NicError> {
        let r = new_azure_resource_from_id(network_interface_id, &self.logger)
            .map_err(|e| NicError::Other(format!("could not parse network interface ID: {e}")))?;
        nic_result(
            network_interface_id,
            self.arm.get_scale_set_interface(
                ctx,
                &r.resource_group_name,
                scale_set_name,
                instance_id,
                &r.name,
            ),
        )
    }
}

/// Overrides of the endpoints the provider calls, for tests and recorded-response runs (not YAML fields).
#[derive(Clone, Debug, Default)]
pub struct Hooks {
    /// Replaces the cloud configuration of `environment` (ARM endpoint, token audience, authority host).
    pub cloud: Option<CloudConfiguration>,
    /// Replaces the IMDS token endpoint.
    pub imds_endpoint: Option<String>,
    /// Allows plain-HTTP ARM endpoints and authority hosts.
    pub allow_http: bool,
}

/// Go `newCredential`.
pub fn new_credential(
    cfg: &SdConfig,
    opts: &IdentityOptions,
) -> Result<Arc<dyn TokenCredential>, String> {
    Ok(match cfg.authentication_method.as_str() {
        AUTH_METHOD_WORKLOAD_IDENTITY => Arc::new(identity::new_workload_identity_credential(
            "", "", "", opts,
        )?),
        AUTH_METHOD_MANAGED_IDENTITY => {
            let id = if cfg.client_id.is_empty() {
                None
            } else {
                Some(cfg.client_id.clone())
            };
            Arc::new(ManagedIdentityCredential::new(id, false, opts))
        }
        AUTH_METHOD_OAUTH => Arc::new(identity::new_client_secret_credential(
            &cfg.tenant_id,
            &cfg.client_id,
            &cfg.client_secret.0,
            opts,
        )?),
        AUTH_METHOD_SDK => identity::new_default_azure_credential(&cfg.tenant_id, opts)?,
        m => return Err(format!("unknown authentication_type {}", yaml::go_quote(m))),
    })
}

/// The network interface cache: LRU with a capacity and a lifetime per entry (Go go-generics-cache).
pub struct NicCache {
    cap: usize,
    tick: u64,
    map: HashMap<String, (Arc<ArmInterface>, Instant, u64)>,
    order: BTreeMap<u64, String>,
}

impl NicCache {
    pub fn new(cap: usize) -> NicCache {
        NicCache {
            cap,
            tick: 0,
            map: HashMap::new(),
            order: BTreeMap::new(),
        }
    }

    pub fn get(&mut self, k: &str) -> Option<Arc<ArmInterface>> {
        let (v, exp, t) = self.map.get(k)?.clone();
        if exp <= Instant::now() {
            self.order.remove(&t);
            self.map.remove(k);
            return None;
        }
        self.tick += 1;
        self.order.remove(&t);
        self.order.insert(self.tick, k.to_string());
        if let Some(e) = self.map.get_mut(k) {
            e.2 = self.tick;
        }
        Some(v)
    }

    pub fn set(&mut self, k: &str, v: Arc<ArmInterface>, ttl: Duration) {
        self.tick += 1;
        if let Some((_, _, t)) = self.map.get(k) {
            self.order.remove(&t.clone());
        }
        self.map
            .insert(k.to_string(), (v, Instant::now() + ttl, self.tick));
        self.order.insert(self.tick, k.to_string());
        while self.map.len() > self.cap {
            let Some((_, oldest)) = self.order.pop_first() else {
                break;
            };
            self.map.remove(&oldest);
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// The refresh function of the Azure discoverer (Go `Discovery.refresh` and its state).
pub struct AzureRefresher {
    cfg: SdConfig,
    port: i64,
    logger: Logger,
    cache: Mutex<NicCache>,
    metrics: Arc<AzureMetrics>,
    pub hooks: Hooks,
}

impl AzureRefresher {
    /// Go `createAzureClient`.
    pub fn create_azure_client(&self) -> Result<AzureClient, String> {
        let cloud = match &self.hooks.cloud {
            Some(c) => c.clone(),
            None => cloud_configuration_from_name(&self.cfg.environment)?,
        };
        let opts = IdentityOptions {
            authority_host: cloud.active_directory_authority_host.clone(),
            imds_endpoint: self
                .hooks
                .imds_endpoint
                .clone()
                .unwrap_or_else(|| identity::IMDS_ENDPOINT.to_string()),
            allow_http: self.hooks.allow_http,
            http: IdentityHttp::new(USER_AGENT)?,
        };
        let credential = new_credential(&self.cfg, &opts)?;
        let http = net::new_client_from_config(
            &self.cfg.http_client_config,
            &net::HttpClientOptions::default(),
        )
        .map_err(|e| e.to_string())?;
        let arm = ArmClients::new(
            &self.cfg.subscription_id,
            &cloud.resource_manager_endpoint,
            &cloud.resource_manager_audience,
            credential,
            http,
            USER_AGENT,
            self.hooks.allow_http,
        );
        Ok(AzureClient {
            arm,
            logger: self.logger.clone(),
        })
    }

    fn fail(&self, e: String) -> Result<Vec<Arc<Group>>, String> {
        self.metrics.failures_count.get().inc();
        Err(e)
    }

    /// Go `refreshAzureClient`.
    pub fn refresh_azure_client(
        &self,
        ctx: &CancelToken,
        client: &dyn Client,
    ) -> Result<Vec<Arc<Group>>, String> {
        let mut machines = match client.get_vms(ctx, &self.cfg.resource_group) {
            Ok(m) => m,
            Err(e) => return self.fail(format!("could not get virtual machines: {e}")),
        };
        self.logger.debug(
            "Found virtual machines during Azure discovery.",
            &[("count", &machines.len().to_string())],
        );
        let scale_sets = match client.get_scale_sets(ctx, &self.cfg.resource_group) {
            Ok(s) => s,
            Err(e) => return self.fail(format!("could not get virtual machine scale sets: {e}")),
        };
        for ss in &scale_sets {
            match client.get_scale_set_vms(ctx, ss) {
                Ok(v) => machines.extend(v),
                Err(e) => {
                    return self.fail(format!("could not get virtual machine scale set vms: {e}"));
                }
            }
        }
        type Slot = Mutex<Option<Result<Option<LabelSet>, String>>>;
        let results: Vec<Slot> = machines.iter().map(|_| Mutex::new(None)).collect();
        let next = AtomicUsize::new(0);
        let workers = machines.len().min(16);
        std::thread::scope(|s| {
            for _ in 0..workers {
                s.spawn(|| {
                    loop {
                        let i = next.fetch_add(1, Ordering::SeqCst);
                        if i >= machines.len() {
                            return;
                        }
                        let r = self.vm_to_label_set(ctx, client, &machines[i]);
                        *results[i].lock().unwrap_or_else(|e| e.into_inner()) = Some(r);
                    }
                });
            }
        });
        let mut tg = Group {
            targets: Vec::new(),
            labels: LabelSet::new(),
            source: String::new(),
        };
        for r in results {
            match r.into_inner().unwrap_or_else(|e| e.into_inner()) {
                Some(Err(e)) => {
                    return self.fail(format!("unable to complete Azure service discovery: {e}"));
                }
                Some(Ok(Some(ls))) => tg.targets.push(ls),
                _ => {}
            }
        }
        Ok(vec![Arc::new(tg)])
    }

    /// Go `vmToLabelSet`: `Ok(None)` skips the VM.
    pub fn vm_to_label_set(
        &self,
        ctx: &CancelToken,
        client: &dyn Client,
        vm: &VirtualMachine,
    ) -> Result<Option<LabelSet>, String> {
        let r = new_azure_resource_from_id(&vm.id, &self.logger)?;
        let mut labels = BTreeMap::new();
        labels.insert(
            AZURE_LABEL_SUBSCRIPTION_ID.to_string(),
            self.cfg.subscription_id.clone(),
        );
        labels.insert(
            AZURE_LABEL_TENANT_ID.to_string(),
            self.cfg.tenant_id.clone(),
        );
        labels.insert(AZURE_LABEL_MACHINE_ID.to_string(), vm.id.clone());
        labels.insert(AZURE_LABEL_MACHINE_NAME.to_string(), vm.name.clone());
        labels.insert(
            AZURE_LABEL_MACHINE_COMPUTER_NAME.to_string(),
            vm.computer_name.clone(),
        );
        labels.insert(AZURE_LABEL_MACHINE_OS_TYPE.to_string(), vm.os_type.clone());
        labels.insert(
            AZURE_LABEL_MACHINE_LOCATION.to_string(),
            vm.location.clone(),
        );
        labels.insert(
            AZURE_LABEL_MACHINE_RESOURCE_GROUP.to_string(),
            r.resource_group_name.clone(),
        );
        labels.insert(AZURE_LABEL_MACHINE_SIZE.to_string(), vm.size.clone());
        if !vm.scale_set.is_empty() {
            labels.insert(
                AZURE_LABEL_MACHINE_SCALE_SET.to_string(),
                vm.scale_set.clone(),
            );
        }
        for (k, v) in &vm.tags {
            // A tag with a JSON null value is skipped.
            let Some(v) = v else { continue };
            labels.insert(
                format!(
                    "{AZURE_LABEL_MACHINE_TAG}{}",
                    crate::util::sanitize_label_name(k)
                ),
                v.clone(),
            );
        }
        // Get the IP address information via separate call to the network provider.
        for nic_id in &vm.network_interfaces {
            let cached = self
                .cache
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(nic_id);
            let nic = match cached {
                Some(n) => {
                    self.metrics.cache_hit_count.get().add(1.0);
                    n
                }
                None => {
                    let r = if vm.scale_set.is_empty() {
                        client.get_vm_network_interface_by_id(ctx, nic_id)
                    } else {
                        client.get_vm_scale_set_vm_network_interface_by_id(
                            ctx,
                            nic_id,
                            &vm.scale_set,
                            &vm.instance_id,
                        )
                    };
                    match r {
                        Ok(n) => {
                            let n = Arc::new(n);
                            self.add_to_cache(nic_id, n.clone());
                            n
                        }
                        Err(NicError::NotFound) => {
                            self.logger.warn(
                                "Network interface does not exist",
                                &[
                                    ("name", nic_id),
                                    ("err", "network interface does not exist"),
                                ],
                            );
                            return Ok(None);
                        }
                        Err(NicError::Other(e)) => return Err(e),
                    }
                }
            };
            let Some(props) = &nic.properties else {
                continue;
            };
            // On deallocated machines the primary flag is not set.
            let Some(primary) = props.primary else {
                self.logger.debug(
                    "Skipping deallocated virtual machine",
                    &[("machine", &vm.name)],
                );
                return Ok(None);
            };
            if primary {
                // Go returns from inside its loop over the IP configurations: only the first one counts.
                if let Some(ip) = props.ip_configurations.first() {
                    let p = ip.as_ref().and_then(|i| i.properties.as_ref());
                    if let Some(addr) = p
                        .and_then(|p| p.public_ip_address.as_ref())
                        .and_then(|a| a.properties.as_ref())
                        .and_then(|a| a.ip_address.as_ref())
                    {
                        labels.insert(AZURE_LABEL_MACHINE_PUBLIC_IP.to_string(), addr.clone());
                    }
                    if let Some(private) = p.and_then(|p| p.private_ip_address.as_ref()) {
                        labels.insert(AZURE_LABEL_MACHINE_PRIVATE_IP.to_string(), private.clone());
                        labels.insert(
                            ADDRESS_LABEL.to_string(),
                            crate::util::join_host_port(private, &self.port.to_string()),
                        );
                        return Ok(Some(LabelSet(labels)));
                    }
                    // No private IP should be impossible: all or nothing.
                    return Err(format!("unable to find a private IP for VM {}", vm.name));
                }
            }
        }
        Ok(None)
    }

    /// Go `addToCache`: lives ten refresh intervals plus a random part of up to three.
    fn add_to_cache(&self, nic_id: &str, nic: Arc<ArmInterface>) {
        let ri = self.cfg.refresh_interval.to_std();
        let max = (ri.as_secs_f64() * 3.0) as u64;
        let mut b = [0u8; 8];
        crypto::random_bytes(&mut b);
        let random = if max > 0 {
            u64::from_le_bytes(b) % max
        } else {
            0
        };
        let exp = ri * 10 + Duration::from_secs(random);
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .set(nic_id, nic, exp);
        self.logger.debug(
            "Adding nic",
            &[("nic", nic_id), ("time", &exp.as_secs_f64().to_string())],
        );
    }
}

impl Refresher for AzureRefresher {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let client = match self.create_azure_client() {
            Ok(c) => c,
            Err(e) => {
                self.logger.debug("Azure discovery completed", &[]);
                return self.fail(format!("could not create Azure client: {e}"));
            }
        };
        let r = self.refresh_azure_client(ctx, &client);
        self.logger.debug("Azure discovery completed", &[]);
        r
    }
}

/// Go `azure.Discovery`.
pub struct Discovery {
    inner: refresh::Discovery<AzureRefresher>,
}

impl Discovery {
    /// Go `NewDiscovery`.
    pub fn new(cfg: &SdConfig, opts: DiscovererOptions) -> Result<Discovery, String> {
        let m = crate::downcast_metrics::<AzureMetrics>(&opts.metrics)
            .ok_or_else(|| "invalid discovery metrics type".to_string())?;
        let refresher = AzureRefresher {
            cfg: cfg.clone(),
            port: cfg.port,
            logger: opts.logger.clone(),
            cache: Mutex::new(NicCache::new(5000)),
            metrics: m.clone(),
            hooks: Hooks::default(),
        };
        let inner = refresh::Discovery::new(refresh::Options {
            logger: opts.logger,
            mech: "azure",
            set_name: &opts.set_name,
            interval: cfg.refresh_interval.to_std(),
            refresh_f: refresher,
            metrics_instantiator: m.refresh_metrics.as_ref(),
        });
        Ok(Discovery { inner })
    }

    /// Sets the endpoint overrides (tests and recorded-response runs).
    pub fn set_hooks(&mut self, hooks: Hooks) {
        self.inner.refresher().hooks = hooks;
    }

    /// The refresh function and its state.
    pub fn refresher(&mut self) -> &mut AzureRefresher {
        self.inner.refresher()
    }

    /// Go `Discovery.refresh`.
    pub fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.inner.refresher().refresh(ctx)
    }

    /// Go `d.metrics.failuresCount`.
    pub fn failures_count(&mut self) -> f64 {
        self.inner.refresher().metrics.failures_count.get().get()
    }

    /// Go `d.metrics.cacheHitCount`.
    pub fn cache_hit_count(&mut self) -> f64 {
        self.inner.refresher().metrics.cache_hit_count.get().get()
    }
}

impl Discoverer for Discovery {
    fn run(&mut self, ctx: &CancelToken, up: Sender) {
        self.inner.run(ctx, up)
    }
}
