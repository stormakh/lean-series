//! Port of `discovery/openstack/hypervisor.go`: one target per Nova hypervisor (`os-hypervisors/detail`).
use std::sync::Arc;

use json::Json;
use model::labelset::{ADDRESS_LABEL, LabelSet};

use super::client::{AuthOptions, ProviderClient, Resource, Service, each_page, field, go_type_name, str_field};
use crate::cancel::CancelToken;
use crate::log::Logger;
use crate::refresh::Refresher;
use crate::targetgroup::Group;
use crate::util::join_host_port;

pub const LABEL_HYPERVISOR_ID: &str = "__meta_openstack_hypervisor_id";
pub const LABEL_HYPERVISOR_HOST_IP: &str = "__meta_openstack_hypervisor_host_ip";
pub const LABEL_HYPERVISOR_HOST_NAME: &str = "__meta_openstack_hypervisor_hostname";
pub const LABEL_HYPERVISOR_STATUS: &str = "__meta_openstack_hypervisor_status";
pub const LABEL_HYPERVISOR_STATE: &str = "__meta_openstack_hypervisor_state";
pub const LABEL_HYPERVISOR_TYPE: &str = "__meta_openstack_hypervisor_type";

/// Go `HypervisorDiscovery`.
pub struct HypervisorDiscovery {
    provider: ProviderClient,
    auth_opts: AuthOptions,
    region: String,
    #[allow(dead_code)]
    logger: Logger,
    port: i64,
    availability: String,
}

impl HypervisorDiscovery {
    pub fn new(provider: ProviderClient, auth_opts: AuthOptions, port: i64, region: String, availability: String, logger: Logger) -> HypervisorDiscovery {
        HypervisorDiscovery { provider, auth_opts, region, logger, port, availability }
    }
}

/// An id that gophercloud reads from `any`: a number (as an integer) or a string.
fn any_id(v: Option<&Json>) -> Result<String, String> {
    match v {
        Some(Json::Int(i)) => Ok(i.to_string()),
        Some(Json::Float(f)) => Ok((*f as i64).to_string()),
        Some(Json::Str(s)) => Ok(s.clone()),
        v => Err(format!("ID has unexpected type: {}", go_type_name(v))),
    }
}

/// The checks of gophercloud's `Hypervisor.UnmarshalJSON` (and `Service.UnmarshalJSON`) that can fail.
fn check_hypervisor(h: &Json) -> Result<(), String> {
    if let Some(Json::Obj(_)) = field(h, "service") {
        any_id(field(field(h, "service").unwrap(), "id"))?;
    }
    match field(h, "cpu_info") {
        None | Some(Json::Null) | Some(Json::Obj(_)) => {}
        Some(Json::Str(s)) => {
            if !s.is_empty() {
                Json::parse(s).map_err(|e| e.to_string())?;
            }
        }
        v => return Err(format!("CPUInfo has unexpected type: {}", go_type_name(v))),
    }
    match field(h, "hypervisor_version") {
        Some(Json::Int(_)) | Some(Json::Float(_)) => {}
        v => return Err(format!("HypervisorVersion has unexpected type: {}", go_type_name(v))),
    }
    for (k, name) in [("free_disk_gb", "FreeDiskGB"), ("local_gb", "LocalGB")] {
        match field(h, k) {
            None | Some(Json::Null) | Some(Json::Int(_)) | Some(Json::Float(_)) => {}
            v => return Err(format!("{name} has unexpected type: {}", go_type_name(v))),
        }
    }
    Ok(())
}

impl Refresher for HypervisorDiscovery {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.provider.authenticate(ctx, &self.auth_opts).map_err(|e| format!("could not authenticate to OpenStack: {e}"))?;
        let client = self.provider.service_client(ctx, Service::Compute, &self.region, &self.availability).map_err(|e| format!("could not create OpenStack compute session: {e}"))?;
        let mut tg = Group { targets: Vec::new(), labels: LabelSet::new(), source: format!("OS_{}", self.region) };
        let res = Resource { key: "hypervisors", links_key: "hypervisors_links", strict: false };
        let port = self.port.to_string();
        each_page(&self.provider, ctx, &client.service_url(&["os-hypervisors", "detail"]), &res, |items| {
            // IsEmpty extracts the page first: a decode error ends the paging without the "could not extract" prefix.
            for h in items {
                check_hypervisor(h)?;
            }
            for h in items {
                let ty = "Hypervisor";
                let host_ip = str_field(h, "host_ip", ty)?;
                let mut labels = LabelSet::new();
                labels.insert(ADDRESS_LABEL.to_string(), join_host_port(&host_ip, &port));
                labels.insert(LABEL_HYPERVISOR_ID.to_string(), any_id(field(h, "id"))?);
                labels.insert(LABEL_HYPERVISOR_HOST_NAME.to_string(), str_field(h, "hypervisor_hostname", ty)?);
                labels.insert(LABEL_HYPERVISOR_HOST_IP.to_string(), host_ip);
                labels.insert(LABEL_HYPERVISOR_STATUS.to_string(), str_field(h, "status", ty)?);
                labels.insert(LABEL_HYPERVISOR_STATE.to_string(), str_field(h, "state", ty)?);
                labels.insert(LABEL_HYPERVISOR_TYPE.to_string(), str_field(h, "hypervisor_type", ty)?);
                tg.targets.push(labels);
            }
            Ok(true)
        })?;
        Ok(vec![Arc::new(tg)])
    }
}
