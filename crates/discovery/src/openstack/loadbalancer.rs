//! Port of `discovery/openstack/loadbalancer.go`: one target per Octavia load balancer that has a listener with
//! the PROMETHEUS protocol, with its floating IP from Neutron.
use std::collections::HashMap;
use std::sync::Arc;

use model::labelset::{ADDRESS_LABEL, LabelSet};

use super::LABEL_PROJECT_ID;
use super::client::{AuthOptions, ProviderClient, Resource, Service, all_pages, field, int_field, str_field, str_list_field};
use crate::cancel::CancelToken;
use crate::log::Logger;
use crate::refresh::Refresher;
use crate::targetgroup::Group;
use crate::util::join_host_port;

pub const LABEL_LOAD_BALANCER_ID: &str = "__meta_openstack_loadbalancer_id";
pub const LABEL_LOAD_BALANCER_NAME: &str = "__meta_openstack_loadbalancer_name";
pub const LABEL_LOAD_BALANCER_OPERATING_STATUS: &str = "__meta_openstack_loadbalancer_operating_status";
pub const LABEL_LOAD_BALANCER_PROVISIONING_STATUS: &str = "__meta_openstack_loadbalancer_provisioning_status";
pub const LABEL_LOAD_BALANCER_AVAILABILITY_ZONE: &str = "__meta_openstack_loadbalancer_availability_zone";
pub const LABEL_LOAD_BALANCER_FLOATING_IP: &str = "__meta_openstack_loadbalancer_floating_ip";
pub const LABEL_LOAD_BALANCER_VIP: &str = "__meta_openstack_loadbalancer_vip";
pub const LABEL_LOAD_BALANCER_PROVIDER: &str = "__meta_openstack_loadbalancer_provider";
pub const LABEL_LOAD_BALANCER_TAGS: &str = "__meta_openstack_loadbalancer_tags";

/// Go `LoadBalancerDiscovery`.
pub struct LoadBalancerDiscovery {
    provider: ProviderClient,
    auth_opts: AuthOptions,
    region: String,
    logger: Logger,
    availability: String,
}

impl LoadBalancerDiscovery {
    pub fn new(provider: ProviderClient, auth_opts: AuthOptions, region: String, availability: String, logger: Logger) -> LoadBalancerDiscovery {
        LoadBalancerDiscovery { provider, auth_opts, region, logger, availability }
    }
}

impl Refresher for LoadBalancerDiscovery {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self.provider.authenticate(ctx, &self.auth_opts).map_err(|e| format!("could not authenticate to OpenStack: {e}"))?;
        let client = self.provider.service_client(ctx, Service::LoadBalancer, &self.region, &self.availability).map_err(|e| format!("could not create OpenStack load balancer session: {e}"))?;
        let network = self.provider.service_client(ctx, Service::Network, &self.region, &self.availability).map_err(|e| format!("could not create OpenStack network session: {e}"))?;

        let lbs = all_pages(&self.provider, ctx, &client.service_url(&["lbaas", "loadbalancers"]), &Resource { key: "loadbalancers", links_key: "loadbalancers_links", strict: false }).map_err(|e| format!("failed to list load balancers: {e}"))?;
        let listeners = all_pages(&self.provider, ctx, &client.service_url(&["lbaas", "listeners"]), &Resource { key: "listeners", links_key: "listeners_links", strict: false }).map_err(|e| format!("failed to list all listeners: {e}"))?;

        // Listeners by load balancer ID: (protocol, port) in list order.
        let mut listener_map: HashMap<String, Vec<(String, i64)>> = HashMap::new();
        for l in &listeners {
            let ty = "Listener";
            let protocol = str_field(l, "protocol", ty).map_err(|e| format!("failed to list all listeners: {e}"))?;
            let port = int_field(l, "protocol_port", ty).map_err(|e| format!("failed to list all listeners: {e}"))?;
            if let Some(json::Json::Arr(ids)) = field(l, "loadbalancers") {
                for lb in ids {
                    let id = str_field(lb, "id", "LoadBalancerID").map_err(|e| format!("failed to list all listeners: {e}"))?;
                    listener_map.entry(id).or_default().push((protocol.clone(), port));
                }
            }
        }

        let fips = all_pages(&self.provider, ctx, &network.service_url(&["floatingips"]), &Resource { key: "floatingips", links_key: "floatingips_links", strict: false }).map_err(|e| format!("failed to list floating IPs: {e}"))?;
        let mut fip_map: HashMap<String, String> = HashMap::new();
        for f in &fips {
            let ty = "FloatingIP";
            let port_id = str_field(f, "port_id", ty).map_err(|e| format!("failed to list floating IPs: {e}"))?;
            if !port_id.is_empty() {
                fip_map.insert(port_id, str_field(f, "floating_ip_address", ty).map_err(|e| format!("failed to list floating IPs: {e}"))?);
            }
        }

        let mut tg = Group { targets: Vec::new(), labels: LabelSet::new(), source: format!("OS_{}", self.region) };
        for lb in &lbs {
            let ty = "LoadBalancer";
            let sf = |k: &str| str_field(lb, k, ty).map_err(|e| format!("failed to list load balancers: {e}"));
            let id = sf("id")?;
            let lb_listeners = match listener_map.get(&id) {
                Some(l) if !l.is_empty() => l,
                _ => {
                    self.logger.debug("Got no listener", &[("loadbalancer", &id)]);
                    continue;
                }
            };
            let Some((_, listener_port)) = lb_listeners.iter().find(|(p, _)| p == "PROMETHEUS") else {
                self.logger.debug("Got no PROMETHEUS listener", &[("loadbalancer", &id)]);
                continue;
            };
            let vip = sf("vip_address")?;
            let mut labels = LabelSet::new();
            labels.insert(ADDRESS_LABEL.to_string(), join_host_port(&vip, &listener_port.to_string()));
            labels.insert(LABEL_LOAD_BALANCER_ID.to_string(), id.clone());
            labels.insert(LABEL_LOAD_BALANCER_NAME.to_string(), sf("name")?);
            labels.insert(LABEL_LOAD_BALANCER_OPERATING_STATUS.to_string(), sf("operating_status")?);
            labels.insert(LABEL_LOAD_BALANCER_PROVISIONING_STATUS.to_string(), sf("provisioning_status")?);
            labels.insert(LABEL_LOAD_BALANCER_AVAILABILITY_ZONE.to_string(), sf("availability_zone")?);
            labels.insert(LABEL_LOAD_BALANCER_VIP.to_string(), vip);
            labels.insert(LABEL_LOAD_BALANCER_PROVIDER.to_string(), sf("provider")?);
            labels.insert(LABEL_PROJECT_ID.to_string(), sf("project_id")?);
            let tags = str_list_field(lb, "tags", ty).map_err(|e| format!("failed to list load balancers: {e}"))?;
            if !tags.is_empty() {
                labels.insert(LABEL_LOAD_BALANCER_TAGS.to_string(), tags.join(","));
            }
            if let Some(fip) = fip_map.get(&sf("vip_port_id")?) {
                labels.insert(LABEL_LOAD_BALANCER_FLOATING_IP.to_string(), fip.clone());
            }
            tg.targets.push(labels);
        }
        Ok(vec![Arc::new(tg)])
    }
}
