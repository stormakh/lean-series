//! Port of `discovery/moby/services.go`: the `services` role of Docker Swarm discovery.
use std::collections::HashMap;
use std::sync::Arc;

use model::labelset::{ADDRESS_LABEL, LabelSet};

use super::client::Filters;
use super::dockerswarm::{SWARM_LABEL, SwarmRefresher};
use super::types::{PortConfig, Service, parse_cidr_ip, prefix_string};
use crate::cancel::CancelToken;
use crate::sdutil::{join_host_port, sanitize_label_name};
use crate::targetgroup::Group;

pub const SWARM_LABEL_SERVICE_ENDPOINT_PORT_NAME: &str = "__meta_dockerswarm_service_endpoint_port_name";
pub const SWARM_LABEL_SERVICE_ENDPOINT_PORT_PUBLISH_MODE: &str = "__meta_dockerswarm_service_endpoint_port_publish_mode";
pub const SWARM_LABEL_SERVICE_ID: &str = "__meta_dockerswarm_service_id";
pub const SWARM_LABEL_SERVICE_LABEL_PREFIX: &str = "__meta_dockerswarm_service_label_";
pub const SWARM_LABEL_SERVICE_NAME: &str = "__meta_dockerswarm_service_name";
pub const SWARM_LABEL_SERVICE_MODE: &str = "__meta_dockerswarm_service_mode";
pub const SWARM_LABEL_SERVICE_UPDATING_STATUS: &str = "__meta_dockerswarm_service_updating_status";
pub const SWARM_LABEL_SERVICE_TASK_CONTAINER_IMAGE: &str = "__meta_dockerswarm_service_task_container_image";
pub const SWARM_LABEL_SERVICE_TASK_CONTAINER_HOSTNAME: &str = "__meta_dockerswarm_service_task_container_hostname";

/// Go `network.TCP`.
pub const TCP: &str = "tcp";

impl SwarmRefresher {
    /// Go `refreshServices`.
    pub(super) fn refresh_services(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let mut tg = Group { source: "DockerSwarm".to_string(), ..Default::default() };
        let services = super::service_list(&mut self.client, ctx, &self.filters).map_err(|e| format!("error while listing swarm services: {e}"))?;
        let network_labels =
            super::network::get_networks_labels(&mut self.client, ctx, SWARM_LABEL).map_err(|e| format!("error while computing swarm network labels: {e}"))?;
        let empty = HashMap::new();

        for s in &services {
            let mut common_labels: Vec<(String, String)> = vec![
                (SWARM_LABEL_SERVICE_ID.to_string(), s.id.clone()),
                (SWARM_LABEL_SERVICE_NAME.to_string(), s.name.clone()),
                (SWARM_LABEL_SERVICE_MODE.to_string(), get_service_value_mode(s).to_string()),
            ];
            // ContainerSpec is only set for services whose task runtime is a container. It is nil for plugin
            // and network-attachment services.
            if let Some((hostname, image)) = &s.container_spec {
                common_labels.push((SWARM_LABEL_SERVICE_TASK_CONTAINER_HOSTNAME.to_string(), hostname.clone()));
                common_labels.push((SWARM_LABEL_SERVICE_TASK_CONTAINER_IMAGE.to_string(), image.clone()));
            }
            if let Some(st) = &s.update_status {
                common_labels.push((SWARM_LABEL_SERVICE_UPDATING_STATUS.to_string(), st.clone()));
            }
            for (k, v) in &s.labels {
                common_labels.push((format!("{SWARM_LABEL_SERVICE_LABEL_PREFIX}{}", sanitize_label_name(k)), v.clone()));
            }

            for p in &s.virtual_ips {
                let mut added = false;
                let addr_s = prefix_string(&p.addr);
                let ip = parse_cidr_ip(&addr_s).map_err(|e| format!("error while parsing address {addr_s}: {e}"))?;
                let net_labels = network_labels.get(&p.network_id).unwrap_or(&empty);

                for e in &s.ports {
                    if e.protocol != TCP {
                        continue;
                    }
                    let mut labels = LabelSet::new();
                    labels.insert(SWARM_LABEL_SERVICE_ENDPOINT_PORT_NAME.to_string(), e.name.clone());
                    labels.insert(SWARM_LABEL_SERVICE_ENDPOINT_PORT_PUBLISH_MODE.to_string(), e.publish_mode.clone());
                    for (k, v) in &common_labels {
                        labels.insert(k.clone(), v.clone());
                    }
                    for (k, v) in net_labels {
                        labels.insert(k.clone(), v.clone());
                    }
                    labels.insert(ADDRESS_LABEL.to_string(), join_host_port(&ip, e.published_port));
                    tg.targets.push(labels);
                    added = true;
                }

                if !added {
                    let mut labels = LabelSet::new();
                    for (k, v) in &common_labels {
                        labels.insert(k.clone(), v.clone());
                    }
                    for (k, v) in net_labels {
                        labels.insert(k.clone(), v.clone());
                    }
                    labels.insert(ADDRESS_LABEL.to_string(), join_host_port(&ip, self.port));
                    tg.targets.push(labels);
                }
            }
        }
        Ok(vec![Arc::new(tg)])
    }

    /// Go `getServicesLabelsAndPorts`.
    #[allow(clippy::type_complexity)]
    pub(super) fn get_services_labels_and_ports(
        &mut self,
        ctx: &CancelToken,
    ) -> Result<(HashMap<String, HashMap<String, String>>, HashMap<String, Vec<PortConfig>>), String> {
        let services = super::service_list(&mut self.client, ctx, &Filters::default())?;
        let mut services_labels = HashMap::with_capacity(services.len());
        let mut services_ports = HashMap::with_capacity(services.len());
        for s in services {
            let mut l = HashMap::new();
            l.insert(SWARM_LABEL_SERVICE_ID.to_string(), s.id.clone());
            l.insert(SWARM_LABEL_SERVICE_NAME.to_string(), s.name.clone());
            l.insert(SWARM_LABEL_SERVICE_MODE.to_string(), get_service_value_mode(&s).to_string());
            for (k, v) in &s.labels {
                l.insert(format!("{SWARM_LABEL_SERVICE_LABEL_PREFIX}{}", sanitize_label_name(k)), v.clone());
            }
            services_labels.insert(s.id.clone(), l);
            services_ports.insert(s.id.clone(), s.ports);
        }
        Ok((services_labels, services_ports))
    }
}

/// Go `getServiceValueMode`.
pub fn get_service_value_mode(s: &Service) -> &'static str {
    if s.mode_global {
        return "global";
    }
    if s.mode_replicated {
        return "replicated";
    }
    ""
}
