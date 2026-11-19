//! Port of `discovery/moby/tasks.go`: the `tasks` role of Docker Swarm discovery.
use std::collections::HashMap;
use std::sync::Arc;

use model::labelset::{ADDRESS_LABEL, LabelSet};

use super::dockerswarm::{SWARM_LABEL, SwarmRefresher};
use super::nodes::SWARM_LABEL_NODE_ADDRESS;
use super::services::TCP;
use super::types::{parse_cidr_ip, prefix_string};
use crate::cancel::CancelToken;
use crate::sdutil::{join_host_port, sanitize_label_name};
use crate::targetgroup::Group;

pub const SWARM_LABEL_TASK_ID: &str = "__meta_dockerswarm_task_id";
pub const SWARM_LABEL_TASK_DESIRED_STATE: &str = "__meta_dockerswarm_task_desired_state";
pub const SWARM_LABEL_TASK_STATUS: &str = "__meta_dockerswarm_task_state";
pub const SWARM_LABEL_TASK_CONTAINER_ID: &str = "__meta_dockerswarm_task_container_id";
pub const SWARM_LABEL_TASK_SLOT: &str = "__meta_dockerswarm_task_slot";
pub const SWARM_LABEL_TASK_PORT_MODE: &str = "__meta_dockerswarm_task_port_publish_mode";
pub const SWARM_LABEL_CONTAINER_LABEL_PREFIX: &str = "__meta_dockerswarm_container_label_";

/// `maps.Copy(dst, src)`.
fn copy_into(dst: &mut HashMap<String, String>, src: Option<&HashMap<String, String>>) {
    if let Some(src) = src {
        for (k, v) in src {
            dst.insert(k.clone(), v.clone());
        }
    }
}

impl SwarmRefresher {
    /// Go `refreshTasks`.
    pub(super) fn refresh_tasks(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let mut tg = Group { source: "DockerSwarm".to_string(), ..Default::default() };
        let tasks = super::task_list(&mut self.client, ctx, &self.filters).map_err(|e| format!("error while listing swarm services: {e}"))?;
        let (service_labels, service_ports) =
            self.get_services_labels_and_ports(ctx).map_err(|e| format!("error while computing services labels and ports: {e}"))?;
        let node_labels = self.get_nodes_labels(ctx).map_err(|e| format!("error while computing nodes labels and ports: {e}"))?;
        let network_labels =
            super::network::get_networks_labels(&mut self.client, ctx, SWARM_LABEL).map_err(|e| format!("error while computing swarm network labels: {e}"))?;
        let empty_labels = HashMap::new();

        for s in &tasks {
            let mut common_labels: HashMap<String, String> = HashMap::new();
            common_labels.insert(SWARM_LABEL_TASK_ID.to_string(), s.id.clone());
            common_labels.insert(SWARM_LABEL_TASK_DESIRED_STATE.to_string(), s.desired_state.clone());
            common_labels.insert(SWARM_LABEL_TASK_STATUS.to_string(), s.state.clone());
            common_labels.insert(SWARM_LABEL_TASK_SLOT.to_string(), s.slot.to_string());

            if let Some(id) = &s.container_id {
                common_labels.insert(SWARM_LABEL_TASK_CONTAINER_ID.to_string(), id.clone());
            }
            if let Some(cl) = &s.container_labels {
                for (k, v) in cl {
                    common_labels.insert(format!("{SWARM_LABEL_CONTAINER_LABEL_PREFIX}{}", sanitize_label_name(k)), v.clone());
                }
            }
            copy_into(&mut common_labels, service_labels.get(&s.service_id));
            copy_into(&mut common_labels, node_labels.get(&s.node_id));

            for p in &s.port_status {
                if p.protocol != TCP {
                    continue;
                }
                let mut labels = LabelSet::new();
                labels.insert(SWARM_LABEL_TASK_PORT_MODE.to_string(), p.publish_mode.clone());
                for (k, v) in &common_labels {
                    labels.insert(k.clone(), v.clone());
                }
                let node_addr = labels.get(SWARM_LABEL_NODE_ADDRESS).cloned().unwrap_or_default();
                labels.insert(ADDRESS_LABEL.to_string(), join_host_port(&node_addr, p.published_port));
                tg.targets.push(labels);
            }

            for network in &s.networks_attachments {
                let net_labels = network_labels.get(&network.network_id).unwrap_or(&empty_labels);
                for address in &network.addresses {
                    let mut added = false;
                    let addr_s = prefix_string(address);
                    let ip = parse_cidr_ip(&addr_s).map_err(|e| format!("error while parsing address {addr_s}: {e}"))?;

                    for p in service_ports.get(&s.service_id).map(|v| v.as_slice()).unwrap_or(&[]) {
                        if p.protocol != TCP {
                            continue;
                        }
                        let mut labels = LabelSet::new();
                        labels.insert(SWARM_LABEL_TASK_PORT_MODE.to_string(), p.publish_mode.clone());
                        for (k, v) in &common_labels {
                            labels.insert(k.clone(), v.clone());
                        }
                        for (k, v) in net_labels {
                            labels.insert(k.clone(), v.clone());
                        }
                        labels.insert(ADDRESS_LABEL.to_string(), join_host_port(&ip, p.published_port));
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
        }
        Ok(vec![Arc::new(tg)])
    }
}
