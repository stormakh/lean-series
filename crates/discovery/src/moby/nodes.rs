//! Port of `discovery/moby/nodes.go`: the `nodes` role of Docker Swarm discovery.
use std::collections::HashMap;
use std::sync::Arc;

use model::labelset::{ADDRESS_LABEL, LabelSet};

use super::client::Filters;
use super::dockerswarm::SwarmRefresher;
use crate::cancel::CancelToken;
use crate::sdutil::{join_host_port, sanitize_label_name};
use crate::targetgroup::Group;

pub const SWARM_LABEL_NODE_ADDRESS: &str = "__meta_dockerswarm_node_address";
pub const SWARM_LABEL_NODE_AVAILABILITY: &str = "__meta_dockerswarm_node_availability";
pub const SWARM_LABEL_NODE_ENGINE_VERSION: &str = "__meta_dockerswarm_node_engine_version";
pub const SWARM_LABEL_NODE_HOSTNAME: &str = "__meta_dockerswarm_node_hostname";
pub const SWARM_LABEL_NODE_ID: &str = "__meta_dockerswarm_node_id";
pub const SWARM_LABEL_NODE_LABEL_PREFIX: &str = "__meta_dockerswarm_node_label_";
pub const SWARM_LABEL_NODE_MANAGER_ADDR: &str = "__meta_dockerswarm_node_manager_address";
pub const SWARM_LABEL_NODE_MANAGER_LEADER: &str = "__meta_dockerswarm_node_manager_leader";
pub const SWARM_LABEL_NODE_MANAGER_REACHABILITY: &str = "__meta_dockerswarm_node_manager_reachability";
pub const SWARM_LABEL_NODE_PLATFORM_ARCHITECTURE: &str = "__meta_dockerswarm_node_platform_architecture";
pub const SWARM_LABEL_NODE_PLATFORM_OS: &str = "__meta_dockerswarm_node_platform_os";
pub const SWARM_LABEL_NODE_ROLE: &str = "__meta_dockerswarm_node_role";
pub const SWARM_LABEL_NODE_STATUS: &str = "__meta_dockerswarm_node_status";

impl SwarmRefresher {
    /// Go `refreshNodes`.
    pub(super) fn refresh_nodes(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let mut tg = Group { source: "DockerSwarm".to_string(), ..Default::default() };
        let nodes = super::node_list(&mut self.client, ctx, &self.filters).map_err(|e| format!("error while listing swarm nodes: {e}"))?;
        for n in nodes {
            let mut labels = LabelSet::from_pairs(&[
                (SWARM_LABEL_NODE_ID, &n.id),
                (SWARM_LABEL_NODE_ROLE, &n.role),
                (SWARM_LABEL_NODE_AVAILABILITY, &n.availability),
                (SWARM_LABEL_NODE_HOSTNAME, &n.hostname),
                (SWARM_LABEL_NODE_PLATFORM_ARCHITECTURE, &n.architecture),
                (SWARM_LABEL_NODE_PLATFORM_OS, &n.os),
                (SWARM_LABEL_NODE_ENGINE_VERSION, &n.engine_version),
                (SWARM_LABEL_NODE_STATUS, &n.state),
                (SWARM_LABEL_NODE_ADDRESS, &n.addr),
            ]);
            if let Some(m) = &n.manager_status {
                labels.insert(SWARM_LABEL_NODE_MANAGER_LEADER.to_string(), m.leader.to_string());
                labels.insert(SWARM_LABEL_NODE_MANAGER_REACHABILITY.to_string(), m.reachability.clone());
                labels.insert(SWARM_LABEL_NODE_MANAGER_ADDR.to_string(), m.addr.clone());
            }
            for (k, v) in &n.labels {
                labels.insert(format!("{SWARM_LABEL_NODE_LABEL_PREFIX}{}", sanitize_label_name(k)), v.clone());
            }
            labels.insert(ADDRESS_LABEL.to_string(), join_host_port(&n.addr, self.port as u64));
            tg.targets.push(labels);
        }
        Ok(vec![Arc::new(tg)])
    }

    /// Go `getNodesLabels`.
    pub(super) fn get_nodes_labels(&mut self, ctx: &CancelToken) -> Result<HashMap<String, HashMap<String, String>>, String> {
        let nodes = super::node_list(&mut self.client, ctx, &Filters::default()).map_err(|e| format!("error while listing swarm nodes: {e}"))?;
        let mut labels = HashMap::with_capacity(nodes.len());
        for n in nodes {
            let mut l: HashMap<String, String> = [
                (SWARM_LABEL_NODE_ID, &n.id),
                (SWARM_LABEL_NODE_ROLE, &n.role),
                (SWARM_LABEL_NODE_ADDRESS, &n.addr),
                (SWARM_LABEL_NODE_AVAILABILITY, &n.availability),
                (SWARM_LABEL_NODE_HOSTNAME, &n.hostname),
                (SWARM_LABEL_NODE_PLATFORM_ARCHITECTURE, &n.architecture),
                (SWARM_LABEL_NODE_PLATFORM_OS, &n.os),
                (SWARM_LABEL_NODE_STATUS, &n.state),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
            for (k, v) in &n.labels {
                l.insert(format!("{SWARM_LABEL_NODE_LABEL_PREFIX}{}", sanitize_label_name(k)), v.clone());
            }
            labels.insert(n.id.clone(), l);
        }
        Ok(labels)
    }
}
