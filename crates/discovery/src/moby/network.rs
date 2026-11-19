//! Port of `discovery/moby/network.go`: the labels of every Docker network, by network ID.
use std::collections::HashMap;

use super::client::Client;
use crate::cancel::CancelToken;
use crate::sdutil::sanitize_label_name;

pub const LABEL_NETWORK_ID: &str = "network_id";
pub const LABEL_NETWORK_NAME: &str = "network_name";
pub const LABEL_NETWORK_SCOPE: &str = "network_scope";
pub const LABEL_NETWORK_INTERNAL: &str = "network_internal";
pub const LABEL_NETWORK_INGRESS: &str = "network_ingress";
pub const LABEL_NETWORK_LABEL_PREFIX: &str = "network_label_";

/// Go `getNetworksLabels`: network ID -> labels (names prefixed with `label_prefix`).
pub fn get_networks_labels(c: &mut Client, ctx: &CancelToken, label_prefix: &str) -> Result<HashMap<String, HashMap<String, String>>, String> {
    let networks = super::network_list(c, ctx)?;
    let mut labels: HashMap<String, HashMap<String, String>> = HashMap::with_capacity(networks.len());
    for network in networks {
        let mut l = HashMap::new();
        l.insert(format!("{label_prefix}{LABEL_NETWORK_ID}"), network.id.clone());
        l.insert(format!("{label_prefix}{LABEL_NETWORK_NAME}"), network.name.clone());
        l.insert(format!("{label_prefix}{LABEL_NETWORK_SCOPE}"), network.scope.clone());
        l.insert(format!("{label_prefix}{LABEL_NETWORK_INTERNAL}"), network.internal.to_string());
        l.insert(format!("{label_prefix}{LABEL_NETWORK_INGRESS}"), network.ingress.to_string());
        for (k, v) in &network.labels {
            l.insert(format!("{label_prefix}{LABEL_NETWORK_LABEL_PREFIX}{}", sanitize_label_name(k)), v.clone());
        }
        labels.insert(network.id, l);
    }
    Ok(labels)
}
