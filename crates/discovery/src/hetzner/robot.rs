//! Hetzner Robot role (Go `robot.go`): `GET <endpoint>/server`, a JSON list of `{"server": {...}}`.
use std::sync::Arc;

use json::Json;
use model::labelset::{ADDRESS_LABEL, LabelSet};

use super::*;
use crate::gojson::{self, Dec, key_is, sub};
use crate::sdutil::join_host_port;

const LABEL_ROBOT_DATACENTER: &str = "__meta_hetzner_robot_datacenter";
const LABEL_ROBOT_PRODUCT: &str = "__meta_hetzner_robot_product";
const LABEL_ROBOT_CANCELLED: &str = "__meta_hetzner_robot_cancelled";

// Go's names of the anonymous struct types of `serversList`, used in its type errors.
const SUBNET_TYPE: &str = r#"struct { IP string "json:\"ip\""; Mask string "json:\"mask\"" }"#;
const SERVER_TYPE: &str = r#"struct { ServerIP string "json:\"server_ip\""; ServerNumber int "json:\"server_number\""; ServerName string "json:\"server_name\""; Dc string "json:\"dc\""; Status string "json:\"status\""; Product string "json:\"product\""; Canceled bool "json:\"cancelled\""; Subnet []struct { IP string "json:\"ip\""; Mask string "json:\"mask\"" } "json:\"subnet\"" }"#;

/// Go `robotDiscovery`.
pub struct RobotDiscovery {
    client: net::Client,
    port: i64,
    endpoint: String,
}

/// Go `newRobotDiscovery`.
pub fn new_robot_discovery(conf: &SdConfig) -> Result<RobotDiscovery, String> {
    Ok(RobotDiscovery {
        client: http_client(conf)?,
        port: conf.port,
        endpoint: conf.robot_endpoint.clone(),
    })
}

#[derive(Default)]
struct Server {
    server_ip: String,
    server_number: i64,
    server_name: String,
    dc: String,
    status: String,
    product: String,
    cancelled: bool,
    subnet: Vec<(String, String)>,
}

/// `json.Unmarshal(b, &servers)` for Go `serversList`.
fn decode(v: &Json) -> Result<Vec<Server>, String> {
    let mut d = Dec::new();
    let mut out = Vec::new();
    let elem_type = format!("struct {{ Server {SERVER_TYPE} \"json:\\\"server\\\"\" }}");
    if let Some(a) = d.arr(v, "", "", "hetzner.serversList") {
        for e in a {
            let mut s = Server::default();
            if let Some(o) = d.obj(e, "", "", &elem_type) {
                for (k, sv) in o {
                    if !key_is(k, "server") {
                        continue;
                    }
                    let Some(so) = d.obj(sv, "", k, SERVER_TYPE) else {
                        continue;
                    };
                    for (k2, x) in so {
                        let p = sub(k, k2);
                        if key_is(k2, "server_ip") {
                            d.str(x, &mut s.server_ip, "", &p);
                        } else if key_is(k2, "server_number") {
                            d.int(x, &mut s.server_number, "", &p, "int");
                        } else if key_is(k2, "server_name") {
                            d.str(x, &mut s.server_name, "", &p);
                        } else if key_is(k2, "dc") {
                            d.str(x, &mut s.dc, "", &p);
                        } else if key_is(k2, "status") {
                            d.str(x, &mut s.status, "", &p);
                        } else if key_is(k2, "product") {
                            d.str(x, &mut s.product, "", &p);
                        } else if key_is(k2, "cancelled") {
                            d.bool(x, &mut s.cancelled, "", &p);
                        } else if key_is(k2, "subnet")
                            && let Some(sa) = d.arr(x, "", &p, &format!("[]{SUBNET_TYPE}"))
                        {
                            s.subnet = Vec::with_capacity(sa.len());
                            for se in sa {
                                let mut n = (String::new(), String::new());
                                for (k3, y) in d.obj(se, "", &p, SUBNET_TYPE).unwrap_or_default() {
                                    if key_is(k3, "ip") {
                                        d.str(y, &mut n.0, "", &sub(&p, k3));
                                    } else if key_is(k3, "mask") {
                                        d.str(y, &mut n.1, "", &sub(&p, k3));
                                    }
                                }
                                s.subnet.push(n);
                            }
                        }
                    }
                }
            }
            out.push(s);
        }
    }
    d.finish()?;
    Ok(out)
}

impl RobotDiscovery {
    /// Go `refresh`.
    pub fn refresh(&mut self, _ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let mut req =
            net::Request::get(&format!("{}/server", self.endpoint)).map_err(|e| e.to_string())?;
        req.header.add("User-Agent", crate::http::USER_AGENT);
        let resp = self.client.do_request(req).map_err(|e| e.to_string())?;
        if resp.status / 100 != 2 {
            return Err(format!(
                "non 2xx status '{}' response during hetzner service discovery with role robot",
                resp.status
            ));
        }
        let servers = decode(&gojson::parse(&resp.body, false)?)?;
        let port = (self.port as u64).to_string();
        let mut targets = Vec::with_capacity(servers.len());
        for s in servers {
            let dc = s.dc.to_lowercase();
            let mut labels = LabelSet::new();
            labels.insert(
                HETZNER_LABEL_ROLE.to_string(),
                HETZNER_ROLE_ROBOT.to_string(),
            );
            labels.insert(
                HETZNER_LABEL_SERVER_ID.to_string(),
                s.server_number.to_string(),
            );
            labels.insert(HETZNER_LABEL_SERVER_NAME.to_string(), s.server_name.clone());
            // label name kept for backward compatibility
            labels.insert(HETZNER_LABEL_DATACENTER.to_string(), dc.clone());
            labels.insert(HETZNER_LABEL_PUBLIC_IPV4.to_string(), s.server_ip.clone());
            labels.insert(HETZNER_LABEL_SERVER_STATUS.to_string(), s.status.clone());
            labels.insert(LABEL_ROBOT_DATACENTER.to_string(), dc);
            labels.insert(LABEL_ROBOT_PRODUCT.to_string(), s.product.clone());
            labels.insert(LABEL_ROBOT_CANCELLED.to_string(), s.cancelled.to_string());
            labels.insert(
                ADDRESS_LABEL.to_string(),
                join_host_port(&s.server_ip, &port),
            );
            for (ip, mask) in &s.subnet {
                // Go `net.ParseIP(ip).To4() == nil`: the first subnet that is not IPv4 (or does not parse)
                if !gojson::parse_ip(ip).is_some_and(|a| gojson::is_ipv4(&a)) {
                    labels.insert(
                        HETZNER_LABEL_PUBLIC_IPV6_NETWORK.to_string(),
                        format!("{ip}/{mask}"),
                    );
                    break;
                }
            }
            targets.push(labels);
        }
        Ok(vec![Arc::new(Group {
            source: "hetzner".to_string(),
            targets,
            labels: LabelSet::new(),
        })])
    }
}
