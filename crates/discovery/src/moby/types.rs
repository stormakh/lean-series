//! The Docker Engine API types the discoverers read (`github.com/moby/moby/api/types` v1.56.1: container
//! summaries, networks, swarm nodes, services and tasks), decoded from JSON as Go's `encoding/json` does for the
//! fields used: names match case-insensitively, `null` and missing fields are zero values, pointers are
//! `Option`s, `netip.Addr` and `netip.Prefix` parse from text.
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use json::Json;

pub type Labels = Vec<(String, String)>;

/// Go encoding/json field lookup: exact name first, then case-insensitive.
pub fn get<'a>(o: &'a Json, k: &str) -> Option<&'a Json> {
    let obj = o.as_obj()?;
    let mut found = None;
    for (n, v) in obj {
        if n == k {
            found = Some(v);
        }
    }
    if found.is_some() {
        return found;
    }
    for (n, v) in obj {
        if n.eq_ignore_ascii_case(k) {
            found = Some(v);
        }
    }
    found
}

/// The JSON kind as Go names it in unmarshal errors.
pub fn json_kind(v: &Json) -> &'static str {
    match v {
        Json::Null => "null",
        Json::Bool(_) => "bool",
        Json::Int(_) | Json::Float(_) => "number",
        Json::Str(_) => "string",
        Json::Arr(_) => "array",
        Json::Obj(_) => "object",
    }
}

/// A syntax error in Go's words (the offset form of this JSON parser).
pub fn go_json_error(e: &json::JsonError) -> String {
    format!("invalid character: {} at offset {}", e.msg, e.offset)
}

fn type_err(v: &Json, ty: &str, field: &str, go_type: &str) -> String {
    format!("json: cannot unmarshal {} into Go struct field {ty}.{field} of type {go_type}", json_kind(v))
}

/// A decoder for one Go struct type (for error texts).
struct Obj<'a> {
    v: &'a Json,
    ty: &'static str,
}

impl<'a> Obj<'a> {
    fn new(v: &'a Json, ty: &'static str, path: &str) -> Result<Option<Obj<'a>>, String> {
        match v {
            Json::Null => Ok(None),
            Json::Obj(_) => Ok(Some(Obj { v, ty })),
            other => Err(if path.is_empty() {
                format!("json: cannot unmarshal {} into Go value of type {ty}", json_kind(other))
            } else {
                format!("json: cannot unmarshal {} into Go struct field {path} of type {ty}", json_kind(other))
            }),
        }
    }
    fn field(&self, k: &str) -> Option<&'a Json> {
        get(self.v, k).filter(|v| !v.is_null())
    }
    fn path(&self, k: &str) -> String {
        format!("{}.{k}", self.ty.rsplit('.').next().unwrap_or(self.ty))
    }
    fn str(&self, k: &str) -> Result<String, String> {
        match self.field(k) {
            None => Ok(String::new()),
            Some(Json::Str(s)) => Ok(s.clone()),
            Some(v) => Err(type_err(v, self.short(), k, "string")),
        }
    }
    fn short(&self) -> &str {
        self.ty.rsplit('.').next().unwrap_or(self.ty)
    }
    fn bool(&self, k: &str) -> Result<bool, String> {
        match self.field(k) {
            None => Ok(false),
            Some(Json::Bool(b)) => Ok(*b),
            Some(v) => Err(type_err(v, self.short(), k, "bool")),
        }
    }
    fn int(&self, k: &str, go_type: &str, min: i64, max: i64) -> Result<i64, String> {
        match self.field(k) {
            None => Ok(0),
            Some(Json::Int(i)) if *i >= min && *i <= max => Ok(*i),
            Some(Json::Float(f)) if f.fract() == 0.0 && *f >= min as f64 && *f <= max as f64 => Ok(*f as i64),
            Some(v @ (Json::Int(_) | Json::Float(_))) => {
                Err(format!("json: cannot unmarshal number {} into Go struct field {}.{k} of type {go_type}", v.stringify(), self.short()))
            }
            Some(v) => Err(type_err(v, self.short(), k, go_type)),
        }
    }
    fn labels(&self, k: &str) -> Result<Labels, String> {
        match self.field(k) {
            None => Ok(Vec::new()),
            Some(Json::Obj(o)) => {
                let mut out: Labels = Vec::with_capacity(o.len());
                for (n, v) in o {
                    match v {
                        Json::Str(s) => {
                            // later duplicates win, as in a Go map
                            out.retain(|(x, _)| x != n);
                            out.push((n.clone(), s.clone()));
                        }
                        Json::Null => {
                            out.retain(|(x, _)| x != n);
                            out.push((n.clone(), String::new()));
                        }
                        other => return Err(type_err(other, self.short(), k, "string")),
                    }
                }
                Ok(out)
            }
            Some(v) => Err(type_err(v, self.short(), k, "map[string]string")),
        }
    }
    fn obj(&self, k: &str, ty: &'static str) -> Result<Option<Obj<'a>>, String> {
        match self.field(k) {
            None => Ok(None),
            Some(v) => Obj::new(v, ty, &self.path(k)),
        }
    }
    fn arr(&self, k: &str, go_type: &str) -> Result<&'a [Json], String> {
        match self.field(k) {
            None => Ok(&[]),
            Some(Json::Arr(a)) => Ok(a),
            Some(v) => Err(type_err(v, self.short(), k, go_type)),
        }
    }
    fn strs(&self, k: &str) -> Result<Vec<String>, String> {
        let mut out = Vec::new();
        for v in self.arr(k, "[]string")? {
            match v {
                Json::Str(s) => out.push(s.clone()),
                Json::Null => out.push(String::new()),
                other => return Err(type_err(other, self.short(), k, "string")),
            }
        }
        Ok(out)
    }
    fn addr(&self, k: &str) -> Result<Option<Addr>, String> {
        match self.field(k) {
            None => Ok(None),
            Some(Json::Str(s)) => Addr::unmarshal_text(s),
            Some(v) => Err(type_err(v, self.short(), k, "netip.Addr")),
        }
    }
    fn prefix(v: &Json, ty: &str, k: &str) -> Result<Option<Prefix>, String> {
        match v {
            Json::Null => Ok(None),
            Json::Str(s) => Prefix::unmarshal_text(s),
            v => Err(type_err(v, ty, k, "netip.Prefix")),
        }
    }
}

fn arr_of<'a>(v: &'a Json, go_type: &str) -> Result<&'a [Json], String> {
    match v {
        Json::Null => Ok(&[]),
        Json::Arr(a) => Ok(a),
        other => Err(format!("json: cannot unmarshal {} into Go value of type {go_type}", json_kind(other))),
    }
}

/// Parses a response body (Go `json.NewDecoder(body).Decode(&v)`).
pub fn parse_body(b: &[u8]) -> Result<Json, String> {
    let t = b.trim_ascii();
    if t.is_empty() {
        return Err("EOF".to_string());
    }
    Json::parse_bytes(t).map_err(|e| go_json_error(&e))
}

// ---------------- netip ----------------

/// Go `netip.Addr` (valid addresses only; the zero Addr is `None` where used).
#[derive(Clone, Debug, PartialEq)]
pub struct Addr {
    pub ip: IpAddr,
    pub zone: String,
}

impl Addr {
    /// Go `netip.ParseAddr`.
    pub fn parse(s: &str) -> Result<Addr, String> {
        let err = |m: &str| format!("ParseAddr({}): {m}", yaml::go_quote(s));
        if s.contains(':') {
            let (a, zone) = match s.split_once('%') {
                Some((a, z)) => {
                    if z.is_empty() {
                        return Err(err("zone must be a non-empty string"));
                    }
                    (a, z)
                }
                None => (s, ""),
            };
            let ip: Ipv6Addr = a.parse().map_err(|_| err("unable to parse IP"))?;
            Ok(Addr { ip: IpAddr::V6(ip), zone: zone.to_string() })
        } else if s.contains('.') {
            let ip: Ipv4Addr = s.parse().map_err(|_| err("unable to parse IP"))?;
            Ok(Addr { ip: IpAddr::V4(ip), zone: String::new() })
        } else {
            Err(err("unable to parse IP"))
        }
    }

    /// Go `Addr.UnmarshalText`: "" is the zero Addr.
    fn unmarshal_text(s: &str) -> Result<Option<Addr>, String> {
        if s.is_empty() {
            return Ok(None);
        }
        Addr::parse(s).map(Some)
    }
}

/// Go `netip.Addr.String`.
impl std::fmt::Display for Addr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.ip {
            IpAddr::V4(v) => write!(f, "{v}"),
            IpAddr::V6(v) => {
                write!(f, "{v}")?;
                if !self.zone.is_empty() {
                    write!(f, "%{}", self.zone)?;
                }
                Ok(())
            }
        }
    }
}

/// Go `netip.Prefix` (valid prefixes; the zero Prefix is `None` where used).
#[derive(Clone, Debug, PartialEq)]
pub struct Prefix {
    pub addr: Addr,
    pub bits: u8,
}

impl Prefix {
    /// Go `netip.ParsePrefix`.
    pub fn parse(s: &str) -> Result<Prefix, String> {
        let err = |m: String| format!("netip.ParsePrefix({}): {m}", yaml::go_quote(s));
        let (a, b) = s.rsplit_once('/').ok_or_else(|| err("no '/'".to_string()))?;
        let addr = Addr::parse(a).map_err(err)?;
        if !addr.zone.is_empty() {
            return Err(err("IPv6 zones cannot be present in a prefix".to_string()));
        }
        let max = if addr.ip.is_ipv4() { 32 } else { 128 };
        let bad = || err(format!("bad bits after slash: {}", yaml::go_quote(b)));
        if b.is_empty() || !b.bytes().all(|c| c.is_ascii_digit()) || (b.len() > 1 && b.starts_with('0')) {
            return Err(bad());
        }
        let bits: u32 = b.parse().map_err(|_| bad())?;
        if bits > max {
            return Err(err("prefix length out of range".to_string()));
        }
        Ok(Prefix { addr, bits: bits as u8 })
    }

    fn unmarshal_text(s: &str) -> Result<Option<Prefix>, String> {
        if s.is_empty() {
            return Ok(None);
        }
        Prefix::parse(s).map(Some)
    }
}

/// Go `netip.Prefix.String` for a possibly zero prefix.
pub fn prefix_string(p: &Option<Prefix>) -> String {
    match p {
        Some(p) => format!("{}/{}", p.addr, p.bits),
        None => "invalid Prefix".to_string(),
    }
}

/// Go `ip, _, err := net.ParseCIDR(s)` followed by `ip.String()`.
pub fn parse_cidr_ip(s: &str) -> Result<String, String> {
    let err = || format!("invalid CIDR address: {s}");
    let (a, b) = s.split_once('/').ok_or_else(err)?;
    if a.contains('%') {
        return Err(err());
    }
    let ip: IpAddr = a.parse().map_err(|_| err())?;
    let max = if ip.is_ipv4() { 32 } else { 128 };
    if b.is_empty() || b.len() > 3 || !b.bytes().all(|c| c.is_ascii_digit()) || b.parse::<u32>().map_or(true, |n| n > max) {
        return Err(err());
    }
    Ok(match ip {
        IpAddr::V4(v) => v.to_string(),
        IpAddr::V6(v) => match v.to_ipv4_mapped() {
            Some(v4) => v4.to_string(),
            None => v.to_string(),
        },
    })
}

// ---------------- container ----------------

/// Go `container.PortSummary`.
#[derive(Clone, Debug, Default)]
pub struct PortSummary {
    pub ip: Option<Addr>,
    pub private_port: u16,
    pub public_port: u16,
    pub type_: String,
}

/// Go `network.EndpointSettings` (fields used).
#[derive(Clone, Debug, Default)]
pub struct EndpointSettings {
    pub network_id: String,
    pub ip_address: Option<Addr>,
    pub global_ipv6_address: Option<Addr>,
}

/// Go `container.Summary` (fields used).
#[derive(Clone, Debug, Default)]
pub struct ContainerSummary {
    pub id: String,
    pub names: Vec<String>,
    pub image: String,
    pub image_id: String,
    pub ports: Vec<PortSummary>,
    pub labels: Labels,
    pub network_mode: String,
    /// `NetworkSettings.Networks` in JSON order; `None` values are null entries.
    pub networks: Vec<(String, Option<EndpointSettings>)>,
}

fn endpoint_settings(v: &Json) -> Result<Option<EndpointSettings>, String> {
    let Some(o) = Obj::new(v, "network.EndpointSettings", "NetworkSettingsSummary.Networks")? else { return Ok(None) };
    Ok(Some(EndpointSettings { network_id: o.str("NetworkID")?, ip_address: o.addr("IPAddress")?, global_ipv6_address: o.addr("GlobalIPv6Address")? }))
}

/// Decodes `[]container.Summary`.
pub fn container_list(v: &Json) -> Result<Vec<ContainerSummary>, String> {
    let mut out = Vec::new();
    for c in arr_of(v, "[]container.Summary")? {
        let Some(o) = Obj::new(c, "container.Summary", "")? else {
            out.push(ContainerSummary::default());
            continue;
        };
        let mut s = ContainerSummary {
            id: o.str("Id")?,
            names: o.strs("Names")?,
            image: o.str("Image")?,
            image_id: o.str("ImageID")?,
            labels: o.labels("Labels")?,
            ..Default::default()
        };
        for p in o.arr("Ports", "[]container.PortSummary")? {
            let Some(po) = Obj::new(p, "container.PortSummary", "Summary.Ports")? else {
                s.ports.push(PortSummary::default());
                continue;
            };
            s.ports.push(PortSummary {
                ip: po.addr("IP")?,
                private_port: po.int("PrivatePort", "uint16", 0, u16::MAX as i64)? as u16,
                public_port: po.int("PublicPort", "uint16", 0, u16::MAX as i64)? as u16,
                type_: po.str("Type")?,
            });
        }
        if let Some(hc) =
            o.obj("HostConfig", "struct { NetworkMode string \"json:\\\",omitempty\\\"\"; Annotations map[string]string \"json:\\\",omitempty\\\"\" }")?
        {
            s.network_mode = hc.str("NetworkMode")?;
        }
        if let Some(ns) = o.obj("NetworkSettings", "container.NetworkSettingsSummary")? {
            match ns.field("Networks") {
                None => {}
                Some(Json::Obj(m)) => {
                    for (k, v) in m {
                        let e = endpoint_settings(v)?;
                        match s.networks.iter_mut().find(|(n, _)| n == k) {
                            Some(slot) => slot.1 = e,
                            None => s.networks.push((k.clone(), e)),
                        }
                    }
                }
                Some(v) => return Err(type_err(v, "NetworkSettingsSummary", "Networks", "map[string]*network.EndpointSettings")),
            }
        }
        out.push(s);
    }
    Ok(out)
}

// ---------------- network ----------------

/// Go `network.Summary` (fields used).
#[derive(Clone, Debug, Default)]
pub struct NetworkSummary {
    pub id: String,
    pub name: String,
    pub scope: String,
    pub internal: bool,
    pub ingress: bool,
    pub labels: Labels,
}

/// Decodes `[]network.Summary`.
pub fn network_list(v: &Json) -> Result<Vec<NetworkSummary>, String> {
    let mut out = Vec::new();
    for n in arr_of(v, "[]network.Summary")? {
        let Some(o) = Obj::new(n, "network.Summary", "")? else {
            out.push(NetworkSummary::default());
            continue;
        };
        out.push(NetworkSummary {
            id: o.str("Id")?,
            name: o.str("Name")?,
            scope: o.str("Scope")?,
            internal: o.bool("Internal")?,
            ingress: o.bool("Ingress")?,
            labels: o.labels("Labels")?,
        });
    }
    Ok(out)
}

// ---------------- swarm ----------------

/// Go `swarm.ManagerStatus`.
#[derive(Clone, Debug, Default)]
pub struct ManagerStatus {
    pub leader: bool,
    pub reachability: String,
    pub addr: String,
}

/// Go `swarm.Node` (fields used).
#[derive(Clone, Debug, Default)]
pub struct Node {
    pub id: String,
    pub role: String,
    pub availability: String,
    pub labels: Labels,
    pub hostname: String,
    pub architecture: String,
    pub os: String,
    pub engine_version: String,
    pub state: String,
    pub addr: String,
    pub manager_status: Option<ManagerStatus>,
}

/// Decodes `[]swarm.Node`.
pub fn node_list(v: &Json) -> Result<Vec<Node>, String> {
    let mut out = Vec::new();
    for n in arr_of(v, "[]swarm.Node")? {
        let Some(o) = Obj::new(n, "swarm.Node", "")? else {
            out.push(Node::default());
            continue;
        };
        let mut node = Node { id: o.str("ID")?, ..Default::default() };
        if let Some(spec) = o.obj("Spec", "swarm.NodeSpec")? {
            node.role = spec.str("Role")?;
            node.availability = spec.str("Availability")?;
            node.labels = spec.labels("Labels")?;
        }
        if let Some(d) = o.obj("Description", "swarm.NodeDescription")? {
            node.hostname = d.str("Hostname")?;
            if let Some(p) = d.obj("Platform", "swarm.Platform")? {
                node.architecture = p.str("Architecture")?;
                node.os = p.str("OS")?;
            }
            if let Some(e) = d.obj("Engine", "swarm.EngineDescription")? {
                node.engine_version = e.str("EngineVersion")?;
            }
        }
        if let Some(s) = o.obj("Status", "swarm.NodeStatus")? {
            node.state = s.str("State")?;
            node.addr = s.str("Addr")?;
        }
        if let Some(m) = o.obj("ManagerStatus", "swarm.ManagerStatus")? {
            node.manager_status = Some(ManagerStatus { leader: m.bool("Leader")?, reachability: m.str("Reachability")?, addr: m.str("Addr")? });
        }
        out.push(node);
    }
    Ok(out)
}

/// Go `swarm.PortConfig` (fields used).
#[derive(Clone, Debug, Default)]
pub struct PortConfig {
    pub name: String,
    pub protocol: String,
    pub published_port: u32,
    pub publish_mode: String,
}

fn port_configs(o: &Obj<'_>, k: &str) -> Result<Vec<PortConfig>, String> {
    let mut out = Vec::new();
    for p in o.arr(k, "[]swarm.PortConfig")? {
        let Some(po) = Obj::new(p, "swarm.PortConfig", &o.path(k))? else {
            out.push(PortConfig::default());
            continue;
        };
        out.push(PortConfig {
            name: po.str("Name")?,
            protocol: po.str("Protocol")?,
            published_port: po.int("PublishedPort", "uint32", 0, u32::MAX as i64)? as u32,
            publish_mode: po.str("PublishMode")?,
        });
    }
    Ok(out)
}

/// Go `swarm.EndpointVirtualIP`.
#[derive(Clone, Debug, Default)]
pub struct VirtualIp {
    pub network_id: String,
    pub addr: Option<Prefix>,
}

/// Go `swarm.Service` (fields used).
#[derive(Clone, Debug, Default)]
pub struct Service {
    pub id: String,
    pub name: String,
    pub labels: Labels,
    pub mode_global: bool,
    pub mode_replicated: bool,
    /// `Spec.TaskTemplate.ContainerSpec`: (hostname, image) when set.
    pub container_spec: Option<(String, String)>,
    /// `UpdateStatus.State` when `UpdateStatus` is set.
    pub update_status: Option<String>,
    pub ports: Vec<PortConfig>,
    pub virtual_ips: Vec<VirtualIp>,
}

/// Decodes `[]swarm.Service`.
pub fn service_list(v: &Json) -> Result<Vec<Service>, String> {
    let mut out = Vec::new();
    for s in arr_of(v, "[]swarm.Service")? {
        let Some(o) = Obj::new(s, "swarm.Service", "")? else {
            out.push(Service::default());
            continue;
        };
        let mut svc = Service { id: o.str("ID")?, ..Default::default() };
        if let Some(spec) = o.obj("Spec", "swarm.ServiceSpec")? {
            svc.name = spec.str("Name")?;
            svc.labels = spec.labels("Labels")?;
            if let Some(tt) = spec.obj("TaskTemplate", "swarm.TaskSpec")?
                && let Some(cs) = tt.obj("ContainerSpec", "swarm.ContainerSpec")?
            {
                svc.container_spec = Some((cs.str("Hostname")?, cs.str("Image")?));
            }
            if let Some(m) = spec.obj("Mode", "swarm.ServiceMode")? {
                svc.mode_global = m.obj("Global", "swarm.GlobalService")?.is_some();
                svc.mode_replicated = m.obj("Replicated", "swarm.ReplicatedService")?.is_some();
            }
        }
        if let Some(u) = o.obj("UpdateStatus", "swarm.UpdateStatus")? {
            svc.update_status = Some(u.str("State")?);
        }
        if let Some(e) = o.obj("Endpoint", "swarm.Endpoint")? {
            svc.ports = port_configs(&e, "Ports")?;
            for vip in e.arr("VirtualIPs", "[]swarm.EndpointVirtualIP")? {
                let Some(vo) = Obj::new(vip, "swarm.EndpointVirtualIP", "Endpoint.VirtualIPs")? else {
                    svc.virtual_ips.push(VirtualIp::default());
                    continue;
                };
                let addr = match vo.field("Addr") {
                    None => None,
                    Some(a) => Obj::prefix(a, "EndpointVirtualIP", "Addr")?,
                };
                svc.virtual_ips.push(VirtualIp { network_id: vo.str("NetworkID")?, addr });
            }
        }
        out.push(svc);
    }
    Ok(out)
}

/// Go `swarm.NetworkAttachment` (fields used).
#[derive(Clone, Debug, Default)]
pub struct NetworkAttachment {
    pub network_id: String,
    pub addresses: Vec<Option<Prefix>>,
}

/// Go `swarm.Task` (fields used).
#[derive(Clone, Debug, Default)]
pub struct Task {
    pub id: String,
    pub service_id: String,
    pub slot: i64,
    pub node_id: String,
    pub state: String,
    pub desired_state: String,
    /// `Status.ContainerStatus.ContainerID` when `ContainerStatus` is set.
    pub container_id: Option<String>,
    pub port_status: Vec<PortConfig>,
    /// `Spec.ContainerSpec.Labels` when `ContainerSpec` is set.
    pub container_labels: Option<Labels>,
    pub networks_attachments: Vec<NetworkAttachment>,
}

/// Decodes `[]swarm.Task`.
pub fn task_list(v: &Json) -> Result<Vec<Task>, String> {
    let mut out = Vec::new();
    for t in arr_of(v, "[]swarm.Task")? {
        let Some(o) = Obj::new(t, "swarm.Task", "")? else {
            out.push(Task::default());
            continue;
        };
        let mut task = Task {
            id: o.str("ID")?,
            service_id: o.str("ServiceID")?,
            slot: o.int("Slot", "int", i64::MIN, i64::MAX)?,
            node_id: o.str("NodeID")?,
            desired_state: o.str("DesiredState")?,
            ..Default::default()
        };
        if let Some(spec) = o.obj("Spec", "swarm.TaskSpec")?
            && let Some(cs) = spec.obj("ContainerSpec", "swarm.ContainerSpec")?
        {
            task.container_labels = Some(cs.labels("Labels")?);
        }
        if let Some(st) = o.obj("Status", "swarm.TaskStatus")? {
            task.state = st.str("State")?;
            if let Some(cs) = st.obj("ContainerStatus", "swarm.ContainerStatus")? {
                task.container_id = Some(cs.str("ContainerID")?);
            }
            if let Some(ps) = st.obj("PortStatus", "swarm.PortStatus")? {
                task.port_status = port_configs(&ps, "Ports")?;
            }
        }
        for na in o.arr("NetworksAttachments", "[]swarm.NetworkAttachment")? {
            let Some(no) = Obj::new(na, "swarm.NetworkAttachment", "Task.NetworksAttachments")? else {
                task.networks_attachments.push(NetworkAttachment::default());
                continue;
            };
            let mut att = NetworkAttachment::default();
            if let Some(n) = no.obj("Network", "swarm.Network")? {
                att.network_id = n.str("ID")?;
            }
            for a in no.arr("Addresses", "[]netip.Prefix")? {
                att.addresses.push(Obj::prefix(a, "NetworkAttachment", "Addresses")?);
            }
            task.networks_attachments.push(att);
        }
        out.push(task);
    }
    Ok(out)
}
