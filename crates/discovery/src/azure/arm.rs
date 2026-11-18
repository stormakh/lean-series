//! The Azure Resource Manager side of the provider, ported from the Go SDK modules Prometheus uses
//! (azcore v1.23.3, armcompute v8.4.0, armnetwork v8.0.0): resource IDs, the request pipeline (telemetry user
//! agent, retries, bearer token), `ResponseError` texts, paged list calls with `nextLink`, and the JSON models
//! with the generated code's field names and error texts.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use json::Json;

use super::identity::{AccessToken, TokenCredential};
use crate::cancel::CancelToken;

/// Go `runtime.Version()` of the reference Prometheus 3.16.0 build (used in the SDK telemetry user agent).
pub const GO_VERSION: &str = "go1.27.3";

/// Go `runtime.GOARCH` for this build.
pub fn go_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        "x86" => "386",
        "powerpc64" => "ppc64le",
        a => a,
    }
}

/// Go azcore `NewTelemetryPolicy` value: `[<application_id> ]azsdk-go-<mod>/<ver> (<go version>; <os>)`.
/// `module` may be fully qualified; only the part after the last '/' is used (so armnetwork/v8 gives "v8").
pub fn telemetry_value(application_id: &str, module: &str, version: &str) -> String {
    let mut s = String::new();
    if !application_id.is_empty() {
        let mut a = application_id.replace(' ', "/");
        if a.len() > 24 {
            a.truncate(24);
        }
        s.push_str(&a);
        s.push(' ');
    }
    let m = module.rsplit('/').next().unwrap_or(module);
    s.push_str(&format!(
        "azsdk-go-{m}/{version} ({GO_VERSION}; {})",
        std::env::consts::OS
    ));
    s
}

// ---------------- resource IDs (azcore arm/internal/resource) ----------------

/// Go `resource.ResourceType`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResourceType {
    pub namespace: String,
    pub type_: String,
    pub types: Vec<String>,
    string_value: String,
}

const BUILT_IN_NAMESPACE: &str = "Microsoft.Resources";

impl ResourceType {
    /// Go `NewResourceType`.
    pub fn new(namespace: &str, type_name: &str) -> ResourceType {
        ResourceType {
            namespace: namespace.to_string(),
            type_: type_name.to_string(),
            types: split_omit_empty(type_name),
            string_value: format!("{namespace}/{type_name}"),
        }
    }
    fn append_child(&self, child: &str) -> ResourceType {
        ResourceType::new(&self.namespace, &format!("{}/{}", self.type_, child))
    }
    fn last_type(&self) -> &str {
        self.types.last().map(|s| s.as_str()).unwrap_or("")
    }
}

impl std::fmt::Display for ResourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.string_value)
    }
}

/// Go `arm.ResourceID`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ResourceId {
    pub parent: Option<Box<ResourceId>>,
    pub subscription_id: String,
    pub resource_group_name: String,
    pub provider: String,
    pub location: String,
    pub resource_type: ResourceType,
    pub name: String,
    is_child: bool,
    string_value: String,
}

impl std::fmt::Display for ResourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.string_value)
    }
}

fn root_resource_id() -> ResourceId {
    ResourceId {
        resource_type: ResourceType::new(BUILT_IN_NAMESPACE, "tenants"),
        ..Default::default()
    }
}

fn split_omit_empty(v: &str) -> Vec<String> {
    v.split('/')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

fn is_tenant_resource(r: &ResourceId) -> bool {
    r.resource_type
        .string_value
        .eq_ignore_ascii_case(&format!("{BUILT_IN_NAMESPACE}/tenants"))
        && r.parent.is_none()
}
fn is_subscription_resource(r: &ResourceId) -> bool {
    r.resource_type
        .string_value
        .eq_ignore_ascii_case(&format!("{BUILT_IN_NAMESPACE}/subscriptions"))
        && r.parent.as_deref().is_some_and(is_tenant_resource)
}
fn is_resource_group_resource(r: &ResourceId) -> bool {
    r.resource_type
        .string_value
        .eq_ignore_ascii_case(&format!("{BUILT_IN_NAMESPACE}/resourceGroups"))
        && r.parent.as_deref().is_some_and(is_subscription_resource)
}

fn choose_resource_type(type_name: &str, parent: &ResourceId) -> ResourceType {
    if type_name.eq_ignore_ascii_case("resourcegroups") && is_subscription_resource(parent) {
        ResourceType::new(BUILT_IN_NAMESPACE, "resourceGroups")
    } else if type_name.eq_ignore_ascii_case("subscriptions") && is_tenant_resource(parent) {
        ResourceType::new(BUILT_IN_NAMESPACE, "subscriptions")
    } else {
        parent.resource_type.append_child(type_name)
    }
}

fn init_resource_id(
    parent: ResourceId,
    rt: ResourceType,
    name: &str,
    is_child: bool,
) -> ResourceId {
    let mut id = ResourceId {
        provider: parent.provider.clone(),
        subscription_id: parent.subscription_id.clone(),
        resource_group_name: parent.resource_group_name.clone(),
        location: parent.location.clone(),
        ..Default::default()
    };
    if rt.string_value == format!("{BUILT_IN_NAMESPACE}/subscriptions") {
        id.subscription_id = name.to_string();
    }
    if rt.last_type().eq_ignore_ascii_case("locations") {
        id.location = name.to_string();
    }
    if rt.string_value == format!("{BUILT_IN_NAMESPACE}/resourceGroups") {
        id.resource_group_name = name.to_string();
    }
    if rt.string_value == format!("{BUILT_IN_NAMESPACE}/providers") {
        id.provider = name.to_string();
    }
    let mut sv = parent.string_value.clone();
    if is_child {
        sv.push('/');
        sv.push_str(rt.last_type());
        if !name.is_empty() {
            sv.push('/');
            sv.push_str(name);
        }
    } else {
        sv.push_str(&format!(
            "/providers/{}/{}/{}",
            rt.namespace, rt.type_, name
        ));
    }
    id.parent = Some(Box::new(parent));
    id.is_child = is_child;
    id.resource_type = rt;
    id.name = name.to_string();
    id.string_value = sv;
    id
}

fn append_next(parent: ResourceId, parts: &[String], id: &str) -> Result<ResourceId, String> {
    let invalid = || format!("invalid resource ID: {id}");
    if parts.is_empty() {
        return Ok(parent);
    }
    if parts.len() == 1 {
        if (parts[0].eq_ignore_ascii_case("subscriptions") && is_tenant_resource(&parent))
            || (parts[0].eq_ignore_ascii_case("resourcegroups")
                && is_subscription_resource(&parent))
        {
            return Err(invalid());
        }
        if is_resource_group_resource(&parent) {
            return Err(invalid());
        }
        let rt = choose_resource_type(&parts[0], &parent);
        return Ok(init_resource_id(parent, rt, "", true));
    }
    if parts[0].eq_ignore_ascii_case("providers")
        && (parts.len() == 2 || parts[2].eq_ignore_ascii_case("providers"))
    {
        if !is_subscription_resource(&parent) && !is_tenant_resource(&parent) {
            return Err(invalid());
        }
        let next = init_resource_id(
            parent,
            ResourceType::new(BUILT_IN_NAMESPACE, "providers"),
            &parts[1],
            true,
        );
        return append_next(next, &parts[2..], id);
    }
    if parts.len() > 3 && parts[0].eq_ignore_ascii_case("providers") {
        let next = init_resource_id(
            parent,
            ResourceType::new(&parts[1], &parts[2]),
            &parts[3],
            false,
        );
        return append_next(next, &parts[4..], id);
    }
    if parts.len() > 1 && !parts[0].eq_ignore_ascii_case("providers") {
        let rt = choose_resource_type(&parts[0], &parent);
        let next = init_resource_id(parent, rt, &parts[1], true);
        return append_next(next, &parts[2..], id);
    }
    Err(invalid())
}

/// Go `arm.ParseResourceID`, with the same error texts.
pub fn parse_resource_id(id: &str) -> Result<ResourceId, String> {
    if id.is_empty() {
        return Err("invalid resource ID: id cannot be empty".to_string());
    }
    if !id.starts_with('/') {
        return Err(format!(
            "invalid resource ID: resource id '{id}' must start with '/'"
        ));
    }
    let parts = split_omit_empty(id);
    if parts.len() < 2 {
        return Err(format!("invalid resource ID: {id}"));
    }
    if !parts[0].eq_ignore_ascii_case("subscriptions")
        && !parts[0].eq_ignore_ascii_case("providers")
    {
        return Err(format!("invalid resource ID: {id}"));
    }
    append_next(root_resource_id(), &parts, id)
}

// ---------------- JSON helpers ----------------

/// Go `json.Indent(dst, src, "", "  ")` for a valid document: the source tokens are kept verbatim.
pub fn go_json_indent(src: &[u8]) -> String {
    let mut out = String::with_capacity(src.len() * 2);
    let s = String::from_utf8_lossy(src);
    let b: Vec<char> = s.chars().collect();
    let mut depth = 0usize;
    let mut need_indent = false;
    let newline = |out: &mut String, depth: usize| {
        out.push('\n');
        for _ in 0..depth {
            out.push_str("  ");
        }
    };
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if need_indent && c != ']' && c != '}' {
            need_indent = false;
            depth += 1;
            newline(&mut out, depth);
        }
        match c {
            '"' => {
                out.push(c);
                i += 1;
                while i < b.len() {
                    out.push(b[i]);
                    if b[i] == '\\' && i + 1 < b.len() {
                        out.push(b[i + 1]);
                        i += 2;
                        continue;
                    }
                    if b[i] == '"' {
                        break;
                    }
                    i += 1;
                }
            }
            '{' | '[' => {
                out.push(c);
                need_indent = true;
            }
            ',' => {
                out.push(c);
                newline(&mut out, depth);
            }
            ':' => {
                out.push(c);
                out.push(' ');
            }
            '}' | ']' => {
                if need_indent {
                    need_indent = false;
                } else {
                    depth = depth.saturating_sub(1);
                    newline(&mut out, depth);
                }
                out.push(c);
            }
            _ => out.push(c),
        }
        i += 1;
    }
    out
}

/// The body of an error as Go's `ResponseError`/`AuthenticationFailedError` print it: indented JSON when the
/// body is JSON, the raw body otherwise.
pub fn indent_body(body: &[u8]) -> String {
    if Json::parse_bytes(body).is_ok() {
        go_json_indent(body)
    } else {
        String::from_utf8_lossy(body).into_owned()
    }
}

fn json_kind(v: &Json) -> &'static str {
    match v {
        Json::Null => "null",
        Json::Bool(_) => "bool",
        Json::Int(_) | Json::Float(_) => "number",
        Json::Str(_) => "string",
        Json::Arr(_) => "array",
        Json::Obj(_) => "object",
    }
}

fn type_err(v: &Json, go_type: &str) -> String {
    format!(
        "json: cannot unmarshal {} into Go value of type {}",
        json_kind(v),
        go_type
    )
}

/// The object members of a generated model, or Go's error for a non-object (`unmarshalling type *T: ...`).
fn members<'a>(v: &'a Json, ty: &str) -> Result<&'a [(String, Json)], String> {
    match v {
        Json::Obj(m) => Ok(m),
        _ => Err(format!(
            "unmarshalling type {ty}: {}",
            type_err(v, "map[string]json.RawMessage")
        )),
    }
}

/// Go `unpopulate` of a string-typed field.
fn un_str(v: &Json, field: &str, go_type: &str) -> Result<Option<String>, String> {
    match v {
        Json::Null => Ok(None),
        Json::Str(s) => Ok(Some(s.clone())),
        _ => Err(format!("struct field {field}: {}", type_err(v, go_type))),
    }
}

fn un_bool(v: &Json, field: &str) -> Result<Option<bool>, String> {
    match v {
        Json::Null => Ok(None),
        Json::Bool(b) => Ok(Some(*b)),
        _ => Err(format!("struct field {field}: {}", type_err(v, "bool"))),
    }
}

fn un_obj<T>(
    v: &Json,
    field: &str,
    f: impl Fn(&Json) -> Result<T, String>,
) -> Result<Option<T>, String> {
    match v {
        Json::Null => Ok(None),
        _ => f(v)
            .map(Some)
            .map_err(|e| format!("struct field {field}: {e}")),
    }
}

fn un_vec<T>(
    v: &Json,
    field: &str,
    go_type: &str,
    f: impl Fn(&Json) -> Result<T, String>,
) -> Result<Vec<Option<T>>, String> {
    match v {
        Json::Null => Ok(Vec::new()),
        Json::Arr(a) => {
            let mut out = Vec::with_capacity(a.len());
            for e in a {
                if e.is_null() {
                    out.push(None);
                } else {
                    out.push(Some(
                        f(e).map_err(|e| format!("struct field {field}: {e}"))?,
                    ));
                }
            }
            Ok(out)
        }
        _ => Err(format!("struct field {field}: {}", type_err(v, go_type))),
    }
}

fn un_tags(v: &Json) -> Result<Option<BTreeMap<String, Option<String>>>, String> {
    match v {
        Json::Null => Ok(None),
        Json::Obj(m) => {
            let mut out = BTreeMap::new();
            for (k, e) in m {
                match e {
                    Json::Null => out.insert(k.clone(), None),
                    Json::Str(s) => out.insert(k.clone(), Some(s.clone())),
                    _ => return Err(format!("struct field Tags: {}", type_err(e, "string"))),
                };
            }
            Ok(Some(out))
        }
        _ => Err(format!(
            "struct field Tags: {}",
            type_err(v, "map[string]*string")
        )),
    }
}

fn put(o: &mut Json, k: &str, v: Option<Json>) {
    if let Some(v) = v {
        o.set(k, v);
    }
}

// ---------------- models ----------------

/// The fields of armcompute `VirtualMachine` / `VirtualMachineScaleSetVM` that discovery reads.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ArmVirtualMachine {
    pub id: Option<String>,
    pub name: Option<String>,
    pub type_: Option<String>,
    pub location: Option<String>,
    /// Only in `VirtualMachineScaleSetVM` (`instanceId`).
    pub instance_id: Option<String>,
    pub tags: Option<BTreeMap<String, Option<String>>>,
    pub properties: Option<VmProperties>,
}

/// armcompute `VirtualMachineProperties` / `VirtualMachineScaleSetVMProperties` (fields used by discovery).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct VmProperties {
    pub os_profile: Option<OsProfile>,
    pub storage_profile: Option<StorageProfile>,
    pub network_profile: Option<NetworkProfile>,
    pub hardware_profile: Option<HardwareProfile>,
}

/// armcompute `OSProfile`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OsProfile {
    pub computer_name: Option<String>,
}

/// armcompute `StorageProfile`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StorageProfile {
    pub os_disk: Option<OsDisk>,
}

/// armcompute `OSDisk`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct OsDisk {
    pub os_type: Option<String>,
}

/// armcompute `NetworkProfile` (a JSON null element of `networkInterfaces` is `None`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NetworkProfile {
    pub network_interfaces: Vec<Option<NetworkInterfaceReference>>,
}

/// armcompute `NetworkInterfaceReference`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NetworkInterfaceReference {
    pub id: Option<String>,
    pub primary: Option<bool>,
}

/// armcompute `HardwareProfile`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HardwareProfile {
    pub vm_size: Option<String>,
}

/// armcompute `VirtualMachineScaleSet` (fields used by discovery).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ArmScaleSet {
    pub id: Option<String>,
    pub name: Option<String>,
    pub location: Option<String>,
    pub type_: Option<String>,
}

/// armnetwork `Interface` (fields used by discovery).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ArmInterface {
    pub id: Option<String>,
    pub name: Option<String>,
    pub properties: Option<InterfaceProperties>,
}

/// armnetwork `InterfacePropertiesFormat`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct InterfaceProperties {
    pub primary: Option<bool>,
    pub ip_configurations: Vec<Option<IpConfiguration>>,
}

/// armnetwork `InterfaceIPConfiguration`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct IpConfiguration {
    pub properties: Option<IpConfigurationProperties>,
}

/// armnetwork `InterfaceIPConfigurationPropertiesFormat`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct IpConfigurationProperties {
    pub private_ip_address: Option<String>,
    pub public_ip_address: Option<PublicIpAddress>,
}

/// armnetwork `PublicIPAddress`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PublicIpAddress {
    pub properties: Option<PublicIpAddressProperties>,
}

/// armnetwork `PublicIPAddressPropertiesFormat`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PublicIpAddressProperties {
    pub ip_address: Option<String>,
}

const T_VM: &str = "*armcompute.VirtualMachine";
const T_VMSSVM: &str = "*armcompute.VirtualMachineScaleSetVM";

impl ArmVirtualMachine {
    /// The generated `UnmarshalJSON` of armcompute `VirtualMachine` (`scale_set_vm`: `VirtualMachineScaleSetVM`).
    pub fn from_json(v: &Json, scale_set_vm: bool) -> Result<ArmVirtualMachine, String> {
        let ty = if scale_set_vm { T_VMSSVM } else { T_VM };
        let props_ty = if scale_set_vm {
            "*armcompute.VirtualMachineScaleSetVMProperties"
        } else {
            "*armcompute.VirtualMachineProperties"
        };
        let w = |e: String| format!("unmarshalling type {ty}: {e}");
        let mut vm = ArmVirtualMachine::default();
        for (k, val) in members(v, ty)? {
            match k.as_str() {
                "id" => vm.id = un_str(val, "ID", "string").map_err(w)?,
                "name" => vm.name = un_str(val, "Name", "string").map_err(w)?,
                "type" => vm.type_ = un_str(val, "Type", "string").map_err(w)?,
                "location" => vm.location = un_str(val, "Location", "string").map_err(w)?,
                "instanceId" if scale_set_vm => {
                    vm.instance_id = un_str(val, "InstanceID", "string").map_err(w)?
                }
                "tags" => vm.tags = un_tags(val).map_err(w)?,
                "properties" => {
                    vm.properties =
                        un_obj(val, "Properties", |p| VmProperties::from_json(p, props_ty))
                            .map_err(w)?
                }
                _ => {}
            }
        }
        Ok(vm)
    }

    /// Go `json.Marshal` of the model (for serving recorded responses).
    pub fn to_json(&self) -> Json {
        let mut o = Json::obj();
        put(&mut o, "id", self.id.as_ref().map(Json::from));
        put(
            &mut o,
            "instanceId",
            self.instance_id.as_ref().map(Json::from),
        );
        put(&mut o, "location", self.location.as_ref().map(Json::from));
        put(&mut o, "name", self.name.as_ref().map(Json::from));
        if let Some(p) = &self.properties {
            let mut po = Json::obj();
            put(
                &mut po,
                "hardwareProfile",
                p.hardware_profile.as_ref().map(|h| {
                    let mut ho = Json::obj();
                    put(&mut ho, "vmSize", h.vm_size.as_ref().map(Json::from));
                    ho
                }),
            );
            put(
                &mut po,
                "networkProfile",
                p.network_profile.as_ref().map(|n| {
                    let refs: Vec<Json> = n
                        .network_interfaces
                        .iter()
                        .map(|r| match r {
                            None => Json::Null,
                            Some(r) => {
                                let mut ro = Json::obj();
                                put(&mut ro, "id", r.id.as_ref().map(Json::from));
                                put(
                                    &mut ro,
                                    "properties",
                                    r.primary.map(|b| Json::obj().with("primary", b)),
                                );
                                ro
                            }
                        })
                        .collect();
                    Json::obj().with("networkInterfaces", Json::Arr(refs))
                }),
            );
            put(
                &mut po,
                "osProfile",
                p.os_profile.as_ref().map(|os| {
                    let mut oo = Json::obj();
                    put(
                        &mut oo,
                        "computerName",
                        os.computer_name.as_ref().map(Json::from),
                    );
                    oo
                }),
            );
            put(
                &mut po,
                "storageProfile",
                p.storage_profile.as_ref().map(|s| {
                    let mut so = Json::obj();
                    put(
                        &mut so,
                        "osDisk",
                        s.os_disk.as_ref().map(|d| {
                            let mut d2 = Json::obj();
                            put(&mut d2, "osType", d.os_type.as_ref().map(Json::from));
                            d2
                        }),
                    );
                    so
                }),
            );
            o.set("properties", po);
        }
        if let Some(t) = &self.tags {
            let mut to = Json::obj();
            for (k, v) in t {
                to.set(k, v.as_ref().map(Json::from).unwrap_or(Json::Null));
            }
            o.set("tags", to);
        }
        put(&mut o, "type", self.type_.as_ref().map(Json::from));
        o
    }
}

impl VmProperties {
    fn from_json(v: &Json, ty: &str) -> Result<VmProperties, String> {
        let w = |e: String| format!("unmarshalling type {ty}: {e}");
        let mut p = VmProperties::default();
        for (k, val) in members(v, ty)? {
            match k.as_str() {
                "osProfile" => p.os_profile = un_obj(val, "OSProfile", |v| {
                    let mut o = OsProfile::default();
                    for (k, val) in members(v, "*armcompute.OSProfile")? {
                        if k == "computerName" {
                            o.computer_name = un_str(val, "ComputerName", "string").map_err(|e| format!("unmarshalling type *armcompute.OSProfile: {e}"))?;
                        }
                    }
                    Ok(o)
                })
                .map_err(w)?,
                "storageProfile" => p.storage_profile = un_obj(val, "StorageProfile", |v| {
                    let mut s = StorageProfile::default();
                    for (k, val) in members(v, "*armcompute.StorageProfile")? {
                        if k == "osDisk" {
                            s.os_disk = un_obj(val, "OSDisk", |v| {
                                let mut d = OsDisk::default();
                                for (k, val) in members(v, "*armcompute.OSDisk")? {
                                    if k == "osType" {
                                        d.os_type = un_str(val, "OSType", "armcompute.OperatingSystemTypes").map_err(|e| format!("unmarshalling type *armcompute.OSDisk: {e}"))?;
                                    }
                                }
                                Ok(d)
                            })
                            .map_err(|e| format!("unmarshalling type *armcompute.StorageProfile: {e}"))?;
                        }
                    }
                    Ok(s)
                })
                .map_err(w)?,
                "networkProfile" => p.network_profile = un_obj(val, "NetworkProfile", |v| {
                    let mut n = NetworkProfile::default();
                    for (k, val) in members(v, "*armcompute.NetworkProfile")? {
                        if k == "networkInterfaces" {
                            n.network_interfaces = un_vec(val, "NetworkInterfaces", "[]*armcompute.NetworkInterfaceReference", |v| {
                                let ty = "*armcompute.NetworkInterfaceReference";
                                let mut r = NetworkInterfaceReference::default();
                                for (k, val) in members(v, ty)? {
                                    match k.as_str() {
                                        "id" => r.id = un_str(val, "ID", "string").map_err(|e| format!("unmarshalling type {ty}: {e}"))?,
                                        "properties" => {
                                            r.primary = un_obj(val, "Properties", |v| {
                                                let pty = "*armcompute.NetworkInterfaceReferenceProperties";
                                                let mut primary = None;
                                                for (k, val) in members(v, pty)? {
                                                    if k == "primary" {
                                                        primary = un_bool(val, "Primary").map_err(|e| format!("unmarshalling type {pty}: {e}"))?;
                                                    }
                                                }
                                                Ok(primary)
                                            })
                                            .map_err(|e| format!("unmarshalling type {ty}: {e}"))?
                                            .flatten()
                                        }
                                        _ => {}
                                    }
                                }
                                Ok(r)
                            })
                            .map_err(|e| format!("unmarshalling type *armcompute.NetworkProfile: {e}"))?;
                        }
                    }
                    Ok(n)
                })
                .map_err(w)?,
                "hardwareProfile" => p.hardware_profile = un_obj(val, "HardwareProfile", |v| {
                    let mut h = HardwareProfile::default();
                    for (k, val) in members(v, "*armcompute.HardwareProfile")? {
                        if k == "vmSize" {
                            h.vm_size = un_str(val, "VMSize", "armcompute.VirtualMachineSizeTypes").map_err(|e| format!("unmarshalling type *armcompute.HardwareProfile: {e}"))?;
                        }
                    }
                    Ok(h)
                })
                .map_err(w)?,
                _ => {}
            }
        }
        Ok(p)
    }
}

impl ArmScaleSet {
    /// The generated `UnmarshalJSON` of armcompute `VirtualMachineScaleSet` (fields used here).
    pub fn from_json(v: &Json) -> Result<ArmScaleSet, String> {
        let ty = "*armcompute.VirtualMachineScaleSet";
        let w = |e: String| format!("unmarshalling type {ty}: {e}");
        let mut s = ArmScaleSet::default();
        for (k, val) in members(v, ty)? {
            match k.as_str() {
                "id" => s.id = un_str(val, "ID", "string").map_err(w)?,
                "name" => s.name = un_str(val, "Name", "string").map_err(w)?,
                "location" => s.location = un_str(val, "Location", "string").map_err(w)?,
                "type" => s.type_ = un_str(val, "Type", "string").map_err(w)?,
                _ => {}
            }
        }
        Ok(s)
    }

    /// Go `json.Marshal` of the model.
    pub fn to_json(&self) -> Json {
        let mut o = Json::obj();
        put(&mut o, "id", self.id.as_ref().map(Json::from));
        put(&mut o, "location", self.location.as_ref().map(Json::from));
        put(&mut o, "name", self.name.as_ref().map(Json::from));
        put(&mut o, "type", self.type_.as_ref().map(Json::from));
        o
    }
}

impl ArmInterface {
    /// The generated `UnmarshalJSON` of armnetwork `Interface` (fields used here).
    pub fn from_json(v: &Json) -> Result<ArmInterface, String> {
        let ty = "*armnetwork.Interface";
        let w = |e: String| format!("unmarshalling type {ty}: {e}");
        let mut n = ArmInterface::default();
        for (k, val) in members(v, ty)? {
            match k.as_str() {
                "id" => n.id = un_str(val, "ID", "string").map_err(w)?,
                "name" => n.name = un_str(val, "Name", "string").map_err(w)?,
                "properties" => {
                    n.properties =
                        un_obj(val, "Properties", InterfaceProperties::from_json).map_err(w)?
                }
                _ => {}
            }
        }
        Ok(n)
    }

    /// Go `json.Marshal` of the model.
    pub fn to_json(&self) -> Json {
        let mut o = Json::obj();
        put(&mut o, "id", self.id.as_ref().map(Json::from));
        put(&mut o, "name", self.name.as_ref().map(Json::from));
        if let Some(p) = &self.properties {
            let mut po = Json::obj();
            let cfgs: Vec<Json> = p
                .ip_configurations
                .iter()
                .map(|c| match c {
                    None => Json::Null,
                    Some(c) => {
                        let mut co = Json::obj();
                        put(
                            &mut co,
                            "properties",
                            c.properties.as_ref().map(|cp| {
                                let mut cpo = Json::obj();
                                put(
                                    &mut cpo,
                                    "privateIPAddress",
                                    cp.private_ip_address.as_ref().map(Json::from),
                                );
                                put(
                                    &mut cpo,
                                    "publicIPAddress",
                                    cp.public_ip_address.as_ref().map(|pip| {
                                        let mut pio = Json::obj();
                                        put(
                                            &mut pio,
                                            "properties",
                                            pip.properties.as_ref().map(|pp| {
                                                let mut ppo = Json::obj();
                                                put(
                                                    &mut ppo,
                                                    "ipAddress",
                                                    pp.ip_address.as_ref().map(Json::from),
                                                );
                                                ppo
                                            }),
                                        );
                                        pio
                                    }),
                                );
                                cpo
                            }),
                        );
                        co
                    }
                })
                .collect();
            if !p.ip_configurations.is_empty() {
                po.set("ipConfigurations", Json::Arr(cfgs));
            }
            put(&mut po, "primary", p.primary.map(Json::from));
            o.set("properties", po);
        }
        o
    }
}

impl InterfaceProperties {
    fn from_json(v: &Json) -> Result<InterfaceProperties, String> {
        let ty = "*armnetwork.InterfacePropertiesFormat";
        let w = |e: String| format!("unmarshalling type {ty}: {e}");
        let mut p = InterfaceProperties::default();
        for (k, val) in members(v, ty)? {
            match k.as_str() {
                "primary" => p.primary = un_bool(val, "Primary").map_err(w)?,
                "ipConfigurations" => {
                    p.ip_configurations = un_vec(
                        val,
                        "IPConfigurations",
                        "[]*armnetwork.InterfaceIPConfiguration",
                        IpConfiguration::from_json,
                    )
                    .map_err(w)?
                }
                _ => {}
            }
        }
        Ok(p)
    }
}

impl IpConfiguration {
    fn from_json(v: &Json) -> Result<IpConfiguration, String> {
        let ty = "*armnetwork.InterfaceIPConfiguration";
        let mut c = IpConfiguration::default();
        for (k, val) in members(v, ty)? {
            if k == "properties" {
                c.properties = un_obj(val, "Properties", |v| {
                    let pty = "*armnetwork.InterfaceIPConfigurationPropertiesFormat";
                    let pw = |e: String| format!("unmarshalling type {pty}: {e}");
                    let mut p = IpConfigurationProperties::default();
                    for (k, val) in members(v, pty)? {
                        match k.as_str() {
                            "privateIPAddress" => {
                                p.private_ip_address =
                                    un_str(val, "PrivateIPAddress", "string").map_err(pw)?
                            }
                            "publicIPAddress" => {
                                p.public_ip_address = un_obj(val, "PublicIPAddress", |v| {
                                    let aty = "*armnetwork.PublicIPAddress";
                                    let mut a = PublicIpAddress::default();
                                    for (k, val) in members(v, aty)? {
                                        if k == "properties" {
                                            a.properties = un_obj(val, "Properties", |v| {
                                                let bty =
                                                    "*armnetwork.PublicIPAddressPropertiesFormat";
                                                let mut b = PublicIpAddressProperties::default();
                                                for (k, val) in members(v, bty)? {
                                                    if k == "ipAddress" {
                                                        b.ip_address = un_str(
                                                            val,
                                                            "IPAddress",
                                                            "string",
                                                        )
                                                        .map_err(|e| {
                                                            format!("unmarshalling type {bty}: {e}")
                                                        })?;
                                                    }
                                                }
                                                Ok(b)
                                            })
                                            .map_err(|e| {
                                                format!("unmarshalling type {aty}: {e}")
                                            })?;
                                        }
                                    }
                                    Ok(a)
                                })
                                .map_err(pw)?
                            }
                            _ => {}
                        }
                    }
                    Ok(p)
                })
                .map_err(|e| format!("unmarshalling type {ty}: {e}"))?;
            }
        }
        Ok(c)
    }
}

/// A page of a list call: `value` and `nextLink`.
pub struct Page<T> {
    pub value: Vec<T>,
    pub next_link: Option<String>,
}

/// The generated `UnmarshalJSON` of a list result (`ty` like "*armcompute.VirtualMachineListResult").
fn parse_page<T>(
    v: &Json,
    ty: &str,
    value_ty: &str,
    f: impl Fn(&Json) -> Result<T, String>,
) -> Result<Page<T>, String> {
    let w = |e: String| format!("unmarshalling type {ty}: {e}");
    let mut page = Page {
        value: Vec::new(),
        next_link: None,
    };
    for (k, val) in members(v, ty)? {
        match k.as_str() {
            "value" => {
                page.value = un_vec(val, "Value", value_ty, &f)
                    .map_err(w)?
                    .into_iter()
                    .flatten()
                    .collect()
            }
            "nextLink" => page.next_link = un_str(val, "NextLink", "string").map_err(w)?,
            _ => {}
        }
    }
    Ok(page)
}

/// Go `runtime.UnmarshalAsJSON`: an empty body is the zero value; otherwise errors are prefixed with the type.
fn unmarshal_as_json<T: Default>(
    body: &[u8],
    ty: &str,
    f: impl Fn(&Json) -> Result<T, String>,
) -> Result<T, String> {
    if body.is_empty() {
        return Ok(T::default());
    }
    let body = body.strip_prefix(b"\xef\xbb\xbf").unwrap_or(body);
    let v = Json::parse_bytes(body).map_err(|e| format!("unmarshalling type {ty}: {e}"))?;
    f(&v).map_err(|e| format!("unmarshalling type {ty}: {e}"))
}

impl<T> Default for Page<T> {
    fn default() -> Self {
        Page {
            value: Vec::new(),
            next_link: None,
        }
    }
}

// ---------------- errors ----------------

/// An error of an ARM call. `status` is set for an `azcore.ResponseError` (a response with an unexpected status).
#[derive(Clone, Debug, PartialEq)]
pub struct ArmError {
    pub status: Option<u16>,
    pub msg: String,
}

impl std::fmt::Display for ArmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.msg)
    }
}

impl From<String> for ArmError {
    fn from(msg: String) -> ArmError {
        ArmError { status: None, msg }
    }
}

const SEPARATOR: &str =
    "--------------------------------------------------------------------------------";

fn extract_error_code(body: &[u8]) -> String {
    if let Ok(v) = Json::parse_bytes(body) {
        let inner = match (&v, v.get("error"), v.get("odata.error")) {
            (Json::Obj(_), Some(w), _) => match w {
                Json::Obj(_) => w,
                _ => return String::new(),
            },
            (Json::Obj(_), None, Some(w)) => match w {
                Json::Obj(_) => w,
                _ => return String::new(),
            },
            (Json::Obj(_), None, None) => &v,
            _ => return String::new(),
        };
        return inner
            .get("code")
            .and_then(|c| c.as_str())
            .unwrap_or("")
            .to_string();
    }
    // Go: `<(?:\w+:)?[c|C]ode>\s*(\w+)\s*<\/(?:\w+:)?[c|C]ode>`, leftmost match.
    let s = String::from_utf8_lossy(body);
    let b = s.as_bytes();
    let word = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
    let space = |c: u8| matches!(c, b' ' | b'\t' | b'\n' | b'\r' | b'\x0c');
    // the tag name after '<' or '</': optional "\w+:" then "[c|C]ode>"; returns the end index
    let tag = |mut i: usize| -> Option<usize> {
        let start = i;
        while i < b.len() && word(b[i]) {
            i += 1;
        }
        let mut cands = vec![start];
        if i > start && i < b.len() && b[i] == b':' {
            cands.push(i + 1);
        }
        cands.into_iter().rev().find_map(|j| {
            if j + 5 <= b.len() && matches!(b[j], b'c' | b'C' | b'|') && &b[j + 1..j + 5] == b"ode>"
            {
                Some(j + 5)
            } else {
                None
            }
        })
    };
    for st in 0..b.len() {
        if b[st] != b'<' {
            continue;
        }
        let Some(mut i) = tag(st + 1) else { continue };
        while i < b.len() && space(b[i]) {
            i += 1;
        }
        let ws = i;
        while i < b.len() && word(b[i]) {
            i += 1;
        }
        if i == ws {
            continue;
        }
        let w = &s[ws..i];
        // \w+ is greedy but may give back nothing useful: after it only spaces then "</"
        while i < b.len() && space(b[i]) {
            i += 1;
        }
        if i + 1 < b.len() && b[i] == b'<' && b[i + 1] == b'/' && tag(i + 2).is_some() {
            return w.to_string();
        }
    }
    String::new()
}

/// Go `runtime.NewResponseError(resp).Error()`.
pub fn response_error(resp: &net::Response) -> ArmError {
    let mut code = resp.header.get_str("x-ms-error-code").to_string();
    if code.is_empty() && !resp.body.is_empty() {
        code = extract_error_code(&resp.body);
    }
    let u = &resp.request.url;
    let mut msg = format!(
        "{} {}://{}{}\n",
        resp.request.method,
        u.scheme,
        u.host,
        u.escaped_path()
    );
    msg.push_str(SEPARATOR);
    msg.push('\n');
    msg.push_str(&format!("RESPONSE {}: {}\n", resp.status, resp.status_text));
    if !code.is_empty() {
        msg.push_str(&format!("ERROR CODE: {code}\n"));
    } else {
        msg.push_str("ERROR CODE UNAVAILABLE\n");
    }
    msg.push_str(SEPARATOR);
    msg.push('\n');
    if !resp.body.is_empty() {
        msg.push_str(&indent_body(&resp.body));
        msg.push('\n');
    } else {
        msg.push_str("Response contained no body\n");
    }
    msg.push_str(SEPARATOR);
    msg.push('\n');
    ArmError {
        status: Some(resp.status),
        msg,
    }
}

// ---------------- pipeline ----------------

/// Go `policy.RetryOptions` after `setDefaults`.
#[derive(Clone, Debug)]
pub struct RetryOptions {
    /// Retries after the first try (Go `MaxRetries`; 0 disables).
    pub max_retries: u32,
    pub retry_delay: Duration,
    pub max_retry_delay: Duration,
    pub status_codes: Vec<u16>,
    pub try_timeout: Option<Duration>,
}

impl Default for RetryOptions {
    fn default() -> Self {
        RetryOptions {
            max_retries: 3,
            retry_delay: Duration::from_millis(800),
            max_retry_delay: Duration::from_secs(60),
            status_codes: vec![408, 429, 500, 502, 503, 504],
            try_timeout: None,
        }
    }
}

fn random_f64() -> f64 {
    let mut b = [0u8; 8];
    crypto::random_bytes(&mut b);
    (u64::from_le_bytes(b) >> 11) as f64 / (1u64 << 53) as f64
}

/// Go `calcDelay`: exponential back-off with jitter.
fn calc_delay(o: &RetryOptions, try_: u32) -> Duration {
    let factor = if try_ < 63 {
        (1u64 << try_) - 1
    } else {
        u64::MAX
    };
    let base = o.retry_delay.as_secs_f64() * factor as f64;
    let jitter = random_f64() / 2.0 + 0.8;
    let d = Duration::from_secs_f64((base * jitter).min(1e9));
    d.min(o.max_retry_delay)
}

/// Go `shared.RetryAfter` (millisecond headers and `Retry-After` in seconds).
fn retry_after(resp: &net::Response) -> Duration {
    for (h, ms) in [
        ("retry-after-ms", true),
        ("x-ms-retry-after-ms", true),
        ("Retry-After", false),
    ] {
        let v = resp.header.get_str(h);
        if v.is_empty() {
            continue;
        }
        if let Ok(n) = v.trim().parse::<u64>() {
            return if ms {
                Duration::from_millis(n)
            } else {
                Duration::from_secs(n)
            };
        }
    }
    Duration::ZERO
}

/// An error the retry policy does not retry (Go `errorinfo.NonRetriable`).
pub struct NonRetriable(pub String);

/// Go azcore `retryPolicy.Do` around one request: `send` makes one try (and may fail without retry).
pub fn do_with_retry(
    ctx: &CancelToken,
    o: &RetryOptions,
    mut send: impl FnMut(Option<Duration>) -> Result<Result<net::Response, String>, NonRetriable>,
) -> Result<net::Response, String> {
    let mut try_ = 1u32;
    loop {
        if ctx.is_cancelled() {
            return Err("context canceled".to_string());
        }
        let r = match send(o.try_timeout) {
            Err(NonRetriable(e)) => return Err(e),
            Ok(r) => r,
        };
        if ctx.is_cancelled() {
            return Err("context canceled".to_string());
        }
        let resp = match &r {
            Ok(resp) if !o.status_codes.contains(&resp.status) => return r,
            Ok(resp) => Some(resp),
            Err(_) => None,
        };
        if try_ == o.max_retries + 1 {
            return r;
        }
        let mut delay = resp.map(retry_after).unwrap_or(Duration::ZERO);
        if delay.is_zero() {
            delay = calc_delay(o, try_);
        } else if delay > o.max_retry_delay {
            return r;
        }
        if ctx.wait_timeout(delay) {
            return Err("context canceled".to_string());
        }
        try_ += 1;
    }
}

/// The token cache of one Go `BearerTokenPolicy` (one per SDK client).
pub struct Bearer {
    cred: Arc<dyn TokenCredential>,
    scopes: Vec<String>,
    token: Mutex<Option<AccessToken>>,
    allow_http: bool,
}

impl Bearer {
    pub fn new(cred: Arc<dyn TokenCredential>, scopes: Vec<String>, allow_http: bool) -> Bearer {
        Bearer {
            cred,
            scopes,
            token: Mutex::new(None),
            allow_http,
        }
    }

    /// The `Authorization` header value, from the cache or a new token (Go `shouldRefresh`: within five
    /// minutes of expiry, or after `RefreshOn`).
    fn authorization(&self, ctx: &CancelToken, req: &net::Request) -> Result<String, NonRetriable> {
        if !req.url.scheme.eq_ignore_ascii_case("https") && !self.allow_http {
            return Err(NonRetriable(
                "authorized requests are not permitted for non-TLS protected (https) endpoints"
                    .to_string(),
            ));
        }
        let mut t = self.token.lock().unwrap_or_else(|e| e.into_inner());
        let now = SystemTime::now();
        let fresh = t.as_ref().is_some_and(|tk| match tk.refresh_on {
            Some(r) => r > now,
            None => tk.expires_on > now + Duration::from_secs(300),
        });
        if !fresh {
            let tk = self
                .cred
                .get_token(ctx, &self.scopes)
                .map_err(|e| NonRetriable(e.to_string()))?;
            *t = Some(tk);
        }
        Ok(format!(
            "Bearer {}",
            t.as_ref().map(|t| t.token.as_str()).unwrap_or("")
        ))
    }
}

/// The pipeline of one ARM SDK client: telemetry user agent, retries, bearer token, the configured HTTP client.
pub struct ArmPipeline {
    http: net::Client,
    user_agent: String,
    bearer: Bearer,
    retry: RetryOptions,
}

impl ArmPipeline {
    pub fn new(http: net::Client, user_agent: String, bearer: Bearer) -> ArmPipeline {
        ArmPipeline {
            http,
            user_agent,
            bearer,
            retry: RetryOptions::default(),
        }
    }

    /// Sends a GET through the pipeline. `accept`: set `Accept: application/json` (first pages only).
    fn get(&self, ctx: &CancelToken, url: &str, accept: bool) -> Result<net::Response, String> {
        let mut req = net::Request::get(url).map_err(|e| e.to_string())?;
        req.decompress = true;
        if accept {
            req.header.set("Accept", "application/json");
        }
        let ua = match req.header.get("User-Agent") {
            Some(u) if !u.is_empty() => format!("{} {u}", self.user_agent),
            _ => self.user_agent.clone(),
        };
        req.header.set("User-Agent", &ua);
        do_with_retry(ctx, &self.retry, |timeout| {
            let mut r = req.clone();
            r.timeout = timeout;
            let auth = self.bearer.authorization(ctx, &r)?;
            r.header.set("Authorization", &auth);
            Ok(self.http.do_request(r).map_err(|e| e.to_string()))
        })
    }
}

/// Go `runtime.JoinPaths(root, path)` for one path.
pub fn join_paths(root: &str, p: &str) -> String {
    let (root, qps) = match root.split_once('?') {
        Some((r, q)) => (r, q),
        None => (root, ""),
    };
    let mut p = p.to_string();
    if !qps.is_empty() {
        p = if p.contains('?') {
            format!("{p}&{qps}")
        } else {
            format!("{p}?{qps}")
        };
    }
    let mut root = root.to_string();
    if root.ends_with('/') && p.starts_with('/') {
        root.pop();
    } else if !root.ends_with('/') && !p.starts_with('/') && !p.starts_with('?') {
        p = format!("/{p}");
    }
    root + &p
}

fn has_scheme(s: &str) -> bool {
    for (i, c) in s.bytes().enumerate() {
        match c {
            b'a'..=b'z' | b'A'..=b'Z' => {}
            b':' => return i > 0,
            _ if i == 0 => return false,
            b'0'..=b'9' | b'+' | b'-' | b'.' => {}
            _ => return false,
        }
    }
    false
}

/// Go `runtime.NewRequestForNextLink`: the query is re-encoded (sorted, spaces as %20) and a relative link is
/// joined to the endpoint.
pub fn next_link_url(endpoint: &str, next_link: &str) -> Result<String, String> {
    let mut link = next_link.to_string();
    if let Some((before, after)) = next_link.split_once('?') {
        let (pairs, err) = yaml::url::parse_query(&after.replace(';', "%3B"));
        if let Some(e) = err {
            return Err(e);
        }
        link = format!(
            "{before}?{}",
            yaml::url::encode_query(&pairs).replace('+', "%20")
        );
    }
    if !has_scheme(&link) {
        link = join_paths(endpoint, &link);
    }
    Ok(link)
}

fn esc(s: &str) -> String {
    yaml::url::path_escape(s)
}

fn query(pairs: &[(&str, &str)], spaces_as_20: bool) -> String {
    let v: Vec<(String, String)> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let q = yaml::url::encode_query(&v);
    if spaces_as_20 {
        q.replace('+', "%20")
    } else {
        q
    }
}

/// armcompute v8.4.0 api-version of the VM, scale set and scale set VM operations.
pub const COMPUTE_API_VERSION: &str = "2026-04-01";
/// armnetwork v8.0.0 api-version of `InterfacesClient.Get`.
pub const NETWORK_API_VERSION: &str = "2025-03-01";
/// armnetwork v8.0.0 api-version of `InterfacesClient.GetVirtualMachineScaleSetNetworkInterface`.
pub const VMSS_NIC_API_VERSION: &str = "2018-10-01";
/// The `$expand` value Prometheus asks for on network interfaces.
pub const NIC_EXPAND: &str = "IPConfigurations/PublicIPAddress";

/// Go `runtime.Pager` loop over a list operation: the first page from `first_url`, then every `nextLink`.
fn list_all<T>(
    p: &ArmPipeline,
    ctx: &CancelToken,
    endpoint: &str,
    first_url: Result<String, String>,
    ty: &str,
    value_ty: &str,
    f: impl Fn(&Json) -> Result<T, String> + Copy,
) -> Result<Vec<T>, ArmError> {
    let mut out = Vec::new();
    let mut next: Option<String> = None;
    loop {
        let (url, first) = match &next {
            None => (first_url.clone()?, true),
            Some(n) => (next_link_url(endpoint, n)?, false),
        };
        let resp = p.get(ctx, &url, first)?;
        if resp.status != 200 {
            return Err(response_error(&resp));
        }
        let page: Page<T> = unmarshal_as_json(&resp.body, ty, |v| parse_page(v, ty, value_ty, f))?;
        out.extend(page.value);
        match page.next_link {
            Some(n) if !n.is_empty() => next = Some(n),
            _ => return Ok(out),
        }
    }
}

/// The four ARM SDK clients discovery uses (`VirtualMachinesClient`, `VirtualMachineScaleSetsClient`,
/// `VirtualMachineScaleSetVMsClient`, `InterfacesClient`), each with its own pipeline and token cache.
pub struct ArmClients {
    pub subscription_id: String,
    pub endpoint: String,
    pub vm: ArmPipeline,
    pub vmss: ArmPipeline,
    pub vmssvm: ArmPipeline,
    pub nic: ArmPipeline,
}

/// The module names and versions in the SDK telemetry user agent.
pub const ARMCOMPUTE_MODULE: &str =
    "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute";
pub const ARMCOMPUTE_VERSION: &str = "v8.4.0";
pub const ARMNETWORK_MODULE: &str =
    "github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v8";
pub const ARMNETWORK_VERSION: &str = "v8.0.0";

impl ArmClients {
    /// Go `armcompute.New*Client` and `armnetwork.NewInterfacesClient` with the same credential and options.
    pub fn new(
        subscription_id: &str,
        endpoint: &str,
        audience: &str,
        cred: Arc<dyn TokenCredential>,
        http: net::Client,
        application_id: &str,
        allow_http: bool,
    ) -> ArmClients {
        let scopes = vec![format!("{audience}/.default")];
        let compute_ua = telemetry_value(application_id, ARMCOMPUTE_MODULE, ARMCOMPUTE_VERSION);
        let network_ua = telemetry_value(application_id, ARMNETWORK_MODULE, ARMNETWORK_VERSION);
        let pl = |ua: &str| {
            ArmPipeline::new(
                http.clone(),
                ua.to_string(),
                Bearer::new(cred.clone(), scopes.clone(), allow_http),
            )
        };
        ArmClients {
            subscription_id: subscription_id.to_string(),
            endpoint: endpoint.to_string(),
            vm: pl(&compute_ua),
            vmss: pl(&compute_ua),
            vmssvm: pl(&compute_ua),
            nic: pl(&network_ua),
        }
    }

    fn compute_url(&self, path: String, ok: Result<(), String>) -> Result<String, String> {
        ok?;
        Ok(format!(
            "{}?{}",
            join_paths(&self.endpoint, &path),
            query(&[("api-version", COMPUTE_API_VERSION)], true)
        ))
    }

    fn sub_check(&self) -> Result<(), String> {
        if self.subscription_id.is_empty() {
            Err("parameter subscriptionID cannot be empty".to_string())
        } else {
            Ok(())
        }
    }

    /// `VirtualMachinesClient.NewListAllPager` (resource group empty) or `NewListPager`.
    pub fn list_vms(
        &self,
        ctx: &CancelToken,
        resource_group: &str,
    ) -> Result<Vec<ArmVirtualMachine>, ArmError> {
        let sub = esc(&self.subscription_id);
        let url = if resource_group.is_empty() {
            self.compute_url(
                format!("/subscriptions/{sub}/providers/Microsoft.Compute/virtualMachines"),
                self.sub_check(),
            )
        } else {
            let ok = self.sub_check().and_then(|_| {
                if resource_group.is_empty() {
                    Err("parameter resourceGroupName cannot be empty".to_string())
                } else {
                    Ok(())
                }
            });
            self.compute_url(format!("/subscriptions/{sub}/resourceGroups/{}/providers/Microsoft.Compute/virtualMachines", esc(resource_group)), ok)
        };
        list_all(
            &self.vm,
            ctx,
            &self.endpoint,
            url,
            "*armcompute.VirtualMachineListResult",
            "[]*armcompute.VirtualMachine",
            |v| ArmVirtualMachine::from_json(v, false),
        )
    }

    /// `VirtualMachineScaleSetsClient.NewListAllPager` (resource group empty) or `NewListPager`.
    pub fn list_scale_sets(
        &self,
        ctx: &CancelToken,
        resource_group: &str,
    ) -> Result<Vec<ArmScaleSet>, ArmError> {
        let sub = esc(&self.subscription_id);
        if resource_group.is_empty() {
            let url = self.compute_url(
                format!("/subscriptions/{sub}/providers/Microsoft.Compute/virtualMachineScaleSets"),
                self.sub_check(),
            );
            list_all(
                &self.vmss,
                ctx,
                &self.endpoint,
                url,
                "*armcompute.VirtualMachineScaleSetListWithLinkResult",
                "[]*armcompute.VirtualMachineScaleSet",
                ArmScaleSet::from_json,
            )
        } else {
            let url = self.compute_url(format!("/subscriptions/{sub}/resourceGroups/{}/providers/Microsoft.Compute/virtualMachineScaleSets", esc(resource_group)), self.sub_check());
            list_all(
                &self.vmss,
                ctx,
                &self.endpoint,
                url,
                "*armcompute.VirtualMachineScaleSetListResult",
                "[]*armcompute.VirtualMachineScaleSet",
                ArmScaleSet::from_json,
            )
        }
    }

    /// `VirtualMachineScaleSetVMsClient.NewListPager`.
    pub fn list_scale_set_vms(
        &self,
        ctx: &CancelToken,
        resource_group: &str,
        scale_set: &str,
    ) -> Result<Vec<ArmVirtualMachine>, ArmError> {
        let ok = self.sub_check().and_then(|_| {
            if resource_group.is_empty() {
                Err("parameter resourceGroupName cannot be empty".to_string())
            } else if scale_set.is_empty() {
                Err("parameter virtualMachineScaleSetName cannot be empty".to_string())
            } else {
                Ok(())
            }
        });
        let url = self.compute_url(format!("/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Compute/virtualMachineScaleSets/{}/virtualMachines", esc(&self.subscription_id), esc(resource_group), esc(scale_set)), ok);
        list_all(
            &self.vmssvm,
            ctx,
            &self.endpoint,
            url,
            "*armcompute.VirtualMachineScaleSetVMListResult",
            "[]*armcompute.VirtualMachineScaleSetVM",
            |v| ArmVirtualMachine::from_json(v, true),
        )
    }

    fn get_interface(&self, ctx: &CancelToken, url: String) -> Result<ArmInterface, ArmError> {
        let resp = self.nic.get(ctx, &url, true)?;
        if resp.status != 200 {
            return Err(response_error(&resp));
        }
        Ok(unmarshal_as_json(
            &resp.body,
            "*armnetwork.Interface",
            ArmInterface::from_json,
        )?)
    }

    /// `InterfacesClient.Get` with `$expand=IPConfigurations/PublicIPAddress`.
    pub fn get_interface_by_name(
        &self,
        ctx: &CancelToken,
        resource_group: &str,
        name: &str,
    ) -> Result<ArmInterface, ArmError> {
        if resource_group.is_empty() {
            return Err("parameter resourceGroupName cannot be empty"
                .to_string()
                .into());
        }
        if name.is_empty() {
            return Err("parameter networkInterfaceName cannot be empty"
                .to_string()
                .into());
        }
        if self.subscription_id.is_empty() {
            return Err("parameter client.subscriptionID cannot be empty"
                .to_string()
                .into());
        }
        let path = format!(
            "/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Network/networkInterfaces/{}",
            esc(&self.subscription_id),
            esc(resource_group),
            esc(name)
        );
        let url = format!(
            "{}?{}",
            join_paths(&self.endpoint, &path),
            query(
                &[
                    ("$expand", NIC_EXPAND),
                    ("api-version", NETWORK_API_VERSION)
                ],
                false
            )
        );
        self.get_interface(ctx, url)
    }

    /// `InterfacesClient.GetVirtualMachineScaleSetNetworkInterface` with `$expand=IPConfigurations/PublicIPAddress`.
    pub fn get_scale_set_interface(
        &self,
        ctx: &CancelToken,
        resource_group: &str,
        scale_set: &str,
        index: &str,
        name: &str,
    ) -> Result<ArmInterface, ArmError> {
        for (v, n) in [
            (resource_group, "resourceGroupName"),
            (scale_set, "virtualMachineScaleSetName"),
            (index, "virtualmachineIndex"),
            (name, "networkInterfaceName"),
        ] {
            if v.is_empty() {
                return Err(format!("parameter {n} cannot be empty").into());
            }
        }
        if self.subscription_id.is_empty() {
            return Err("parameter client.subscriptionID cannot be empty"
                .to_string()
                .into());
        }
        let path = format!(
            "/subscriptions/{}/resourceGroups/{}/providers/microsoft.Compute/virtualMachineScaleSets/{}/virtualMachines/{}/networkInterfaces/{}",
            esc(&self.subscription_id),
            esc(resource_group),
            esc(scale_set),
            esc(index),
            esc(name)
        );
        let url = format!(
            "{}?{}",
            join_paths(&self.endpoint, &path),
            query(
                &[
                    ("$expand", NIC_EXPAND),
                    ("api-version", VMSS_NIC_API_VERSION)
                ],
                false
            )
        );
        self.get_interface(ctx, url)
    }
}
