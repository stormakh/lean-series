//! The protobuf messages of `discovery/xds` (Kuma `MonitoringAssignment`, envoy `DiscoveryRequest`,
//! `DiscoveryResponse`, `google.protobuf.Any`), with hand-written protobuf wire encoding and decoding (Go
//! `proto.Marshal` deterministic / `proto.Unmarshal` with `DiscardUnknown`) and protojson (Go
//! `protojson.Marshal` with `UseProtoNames`, `protojson.Unmarshal` with `DiscardUnknown` and a resolver that
//! knows only the three registered message types).
//!
//! Differs from Go:
//! - Only the fields Prometheus uses are modelled: `Node` has `id` only, `DiscoveryRequest` has no
//!   `resource_locators`/`error_detail` and `DiscoveryResponse` no `resource_errors`. Those fields are skipped as
//!   unknown fields (Go type-checks them).
//! - protojson errors have no `(line L:C)` position (syntax errors give a line and column), and a JSON key
//!   that appears twice with the same spelling takes the last value instead of failing with "duplicate field".
//! - The "proto: " error prefix always uses plain spaces (Go picks regular or non-breaking spaces at random).
use std::collections::BTreeMap;

use json::Json;

/// Go `errDecode`.
pub const ERR_DECODE: &str = "proto: cannot parse invalid wire-format data";
/// Go `errInvalidUTF8` (proto3 string fields).
pub const ERR_INVALID_UTF8: &str = "string field contains invalid UTF-8";

pub const DISCOVERY_REQUEST_NAME: &str = "envoy.service.discovery.v3.DiscoveryRequest";
pub const DISCOVERY_RESPONSE_NAME: &str = "envoy.service.discovery.v3.DiscoveryResponse";
pub const MONITORING_ASSIGNMENT_NAME: &str = "kuma.observability.v1.MonitoringAssignment";

// ---------------- messages ----------------

/// Go `MonitoringAssignment` (kuma.observability.v1).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MonitoringAssignment {
    pub mesh: String,
    pub service: String,
    pub targets: Vec<MonitoringAssignmentTarget>,
    pub labels: BTreeMap<String, String>,
}

/// Go `MonitoringAssignment_Target`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MonitoringAssignmentTarget {
    pub name: String,
    pub scheme: String,
    pub address: String,
    pub metrics_path: String,
    pub labels: BTreeMap<String, String>,
}

/// Go envoy `core.v3.Node` (only `id`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Node {
    pub id: String,
}

/// Go envoy `core.v3.ControlPlane`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ControlPlane {
    pub identifier: String,
}

/// Go envoy `discovery.v3.DiscoveryRequest`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiscoveryRequest {
    pub version_info: String,
    pub node: Option<Node>,
    pub resource_names: Vec<String>,
    pub type_url: String,
    pub response_nonce: String,
}

/// Go envoy `discovery.v3.DiscoveryResponse`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DiscoveryResponse {
    pub version_info: String,
    pub resources: Vec<Any>,
    pub canary: bool,
    pub type_url: String,
    pub nonce: String,
    pub control_plane: Option<ControlPlane>,
}

/// Go `anypb.Any`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Any {
    pub type_url: String,
    pub value: Vec<u8>,
}

impl Any {
    /// Go `Any.MessageName`: the part after the last '/', or "" when it is not a valid full name.
    pub fn message_name(&self) -> &str {
        let n = match self.type_url.rfind('/') {
            Some(i) => &self.type_url[i + 1..],
            None => &self.type_url,
        };
        if valid_full_name(n) { n } else { "" }
    }

    /// Go `Any.MessageIs`.
    pub fn message_is(&self, full_name: &str) -> bool {
        let u = &self.type_url;
        u.ends_with(full_name) && (u.len() == full_name.len() || u.as_bytes()[u.len() - full_name.len() - 1] == b'/')
    }

    /// Go `anypb.UnmarshalTo` (with `DiscardUnknown`).
    pub fn unmarshal_to<M: Message>(&self) -> Result<M, String> {
        if !self.message_is(M::FULL_NAME) {
            return Err(format!("proto: mismatched message type: got {}, want {}", go_q(M::FULL_NAME), go_q(self.message_name())));
        }
        M::decode(&self.value)
    }
}

/// Go `protoreflect.FullName.IsValid`.
fn valid_full_name(s: &str) -> bool {
    !s.is_empty()
        && s.split('.').all(|p| {
            let b = p.as_bytes();
            !b.is_empty() && (b[0].is_ascii_alphabetic() || b[0] == b'_') && b.iter().all(|c| c.is_ascii_alphanumeric() || *c == b'_')
        })
}

fn go_q(s: &str) -> String {
    yaml::go_quote(s)
}

// ---------------- the message trait ----------------

/// A message with protobuf and protojson forms.
pub trait Message: Default {
    const FULL_NAME: &'static str;
    /// (field number, proto name, JSON name), in declaration order.
    const FIELDS: &'static [(u32, &'static str, &'static str)];
    /// Merges protobuf wire data into `self` (Go `proto.UnmarshalOptions{Merge: true, DiscardUnknown: true}`).
    fn merge(&mut self, b: &[u8]) -> Result<(), String>;
    /// Appends the protobuf encoding (Go deterministic `proto.Marshal`: field number order, sorted map keys).
    fn encode(&self, out: &mut Vec<u8>);
    /// Sets one field from its protojson value (never JSON null).
    fn json_field(&mut self, num: u32, v: &Json) -> Result<(), String>;
    /// The non-default fields as (proto name, protojson text), in declaration order.
    fn json_fields(&self) -> Result<Vec<(&'static str, String)>, String>;

    fn decode(b: &[u8]) -> Result<Self, String> {
        let mut m = Self::default();
        m.merge(b)?;
        Ok(m)
    }
    fn encode_to_vec(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.encode(&mut v);
        v
    }
    /// Go `protojson.MarshalOptions{UseProtoNames: true}.Marshal`.
    fn to_proto_json(&self) -> Result<String, String> {
        Ok(json_obj(&self.json_fields()?))
    }
    /// Go `protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal`.
    fn from_proto_json(b: &[u8]) -> Result<Self, String> {
        let v = parse_json(b)?;
        message_from_json(&v)
    }
}

// ---------------- protobuf wire ----------------

const WT_VARINT: u64 = 0;
const WT_FIXED64: u64 = 1;
const WT_BYTES: u64 = 2;
const WT_START_GROUP: u64 = 3;
const WT_END_GROUP: u64 = 4;
const WT_FIXED32: u64 = 5;
const MAX_FIELD_NUMBER: u64 = (1 << 29) - 1;

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn put_bytes(out: &mut Vec<u8>, num: u32, b: &[u8]) {
    put_varint(out, ((num as u64) << 3) | WT_BYTES);
    put_varint(out, b.len() as u64);
    out.extend_from_slice(b);
}

/// A proto3 string or bytes field: omitted when empty.
fn put_str(out: &mut Vec<u8>, num: u32, s: &[u8]) {
    if !s.is_empty() {
        put_bytes(out, num, s);
    }
}

fn put_msg<M: Message>(out: &mut Vec<u8>, num: u32, m: &M) {
    put_bytes(out, num, &m.encode_to_vec());
}

fn put_map(out: &mut Vec<u8>, num: u32, m: &BTreeMap<String, String>) {
    for (k, v) in m {
        let mut e = Vec::new();
        put_str(&mut e, 1, k.as_bytes());
        put_str(&mut e, 2, v.as_bytes());
        put_bytes(out, num, &e);
    }
}

/// Go `protowire.ConsumeVarint`.
fn get_varint(b: &[u8], p: &mut usize) -> Result<u64, String> {
    let mut v: u64 = 0;
    for i in 0..10 {
        let Some(&c) = b.get(*p) else { return Err(ERR_DECODE.to_string()) };
        *p += 1;
        if i == 9 && c > 1 {
            return Err(ERR_DECODE.to_string());
        }
        v |= ((c & 0x7f) as u64) << (7 * i);
        if c < 0x80 {
            return Ok(v);
        }
    }
    Err(ERR_DECODE.to_string())
}

/// Go `protowire.ConsumeTag`: the field number and wire type.
fn get_tag(b: &[u8], p: &mut usize) -> Result<(u64, u64), String> {
    let t = get_varint(b, p)?;
    let num = t >> 3;
    if num == 0 || num > MAX_FIELD_NUMBER {
        return Err(ERR_DECODE.to_string());
    }
    Ok((num, t & 7))
}

/// A field value read from the wire.
enum Val<'a> {
    Varint(u64),
    Bytes(&'a [u8]),
    /// fixed32, fixed64 and groups (no field here has these types).
    Other,
}

fn take(b: &[u8], p: &mut usize, n: u64) -> Result<(), String> {
    if n > (b.len() - *p) as u64 {
        return Err(ERR_DECODE.to_string());
    }
    *p += n as usize;
    Ok(())
}

/// Go `protowire.ConsumeFieldValue` for one field whose tag was read.
fn get_value<'a>(b: &'a [u8], p: &mut usize, num: u64, wt: u64) -> Result<Val<'a>, String> {
    match wt {
        WT_VARINT => Ok(Val::Varint(get_varint(b, p)?)),
        WT_FIXED64 => take(b, p, 8).map(|_| Val::Other),
        WT_FIXED32 => take(b, p, 4).map(|_| Val::Other),
        WT_BYTES => {
            let n = get_varint(b, p)?;
            let start = *p;
            take(b, p, n)?;
            Ok(Val::Bytes(&b[start..*p]))
        }
        WT_START_GROUP => {
            // skip to the matching end group (iterative: groups may nest)
            let mut stack = vec![num];
            while let Some(&want) = stack.last() {
                let (n2, wt2) = get_tag(b, p)?;
                match wt2 {
                    WT_END_GROUP => {
                        if n2 != want {
                            return Err(ERR_DECODE.to_string());
                        }
                        stack.pop();
                    }
                    WT_START_GROUP => stack.push(n2),
                    _ => {
                        get_value(b, p, n2, wt2)?;
                    }
                }
            }
            Ok(Val::Other)
        }
        _ => Err(ERR_DECODE.to_string()),
    }
}

/// Calls `f` for each field of a message in wire order.
fn for_each_field<'a>(b: &'a [u8], mut f: impl FnMut(u64, Val<'a>) -> Result<(), String>) -> Result<(), String> {
    let mut p = 0;
    while p < b.len() {
        let (num, wt) = get_tag(b, &mut p)?;
        let v = get_value(b, &mut p, num, wt)?;
        f(num, v)?;
    }
    Ok(())
}

fn utf8(b: &[u8]) -> Result<String, String> {
    String::from_utf8(b.to_vec()).map_err(|_| ERR_INVALID_UTF8.to_string())
}

/// A map<string, string> entry: key 1, value 2 (missing ones are "").
fn map_entry(b: &[u8], m: &mut BTreeMap<String, String>) -> Result<(), String> {
    let (mut k, mut v) = (String::new(), String::new());
    for_each_field(b, |num, val| {
        match (num, val) {
            (1, Val::Bytes(x)) => k = utf8(x)?,
            (2, Val::Bytes(x)) => v = utf8(x)?,
            _ => {}
        }
        Ok(())
    })?;
    m.insert(k, v);
    Ok(())
}

fn merge_opt<M: Message>(slot: &mut Option<M>, b: &[u8]) -> Result<(), String> {
    slot.get_or_insert_with(M::default).merge(b)
}

// ---------------- protojson ----------------

/// Go protojson's string escaping.
fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn json_obj(fields: &[(&str, String)]) -> String {
    let mut out = String::from("{");
    for (i, (k, v)) in fields.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&json_str(k));
        out.push(':');
        out.push_str(v);
    }
    out.push('}');
    out
}

fn json_arr(items: Vec<String>) -> String {
    format!("[{}]", items.join(","))
}

fn json_map(m: &BTreeMap<String, String>) -> String {
    let f: Vec<(&str, String)> = m.iter().map(|(k, v)| (k.as_str(), json_str(v))).collect();
    json_obj(&f)
}

fn push_str_field(out: &mut Vec<(&'static str, String)>, name: &'static str, s: &str) {
    if !s.is_empty() {
        out.push((name, json_str(s)));
    }
}

fn push_map_field(out: &mut Vec<(&'static str, String)>, name: &'static str, m: &BTreeMap<String, String>) {
    if !m.is_empty() {
        out.push((name, json_map(m)));
    }
}

/// Parses JSON text; syntax errors read like Go's (`proto: syntax error (line L:C): ...`).
fn parse_json(b: &[u8]) -> Result<Json, String> {
    if std::str::from_utf8(b).is_err() {
        return Err("proto: syntax error: invalid UTF-8 in string".to_string());
    }
    Json::parse_bytes(b).map_err(|e| {
        let off = e.offset.min(b.len());
        let line = b[..off].iter().filter(|c| **c == b'\n').count() + 1;
        let col = off - b[..off].iter().rposition(|c| *c == b'\n').map(|i| i + 1).unwrap_or(0) + 1;
        format!("proto: syntax error (line {line}:{col}): {}", e.msg)
    })
}

fn unexpected(v: &Json) -> String {
    format!("proto: unexpected token {}", v.stringify())
}

fn json_name(fields: &[(u32, &'static str, &'static str)], num: u32) -> &'static str {
    fields.iter().find(|f| f.0 == num).map(|f| f.2).unwrap_or("")
}

/// A protojson string value of field `name` (JSON name).
fn str_val(v: &Json, name: &str) -> Result<String, String> {
    match v {
        Json::Str(s) => Ok(s.clone()),
        _ => Err(format!("proto: invalid value for string field {name}: {}", v.stringify())),
    }
}

fn bool_val(v: &Json, name: &str) -> Result<bool, String> {
    match v {
        Json::Bool(b) => Ok(*b),
        _ => Err(format!("proto: invalid value for bool field {name}: {}", v.stringify())),
    }
}

fn arr_val(v: &Json) -> Result<&Vec<Json>, String> {
    v.as_arr().ok_or_else(|| unexpected(v))
}

fn map_val(v: &Json) -> Result<BTreeMap<String, String>, String> {
    let o = v.as_obj().ok_or_else(|| unexpected(v))?;
    let mut m = BTreeMap::new();
    for (k, x) in o {
        if m.insert(k.clone(), str_val(x, "value")?).is_some() {
            return Err(format!("proto: duplicate map key {}", json_str(k)));
        }
    }
    Ok(m)
}

/// Go protojson `unmarshalMessage` for a JSON object.
fn message_from_json<M: Message>(v: &Json) -> Result<M, String> {
    message_from_json_skip(v, false)
}

fn message_from_json_skip<M: Message>(v: &Json, skip_type_url: bool) -> Result<M, String> {
    let o = v.as_obj().ok_or_else(|| unexpected(v))?;
    let mut m = M::default();
    let mut seen: Vec<u32> = Vec::new();
    for (k, x) in o {
        if skip_type_url && k == "@type" {
            continue;
        }
        let Some(f) = M::FIELDS.iter().find(|f| f.2 == k).or_else(|| M::FIELDS.iter().find(|f| f.1 == k)) else {
            continue; // DiscardUnknown
        };
        if seen.contains(&f.0) {
            return Err(format!("proto: duplicate field {}", json_str(k)));
        }
        seen.push(f.0);
        if x.is_null() {
            continue;
        }
        m.json_field(f.0, x)?;
    }
    Ok(m)
}

fn message_val<M: Message>(v: &Json) -> Result<M, String> {
    message_from_json(v)
}

/// Go protojson `unmarshalAny` with the registered types only.
fn any_from_json(v: &Json) -> Result<Any, String> {
    let o = v.as_obj().ok_or_else(|| unexpected(v))?;
    if o.is_empty() {
        return Ok(Any::default());
    }
    let Some((_, t)) = o.iter().find(|(k, _)| k == "@type") else {
        // DiscardUnknown: treated as an empty Any
        return Ok(Any::default());
    };
    let Json::Str(type_url) = t else {
        return Err(format!("proto: @type field value is not a string: {}", t.stringify()));
    };
    if type_url.is_empty() {
        return Err("proto: @type field contains empty value".to_string());
    }
    let name = type_url.rsplit_once('/').map(|(_, n)| n).unwrap_or(type_url);
    let value = match name {
        MONITORING_ASSIGNMENT_NAME => message_from_json_skip::<MonitoringAssignment>(v, true)?.encode_to_vec(),
        DISCOVERY_REQUEST_NAME => message_from_json_skip::<DiscoveryRequest>(v, true)?.encode_to_vec(),
        DISCOVERY_RESPONSE_NAME => message_from_json_skip::<DiscoveryResponse>(v, true)?.encode_to_vec(),
        _ => return Err(format!("proto: unable to resolve {}: \"not found\"", json_str(type_url))),
    };
    Ok(Any { type_url: type_url.clone(), value })
}

/// Go protojson `marshalAny` with the registered types only.
fn any_to_json(a: &Any) -> Result<String, String> {
    if a.type_url.is_empty() {
        if a.value.is_empty() {
            return Ok("{}".to_string());
        }
        return Err("proto: google.protobuf.Any: type_url is not set".to_string());
    }
    let name = a.type_url.rsplit_once('/').map(|(_, n)| n).unwrap_or(&a.type_url);
    let unable = |e: String| format!("proto: google.protobuf.Any: unable to unmarshal {}: {}", json_str(&a.type_url), e.trim_start_matches("proto: "));
    let fields = match name {
        MONITORING_ASSIGNMENT_NAME => MonitoringAssignment::decode(&a.value).map_err(unable)?.json_fields()?,
        DISCOVERY_REQUEST_NAME => DiscoveryRequest::decode(&a.value).map_err(unable)?.json_fields()?,
        DISCOVERY_RESPONSE_NAME => DiscoveryResponse::decode(&a.value).map_err(unable)?.json_fields()?,
        _ => return Err(format!("proto: google.protobuf.Any: unable to resolve {}: \"not found\"", json_str(&a.type_url))),
    };
    let mut all = vec![("@type", json_str(&a.type_url))];
    all.extend(fields);
    Ok(json_obj(&all))
}

// ---------------- implementations ----------------

impl Message for MonitoringAssignmentTarget {
    const FULL_NAME: &'static str = "kuma.observability.v1.MonitoringAssignment.Target";
    const FIELDS: &'static [(u32, &'static str, &'static str)] =
        &[(1, "name", "name"), (2, "scheme", "scheme"), (3, "address", "address"), (4, "metrics_path", "metricsPath"), (5, "labels", "labels")];

    fn merge(&mut self, b: &[u8]) -> Result<(), String> {
        for_each_field(b, |num, v| {
            match (num, v) {
                (1, Val::Bytes(x)) => self.name = utf8(x)?,
                (2, Val::Bytes(x)) => self.scheme = utf8(x)?,
                (3, Val::Bytes(x)) => self.address = utf8(x)?,
                (4, Val::Bytes(x)) => self.metrics_path = utf8(x)?,
                (5, Val::Bytes(x)) => map_entry(x, &mut self.labels)?,
                _ => {}
            }
            Ok(())
        })
    }
    fn encode(&self, out: &mut Vec<u8>) {
        put_str(out, 1, self.name.as_bytes());
        put_str(out, 2, self.scheme.as_bytes());
        put_str(out, 3, self.address.as_bytes());
        put_str(out, 4, self.metrics_path.as_bytes());
        put_map(out, 5, &self.labels);
    }
    fn json_field(&mut self, num: u32, v: &Json) -> Result<(), String> {
        let n = json_name(Self::FIELDS, num);
        match num {
            1 => self.name = str_val(v, n)?,
            2 => self.scheme = str_val(v, n)?,
            3 => self.address = str_val(v, n)?,
            4 => self.metrics_path = str_val(v, n)?,
            _ => self.labels = map_val(v)?,
        }
        Ok(())
    }
    fn json_fields(&self) -> Result<Vec<(&'static str, String)>, String> {
        let mut f = Vec::new();
        push_str_field(&mut f, "name", &self.name);
        push_str_field(&mut f, "scheme", &self.scheme);
        push_str_field(&mut f, "address", &self.address);
        push_str_field(&mut f, "metrics_path", &self.metrics_path);
        push_map_field(&mut f, "labels", &self.labels);
        Ok(f)
    }
}

impl Message for MonitoringAssignment {
    const FULL_NAME: &'static str = MONITORING_ASSIGNMENT_NAME;
    const FIELDS: &'static [(u32, &'static str, &'static str)] = &[(2, "mesh", "mesh"), (3, "service", "service"), (4, "targets", "targets"), (5, "labels", "labels")];

    fn merge(&mut self, b: &[u8]) -> Result<(), String> {
        for_each_field(b, |num, v| {
            match (num, v) {
                (2, Val::Bytes(x)) => self.mesh = utf8(x)?,
                (3, Val::Bytes(x)) => self.service = utf8(x)?,
                (4, Val::Bytes(x)) => self.targets.push(MonitoringAssignmentTarget::decode(x)?),
                (5, Val::Bytes(x)) => map_entry(x, &mut self.labels)?,
                _ => {}
            }
            Ok(())
        })
    }
    fn encode(&self, out: &mut Vec<u8>) {
        put_str(out, 2, self.mesh.as_bytes());
        put_str(out, 3, self.service.as_bytes());
        for t in &self.targets {
            put_msg(out, 4, t);
        }
        put_map(out, 5, &self.labels);
    }
    fn json_field(&mut self, num: u32, v: &Json) -> Result<(), String> {
        let n = json_name(Self::FIELDS, num);
        match num {
            2 => self.mesh = str_val(v, n)?,
            3 => self.service = str_val(v, n)?,
            4 => self.targets = arr_val(v)?.iter().map(message_val).collect::<Result<_, _>>()?,
            _ => self.labels = map_val(v)?,
        }
        Ok(())
    }
    fn json_fields(&self) -> Result<Vec<(&'static str, String)>, String> {
        let mut f = Vec::new();
        push_str_field(&mut f, "mesh", &self.mesh);
        push_str_field(&mut f, "service", &self.service);
        if !self.targets.is_empty() {
            f.push(("targets", json_arr(self.targets.iter().map(|t| t.to_proto_json()).collect::<Result<_, _>>()?)));
        }
        push_map_field(&mut f, "labels", &self.labels);
        Ok(f)
    }
}

impl Message for Node {
    const FULL_NAME: &'static str = "envoy.config.core.v3.Node";
    const FIELDS: &'static [(u32, &'static str, &'static str)] = &[(1, "id", "id")];

    fn merge(&mut self, b: &[u8]) -> Result<(), String> {
        for_each_field(b, |num, v| {
            if let (1, Val::Bytes(x)) = (num, v) {
                self.id = utf8(x)?;
            }
            Ok(())
        })
    }
    fn encode(&self, out: &mut Vec<u8>) {
        put_str(out, 1, self.id.as_bytes());
    }
    fn json_field(&mut self, _num: u32, v: &Json) -> Result<(), String> {
        self.id = str_val(v, "id")?;
        Ok(())
    }
    fn json_fields(&self) -> Result<Vec<(&'static str, String)>, String> {
        let mut f = Vec::new();
        push_str_field(&mut f, "id", &self.id);
        Ok(f)
    }
}

impl Message for ControlPlane {
    const FULL_NAME: &'static str = "envoy.config.core.v3.ControlPlane";
    const FIELDS: &'static [(u32, &'static str, &'static str)] = &[(1, "identifier", "identifier")];

    fn merge(&mut self, b: &[u8]) -> Result<(), String> {
        for_each_field(b, |num, v| {
            if let (1, Val::Bytes(x)) = (num, v) {
                self.identifier = utf8(x)?;
            }
            Ok(())
        })
    }
    fn encode(&self, out: &mut Vec<u8>) {
        put_str(out, 1, self.identifier.as_bytes());
    }
    fn json_field(&mut self, _num: u32, v: &Json) -> Result<(), String> {
        self.identifier = str_val(v, "identifier")?;
        Ok(())
    }
    fn json_fields(&self) -> Result<Vec<(&'static str, String)>, String> {
        let mut f = Vec::new();
        push_str_field(&mut f, "identifier", &self.identifier);
        Ok(f)
    }
}

impl Message for Any {
    const FULL_NAME: &'static str = "google.protobuf.Any";
    const FIELDS: &'static [(u32, &'static str, &'static str)] = &[(1, "type_url", "typeUrl"), (2, "value", "value")];

    fn merge(&mut self, b: &[u8]) -> Result<(), String> {
        for_each_field(b, |num, v| {
            match (num, v) {
                (1, Val::Bytes(x)) => self.type_url = utf8(x)?,
                (2, Val::Bytes(x)) => self.value = x.to_vec(),
                _ => {}
            }
            Ok(())
        })
    }
    fn encode(&self, out: &mut Vec<u8>) {
        put_str(out, 1, self.type_url.as_bytes());
        put_str(out, 2, &self.value);
    }
    /// Not used: an Any has its own JSON form (`any_from_json`).
    fn json_field(&mut self, _num: u32, _v: &Json) -> Result<(), String> {
        Ok(())
    }
    fn json_fields(&self) -> Result<Vec<(&'static str, String)>, String> {
        Ok(Vec::new())
    }
    fn to_proto_json(&self) -> Result<String, String> {
        any_to_json(self)
    }
    fn from_proto_json(b: &[u8]) -> Result<Self, String> {
        any_from_json(&parse_json(b)?)
    }
}

impl Message for DiscoveryRequest {
    const FULL_NAME: &'static str = DISCOVERY_REQUEST_NAME;
    const FIELDS: &'static [(u32, &'static str, &'static str)] =
        &[(1, "version_info", "versionInfo"), (2, "node", "node"), (3, "resource_names", "resourceNames"), (4, "type_url", "typeUrl"), (5, "response_nonce", "responseNonce")];

    fn merge(&mut self, b: &[u8]) -> Result<(), String> {
        for_each_field(b, |num, v| {
            match (num, v) {
                (1, Val::Bytes(x)) => self.version_info = utf8(x)?,
                (2, Val::Bytes(x)) => merge_opt(&mut self.node, x)?,
                (3, Val::Bytes(x)) => self.resource_names.push(utf8(x)?),
                (4, Val::Bytes(x)) => self.type_url = utf8(x)?,
                (5, Val::Bytes(x)) => self.response_nonce = utf8(x)?,
                _ => {}
            }
            Ok(())
        })
    }
    fn encode(&self, out: &mut Vec<u8>) {
        put_str(out, 1, self.version_info.as_bytes());
        if let Some(n) = &self.node {
            put_msg(out, 2, n);
        }
        for r in &self.resource_names {
            put_bytes(out, 3, r.as_bytes());
        }
        put_str(out, 4, self.type_url.as_bytes());
        put_str(out, 5, self.response_nonce.as_bytes());
    }
    fn json_field(&mut self, num: u32, v: &Json) -> Result<(), String> {
        let n = json_name(Self::FIELDS, num);
        match num {
            1 => self.version_info = str_val(v, n)?,
            2 => self.node = Some(message_val(v)?),
            3 => self.resource_names = arr_val(v)?.iter().map(|x| str_val(x, n)).collect::<Result<_, _>>()?,
            4 => self.type_url = str_val(v, n)?,
            _ => self.response_nonce = str_val(v, n)?,
        }
        Ok(())
    }
    fn json_fields(&self) -> Result<Vec<(&'static str, String)>, String> {
        let mut f = Vec::new();
        push_str_field(&mut f, "version_info", &self.version_info);
        if let Some(n) = &self.node {
            f.push(("node", n.to_proto_json()?));
        }
        if !self.resource_names.is_empty() {
            f.push(("resource_names", json_arr(self.resource_names.iter().map(|s| json_str(s)).collect())));
        }
        push_str_field(&mut f, "type_url", &self.type_url);
        push_str_field(&mut f, "response_nonce", &self.response_nonce);
        Ok(f)
    }
}

impl Message for DiscoveryResponse {
    const FULL_NAME: &'static str = DISCOVERY_RESPONSE_NAME;
    const FIELDS: &'static [(u32, &'static str, &'static str)] =
        &[(1, "version_info", "versionInfo"), (2, "resources", "resources"), (3, "canary", "canary"), (4, "type_url", "typeUrl"), (5, "nonce", "nonce"), (6, "control_plane", "controlPlane")];

    fn merge(&mut self, b: &[u8]) -> Result<(), String> {
        for_each_field(b, |num, v| {
            match (num, v) {
                (1, Val::Bytes(x)) => self.version_info = utf8(x)?,
                (2, Val::Bytes(x)) => self.resources.push(Any::decode(x)?),
                (3, Val::Varint(x)) => self.canary = x != 0,
                (4, Val::Bytes(x)) => self.type_url = utf8(x)?,
                (5, Val::Bytes(x)) => self.nonce = utf8(x)?,
                (6, Val::Bytes(x)) => merge_opt(&mut self.control_plane, x)?,
                _ => {}
            }
            Ok(())
        })
    }
    fn encode(&self, out: &mut Vec<u8>) {
        put_str(out, 1, self.version_info.as_bytes());
        for r in &self.resources {
            put_msg(out, 2, r);
        }
        if self.canary {
            put_varint(out, (3 << 3) | WT_VARINT);
            put_varint(out, 1);
        }
        put_str(out, 4, self.type_url.as_bytes());
        put_str(out, 5, self.nonce.as_bytes());
        if let Some(c) = &self.control_plane {
            put_msg(out, 6, c);
        }
    }
    fn json_field(&mut self, num: u32, v: &Json) -> Result<(), String> {
        let n = json_name(Self::FIELDS, num);
        match num {
            1 => self.version_info = str_val(v, n)?,
            2 => self.resources = arr_val(v)?.iter().map(any_from_json).collect::<Result<_, _>>()?,
            3 => self.canary = bool_val(v, n)?,
            4 => self.type_url = str_val(v, n)?,
            5 => self.nonce = str_val(v, n)?,
            _ => self.control_plane = Some(message_val(v)?),
        }
        Ok(())
    }
    fn json_fields(&self) -> Result<Vec<(&'static str, String)>, String> {
        let mut f = Vec::new();
        push_str_field(&mut f, "version_info", &self.version_info);
        if !self.resources.is_empty() {
            f.push(("resources", json_arr(self.resources.iter().map(any_to_json).collect::<Result<_, _>>()?)));
        }
        if self.canary {
            f.push(("canary", "true".to_string()));
        }
        push_str_field(&mut f, "type_url", &self.type_url);
        push_str_field(&mut f, "nonce", &self.nonce);
        if let Some(c) = &self.control_plane {
            f.push(("control_plane", c.to_proto_json()?));
        }
        Ok(f)
    }
}
