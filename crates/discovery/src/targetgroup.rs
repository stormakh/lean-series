//! Port of `discovery/targetgroup`: a group of targets with common labels, and its YAML and JSON forms.
use std::fmt;

use model::labelset::{ADDRESS_LABEL, LabelSet, json_kind};
use yaml::{Decoder, Marshal, Node, Unmarshal, Value};

/// A set of targets with a common label set (Go `targetgroup.Group`).
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub struct Group {
    /// The targets, each identified by its label set (usually only `__address__`).
    pub targets: Vec<LabelSet>,
    /// Labels common to all targets of the group.
    pub labels: LabelSet,
    /// An identifier of the group within its provider.
    pub source: String,
}

/// Go `Group.String()`: the source.
impl fmt::Display for Group {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.source)
    }
}

impl Group {
    /// A group of `__address__` targets.
    pub fn from_addresses<S: AsRef<str>>(addrs: &[S], labels: LabelSet, source: &str) -> Group {
        Group { targets: addrs.iter().map(|a| LabelSet::from_pairs(&[(ADDRESS_LABEL, a.as_ref())])).collect(), labels, source: source.to_string() }
    }

    /// Go `MarshalJSON`: `{"targets":[addresses],"labels":{...}}` (labels omitted when empty).
    pub fn to_json(&self) -> json::Json {
        let mut o = json::Json::obj();
        o.set("targets", json::Json::Arr(self.targets.iter().map(|t| json::Json::Str(t.get(ADDRESS_LABEL).cloned().unwrap_or_default())).collect()));
        if !self.labels.is_empty() {
            o.set("labels", self.labels.to_json());
        }
        o
    }

    /// Go `json.Marshal(group)`.
    pub fn marshal_json(&self) -> String {
        self.to_json().stringify()
    }

    /// Go `UnmarshalJSON` (unknown fields are an error).
    pub fn from_json(v: &json::Json) -> Result<Group, String> {
        let o = match v {
            json::Json::Obj(o) => o,
            // a JSON null leaves a zero group
            json::Json::Null => return Ok(Group::default()),
            other => return Err(format!("json: cannot unmarshal {} into Go value of type struct {{ Targets []string \"json:\\\"targets\\\"\"; Labels model.LabelSet \"json:\\\"labels\\\"\" }}", json_kind(other))),
        };
        let mut targets: Vec<String> = Vec::new();
        let mut labels = LabelSet::new();
        for (k, v) in o {
            // encoding/json matches field names case-insensitively
            match k.to_ascii_lowercase().as_str() {
                "targets" => match v {
                    json::Json::Null => targets.clear(),
                    json::Json::Arr(a) => {
                        targets = Vec::with_capacity(a.len());
                        for t in a {
                            match t {
                                json::Json::Str(s) => targets.push(s.clone()),
                                json::Json::Null => targets.push(String::new()),
                                other => return Err(format!("json: cannot unmarshal {} into Go struct field .targets of type string", json_kind(other))),
                            }
                        }
                    }
                    other => return Err(format!("json: cannot unmarshal {} into Go struct field .targets of type []string", json_kind(other))),
                },
                "labels" => labels = LabelSet::from_json(v)?,
                _ => return Err(format!("json: unknown field {}", json::quote(k))),
            }
        }
        Ok(Group { targets: targets.into_iter().map(|t| LabelSet::from_pairs(&[(ADDRESS_LABEL, &t)])).collect(), labels, source: String::new() })
    }

    /// Go `UnmarshalJSON` of JSON text.
    pub fn unmarshal_json(b: &str) -> Result<Group, String> {
        let v = json::Json::parse(b).map_err(|e| e.to_string())?;
        Group::from_json(&v)
    }

    /// Go `json.Unmarshal(b, &[]*targetgroup.Group)`; a null item is None.
    pub fn list_from_json(b: &[u8]) -> Result<Vec<Option<Group>>, String> {
        let v = json::Json::parse_bytes(b).map_err(|e| e.to_string())?;
        match v {
            json::Json::Null => Ok(Vec::new()),
            json::Json::Arr(a) => {
                let mut out = Vec::with_capacity(a.len());
                for g in &a {
                    if g.is_null() {
                        out.push(None);
                    } else {
                        out.push(Some(Group::from_json(g)?));
                    }
                }
                Ok(out)
            }
            other => Err(format!("json: cannot unmarshal {} into Go value of type []*targetgroup.Group", json_kind(&other))),
        }
    }
}

// The anonymous struct of Go's UnmarshalYAML.
#[derive(Default)]
struct YamlGroup {
    targets: Vec<String>,
    labels: LabelSet,
}
yaml::yaml_struct!(YamlGroup, "struct { Targets []string \"yaml:\\\"targets\\\"\"; Labels model.LabelSet \"yaml:\\\"labels\\\"\" }", [
    (targets: "targets")
    (labels: "labels")
]);

impl Unmarshal for Group {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            let mut g = YamlGroup::default();
            d.inner_struct(n, &mut g)?;
            self.targets = g.targets.iter().map(|t| LabelSet::from_pairs(&[(ADDRESS_LABEL, t)])).collect();
            self.labels = g.labels;
            Ok(())
        })
    }
    fn go_type() -> String {
        "targetgroup.Group".to_string()
    }
}

impl Marshal for Group {
    /// Go `MarshalYAML`: `targets` (addresses) and `labels` (omitted when empty).
    fn marshal_yaml(&self) -> Result<Value, String> {
        let mut out = vec![(Value::Str("targets".into()), Value::Seq(self.targets.iter().map(|t| Value::Str(t.get(ADDRESS_LABEL).cloned().unwrap_or_default())).collect()))];
        if !self.labels.is_empty() {
            out.push((Value::Str("labels".into()), self.labels.marshal_yaml()?));
        }
        Ok(Value::Map(out))
    }
    fn is_zero(&self) -> bool {
        false
    }
}
