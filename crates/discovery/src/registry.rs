//! Port of `discovery/registry.go`: the SD config types by YAML key, `Configs` (a list of configs that reads and
//! writes as `<name>_sd_configs` keys), and the inline form used by scrape and Alertmanager configs.
//!
//! Go builds a struct type at run time with one `[]*T` field per registered type, sorted by field name, and
//! decodes into it; here a decoding wrapper answers those keys. Keys are handled in the same sorted order,
//! static configs last, and errors have the same text.
use std::any::TypeId;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, OnceLock, RwLock};

use yaml::{Decoder, Kind, Marshal, Node, Struct, Unmarshal, Value};

use crate::metrics::Registry;
use crate::targetgroup::Group;
use crate::{Config, DiscovererMetrics, RefreshMetricsInstantiator, StaticConfig};

pub const STATIC_CONFIGS_KEY: &str = "static_configs";

/// An SD config type that can be registered: decodable, encodable, with a default (Go zero) value.
pub trait SdConfigType: Config + Unmarshal + Marshal + Default + Clone + PartialEq {
    /// The Go type name, as in `[]*file.SDConfig` type errors.
    const GO_TYPE: &'static str;
}

type DecodeFn = fn(&Node, &mut Decoder) -> yaml::Result<Vec<Option<Box<dyn Config>>>>;
type MarshalFn = fn(&dyn Config) -> Result<Value, String>;
type MetricsFn = fn(&Arc<Registry>, Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics>;

/// One registered SD config type.
pub struct Registration {
    pub name: &'static str,
    pub yaml_key: String,
    type_id: TypeId,
    decode: DecodeFn,
    marshal: MarshalFn,
    pub new_metrics: MetricsFn,
}

/// The providers built into this crate. Adding a provider: one line here (see README.md).
fn builtin() -> Vec<Registration> {
    vec![
        registration::<crate::file::SdConfig>(),
        registration::<crate::http::SdConfig>(),
        registration::<crate::linode::SdConfig>(),
        registration::<crate::puppetdb::SdConfig>(),
        registration::<crate::uyuni::SdConfig>(),
        registration::<crate::vultr::SdConfig>(),
    ]
}

fn registry() -> &'static RwLock<Vec<Arc<Registration>>> {
    static R: OnceLock<RwLock<Vec<Arc<Registration>>>> = OnceLock::new();
    R.get_or_init(|| {
        let mut v: Vec<Arc<Registration>> = Vec::new();
        for r in builtin() {
            if v.iter().any(|o| o.name == r.name) {
                panic!("discovery: Config named {} is already registered", yaml::go_quote(r.name));
            }
            v.push(Arc::new(r));
        }
        RwLock::new(v)
    })
}

/// The registrations, sorted by YAML key.
pub fn registrations() -> Vec<Arc<Registration>> {
    let mut v = registry().read().unwrap_or_else(|e| e.into_inner()).clone();
    v.sort_by(|a, b| a.yaml_key.cmp(&b.yaml_key));
    v
}

fn lookup_key(key: &str) -> Option<Arc<Registration>> {
    registry().read().unwrap_or_else(|e| e.into_inner()).iter().find(|r| r.yaml_key == key).cloned()
}

fn lookup_type(t: TypeId) -> Option<Arc<Registration>> {
    registry().read().unwrap_or_else(|e| e.into_inner()).iter().find(|r| r.type_id == t).cloned()
}

fn decode_list<T: SdConfigType>(n: &Node, d: &mut Decoder) -> yaml::Result<Vec<Option<Box<dyn Config>>>> {
    let mut l: List<T> = List(Vec::new(), std::marker::PhantomData);
    d.unmarshal(n, &mut l)?;
    Ok(l.0.into_iter().map(|c| c.map(|c| Box::new(c) as Box<dyn Config>)).collect())
}

fn marshal_one<T: SdConfigType>(c: &dyn Config) -> Result<Value, String> {
    match c.as_any().downcast_ref::<T>() {
        Some(t) => t.marshal_yaml(),
        None => Err("discovery: internal error: config type mismatch".to_string()),
    }
}

fn new_metrics<T: SdConfigType>(reg: &Arc<Registry>, rmi: Arc<dyn RefreshMetricsInstantiator>) -> Arc<dyn DiscovererMetrics> {
    T::default().new_discoverer_metrics(reg, rmi)
}

/// The registration of an SD config type (for `builtin` and `register_config`).
pub fn registration<T: SdConfigType>() -> Registration {
    let name = T::default().name();
    Registration { name, yaml_key: format!("{name}_sd_configs"), type_id: TypeId::of::<T>(), decode: decode_list::<T>, marshal: marshal_one::<T>, new_metrics: new_metrics::<T> }
}

/// Go `RegisterConfig`: registers an SD config type at run time (panics if the name is taken).
pub fn register_config<T: SdConfigType>() {
    let r = registration::<T>();
    let mut v = registry().write().unwrap_or_else(|e| e.into_inner());
    if v.iter().any(|o| o.name == r.name) || r.name == "static" {
        panic!("discovery: Config named {} is already registered", yaml::go_quote(r.name));
    }
    v.push(Arc::new(r));
}

/// Go `RegisteredConfigNames`: the names of all registered providers, sorted (with "static").
pub fn registered_config_names() -> Vec<String> {
    let mut names: Vec<String> = registry().read().unwrap_or_else(|e| e.into_inner()).iter().map(|r| r.name.to_string()).collect();
    names.push("static".to_string());
    names.sort();
    names
}

/// Go `[]*T`: a sequence of nullable items.
struct List<T>(Vec<Option<T>>, std::marker::PhantomData<T>);

impl<T> Default for List<T> {
    fn default() -> Self {
        List(Vec::new(), std::marker::PhantomData)
    }
}

trait GoSliceType {
    fn slice_type() -> String;
}
impl<T: SdConfigType> GoSliceType for T {
    fn slice_type() -> String {
        format!("[]*{}", T::GO_TYPE)
    }
}
impl GoSliceType for Group {
    fn slice_type() -> String {
        "[]*targetgroup.Group".to_string()
    }
}

impl<T: Unmarshal + Default + GoSliceType> Unmarshal for List<T> {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        match n.kind {
            Kind::Sequence => {
                let mut out = Vec::with_capacity(n.children.len());
                for c in n.children.iter() {
                    let mut e: Option<T> = None;
                    if d.unmarshal(c, &mut e)? {
                        out.push(e);
                    }
                }
                self.0 = out;
                Ok(true)
            }
            Kind::Mapping => {
                d.terror(n, yaml::node::MAP_TAG, &T::slice_type());
                Ok(false)
            }
            _ => {
                let (tag, _) = d.scalar_tag(n)?;
                d.terror(n, &tag, &T::slice_type());
                Ok(false)
            }
        }
    }
    fn go_type() -> String {
        T::slice_type()
    }
}

// ---------------- Configs ----------------

/// A list of SD configs (Go `discovery.Configs`). Static target groups are one `StaticConfig`.
#[derive(Clone, Default, PartialEq)]
pub struct Configs(pub Vec<Box<dyn Config>>);

impl fmt::Debug for Configs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.0.iter()).finish()
    }
}

impl std::ops::Deref for Configs {
    type Target = Vec<Box<dyn Config>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl std::ops::DerefMut for Configs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Configs {
    /// Go `SetDirectory`.
    pub fn set_directory(&mut self, dir: &str) {
        for c in self.0.iter_mut() {
            c.set_directory(dir);
        }
    }
}

/// The decoded `<name>_sd_configs` lists, by YAML key (sorted as Go's field names).
#[derive(Default)]
struct Slots {
    static_groups: Option<Vec<Option<Group>>>,
    lists: BTreeMap<String, Vec<Option<Box<dyn Config>>>>,
}

impl Slots {
    fn has_key(key: &str) -> bool {
        key == STATIC_CONFIGS_KEY || lookup_key(key).is_some()
    }

    fn decode(&mut self, key: &str, n: &Node, d: &mut Decoder) -> Option<yaml::Result<bool>> {
        if key == STATIC_CONFIGS_KEY {
            let mut l: List<Group> = List::default();
            let r = d.unmarshal(n, &mut l);
            self.static_groups = Some(l.0);
            return Some(r);
        }
        let reg = lookup_key(key)?;
        Some((reg.decode)(n, d).map(|v| {
            self.lists.insert(key.to_string(), v);
            true
        }))
    }

    /// Go `readConfigs`: fields in sorted key order, static groups last as one `StaticConfig`.
    fn read_configs(self) -> Result<Configs, String> {
        let mut keys: Vec<String> = self.lists.keys().cloned().collect();
        if self.static_groups.is_some() {
            keys.push(STATIC_CONFIGS_KEY.to_string());
        }
        keys.sort();
        let mut lists = self.lists;
        let mut static_groups = self.static_groups;
        let mut configs: Vec<Box<dyn Config>> = Vec::new();
        let mut targets: Vec<Group> = Vec::new();
        for k in keys {
            if k == STATIC_CONFIGS_KEY {
                for g in static_groups.take().unwrap_or_default() {
                    match g {
                        None => return Err(format!("empty or null section in {k}")),
                        Some(mut g) => {
                            g.source = targets.len().to_string();
                            targets.push(g);
                        }
                    }
                }
            } else {
                for c in lists.remove(&k).unwrap_or_default() {
                    match c {
                        None => return Err(format!("empty or null section in {k}")),
                        Some(c) => configs.push(c),
                    }
                }
            }
        }
        if !targets.is_empty() {
            configs.push(Box::new(StaticConfig(targets)));
        }
        Ok(Configs(configs))
    }
}

/// Go `writeConfigs` + the sorted fields: the `<name>_sd_configs` pairs (omitempty).
fn write_configs(configs: &Configs, out: &mut Vec<(Value, Value)>) -> Result<(), String> {
    let mut by_key: BTreeMap<String, Vec<Value>> = BTreeMap::new();
    for c in configs.iter() {
        if let Some(sc) = c.as_any().downcast_ref::<StaticConfig>() {
            let e = by_key.entry(STATIC_CONFIGS_KEY.to_string()).or_default();
            for g in &sc.0 {
                e.push(g.marshal_yaml()?);
            }
            continue;
        }
        let reg = lookup_type(c.as_any().type_id()).ok_or_else(|| format!("discovery: cannot marshal unregistered Config type: {}", go_type_of(c.as_ref())))?;
        let v = (reg.marshal)(c.as_ref())?;
        by_key.entry(reg.yaml_key.clone()).or_default().push(v);
    }
    for (k, v) in by_key {
        if !v.is_empty() {
            out.push((Value::Str(k), Value::Seq(v)));
        }
    }
    Ok(())
}

fn go_type_of(c: &dyn Config) -> String {
    format!("{c:?}").split(['(', ' ', '{']).next().unwrap_or("").to_string()
}

/// The decoding view of a struct with inline SD configs.
struct Inline<'a, T: Struct> {
    inner: &'a mut T,
    slots: Slots,
}

impl<T: Struct> Struct for Inline<'_, T> {
    fn type_name(&self) -> &'static str {
        self.inner.type_name()
    }
    fn has_field(&self, k: &str) -> bool {
        self.inner.has_field(k) || Slots::has_key(k)
    }
    fn field(&mut self, k: &str) -> Option<&mut dyn yaml::Field> {
        self.inner.field(k)
    }
    fn decode_key(&mut self, key: &str, n: &Node, d: &mut Decoder) -> Option<yaml::Result<bool>> {
        if self.inner.has_field(key) {
            return self.inner.decode_key(key, n, d);
        }
        self.slots.decode(key, n, d)
    }
    fn marshal_fields(&self, out: &mut Vec<(Value, Value)>) -> Result<(), String> {
        self.inner.marshal_fields(out)
    }
    fn fields_zero(&self) -> bool {
        self.inner.fields_zero()
    }
}

/// Go `UnmarshalYAMLWithInlineConfigs`: decodes `n` into the plain struct `out` (its `Struct` fields) and the
/// `<name>_sd_configs` keys into `configs`. Unknown keys and type errors mention `out`'s type name.
pub fn unmarshal_yaml_with_inline_configs<T: Struct>(n: &Node, d: &mut Decoder, out: &mut T, configs: &mut Configs) -> yaml::Result<()> {
    let mut nulls = Vec::new();
    unmarshal_yaml_with_inline_configs_nulls(n, d, out, configs, &mut nulls)
}

/// Like `unmarshal_yaml_with_inline_configs`, and also returns the null items of `ptrs` fields (see
/// `yaml::Decoder::inner_struct_nulls`).
pub fn unmarshal_yaml_with_inline_configs_nulls<T: Struct>(n: &Node, d: &mut Decoder, out: &mut T, configs: &mut Configs, nulls: &mut Vec<(&'static str, usize)>) -> yaml::Result<()> {
    let mut w = Inline { inner: out, slots: Slots::default() };
    *nulls = d.inner_struct_nulls(n, &mut w)?;
    *configs = w.slots.read_configs().map_err(yaml::Error::Msg)?;
    Ok(())
}

/// Go `MarshalYAMLWithInlineConfigs`: the struct's fields followed by the SD config keys.
pub fn marshal_yaml_with_inline_configs<T: Struct>(v: &T, configs: &Configs) -> Result<Value, String> {
    let mut out = Vec::new();
    v.marshal_fields(&mut out)?;
    write_configs(configs, &mut out)?;
    Ok(Value::Map(out))
}

// Configs on its own (Go `Configs.UnmarshalYAML` / `MarshalYAML`): a struct with only the SD keys.
#[derive(Default)]
struct Empty;
impl Struct for Empty {
    fn type_name(&self) -> &'static str {
        "discovery.Configs"
    }
    fn has_field(&self, _k: &str) -> bool {
        false
    }
    fn field(&mut self, _k: &str) -> Option<&mut dyn yaml::Field> {
        None
    }
    fn marshal_fields(&self, _out: &mut Vec<(Value, Value)>) -> Result<(), String> {
        Ok(())
    }
    fn fields_zero(&self) -> bool {
        true
    }
}

impl Unmarshal for Configs {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            let mut e = Empty;
            unmarshal_yaml_with_inline_configs(n, d, &mut e, self)
        })
    }
    fn go_type() -> String {
        "discovery.Configs".to_string()
    }
}

impl Marshal for Configs {
    fn marshal_yaml(&self) -> Result<Value, String> {
        marshal_yaml_with_inline_configs(&Empty, self)
    }
    fn is_zero(&self) -> bool {
        self.0.is_empty()
    }
}
