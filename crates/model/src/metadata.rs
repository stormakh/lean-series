//! Port of `model/metadata` (and `model.MetricType` from prometheus/common).

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock, Weak};

/// The type of a metric (Go `model.MetricType`, a string).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub enum MetricType {
    /// The empty string.
    #[default]
    Empty,
    Counter,
    Gauge,
    Histogram,
    GaugeHistogram,
    Summary,
    Info,
    StateSet,
    Unknown,
}

impl MetricType {
    pub fn as_str(self) -> &'static str {
        match self {
            MetricType::Empty => "",
            MetricType::Counter => "counter",
            MetricType::Gauge => "gauge",
            MetricType::Histogram => "histogram",
            MetricType::GaugeHistogram => "gaugehistogram",
            MetricType::Summary => "summary",
            MetricType::Info => "info",
            MetricType::StateSet => "stateset",
            MetricType::Unknown => "unknown",
        }
    }

    /// Parses a type name; None for strings that are not a known type.
    pub fn parse(s: &str) -> Option<MetricType> {
        Some(match s {
            "" => MetricType::Empty,
            "counter" => MetricType::Counter,
            "gauge" => MetricType::Gauge,
            "histogram" => MetricType::Histogram,
            "gaugehistogram" => MetricType::GaugeHistogram,
            "summary" => MetricType::Summary,
            "info" => MetricType::Info,
            "stateset" => MetricType::StateSet,
            "unknown" => MetricType::Unknown,
            _ => return None,
        })
    }
}

impl fmt::Display for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A series' metadata.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct Metadata {
    pub typ: MetricType,
    pub unit: String,
    pub help: String,
}

impl Metadata {
    /// Whether the metadata is empty (an unknown type counts as empty).
    pub fn is_empty(&self) -> bool {
        (self.typ == MetricType::Empty || self.typ == MetricType::Unknown) && self.unit.is_empty() && self.help.is_empty()
    }

    /// Whether `self` is semantically the same as `other` (unknown type equals the empty type).
    pub fn equals(&self, other: &Metadata) -> bool {
        if self.unit != other.unit || self.help != other.help {
            return false;
        }
        if self.typ == MetricType::Empty || self.typ == MetricType::Unknown {
            return other.typ == MetricType::Empty || other.typ == MetricType::Unknown;
        }
        self.typ == other.typ
    }

    /// The JSON object `{"type":..,"unit":..,"help":..}`.
    pub fn to_json(&self) -> json::Json {
        json::Json::obj().with("type", self.typ.as_str()).with("unit", self.unit.as_str()).with("help", self.help.as_str())
    }
}

/// An interned `Metadata` (Go `unique.Handle[Metadata]`). The default value is the zero handle. Equal metadata
/// interned at the same time share one allocation, so handles compare by pointer.
#[derive(Clone, Debug, Default)]
pub struct Handle(Option<Arc<Metadata>>);

impl Handle {
    /// Go `unique.Make`.
    pub fn make(m: Metadata) -> Handle {
        static TABLE: OnceLock<Mutex<HashMap<Metadata, Weak<Metadata>>>> = OnceLock::new();
        let mut t = TABLE.get_or_init(Default::default).lock().unwrap_or_else(|e| e.into_inner());
        if let Some(a) = t.get(&m).and_then(Weak::upgrade) {
            return Handle(Some(a));
        }
        if t.len() > 1024 {
            t.retain(|_, w| w.strong_count() > 0);
        }
        let a = Arc::new(m.clone());
        t.insert(m, Arc::downgrade(&a));
        Handle(Some(a))
    }

    /// The metadata (Go `Handle.Value`). Panics on the zero handle, as Go does.
    pub fn value(&self) -> &Metadata {
        self.0.as_deref().expect("zero unique.Handle")
    }
}

impl PartialEq for Handle {
    fn eq(&self, o: &Handle) -> bool {
        match (&self.0, &o.0) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        }
    }
}

impl Eq for Handle {}

/// Whether `h` is the zero handle (no metadata was set).
pub fn is_zero_handle(h: &Handle) -> bool {
    h.0.is_none()
}
