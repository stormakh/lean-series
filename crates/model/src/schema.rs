//! Port of `schema/labels.go`: the metric schema metadata (name, type, unit) that the type-and-unit feature stores
//! as the `__name__`, `__type__` and `__unit__` labels.

use crate::labels::{Builder, Labels, ScratchBuilder, METRIC_NAME};
use crate::metadata::MetricType;

/// The label that holds the metric type (Go `model.MetricTypeLabel`).
pub const METRIC_TYPE_LABEL: &str = "__type__";
/// The label that holds the metric unit (Go `model.MetricUnitLabel`).
pub const METRIC_UNIT_LABEL: &str = "__unit__";

/// Whether `name` is one of the schema metadata labels.
pub fn is_metadata_label(name: &str) -> bool {
    name == METRIC_NAME || name == METRIC_TYPE_LABEL || name == METRIC_UNIT_LABEL
}

/// The metric schema metadata. `typ` is a Go `model.MetricType`, which is any string ("" means unknown).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Metadata {
    pub name: String,
    pub typ: String,
    pub unit: String,
}

impl Metadata {
    /// Reads the metadata from the labels (an absent type is "unknown").
    pub fn from_labels(ls: &Labels) -> Metadata {
        let t = ls.get(METRIC_TYPE_LABEL);
        Metadata {
            name: ls.get(METRIC_NAME).to_string(),
            typ: if t.is_empty() { MetricType::Unknown.as_str().to_string() } else { t.to_string() },
            unit: ls.get(METRIC_UNIT_LABEL).to_string(),
        }
    }

    /// Whether the type is empty or unknown.
    pub fn is_type_empty(&self) -> bool {
        self.typ.is_empty() || self.typ == MetricType::Unknown.as_str()
    }

    /// Whether the field for `label_name` is empty (true for labels that are not metadata labels).
    pub fn is_empty_for(&self, label_name: &str) -> bool {
        match label_name {
            METRIC_NAME => self.name.is_empty(),
            METRIC_TYPE_LABEL => self.is_type_empty(),
            METRIC_UNIT_LABEL => self.unit.is_empty(),
            _ => true,
        }
    }

    /// Adds the non-empty fields as labels.
    pub fn add_to_labels(&self, b: &mut ScratchBuilder) {
        if !self.name.is_empty() {
            b.add(METRIC_NAME, &self.name);
        }
        if !self.is_type_empty() {
            b.add(METRIC_TYPE_LABEL, &self.typ);
        }
        if !self.unit.is_empty() {
            b.add(METRIC_UNIT_LABEL, &self.unit);
        }
    }

    /// Sets the fields as labels with `Builder::set` semantics (empty fields delete the labels).
    pub fn set_to_labels(&self, b: &mut Builder) {
        b.set(METRIC_NAME, &self.name);
        if self.typ == MetricType::Unknown.as_str() {
            b.set(METRIC_TYPE_LABEL, "");
        } else {
            b.set(METRIC_TYPE_LABEL, &self.typ);
        }
        b.set(METRIC_UNIT_LABEL, &self.unit);
    }

    /// Go `NewIgnoreOverriddenMetadataLabelScratchBuilder`.
    pub fn new_ignore_overridden_metadata_label_scratch_builder<'a>(
        &'a self,
        b: &'a mut ScratchBuilder,
    ) -> IgnoreOverriddenMetadataLabelScratchBuilder<'a> {
        IgnoreOverriddenMetadataLabelScratchBuilder { b, overwrite: self }
    }
}

/// A `ScratchBuilder` wrapper that ignores labels that would collide with non-empty fields of the metadata.
pub struct IgnoreOverriddenMetadataLabelScratchBuilder<'a> {
    pub b: &'a mut ScratchBuilder,
    overwrite: &'a Metadata,
}

impl IgnoreOverriddenMetadataLabelScratchBuilder<'_> {
    /// Adds a name/value pair unless the metadata sets that field.
    pub fn add(&mut self, name: &str, value: &str) {
        if !self.overwrite.is_empty_for(name) {
            return;
        }
        self.b.add(name, value);
    }
}
