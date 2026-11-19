//! Port of `metrics_to_prw.go`: the converter that appends OTLP metrics to a `storage::AppenderV2`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Duration;

use model::labels::{Builder, Labels, ScratchBuilder};
use model::metadata::Metadata;
use storage::{Annotations, AppenderV2, Context, Error};

use super::super::namer::{self, LabelNamer, MetricNamer, UnitNamer};
use super::super::pdata::{self, AggregationTemporality, Map, MetricType, Resource, ScopeMetrics, Timestamp};
use super::annotations::{WarningCategory, new_categorized_warning};
use super::context::EveryNTimes;
use super::metadata::otel_metric_type_to_prom_metric_type;
use super::{SERVICE_INSTANCE_ID_KEY, SERVICE_NAME_KEY, SERVICE_NAMESPACE_KEY};

/// Go `PromoteResourceAttributes`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PromoteResourceAttributes {
    pub promote_all: bool,
    pub attrs: HashSet<String>,
}

/// Go `Settings`.
#[derive(Clone, Debug, Default)]
pub struct Settings {
    pub namespace: String,
    pub external_labels: BTreeMap<String, String>,
    pub disable_target_info: bool,
    pub add_metric_suffixes: bool,
    pub allow_utf8: bool,
    pub promote_resource_attributes: Option<PromoteResourceAttributes>,
    pub keep_identifying_resource_attributes: bool,
    pub convert_histograms_to_nhcb: bool,
    pub allow_delta_temporality: bool,
    /// The PromQL engine lookback delta.
    pub lookback_delta: Duration,
    /// Promote OTel scope metadata to metric labels.
    pub promote_scope_metadata: bool,
    pub enable_type_and_unit_labels: bool,
    /// Prepend `key` to labels that start with a single `_`.
    pub label_name_underscore_sanitization: bool,
    /// Keep runs of underscores in label names when UTF-8 is not allowed.
    pub label_name_preserve_multiple_underscores: bool,
}

/// Labels that are the same for every data point of a `ResourceMetrics` (Go `cachedResourceLabels`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CachedResourceLabels {
    /// From service.name and service.namespace.
    pub job_label: String,
    /// From service.instance.id.
    pub instance_label: String,
    pub promoted_labels: Labels,
    pub external_labels: BTreeMap<String, String>,
}

/// Scope metadata labels (Go `cachedScopeLabels`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CachedScopeLabels {
    pub scope_name: String,
    pub scope_version: String,
    pub scope_schema_url: String,
    /// The `otel_scope_*` attribute labels.
    pub scope_attrs: Labels,
}

/// Which attributes are being converted, for collision warnings (Go `collisionAttrSource`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CollisionAttrSource {
    #[default]
    DataPoint,
    Resource,
}

impl CollisionAttrSource {
    pub fn as_str(self) -> &'static str {
        match self {
            CollisionAttrSource::Resource => "resource",
            CollisionAttrSource::DataPoint => "data point",
        }
    }
}

/// Identifies a target_info sample (Go `targetInfoKey`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct TargetInfoKey {
    pub labels_hash: u64,
    pub timestamp: i64,
}

/// Go `maxSanitizedLabels`: the label name cache is kept across requests only up to this size.
pub const MAX_SANITIZED_LABELS: usize = 64;

/// Converts OTLP metrics to Prometheus samples (Go `PrometheusConverter`). The fields are public for the tests.
#[derive(Default)]
pub struct PrometheusConverter {
    pub every_n: EveryNTimes,
    pub scratch_builder: ScratchBuilder,
    pub builder: Builder,
    pub appender: Option<Box<dyn AppenderV2>>,
    /// target_info samples of this batch, to skip duplicates.
    pub seen_target_info: HashSet<TargetInfoKey>,
    pub resource_labels: Option<CachedResourceLabels>,
    pub scope_labels: Option<CachedScopeLabels>,
    pub label_namer: LabelNamer,
    /// Cache of sanitized label names (`None` is Go's nil map).
    pub sanitized_labels: Option<HashMap<String, String>>,
    /// Warnings about classic histogram points with an explicit +Inf bound.
    pub plus_inf_annots: Annotations,
    /// Warnings about attribute names that collide after sanitization.
    pub collision_annots: Annotations,
    /// Label names whose collision was already recorded in this `from_metrics` call.
    pub recorded_collisions: Option<HashSet<String>>,
    pub collision_source: CollisionAttrSource,
}

/// Go `scope`: the scope of a `ScopeMetrics`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Scope {
    pub name: String,
    pub version: String,
    pub schema_url: String,
    pub attributes: Map,
}

impl Scope {
    /// Go `newScopeFromScopeMetrics`.
    pub fn from_scope_metrics(sm: &ScopeMetrics) -> Scope {
        Scope {
            name: sm.scope.name.clone(),
            version: sm.scope.version.clone(),
            schema_url: sm.schema_url.clone(),
            attributes: sm.scope.attributes.clone(),
        }
    }
}

/// Go `AppOpts` as the converter builds it per metric (`storage.AOptions` without exemplars).
#[derive(Clone, Debug, Default)]
pub struct AppOpts {
    pub metadata: Metadata,
    pub metric_family_name: String,
}

/// Go `buildLabelName` over the converter's cache (a free function so that the builders can stay borrowed).
pub(super) fn build_label_name(
    cache: &mut Option<HashMap<String, String>>,
    namer: &LabelNamer,
    label: &str,
) -> Result<String, Error> {
    if let Some(s) = cache.as_ref().and_then(|c| c.get(label)) {
        return Ok(s.clone());
    }
    let sanitized = namer.build(label).map_err(Error::msg)?;
    // Go panics writing to a nil map; a converter that was reset with no appender is never used.
    cache.get_or_insert_with(HashMap::new).insert(label.to_string(), sanitized.clone());
    Ok(sanitized)
}

impl PrometheusConverter {
    /// Go `NewPrometheusConverter`.
    pub fn new(appender: Option<Box<dyn AppenderV2>>) -> PrometheusConverter {
        let mut c = PrometheusConverter::default();
        c.reset(appender);
        c
    }

    /// Go `Reset`: prepares the converter for a request with `appender`, keeping only the bounded label name
    /// cache. Must only be called after the previous appender was committed or rolled back. Passing `None`
    /// clears request-local state before the converter goes back to a pool. Returns the previous appender.
    pub fn reset(&mut self, appender: Option<Box<dyn AppenderV2>>) -> Option<Box<dyn AppenderV2>> {
        let mut sanitized = self.sanitized_labels.take();
        match &mut sanitized {
            Some(m) if m.len() > MAX_SANITIZED_LABELS => sanitized = None,
            Some(m) => m.clear(),
            None => {}
        }
        let has_appender = appender.is_some();
        let old = std::mem::replace(
            self,
            PrometheusConverter { appender, sanitized_labels: sanitized, ..Default::default() },
        );
        if has_appender {
            // Builders keep their input strings after a reset, so use fresh ones for every request.
            self.scratch_builder = ScratchBuilder::new(0);
            self.builder = Builder::new(Labels::default());
            if self.sanitized_labels.is_none() {
                self.sanitized_labels = Some(HashMap::with_capacity(MAX_SANITIZED_LABELS));
            }
        }
        old.appender
    }

    /// The appender, to commit or roll back after `from_metrics`.
    pub fn appender_mut(&mut self) -> Option<&mut (dyn AppenderV2 + 'static)> {
        self.appender.as_deref_mut()
    }

    /// Go `buildLabelName`: a sanitized label name, cached.
    pub fn build_label_name(&mut self, label: &str) -> Result<String, Error> {
        build_label_name(&mut self.sanitized_labels, &self.label_namer, label)
    }

    /// Go `FromMetrics`: appends the metrics to the appender. Returns warnings and the joined errors of the
    /// metrics that failed; the other metrics are still appended.
    pub fn from_metrics(&mut self, ctx: &Context, md: &pdata::Metrics, settings: &Settings) -> (Annotations, Option<Error>) {
        let mut annots = Annotations::new();
        let errs = self.from_metrics_inner(ctx, md, settings, &mut annots);
        annots.merge(std::mem::take(&mut self.collision_annots));
        annots.merge(std::mem::take(&mut self.plus_inf_annots));
        (annots, Error::join(errs.into_iter().map(Some)))
    }

    fn from_metrics_inner(
        &mut self,
        ctx: &Context,
        md: &pdata::Metrics,
        settings: &Settings,
        annots: &mut Annotations,
    ) -> Vec<Error> {
        let namer = MetricNamer {
            namespace: settings.namespace.clone(),
            with_metric_suffixes: settings.add_metric_suffixes,
            utf8_allowed: settings.allow_utf8,
        };
        let unit_namer = UnitNamer::default();
        self.every_n = EveryNTimes::new(128);
        self.seen_target_info = HashSet::new();
        self.collision_annots = Annotations::new();
        self.plus_inf_annots = Annotations::new();
        self.recorded_collisions = None;
        self.collision_source = CollisionAttrSource::DataPoint;
        let mut errs: Vec<Error> = Vec::new();

        for resource_metrics in &md.resource_metrics {
            let resource = &resource_metrics.resource;
            if let Err(e) = self.set_resource_context(resource, settings) {
                errs.push(e);
                continue;
            }
            // The earliest and latest timestamps of the resource, for target_info.
            let mut earliest_timestamp: Timestamp = u64::MAX;
            let mut latest_timestamp: Timestamp = 0;
            for scope_metrics in &resource_metrics.scope_metrics {
                let scope = Scope::from_scope_metrics(scope_metrics);
                if let Err(e) = self.set_scope_context(&scope, settings) {
                    errs.push(e);
                    continue;
                }
                for metric in &scope_metrics.metrics {
                    if let Err(e) = self.every_n.check_context(ctx) {
                        errs.push(e);
                        return errs;
                    }
                    (earliest_timestamp, latest_timestamp) =
                        find_min_and_max_timestamps(metric, earliest_timestamp, latest_timestamp);
                    let (temporality, has_temporality) = match aggregation_temporality(metric) {
                        Ok(t) => t,
                        Err(e) => {
                            errs.push(e);
                            continue;
                        }
                    };
                    if has_temporality
                        && !(temporality == AggregationTemporality::CUMULATIVE
                            || (settings.allow_delta_temporality && temporality == AggregationTemporality::DELTA))
                    {
                        errs.push(Error::msg(format!(
                            "invalid temporality and type combination for metric {}",
                            namer::go_quote(&metric.name)
                        )));
                        continue;
                    }
                    let prom_name = match namer.build(&translator_metric_from_otel_metric(metric)) {
                        Ok(n) => n,
                        Err(e) => {
                            errs.push(Error::msg(e));
                            continue;
                        }
                    };
                    let app_opts = AppOpts {
                        metadata: Metadata {
                            typ: otel_metric_type_to_prom_metric_type(metric),
                            unit: unit_namer.build(&metric.unit),
                            help: metric.description.clone(),
                        },
                        metric_family_name: prom_name,
                    };
                    if metric.data_points_len() == 0 {
                        annots.add(new_categorized_warning(
                            WarningCategory::EmptyDataPoints,
                            format!("empty data points. {} is dropped", metric.name),
                        ));
                        continue;
                    }
                    let res = match metric.typ() {
                        MetricType::Gauge => {
                            self.add_gauge_number_data_points(ctx, &metric.gauge().data_points, settings, &app_opts)
                        }
                        MetricType::Sum => {
                            self.add_sum_number_data_points(ctx, &metric.sum().data_points, settings, &app_opts)
                        }
                        MetricType::Histogram => {
                            let dps = &metric.histogram().data_points;
                            if settings.convert_histograms_to_nhcb {
                                let (ws, r) =
                                    self.add_custom_buckets_histogram_data_points(ctx, dps, settings, temporality, &app_opts);
                                annots.merge(ws);
                                r
                            } else {
                                self.add_histogram_data_points(ctx, dps, settings, &app_opts)
                            }
                        }
                        MetricType::ExponentialHistogram => {
                            let (ws, r) = self.add_exponential_histogram_data_points(
                                ctx,
                                &metric.exponential_histogram().data_points,
                                settings,
                                temporality,
                                &app_opts,
                            );
                            annots.merge(ws);
                            r
                        }
                        MetricType::Summary => {
                            self.add_summary_data_points(ctx, &metric.summary().data_points, settings, &app_opts)
                        }
                        MetricType::Empty => Err(Error::msg("unsupported metric type")),
                    };
                    if let Err(e) = res {
                        let canceled =
                            e.is(&storage::ERR_CANCELED) || e.is(&storage::ERR_DEADLINE_EXCEEDED);
                        errs.push(e);
                        if canceled {
                            return errs;
                        }
                    }
                }
            }
            if earliest_timestamp < u64::MAX {
                // At least one sample for this resource: add its target_info series.
                if let Err(e) =
                    self.add_resource_target_info(resource, settings, earliest_timestamp, latest_timestamp)
                {
                    errs.push(e);
                }
            }
        }
        errs
    }

    /// Go `setResourceContext`: computes the labels shared by all data points of a resource. On error the
    /// resource and scope caches are cleared.
    pub fn set_resource_context(&mut self, resource: &Resource, settings: &Settings) -> Result<(), Error> {
        let attrs = &resource.attributes;
        let mut rl = CachedResourceLabels { external_labels: settings.external_labels.clone(), ..Default::default() };
        self.label_namer = LabelNamer {
            utf8_allowed: settings.allow_utf8,
            underscore_label_sanitization: settings.label_name_underscore_sanitization,
            preserve_multiple_underscores: settings.label_name_preserve_multiple_underscores,
        };
        if let Some(service_name) = attrs.get(SERVICE_NAME_KEY) {
            let mut val = service_name.as_string();
            if let Some(ns) = attrs.get(SERVICE_NAMESPACE_KEY) {
                val = format!("{}/{}", ns.as_string(), val);
            }
            rl.job_label = val;
        }
        if let Some(instance) = attrs.get(SERVICE_INSTANCE_ID_KEY) {
            rl.instance_label = instance.as_string();
        }
        self.resource_labels = Some(rl);
        if let Some(p) = &settings.promote_resource_attributes {
            self.builder.reset(Labels::default());
            if let Err(e) =
                add_promoted_attributes(p, &mut self.builder, attrs, &mut self.sanitized_labels, &self.label_namer)
            {
                self.clear_resource_context();
                return Err(e);
            }
            let promoted = self.builder.labels();
            if let Some(rl) = &mut self.resource_labels {
                rl.promoted_labels = promoted;
            }
        }
        Ok(())
    }

    /// Go `setScopeContext`: computes the scope labels when scope promotion is on. On error the scope cache is
    /// cleared.
    pub fn set_scope_context(&mut self, scope: &Scope, settings: &Settings) -> Result<(), Error> {
        if !settings.promote_scope_metadata || scope.name.is_empty() {
            self.scope_labels = None;
            return Ok(());
        }
        let mut sl = CachedScopeLabels {
            scope_name: scope.name.clone(),
            scope_version: scope.version.clone(),
            scope_schema_url: scope.schema_url.clone(),
            scope_attrs: Labels::default(),
        };
        self.builder.reset(Labels::default());
        for (k, v) in scope.attributes.iter() {
            let name = match build_label_name(&mut self.sanitized_labels, &self.label_namer, &format!("otel_scope_{k}")) {
                Ok(n) => n,
                Err(e) => {
                    self.scope_labels = None;
                    return Err(e);
                }
            };
            self.builder.set(&name, &v.as_string());
        }
        sl.scope_attrs = self.builder.labels();
        self.scope_labels = Some(sl);
        Ok(())
    }

    /// Go `clearResourceContext`.
    pub fn clear_resource_context(&mut self) {
        self.resource_labels = None;
        self.scope_labels = None;
    }
}

/// Go `TranslatorMetricFromOtelMetric`.
pub fn translator_metric_from_otel_metric(metric: &pdata::Metric) -> namer::Metric {
    let typ = match metric.typ() {
        MetricType::Gauge => namer::MetricType::Gauge,
        MetricType::Sum => {
            if metric.sum().is_monotonic {
                namer::MetricType::MonotonicCounter
            } else {
                namer::MetricType::NonMonotonicCounter
            }
        }
        MetricType::Summary => namer::MetricType::Summary,
        MetricType::Histogram => namer::MetricType::Histogram,
        MetricType::ExponentialHistogram => namer::MetricType::ExponentialHistogram,
        MetricType::Empty => namer::MetricType::Unknown,
    };
    namer::Metric { name: metric.name.clone(), unit: metric.unit.clone(), typ }
}

/// Go `aggregationTemporality`: the temporality and whether the metric type has one.
fn aggregation_temporality(metric: &pdata::Metric) -> Result<(AggregationTemporality, bool), Error> {
    match &metric.data {
        pdata::MetricData::Gauge(_) | pdata::MetricData::Summary(_) => {
            Ok((AggregationTemporality::UNSPECIFIED, false))
        }
        pdata::MetricData::Sum(s) => Ok((s.aggregation_temporality, true)),
        pdata::MetricData::Histogram(h) => Ok((h.aggregation_temporality, true)),
        pdata::MetricData::ExponentialHistogram(h) => Ok((h.aggregation_temporality, true)),
        pdata::MetricData::Empty => Err(Error::msg(format!(
            "could not get aggregation temporality for {} as it has unsupported metric type {}",
            metric.name,
            metric.typ()
        ))),
    }
}

/// Go `NewPromoteResourceAttributes`.
pub fn new_promote_resource_attributes(otlp_cfg: &config::OtlpConfig) -> PromoteResourceAttributes {
    let attrs = if otlp_cfg.promote_all_resource_attributes {
        &otlp_cfg.ignore_resource_attributes
    } else {
        &otlp_cfg.promote_resource_attributes
    };
    PromoteResourceAttributes {
        promote_all: otlp_cfg.promote_all_resource_attributes,
        attrs: attrs.iter().cloned().collect(),
    }
}

/// Go `addPromotedAttributes`.
fn add_promoted_attributes(
    s: &PromoteResourceAttributes,
    builder: &mut Builder,
    resource_attributes: &Map,
    cache: &mut Option<HashMap<String, String>>,
    namer: &LabelNamer,
) -> Result<(), Error> {
    for (name, value) in resource_attributes.iter() {
        let exists = s.attrs.contains(name);
        if exists != s.promote_all {
            let normalized = build_label_name(cache, namer, name)?;
            builder.set(&normalized, &value.as_string());
        }
    }
    Ok(())
}

/// Go `findMinAndMaxTimestamps`.
pub fn find_min_and_max_timestamps(metric: &pdata::Metric, mut min_ts: Timestamp, mut max_ts: Timestamp) -> (Timestamp, Timestamp) {
    let mut f = |ts: Timestamp| {
        min_ts = min_ts.min(ts);
        max_ts = max_ts.max(ts);
    };
    match &metric.data {
        pdata::MetricData::Gauge(g) => g.data_points.iter().for_each(|p| f(p.timestamp)),
        pdata::MetricData::Sum(s) => s.data_points.iter().for_each(|p| f(p.timestamp)),
        pdata::MetricData::Histogram(h) => h.data_points.iter().for_each(|p| f(p.timestamp)),
        pdata::MetricData::ExponentialHistogram(h) => h.data_points.iter().for_each(|p| f(p.timestamp)),
        pdata::MetricData::Summary(s) => s.data_points.iter().for_each(|p| f(p.timestamp)),
        pdata::MetricData::Empty => {}
    }
    (min_ts, max_ts)
}
