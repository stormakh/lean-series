//! Port of `helper.go`: labels from attributes, classic histograms, summaries, exemplars and target_info.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::time::Duration;

use model::exemplar::{self, Exemplar};
use model::histogram::Histogram;
use model::labels::{Labels, METRIC_NAME};
use model::metadata::{Metadata, MetricType};
use model::schema::{METRIC_TYPE_LABEL, METRIC_UNIT_LABEL};
use model::value::stale_nan;
use storage::{AppendV2Options, Context, Error};

use super::super::namer::{UnitNamer, go_quote};
use super::super::pdata::{
    self, HistogramDataPoint, Map, NumberValue, Resource, SummaryDataPoint, Timestamp, id_is_empty,
    timestamp_unix_milli,
};
use super::annotations::{WarningCategory, new_categorized_warning};
use super::metrics_to_prw::{AppOpts, CollisionAttrSource, PrometheusConverter, Settings, TargetInfoKey, build_label_name};
use super::{SERVICE_INSTANCE_ID_KEY, SERVICE_NAME_KEY, SERVICE_NAMESPACE_KEY};

pub const SUM_STR: &str = "_sum";
pub const COUNT_STR: &str = "_count";
pub const BUCKET_STR: &str = "_bucket";
pub const LE_STR: &str = "le";
pub const QUANTILE_STR: &str = "quantile";
pub const P_INF_STR: &str = "+Inf";
/// The maximum number of UTF-8 characters of exemplar labels (OpenMetrics).
pub const MAX_EXEMPLAR_RUNES: usize = 128;
pub const TRACE_ID_KEY: &str = "trace_id";
pub const SPAN_ID_KEY: &str = "span_id";
pub const TARGET_METRIC_NAME: &str = "target_info";
pub const DEFAULT_LOOKBACK_DELTA: Duration = Duration::from_secs(5 * 60);

/// Go `reservedLabelNames`: attribute names dropped because the converter sets them itself.
pub const RESERVED_LABEL_NAMES: &[&str] = &[METRIC_NAME];

/// Go `convertTimeStamp`: OTLP nanoseconds to milliseconds.
pub fn convert_time_stamp(ts: Timestamp) -> i64 {
    (ts as i64) / 1_000_000
}

/// Go's `%f` of a float64.
pub(super) fn go_f6(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f == f64::INFINITY {
        "+Inf".to_string()
    } else if f == f64::NEG_INFINITY {
        "-Inf".to_string()
    } else {
        format!("{f:.6}")
    }
}

/// Go `strconv.FormatFloat(f, 'f', -1, 64)`.
pub(super) fn format_float_f(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f == f64::INFINITY {
        "+Inf".to_string()
    } else if f == f64::NEG_INFINITY {
        "-Inf".to_string()
    } else {
        // Rust's Display gives the shortest round-trip digits without an exponent, as 'f' with -1 does.
        format!("{f}")
    }
}

fn hex_lower(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

/// Go `isOverriddenByTraceContext`.
fn is_overridden_by_trace_context(key: &str, has_trace_id: bool, has_span_id: bool) -> bool {
    (has_trace_id && key == TRACE_ID_KEY) || (has_span_id && key == SPAN_ID_KEY)
}

impl PrometheusConverter {
    /// Appends one sample with the per-metric options.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn append(
        &mut self,
        ls: &Labels,
        st: i64,
        t: i64,
        v: f64,
        h: Option<&Histogram>,
        app_opts: &AppOpts,
        exemplars: &[Exemplar],
    ) -> Result<(), Error> {
        let opts = AppendV2Options {
            metric_family_name: &app_opts.metric_family_name,
            metadata: Some(&app_opts.metadata),
            exemplars,
            reject_out_of_order: false,
        };
        let app = self.appender.as_mut().expect("PrometheusConverter used without an appender");
        app.append(0, ls, st, t, v, h, None, &opts).map(|_| ())
    }

    /// Go `createAttributes`: the labels of a data point from its attributes, the cached resource and scope
    /// labels, type and unit labels, and `extras` name/value pairs (an unpaired last value is ignored). Label
    /// names are sanitized; attributes that collide after sanitization are joined with `;` and recorded as a
    /// warning. Needs the resource context to be set first.
    #[allow(clippy::too_many_arguments)]
    pub fn create_attributes(
        &mut self,
        attributes: &Map,
        settings: &Settings,
        ignore_attrs: &[&str],
        log_on_overwrite: bool,
        meta: &Metadata,
        extras: &[&str],
    ) -> Result<Labels, Error> {
        if self.resource_labels.is_none() {
            return Err(Error::msg("createAttributes called without initializing resource context"));
        }
        // Sort the attributes by key, so that keys colliding after sanitization merge in a stable order.
        self.scratch_builder.reset();
        for (key, value) in attributes.iter() {
            if !ignore_attrs.contains(&key) {
                self.scratch_builder.add(key, &value.as_string());
            }
        }
        self.scratch_builder.sort();
        let sorted_labels = self.scratch_builder.labels();

        if settings.allow_utf8 {
            // No conflicts are possible.
            self.builder.reset(sorted_labels);
        } else {
            self.builder.reset(Labels::default());
            for (name, value) in sorted_labels.iter() {
                let final_key = build_label_name(&mut self.sanitized_labels, &self.label_namer, name)?;
                let existing = self.builder.get(&final_key);
                if !existing.is_empty() {
                    let joined = format!("{existing};{value}");
                    self.record_label_collision(&sorted_labels, &final_key);
                    self.builder.set(&final_key, &joined);
                } else {
                    self.builder.set(&final_key, value);
                }
            }
        }

        let rl = self.resource_labels.as_ref().unwrap();
        if settings.promote_resource_attributes.is_some() {
            for (name, value) in rl.promoted_labels.iter() {
                if self.builder.get(name).is_empty() {
                    self.builder.set(name, value);
                }
            }
        }
        if !rl.job_label.is_empty() {
            self.builder.set("job", &rl.job_label);
        }
        if !rl.instance_label.is_empty() {
            self.builder.set("instance", &rl.instance_label);
        }
        for (key, value) in &rl.external_labels {
            if self.builder.get(key).is_empty() {
                self.builder.set(key, value);
            }
        }

        if let Some(sl) = &self.scope_labels {
            for (name, value) in sl.scope_attrs.iter() {
                self.builder.set(name, value);
            }
            self.builder.set("otel_scope_name", &sl.scope_name);
            self.builder.set("otel_scope_version", &sl.scope_version);
            self.builder.set("otel_scope_schema_url", &sl.scope_schema_url);
        }

        if settings.enable_type_and_unit_labels {
            let unit_namer = UnitNamer { utf8_allowed: settings.allow_utf8 };
            if meta.typ != MetricType::Unknown {
                self.builder.set(METRIC_TYPE_LABEL, &meta.typ.as_str().to_lowercase());
            }
            if !meta.unit.is_empty() {
                self.builder.set(METRIC_UNIT_LABEL, &unit_namer.build(&meta.unit));
            }
        }

        let mut i = 0;
        while i + 1 < extras.len() {
            let mut name = extras[i].to_string();
            if log_on_overwrite && !self.builder.get(&name).is_empty() {
                eprintln!("label {name} is overwritten. Check if Prometheus reserved labels are used.");
            }
            // Internal labels are kept as they are.
            if name.len() <= 4 || !name.starts_with("__") || !name.ends_with("__") {
                name = build_label_name(&mut self.sanitized_labels, &self.label_namer, &name)?;
            }
            self.builder.set(&name, extras[i + 1]);
            i += 2;
        }
        Ok(self.builder.labels())
    }

    /// Go `recordLabelCollision`: one warning per sanitized label name and `from_metrics` call, naming the
    /// attributes that map to `final_key`.
    fn record_label_collision(&mut self, sorted_labels: &Labels, final_key: &str) {
        if self.recorded_collisions.as_ref().is_some_and(|r| r.contains(final_key)) {
            return;
        }
        let mut keys = String::new();
        for (name, _) in sorted_labels.iter() {
            match build_label_name(&mut self.sanitized_labels, &self.label_namer, name) {
                Ok(n) if n == final_key => {}
                _ => continue,
            }
            if !keys.is_empty() {
                keys.push_str(", ");
            }
            keys.push_str(&go_quote(name));
        }
        self.collision_annots.add(new_categorized_warning(
            WarningCategory::LabelNameCollision,
            format!(
                "OTLP {} attributes {} collide as label {} after name sanitization, values are concatenated with ';'",
                self.collision_source.as_str(),
                keys,
                go_quote(final_key)
            ),
        ));
        self.recorded_collisions.get_or_insert_with(|| HashSet::with_capacity(4)).insert(final_key.to_string());
    }

    /// Go `addHistogramDataPoints`: classic histogram series (`_sum`, `_count`, `_bucket`) for OTLP explicit
    /// bucket histograms.
    pub fn add_histogram_data_points(
        &mut self,
        ctx: &Context,
        data_points: &[HistogramDataPoint],
        settings: &Settings,
        app_opts: &AppOpts,
    ) -> Result<(), Error> {
        let count_metric_name = format!("{}{}", app_opts.metric_family_name, COUNT_STR);
        let bucket_metric_name = format!("{}{}", app_opts.metric_family_name, BUCKET_STR);
        let sum_metric_name = format!("{}{}", app_opts.metric_family_name, SUM_STR);
        let mut bucket_exemplars: Vec<Exemplar> = Vec::new();
        for pt in data_points {
            self.every_n.check_context(ctx)?;
            let no_value = pt.flags.no_recorded_value();
            let timestamp = convert_time_stamp(pt.timestamp);
            let start_timestamp = convert_time_stamp(pt.start_timestamp);
            let base_labels =
                self.create_attributes(&pt.attributes, settings, RESERVED_LABEL_NAMES, false, &app_opts.metadata, &[])?;

            // An unset sum means that the _sum series is left out. The exemplars of the previous data point
            // must not leak into _sum and _count.
            if let Some(sum) = pt.sum {
                let val = if no_value { stale_nan() } else { sum };
                let sum_labels = self.add_labels(&sum_metric_name, &base_labels, &[]);
                self.append(&sum_labels, start_timestamp, timestamp, val, None, app_opts, &[])?;
            }

            let val = if no_value { stale_nan() } else { pt.count as f64 };
            let count_labels = self.add_labels(&count_metric_name, &base_labels, &[]);
            self.append(&count_labels, start_timestamp, timestamp, val, None, app_opts, &[])?;

            let mut exemplars = self.get_prom_exemplars(ctx, &pt.exemplars)?;
            let mut next_exemplar_idx = 0;
            let mut cumulative_count: u64 = 0;

            // Buckets: the explicit bounds plus one.
            let n = pt.explicit_bounds.len().min(pt.bucket_counts.len());
            for i in 0..n {
                self.every_n.check_context(ctx)?;
                let bound = pt.explicit_bounds[i];
                cumulative_count = cumulative_count.wrapping_add(pt.bucket_counts[i]);

                // The le="+Inf" series comes from count below; writing this bound would write it twice.
                if bound == f64::INFINITY {
                    self.plus_inf_annots.add(new_categorized_warning(
                        WarningCategory::HistogramPlusInfBound,
                        "histogram data point has an explicit +Inf bound, le=\"+Inf\" is taken from count".to_string(),
                    ));
                    continue;
                }

                // The exemplars of this bucket. Exemplars and buckets are both in ascending order.
                bucket_exemplars.clear();
                while next_exemplar_idx < exemplars.len() {
                    let ex = &exemplars[next_exemplar_idx];
                    if ex.value > bound {
                        break;
                    }
                    bucket_exemplars.push(ex.clone());
                    next_exemplar_idx += 1;
                }
                // Appenders need exemplars sorted by timestamp.
                bucket_exemplars.sort_by(|a, b| exemplar::compare(a, b).cmp(&0));
                let val = if no_value { stale_nan() } else { cumulative_count as f64 };
                let bound_str = format_float_f(bound);
                let bucket_labels = self.add_labels(&bucket_metric_name, &base_labels, &[LE_STR, &bound_str]);
                let ex = std::mem::take(&mut bucket_exemplars);
                let r = self.append(&bucket_labels, start_timestamp, timestamp, val, None, app_opts, &ex);
                bucket_exemplars = ex;
                r?;
            }

            let rest = &mut exemplars[next_exemplar_idx..];
            rest.sort_by(|a, b| exemplar::compare(a, b).cmp(&0));
            let val = if no_value { stale_nan() } else { pt.count as f64 };
            let inf_labels = self.add_labels(&bucket_metric_name, &base_labels, &[LE_STR, P_INF_STR]);
            self.append(&inf_labels, start_timestamp, timestamp, val, None, app_opts, &exemplars[next_exemplar_idx..])?;
        }
        Ok(())
    }

    /// Go `getPromExemplars`: Prometheus exemplars with `trace_id`, `span_id` and, when they fit in 128 runes,
    /// the filtered attributes as labels.
    pub fn get_prom_exemplars(&mut self, ctx: &Context, exemplars: &[pdata::Exemplar]) -> Result<Vec<Exemplar>, Error> {
        let mut out = Vec::with_capacity(exemplars.len());
        for ex in exemplars {
            self.every_n.check_context(ctx)?;
            let mut exemplar_runes = 0usize;
            let ts = timestamp_unix_milli(ex.timestamp);
            let value = match ex.value {
                NumberValue::Int(v) => v as f64,
                NumberValue::Double(v) => v,
                NumberValue::Empty => {
                    return Err(Error::msg(format!("unsupported exemplar value type: {}", ex.value_type())));
                }
            };
            self.scratch_builder.reset();
            let mut has_trace_id = false;
            let mut has_span_id = false;
            if !id_is_empty(&ex.trace_id) {
                has_trace_id = true;
                let val = hex_lower(&ex.trace_id);
                exemplar_runes += TRACE_ID_KEY.chars().count() + val.chars().count();
                self.scratch_builder.add(TRACE_ID_KEY, &val);
            }
            if !id_is_empty(&ex.span_id) {
                has_span_id = true;
                let val = hex_lower(&ex.span_id);
                exemplar_runes += SPAN_ID_KEY.chars().count() + val.chars().count();
                self.scratch_builder.add(SPAN_ID_KEY, &val);
            }
            let attrs = &ex.filtered_attributes;
            let mut values = Vec::with_capacity(attrs.len());
            for (key, value) in attrs.iter() {
                if is_overridden_by_trace_context(key, has_trace_id, has_span_id) {
                    continue;
                }
                let v = value.as_string();
                exemplar_runes += key.chars().count() + v.chars().count();
                values.push((key, v));
            }
            // Add the filtered attributes only if the labels stay within the limit.
            if exemplar_runes <= MAX_EXEMPLAR_RUNES {
                for (key, v) in &values {
                    self.scratch_builder.add(key, v);
                }
            }
            self.scratch_builder.sort();
            out.push(Exemplar { labels: self.scratch_builder.labels(), value, ts, has_ts: ts != 0 });
        }
        Ok(out)
    }

    /// Go `addSummaryDataPoints`: `_sum`, `_count` and one series per quantile.
    pub fn add_summary_data_points(
        &mut self,
        ctx: &Context,
        data_points: &[SummaryDataPoint],
        settings: &Settings,
        app_opts: &AppOpts,
    ) -> Result<(), Error> {
        let sum_name = format!("{}{}", app_opts.metric_family_name, SUM_STR);
        let count_name = format!("{}{}", app_opts.metric_family_name, COUNT_STR);
        for pt in data_points {
            self.every_n.check_context(ctx)?;
            let no_value = pt.flags.no_recorded_value();
            let timestamp = convert_time_stamp(pt.timestamp);
            let start_timestamp = convert_time_stamp(pt.start_timestamp);
            let base_labels =
                self.create_attributes(&pt.attributes, settings, RESERVED_LABEL_NAMES, false, &app_opts.metadata, &[])?;

            let val = if no_value { stale_nan() } else { pt.sum };
            let sum_labels = self.add_labels(&sum_name, &base_labels, &[]);
            self.append(&sum_labels, start_timestamp, timestamp, val, None, app_opts, &[])?;

            let val = if no_value { stale_nan() } else { pt.count as f64 };
            let count_labels = self.add_labels(&count_name, &base_labels, &[]);
            self.append(&count_labels, start_timestamp, timestamp, val, None, app_opts, &[])?;

            for qt in &pt.quantile_values {
                let val = if no_value { stale_nan() } else { qt.value };
                let percentile_str = format_float_f(qt.quantile);
                let qt_labels =
                    self.add_labels(&app_opts.metric_family_name, &base_labels, &[QUANTILE_STR, &percentile_str]);
                self.append(&qt_labels, start_timestamp, timestamp, val, None, app_opts, &[])?;
            }
        }
        Ok(())
    }

    /// Go `addLabels`: `base_labels` with `__name__=name` and the `extras` pairs (an unpaired last value is
    /// ignored).
    pub fn add_labels(&mut self, name: &str, base_labels: &Labels, extras: &[&str]) -> Labels {
        self.builder.reset(base_labels.clone());
        let n = extras.len() - extras.len() % 2;
        let mut i = 0;
        while i < n {
            self.builder.set(extras[i], extras[i + 1]);
            i += 2;
        }
        self.builder.set(METRIC_NAME, name);
        self.builder.labels()
    }

    /// Go `addResourceTargetInfo`: the `target_info` series of a resource, with samples from
    /// `earliest_timestamp` to `latest_timestamp`, one every lookback delta / 2.
    pub fn add_resource_target_info(
        &mut self,
        resource: &Resource,
        settings: &Settings,
        earliest_timestamp: Timestamp,
        latest_timestamp: Timestamp,
    ) -> Result<(), Error> {
        if settings.disable_target_info {
            return Ok(());
        }
        let attributes = &resource.attributes;
        let mut identifying_attrs: &[&str] = &[SERVICE_NAMESPACE_KEY, SERVICE_NAME_KEY, SERVICE_INSTANCE_ID_KEY];
        let mut non_identifying = attributes.len();
        for a in identifying_attrs {
            if attributes.get(a).is_some() {
                non_identifying -= 1;
            }
        }
        if non_identifying == 0 {
            // Only job and instance: target_info would add nothing.
            return Ok(());
        }
        let mut name = TARGET_METRIC_NAME.to_string();
        if !settings.namespace.is_empty() {
            name = format!("{}_{}", settings.namespace, name);
        }
        let mut settings = settings.clone();
        settings.promote_resource_attributes = None;
        if settings.keep_identifying_resource_attributes {
            // Do not drop the identifying attributes.
            identifying_attrs = &[];
        }
        let app_opts = AppOpts {
            metadata: Metadata { typ: MetricType::Gauge, help: "Target metadata".to_string(), ..Default::default() },
            metric_family_name: name.clone(),
        };
        // target_info is a resource-level metric: no scope labels.
        let saved_scope_labels = self.scope_labels.take();
        self.collision_source = CollisionAttrSource::Resource;
        let res = self.create_attributes(
            attributes,
            &settings,
            identifying_attrs,
            false,
            &Metadata::default(),
            &[METRIC_NAME, &name],
        );
        self.collision_source = CollisionAttrSource::DataPoint;
        self.scope_labels = saved_scope_labels;
        let lbls = res?;
        let have_identifier = lbls.iter().any(|(n, _)| n == "job" || n == "instance");
        if !have_identifier {
            // target_info needs at least one identifying label.
            return Ok(());
        }

        if settings.lookback_delta.is_zero() {
            settings.lookback_delta = DEFAULT_LOOKBACK_DELTA;
        }
        let interval = (settings.lookback_delta / 2).as_nanos() as i128;
        // Samples with the same labels and timestamp across resources of the batch are written once.
        let labels_hash = lbls.hash();
        let latest = latest_timestamp as i64 as i128;
        let mut t = earliest_timestamp as i64 as i128;
        while t < latest {
            let ms = t.div_euclid(1_000_000) as i64;
            let key = TargetInfoKey { labels_hash, timestamp: ms };
            if self.seen_target_info.insert(key) {
                self.append(&lbls, 0, ms, 1.0, None, &app_opts, &[])?;
            }
            if interval == 0 {
                break;
            }
            t += interval;
        }
        // The final sample at the latest timestamp.
        let final_ms = latest.div_euclid(1_000_000) as i64;
        let key = TargetInfoKey { labels_hash, timestamp: final_ms };
        if !self.seen_target_info.insert(key) {
            return Ok(());
        }
        self.append(&lbls, 0, final_ms, 1.0, None, &app_opts, &[])
    }
}
