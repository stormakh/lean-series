//! Port of `storage/remote/otlptranslator/prometheusremotewrite`: converts OTLP metrics to Prometheus samples
//! appended to a `storage::AppenderV2`.

pub mod annotations;
pub mod context;
pub mod helper;
pub mod histograms;
pub mod metadata;
pub mod metrics_to_prw;
pub mod number_data_points;

pub use annotations::*;
pub use context::EveryNTimes;
pub use helper::*;
pub use histograms::*;
pub use metadata::otel_metric_type_to_prom_metric_type;
pub use metrics_to_prw::*;

/// OTel semantic conventions v1.26.0 resource attribute keys.
pub const SERVICE_NAME_KEY: &str = "service.name";
pub const SERVICE_NAMESPACE_KEY: &str = "service.namespace";
pub const SERVICE_INSTANCE_ID_KEY: &str = "service.instance.id";
