//! OTLP support: a minimal OTLP metrics data model with its protobuf and JSON codecs (`pdata`), the
//! `github.com/prometheus/otlptranslator` naming library (`namer`), the OTLP to Prometheus converter
//! (`prometheusremotewrite`) and the delta to cumulative processor the OTLP write handler can use.

pub mod deltatocumulative;
pub mod namer;
pub mod pdata;
pub mod pdata_json;
pub mod pdata_proto;
pub mod prometheusremotewrite;
