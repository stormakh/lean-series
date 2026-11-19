//! Go `text/template` and `html/template` engine, and Prometheus `template/` (alert and console templates).
//!
//! # Layers
//!
//! - [`value`]: dynamic Go values ([`Value`], [`Type`], [`Func`]): what Go's `reflect` gives templates.
//! - [`gofmt`]: Go `fmt` (`Sprint`, `Sprintf`, `Sprintln`, `%v` of every value) and `strconv` float formatting.
//! - [`parse`]: Go `text/template/parse` (lexer, parse tree, parser) with Go's error texts.
//! - [`text`]: Go `text/template` (template sets, `missingkey` options, builtins, executor).
//! - [`html`]: Go `html/template` contextual autoescaping (HTML, attributes, JS, CSS, URLs, srcset).
//! - [`prom`]: Prometheus `template/template.go`: the template functions (`query`, `humanize`, ...),
//!   [`Expander`] and [`alert_template_data`].
//!
//! # Use from rules
//!
//! ```ignore
//! let data = template::alert_template_data(labels, external_labels, external_url, &sample);
//! let defs = "{{$labels := .Labels}}{{$externalLabels := .ExternalLabels}}{{$externalURL := .ExternalURL}}{{$value := .Value}}";
//! let exp = template::Expander::new(ctx, &format!("{defs}{text}"), &name, data, ts_ms, Some(query_fn), Some(&url), None);
//! let out = exp.expand()?; // Err holds Go's error text
//! ```
//!
//! Differences from Go are listed in the crate README section of the port report; the main ones: times are
//! UTC only (`Local` returns UTC), `sortByLabel` returns a sorted copy (Go sorts in place), and `title` uses a
//! simplified word segmentation.

pub mod gofmt;
pub mod gotime;
pub mod html;
pub mod methods;
pub mod parse;
pub mod prom;
mod stack;
pub mod text;
pub mod value;

pub use prom::{
    Expander, QueryFunc, alert_template_data, alert_template_data_type, float_to_time, new_sample,
    query_result_type, sample_type, template_function_names, text_expansion_failures_total, text_expansions_total,
};
pub use text::{FuncMap, MissingKey};
pub use value::{Func, IntKind, Kind, Signature, StrKind, StructType, Type, UintKind, Value};
pub use yaml::url::GoUrl;
