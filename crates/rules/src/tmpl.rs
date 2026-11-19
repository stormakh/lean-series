//! The bridge to the `template` crate: alert template expansion (Go `template.NewTemplateExpander` with
//! `AlertTemplateData`) and the parse check of rulefmt.

use std::collections::HashMap;

use promql::Sample;
use storage::Context;

use crate::rule::{QueryFunc, Time};

/// Expands an alert template. On error returns the error text and the template data as Go logs it.
#[allow(clippy::too_many_arguments)]
pub(crate) fn expand_alert(
    ctx: &Context,
    text: &str,
    name: &str,
    labels: &HashMap<String, String>,
    external_labels: &HashMap<String, String>,
    external_url_label: &str,
    smpl: &Sample,
    ts: Time,
    query: &QueryFunc,
    external_url: Option<&str>,
) -> Result<String, (String, String)> {
    let _ = (ctx, name, labels, external_labels, external_url_label, smpl, ts, query, external_url);
    Ok(text.to_string()) // TODO(w3-rules): template crate
}

/// Parses `text` as the template named `name` with Prometheus' template functions.
pub(crate) fn parse_test(text: &str, name: &str) -> Result<(), String> {
    let _ = (text, name);
    Ok(()) // TODO(w3-rules): template crate
}
