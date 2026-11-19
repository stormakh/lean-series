//! The bridge to the `template` crate: alert template expansion (Go `template.NewTemplateExpander` with
//! `AlertTemplateData`) and the parse check of rulefmt.

use std::collections::HashMap;
use std::sync::Arc;

use promql::Sample;
use storage::Context;
use yaml::url::GoUrl;

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
    external_url: Option<&GoUrl>,
) -> Result<String, (String, String)> {
    let data = template::alert_template_data(
        labels.iter().map(|(k, v)| (k.clone(), v.as_str())),
        external_labels.iter().map(|(k, v)| (k.clone(), v.as_str())),
        external_url_label,
        smpl,
    );
    let q = query.clone();
    // Go converts the rules QueryFunc to template.QueryFunc (same signature, time.Time at millisecond precision).
    let tq: template::QueryFunc = Arc::new(move |ctx: &Context, qs: &str, ts_ms: i64| q(ctx, qs, Time::from_millis(ts_ms)));
    let exp = template::Expander::new(ctx.clone(), text, name, data.clone(), ts.millis(), Some(tq), external_url, None);
    exp.expand().map_err(|e| (e, template::gofmt::sprint(&[data])))
}

/// Go `NewTemplateExpander(...).ParseTest()` with the empty alert template data: parses `text` as the template
/// named `name` with Prometheus' template functions.
pub(crate) fn parse_test(text: &str, name: &str) -> Result<(), String> {
    let empty: [(String, &str); 0] = [];
    let data = template::alert_template_data(empty.clone(), empty, "", &Sample::default());
    let exp = template::Expander::new(Context::background(), text, name, data, Time::now().millis(), None, None, None);
    exp.parse_test()
}
