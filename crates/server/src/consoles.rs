//! Console templates (Go `web.go` consoles handler expansion).

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use storage::{Context, Queryable};
use template::{Expander, StrKind, StructType, Type, Value};
use web::web::{ConsoleExpander, ConsoleRequest};

/// Go's convenience definitions that the handler prepends to every console template.
const DEFS: &str = "{{$rawParams := .RawParams }}{{$params := .Params}}{{$path := .Path}}{{$externalLabels := .ExternalLabels}}";

fn string_t() -> Type {
    Type::String(StrKind::String)
}

/// Go `url.Values` (`map[string][]string`).
fn url_values_t() -> Type {
    Type::Map(Arc::new(Type::slice_of(string_t())), Some("url.Values".into()))
}

/// The anonymous struct type of the console data.
fn console_data_type() -> Arc<StructType> {
    static T: OnceLock<Arc<StructType>> = OnceLock::new();
    T.get_or_init(|| {
        Arc::new(StructType {
            name: "struct { RawParams url.Values; Params map[string]string; Path string; ExternalLabels map[string]string }"
                .to_string(),
            fields: vec![
                ("RawParams".to_string(), url_values_t()),
                ("Params".to_string(), Type::map_of(string_t())),
                ("Path".to_string(), string_t()),
                ("ExternalLabels".to_string(), Type::map_of(string_t())),
            ],
        })
    })
    .clone()
}

fn console_data(r: &ConsoleRequest<'_>) -> Value {
    let raw: BTreeMap<String, Value> = r
        .raw_params
        .iter()
        .map(|(k, vs)| (k.clone(), Value::slice(string_t(), vs.iter().map(|v| Value::string(v.as_str())).collect())))
        .collect();
    let raw = Value::Map(Arc::new(template::value::MapValue { ty: url_values_t(), entries: raw }));
    Value::new_struct(
        console_data_type(),
        vec![
            raw,
            Value::string_map(r.params.iter().map(|(k, v)| (k.clone(), v.as_str()))),
            Value::string(r.path.as_str()),
            Value::string_map(r.external_labels.iter().map(|(k, v)| (k.clone(), v.as_str()))),
        ],
    )
}

/// Builds the console template expander of the web handler (Go `Handler.consoles` after reading the file):
/// `template.NewTemplateExpander` with the console data, the `query` function running instant queries through
/// `engine` against `queryable` (Go `rules.EngineQueryFunc`), and `external_url` (Go `Options.ExternalURL`;
/// an unparsable URL gives none). The query context carries the "httpRequest" origin of the console request,
/// so the query log line has the client IP, method and console path.
pub fn console_expander(engine: promql::Engine, queryable: Arc<dyn Queryable>, external_url: &str) -> ConsoleExpander {
    let external_url = yaml::url::parse(external_url).ok();
    let rules_query = rules::engine_query_func(Arc::new(engine), queryable);
    let query_func: template::QueryFunc =
        Arc::new(move |ctx: &Context, qs: &str, ts_ms: i64| rules_query(ctx, qs, rules::Time::from_millis(ts_ms)));
    Arc::new(move |r: &ConsoleRequest<'_>| {
        let ctx = web::httputil::context_from_request(&Context::background(), &r.client_ip, &r.method, &r.request_path);
        let exp = Expander::new(
            ctx,
            &format!("{}{}", DEFS, r.text),
            &r.name,
            console_data(r),
            r.now_ms,
            Some(query_func.clone()),
            external_url.as_ref(),
            None,
        );
        let files: Vec<String> = r.library_files.iter().map(|p| p.to_string_lossy().into_owned()).collect();
        exp.expand_html(&files)
    })
}
