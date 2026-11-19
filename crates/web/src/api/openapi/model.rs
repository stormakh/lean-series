//! The parts of the libopenapi high-level model the document uses, and the helpers of
//! `web/api/v1/openapi_helpers.go`. Each type writes its fields in libopenapi's struct order, which is the
//! order `Render` prints them in; empty strings and lists are left out (`omitempty`).

use std::borrow::Cow;

use model::labels::Labels;
use promql::{Sample, Series};

use super::emit::{Node, Str, go_map};

/// Go `base.Schema` behind a `base.SchemaProxy` (a reference when `reference` is set).
#[derive(Clone, Debug, Default)]
pub struct Schema {
    pub reference: Str,
    pub typ: &'static str,
    pub one_of: Vec<Schema>,
    pub any_of: Vec<Schema>,
    pub items: Option<Box<Schema>>,
    pub content_schema: Option<Box<Schema>>,
    pub properties: Vec<(Str, Schema)>,
    pub title: Str,
    pub minimum: Option<f64>,
    pub format: &'static str,
    pub max_items: Option<i64>,
    pub min_items: Option<i64>,
    pub required: Vec<&'static str>,
    pub enum_values: Vec<Node>,
    pub additional_properties: Option<AdditionalProperties>,
    pub description: Str,
    pub content_media_type: &'static str,
    pub default: Option<Node>,
    pub example: Option<Node>,
}

/// Go `base.DynamicValue[*base.SchemaProxy, bool]` of `additionalProperties`.
#[derive(Clone, Debug)]
pub enum AdditionalProperties {
    Bool(bool),
    Schema(Box<Schema>),
}

/// Builds a mapping, leaving out empty values.
#[derive(Default)]
pub struct MapBuilder(pub Vec<(Str, Node)>);

impl MapBuilder {
    pub fn put(&mut self, k: &'static str, v: Node) {
        self.0.push((Cow::Borrowed(k), v));
    }
    pub fn str(&mut self, k: &'static str, v: &Str) {
        if !v.is_empty() {
            self.put(k, Node::Str(v.clone()));
        }
    }
    pub fn node(self) -> Node {
        Node::Map(self.0)
    }
}

fn strs(v: &[&'static str]) -> Node {
    Node::Seq(v.iter().map(|s| Node::Str(Cow::Borrowed(*s))).collect())
}

impl Schema {
    pub fn to_node(&self) -> Node {
        let mut m = MapBuilder::default();
        if !self.reference.is_empty() {
            m.0.push((Cow::Borrowed("$ref"), Node::Str(self.reference.clone())));
            return m.node();
        }
        if !self.typ.is_empty() {
            m.put("type", Node::from(self.typ));
        }
        if !self.one_of.is_empty() {
            m.put(
                "oneOf",
                Node::Seq(self.one_of.iter().map(Schema::to_node).collect()),
            );
        }
        if !self.any_of.is_empty() {
            m.put(
                "anyOf",
                Node::Seq(self.any_of.iter().map(Schema::to_node).collect()),
            );
        }
        if let Some(i) = &self.items {
            m.put("items", i.to_node());
        }
        if let Some(c) = &self.content_schema {
            m.put("contentSchema", c.to_node());
        }
        if !self.properties.is_empty() {
            m.put(
                "properties",
                Node::Map(
                    self.properties
                        .iter()
                        .map(|(k, v)| (k.clone(), v.to_node()))
                        .collect(),
                ),
            );
        }
        m.str("title", &self.title);
        if let Some(v) = self.minimum {
            m.put("minimum", Node::Float(v));
        }
        if !self.format.is_empty() {
            m.put("format", Node::from(self.format));
        }
        if let Some(v) = self.max_items {
            m.put("maxItems", Node::Int(v));
        }
        if let Some(v) = self.min_items {
            m.put("minItems", Node::Int(v));
        }
        if !self.required.is_empty() {
            m.put("required", strs(&self.required));
        }
        if !self.enum_values.is_empty() {
            m.put("enum", Node::Seq(self.enum_values.clone()));
        }
        match &self.additional_properties {
            Some(AdditionalProperties::Bool(b)) => m.put("additionalProperties", Node::Bool(*b)),
            Some(AdditionalProperties::Schema(s)) => m.put("additionalProperties", s.to_node()),
            None => {}
        }
        m.str("description", &self.description);
        if !self.content_media_type.is_empty() {
            m.put("contentMediaType", Node::from(self.content_media_type));
        }
        if let Some(v) = &self.default {
            m.put("default", v.clone());
        }
        if let Some(v) = &self.example {
            m.put("example", v.clone());
        }
        m.node()
    }
}

/// Go `base.Example`.
#[derive(Clone, Debug)]
pub struct Example {
    pub summary: Str,
    pub description: Str,
    pub value: Node,
}

/// An ordered map of examples (Go `*orderedmap.Map[string, *base.Example]`).
pub type Examples = Vec<(Str, Example)>;

fn examples_node(e: &Examples) -> Node {
    Node::Map(
        e.iter()
            .map(|(k, ex)| {
                let mut m = MapBuilder::default();
                m.str("summary", &ex.summary);
                m.str("description", &ex.description);
                m.put("value", ex.value.clone());
                (k.clone(), m.node())
            })
            .collect(),
    )
}

/// Go `v3.MediaType`.
#[derive(Clone, Debug, Default)]
pub struct MediaType {
    pub schema: Option<Schema>,
    pub item_schema: Option<Schema>,
    pub examples: Option<Examples>,
}

impl MediaType {
    fn to_node(&self) -> Node {
        let mut m = MapBuilder::default();
        if let Some(s) = &self.schema {
            m.put("schema", s.to_node());
        }
        if let Some(s) = &self.item_schema {
            m.put("itemSchema", s.to_node());
        }
        if let Some(e) = &self.examples
            && !e.is_empty()
        {
            m.put("examples", examples_node(e));
        }
        m.node()
    }
}

fn content_node(c: &[(&'static str, MediaType)]) -> Node {
    Node::Map(
        c.iter()
            .map(|(k, v)| (Cow::Borrowed(*k), v.to_node()))
            .collect(),
    )
}

/// Go `v3.Parameter`.
#[derive(Clone, Debug)]
pub struct Parameter {
    pub name: &'static str,
    pub location: &'static str,
    pub description: &'static str,
    pub required: bool,
    pub explode: Option<bool>,
    pub schema: Schema,
    pub examples: Examples,
}

impl Parameter {
    fn to_node(&self) -> Node {
        let mut m = MapBuilder::default();
        m.put("name", Node::from(self.name));
        m.put("in", Node::from(self.location));
        m.put("description", Node::from(self.description));
        m.put("required", Node::Bool(self.required));
        if let Some(e) = self.explode {
            m.put("explode", Node::Bool(e));
        }
        m.put("schema", self.schema.to_node());
        if !self.examples.is_empty() {
            m.put("examples", examples_node(&self.examples));
        }
        m.node()
    }
}

/// Go `v3.RequestBody`.
#[derive(Clone, Debug)]
pub struct RequestBody {
    pub description: &'static str,
    pub content: Vec<(&'static str, MediaType)>,
    pub required: bool,
}

/// Go `v3.Response`.
#[derive(Clone, Debug)]
pub struct Response {
    pub description: &'static str,
    pub content: Vec<(&'static str, MediaType)>,
}

/// Go `v3.Responses` (its `Codes`).
pub type Responses = Vec<(&'static str, Response)>;

/// Go `v3.Operation`.
#[derive(Clone, Debug, Default)]
pub struct Operation {
    pub tags: Vec<&'static str>,
    pub summary: &'static str,
    pub description: &'static str,
    pub operation_id: &'static str,
    pub parameters: Vec<Parameter>,
    pub request_body: Option<RequestBody>,
    pub responses: Responses,
}

impl Operation {
    fn to_node(&self) -> Node {
        let mut m = MapBuilder::default();
        if !self.tags.is_empty() {
            m.put("tags", strs(&self.tags));
        }
        if !self.summary.is_empty() {
            m.put("summary", Node::from(self.summary));
        }
        if !self.description.is_empty() {
            m.put("description", Node::from(self.description));
        }
        m.put("operationId", Node::from(self.operation_id));
        if !self.parameters.is_empty() {
            m.put(
                "parameters",
                Node::Seq(self.parameters.iter().map(Parameter::to_node).collect()),
            );
        }
        if let Some(rb) = &self.request_body {
            let mut b = MapBuilder::default();
            b.put("description", Node::from(rb.description));
            b.put("content", content_node(&rb.content));
            b.put("required", Node::Bool(rb.required));
            m.put("requestBody", b.node());
        }
        let codes = self
            .responses
            .iter()
            .map(|(code, r)| {
                let mut b = MapBuilder::default();
                b.put("description", Node::from(r.description));
                if !r.content.is_empty() {
                    b.put("content", content_node(&r.content));
                }
                (Cow::Borrowed(*code), b.node())
            })
            .collect();
        m.put("responses", Node::Map(codes));
        m.node()
    }
}

/// Go `v3.PathItem`.
#[derive(Clone, Debug, Default)]
pub struct PathItem {
    pub get: Option<Operation>,
    pub put: Option<Operation>,
    pub post: Option<Operation>,
    pub delete: Option<Operation>,
    pub options: Option<Operation>,
}

impl PathItem {
    pub fn to_node(&self) -> Node {
        let mut m = MapBuilder::default();
        for (k, op) in [
            ("get", &self.get),
            ("put", &self.put),
            ("post", &self.post),
            ("delete", &self.delete),
            ("options", &self.options),
        ] {
            if let Some(op) = op {
                m.put(k, op.to_node());
            }
        }
        m.node()
    }
}

// ---------------------------------------------------------------------------------------------------------
// Port of openapi_helpers.go.

/// Go `exampleTime`: 2026-01-02T13:37:00Z as Unix seconds.
pub const EXAMPLE_TIME: i64 = 1767361020;

/// Go `example{name, value}`.
pub type ExampleArg = (&'static str, Node);

/// Go `exampleMap`.
pub fn example_map(exs: Vec<ExampleArg>) -> Examples {
    exs.into_iter()
        .map(|(name, value)| {
            (
                Cow::Borrowed(name),
                Example {
                    summary: Str::default(),
                    description: Str::default(),
                    value,
                },
            )
        })
        .collect()
}

/// An example with a summary (Go `&base.Example{Summary: ..., Value: ...}`).
pub fn ex(name: &'static str, summary: &'static str, value: Node) -> (Str, Example) {
    (
        Cow::Borrowed(name),
        Example {
            summary: Cow::Borrowed(summary),
            description: Str::default(),
            value,
        },
    )
}

/// Go `schemaRef`.
pub fn schema_ref(r: &str) -> Schema {
    Schema {
        reference: Cow::Owned(r.to_string()),
        ..Default::default()
    }
}

/// A reference to `#/components/schemas/<name>`.
pub fn component_ref(name: &str) -> Schema {
    schema_ref(&format!("#/components/schemas/{name}"))
}

/// Go `schemaFromType`.
pub fn schema_from_type(t: &'static str) -> Schema {
    Schema {
        typ: t,
        ..Default::default()
    }
}

pub fn string_schema() -> Schema {
    schema_from_type("string")
}

pub fn integer_schema() -> Schema {
    Schema {
        typ: "integer",
        format: "int64",
        ..Default::default()
    }
}

pub fn boolean_schema() -> Schema {
    schema_from_type("boolean")
}

pub fn string_schema_with_description(description: &'static str) -> Schema {
    Schema {
        typ: "string",
        description: Cow::Borrowed(description),
        ..Default::default()
    }
}

pub fn string_schema_with_description_and_example(
    description: &'static str,
    example: impl Into<Node>,
) -> Schema {
    Schema {
        typ: "string",
        description: Cow::Borrowed(description),
        example: Some(example.into()),
        ..Default::default()
    }
}

pub fn integer_schema_with_description(description: &'static str) -> Schema {
    Schema {
        typ: "integer",
        format: "int64",
        description: Cow::Borrowed(description),
        ..Default::default()
    }
}

pub fn integer_schema_with_description_and_example(
    description: &'static str,
    example: impl Into<Node>,
) -> Schema {
    Schema {
        typ: "integer",
        format: "int64",
        description: Cow::Borrowed(description),
        example: Some(example.into()),
        ..Default::default()
    }
}

pub fn integer_schema_with_default(default: i64) -> Schema {
    Schema {
        typ: "integer",
        format: "int64",
        minimum: Some(1.0),
        default: Some(Node::Int(default)),
        ..Default::default()
    }
}

pub fn integer_schema_with_description_default_and_example(
    description: &'static str,
    default: i64,
    example: impl Into<Node>,
) -> Schema {
    Schema {
        typ: "integer",
        format: "int64",
        description: Cow::Borrowed(description),
        minimum: Some(1.0),
        default: Some(Node::Int(default)),
        example: Some(example.into()),
        ..Default::default()
    }
}

pub fn boolean_schema_with_description(description: &'static str) -> Schema {
    Schema {
        typ: "boolean",
        description: Cow::Borrowed(description),
        ..Default::default()
    }
}

/// An array of `items` (Go `Type: []string{"array"}, Items: ...`).
pub fn array_of(items: Schema) -> Schema {
    Schema {
        typ: "array",
        items: Some(Box::new(items)),
        ..Default::default()
    }
}

pub fn string_array_schema_with_description(description: &'static str) -> Schema {
    Schema {
        description: Cow::Borrowed(description),
        ..array_of(string_schema())
    }
}

pub fn string_array_schema_with_description_and_example(
    description: &'static str,
    example: Node,
) -> Schema {
    Schema {
        description: Cow::Borrowed(description),
        example: Some(example),
        ..array_of(string_schema())
    }
}

/// Go `statusSchema`.
pub fn status_schema() -> Schema {
    Schema {
        typ: "string",
        enum_values: vec![
            Node::Plain(Cow::Borrowed("success")),
            Node::Plain(Cow::Borrowed("error")),
        ],
        description: Cow::Borrowed("Response status."),
        example: Some(Node::Plain(Cow::Borrowed("success"))),
        ..Default::default()
    }
}

pub fn warnings_schema() -> Schema {
    string_array_schema_with_description(
        "Only set if there were warnings while executing the request. There will still be data in the data field.",
    )
}

pub fn infos_schema() -> Schema {
    string_array_schema_with_description(
        "Only set if there were info-level annotations while executing the request.",
    )
}

/// Go `timestampSchema`.
pub fn timestamp_schema() -> Schema {
    Schema {
        one_of: vec![
            Schema {
                typ: "string",
                format: "date-time",
                description: Cow::Borrowed("RFC3339 timestamp."),
                ..Default::default()
            },
            Schema {
                typ: "number",
                format: "unixtime",
                description: Cow::Borrowed("Unix timestamp in seconds."),
                ..Default::default()
            },
        ],
        description: Cow::Borrowed("Timestamp in RFC3339 format or Unix timestamp in seconds."),
        ..Default::default()
    }
}

/// Go `durationSchema`.
pub fn duration_schema() -> Schema {
    Schema {
        one_of: vec![
            Schema {
                typ: "string",
                format: "duration",
                description: Cow::Borrowed(
                    "Human-readable form such as 15s or 2m30s. Supported units are ms (milliseconds), s (seconds), m (minutes), h (hours), d (days), w (weeks) and y (years).",
                ),
                ..Default::default()
            },
            Schema {
                typ: "number",
                format: "float",
                description: Cow::Borrowed("Fractional number of seconds."),
                ..Default::default()
            },
        ],
        description: Cow::Borrowed("Duration in human-readable or numeric format."),
        ..Default::default()
    }
}

pub fn string_schema_with_const_value(value: &'static str) -> Schema {
    Schema {
        typ: "string",
        enum_values: vec![Node::Plain(Cow::Borrowed(value))],
        ..Default::default()
    }
}

/// Go `enumStringSchema`.
pub fn enum_string_schema(values: &[&'static str]) -> Schema {
    Schema {
        typ: "string",
        enum_values: values
            .iter()
            .map(|v| Node::Plain(Cow::Borrowed(*v)))
            .collect(),
        example: Some(Node::from(values[0])),
        ..Default::default()
    }
}

/// Go `enumStringSchemaWithDescription`.
pub fn enum_string_schema_with_description(
    description: &'static str,
    values: &[&'static str],
) -> Schema {
    Schema {
        description: Cow::Borrowed(description),
        ..enum_string_schema(values)
    }
}

pub fn date_time_schema_with_description(description: &'static str) -> Schema {
    Schema {
        typ: "string",
        format: "date-time",
        description: Cow::Borrowed(description),
        ..Default::default()
    }
}

pub fn number_schema_with_description(description: &'static str) -> Schema {
    Schema {
        typ: "number",
        format: "double",
        description: Cow::Borrowed(description),
        ..Default::default()
    }
}

/// Go `errorResponse`.
pub fn error_response() -> Response {
    Response {
        description: "Error",
        content: vec![(
            "application/json",
            MediaType {
                schema: Some(component_ref("Error")),
                ..Default::default()
            },
        )],
    }
}

/// Go `responsesNoContent`.
pub fn responses_no_content() -> Responses {
    vec![
        (
            "204",
            Response {
                description: "No Content",
                content: Vec::new(),
            },
        ),
        ("default", error_response()),
    ]
}

/// Go `pathParam`.
pub fn path_param(name: &'static str, description: &'static str, schema: Schema) -> Parameter {
    Parameter {
        name,
        location: "path",
        description,
        required: true,
        explode: None,
        schema,
        examples: Vec::new(),
    }
}

/// Go `formRequestBodyWithExamples`.
pub fn form_request_body_with_examples(
    schema_name: &str,
    examples: Examples,
    description: &'static str,
) -> RequestBody {
    RequestBody {
        required: true,
        description,
        content: vec![(
            "application/x-www-form-urlencoded",
            MediaType {
                schema: Some(component_ref(schema_name)),
                examples: Some(examples),
                ..Default::default()
            },
        )],
    }
}

/// Go `jsonResponseWithExamples`.
pub fn json_response_with_examples(
    schema_name: &str,
    examples: Examples,
    description: &'static str,
) -> Response {
    Response {
        description,
        content: vec![(
            "application/json",
            MediaType {
                schema: Some(component_ref(schema_name)),
                examples: Some(examples),
                ..Default::default()
            },
        )],
    }
}

/// Go `textResponseWithExamples`.
pub fn text_response_with_examples(
    content_type: &'static str,
    schema: Schema,
    examples: Examples,
    description: &'static str,
) -> Response {
    Response {
        description,
        content: vec![(
            content_type,
            MediaType {
                schema: Some(schema),
                examples: Some(examples),
                ..Default::default()
            },
        )],
    }
}

/// Go `ndjsonResponsesWithErrorExamples`.
pub fn ndjson_responses_with_error_examples(
    success_examples: Examples,
    error_examples: Examples,
    success_description: &'static str,
    error_description: &'static str,
) -> Responses {
    vec![
        (
            "200",
            text_response_with_examples(
                "application/x-ndjson",
                string_schema_with_description("NDJSON response stream."),
                success_examples,
                success_description,
            ),
        ),
        (
            "default",
            json_response_with_examples("Error", error_examples, error_description),
        ),
    ]
}

/// Go `responsesWithErrorExamples`.
pub fn responses_with_error_examples(
    ok_schema_name: &str,
    success_examples: Examples,
    error_examples: Examples,
    success_description: &'static str,
    error_description: &'static str,
) -> Responses {
    vec![
        (
            "200",
            json_response_with_examples(ok_schema_name, success_examples, success_description),
        ),
        (
            "default",
            json_response_with_examples("Error", error_examples, error_description),
        ),
    ]
}

/// Go `timestampExamples(t)` for a time with whole seconds in UTC (`t.Format(time.RFC3339Nano)`, `t.Unix()`).
pub fn timestamp_examples(unix: i64) -> Vec<ExampleArg> {
    vec![
        ("RFC3339", Node::from(rfc3339_utc(unix))),
        ("epoch", Node::Int(unix)),
    ]
}

/// RFC 3339 text of a Unix time in seconds, in UTC.
fn rfc3339_utc(unix: i64) -> String {
    let days = unix.div_euclid(86400);
    let secs = unix.rem_euclid(86400);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y,
        m,
        d,
        secs / 3600,
        secs / 60 % 60,
        secs % 60
    )
}

/// Go `queryParamWithExample`.
pub fn query_param_with_example(
    name: &'static str,
    description: &'static str,
    required: bool,
    schema: Schema,
    examples: Vec<ExampleArg>,
) -> Parameter {
    Parameter {
        name,
        location: "query",
        description,
        required,
        explode: Some(false),
        schema,
        examples: example_map(examples),
    }
}

/// One example named "example" (Go `[]example{{"example", v}}`).
pub fn one(v: impl Into<Node>) -> Vec<ExampleArg> {
    vec![("example", v.into())]
}

/// Go `vectorExample`: the response JSON as the API writes it, decoded by go-yaml.
pub fn vector_example(v: &[Sample]) -> Node {
    let mut s =
        String::from("{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[");
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        crate::api::jsonutil::write_sample(x, &mut s);
    }
    s.push_str("]}}");
    Node::Flow(s)
}

/// Go `matrixExample`.
pub fn matrix_example(m: &[Series]) -> Node {
    let mut s =
        String::from("{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[");
    for (i, x) in m.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        crate::api::jsonutil::write_series(x, &mut s);
    }
    s.push_str("]}}");
    Node::Flow(s)
}

/// Go `labels.FromStrings`.
pub fn labels(ss: &[&str]) -> Labels {
    model::labels::from_strings(ss)
}

/// Go `map[string]string` of label-like pairs.
pub fn str_map(pairs: &[(&'static str, &'static str)]) -> Node {
    go_map(pairs.iter().map(|(k, v)| (*k, Node::from(*v))).collect())
}
