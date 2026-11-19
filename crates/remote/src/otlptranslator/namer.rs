//! Port of the library `github.com/prometheus/otlptranslator` v1.0.0: OTLP to Prometheus metric, label and unit
//! name building, translation strategies and well-known names.

/// Go `ExemplarTraceIDKey`.
pub const EXEMPLAR_TRACE_ID_KEY: &str = "trace_id";
/// Go `ExemplarSpanIDKey`.
pub const EXEMPLAR_SPAN_ID_KEY: &str = "span_id";
/// Go `ScopeNameLabelKey`.
pub const SCOPE_NAME_LABEL_KEY: &str = "otel_scope_name";
/// Go `ScopeVersionLabelKey`.
pub const SCOPE_VERSION_LABEL_KEY: &str = "otel_scope_version";
/// Go `TargetInfoMetricName`.
pub const TARGET_INFO_METRIC_NAME: &str = "target_info";

// ---------------------------------------------------------------------------------------------------------------
// strategy.go

/// Go `NoUTF8EscapingWithSuffixes`.
pub const NO_UTF8_ESCAPING_WITH_SUFFIXES: &str = "NoUTF8EscapingWithSuffixes";
/// Go `UnderscoreEscapingWithSuffixes` (the default).
pub const UNDERSCORE_ESCAPING_WITH_SUFFIXES: &str = "UnderscoreEscapingWithSuffixes";
/// Go `UnderscoreEscapingWithoutSuffixes`.
pub const UNDERSCORE_ESCAPING_WITHOUT_SUFFIXES: &str = "UnderscoreEscapingWithoutSuffixes";
/// Go `NoTranslation` (experimental).
pub const NO_TRANSLATION: &str = "NoTranslation";

/// Go `TranslationStrategyOption.ShouldEscape`: whether names are escaped to the legacy character set. Unknown
/// strategies (including "") do not escape.
pub fn should_escape(strategy: &str) -> bool {
    matches!(strategy, UNDERSCORE_ESCAPING_WITH_SUFFIXES | UNDERSCORE_ESCAPING_WITHOUT_SUFFIXES)
}

/// Go `TranslationStrategyOption.ShouldAddSuffixes`. Unknown strategies (including "") add no suffixes.
pub fn should_add_suffixes(strategy: &str) -> bool {
    matches!(strategy, UNDERSCORE_ESCAPING_WITH_SUFFIXES | NO_UTF8_ESCAPING_WITH_SUFFIXES)
}

// ---------------------------------------------------------------------------------------------------------------
// metric_type.go

/// Go `otlptranslator.MetricType`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum MetricType {
    #[default]
    Unknown,
    NonMonotonicCounter,
    MonotonicCounter,
    Gauge,
    Histogram,
    ExponentialHistogram,
    Summary,
}

// ---------------------------------------------------------------------------------------------------------------
// strconv.go

fn is_valid_compliant_label_char(r: char) -> bool {
    r.is_ascii_alphanumeric()
}

fn is_reserved_label(name: &str) -> Option<&str> {
    if name.len() < 4 || !name.starts_with("__") || !name.ends_with("__") {
        return None;
    }
    Some(&name[2..name.len() - 2])
}

/// Go `sanitizeLabelName`: replaces invalid characters by `_` and, unless `preserve_multiple_underscores`,
/// collapses runs of them. Reserved names (`__x__`) keep their outer underscores.
fn sanitize_label_name(name: &str, preserve_multiple_underscores: bool) -> String {
    if preserve_multiple_underscores {
        return name.chars().map(|r| if is_valid_compliant_label_char(r) { r } else { '_' }).collect();
    }
    let (reserved, name) = match is_reserved_label(name) {
        Some(inner) => (true, inner),
        None => (false, name),
    };
    let mut b = String::with_capacity(name.len() + 4);
    let mut prev_was_underscore = false;
    for r in name.chars() {
        if is_valid_compliant_label_char(r) {
            b.push(r);
            prev_was_underscore = false;
        } else if !prev_was_underscore {
            b.push('_');
            prev_was_underscore = true;
        }
    }
    if reserved {
        return format!("__{b}__");
    }
    b
}

fn collapse_multiple_underscores(s: &str) -> String {
    let mut b = String::with_capacity(s.len());
    let mut prev = false;
    for r in s.chars() {
        if r == '_' {
            if !prev {
                b.push('_');
                prev = true;
            }
        } else {
            b.push(r);
            prev = false;
        }
    }
    b
}

// ---------------------------------------------------------------------------------------------------------------
// label_namer.go

/// Go `LabelNamer`: normalizes OTLP attribute names to Prometheus label names.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LabelNamer {
    pub utf8_allowed: bool,
    /// Prepends `key` to labels that start with a single `_`. Deprecated upstream.
    pub underscore_label_sanitization: bool,
    /// Keeps runs of underscores when `utf8_allowed` is false.
    pub preserve_multiple_underscores: bool,
}

fn has_underscores_only(s: &str) -> bool {
    s.chars().all(|c| c == '_')
}

impl LabelNamer {
    /// Go `LabelNamer.Build`.
    pub fn build(&self, label: &str) -> Result<String, String> {
        if label.is_empty() {
            return Err("label name is empty".to_string());
        }
        if self.utf8_allowed {
            if has_underscores_only(label) {
                return Err(format!("label name {} contains only underscores", go_quote(label)));
            }
            return Ok(label.to_string());
        }
        let mut normalized = sanitize_label_name(label, self.preserve_multiple_underscores);
        if normalized.as_bytes()[0].is_ascii_digit() {
            normalized = format!("key_{normalized}");
        } else if self.underscore_label_sanitization && normalized.starts_with('_') && !normalized.starts_with("__") {
            normalized = format!("key{normalized}");
        }
        if has_underscores_only(&normalized) {
            return Err(format!(
                "normalization for label name {} resulted in invalid name {}",
                go_quote(label),
                go_quote(&normalized)
            ));
        }
        Ok(normalized)
    }
}

/// Go `%q` of a string.
pub fn go_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x07' => out.push_str("\\a"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            '\x0b' => out.push_str("\\v"),
            c if (c as u32) < 0x20 || c == '\x7f' => out.push_str(&format!("\\x{:02x}", c as u32)),
            c if is_go_printable(c) => out.push(c),
            c if (c as u32) < 0x10000 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push_str(&format!("\\U{:08x}", c as u32)),
        }
    }
    out.push('"');
    out
}

/// An approximation of Go's `strconv.IsPrint` for non-ASCII characters: control and format characters and
/// separators other than space are not printable.
fn is_go_printable(c: char) -> bool {
    let u = c as u32;
    if u < 0x80 {
        return (0x20..0x7f).contains(&u);
    }
    !(c.is_control()
        || (0x2000..=0x200f).contains(&u)
        || (0x2028..=0x202f).contains(&u)
        || (0x205f..=0x206f).contains(&u)
        || u == 0xa0
        || u == 0xad
        || u == 0xfeff
        || u == 0x3000
        || (0xfff9..=0xfffb).contains(&u)
        || (0xe000..=0xf8ff).contains(&u)
        || u >= 0xf0000)
}

// ---------------------------------------------------------------------------------------------------------------
// unit_namer.go

fn unit_map_get(unit: &str) -> Option<&'static str> {
    Some(match unit {
        // Time
        "d" => "days",
        "h" => "hours",
        "min" => "minutes",
        "s" => "seconds",
        "ms" => "milliseconds",
        "us" => "microseconds",
        "ns" => "nanoseconds",
        // Bytes
        "By" => "bytes",
        "KiBy" => "kibibytes",
        "MiBy" => "mebibytes",
        "GiBy" => "gibibytes",
        "TiBy" => "tibibytes",
        "KBy" => "kilobytes",
        "MBy" => "megabytes",
        "GBy" => "gigabytes",
        "TBy" => "terabytes",
        // SI
        "m" => "meters",
        "V" => "volts",
        "A" => "amperes",
        "J" => "joules",
        "W" => "watts",
        "g" => "grams",
        // Misc
        "Cel" => "celsius",
        "Hz" => "hertz",
        "1" => "",
        "%" => "percent",
        _ => return None,
    })
}

fn per_unit_map_get(unit: &str) -> Option<&'static str> {
    Some(match unit {
        "s" => "second",
        "m" => "minute",
        "h" => "hour",
        "d" => "day",
        "w" => "week",
        "mo" => "month",
        "y" => "year",
        _ => return None,
    })
}

/// Go `UnitNamer`: builds the Prometheus unit from an OTLP unit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UnitNamer {
    pub utf8_allowed: bool,
}

impl UnitNamer {
    /// Go `UnitNamer.Build`.
    pub fn build(&self, unit: &str) -> String {
        let (mut main_unit, mut per_unit) = build_unit_suffixes(unit);
        if !self.utf8_allowed {
            main_unit = clean_up_unit(&main_unit);
            per_unit = clean_up_unit(&per_unit);
        }
        let mut u = if !main_unit.is_empty() && !per_unit.is_empty() {
            format!("{main_unit}_{per_unit}")
        } else if !main_unit.is_empty() {
            main_unit
        } else {
            per_unit
        };
        if u.starts_with('_') {
            u.remove(0);
        }
        if u.ends_with('_') {
            u.pop();
        }
        u
    }
}

/// Go `buildUnitSuffixes`: the main unit and the `per_` unit.
fn build_unit_suffixes(unit: &str) -> (String, String) {
    let mut main_unit_suffix = String::new();
    let mut per_unit_suffix = String::new();
    let mut tokens = unit.splitn(2, '/');
    // Go's SplitN always returns at least one token.
    let main = tokens.next().unwrap_or("");
    let main_otel = main.trim_matches(go_space);
    if !main_otel.is_empty() && !main_otel.contains(['{', '}']) {
        main_unit_suffix = unit_map_get(main_otel).map(str::to_string).unwrap_or_else(|| main_otel.to_string());
    }
    if let Some(per) = tokens.next()
        && !per.is_empty()
    {
        let per_otel = per.trim_matches(go_space);
        if !per_otel.is_empty() && !per_otel.contains(['{', '}']) {
            per_unit_suffix = per_unit_map_get(per_otel).map(str::to_string).unwrap_or_else(|| per_otel.to_string());
        }
        if !per_unit_suffix.is_empty() {
            per_unit_suffix = format!("per_{per_unit_suffix}");
        }
    }
    (main_unit_suffix, per_unit_suffix)
}

/// Go `unicode.IsSpace` (what `strings.TrimSpace` trims).
fn go_space(c: char) -> bool {
    matches!(c, '\t' | '\n' | '\x0b' | '\x0c' | '\r' | ' ' | '\u{85}' | '\u{a0}') || (c as u32 > 0xff && c.is_whitespace())
}

fn is_valid_compliant_metric_char(r: char) -> bool {
    r.is_ascii_alphanumeric() || r == ':'
}

/// Go `cleanUpUnit`.
fn clean_up_unit(unit: &str) -> String {
    let mapped: String = unit.chars().map(|r| if is_valid_compliant_metric_char(r) { r } else { '_' }).collect();
    let c = collapse_multiple_underscores(&mapped);
    match c.strip_prefix('_') {
        Some(s) => s.to_string(),
        None => c,
    }
}

// ---------------------------------------------------------------------------------------------------------------
// metric_namer.go

/// Go `Metric`: the parts of an OTLP metric that name building needs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Metric {
    pub name: String,
    pub unit: String,
    pub typ: MetricType,
}

/// Go `MetricNamer`: builds Prometheus metric names from OTLP metrics.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetricNamer {
    pub namespace: String,
    pub with_metric_suffixes: bool,
    pub utf8_allowed: bool,
}

/// Go `strings.FieldsFunc`: the non-empty runs of characters for which `sep` is false.
fn fields_func(s: &str, sep: impl Fn(char) -> bool) -> Vec<&str> {
    s.split(|c| sep(c)).filter(|t| !t.is_empty()).collect()
}

impl MetricNamer {
    /// Go `NewMetricNamer`.
    pub fn new(namespace: &str, strategy: &str) -> MetricNamer {
        MetricNamer {
            namespace: namespace.to_string(),
            with_metric_suffixes: should_add_suffixes(strategy),
            utf8_allowed: !should_escape(strategy),
        }
    }

    /// Go `MetricNamer.Build`.
    pub fn build(&self, metric: &Metric) -> Result<String, String> {
        if self.utf8_allowed {
            return Ok(self.build_metric_name(&metric.name, &metric.unit, metric.typ));
        }
        self.build_compliant_metric_name(&metric.name, &metric.unit, metric.typ)
    }

    fn build_compliant_metric_name(&self, name: &str, unit: &str, typ: MetricType) -> Result<String, String> {
        let normalized = if self.with_metric_suffixes {
            normalize_name(name, unit, typ, &self.namespace)
        } else {
            let sep = |r: char| !is_valid_compliant_metric_char(r) && r != '_';
            let mut metric_name = fields_func(name, sep).join("_");
            if !self.namespace.is_empty() {
                let namespace = fields_func(&self.namespace, sep).join("_");
                format!("{namespace}_{metric_name}")
            } else {
                if metric_name.as_bytes().first().is_some_and(|b| b.is_ascii_digit()) {
                    metric_name = format!("_{metric_name}");
                }
                metric_name
            }
        };
        // The deferred checks of Go's buildCompliantMetricName.
        if normalized.is_empty() {
            return Err(format!("normalization for metric {} resulted in empty name", go_quote(name)));
        }
        if normalized == name || !has_underscores_only(&normalized) {
            return Ok(normalized);
        }
        Err(format!("normalization for metric {} resulted in invalid name {}", go_quote(name), go_quote(&normalized)))
    }

    fn build_metric_name(&self, input_name: &str, unit: &str, typ: MetricType) -> String {
        let mut name = input_name.to_string();
        if !self.namespace.is_empty() {
            name = format!("{}_{}", self.namespace, name);
        }
        if !self.with_metric_suffixes {
            return name;
        }
        // Go appends these in deferred functions, which run in reverse order: per-unit, then total, then ratio.
        let add_ratio = unit == "1" && typ == MetricType::Gauge;
        if add_ratio {
            name = trim_suffix_and_delimiter(&name, "ratio");
        }
        let add_total = typ == MetricType::MonotonicCounter;
        if add_total {
            name = trim_suffix_and_delimiter(&name, "total");
        }
        let (main_unit_suffix, per_unit_suffix) = build_unit_suffixes(unit);
        if !per_unit_suffix.is_empty() {
            name = trim_suffix_and_delimiter(&name, &per_unit_suffix);
        }
        if !main_unit_suffix.is_empty() && !name.ends_with(&main_unit_suffix) {
            name = format!("{name}_{main_unit_suffix}");
        }
        if !per_unit_suffix.is_empty() {
            name = format!("{name}_{per_unit_suffix}");
        }
        if add_total {
            name.push_str("_total");
        }
        if add_ratio {
            name.push_str("_ratio");
        }
        name
    }
}

/// Go `normalizeName`.
fn normalize_name(name: &str, unit: &str, typ: MetricType, namespace: &str) -> String {
    let mut tokens: Vec<String> =
        fields_func(name, |r| !is_valid_compliant_metric_char(r)).into_iter().map(str::to_string).collect();
    let (main_unit_suffix, per_unit_suffix) = build_unit_suffixes(unit);
    tokens = add_unit_tokens(tokens, clean_up_unit(&main_unit_suffix), clean_up_unit(&per_unit_suffix));
    if typ == MetricType::MonotonicCounter {
        tokens.retain(|t| t != "total");
        tokens.push("total".to_string());
    }
    if unit == "1" && typ == MetricType::Gauge {
        tokens.retain(|t| t != "ratio");
        tokens.push("ratio".to_string());
    }
    if !namespace.is_empty() {
        tokens.insert(0, namespace.to_string());
    }
    let mut normalized = tokens.join("_");
    if normalized.as_bytes().first().is_some_and(|b| b.is_ascii_digit()) {
        normalized = format!("_{normalized}");
    }
    normalized
}

/// Go `addUnitTokens`.
fn add_unit_tokens(mut tokens: Vec<String>, mut main: String, mut per: String) -> Vec<String> {
    if tokens.contains(&main) {
        main.clear();
    }
    if per == "per_" {
        per.clear();
    } else {
        if let Some(p) = per.strip_suffix('_') {
            per = p.to_string();
        }
        if tokens.contains(&per) {
            per.clear();
        }
    }
    if !per.is_empty()
        && let Some(m) = main.strip_suffix('_')
    {
        main = m.to_string();
    }
    if !main.is_empty() {
        tokens.push(main);
    }
    if !per.is_empty() {
        tokens.push(per);
    }
    tokens
}

/// Go `trimSuffixAndDelimiter`.
fn trim_suffix_and_delimiter(name: &str, suffix: &str) -> String {
    if name.ends_with(suffix) && name.len() > suffix.len() + 1 {
        return name[..name.len() - (suffix.len() + 1)].to_string();
    }
    name.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(name: &str, unit: &str, typ: MetricType) -> Metric {
        Metric { name: name.into(), unit: unit.into(), typ }
    }

    #[test]
    fn metric_names() {
        let n = MetricNamer { with_metric_suffixes: true, ..Default::default() };
        assert_eq!(n.build(&m("system.io", "By", MetricType::MonotonicCounter)).unwrap(), "system_io_bytes_total");
        assert_eq!(n.build(&m("cpu.utilization", "1", MetricType::Gauge)).unwrap(), "cpu_utilization_ratio");
        assert_eq!(n.build(&m("speed", "m/s", MetricType::Gauge)).unwrap(), "speed_meters_per_second");
        assert_eq!(n.build(&m("2xx", "", MetricType::Gauge)).unwrap(), "_2xx");
        assert!(n.build(&m("___", "", MetricType::Gauge)).is_err());
        let u = MetricNamer { with_metric_suffixes: true, utf8_allowed: true, ..Default::default() };
        assert_eq!(u.build(&m("a.b", "m/s", MetricType::MonotonicCounter)).unwrap(), "a.b_meters_per_second_total");
        assert_eq!(u.build(&m("test.counter", "By", MetricType::MonotonicCounter)).unwrap(), "test.counter_bytes_total");
    }

    #[test]
    fn label_names() {
        let n = LabelNamer::default();
        assert_eq!(n.build("foo.bar").unwrap(), "foo_bar");
        assert_eq!(n.build("0x").unwrap(), "key_0x");
        assert_eq!(n.build("__a.b__").unwrap(), "__a_b__");
        assert_eq!(n.build("...").unwrap_err(), r#"normalization for label name "..." resulted in invalid name "_""#);
        let s = LabelNamer { underscore_label_sanitization: true, preserve_multiple_underscores: true, ..Default::default() };
        assert_eq!(s.build("_private").unwrap(), "key_private");
        assert_eq!(UnitNamer::default().build("m/s"), "meters_per_second");
        assert_eq!(UnitNamer::default().build("{req}/s"), "per_second");
    }
}
