//! Port of `util/strutil/strconv.go`: UI links for expressions and label name sanitizing.

/// Go `TableLinkForExpression`: an escaped relative link to the table view of the expression.
pub fn table_link_for_expression(expr: &str) -> String {
    format!("/graph?g0.expr={}&g0.tab=1", yaml::url::query_escape(expr))
}

/// Go `GraphLinkForExpression`: an escaped relative link to the graph view of the expression.
pub fn graph_link_for_expression(expr: &str) -> String {
    format!("/graph?g0.expr={}&g0.tab=0", yaml::url::query_escape(expr))
}

/// Go `SanitizeLabelName`: replaces every character outside `[a-zA-Z0-9_]` with an underscore.
///
/// This does not handle all Prometheus label name restrictions (such as not starting with a digit 0-9), and
/// hence should only be used if the label name is prefixed with a known valid string.
pub fn sanitize_label_name(name: &str) -> String {
    name.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect()
}

/// Go `SanitizeFullLabelName`: replaces any invalid character with an underscore (also a leading digit), and an
/// empty string with a single underscore.
pub fn sanitize_full_label_name(name: &str) -> String {
    if name.is_empty() {
        return "_".to_string();
    }
    // Go ranges over runes with their byte index; only index 0 matters.
    name.char_indices()
        .map(|(i, b)| {
            if b.is_ascii_alphabetic() || b == '_' || (b.is_ascii_digit() && i > 0) { b } else { '_' }
        })
        .collect()
}
