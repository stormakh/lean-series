//! The template parse check of rulefmt (Go `NewTemplateExpander(...).ParseTest()`).

/// Parses `text` as the template named `name` with Prometheus' template functions.
pub fn parse_test(text: &str, name: &str) -> Result<(), String> {
    let _ = (text, name);
    Ok(()) // TODO(w3-rules): call template::Expander::parse_test when the template crate lands
}
