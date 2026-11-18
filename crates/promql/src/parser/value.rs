//! Port of `promql/parser/value.go`: the value types of expressions.

use std::fmt;

/// The type of a value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum ValueType {
    #[default]
    None,
    Vector,
    Scalar,
    Matrix,
    String,
}

impl ValueType {
    /// The Go string value: "none", "vector", "scalar", "matrix", "string".
    pub fn as_str(self) -> &'static str {
        match self {
            ValueType::None => "none",
            ValueType::Vector => "vector",
            ValueType::Scalar => "scalar",
            ValueType::Matrix => "matrix",
            ValueType::String => "string",
        }
    }
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The user facing name of a value type as the documentation uses it (Go `DocumentedType`).
pub fn documented_type(t: ValueType) -> &'static str {
    match t {
        ValueType::Vector => "instant vector",
        ValueType::Matrix => "range vector",
        _ => t.as_str(),
    }
}
