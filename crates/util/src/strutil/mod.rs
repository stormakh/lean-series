//! Port of `util/strutil`: PromQL string unquoting, expression links, label name sanitizing, and fuzzy string
//! scoring (Jaro-Winkler and subsequence).

mod jarowinkler;
mod quote;
mod strconv;
mod subsequence;

pub use jarowinkler::{JaroWinklerMatcher, JaroWinklerWorkspace};
pub use quote::{ERR_SYNTAX, unquote, unquote_bytes, unquote_checked, unquote_invalid_bytes};
pub use strconv::{graph_link_for_expression, sanitize_full_label_name, sanitize_label_name, table_link_for_expression};
pub use subsequence::SubsequenceMatcher;
