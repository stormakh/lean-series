//! Port of Prometheus `rules/` (recording and alerting rules, groups, the manager) and `model/rulefmt`.
pub mod rulefmt;
mod template_check;

pub use discovery::log;
