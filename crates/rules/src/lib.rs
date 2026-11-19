//! Port of Prometheus `rules/` (recording and alerting rules, groups, the manager), `model/rulefmt` and the
//! bridge to `template/`.
//!
//! API summary (Go name in brackets):
//! - [`Manager::new`] (`NewManager`) with [`ManagerOptions`]: `query_func` (usually [`engine_query_func`]),
//!   `notify_func` (usually [`send_alerts`] over a `notifier::Manager`), `appendable_v2` and `queryable` (the
//!   storage), `context`, `outage_tolerance`, `for_grace_period`, `resend_delay`, `registerer`, ...
//! - [`Manager::update`] (`Update`) loads rule files; [`Manager::run`] blocks until [`Manager::stop`]
//!   (or call [`Manager::start`] and keep the thread free).
//! - API accessors: [`Manager::rule_groups`], [`Manager::rules`], [`Manager::alerting_rules`];
//!   per group: `name`, `file`, `interval`, `limit`, `query_offset`, `rules`, `get_evaluation_time`,
//!   `get_last_evaluation`; per rule ([`Rule`] trait): `name`, `labels`, `query`, `health`, `last_error`,
//!   `get_evaluation_duration`, `get_evaluation_timestamp`; alerting rules ([`AlertingRule`] via
//!   `rule.as_alerting()`): `state`, `active_alerts`, `hold_duration`, `keep_firing_for`, `annotations`.
//! - [`rulefmt::parse_file`] / [`rulefmt::parse`] for `promtool check rules`.
pub mod alerting;
pub mod group;
pub mod manager;
pub mod recording;
pub mod rule;
pub mod rulefmt;
mod tmpl;

pub use alerting::{Alert, AlertState, AlertingRule, NotifyFunc};
pub use discovery::log;
pub use group::{
    build_dependency_map, group_key, matches_matcher_sets, DependencyMap, Group, GroupEvalIterationFunc, GroupOptions,
    Metrics, RuleConcurrencyController, Signal,
};
pub use manager::{
    default_eval_iteration_func, engine_query_func, from_maps, new_rule_concurrency_controller, parse_files,
    send_alerts, DefaultRuleDependencyController, EngineQuery, FileLoader, GroupLoader, Manager, ManagerOptions,
    QueryEngine, RuleDependencyController, Sender, SequentialRuleEvalController,
};
pub use recording::RecordingRule;
pub use rule::{
    from_origin_context, new_origin_context, new_rule_detail, same_rule, QueryFunc, Rule, RuleDetail, RuleHealth,
    RuleRef, Time,
};
