//! Port of Prometheus `web/`: the HTTP API v1 (`api`), the web handler with lifecycle endpoints, federation and
//! the UI (`web`, `federate`, `ui`), self-instrumentation (`metrics`), and the helpers they share.
//!
//! API summary for the server:
//! - [`api::Api::new`]`(`[`api::ApiOptions`]`)` builds the API from its parts (query engine, queryable, retrievers,
//!   TSDB admin, config getter, flags, build info, notifications, gatherer, remote handlers); wrap it in an `Arc`
//!   and mount it with [`api::Api::register`] on a [`route::Router`] at `/api/v1` (under the route prefix).
//! - Retriever traits the server implements or wires: [`api::TargetRetriever`] and [`api::ScrapePoolsRetriever`]
//!   (implemented for `scrape::Manager`), [`api::AlertmanagerRetriever`], [`api::RulesRetriever`] (rules and
//!   notifier), [`api::TsdbAdminStats`], [`api::QueryEngine`] (implemented for `promql::Engine`).
pub mod api;
pub mod gotime;
pub mod httputil;
pub mod metrics;
pub mod notifications;
pub mod route;
