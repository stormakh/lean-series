//! Port of Prometheus `scrape/`: the scrape manager, scrape pools, scrape loops, targets, the scrape cache and
//! metadata store, limits, staleness markers, report series and start timestamp synthesis.
//!
//! Concurrency: no thread per target. A scheduler thread with a timer heap hands due scrape loops to an elastic
//! pool of worker threads ([`runtime`]); a scrape (blocking HTTP) runs on a worker and never on the scheduler.
//! Appends go through `storage::Appendable` or `storage::AppendableV2`.
//!
//! API summary for the server and web API:
//! - [`Manager::new`]`(options, logger, failure_logger_factory, appendable, appendable_v2, registry)`,
//!   [`Manager::apply_config`]`(&cfg)`, [`Manager::run`]`(discovery_sync_receiver)` (blocks), [`Manager::stop`].
//! - Web API v1: [`Manager::targets_active`], [`Manager::targets_dropped`], [`Manager::targets_dropped_counts`],
//!   [`Manager::targets_all`], [`Manager::scrape_pools`], [`Manager::scrape_pool_config`]; each [`Target`] has
//!   `labels()`, `discovered_labels()`, `url_string()`, `health()`, `last_error()`, `last_scrape()`,
//!   `last_scrape_duration()`, `scrape_config()`, and the metadata accessors `list_metadata()`,
//!   `get_metadata(name)`.

pub mod cache;
pub mod ctx;
pub mod failure_log;
pub mod limits;
pub mod manager;
pub mod metrics;
pub mod pool;
pub mod runtime;
pub mod scrape_loop;
pub mod scraper;
pub mod st_synthesis;
pub mod target;
pub mod testing;
pub mod util;

pub use cache::ScrapeCache;
pub use ctx::Ctx;
pub use failure_log::{FailureLogger, JsonFileLogger};
pub use manager::{Manager, Options};
pub use metrics::ScrapeMetrics;
pub use pool::{Loop, ScrapePool};
pub use runtime::Runtime;
pub use scrape_loop::ScrapeLoop;
pub use scraper::{Scraper, TargetScraper};
pub use target::{MetricMetadata, MetricMetadataStore, Target, TargetHealth, populate_labels, targets_from_group};
