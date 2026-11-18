//! Port of `promql/engine.go`: the query engine.
//!
//! Design notes (Rust):
//! - The parsed expression is never changed while it is evaluated. Go stores selected series and `@`-adjusted
//!   offsets in the AST; here they live in side tables of the query, keyed by the address of the node.
//! - Errors are `Result`s instead of panics; runtime panics inside an evaluation become "unexpected error" as
//!   with Go's `recover`.
//! - Times are milliseconds since the epoch, durations in the AST are nanoseconds as in the parser.

mod aggregate;
mod binop;
pub(crate) mod eval;
mod preprocess;

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use model::labels::Labels;
use storage::{Annotations, CancelToken, Context, Error, Queryable, ERR_CANCELED, ERR_DEADLINE_EXCEEDED};
use util::stats::{self, QuerySamples, QueryTiming, QueryTimers, Statistics};

use crate::features::{self, Collector};
use crate::parser::{self, Expr, Parser, ValueType};
use crate::value::*;

pub use aggregate::{HashRatioSampler, RatioSampler};
pub use binop::{scalar_binop, vector_elem_binop};
pub use eval::{EvalNodeHelper, EvalSeriesHelper, Evaluator, MatrixArg, recover_evaluator};
pub use eval::QueryBudget;
pub use preprocess::{
    find_min_max_time, for_each_child_mut, get_time_ranges_for_selector, preprocess_expr, set_offset_for_at_modifier,
    subquery_evaluation_times, subquery_times,
};

/// Go `env` of the engine errors.
pub const ENV: &str = "query execution";
/// The default lookback delta (5 minutes).
pub const DEFAULT_LOOKBACK_DELTA: Duration = Duration::from_secs(300);
/// The largest float that converts to an int64 without overflow.
pub const MAX_INT64: f64 = 9223372036854774784.0;
/// The smallest float that converts to an int64 without underflow.
pub const MIN_INT64: f64 = -9223372036854775808.0;

// -----------------------------------------------------------------------------------------------------------------
// Errors.

/// The typed errors of the engine (Go `ErrQueryTimeout`, `ErrQueryCanceled`, ...). They are wrapped in
/// [`storage::Error::Other`]; use [`PromqlError::find`] to get one back (Go `errors.As`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PromqlError {
    QueryTimeout(String),
    QueryCanceled(String),
    TooManySamples(String),
    TooManySeries(String),
    TooManySamplesRead(String),
    /// Go `ErrLimitAboveCeiling`.
    LimitAboveCeiling { limit: String, requested: String, ceiling: String },
}

impl fmt::Display for PromqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PromqlError::QueryTimeout(e) => write!(f, "query timed out in {}", e),
            PromqlError::QueryCanceled(e) => write!(f, "query was canceled in {}", e),
            PromqlError::TooManySamples(e) => write!(f, "query processing would load too many samples into memory in {}", e),
            PromqlError::TooManySeries(e) => write!(f, "query would load too many series into memory in {}", e),
            PromqlError::TooManySamplesRead(e) => write!(f, "query would read too many samples in {}", e),
            PromqlError::LimitAboveCeiling { limit, requested, ceiling } => {
                write!(f, "requested {} of {} exceeds the server limit of {}", limit, requested, ceiling)
            }
        }
    }
}

impl std::error::Error for PromqlError {}

impl PromqlError {
    /// As a storage error.
    pub fn err(self) -> Error {
        Error::other(self)
    }

    /// The typed engine error inside `err`, if any (Go `errors.As`).
    pub fn find(err: &Error) -> Option<&PromqlError> {
        err.downcast_ref::<PromqlError>()
    }
}

/// Go `ErrStorage`: an error of the storage layer during query handling.
#[derive(Clone, Debug)]
pub struct ErrStorage(pub Error);

impl fmt::Display for ErrStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for ErrStorage {}

/// An error with the warnings collected before it (Go `errWithWarnings`).
#[derive(Clone, Debug)]
pub struct ErrWithWarnings {
    pub err: Error,
    pub warnings: Annotations,
}

impl fmt::Display for ErrWithWarnings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.err, f)
    }
}

impl std::error::Error for ErrWithWarnings {}

static AT_MODIFIER_DISABLED: storage::Sentinel = storage::Sentinel::new("@ modifier is disabled");
/// Go `ErrValidationAtModifierDisabled`.
pub const ERR_VALIDATION_AT_MODIFIER_DISABLED: Error = Error::Sentinel(&AT_MODIFIER_DISABLED);
static NEGATIVE_OFFSET_DISABLED: storage::Sentinel = storage::Sentinel::new("negative offset is disabled");
/// Go `ErrValidationNegativeOffsetDisabled`.
pub const ERR_VALIDATION_NEGATIVE_OFFSET_DISABLED: Error = Error::Sentinel(&NEGATIVE_OFFSET_DISABLED);
static QUERY_COST_DURATION: storage::Sentinel = storage::Sentinel::new("query cost duration exceeded");
/// Go `errQueryCostDuration`.
pub const ERR_QUERY_COST_DURATION: Error = Error::Sentinel(&QUERY_COST_DURATION);

/// Go `contextErr`: maps context errors to the engine errors.
pub fn context_err(err: Error, env: &str) -> Error {
    if err.is(&ERR_CANCELED) {
        PromqlError::QueryCanceled(env.to_string()).err()
    } else if err.is(&ERR_DEADLINE_EXCEEDED) {
        PromqlError::QueryTimeout(env.to_string()).err()
    } else {
        err
    }
}

/// Go `contextDone`.
#[inline]
pub fn context_done(ctx: &Context, env: &str) -> Result<(), Error> {
    match ctx.err() {
        None => Ok(()),
        Some(e) => Err(context_err(e, env)),
    }
}

// -----------------------------------------------------------------------------------------------------------------
// Options.

/// Options of one query (Go `QueryOpts`, `PrometheusQueryOpts` and `QueryCostOpts`).
#[derive(Clone, Debug, Default)]
pub struct QueryOpts {
    /// Records per-step statistics if the engine has them enabled as well.
    pub enable_per_step_stats: bool,
    /// The lookback delta of this query; zero uses the engine's.
    pub lookback_delta: Duration,
    /// Overrides the engine's use of start timestamps in functions such as rate().
    pub use_start_timestamps: Option<bool>,
    /// Per-query cost limit overrides (only with the query-cost feature); zero means no override.
    pub max_series: u64,
    pub max_samples_read: u64,
    pub max_query_duration: Duration,
}

impl QueryOpts {
    /// Go `NewPrometheusQueryOpts`.
    pub fn new(enable_per_step_stats: bool, lookback_delta: Duration, use_start_timestamps: Option<bool>) -> QueryOpts {
        QueryOpts { enable_per_step_stats, lookback_delta, use_start_timestamps, ..Default::default() }
    }

    /// Go `NewPrometheusQueryOptsWithLimits`.
    pub fn with_limits(
        enable_per_step_stats: bool,
        lookback_delta: Duration,
        max_series: u64,
        max_samples_read: u64,
        max_query_duration: Duration,
        use_start_timestamps: Option<bool>,
    ) -> QueryOpts {
        QueryOpts {
            enable_per_step_stats,
            lookback_delta,
            use_start_timestamps,
            max_series,
            max_samples_read,
            max_query_duration,
        }
    }
}

/// Tracks active queries and limits their number (Go `QueryTracker`).
pub trait QueryTracker: Send + Sync {
    /// The maximum number of concurrent queries.
    fn get_max_concurrent(&self) -> usize;
    /// Inserts a query; blocks while the maximum number of queries runs. Fails when the context is done first.
    fn insert(&self, ctx: &Context, query: &str) -> Result<usize, Error>;
    /// Removes a query by the index `insert` returned.
    fn delete(&self, insert_index: usize);
    /// Releases the tracker.
    fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// A value of a query log field.
#[derive(Clone, Debug, PartialEq)]
pub enum LogValue {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

/// One logged query (the attributes of Go's "promql query logged" record).
#[derive(Clone, Debug)]
pub struct QueryLogEntry {
    /// "query", "start", "end", "step" (in this order for range and instant queries).
    pub params: Vec<(String, LogValue)>,
    pub error: Option<Error>,
    pub stats: stats::BuiltinStats,
    /// The origin data of the context (Go `NewOriginContext`), in insertion order.
    pub origin: Vec<(String, LogValue)>,
}

/// Logs executed queries (Go `QueryLogger`, an `slog.Handler` and `io.Closer`).
pub trait QueryLogger: Send + Sync {
    fn log(&self, entry: &QueryLogEntry);
    fn close(&self) -> Result<(), Error>;
}

/// Computes the default subquery step from the subquery range in milliseconds.
pub type NoStepSubqueryIntervalFn = Arc<dyn Fn(i64) -> i64 + Send + Sync>;

/// The configuration of an engine (Go `EngineOpts`).
#[derive(Clone, Default)]
pub struct EngineOpts {
    pub max_samples: usize,
    pub timeout: Duration,
    pub active_query_tracker: Option<Arc<dyn QueryTracker>>,
    /// The time since the last sample after which a series is considered stale; zero means 5 minutes.
    pub lookback_delta: Duration,
    /// The default evaluation interval of a subquery without step, in milliseconds.
    pub no_step_subquery_interval_fn: Option<NoStepSubqueryIntervalFn>,
    pub enable_at_modifier: bool,
    pub enable_negative_offset: bool,
    pub enable_per_step_stats: bool,
    pub enable_delayed_name_removal: bool,
    pub enable_type_and_unit_labels: bool,
    pub use_start_timestamps: bool,
    pub enable_query_cost: bool,
    pub feature_registry: Option<Arc<dyn Collector + Send + Sync>>,
    /// The parser; `None` uses default options.
    pub parser: Option<Parser>,
}

/// The counters of an engine (Go `engineMetrics`), for self-instrumentation.
#[derive(Debug, Default)]
pub struct EngineMetrics {
    pub current_queries: AtomicI64,
    pub max_concurrent_queries: AtomicI64,
    pub query_log_enabled: AtomicI64,
    pub query_log_failures: AtomicU64,
    pub query_samples: AtomicU64,
    pub query_samples_read: AtomicU64,
    /// Rejections by the query-cost guardrails: max_series, max_samples_read, max_duration.
    pub query_rejected_total: Mutex<HashMap<&'static str, u64>>,
}

struct EngineInner {
    metrics: EngineMetrics,
    timeout: Duration,
    max_samples_per_query: usize,
    active_query_tracker: Option<Arc<dyn QueryTracker>>,
    query_logger: RwLock<Option<Arc<dyn QueryLogger>>>,
    lookback_delta: Duration,
    no_step_subquery_interval_fn: NoStepSubqueryIntervalFn,
    enable_at_modifier: bool,
    enable_negative_offset: bool,
    enable_per_step_stats: bool,
    enable_delayed_name_removal: bool,
    enable_type_and_unit_labels: bool,
    use_start_timestamps: bool,
    enable_query_cost: bool,
    parser: Parser,
    query_limits: RwLock<(u64, u64, Duration)>,
}

/// The query engine (Go `Engine`). Cheap to clone; clones share the engine.
#[derive(Clone)]
pub struct Engine {
    inner: Arc<EngineInner>,
}

impl fmt::Debug for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Engine")
    }
}

impl Engine {
    /// Go `NewEngine`.
    pub fn new(mut opts: EngineOpts) -> Engine {
        let metrics = EngineMetrics::default();
        match &opts.active_query_tracker {
            Some(t) => metrics.max_concurrent_queries.store(t.get_max_concurrent() as i64, Ordering::Relaxed),
            None => metrics.max_concurrent_queries.store(-1, Ordering::Relaxed),
        }
        let parser = opts.parser.take().unwrap_or_default();
        if opts.lookback_delta.is_zero() {
            opts.lookback_delta = DEFAULT_LOOKBACK_DELTA;
        }
        if let Some(r) = &opts.feature_registry {
            r.set(features::PROMQL, "at_modifier", opts.enable_at_modifier);
            r.set(features::PROMQL, "negative_offset", opts.enable_negative_offset);
            r.set(features::PROMQL, "per_step_stats", opts.enable_per_step_stats);
            r.set(features::PROMQL, "delayed_name_removal", opts.enable_delayed_name_removal);
            r.set(features::PROMQL, "type_and_unit_labels", opts.enable_type_and_unit_labels);
            r.enable(features::PROMQL, "per_query_lookback_delta");
            r.enable(features::PROMQL, "subqueries");
            parser.register_features(&**r);
        }
        let no_step = opts.no_step_subquery_interval_fn.clone().unwrap_or_else(|| Arc::new(|_| 60_000));
        Engine {
            inner: Arc::new(EngineInner {
                metrics,
                timeout: opts.timeout,
                max_samples_per_query: opts.max_samples,
                active_query_tracker: opts.active_query_tracker,
                query_logger: RwLock::new(None),
                lookback_delta: opts.lookback_delta,
                no_step_subquery_interval_fn: no_step,
                enable_at_modifier: opts.enable_at_modifier,
                enable_negative_offset: opts.enable_negative_offset,
                enable_per_step_stats: opts.enable_per_step_stats,
                enable_delayed_name_removal: opts.enable_delayed_name_removal,
                enable_type_and_unit_labels: opts.enable_type_and_unit_labels,
                use_start_timestamps: opts.use_start_timestamps,
                enable_query_cost: opts.enable_query_cost,
                parser,
                query_limits: RwLock::new((0, 0, Duration::ZERO)),
            }),
        }
    }

    /// The counters of the engine.
    pub fn metrics(&self) -> &EngineMetrics {
        &self.inner.metrics
    }

    /// The parser of the engine.
    pub fn parser(&self) -> &Parser {
        &self.inner.parser
    }

    /// The default lookback delta of the engine.
    pub fn lookback_delta(&self) -> Duration {
        self.inner.lookback_delta
    }

    /// Go `SetQueryLimits`: the reloadable query-cost limits.
    pub fn set_query_limits(&self, max_series: u64, max_samples_read: u64, max_query_duration: Duration) {
        *self.inner.query_limits.write().unwrap() = (max_series, max_samples_read, max_query_duration);
    }

    /// Go `Close`.
    pub fn close(&self) -> Result<(), Error> {
        match &self.inner.active_query_tracker {
            Some(t) => t.close(),
            None => Ok(()),
        }
    }

    /// Go `SetQueryLogger`.
    pub fn set_query_logger(&self, l: Option<Arc<dyn QueryLogger>>) {
        let mut g = self.inner.query_logger.write().unwrap();
        if let Some(old) = g.take() {
            // An error closing the old file must not make the reload fail.
            let _ = old.close();
        }
        self.inner.metrics.query_log_enabled.store(if l.is_some() { 1 } else { 0 }, Ordering::Relaxed);
        *g = l;
    }

    /// Go `AcquireQuerySlot`: reserves a concurrent query slot; the returned guard releases it.
    pub fn acquire_query_slot(&self, ctx: &Context, qs: &str) -> Result<QuerySlot, Error> {
        match &self.inner.active_query_tracker {
            None => Ok(QuerySlot { tracker: None, index: 0 }),
            Some(t) => {
                let i = t.insert(ctx, qs)?;
                Ok(QuerySlot { tracker: Some(t.clone()), index: i })
            }
        }
    }

    /// Go `NewInstantQuery`. `ts` is in milliseconds.
    pub fn new_instant_query(
        &self,
        ctx: &Context,
        q: Arc<dyn Queryable>,
        opts: Option<&QueryOpts>,
        qs: &str,
        ts: i64,
    ) -> Result<Query, Error> {
        self.new_query_impl(ctx, q, opts, qs, ts, ts, 0, false)
    }

    /// Go `NewRangeQuery`. Times and `interval` are in milliseconds.
    pub fn new_range_query(
        &self,
        ctx: &Context,
        q: Arc<dyn Queryable>,
        opts: Option<&QueryOpts>,
        qs: &str,
        start: i64,
        end: i64,
        interval: i64,
    ) -> Result<Query, Error> {
        self.new_query_impl(ctx, q, opts, qs, start, end, interval, true)
    }

    #[allow(clippy::too_many_arguments)]
    fn new_query_impl(
        &self,
        ctx: &Context,
        q: Arc<dyn Queryable>,
        opts: Option<&QueryOpts>,
        qs: &str,
        start: i64,
        end: i64,
        interval: i64,
        range: bool,
    ) -> Result<Query, Error> {
        let mut qry = self.new_query(q, qs, opts, start, end, interval)?;
        let ng = &*self.inner;
        let mut ctx = ctx.clone();
        let mut _cancel = None;
        if ng.enable_query_cost {
            let (c, t) = qry.execution_context(&ctx);
            ctx = c;
            _cancel = Some(t);
        }
        let res = (|| {
            let _slot = match self.queue_active(&ctx, &qry.q) {
                Ok(s) => s,
                Err(e) => return Err(if ng.enable_query_cost { context_err(e, ENV) } else { e }),
            };
            if ng.enable_query_cost {
                context_done(&ctx, ENV)?;
            }
            let expr = match ng.parser.parse_expr(qs) {
                Ok(e) => e,
                Err(e) => return Err(qry.construction_error(&ctx, Error::other(e))),
            };
            if let Err(e) = self.validate_opts(&expr) {
                return Err(qry.construction_error(&ctx, e));
            }
            if range && expr.value_type() != ValueType::Vector && expr.value_type() != ValueType::Scalar {
                return Err(Error::msg(format!(
                    "invalid expression type {} for range query, must be Scalar or instant Vector",
                    parser::quote::go_quote(parser::documented_type(expr.value_type()))
                )));
            }
            let pre = preprocess_expr(expr, start, end, interval);
            if ng.enable_query_cost {
                context_done(&ctx, ENV)?;
            }
            match pre {
                Ok(e) => {
                    qry.stmt.expr = e;
                    Ok(())
                }
                Err(e) => Err(Error::msg(e)),
            }
        })();
        if ng.enable_query_cost {
            if let Err(e) = &res {
                self.observe_query_rejection(&ctx, e);
            }
        }
        res.map(|_| qry)
    }

    fn new_query(
        &self,
        q: Arc<dyn Queryable>,
        qs: &str,
        opts: Option<&QueryOpts>,
        start: i64,
        end: i64,
        interval: i64,
    ) -> Result<Query, Error> {
        let ng = &*self.inner;
        let default_opts = QueryOpts::default();
        let opts = opts.unwrap_or(&default_opts);
        let mut lookback_delta = opts.lookback_delta;
        if lookback_delta.is_zero() {
            lookback_delta = ng.lookback_delta;
        }
        let use_start_timestamps = opts.use_start_timestamps.unwrap_or(ng.use_start_timestamps);
        let mut qry = Query {
            engine: self.clone(),
            queryable: q,
            q: qs.to_string(),
            stmt: EvalStmt { expr: Expr::default_placeholder(), start, end, interval, lookback_delta },
            stats: QueryTimers::new(),
            sample_stats: QuerySamples::new(ng.enable_per_step_stats && opts.enable_per_step_stats),
            matrix: Vec::new(),
            cancel: Arc::new(Mutex::new(None)),
            max_series: 0,
            max_samples_read: 0,
            max_query_duration: Duration::ZERO,
            deadline: None,
            use_start_timestamps,
            origin: Vec::new(),
            test_stmt: None,
        };
        if ng.enable_query_cost {
            let (ceil_series, ceil_samples_read, ceil_duration) = *ng.query_limits.read().unwrap();
            qry.max_series = resolve_uint_limit("max_series", opts.max_series, ceil_series)?;
            qry.max_samples_read = resolve_uint_limit("max_samples_read", opts.max_samples_read, ceil_samples_read)?;
            if !ceil_duration.is_zero() || !opts.max_query_duration.is_zero() {
                let ceiling = if ceil_duration.is_zero() { ng.timeout } else { ceil_duration };
                qry.max_query_duration =
                    resolve_duration_limit("max_query_duration", opts.max_query_duration, ceiling)?;
            }
            let timeout = if qry.max_query_duration.is_zero() { ng.timeout } else { qry.max_query_duration };
            qry.deadline = Some(Instant::now() + timeout);
        }
        Ok(qry)
    }

    /// Go `NewTestQuery`: a query that runs `f` instead of evaluating a statement.
    pub fn new_test_query(&self, f: impl Fn(&Context) -> Result<(), Error> + Send + Sync + 'static) -> Query {
        Query {
            engine: self.clone(),
            queryable: Arc::new(storage::QueryableFunc(|_, _| Ok(storage::noop_querier()))),
            q: "test statement".to_string(),
            stmt: EvalStmt { expr: Expr::default_placeholder(), start: 0, end: 0, interval: 0, lookback_delta: Duration::ZERO },
            stats: QueryTimers::new(),
            sample_stats: QuerySamples::new(self.inner.enable_per_step_stats),
            matrix: Vec::new(),
            cancel: Arc::new(Mutex::new(None)),
            max_series: 0,
            max_samples_read: 0,
            max_query_duration: Duration::ZERO,
            deadline: None,
            use_start_timestamps: false,
            origin: Vec::new(),
            test_stmt: Some(Arc::new(f)),
        }
    }

    /// Go `validateOpts`.
    fn validate_opts(&self, expr: &Expr) -> Result<(), Error> {
        let ng = &*self.inner;
        if ng.enable_at_modifier && ng.enable_negative_offset {
            return Ok(());
        }
        let mut at_used = false;
        let mut neg_used = false;
        parser::inspect(expr, |node, _| {
            let (ts, soe, off) = match node {
                Expr::VectorSelector(n) => (n.timestamp, n.start_or_end, n.original_offset),
                Expr::MatrixSelector(m) => match &m.vector_selector {
                    Expr::VectorSelector(n) => (n.timestamp, n.start_or_end, n.original_offset),
                    _ => (None, parser::ItemType(0), 0),
                },
                Expr::Subquery(n) => (n.timestamp, n.start_or_end, n.original_offset),
                _ => (None, parser::ItemType(0), 0),
            };
            if !matches!(node, Expr::VectorSelector(_) | Expr::MatrixSelector(_) | Expr::Subquery(_)) {
                // Go only updates the flags for selectors and subqueries, but checks after every node.
            } else {
                if ts.is_some() || soe == parser::START || soe == parser::END {
                    at_used = true;
                }
                if off < 0 {
                    neg_used = true;
                }
            }
            if at_used && !ng.enable_at_modifier {
                return Err(ERR_VALIDATION_AT_MODIFIER_DISABLED);
            }
            if neg_used && !ng.enable_negative_offset {
                return Err(ERR_VALIDATION_NEGATIVE_OFFSET_DISABLED);
            }
            Ok(())
        })
    }

    fn queue_active(&self, ctx: &Context, q: &str) -> Result<QuerySlot, Error> {
        self.acquire_query_slot(ctx, q)
    }

    fn observe_query_rejection(&self, ctx: &Context, err: &Error) {
        let ng = &*self.inner;
        if !ng.enable_query_cost {
            return;
        }
        let reason = match PromqlError::find(err) {
            Some(PromqlError::TooManySeries(_)) => Some("max_series"),
            Some(PromqlError::TooManySamplesRead(_)) => Some("max_samples_read"),
            Some(PromqlError::QueryTimeout(_)) if context_cause_is_cost_duration(ctx) => Some("max_duration"),
            _ => None,
        };
        if let Some(r) = reason {
            *ng.metrics.query_rejected_total.lock().unwrap().entry(r).or_insert(0) += 1;
        }
    }

    /// The query-cost rejections by reason.
    pub fn query_rejected_total(&self, reason: &str) -> u64 {
        self.inner.metrics.query_rejected_total.lock().unwrap().get(reason).copied().unwrap_or(0)
    }
}

thread_local! {
    static COST_DEADLINE_CAUSE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn context_cause_is_cost_duration(ctx: &Context) -> bool {
    ctx.err().is_some_and(|e| e.is(&ERR_DEADLINE_EXCEEDED)) && COST_DEADLINE_CAUSE.with(|c| c.get())
}

/// A reserved query slot; dropping it releases the slot.
pub struct QuerySlot {
    tracker: Option<Arc<dyn QueryTracker>>,
    index: usize,
}

impl Drop for QuerySlot {
    fn drop(&mut self) {
        if let Some(t) = &self.tracker {
            t.delete(self.index);
        }
    }
}

fn resolve_uint_limit(name: &str, requested: u64, ceiling: u64) -> Result<u64, Error> {
    if requested == 0 {
        return Ok(ceiling);
    }
    if ceiling == 0 || requested <= ceiling {
        return Ok(requested);
    }
    Err(PromqlError::LimitAboveCeiling {
        limit: name.to_string(),
        requested: requested.to_string(),
        ceiling: ceiling.to_string(),
    }
    .err())
}

fn resolve_duration_limit(name: &str, requested: Duration, ceiling: Duration) -> Result<Duration, Error> {
    if requested.is_zero() {
        return Ok(ceiling);
    }
    if ceiling.is_zero() || requested <= ceiling {
        return Ok(requested);
    }
    Err(PromqlError::LimitAboveCeiling {
        limit: name.to_string(),
        requested: stats::go_duration_string(requested),
        ceiling: stats::go_duration_string(ceiling),
    }
    .err())
}

fn uint64_to_int64_limit(v: u64) -> i64 {
    if v > i64::MAX as u64 { i64::MAX } else { v as i64 }
}

// -----------------------------------------------------------------------------------------------------------------
// Queries.

/// An evaluation statement (Go `parser.EvalStmt`). Times and interval are milliseconds.
#[derive(Clone, Debug)]
pub struct EvalStmt {
    pub expr: Expr,
    pub start: i64,
    pub end: i64,
    pub interval: i64,
    pub lookback_delta: Duration,
}

type TestStmt = Arc<dyn Fn(&Context) -> Result<(), Error> + Send + Sync>;

/// A query to run on the engine (Go `Query`).
pub struct Query {
    engine: Engine,
    queryable: Arc<dyn Queryable>,
    q: String,
    stmt: EvalStmt,
    stats: QueryTimers,
    sample_stats: QuerySamples,
    matrix: Matrix,
    cancel: Arc<Mutex<Option<CancelToken>>>,
    max_series: u64,
    max_samples_read: u64,
    max_query_duration: Duration,
    deadline: Option<Instant>,
    use_start_timestamps: bool,
    origin: Vec<(String, LogValue)>,
    test_stmt: Option<TestStmt>,
}

/// Cancels a running query from another thread (Go `Query.Cancel`).
#[derive(Clone)]
pub struct QueryCanceler(Arc<Mutex<Option<CancelToken>>>);

impl QueryCanceler {
    pub fn cancel(&self) {
        if let Some(t) = &*self.0.lock().unwrap() {
            t.cancel();
        }
    }
}

impl Query {
    /// Go `Statement` (the evaluation statement).
    pub fn statement(&self) -> &EvalStmt {
        &self.stmt
    }

    /// Go `String`: the query string.
    pub fn query_string(&self) -> &str {
        &self.q
    }

    /// Go `Stats`.
    pub fn stats(&self) -> Statistics {
        Statistics { timers: self.stats.clone(), samples: Some(self.sample_stats.clone()) }
    }

    /// The sample statistics.
    pub fn sample_stats(&self) -> &QuerySamples {
        &self.sample_stats
    }

    /// Go `Cancel`: cancels the running execution.
    pub fn cancel(&self) {
        self.canceler().cancel();
    }

    /// A handle that cancels the running execution from another thread.
    pub fn canceler(&self) -> QueryCanceler {
        QueryCanceler(self.cancel.clone())
    }

    /// Go `Close`: releases the result buffers.
    pub fn close(&mut self) {
        self.matrix = Vec::new();
    }

    /// Sets the origin data that the query logger logs (Go `NewOriginContext`).
    pub fn set_origin(&mut self, origin: Vec<(String, LogValue)>) {
        self.origin = origin;
    }

    fn execution_context(&self, ctx: &Context) -> (Context, CancelToken) {
        match self.deadline {
            None => ctx.with_timeout(self.engine.inner.timeout),
            Some(d) => {
                COST_DEADLINE_CAUSE.with(|c| c.set(!self.max_query_duration.is_zero()));
                ctx.with_deadline(d)
            }
        }
    }

    fn construction_error(&self, ctx: &Context, err: Error) -> Error {
        if self.deadline.is_some() {
            if let Err(e) = context_done(ctx, ENV) {
                return e;
            }
        }
        err
    }

    /// Go `Exec`: runs the query. Can only be called once.
    pub fn exec(&mut self, ctx: &Context) -> QueryResult {
        let (value, warnings, err) = self.exec_inner(ctx);
        QueryResult { err, value, warnings }
    }

    fn exec_inner(&mut self, ctx: &Context) -> (Option<Value>, Annotations, Option<Error>) {
        let engine = self.engine.clone();
        let ng = &*engine.inner;
        ng.metrics.current_queries.fetch_add(1, Ordering::Relaxed);
        let (ctx, cancel) = self.execution_context(ctx);
        *self.cancel.lock().unwrap() = Some(cancel.clone());

        self.stats.start(QueryTiming::ExecTotalTime);
        let (v, ws, err) = self.exec_timed(&ctx);
        self.stats.finish(QueryTiming::ExecTotalTime);
        cancel.cancel();

        // The query logger.
        {
            let g = ng.query_logger.read().unwrap();
            if let Some(l) = &*g {
                let mut params = vec![("query".to_string(), LogValue::Str(self.q.clone()))];
                if self.test_stmt.is_none() {
                    params.push(("start".to_string(), LogValue::Str(format_date(self.stmt.start))));
                    params.push(("end".to_string(), LogValue::Str(format_date(self.stmt.end))));
                    params.push(("step".to_string(), LogValue::Int(self.stmt.interval / 1000)));
                }
                let entry = QueryLogEntry {
                    params,
                    error: err.clone(),
                    stats: stats::new_query_stats(&self.stats()),
                    origin: self.origin.clone(),
                };
                l.log(&entry);
            }
        }

        ng.metrics.current_queries.fetch_sub(1, Ordering::Relaxed);
        ng.metrics.query_samples.fetch_add(self.sample_stats.total_samples.max(0) as u64, Ordering::Relaxed);
        ng.metrics.query_samples_read.fetch_add(self.sample_stats.samples_read.max(0) as u64, Ordering::Relaxed);
        if ng.enable_query_cost {
            if let Some(e) = &err {
                engine.observe_query_rejection(&ctx, e);
            }
        }
        (v, ws, err)
    }

    fn exec_timed(&mut self, ctx: &Context) -> (Option<Value>, Annotations, Option<Error>) {
        let engine = self.engine.clone();
        let ng = &*engine.inner;
        let _slot = match engine.queue_active_timed(ctx, &self.q, &mut self.stats) {
            Ok(s) => s,
            Err(e) => {
                let e = if ng.enable_query_cost { context_err(e, ENV) } else { e };
                return (None, Annotations::new(), Some(e));
            }
        };
        self.stats.start(QueryTiming::EvalTotalTime);
        let res: Result<(Option<Value>, Annotations), (Error, Annotations)> = (|| {
            if let Err(e) = context_done(ctx, ENV) {
                return Err((e, Annotations::new()));
            }
            if let Some(f) = &self.test_stmt {
                if let Err(e) = f(ctx) {
                    return Err((e, Annotations::new()));
                }
                return Ok((None, Annotations::new()));
            }
            match self.exec_eval_stmt(ctx) {
                Ok((v, ws)) => {
                    if ng.enable_query_cost {
                        if let Err(e) = context_done(ctx, "expression evaluation") {
                            return Err((e, ws));
                        }
                    }
                    Ok((Some(v), ws))
                }
                Err(e) => Err(e),
            }
        })();
        self.stats.finish(QueryTiming::EvalTotalTime);
        match res {
            Ok((v, ws)) => (v, ws, None),
            Err((e, ws)) => (None, ws, Some(e)),
        }
    }
}

impl Engine {
    fn queue_active_timed(&self, ctx: &Context, q: &str, timers: &mut QueryTimers) -> Result<QuerySlot, Error> {
        if self.inner.active_query_tracker.is_none() {
            return Ok(QuerySlot { tracker: None, index: 0 });
        }
        timers.start(QueryTiming::ExecQueueTime);
        let r = self.queue_active(ctx, q);
        timers.finish(QueryTiming::ExecQueueTime);
        r
    }
}

/// Go `formatDate`: RFC 3339 with milliseconds in UTC.
pub fn format_date(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let millis = ms.rem_euclid(1000);
    let s = storage::annotations::rfc3339_seconds(secs);
    // rfc3339_seconds ends with "Z".
    format!("{}.{:03}Z", s.trim_end_matches('Z'), millis)
}

/// A placeholder expression for statements that have no expression yet.
trait Placeholder {
    fn default_placeholder() -> Expr;
}

impl Placeholder for Expr {
    fn default_placeholder() -> Expr {
        Expr::NumberLiteral(Box::default())
    }
}

/// The result labels of absent functions (Go `createLabelsForAbsentFunction`), shared with the functions module.
pub fn labels_from_pairs(pairs: &[(&str, &str)]) -> Labels {
    let mut b = model::labels::ScratchBuilder::new(pairs.len());
    for (n, v) in pairs {
        b.add(n, v);
    }
    b.sort();
    b.labels()
}

/// Go `math.Pow` (Rust's `powf` differs in a few special cases).
pub fn go_pow(x: f64, y: f64) -> f64 {
    if y == 0.0 || x == 1.0 {
        return 1.0;
    }
    if y == 1.0 {
        return x;
    }
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 {
        if y < 0.0 {
            if is_odd_int(y) {
                return f64::INFINITY.copysign(x);
            }
            return f64::INFINITY;
        }
        if y > 0.0 {
            if is_odd_int(y) {
                return x;
            }
            return 0.0;
        }
    }
    if y.is_infinite() {
        if x == -1.0 {
            return 1.0;
        }
        if (x.abs() < 1.0) == (y > 0.0) {
            return 0.0;
        }
        return f64::INFINITY;
    }
    x.powf(y)
}

fn is_odd_int(x: f64) -> bool {
    if x.abs() >= (1u64 << 53) as f64 {
        return false;
    }
    let xi = x.trunc();
    xi == x && (xi as i64) & 1 == 1
}

/// Go `math.Mod`.
pub fn go_mod(x: f64, y: f64) -> f64 {
    x % y
}

/// Go `evaluator.subqueryTimeRange` for an evaluator over `[start, end]` with `interval` (ms), after
/// `setOffsetForAtModifier(start, expr)`. For tests of the subquery time grid.
#[doc(hidden)]
pub fn subquery_time_range_for(
    start: i64,
    end: i64,
    interval: i64,
    expr: &Expr,
    no_step: &(dyn Fn(i64) -> i64 + Send + Sync),
) -> Option<(i64, i64, i64)> {
    let Expr::Subquery(sq) = expr else { return None };
    let state = eval::QueryState::default();
    set_offset_for_at_modifier(start, expr, &state);
    let ctx = Context::background();
    let querier = storage::noop_querier();
    let ev = Evaluator {
        start_timestamp: start,
        end_timestamp: end,
        interval,
        max_samples: 0,
        current_samples: 0,
        budget: None,
        lookback_delta: 0,
        samples_stats: QuerySamples::new(false),
        no_step_subquery_interval_fn: no_step,
        enable_delayed_name_removal: false,
        enable_type_and_unit_labels: false,
        use_start_timestamps: false,
        querier: &*querier,
        ctx: &ctx,
        state: &state,
    };
    Some(ev.subquery_time_range(sq))
}
