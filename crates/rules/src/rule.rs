//! Port of `rules/rule.go` and `rules/origin.go`: the `Rule` interface, rule health, rule details carried in the
//! query context, and the small Go types the package needs (time.Time, an atomic error cell).

use std::any::Any;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use model::labels::Labels;
use promql::parser::Expr;
use promql::Vector;
use storage::{Context, Error};

/// Go `time.Time` as the rules package uses it: nanoseconds since the Unix epoch, with Go's zero time as a
/// distinct value (it sorts before every other time).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Time(i64);

const ZERO_NS: i64 = i64::MIN;
/// Go's zero time in Unix seconds (January 1, year 1 UTC).
const ZERO_UNIX: i64 = -62135596800;

impl Default for Time {
    fn default() -> Time {
        Time::ZERO
    }
}

impl Time {
    /// Go `time.Time{}`.
    pub const ZERO: Time = Time(ZERO_NS);

    /// Go `time.Unix(sec, nsec)`.
    pub const fn unix(sec: i64, nsec: i64) -> Time {
        Time(sec * 1_000_000_000 + nsec)
    }
    /// Go `time.Unix(0, ns)`.
    pub const fn from_unix_nano(ns: i64) -> Time {
        Time(ns)
    }
    /// Go `timestamp.Time(ms)`.
    pub const fn from_millis(ms: i64) -> Time {
        Time(ms * 1_000_000)
    }
    /// Go `time.Now()`.
    pub fn now() -> Time {
        Time::from_system(SystemTime::now())
    }
    pub fn from_system(t: SystemTime) -> Time {
        match t.duration_since(UNIX_EPOCH) {
            Ok(d) => Time(d.as_nanos() as i64),
            Err(e) => Time(-(e.duration().as_nanos() as i64)),
        }
    }
    /// The time as a `SystemTime`; `None` for the zero time.
    pub fn to_system(self) -> Option<SystemTime> {
        if self.is_zero() {
            return None;
        }
        Some(if self.0 >= 0 {
            UNIX_EPOCH + Duration::from_nanos(self.0 as u64)
        } else {
            UNIX_EPOCH - Duration::from_nanos(self.0.unsigned_abs())
        })
    }
    pub fn is_zero(self) -> bool {
        self.0 == ZERO_NS
    }
    /// Go `t.UnixNano()` (undefined for the zero time in Go; here the minimum).
    pub fn unix_nano(self) -> i64 {
        self.0
    }
    /// Go `t.Unix()`.
    pub fn unix_secs(self) -> i64 {
        if self.is_zero() {
            return ZERO_UNIX;
        }
        self.0.div_euclid(1_000_000_000)
    }
    /// Go `timestamp.FromTime(t)` (milliseconds).
    pub fn millis(self) -> i64 {
        if self.is_zero() {
            return ZERO_UNIX * 1000;
        }
        self.0.div_euclid(1_000_000)
    }
    /// Go `t.Add(d)` with `d` in nanoseconds.
    pub fn add_nanos(self, d: i64) -> Time {
        if self.is_zero() {
            // Adding to the zero time keeps it far in the past; only used with small values.
            return Time(ZERO_NS.saturating_add(d.max(1)));
        }
        Time(self.0.saturating_add(d))
    }
    /// Go `t.Add(d)`.
    pub fn add(self, d: Duration) -> Time {
        self.add_nanos(d.as_nanos() as i64)
    }
    /// Go `t.Add(-d)`.
    pub fn sub_dur(self, d: Duration) -> Time {
        self.add_nanos(-(d.as_nanos() as i64))
    }
    /// Go `t.Sub(u)` in nanoseconds (saturating).
    pub fn sub(self, u: Time) -> i64 {
        let a = if self.is_zero() { ZERO_UNIX as i128 * 1_000_000_000 } else { self.0 as i128 };
        let b = if u.is_zero() { ZERO_UNIX as i128 * 1_000_000_000 } else { u.0 as i128 };
        (a - b).clamp(i64::MIN as i128, i64::MAX as i128) as i64
    }
    /// Go `t.After(u)`.
    pub fn after(self, u: Time) -> bool {
        self > u
    }
    /// Go `t.Before(u)`.
    pub fn before(self, u: Time) -> bool {
        self < u
    }
    /// Go `t.UTC().Format(time.RFC3339Nano)`-like text for logs.
    pub fn format_rfc3339(self) -> String {
        let secs = self.unix_secs();
        let nanos = if self.is_zero() { 0 } else { self.0.rem_euclid(1_000_000_000) };
        let days = secs.div_euclid(86400);
        let rem = secs.rem_euclid(86400);
        let (y, m, d) = civil_from_days(days);
        let mut s = format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}", rem / 3600, rem % 3600 / 60, rem % 60);
        if nanos != 0 {
            let f = format!("{nanos:09}");
            s.push('.');
            s.push_str(f.trim_end_matches('0'));
        }
        s.push('Z');
        s
    }
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Go `time.Duration` as nanoseconds (it can be negative).
pub fn dur_nanos(d: Duration) -> i64 {
    d.as_nanos().min(i64::MAX as u128) as i64
}

/// Go `RuleHealth`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RuleHealth {
    Unknown,
    Good,
    Bad,
}

impl RuleHealth {
    pub fn as_str(self) -> &'static str {
        match self {
            RuleHealth::Unknown => "unknown",
            RuleHealth::Good => "ok",
            RuleHealth::Bad => "err",
        }
    }
}

impl std::fmt::Display for RuleHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Go `QueryFunc`: runs a PromQL instant query at a time.
pub type QueryFunc = Arc<dyn Fn(&Context, &str, Time) -> Result<Vector, Error> + Send + Sync>;

/// A shared rule (Go `Rule` interface value). Identity is the allocation (`Arc::ptr_eq`).
pub type RuleRef = Arc<dyn Rule>;

/// Go `Rule`: a recording or alerting rule.
pub trait Rule: Any + Send + Sync {
    fn name(&self) -> &str;
    /// The labels of the rule.
    fn labels(&self) -> &Labels;
    /// Evaluates the rule, including any associated recording or alerting actions.
    #[allow(clippy::too_many_arguments)]
    fn eval(
        &self,
        ctx: &Context,
        query_offset: Duration,
        evaluation_time: Time,
        query_func: &QueryFunc,
        external_url: Option<&str>,
        limit: i64,
    ) -> Result<Vector, Error>;
    /// A human-readable representation (YAML) of the rule.
    fn string(&self) -> String;
    /// The rule query expression (`None` only for test rules without one).
    fn query(&self) -> Option<&Expr>;
    fn set_last_error(&self, err: Option<Error>);
    fn last_error(&self) -> Option<Error>;
    fn set_health(&self, h: RuleHealth);
    fn health(&self) -> RuleHealth;
    fn set_evaluation_duration(&self, d: Duration);
    fn get_evaluation_duration(&self) -> Duration;
    fn set_evaluation_timestamp(&self, t: Time);
    fn get_evaluation_timestamp(&self) -> Time;
    /// Sets the rules which depend on the output of this rule.
    fn set_dependent_rules(&self, rules: &[RuleRef]);
    /// True if it is guaranteed that no other rule in the group depends on this one.
    fn no_dependent_rules(&self) -> bool;
    fn dependent_rules(&self) -> Vec<RuleRef>;
    /// Sets the rules on which this rule depends.
    fn set_dependency_rules(&self, rules: &[RuleRef]);
    /// True if it is guaranteed that this rule does not depend on any other rule of the group.
    fn no_dependency_rules(&self) -> bool;
    fn dependency_rules(&self) -> Vec<RuleRef>;
    /// For type checks (Go type switches on `*AlertingRule` / `*RecordingRule`).
    fn as_any(&self) -> &dyn Any;
    /// The type name (Go `%T`), for the panic of `new_rule_detail`.
    fn type_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

impl dyn Rule {
    /// Go `rule.(*AlertingRule)`.
    pub fn as_alerting(&self) -> Option<&crate::alerting::AlertingRule> {
        self.as_any().downcast_ref()
    }
    /// Go `rule.(*RecordingRule)`.
    pub fn as_recording(&self) -> Option<&crate::recording::RecordingRule> {
        self.as_any().downcast_ref()
    }
}

/// Whether two rule references are the same rule.
pub fn same_rule(a: &RuleRef, b: &RuleRef) -> bool {
    std::ptr::addr_eq(Arc::as_ptr(a), Arc::as_ptr(b))
}

/// The state that recording and alerting rules share: health, last error, evaluation time and duration, and the
/// dependency information (kept as weak references, since rules point at each other).
#[derive(Default)]
pub(crate) struct RuleState {
    health: Mutex<Option<RuleHealth>>,
    last_error: Mutex<Option<Error>>,
    evaluation_duration: Mutex<Duration>,
    evaluation_timestamp: Mutex<Time>,
    dependents: RwLock<Option<Vec<Weak<dyn Rule>>>>,
    dependencies: RwLock<Option<Vec<Weak<dyn Rule>>>>,
}

fn upgrade(v: &[Weak<dyn Rule>]) -> Vec<RuleRef> {
    v.iter().filter_map(|w| w.upgrade()).collect()
}

impl RuleState {
    pub(crate) fn set_last_error(&self, e: Option<Error>) {
        *self.last_error.lock().unwrap() = e;
    }
    pub(crate) fn last_error(&self) -> Option<Error> {
        self.last_error.lock().unwrap().clone()
    }
    pub(crate) fn set_health(&self, h: RuleHealth) {
        *self.health.lock().unwrap() = Some(h);
    }
    pub(crate) fn health(&self) -> RuleHealth {
        self.health.lock().unwrap().unwrap_or(RuleHealth::Unknown)
    }
    pub(crate) fn set_evaluation_duration(&self, d: Duration) {
        *self.evaluation_duration.lock().unwrap() = d;
    }
    pub(crate) fn evaluation_duration(&self) -> Duration {
        *self.evaluation_duration.lock().unwrap()
    }
    pub(crate) fn set_evaluation_timestamp(&self, t: Time) {
        *self.evaluation_timestamp.lock().unwrap() = t;
    }
    pub(crate) fn evaluation_timestamp(&self) -> Time {
        *self.evaluation_timestamp.lock().unwrap()
    }
    pub(crate) fn set_dependents(&self, rules: &[RuleRef]) {
        *self.dependents.write().unwrap() = Some(rules.iter().map(Arc::downgrade).collect());
    }
    pub(crate) fn no_dependents(&self) -> bool {
        match &*self.dependents.read().unwrap() {
            None => false,
            Some(v) => v.is_empty(),
        }
    }
    pub(crate) fn dependents(&self) -> Vec<RuleRef> {
        self.dependents.read().unwrap().as_deref().map(upgrade).unwrap_or_default()
    }
    pub(crate) fn set_dependencies(&self, rules: &[RuleRef]) {
        *self.dependencies.write().unwrap() = Some(rules.iter().map(Arc::downgrade).collect());
    }
    pub(crate) fn no_dependencies(&self) -> bool {
        match &*self.dependencies.read().unwrap() {
            None => false,
            Some(v) => v.is_empty(),
        }
    }
    pub(crate) fn dependencies(&self) -> Vec<RuleRef> {
        self.dependencies.read().unwrap().as_deref().map(upgrade).unwrap_or_default()
    }
}

// ---------------- origin.go ----------------

/// Go `KindAlerting`.
pub const KIND_ALERTING: &str = "alerting";
/// Go `KindRecording`.
pub const KIND_RECORDING: &str = "recording";

/// Go `RuleDetail`: what a query knows about the rule that runs it.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RuleDetail {
    pub name: String,
    pub query: String,
    pub labels: Labels,
    pub kind: String,
    /// True if it is guaranteed that no other rule of the group depends on this one.
    pub no_dependent_rules: bool,
    /// True if it is guaranteed that this rule depends on no other rule of the group.
    pub no_dependency_rules: bool,
}

/// Go `NewRuleDetail`. Panics for a rule that is neither an alerting nor a recording rule, as Go does.
pub fn new_rule_detail(r: &dyn Rule) -> RuleDetail {
    let kind = if r.as_alerting().is_some() {
        KIND_ALERTING
    } else if r.as_recording().is_some() {
        KIND_RECORDING
    } else {
        panic!("unknown rule type \"{}\"", r.type_name());
    };
    RuleDetail {
        name: r.name().to_string(),
        query: r.query().map(|q| q.to_string()).unwrap_or_default(),
        labels: r.labels().clone(),
        kind: kind.to_string(),
        no_dependent_rules: r.no_dependent_rules(),
        no_dependency_rules: r.no_dependency_rules(),
    }
}

/// Go `NewOriginContext`.
pub fn new_origin_context(ctx: &Context, rule: RuleDetail) -> Context {
    ctx.with_value(rule)
}

/// Go `FromOriginContext`: the rule detail of the context, or an empty one.
pub fn from_origin_context(ctx: &Context) -> RuleDetail {
    ctx.value::<RuleDetail>().cloned().unwrap_or_default()
}
