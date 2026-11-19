//! Port of `rules/recording.go`.

use std::any::Any;
use std::time::Duration;

use model::labels::{self, Builder, Labels};
use promql::parser::Expr;
use promql::Vector;
use storage::error::Sentinel;
use storage::{Context, Error};

use crate::rule::{new_origin_context, new_rule_detail, QueryFunc, Rule, RuleHealth, RuleRef, RuleState, Time};
use crate::rulefmt;

static DUPLICATE_RECORDING_LABEL_SET: Sentinel =
    Sentinel::new("vector contains metrics with the same labelset after applying rule labels");
/// Go `ErrDuplicateRecordingLabelSet`.
pub const ERR_DUPLICATE_RECORDING_LABEL_SET: Error = Error::Sentinel(&DUPLICATE_RECORDING_LABEL_SET);

/// Go `RecordingRule`: records its vector expression into new time series.
pub struct RecordingRule {
    name: String,
    vector: Option<Expr>,
    labels: Labels,
    state: RuleState,
}

impl RecordingRule {
    /// Go `NewRecordingRule`.
    pub fn new(name: &str, vector: impl Into<Option<Expr>>, lset: Labels) -> RecordingRule {
        RecordingRule { name: name.to_string(), vector: vector.into(), labels: lset, state: RuleState::default() }
    }
}

/// Go `vector.ContainsSameLabelset()`.
pub(crate) fn contains_same_labelset(v: &Vector) -> bool {
    promql::contains_same_labelset(v.iter().map(|s| &s.metric))
}

impl RecordingRule {
    /// The expression as text (Go `vector.String()`).
    pub fn expr_string(&self) -> String {
        self.vector.as_ref().map(|e| e.to_string()).unwrap_or_default()
    }
}

impl Rule for RecordingRule {
    fn name(&self) -> &str {
        &self.name
    }
    fn labels(&self) -> &Labels {
        &self.labels
    }

    fn eval(&self, ctx: &Context, query_offset: Duration, ts: Time, query: &QueryFunc, _external_url: Option<&str>, limit: i64) -> Result<Vector, Error> {
        let ctx = new_origin_context(ctx, new_rule_detail(self));
        let mut vector = query(&ctx, &self.expr_string(), ts.sub_dur(query_offset))?;

        // Override the metric name and labels.
        let mut lb = Builder::new(labels::empty_labels());
        for sample in vector.iter_mut() {
            lb.reset(sample.metric.clone());
            lb.set(labels::METRIC_NAME, &self.name);
            self.labels.range(|n, v| {
                lb.set(n, v);
            });
            sample.metric = lb.labels();
        }

        // Check that the rule does not produce identical metrics after applying labels.
        if contains_same_labelset(&vector) {
            return Err(ERR_DUPLICATE_RECORDING_LABEL_SET);
        }

        let num_series = vector.len() as i64;
        if limit > 0 && num_series > limit {
            return Err(Error::msg(format!("exceeded limit of {limit} with {num_series} series")));
        }

        self.set_health(RuleHealth::Good);
        self.set_last_error(None);
        Ok(vector)
    }

    fn string(&self) -> String {
        rulefmt::Rule { record: self.name.clone(), expr: self.expr_string(), labels: self.labels.map().into_iter().collect(), ..Default::default() }
            .to_yaml()
    }
    fn query(&self) -> Option<&Expr> {
        self.vector.as_ref()
    }
    fn set_last_error(&self, err: Option<Error>) {
        self.state.set_last_error(err)
    }
    fn last_error(&self) -> Option<Error> {
        self.state.last_error()
    }
    fn set_health(&self, h: RuleHealth) {
        self.state.set_health(h)
    }
    fn health(&self) -> RuleHealth {
        self.state.health()
    }
    fn set_evaluation_duration(&self, d: Duration) {
        self.state.set_evaluation_duration(d)
    }
    fn get_evaluation_duration(&self) -> Duration {
        self.state.evaluation_duration()
    }
    fn set_evaluation_timestamp(&self, t: Time) {
        self.state.set_evaluation_timestamp(t)
    }
    fn get_evaluation_timestamp(&self) -> Time {
        self.state.evaluation_timestamp()
    }
    fn set_dependent_rules(&self, rules: &[RuleRef]) {
        self.state.set_dependents(rules)
    }
    fn no_dependent_rules(&self) -> bool {
        self.state.no_dependents()
    }
    fn dependent_rules(&self) -> Vec<RuleRef> {
        self.state.dependents()
    }
    fn set_dependency_rules(&self, rules: &[RuleRef]) {
        self.state.set_dependencies(rules)
    }
    fn no_dependency_rules(&self) -> bool {
        self.state.no_dependencies()
    }
    fn dependency_rules(&self) -> Vec<RuleRef> {
        self.state.dependencies()
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
