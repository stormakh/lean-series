//! The real retrievers over the rules and notifier crates: what Go's `web/api/v1` reads from `*rules.Manager`,
//! `*rules.Group`, `*rules.AlertingRule`, `*rules.RecordingRule`, `rules.Alert` and `*notifier.Manager`.
//!
//! - [`RulesManagerRetriever::new`] wraps a `rules::Manager` (Go `RulesRetriever` of the server).
//! - [`GroupView::new`] and [`RuleAdapter::new`] wrap single groups and rules (for retrievers built over groups
//!   that no manager holds, as in Go's `rulesRetrieverMock`).
//! - [`NotifierRetriever::new`] wraps a `notifier::Manager` (Go `AlertmanagerRetriever`).

use std::sync::Arc;
use std::time::Duration;

use model::labels::{Labels, Matcher};

use super::retrievers::{AlertingRuleView, AlertmanagerRetriever, RuleAlert, RuleGroupView, RuleRef, RuleView, RulesRetriever};
use crate::gotime::{GoDuration, Time};

/// Go `time.Time` of the rules crate to the API time (the zero time stays zero).
pub fn rules_time(t: rules::Time) -> Time {
    if t.is_zero() {
        Time::ZERO
    } else {
        Time::unix(0, t.unix_nano())
    }
}

fn go_duration(d: Duration) -> GoDuration {
    d.as_nanos().min(i64::MAX as u128) as GoDuration
}

/// One rule of the rules crate (alerting or recording) as the API reads it.
pub struct RuleAdapter(rules::RuleRef);

impl RuleAdapter {
    pub fn new(r: rules::RuleRef) -> RuleAdapter {
        RuleAdapter(r)
    }

    /// Go's type switch: an alerting rule or a recording rule.
    pub fn rule_ref(r: rules::RuleRef) -> RuleRef {
        if r.as_alerting().is_some() {
            RuleRef::Alerting(Arc::new(RuleAdapter(r)))
        } else {
            RuleRef::Recording(Arc::new(RuleAdapter(r)))
        }
    }

    fn alerting(&self) -> &rules::AlertingRule {
        self.0.as_alerting().expect("alerting rule")
    }
}

impl RuleView for RuleAdapter {
    fn name(&self) -> String {
        self.0.name().to_string()
    }
    fn query(&self) -> String {
        self.0.query().map(|e| e.to_string()).unwrap_or_default()
    }
    fn labels(&self) -> Labels {
        self.0.labels().clone()
    }
    fn health(&self) -> String {
        self.0.health().as_str().to_string()
    }
    fn last_error(&self) -> Option<String> {
        self.0.last_error().map(|e| e.to_string())
    }
    fn evaluation_duration(&self) -> GoDuration {
        go_duration(self.0.get_evaluation_duration())
    }
    fn evaluation_timestamp(&self) -> Time {
        rules_time(self.0.get_evaluation_timestamp())
    }
}

impl AlertingRuleView for RuleAdapter {
    fn state(&self) -> String {
        self.alerting().state().as_str().to_string()
    }
    fn hold_duration(&self) -> GoDuration {
        go_duration(self.alerting().hold_duration())
    }
    fn keep_firing_for(&self) -> GoDuration {
        go_duration(self.alerting().keep_firing_for())
    }
    fn annotations(&self) -> Labels {
        self.alerting().annotations().clone()
    }
    fn active_alerts(&self) -> Vec<RuleAlert> {
        self.alerting()
            .active_alerts()
            .into_iter()
            .map(|a| RuleAlert {
                labels: a.labels,
                annotations: a.annotations,
                state: a.state.as_str().to_string(),
                active_at: rules_time(a.active_at),
                keep_firing_since: rules_time(a.keep_firing_since),
                value: a.value,
            })
            .collect()
    }
}

/// A rule group of the rules crate as the API reads it.
pub struct GroupView(Arc<rules::Group>);

impl GroupView {
    pub fn new(g: Arc<rules::Group>) -> GroupView {
        GroupView(g)
    }
}

impl RuleGroupView for GroupView {
    fn name(&self) -> String {
        self.0.name().to_string()
    }
    fn file(&self) -> String {
        self.0.file().to_string()
    }
    fn interval(&self) -> GoDuration {
        go_duration(self.0.interval())
    }
    fn limit(&self) -> i64 {
        self.0.limit()
    }
    fn evaluation_time(&self) -> GoDuration {
        go_duration(self.0.get_evaluation_time())
    }
    fn last_evaluation(&self) -> Time {
        rules_time(self.0.get_last_evaluation())
    }
    fn rules(&self, matcher_sets: &[Vec<Matcher>]) -> Vec<RuleRef> {
        self.0.rules(matcher_sets).into_iter().map(RuleAdapter::rule_ref).collect()
    }
}

/// Go `RulesRetriever` over a `rules::Manager` (the server's `RuleGroups` and `AlertingRules`).
pub struct RulesManagerRetriever(Arc<rules::Manager>);

impl RulesManagerRetriever {
    pub fn new(m: Arc<rules::Manager>) -> RulesManagerRetriever {
        RulesManagerRetriever(m)
    }
}

impl RulesRetriever for RulesManagerRetriever {
    fn rule_groups(&self) -> Vec<Arc<dyn RuleGroupView>> {
        self.0.rule_groups().into_iter().map(|g| Arc::new(GroupView(g)) as Arc<dyn RuleGroupView>).collect()
    }
    fn alerting_rules(&self) -> Vec<Arc<dyn AlertingRuleView>> {
        self.0.alerting_rules().into_iter().map(|r| Arc::new(RuleAdapter(r)) as Arc<dyn AlertingRuleView>).collect()
    }
}

/// Go `AlertmanagerRetriever` over a `notifier::Manager`: the URLs as `url.URL.String()`.
pub struct NotifierRetriever(Arc<notifier::Manager>);

impl NotifierRetriever {
    pub fn new(m: Arc<notifier::Manager>) -> NotifierRetriever {
        NotifierRetriever(m)
    }
}

impl AlertmanagerRetriever for NotifierRetriever {
    fn alertmanagers(&self) -> Vec<String> {
        self.0.alertmanagers().iter().map(|u| u.to_string()).collect()
    }
    fn dropped_alertmanagers(&self) -> Vec<String> {
        self.0.dropped_alertmanagers().iter().map(|u| u.to_string()).collect()
    }
}
