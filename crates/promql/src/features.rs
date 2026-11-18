//! The part of `util/features` that PromQL uses: a registry of enabled features by category.

use std::collections::HashMap;
use std::sync::Mutex;

pub const API: &str = "api";
pub const PROMQL: &str = "promql";
pub const PROMQL_FUNCTIONS: &str = "promql_functions";
pub const PROMQL_OPERATORS: &str = "promql_operators";

/// Collects features and their enabled state (Go `features.Collector`).
pub trait Collector {
    /// Sets a feature to the given state. Category and name use snake_case.
    fn set(&self, category: &str, name: &str, enabled: bool);

    /// Marks a feature as enabled.
    fn enable(&self, category: &str, name: &str) {
        self.set(category, name, true);
    }

    /// Marks a feature as disabled.
    fn disable(&self, category: &str, name: &str) {
        self.set(category, name, false);
    }

    /// A copy of all features by category.
    fn get(&self) -> HashMap<String, HashMap<String, bool>>;
}

/// A thread-safe feature registry (Go `NewRegistry`).
#[derive(Debug, Default)]
pub struct Registry {
    features: Mutex<HashMap<String, HashMap<String, bool>>>,
}

impl Registry {
    pub fn new() -> Registry {
        Registry::default()
    }
}

impl Collector for Registry {
    fn set(&self, category: &str, name: &str, enabled: bool) {
        let mut f = self.features.lock().unwrap_or_else(|e| e.into_inner());
        f.entry(category.to_string()).or_default().insert(name.to_string(), enabled);
    }

    fn get(&self) -> HashMap<String, HashMap<String, bool>> {
        self.features.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}
