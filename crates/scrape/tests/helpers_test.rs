//! Port of upstream/scrape/helpers_test.go (TestSelectAppendable). The other helpers of that file live in
//! `scrape::testing` and in the test files that use them (manager_test.rs: setupSynctestManager & co).

use std::sync::Arc;

use util::teststorage::Appendable;

/// Go `selectedAppendable`: V1 or V2 of an appendable that implements both.
struct SelectedAppendable {
    use_v2: bool,
    app: Appendable,
}

impl SelectedAppendable {
    /// Go `V1`: the Appendable if V1 is selected, otherwise None.
    fn v1(&self) -> Option<Arc<dyn storage::Appendable>> {
        if self.use_v2 { None } else { Some(Arc::new(self.app.clone())) }
    }
    /// Go `V2`: the AppendableV2 if V2 is selected, otherwise None.
    fn v2(&self) -> Option<Arc<dyn storage::AppendableV2>> {
        if !self.use_v2 { None } else { Some(Arc::new(self.app.clone())) }
    }
}

/// Go `selectAppendable`.
fn select_appendable(app: Appendable, app_v2: bool) -> SelectedAppendable {
    SelectedAppendable { app, use_v2: app_v2 }
}

/// Go `foreachAppendable`.
fn foreach_appendable(mut f: impl FnMut(bool)) {
    for app_v2 in [false, true] {
        f(app_v2);
    }
}

#[test]
fn test_select_appendable() {
    let mut i = 0;
    foreach_appendable(|app_v2| {
        match i {
            0 => {
                assert!(!app_v2);
                let s = select_appendable(Appendable::new(), app_v2);
                assert!(s.v1().is_some());
                assert!(s.v2().is_none());
            }
            1 => {
                assert!(app_v2);
                let s = select_appendable(Appendable::new(), app_v2);
                assert!(s.v1().is_none());
                assert!(s.v2().is_some());
            }
            _ => panic!("too many iterations"),
        }
        i += 1;
    });
}
