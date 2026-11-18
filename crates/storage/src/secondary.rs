//! Port of `storage/secondary.go`: a querier treated as best effort. Errors become warnings with an empty result,
//! and if any series set of the querier fails on its first `next`, all its sets return nothing (all or nothing).

use std::sync::{Arc, Mutex, MutexGuard};

use model::matcher::Matcher;

use crate::annotations::Annotations;
use crate::context::Context;
use crate::error::{Error, Sentinel};
use crate::generic::{GBox, GenericQuerier, NoopGenericSet, new_search_result_set_from_slice};
use crate::interface::*;
use crate::lazy::{LazyGenericSeriesSet, WarningsOnlySeriesSet};

static SECONDARY_SEARCH_UNSUPPORTED: Sentinel =
    Sentinel::new("search is not supported by a secondary storage backend; results may be incomplete");
/// Go `errSecondarySearchUnsupported`.
pub const ERR_SECONDARY_SEARCH_UNSUPPORTED: Error = Error::Sentinel(&SECONDARY_SEARCH_UNSUPPORTED);

enum Slot<T> {
    Set(GBox<T>),
    WarningsOnly(Annotations),
    Noop,
    Taken,
}

struct State<T> {
    once_done: bool,
    done: bool,
    async_sets: Vec<Slot<T>>,
}

/// Go `secondaryQuerier`.
pub(crate) struct SecondaryQuerier<T> {
    inner: Box<dyn GenericQuerier<T>>,
    state: Arc<Mutex<State<T>>>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

impl<T: Send + 'static> SecondaryQuerier<T> {
    pub(crate) fn new(inner: Box<dyn GenericQuerier<T>>) -> Self {
        SecondaryQuerier { inner, state: Arc::new(Mutex::new(State { once_done: false, done: false, async_sets: Vec::new() })) }
    }
}

fn init_sets<T: 'static>(state: &Mutex<State<T>>, curr: usize) -> (GBox<T>, bool) {
    let mut st = lock(state);
    if !st.once_done {
        st.once_done = true;
        // On the first init, check that every set's first Next succeeds, for consistent partial failures.
        let n = st.async_sets.len();
        for i in 0..n {
            let Slot::Set(set) = &mut st.async_sets[i] else { continue };
            if set.next() {
                continue;
            }
            let mut ws = set.warnings();
            if let Some(err) = set.err() {
                // One set failed: the current one returns the error as a warning, the others nothing.
                ws.add(err);
                st.async_sets[curr] = Slot::WarningsOnly(ws);
                for j in 0..n {
                    if j != curr {
                        st.async_sets[j] = Slot::Noop;
                    }
                }
                break;
            }
            // Exhausted set.
            st.async_sets[i] = Slot::WarningsOnly(ws);
        }
        st.done = true;
    }
    match std::mem::replace(&mut st.async_sets[curr], Slot::Taken) {
        Slot::Set(s) => (s, true),
        Slot::WarningsOnly(ws) => (Box::new(WarningsOnlySeriesSet(ws)), false),
        Slot::Noop => (Box::new(NoopGenericSet), false),
        Slot::Taken => panic!("secondaryQuerier: series set initialized twice"),
    }
}

impl<T: Send + 'static> GenericQuerier<T> for SecondaryQuerier<T>
where
    SecondaryQuerier<T>: Searcher,
{
    fn select(&self, ctx: &Context, sort: bool, hints: Option<&SelectHints>, matchers: &[Matcher]) -> GBox<T> {
        let curr = {
            let st = lock(&self.state);
            if st.done {
                panic!("secondaryQuerier: Select invoked after first Next of any returned SeriesSet was done");
            }
            st.async_sets.len()
        };
        let set = self.inner.select(ctx, sort, hints, matchers);
        lock(&self.state).async_sets.push(Slot::Set(set));
        let state = self.state.clone();
        Box::new(LazyGenericSeriesSet::new(Box::new(move || init_sets(&state, curr))))
    }

    fn label_values(&self, ctx: &Context, name: &str, hints: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        let (vals, mut w, err) = self.inner.label_values(ctx, name, hints, m);
        if let Some(err) = err {
            w.add(err);
            return (Vec::new(), w, None);
        }
        (vals, w, None)
    }

    fn label_names(&self, ctx: &Context, hints: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        let (names, mut w, err) = self.inner.label_names(ctx, hints, m);
        if let Some(err) = err {
            w.add(err);
            return (Vec::new(), w, None);
        }
        (names, w, None)
    }

    fn close(&mut self) -> Option<Error> {
        self.inner.close()
    }

    fn searcher(&self) -> Option<&dyn Searcher> {
        Some(self)
    }

    fn children(&self) -> Vec<&dyn Querier> {
        self.inner.children()
    }
}

impl<T: Send + 'static> Searcher for SecondaryQuerier<T> {
    fn search_label_names<'a>(
        &'a self,
        ctx: &Context,
        hints: Option<&SearchHints>,
        matchers: &[Matcher],
    ) -> BoxSearchResultSet<'a> {
        match self.inner.searcher() {
            None => new_search_result_set_from_slice(Vec::new(), Annotations::new().with(ERR_SECONDARY_SEARCH_UNSUPPORTED)),
            Some(s) => warnings_on_error_search_result_set(s.search_label_names(ctx, hints, matchers)),
        }
    }

    fn search_label_values<'a>(
        &'a self,
        ctx: &Context,
        name: &str,
        hints: Option<&SearchHints>,
        matchers: &[Matcher],
    ) -> BoxSearchResultSet<'a> {
        match self.inner.searcher() {
            None => new_search_result_set_from_slice(Vec::new(), Annotations::new().with(ERR_SECONDARY_SEARCH_UNSUPPORTED)),
            Some(s) => warnings_on_error_search_result_set(s.search_label_values(ctx, name, hints, matchers)),
        }
    }
}

/// Go `warningsOnErrorSearchSet`: an iteration error becomes a warning.
struct WarningsOnErrorSearchSet<'a> {
    rs: Option<BoxSearchResultSet<'a>>,
    warnings: Annotations,
}

fn warnings_on_error_search_result_set(rs: BoxSearchResultSet<'_>) -> BoxSearchResultSet<'_> {
    Box::new(WarningsOnErrorSearchSet { rs: Some(rs), warnings: Annotations::new() })
}

static EMPTY_RESULT: SearchResult = SearchResult { value: String::new(), score: 0.0 };

impl SearchResultSet for WarningsOnErrorSearchSet<'_> {
    fn next(&mut self) -> bool {
        let Some(rs) = &mut self.rs else { return false };
        if rs.next() {
            return true;
        }
        if let Some(err) = rs.err() {
            let mut ws = Annotations::new();
            ws.merge(rs.warnings());
            ws.add(err);
            self.warnings = ws;
            let _ = rs.close();
            self.rs = None;
        }
        false
    }
    fn at(&self) -> &SearchResult {
        match &self.rs {
            Some(rs) => rs.at(),
            None => &EMPTY_RESULT,
        }
    }
    fn warnings(&self) -> Annotations {
        match &self.rs {
            Some(rs) => rs.warnings(),
            None => self.warnings.clone(),
        }
    }
    fn err(&self) -> Option<Error> {
        None
    }
    fn close(&mut self) -> Option<Error> {
        let mut rs = self.rs.take()?;
        self.warnings = rs.warnings();
        rs.close()
    }
}
