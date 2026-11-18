//! Port of `storage/lazy.go`: a set that is created on the first `next`, and sets with only warnings or only an
//! error.

use crate::annotations::Annotations;
use crate::error::Error;
use crate::generic::{GBox, GenericSet};

pub(crate) type LazyInit<T> = Box<dyn FnOnce() -> (GBox<T>, bool) + Send>;

/// Go `lazyGenericSeriesSet`: `init` returns the set and the result of its first `next`.
pub(crate) struct LazyGenericSeriesSet<T> {
    init: Option<LazyInit<T>>,
    set: Option<GBox<T>>,
}

impl<T> LazyGenericSeriesSet<T> {
    pub(crate) fn new(init: LazyInit<T>) -> Self {
        LazyGenericSeriesSet { init: Some(init), set: None }
    }
}

impl<T> GenericSet<T> for LazyGenericSeriesSet<T> {
    fn next(&mut self) -> bool {
        if let Some(set) = &mut self.set {
            return set.next();
        }
        let init = self.init.take().expect("lazy series set initialized twice");
        let (set, ok) = init();
        self.set = Some(set);
        ok
    }
    fn at(&self) -> &T {
        match &self.set {
            Some(s) => s.at(),
            None => panic!("At called before Next on a lazy series set"),
        }
    }
    fn err(&self) -> Option<Error> {
        self.set.as_ref().and_then(|s| s.err())
    }
    fn warnings(&self) -> Annotations {
        self.set.as_ref().map(|s| s.warnings()).unwrap_or_default()
    }
}

/// Go `warningsOnlySeriesSet`.
pub(crate) struct WarningsOnlySeriesSet(pub Annotations);

impl<T> GenericSet<T> for WarningsOnlySeriesSet {
    fn next(&mut self) -> bool {
        false
    }
    fn at(&self) -> &T {
        panic!("At called on a warnings-only series set")
    }
    fn err(&self) -> Option<Error> {
        None
    }
    fn warnings(&self) -> Annotations {
        self.0.clone()
    }
}

/// Go `errorOnlySeriesSet`.
pub(crate) struct ErrorOnlySeriesSet(pub Error);

impl<T> GenericSet<T> for ErrorOnlySeriesSet {
    fn next(&mut self) -> bool {
        false
    }
    fn at(&self) -> &T {
        panic!("At called on an error-only series set")
    }
    fn err(&self) -> Option<Error> {
        Some(self.0.clone())
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}
