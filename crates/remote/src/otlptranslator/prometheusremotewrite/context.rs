//! Port of `context.go`.

use storage::{Context, Error};

/// Checks the context only every `n` calls, because checking it has a cost (Go `everyNTimes`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EveryNTimes {
    pub n: i32,
    pub i: i32,
    pub err: Option<Error>,
}

impl EveryNTimes {
    pub fn new(n: i32) -> EveryNTimes {
        EveryNTimes { n, i: 0, err: None }
    }

    /// Go `checkContext`: the context's error, checked every `n` calls and remembered once set.
    pub fn check_context(&mut self, ctx: &Context) -> Result<(), Error> {
        if let Some(e) = &self.err {
            return Err(e.clone());
        }
        self.i += 1;
        if self.i >= self.n {
            self.i = 0;
            self.err = ctx.err();
        }
        match &self.err {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }
}
