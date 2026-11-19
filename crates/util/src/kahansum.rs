//! Port of `util/kahansum`. The code lives in `model::histogram::kahansum`, because the histogram code needs it
//! and model cannot depend on this crate.
pub use model::histogram::kahansum::*;
