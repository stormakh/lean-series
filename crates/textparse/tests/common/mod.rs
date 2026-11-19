//! Helpers shared by the text parser tests (Go helpers of `promparse_test.go` and `openmetricsparse_test.go`).
#![allow(dead_code)]

use model::labels::{self, Builder, Labels};
use model::metadata::MetricType;
use textparse::testing::ParsedEntry;

/// `labels.FromStrings(...)`.
#[macro_export]
macro_rules! ls {
    ($($s:expr),* $(,)?) => {
        model::labels::from_strings(&[$($s),*] as &[&str])
    };
}

/// A `parsedEntry{...}` literal: unset fields keep their zero values; string fields take `&str`.
#[macro_export]
macro_rules! pe {
    (@v m $e:expr) => { Vec::<u8>::from($e) };
    (@v help $e:expr) => { String::from($e) };
    (@v unit $e:expr) => { String::from($e) };
    (@v comment $e:expr) => { String::from($e) };
    (@v $f:ident $e:expr) => { $e };
    ($($f:ident : $e:expr),* $(,)?) => {
        textparse::testing::ParsedEntry { $($f: $crate::pe!(@v $f $e),)* ..Default::default() }
    };
}

/// Go `typeAndUnitLabels`.
pub fn type_and_unit_labels(type_and_unit_enabled: bool, enabled: Labels, disabled: Labels) -> Labels {
    if type_and_unit_enabled {
        return enabled;
    }
    disabled
}

/// Go `todoDetectFamilySwitch`: with type and unit labels enabled, the parser knowingly inherits the type of the
/// previous metric family when a new family starts without a TYPE line.
pub fn todo_detect_family_switch(
    type_and_unit_enabled: bool,
    expected: Labels,
    broken_type_inherited: MetricType,
) -> Labels {
    if type_and_unit_enabled && broken_type_inherited != MetricType::Unknown {
        // Hack for now.
        let mut b = Builder::new(expected);
        b.set("__type__", broken_type_inherited.as_str());
        return b.labels();
    }
    expected
}

/// Go `resetValAndLset`.
pub fn reset_val_and_lset(e: &mut [ParsedEntry]) {
    for x in e.iter_mut() {
        x.v = 0.0;
        x.lset = labels::empty_labels();
    }
}
