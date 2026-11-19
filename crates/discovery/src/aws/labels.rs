//! Helpers for the AWS Query roles (RDS, ElastiCache): label tables over XML elements, and a bounded worker pool
//! (Go `errgroup` with `SetLimit`).
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use model::labelset::LabelSet;

use super::xml::Element;
use crate::cancel::CancelToken;

/// How the SDK types a field and how Go turns it into a label value.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Kind {
    /// `*string`: set when present.
    Str,
    /// An enum string: set when not empty.
    Enum,
    /// `*bool`: `strconv.FormatBool`.
    Bool,
    /// `*int32` / `*int64`: `strconv.Itoa`.
    Int,
    /// `*time.Time`: `Format(time.RFC3339)` (the parsed zone offset is kept).
    Time,
}

/// Go `strconv.ParseBool`.
pub fn go_parse_bool(s: &str) -> Option<bool> {
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// The label value of a field as Go formats it (None: the field is unset). Errors are the SDK's
/// deserialization failures.
pub fn value(e: &Element, path: &str, kind: Kind) -> Result<Option<String>, String> {
    let Some(t) = e.text_at(path) else { return Ok(None) };
    Ok(match kind {
        Kind::Str => Some(t.to_string()),
        Kind::Enum => (!t.is_empty()).then(|| t.to_string()),
        Kind::Bool => Some(go_parse_bool(t).ok_or_else(|| format!("expected Boolean to be of type *bool, got {t}"))?.to_string()),
        Kind::Int => Some(t.trim().parse::<i64>().map_err(|_| format!("strconv.ParseInt: parsing {}: invalid syntax", yaml::go_quote(t)))?.to_string()),
        Kind::Time => Some(crate::util::reformat_rfc3339(t).ok_or_else(|| format!("failed to parse timestamp {}", yaml::go_quote(t)))?),
    })
}

/// Sets `prefix + suffix` for every field of `table` that is set in `e`.
pub fn apply(e: &Element, prefix: &str, table: &[(&str, &str, Kind)], out: &mut LabelSet) -> Result<(), String> {
    for (suffix, path, kind) in table {
        if let Some(v) = value(e, path, *kind)? {
            out.insert(format!("{prefix}{suffix}"), v);
        }
    }
    Ok(())
}

/// Runs `f` for every item with at most `limit` running at once (Go `errgroup.WithContext` + `SetLimit`). The
/// first error cancels the context the others see and is returned.
pub fn run_limited<T: Sync, R: Send>(ctx: &CancelToken, items: &[T], limit: usize, f: impl Fn(&CancelToken, &T) -> Result<R, String> + Sync) -> Result<Vec<R>, String> {
    let ectx = ctx.child();
    let next = AtomicUsize::new(0);
    let results: Mutex<Vec<R>> = Mutex::new(Vec::new());
    let first_err: Mutex<Option<String>> = Mutex::new(None);
    let workers = limit.max(1).min(items.len());
    std::thread::scope(|s| {
        for _ in 0..workers {
            s.spawn(|| {
                loop {
                    let i = next.fetch_add(1, Ordering::SeqCst);
                    if i >= items.len() {
                        return;
                    }
                    // errgroup.Go still runs every function; a cancelled context makes the calls fail fast
                    match f(&ectx, &items[i]) {
                        Ok(r) => results.lock().unwrap_or_else(|p| p.into_inner()).push(r),
                        Err(e) => {
                            let mut g = first_err.lock().unwrap_or_else(|p| p.into_inner());
                            if g.is_none() {
                                *g = Some(e);
                                ectx.cancel();
                            }
                        }
                    }
                }
            });
        }
    });
    if let Some(e) = first_err.into_inner().unwrap_or_else(|p| p.into_inner()) {
        return Err(e);
    }
    Ok(results.into_inner().unwrap_or_else(|p| p.into_inner()))
}
