//! Binary operators (Go `VectorAnd`, `VectorOr`, `VectorUnless`, `VectorBinop`, `VectorscalarBinop`,
//! `scalarBinop`, `vectorElemBinop`).

use std::sync::Arc;

use model::histogram::{CounterResetHint, ErrorKind, FloatHistogram};
use model::labels::{Labels, METRIC_NAME};
use model::schema::{self, METRIC_TYPE_LABEL, METRIC_UNIT_LABEL};
use storage::annotations::{self as annos, HistogramOperation, PositionRange, PROMQL_INFO, PROMQL_WARNING};
use storage::{Annotations, Error};

use super::eval::{EvalResult, drop_metadata, errorf};
use super::{EvalNodeHelper, EvalSeriesHelper, Evaluator, go_pow};
use crate::parser::{
    ADD, ATAN2, BinaryExpr, DIV, EQLC, GTE, GTR, ItemType, LSS, LTE, MOD, MUL, NEQ, POW, SUB, TRIM_LOWER,
    TRIM_UPPER, VectorMatchCardinality, VectorMatching,
};
use crate::value::*;

#[inline]
fn btos(b: bool) -> f64 {
    if b { 1.0 } else { 0.0 }
}

/// Whether the operation changes the meaning of the metric (Go `changesMetricSchema`).
pub(crate) fn changes_metric_schema(op: ItemType) -> bool {
    matches!(op, ADD | SUB | DIV | MUL | POW | MOD | ATAN2)
}

/// Go `scalarBinop`.
pub fn scalar_binop(op: ItemType, lhs: f64, rhs: f64) -> f64 {
    match op {
        ADD => lhs + rhs,
        SUB => lhs - rhs,
        MUL => lhs * rhs,
        DIV => lhs / rhs,
        POW => go_pow(lhs, rhs),
        MOD => lhs % rhs,
        EQLC => btos(lhs == rhs),
        NEQ => btos(lhs != rhs),
        GTR => btos(lhs > rhs),
        LSS => btos(lhs < rhs),
        GTE => btos(lhs >= rhs),
        LTE => btos(lhs <= rhs),
        ATAN2 => lhs.atan2(rhs),
        _ => panic!("operator {:?} not allowed for Scalar operations", op.to_string()),
    }
}

/// The result of [`vector_elem_binop`].
#[derive(Debug, Default)]
pub struct ElemBinopResult {
    pub f: f64,
    pub h: Option<HistogramRef>,
    pub keep: bool,
    pub info: Option<Error>,
    pub err: Option<Error>,
}

fn op_str(op: ItemType) -> String {
    op.to_string()
}

fn incompatible(l: &str, op: ItemType, r: &str, pos: PositionRange) -> ElemBinopResult {
    ElemBinopResult { err: Some(annos::new_incompatible_types_in_bin_op_info(l, &op_str(op), r, pos)), ..Default::default() }
}

fn new_h(h: FloatHistogram) -> Option<HistogramRef> {
    Some(Arc::new(h))
}

/// Go `vectorElemBinop`.
pub fn vector_elem_binop(
    op: ItemType,
    lhs: f64,
    rhs: f64,
    hlhs: Option<&HistogramRef>,
    hrhs: Option<&HistogramRef>,
    pos: PositionRange,
) -> ElemBinopResult {
    let ok = |f: f64, keep: bool| ElemBinopResult { f, keep, ..Default::default() };
    match (hlhs, hrhs) {
        (None, None) => match op {
            ADD => ok(lhs + rhs, true),
            SUB => ok(lhs - rhs, true),
            MUL => ok(lhs * rhs, true),
            DIV => ok(lhs / rhs, true),
            POW => ok(go_pow(lhs, rhs), true),
            MOD => ok(lhs % rhs, true),
            EQLC => ok(lhs, lhs == rhs),
            NEQ => ok(lhs, lhs != rhs),
            GTR => ok(lhs, lhs > rhs),
            LSS => ok(lhs, lhs < rhs),
            GTE => ok(lhs, lhs >= rhs),
            LTE => ok(lhs, lhs <= rhs),
            ATAN2 => ok(lhs.atan2(rhs), true),
            TRIM_LOWER | TRIM_UPPER => incompatible("float", op, "float", pos),
            _ => panic!("operator {:?} not allowed for operations between Vectors", op.to_string()),
        },
        (None, Some(hr)) => match op {
            MUL => {
                let mut h = (**hr).clone();
                h.mul(lhs).compact(0);
                ElemBinopResult { h: new_h(h), keep: true, ..Default::default() }
            }
            ADD | SUB | DIV | POW | MOD | EQLC | NEQ | GTR | TRIM_LOWER | TRIM_UPPER | LSS | GTE | LTE | ATAN2 => {
                incompatible("float", op, "histogram", pos)
            }
            _ => panic!("operator {:?} not allowed for operations between Vectors", op.to_string()),
        },
        (Some(hl), None) => match op {
            MUL => {
                let mut h = (**hl).clone();
                h.mul(rhs).compact(0);
                ElemBinopResult { h: new_h(h), keep: true, ..Default::default() }
            }
            DIV => {
                let mut h = (**hl).clone();
                h.div(rhs).compact(0);
                ElemBinopResult { h: new_h(h), keep: true, ..Default::default() }
            }
            TRIM_UPPER | TRIM_LOWER => {
                let mut h = (**hl).clone();
                h.trim_buckets(rhs, op == TRIM_UPPER);
                ElemBinopResult { h: new_h(h), keep: true, ..Default::default() }
            }
            ADD | SUB | POW | MOD | EQLC | NEQ | GTR | LSS | GTE | LTE | ATAN2 => {
                incompatible("histogram", op, "float", pos)
            }
            _ => panic!("operator {:?} not allowed for operations between Vectors", op.to_string()),
        },
        (Some(hl), Some(hr)) => match op {
            ADD | SUB => {
                let mut res = (**hl).clone();
                let r = if op == ADD { res.add(hr) } else { res.sub(hr) };
                let (collision, reconciled) = match r {
                    Ok(x) => x,
                    Err(e) => return ElemBinopResult { err: Some(e.into()), ..Default::default() },
                };
                let hop = if op == ADD { HistogramOperation::Add } else { HistogramOperation::Sub };
                if op == SUB {
                    res.counter_reset_hint = CounterResetHint::GaugeType;
                }
                let err = if collision { Some(annos::new_histogram_counter_reset_collision_warning(pos, hop)) } else { None };
                let info = if reconciled { Some(annos::new_mismatched_custom_buckets_histograms_info(pos, hop)) } else { None };
                res.compact(0);
                ElemBinopResult { h: new_h(res), keep: true, info, err, f: 0.0 }
            }
            EQLC => ElemBinopResult { h: Some(hl.clone()), keep: hl.equals(hr), ..Default::default() },
            NEQ => ElemBinopResult { h: Some(hl.clone()), keep: !hl.equals(hr), ..Default::default() },
            MUL | DIV | POW | MOD | GTR | LSS | GTE | LTE | ATAN2 | TRIM_LOWER | TRIM_UPPER => {
                incompatible("histogram", op, "histogram", pos)
            }
            _ => panic!("operator {:?} not allowed for operations between Vectors", op.to_string()),
        },
    }
}

/// Whether `err` is the histogram error `kind`.
pub(crate) fn is_histogram_error(err: &Error, kind: ErrorKind) -> bool {
    match err {
        Error::Chunk(c) => c.is_histogram(kind),
        Error::Wrapped(w) => is_histogram_error(&w.1, kind),
        _ => false,
    }
}

/// Go `handleVectorBinopError`: annotations for the error of a binary operation; invalid histograms fail the
/// query.
pub(crate) fn handle_vector_binop_error(err: Option<Error>, e: &BinaryExpr, pos: PositionRange) -> EvalResult<Annotations> {
    let Some(err) = err else {
        return Ok(Annotations::new());
    };
    let info = Error::Sentinel(&PROMQL_INFO);
    let warn = Error::Sentinel(&PROMQL_WARNING);
    if err.is(&info) || err.is(&warn) {
        return Ok(Annotations::new().with(err));
    }
    if is_histogram_error(&err, ErrorKind::HistogramsIncompatibleSchema) {
        return Ok(Annotations::new().with(annos::new_incompatible_bucket_layout_in_bin_op_warning(&op_str(e.op), pos)));
    }
    Err(err)
}

pub(crate) fn vector_and(
    lhs: &[Sample],
    rhs: &[Sample],
    matching: &VectorMatching,
    lhsh: &[EvalSeriesHelper],
    rhsh: &[EvalSeriesHelper],
    enh: &mut EvalNodeHelper,
) {
    if matching.card != VectorMatchCardinality::ManyToMany {
        panic!("set operations must only use many-to-many matching");
    }
    if lhs.is_empty() || rhs.is_empty() {
        return;
    }
    enh.reset_sigs_present();
    for sh in rhsh {
        enh.sigs_present[sh.sig_ordinal] = true;
    }
    for (i, ls) in lhs.iter().enumerate() {
        if enh.sigs_present[lhsh[i].sig_ordinal] {
            enh.out.push(ls.clone());
        }
    }
}

pub(crate) fn vector_or(
    lhs: &[Sample],
    rhs: &[Sample],
    matching: &VectorMatching,
    lhsh: &[EvalSeriesHelper],
    rhsh: &[EvalSeriesHelper],
    enh: &mut EvalNodeHelper,
) {
    if matching.card != VectorMatchCardinality::ManyToMany {
        panic!("set operations must only use many-to-many matching");
    }
    if lhs.is_empty() {
        enh.out.extend_from_slice(rhs);
        return;
    }
    if rhs.is_empty() {
        enh.out.extend_from_slice(lhs);
        return;
    }
    enh.reset_sigs_present();
    for (i, ls) in lhs.iter().enumerate() {
        enh.sigs_present[lhsh[i].sig_ordinal] = true;
        enh.out.push(ls.clone());
    }
    for (j, rs) in rhs.iter().enumerate() {
        if !enh.sigs_present[rhsh[j].sig_ordinal] {
            enh.out.push(rs.clone());
        }
    }
}

pub(crate) fn vector_unless(
    lhs: &[Sample],
    rhs: &[Sample],
    matching: &VectorMatching,
    lhsh: &[EvalSeriesHelper],
    rhsh: &[EvalSeriesHelper],
    enh: &mut EvalNodeHelper,
) {
    if matching.card != VectorMatchCardinality::ManyToMany {
        panic!("set operations must only use many-to-many matching");
    }
    if lhs.is_empty() || rhs.is_empty() {
        enh.out.extend_from_slice(lhs);
        return;
    }
    enh.reset_sigs_present();
    for sh in rhsh {
        enh.sigs_present[sh.sig_ordinal] = true;
    }
    for (i, ls) in lhs.iter().enumerate() {
        if !enh.sigs_present[lhsh[i].sig_ordinal] {
            enh.out.push(ls.clone());
        }
    }
}

/// Go `matchLabels`.
pub(crate) fn match_labels(metric: &Labels, on: bool, names: &[String]) -> Labels {
    if on {
        metric.match_labels(true, names)
    } else {
        let mut all: Vec<&str> = vec![METRIC_NAME, METRIC_TYPE_LABEL, METRIC_UNIT_LABEL];
        all.extend(names.iter().map(|s| s.as_str()));
        metric.match_labels(false, &all)
    }
}

/// Go `resultMetric`.
fn result_metric(
    lhs: &Labels,
    rhs: &Labels,
    op: ItemType,
    matching: &VectorMatching,
    drop_metric_name: bool,
    enh: &mut EvalNodeHelper,
) -> Labels {
    enh.lbl_result_buf.clear();
    enh.lbl_result_buf.extend_from_slice(lhs.data());
    enh.lbl_result_buf.extend_from_slice(rhs.data());
    if let Some(ret) = enh.result_metric.get(enh.lbl_result_buf.as_slice()) {
        return ret.clone();
    }
    let key = enh.lbl_result_buf.clone();
    let lb = enh.reset_builder(lhs.clone());
    if drop_metric_name || changes_metric_schema(op) {
        schema::Metadata::default().set_to_labels(lb);
    }
    if matching.card == VectorMatchCardinality::OneToOne {
        if matching.on {
            lb.keep(&matching.matching_labels);
        } else {
            lb.del_many(&matching.matching_labels);
        }
    }
    for ln in &matching.include {
        let v = rhs.get(ln);
        if !v.is_empty() {
            lb.set(ln, v);
        } else {
            lb.del(ln);
        }
    }
    let ret = lb.labels();
    enh.result_metric.insert(key, ret.clone());
    ret
}

/// Go `VectorBinop`: a binary operation between two vectors, except set operators. Returns the last error of
/// an element operation (an annotation or a histogram error), as Go does.
#[allow(clippy::too_many_arguments)]
pub(crate) fn vector_binop(
    ev: &Evaluator<'_>,
    op: ItemType,
    lhs: &[Sample],
    rhs: &[Sample],
    matching: &VectorMatching,
    return_bool: bool,
    lhsh: &[EvalSeriesHelper],
    rhsh: &[EvalSeriesHelper],
    enh: &mut EvalNodeHelper,
    pos: PositionRange,
) -> EvalResult<Option<Error>> {
    if matching.card == VectorMatchCardinality::ManyToMany {
        panic!("many-to-many only allowed for set operators");
    }
    let fills = matching.fill_values;
    if (lhs.is_empty() && rhs.is_empty())
        || ((lhs.is_empty() || rhs.is_empty()) && fills.rhs.is_none() && fills.lhs.is_none())
    {
        return Ok(None);
    }
    let one_to_many = matching.card == VectorMatchCardinality::OneToMany;
    let (lhs, rhs, lhsh, rhsh, fill_lhs, fill_rhs) =
        if one_to_many { (rhs, lhs, rhsh, lhsh, fills.rhs, fills.lhs) } else { (lhs, rhs, lhsh, rhsh, fills.lhs, fills.rhs) };

    enh.reset_right_sigs();
    enh.reset_sigs_present();
    for (i, rs) in rhs.iter().enumerate() {
        let sig = rhsh[i].sig_ordinal;
        if enh.sigs_present[sig] {
            let dupl = enh.right_sigs[sig].as_ref().unwrap();
            let one_side = if one_to_many { "left" } else { "right" };
            let matched = match_labels(&rs.metric, matching.on, &matching.matching_labels);
            let (mut d1, mut d2) = (rs.metric.to_string(), dupl.metric.to_string());
            if d1 > d2 {
                std::mem::swap(&mut d1, &mut d2);
            }
            return Err(errorf(format!(
                "found duplicate series for the match group {} on the {} hand-side of the operation: [{}, {}];many-to-many matching not allowed: matching labels must be unique on one side",
                matched, one_side, d1, d2
            )));
        }
        enh.right_sigs[sig] = Some(rs.clone());
        enh.sigs_present[sig] = true;
    }
    let one_to_one = matching.card == VectorMatchCardinality::OneToOne;
    if one_to_one {
        enh.reset_matched_sigs_present();
    } else {
        enh.reset_matched_sigs();
    }

    let mut last_err: Option<Error> = None;
    let drop_metric_name = !ev.enable_delayed_name_removal && return_bool;

    let mut do_bin_op = |ls: &Sample, rs: &Sample, sig: usize, enh: &mut EvalNodeHelper| -> EvalResult<()> {
        let (mut fl, mut fr) = (ls.f, rs.f);
        let (mut hl, mut hr) = (ls.h.as_ref(), rs.h.as_ref());
        if one_to_many {
            std::mem::swap(&mut fl, &mut fr);
            std::mem::swap(&mut hl, &mut hr);
        }
        let r = vector_elem_binop(op, fl, fr, hl, hr, pos);
        if let Some(e) = r.err {
            last_err = Some(e);
            return Ok(());
        }
        if let Some(i) = r.info {
            last_err = Some(i);
        }
        let (mut fv, mut hv) = (r.f, r.h);
        if return_bool {
            hv = None;
            fv = if r.keep { 1.0 } else { 0.0 };
        }
        let metric = result_metric(&ls.metric, &rs.metric, op, matching, drop_metric_name, enh);
        if one_to_one {
            if enh.matched_sigs_present[sig] {
                return Err(errorf("multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)"));
            }
            enh.matched_sigs_present[sig] = true;
        } else {
            let insert_sig = metric.hash();
            if !enh.matched_sigs[sig].insert(insert_sig) {
                return Err(errorf("multiple matches for labels: grouping labels must ensure unique matches"));
            }
        }
        if !r.keep && !return_bool {
            return Ok(());
        }
        enh.out.push(Sample { metric, f: fv, h: hv, t: 0, drop_name: return_bool });
        Ok(())
    };

    for (i, ls) in lhs.iter().enumerate() {
        let sig = lhsh[i].sig_ordinal;
        if enh.sigs_present[sig] {
            let rs = enh.right_sigs[sig].clone().unwrap();
            do_bin_op(ls, &rs, sig, enh)?;
        } else {
            let Some(fill) = fill_rhs else { continue };
            let rs = Sample { metric: match_labels(&ls.metric, matching.on, &matching.matching_labels), f: fill, ..Default::default() };
            do_bin_op(ls, &rs, sig, enh)?;
        }
    }
    if let Some(fill) = fill_lhs {
        for (i, rs) in rhs.iter().enumerate() {
            let sig = rhsh[i].sig_ordinal;
            if (one_to_one && enh.matched_sigs_present[sig]) || (!one_to_one && !enh.matched_sigs[sig].is_empty()) {
                continue;
            }
            let ls = Sample { metric: match_labels(&rs.metric, matching.on, &matching.matching_labels), f: fill, ..Default::default() };
            do_bin_op(&ls, rs, sig, enh)?;
        }
    }
    Ok(last_err)
}

/// Go `VectorscalarBinop`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn vector_scalar_binop(
    ev: &Evaluator<'_>,
    op: ItemType,
    lhs: &[Sample],
    rhs: f64,
    swap: bool,
    return_bool: bool,
    enh: &mut EvalNodeHelper,
    pos: PositionRange,
) -> Option<Error> {
    let mut last_err = None;
    let warn = Error::Sentinel(&PROMQL_WARNING);
    for s in lhs {
        let (mut lf, mut rf) = (s.f, rhs);
        let (mut lh, mut rh): (Option<&HistogramRef>, Option<&HistogramRef>) = (s.h.as_ref(), None);
        if swap {
            std::mem::swap(&mut lf, &mut rf);
            std::mem::swap(&mut lh, &mut rh);
        }
        let r = vector_elem_binop(op, lf, rf, lh, rh, pos);
        if let Some(e) = r.err {
            if !e.is(&warn) {
                last_err = Some(e);
                continue;
            }
        }
        let (mut f, mut h, mut keep) = (r.f, r.h, r.keep);
        if op.is_comparison_operator() && swap {
            f = rf;
            h = rh.cloned();
        }
        if return_bool {
            f = if keep { 1.0 } else { 0.0 };
            keep = true;
        }
        if keep {
            let mut out = s.clone();
            out.f = f;
            out.h = h;
            if changes_metric_schema(op) || return_bool {
                if !ev.enable_delayed_name_removal {
                    out.metric = drop_metadata(&out.metric);
                }
                out.drop_name = true;
            }
            enh.out.push(out);
        }
    }
    last_err
}
