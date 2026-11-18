//! Port of `promql/info.go`: the `info` function.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use model::labels::{Labels, METRIC_NAME, MatchType, Matcher, ScratchBuilder};
use model::schema;
use storage::{Annotations, Error, SelectHints, SeriesArc};

use crate::engine::eval::{CursorMatrix, EvalResult, errorf, err_too_many_samples};
use crate::engine::{EvalNodeHelper, Evaluator, context_done};
use crate::parser::{Expr, ValueType};
use crate::value::*;

const TARGET_INFO: &str = "target_info";

/// The labels considered as identifying for info metrics.
const IDENTIFYING_LABELS: [&str; 2] = ["instance", "job"];

fn dur_ms(ns: i64) -> i64 {
    ns / 1_000_000
}

#[derive(Clone, Copy, Debug, Default)]
struct InfoSeriesReference {
    timestamp: Option<i64>,
    offset: i64,
}

impl InfoSeriesReference {
    fn equal(&self, other: &InfoSeriesReference) -> bool {
        match (self.timestamp, other.timestamp) {
            (Some(a), Some(b)) => a - dur_ms(self.offset) == b - dur_ms(other.offset),
            (None, None) => dur_ms(self.offset) == dur_ms(other.offset),
            _ => false,
        }
    }
}

/// Go `infoSelectTimestampAndOffset`: the first vector-producing selector's reference (@ timestamp and offset in
/// nanoseconds) and whether every vector-producing path has a selector with the same effective reference time.
pub fn info_select_timestamp_and_offset(expr: &Expr) -> (Option<i64>, i64, bool) {
    struct St {
        first: InfoSeriesReference,
        found: bool,
        reference_free: bool,
        uniform: bool,
    }
    fn inspect<'a>(expr: &'a Expr, path: &mut Vec<&'a Expr>, st: &mut St) -> bool {
        let t = expr.value_type();
        if t != ValueType::Vector && t != ValueType::Matrix {
            return false;
        }
        if let Expr::VectorSelector(n) = expr {
            let mut r = InfoSeriesReference { timestamp: n.timestamp, offset: n.original_offset };
            let mut i = path.len();
            while r.timestamp.is_none() && i > 0 {
                i -= 1;
                if let Expr::Subquery(sq) = path[i] {
                    r.offset += sq.original_offset;
                    r.timestamp = sq.timestamp;
                }
            }
            if !st.found {
                st.first = r;
                st.found = true;
            } else if !st.first.equal(&r) {
                st.uniform = false;
            }
            return true;
        }
        path.push(expr);
        let res = if let Expr::Call(c) = expr
            && c.func.name == "info"
        {
            inspect(&c.args[0], path, st)
        } else {
            let mut has_selector = false;
            expr.for_each_child(|child| {
                if inspect(child, path, st) {
                    has_selector = true;
                }
                true
            });
            if !has_selector {
                st.reference_free = true;
            }
            has_selector
        };
        path.pop();
        res
    }
    let mut st = St { first: InfoSeriesReference::default(), found: false, reference_free: false, uniform: true };
    inspect(expr, &mut Vec::new(), &mut st);
    if !st.found {
        return (None, 0, false);
    }
    (st.first.timestamp, st.first.offset, st.uniform && !st.reference_free)
}

/// Go `infoSeriesSelectTimestampAndOffset`.
pub fn info_series_select_timestamp_and_offset(expr: &Expr) -> (Option<i64>, i64) {
    let (ts, off, uniform) = info_select_timestamp_and_offset(expr);
    if !uniform {
        return (None, 0);
    }
    (ts, off)
}

/// Go `effectiveInfoNameMatchers`.
fn effective_info_name_matchers(matchers: &[Matcher]) -> Vec<Matcher> {
    if matchers.iter().any(|m| m.typ == MatchType::Equal || m.typ == MatchType::Regexp) {
        return matchers.to_vec();
    }
    if !matchers.is_empty() {
        let mut v = vec![Matcher::must_new(MatchType::Regexp, METRIC_NAME, ".+_info")];
        v.extend(matchers.iter().cloned());
        return v;
    }
    vec![Matcher::must_new(MatchType::Equal, METRIC_NAME, TARGET_INFO)]
}

/// Data label matchers by label name, in a stable order.
type DataLabelMatchers = BTreeMap<String, Vec<Matcher>>;

/// Go `evalInfo`.
pub(crate) fn eval_info(ev: &mut Evaluator<'_>, args: &[Expr]) -> EvalResult<(Value, Annotations)> {
    let (node_ts, offset) = info_series_select_timestamp_and_offset(&args[0]);
    let (mat, mut annots) = ev.eval_matrix(&args[0])?;
    let mut data_label_matchers: DataLabelMatchers = BTreeMap::new();
    let mut info_name_matchers = Vec::new();
    if args.len() > 1 {
        let Expr::VectorSelector(sel) = &args[1] else {
            return Err(errorf("info: second argument must be a label selector"));
        };
        for m in &sel.label_matchers {
            data_label_matchers.entry(m.name.clone()).or_default().push(m.clone());
            if m.name == METRIC_NAME {
                info_name_matchers.push(m.clone());
            }
        }
    } else {
        info_name_matchers.push(Matcher::must_new(MatchType::Equal, METRIC_NAME, TARGET_INFO));
    }
    let effective = effective_info_name_matchers(&info_name_matchers);
    let mut ignore_series: HashSet<u64> = HashSet::new();
    for s in &mat {
        let name = s.metric.get(METRIC_NAME);
        if effective.iter().all(|m| m.matches(name)) {
            ignore_series.insert(s.metric.hash());
        }
    }
    let hints = info_select_hints(ev, node_ts, offset);
    let (info_mat, ws) = fetch_info_series(ev, &mat, &ignore_series, &mut data_label_matchers, &hints, node_ts, offset)?;
    annots.merge(ws);
    let (res, ws) = combine_with_info_series(ev, mat, info_mat, &ignore_series, &data_label_matchers)?;
    annots.merge(ws);
    Ok((Value::Matrix(res), annots))
}

/// Go `infoSelectHints`.
fn info_select_hints(ev: &Evaluator<'_>, node_ts: Option<i64>, offset: i64) -> SelectHints {
    let off = dur_ms(offset);
    let (mut start, mut end) = (ev.start_timestamp, ev.end_timestamp);
    if let Some(t) = node_ts {
        start = t;
        end = t;
    }
    start -= dur_ms(ev.lookback_delta) - 1;
    start -= off;
    end -= off;
    SelectHints { start, end, step: ev.interval, func: "info".to_string(), ..Default::default() }
}

/// Go `fetchInfoSeries`.
fn fetch_info_series(
    ev: &mut Evaluator<'_>,
    mat: &[Series],
    ignore_series: &HashSet<u64>,
    data_label_matchers: &mut DataLabelMatchers,
    hints: &SelectHints,
    at_timestamp: Option<i64>,
    offset: i64,
) -> EvalResult<(Matrix, Annotations)> {
    let remove_name = |dlm: &mut DataLabelMatchers| {
        for ms in dlm.values_mut() {
            ms.retain(|m| m.name != METRIC_NAME);
        }
        dlm.retain(|_, ms| !ms.is_empty());
    };
    let sets = info_identifying_matcher_sets(mat, ignore_series);
    if sets.is_empty() {
        remove_name(data_label_matchers);
        return Ok((Vec::new(), Annotations::new()));
    }
    let mut name_matchers = Vec::new();
    let mut data_matchers = Vec::new();
    for ms in data_label_matchers.values() {
        for m in ms {
            if m.name == METRIC_NAME {
                name_matchers.push(m.clone());
            } else {
                data_matchers.push(m.clone());
            }
        }
    }
    remove_name(data_label_matchers);
    let effective = effective_info_name_matchers(&name_matchers);
    let mut info_series: Vec<SeriesArc> = Vec::new();
    let mut warnings = Annotations::new();
    for ids in sets {
        let mut matchers = Vec::with_capacity(ids.len() + data_matchers.len() + effective.len());
        matchers.extend(ids);
        matchers.extend(data_matchers.iter().cloned());
        matchers.extend(effective.iter().cloned());
        let set = ev.querier.select(ev.ctx, false, Some(hints), &matchers);
        let (series, ws, err) = ev.expand_series_set_pub(set);
        warnings.merge(ws);
        if let Some(e) = err {
            return Err(e);
        }
        info_series.extend(series);
    }
    let info_mat = ev.eval_series(&info_series, offset, true, at_timestamp)?;
    Ok((info_mat, warnings))
}

/// Go `infoIdentifyingMatcherSets`.
fn info_identifying_matcher_sets(mat: &[Series], ignore_series: &HashSet<u64>) -> Vec<Vec<Matcher>> {
    const INSTANCE: usize = 0;
    const JOB: usize = 1;
    const JOB_PRESENT: u64 = 1;
    const INSTANCE_PRESENT: u64 = 2;
    let presence_bits = [INSTANCE_PRESENT, JOB_PRESENT];
    let mut groups: BTreeMap<u64, [BTreeSet<String>; 2]> = BTreeMap::new();
    for s in mat {
        if ignore_series.contains(&s.metric.hash()) {
            continue;
        }
        let values = [s.metric.get(IDENTIFYING_LABELS[INSTANCE]), s.metric.get(IDENTIFYING_LABELS[JOB])];
        let mut presence = 0;
        if !values[INSTANCE].is_empty() {
            presence |= INSTANCE_PRESENT;
        }
        if !values[JOB].is_empty() {
            presence |= JOB_PRESENT;
        }
        if presence == 0 {
            continue;
        }
        let g = groups.entry(presence).or_default();
        for (i, v) in values.iter().enumerate() {
            if !v.is_empty() {
                g[i].insert(v.to_string());
            }
        }
    }
    let mut sets = Vec::with_capacity(groups.len());
    for (presence, g) in groups {
        let mut matchers = Vec::with_capacity(2);
        for (i, name) in IDENTIFYING_LABELS.iter().enumerate() {
            if presence & presence_bits[i] == 0 {
                matchers.push(Matcher::must_new(MatchType::Equal, name, ""));
                continue;
            }
            let mut re = String::new();
            for (j, v) in g[i].iter().enumerate() {
                if j > 0 {
                    re.push('|');
                }
                re.push_str(&model::regexp::quote_meta(v));
            }
            matchers.push(Matcher::must_new(MatchType::Regexp, name, &re));
        }
        sets.push(matchers);
    }
    sets
}

/// Go `combineWithInfoSeries`.
fn combine_with_info_series(
    ev: &mut Evaluator<'_>,
    mat: Matrix,
    info_mat: Matrix,
    ignore_series: &HashSet<u64>,
    data_label_matchers: &DataLabelMatchers,
) -> EvalResult<(Matrix, Annotations)> {
    let mut lb = ScratchBuilder::new(0);
    let mut sig = |name: &str, lset: &Labels| -> Vec<u8> {
        lb.reset();
        lb.add(METRIC_NAME, name);
        let m = lset.match_labels(true, &IDENTIFYING_LABELS);
        for (n, v) in m.iter() {
            lb.add(n, v);
        }
        lb.sort();
        lb.labels().data().to_vec()
    };
    let mut info_metrics: BTreeSet<String> = BTreeSet::new();
    for is in &info_mat {
        info_metrics.insert(is.metric.get(METRIC_NAME).to_string());
    }
    let num_steps = ((ev.end_timestamp - ev.start_timestamp) / ev.interval) as usize + 1;
    let original_num_samples = ev.current_samples;
    let biggest_len = mat.len().max(info_mat.len());
    let mut enh = EvalNodeHelper { out: Vec::with_capacity(biggest_len), ..Default::default() };

    let mut base_sigs: HashMap<u64, Vec<(String, Vec<u8>)>> = HashMap::with_capacity(mat.len());
    for s in &mat {
        let sigs = info_metrics.iter().map(|n| (n.clone(), sig(n, &s.metric))).collect();
        base_sigs.insert(s.metric.hash(), sigs);
    }
    let mut info_sigs: HashMap<u64, Vec<u8>> = HashMap::with_capacity(info_mat.len());
    for s in &info_mat {
        let name = s.metric.get(METRIC_NAME).to_string();
        info_sigs.insert(s.metric.hash(), sig(&name, &s.metric));
    }

    let mut base = CursorMatrix::new(mat);
    let mut info = CursorMatrix::new(info_mat);
    let mut base_vec: Vector = Vec::new();
    let mut info_vec: Vector = Vec::new();
    let mut out_series: Vec<(Series, i64)> = Vec::new();
    let mut out_index: HashMap<u64, usize> = HashMap::new();
    let mut temp_num_samples = ev.current_samples;
    let mut right: HashMap<Vec<u8>, Sample> = HashMap::new();
    let mut ts = ev.start_timestamp;
    while ts <= ev.end_timestamp {
        context_done(ev.ctx, "expression evaluation")?;
        ev.current_samples = temp_num_samples;
        ev.gather_vector_pub(ts, &mut base, &mut base_vec)?;
        ev.gather_vector_pub(ts, &mut info, &mut info_vec)?;
        enh.ts = ts;
        enh.out.clear();
        combine_with_info_vector(&base_vec, &info_vec, ignore_series, &base_sigs, &info_sigs, &mut enh, data_label_matchers, &mut right)?;
        let n = vector_total_samples(&enh.out) as i64;
        ev.current_samples += n;
        temp_num_samples += n;
        ev.update_peak();
        if ev.current_samples > ev.max_samples {
            return Err(err_too_many_samples());
        }
        for sample in enh.out.drain(..) {
            let h = sample.metric.hash();
            match out_index.get(&h) {
                Some(&i) => {
                    let (ss, last) = &mut out_series[i];
                    if *last == ts {
                        return Err(errorf("vector cannot contain metrics with the same labelset"));
                    }
                    *last = ts;
                    push_point(ss, ts, sample.f, sample.h, num_steps);
                }
                None => {
                    let mut ss = Series::new(sample.metric);
                    ss.drop_name = sample.drop_name;
                    push_point(&mut ss, ts, sample.f, sample.h, num_steps);
                    out_index.insert(h, out_series.len());
                    out_series.push((ss, ts));
                }
            }
        }
        ts += ev.interval;
    }
    let output: Matrix = out_series.into_iter().map(|(s, _)| s).collect();
    ev.current_samples = original_num_samples + matrix_total_samples(&output) as i64;
    ev.update_peak();
    Ok((output, Annotations::new()))
}

fn push_point(ss: &mut Series, ts: i64, f: f64, h: Option<HistogramRef>, num_steps: usize) {
    match h {
        None => {
            if ss.floats.capacity() == 0 {
                ss.floats.reserve_exact(num_steps.min(5000));
            }
            ss.floats.push(FPoint { t: ts, f });
        }
        Some(h) => {
            if ss.histograms.capacity() == 0 {
                ss.histograms.reserve_exact(num_steps.min(5000));
            }
            ss.histograms.push(HPoint { t: ts, h });
        }
    }
}

/// Go `combineWithInfoVector`.
#[allow(clippy::too_many_arguments)]
fn combine_with_info_vector(
    base: &[Sample],
    info: &[Sample],
    ignore_series: &HashSet<u64>,
    base_sigs: &HashMap<u64, Vec<(String, Vec<u8>)>>,
    info_sigs: &HashMap<u64, Vec<u8>>,
    enh: &mut EvalNodeHelper,
    data_label_matchers: &DataLabelMatchers,
    right: &mut HashMap<Vec<u8>, Sample>,
) -> EvalResult<()> {
    if base.is_empty() {
        return Ok(());
    }
    right.clear();
    for s in info {
        if s.h.is_some() {
            return Err(errorf("info sample should be float"));
        }
        let orig_t = s.f as i64;
        let sig = info_sigs[&s.metric.hash()].clone();
        match right.get(&sig) {
            Some(existing) => {
                let existing_t = existing.f as i64;
                if existing_t < orig_t {
                    right.insert(sig, s.clone());
                } else if existing_t == orig_t {
                    return Err(errorf(format!(
                        "found duplicate series for info metric: existing {} @ {}, new {} @ {}",
                        existing.metric, existing_t, s.metric, orig_t
                    )));
                }
            }
            None => {
                right.insert(sig, s.clone());
            }
        }
    }
    for bs in base {
        let hash = bs.metric.hash();
        if ignore_series.contains(&hash) {
            enh.out.push(Sample { metric: bs.metric.clone(), f: bs.f, h: bs.h.clone(), t: 0, drop_name: bs.drop_name });
            continue;
        }
        let lb = enh.reset_builder(Labels::empty());
        let mut seen: HashSet<&str> = HashSet::new();
        if let Some(sigs) = base_sigs.get(&hash) {
            for (info_name, sig) in sigs {
                let Some(is) = right.get(sig) else { continue };
                if seen.contains(info_name.as_str()) {
                    continue;
                }
                let mut err = None;
                for (n, v) in is.metric.iter() {
                    if schema::is_metadata_label(n) {
                        continue;
                    }
                    if !data_label_matchers.is_empty() && !data_label_matchers.contains_key(n) {
                        continue;
                    }
                    let cur = lb.get(n);
                    if !cur.is_empty() && cur != v {
                        err = Some(Error::msg(format!("conflicting label: {}", n)));
                        break;
                    }
                    if bs.metric.has(n) {
                        continue;
                    }
                    lb.set(n, v);
                }
                if let Some(e) = err {
                    return Err(e);
                }
                seen.insert(info_name.as_str());
            }
        }
        let info_lbls = lb.labels();
        if seen.is_empty() {
            let all_match_empty = data_label_matchers.values().all(|ms| ms.iter().all(|m| m.matches("")));
            if !all_match_empty {
                continue;
            }
        }
        let lb = enh.reset_builder(bs.metric.clone());
        for (n, v) in info_lbls.iter() {
            lb.set(n, v);
        }
        let metric = lb.labels();
        enh.out.push(Sample { metric, f: bs.f, h: bs.h.clone(), t: 0, drop_name: bs.drop_name });
    }
    Ok(())
}
