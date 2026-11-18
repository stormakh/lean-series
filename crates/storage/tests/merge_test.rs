//! Port of `storage/merge_test.go`.

use std::sync::{Arc, Mutex};

use chunkenc::{AnyIterator, ChunkIterator, Encoding, ValueType};
use model::histogram::{CounterResetHint, FloatHistogram, Histogram};
use model::labels::{self, Labels};
use model::matcher::{MatchType, Matcher};
use storage::chunks::{chunk_from_samples, chunk_metas_to_samples, generate_samples};
use storage::tsdbutil::{generate_test_float_histogram, generate_test_histogram};
use storage::*;

// ---------------------------------------------------------------------------------------------------------------
// Helpers.

fn ls(s: &[&str]) -> Labels {
    labels::from_strings(s)
}

fn fs(st: i64, t: i64, f: f64) -> Sample {
    Sample::float(st, t, f)
}

/// Go `histogramSample`: a histogram that is unique to the ts.
fn histogram_sample(ts: i64, hint: CounterResetHint) -> Sample {
    let mut h = generate_test_histogram(ts + 1);
    h.counter_reset_hint = hint;
    Sample::histogram(0, ts, h)
}

/// Go `floatHistogramSample`.
fn float_histogram_sample(ts: i64, hint: CounterResetHint) -> Sample {
    let mut fh = generate_test_float_histogram(ts + 1);
    fh.counter_reset_hint = hint;
    Sample::float_histogram(0, ts, fh)
}

// Shorthands for counter reset hints.
const UK: CounterResetHint = CounterResetHint::UnknownCounterReset;
const CR: CounterResetHint = CounterResetHint::CounterReset;
const NR: CounterResetHint = CounterResetHint::NotCounterReset;
const GA: CounterResetHint = CounterResetHint::GaugeType;

fn list(lset: Labels, s: Vec<Sample>) -> SeriesArc {
    new_list_series(lset, s)
}

/// Go `NewListChunkSeriesFromSamples(lset, samples...)`.
fn lcs(lset: Labels, samples: &[Vec<Sample>]) -> ChunkSeriesArc {
    let refs: Vec<&[Sample]> = samples.iter().map(|v| v.as_slice()).collect();
    new_list_chunk_series_from_samples(lset, &refs)
}

fn chained() -> VerticalSeriesMergeFunc {
    Arc::new(chained_series_merge)
}

/// Go `require.Equal` on `[]chunks.Meta`: the references, time ranges and the chunk contents.
fn assert_metas_eq(exp: &[Meta], act: &[Meta], msg: &str) {
    assert_eq!(exp.len(), act.len(), "{}: number of chunks", msg);
    for (i, (e, a)) in exp.iter().zip(act).enumerate() {
        assert_eq!(e.chunk_ref, a.chunk_ref, "{}: chunk {} ref", msg, i);
        assert_eq!(e.min_time, a.min_time, "{}: chunk {} min time", msg, i);
        assert_eq!(e.max_time, a.max_time, "{}: chunk {} max time", msg, i);
        assert_eq!(
            e.chunk.is_some(),
            a.chunk.is_some(),
            "{}: chunk {} presence",
            msg,
            i
        );
        if let (Some(ec), Some(ac)) = (&e.chunk, &a.chunk) {
            assert_eq!(
                ec.encoding(),
                ac.encoding(),
                "{}: chunk {} encoding",
                msg,
                i
            );
            assert_eq!(ec.bytes(), ac.bytes(), "{}: chunk {} bytes", msg, i);
            assert_eq!(
                ec.num_samples(),
                ac.num_samples(),
                "{}: chunk {} samples",
                msg,
                i
            );
        }
    }
}

fn assert_chunk_results_eq(
    exp: &Result<Vec<Meta>, Error>,
    act: &Result<Vec<Meta>, Error>,
    msg: &str,
) {
    match (exp, act) {
        (Ok(e), Ok(a)) => assert_metas_eq(e, a, msg),
        (Err(e), Err(a)) => assert_eq!(e, a, "{}", msg),
        _ => panic!(
            "{}: expected {:?}, got {:?}",
            msg,
            exp.as_ref().err(),
            act.as_ref().err()
        ),
    }
}

fn expand_chunk_series(s: &dyn ChunkSeries) -> Result<Vec<Meta>, Error> {
    let mut it = s.iterator(None);
    expand_chunks(&mut *it)
}

fn expand_series(s: &dyn Series) -> Result<Vec<Sample>, Error> {
    let mut it = s.iterator(None);
    expand_samples(&mut it)
}

/// Go `mockSeriesSet`.
struct MockSeriesSet {
    idx: isize,
    series: Vec<SeriesArc>,
    warnings: Annotations,
    err: Option<Error>,
}

/// Go `NewMockSeriesSet`.
fn new_mock_series_set(series: Vec<SeriesArc>) -> MockSeriesSet {
    MockSeriesSet {
        idx: -1,
        series,
        warnings: Annotations::new(),
        err: None,
    }
}

impl SeriesSet for MockSeriesSet {
    fn next(&mut self) -> bool {
        if self.err.is_some() {
            return false;
        }
        self.idx += 1;
        (self.idx as usize) < self.series.len()
    }
    fn at(&self) -> &SeriesArc {
        &self.series[self.idx as usize]
    }
    fn err(&self) -> Option<Error> {
        self.err.clone()
    }
    fn warnings(&self) -> Annotations {
        self.warnings.clone()
    }
}

/// Go `mockChunkSeriesSet`.
struct MockChunkSeriesSet {
    idx: isize,
    series: Vec<ChunkSeriesArc>,
}

fn new_mock_chunk_series_set(series: Vec<ChunkSeriesArc>) -> MockChunkSeriesSet {
    MockChunkSeriesSet { idx: -1, series }
}

impl ChunkSeriesSet for MockChunkSeriesSet {
    fn next(&mut self) -> bool {
        self.idx += 1;
        (self.idx as usize) < self.series.len()
    }
    fn at(&self) -> &ChunkSeriesArc {
        &self.series[self.idx as usize]
    }
    fn err(&self) -> Option<Error> {
        None
    }
    fn warnings(&self) -> Annotations {
        Annotations::new()
    }
}

/// A label values request (Go `labelNameRequest`); matchers as (type, name, value).
#[derive(Debug, PartialEq)]
struct LabelNameRequest {
    name: String,
    matchers: Vec<(MatchType, String, String)>,
}

/// The recorded calls of a mock querier.
// adapted: Go inspects the *mockQuerier inside the merge tree; here the test keeps a handle to the shared state.
#[derive(Default)]
struct MockState {
    closed: bool,
    label_names_calls: usize,
    label_names_requested: Vec<LabelNameRequest>,
    sorted_series_requested: Vec<bool>,
}

/// Go `mockQuerier`.
#[derive(Default)]
struct MockQuerier {
    state: Arc<Mutex<MockState>>,
    /// Response for Select.
    to_return: Vec<SeriesArc>,
    /// Response for LabelNames and LabelValues; turned into a Select response if `to_return` is empty.
    resp: Vec<String>,
    warnings: Annotations,
    err: Option<Error>,
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

impl LabelQuerier for MockQuerier {
    fn label_values(
        &self,
        _: &Context,
        name: &str,
        _: Option<&LabelHints>,
        matchers: &[Matcher],
    ) -> LabelResult {
        self.state
            .lock()
            .unwrap()
            .label_names_requested
            .push(LabelNameRequest {
                name: name.to_string(),
                matchers: matchers
                    .iter()
                    .map(|m| (m.typ, m.name.clone(), m.value.clone()))
                    .collect(),
            });
        (self.resp.clone(), self.warnings.clone(), self.err.clone())
    }
    fn label_names(&self, _: &Context, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        self.state.lock().unwrap().label_names_calls += 1;
        (self.resp.clone(), self.warnings.clone(), self.err.clone())
    }
    fn close(&mut self) -> Option<Error> {
        self.state.lock().unwrap().closed = true;
        None
    }
}

impl MockQuerier {
    fn do_select(&self, sort_series: bool) -> BoxSeriesSet {
        self.state
            .lock()
            .unwrap()
            .sorted_series_requested
            .push(sort_series);
        let mut ret: Vec<SeriesArc> = if !self.to_return.is_empty() {
            self.to_return.clone()
        } else {
            self.resp
                .iter()
                .map(|l| list(ls(&["test", l]), vec![]))
                .collect()
        };
        if sort_series {
            ret.sort_by(|a, b| labels::compare(a.labels(), b.labels()).cmp(&0));
        }
        Box::new(MockSeriesSet {
            idx: -1,
            series: ret,
            warnings: self.warnings.clone(),
            err: self.err.clone(),
        })
    }
}

impl Querier for MockQuerier {
    fn select(
        &self,
        _: &Context,
        sort_series: bool,
        _: Option<&SelectHints>,
        _: &[Matcher],
    ) -> BoxSeriesSet {
        self.do_select(sort_series)
    }
}

/// Go `mockChunkQuerier`.
struct MockChunkQuerier {
    to_return: Vec<ChunkSeriesArc>,
}

impl LabelQuerier for MockChunkQuerier {
    // adapted: Go embeds a nil LabelQuerier, which the tests never call.
    fn label_values(
        &self,
        _: &Context,
        _: &str,
        _: Option<&LabelHints>,
        _: &[Matcher],
    ) -> LabelResult {
        unimplemented!()
    }
    fn label_names(&self, _: &Context, _: Option<&LabelHints>, _: &[Matcher]) -> LabelResult {
        unimplemented!()
    }
    fn close(&mut self) -> Option<Error> {
        None
    }
}

impl ChunkQuerier for MockChunkQuerier {
    fn select(
        &self,
        _: &Context,
        sort_series: bool,
        _: Option<&SelectHints>,
        _: &[Matcher],
    ) -> BoxChunkSeriesSet {
        let mut cpy = self.to_return.clone();
        if sort_series {
            cpy.sort_by(|a, b| labels::compare(a.labels(), b.labels()).cmp(&0));
        }
        Box::new(new_mock_chunk_series_set(cpy))
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Merge queriers.

#[test]
fn test_merge_querier_with_chain_merger() {
    struct Case {
        name: &'static str,
        primary_querier_series: Option<Vec<SeriesArc>>,
        querier_series: Vec<Vec<SeriesArc>>,
        extra_queriers: Vec<BoxQuerier>,
        expected: Vec<SeriesArc>,
    }
    let bb = || ls(&["bar", "baz"]);
    let fb = || ls(&["foo", "bar"]);
    let empties = |n: usize| (0..n).map(|_| Vec::new()).collect::<Vec<Vec<SeriesArc>>>();
    let cases = vec![
        Case {
            name: "one primary querier with no series",
            primary_querier_series: Some(vec![]),
            querier_series: vec![],
            extra_queriers: vec![],
            expected: vec![],
        },
        Case {
            name: "one secondary querier with no series",
            primary_querier_series: None,
            querier_series: empties(1),
            extra_queriers: vec![],
            expected: vec![],
        },
        Case {
            name: "many secondary queriers with no series",
            primary_querier_series: None,
            querier_series: empties(7),
            extra_queriers: vec![],
            expected: vec![],
        },
        Case {
            name: "mix of queriers with no series",
            primary_querier_series: Some(vec![]),
            querier_series: empties(7),
            extra_queriers: vec![],
            expected: vec![],
        },
        // Test rest of cases on secondary queriers as the different between primary vs secondary is just error handling.
        Case {
            name: "one querier, two series",
            primary_querier_series: None,
            querier_series: vec![vec![
                list(bb(), vec![fs(0, 1, 1.0), fs(0, 2, 2.0), fs(0, 3, 3.0)]),
                list(fb(), vec![fs(0, 0, 0.0), fs(0, 1, 1.0), fs(0, 2, 2.0)]),
            ]],
            extra_queriers: vec![],
            expected: vec![
                list(bb(), vec![fs(0, 1, 1.0), fs(0, 2, 2.0), fs(0, 3, 3.0)]),
                list(fb(), vec![fs(0, 0, 0.0), fs(0, 1, 1.0), fs(0, 2, 2.0)]),
            ],
        },
        Case {
            name: "two queriers, one different series each",
            primary_querier_series: None,
            querier_series: vec![
                vec![list(
                    bb(),
                    vec![fs(0, 1, 1.0), fs(0, 2, 2.0), fs(0, 3, 3.0)],
                )],
                vec![list(
                    fb(),
                    vec![fs(0, 0, 0.0), fs(0, 1, 1.0), fs(0, 2, 2.0)],
                )],
            ],
            extra_queriers: vec![],
            expected: vec![
                list(bb(), vec![fs(0, 1, 1.0), fs(0, 2, 2.0), fs(0, 3, 3.0)]),
                list(fb(), vec![fs(0, 0, 0.0), fs(0, 1, 1.0), fs(0, 2, 2.0)]),
            ],
        },
        Case {
            name: "two time unsorted queriers, two series each",
            primary_querier_series: None,
            querier_series: vec![
                vec![
                    list(bb(), vec![fs(0, 5, 5.0), fs(0, 6, 6.0)]),
                    list(fb(), vec![fs(0, 0, 0.0), fs(0, 1, 1.0), fs(0, 2, 2.0)]),
                ],
                vec![
                    list(bb(), vec![fs(0, 1, 1.0), fs(0, 2, 2.0), fs(0, 3, 3.0)]),
                    list(fb(), vec![fs(0, 3, 3.0), fs(0, 4, 4.0)]),
                ],
            ],
            extra_queriers: vec![],
            expected: vec![
                list(
                    bb(),
                    vec![
                        fs(0, 1, 1.0),
                        fs(0, 2, 2.0),
                        fs(0, 3, 3.0),
                        fs(0, 5, 5.0),
                        fs(0, 6, 6.0),
                    ],
                ),
                list(
                    fb(),
                    vec![
                        fs(0, 0, 0.0),
                        fs(0, 1, 1.0),
                        fs(0, 2, 2.0),
                        fs(0, 3, 3.0),
                        fs(0, 4, 4.0),
                    ],
                ),
            ],
        },
        Case {
            name: "five queriers, only two queriers have two time unsorted series each",
            primary_querier_series: None,
            querier_series: vec![
                vec![],
                vec![],
                vec![
                    list(bb(), vec![fs(0, 5, 5.0), fs(0, 6, 6.0)]),
                    list(fb(), vec![fs(0, 0, 0.0), fs(0, 1, 1.0), fs(0, 2, 2.0)]),
                ],
                vec![
                    list(bb(), vec![fs(0, 1, 1.0), fs(0, 2, 2.0), fs(0, 3, 3.0)]),
                    list(fb(), vec![fs(0, 3, 3.0), fs(0, 4, 4.0)]),
                ],
                vec![],
            ],
            extra_queriers: vec![],
            expected: vec![
                list(
                    bb(),
                    vec![
                        fs(0, 1, 1.0),
                        fs(0, 2, 2.0),
                        fs(0, 3, 3.0),
                        fs(0, 5, 5.0),
                        fs(0, 6, 6.0),
                    ],
                ),
                list(
                    fb(),
                    vec![
                        fs(0, 0, 0.0),
                        fs(0, 1, 1.0),
                        fs(0, 2, 2.0),
                        fs(0, 3, 3.0),
                        fs(0, 4, 4.0),
                    ],
                ),
            ],
        },
        Case {
            name: "two queriers, only two queriers have two time unsorted series each, with 3 noop and one nil querier together",
            primary_querier_series: None,
            querier_series: vec![
                vec![],
                vec![],
                vec![
                    list(bb(), vec![fs(0, 5, 5.0), fs(0, 6, 6.0)]),
                    list(fb(), vec![fs(0, 0, 0.0), fs(0, 1, 1.0), fs(0, 2, 2.0)]),
                ],
                vec![
                    list(bb(), vec![fs(0, 1, 1.0), fs(0, 2, 2.0), fs(0, 3, 3.0)]),
                    list(fb(), vec![fs(0, 3, 3.0), fs(0, 4, 4.0)]),
                ],
                vec![],
            ],
            // adapted: Rust has no nil querier; the nil entry is left out.
            extra_queriers: vec![noop_querier(), noop_querier(), noop_querier()],
            expected: vec![
                list(
                    bb(),
                    vec![
                        fs(0, 1, 1.0),
                        fs(0, 2, 2.0),
                        fs(0, 3, 3.0),
                        fs(0, 5, 5.0),
                        fs(0, 6, 6.0),
                    ],
                ),
                list(
                    fb(),
                    vec![
                        fs(0, 0, 0.0),
                        fs(0, 1, 1.0),
                        fs(0, 2, 2.0),
                        fs(0, 3, 3.0),
                        fs(0, 4, 4.0),
                    ],
                ),
            ],
        },
        Case {
            name: "two queriers, with two series, one is overlapping",
            primary_querier_series: None,
            querier_series: vec![
                vec![],
                vec![],
                vec![
                    list(
                        bb(),
                        vec![fs(0, 2, 21.0), fs(0, 3, 31.0), fs(0, 5, 5.0), fs(0, 6, 6.0)],
                    ),
                    list(fb(), vec![fs(0, 0, 0.0), fs(0, 1, 1.0), fs(0, 2, 2.0)]),
                ],
                vec![
                    list(bb(), vec![fs(0, 1, 1.0), fs(0, 2, 22.0), fs(0, 3, 32.0)]),
                    list(fb(), vec![fs(0, 3, 3.0), fs(0, 4, 4.0)]),
                ],
                vec![],
            ],
            extra_queriers: vec![],
            expected: vec![
                list(
                    bb(),
                    vec![
                        fs(0, 1, 1.0),
                        fs(0, 2, 21.0),
                        fs(0, 3, 31.0),
                        fs(0, 5, 5.0),
                        fs(0, 6, 6.0),
                    ],
                ),
                list(
                    fb(),
                    vec![
                        fs(0, 0, 0.0),
                        fs(0, 1, 1.0),
                        fs(0, 2, 2.0),
                        fs(0, 3, 3.0),
                        fs(0, 4, 4.0),
                    ],
                ),
            ],
        },
        Case {
            name: "two queries, one with NaN samples series",
            primary_querier_series: None,
            querier_series: vec![
                vec![list(fb(), vec![fs(0, 0, f64::NAN)])],
                vec![list(fb(), vec![fs(0, 1, 1.0)])],
            ],
            extra_queriers: vec![],
            expected: vec![list(fb(), vec![fs(0, 0, f64::NAN), fs(0, 1, 1.0)])],
        },
    ];

    for tc in cases {
        let mut p: Vec<BoxQuerier> = Vec::new();
        if let Some(s) = tc.primary_querier_series {
            p.push(Box::new(MockQuerier {
                to_return: s,
                ..Default::default()
            }));
        }
        let mut qs: Vec<BoxQuerier> = Vec::new();
        for s in tc.querier_series {
            qs.push(Box::new(MockQuerier {
                to_return: s,
                ..Default::default()
            }));
        }
        qs.extend(tc.extra_queriers);

        let q = new_merge_querier(p, qs, chained());
        let mut merged_querier = q.select(&Context::background(), false, None, &[]);

        // Get all merged series upfront to make sure there are no incorrectly retained shared buffers.
        let mut merged_series = Vec::new();
        while merged_querier.next() {
            merged_series.push(merged_querier.at().clone());
        }
        assert!(merged_querier.err().is_none(), "{}", tc.name);

        let mut expected = new_mock_series_set(tc.expected);
        for actual in &merged_series {
            assert!(expected.next(), "{}: Expected Next() to be true", tc.name);
            let exp = expected.at().clone();
            assert_eq!(exp.labels(), actual.labels(), "{}", tc.name);
            let exp_smpl = expand_series(&*exp);
            let act_smpl = expand_series(&**actual);
            assert_eq!(exp_smpl, act_smpl, "{}", tc.name);
        }
        assert!(!expected.next(), "{}: Expected Next() to be false", tc.name);
    }
}

#[test]
fn test_merge_chunk_querier_with_no_vertical_chunk_series_merger() {
    struct Case {
        name: &'static str,
        primary: Option<Vec<ChunkSeriesArc>>,
        chk: Vec<Vec<ChunkSeriesArc>>,
        extra: Vec<BoxChunkQuerier>,
        expected: Vec<ChunkSeriesArc>,
    }
    let bb = || ls(&["bar", "baz"]);
    let fb = || ls(&["foo", "bar"]);
    let empties = |n: usize| {
        (0..n)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<ChunkSeriesArc>>>()
    };
    let cases = vec![
        Case {
            name: "one primary querier with no series",
            primary: Some(vec![]),
            chk: vec![],
            extra: vec![],
            expected: vec![],
        },
        Case {
            name: "one secondary querier with no series",
            primary: None,
            chk: empties(1),
            extra: vec![],
            expected: vec![],
        },
        Case {
            name: "many secondary queriers with no series",
            primary: None,
            chk: empties(7),
            extra: vec![],
            expected: vec![],
        },
        Case {
            name: "mix of queriers with no series",
            primary: Some(vec![]),
            chk: empties(7),
            extra: vec![],
            expected: vec![],
        },
        // Test rest of cases on secondary queriers as the different between primary vs secondary is just error handling.
        Case {
            name: "one querier, two series",
            primary: None,
            chk: vec![vec![
                lcs(
                    bb(),
                    &[vec![fs(0, 1, 1.0), fs(0, 2, 2.0)], vec![fs(0, 3, 3.0)]],
                ),
                lcs(
                    fb(),
                    &[vec![fs(0, 0, 0.0), fs(0, 1, 1.0)], vec![fs(0, 2, 2.0)]],
                ),
            ]],
            extra: vec![],
            expected: vec![
                lcs(
                    bb(),
                    &[vec![fs(0, 1, 1.0), fs(0, 2, 2.0)], vec![fs(0, 3, 3.0)]],
                ),
                lcs(
                    fb(),
                    &[vec![fs(0, 0, 0.0), fs(0, 1, 1.0)], vec![fs(0, 2, 2.0)]],
                ),
            ],
        },
        Case {
            name: "two secondaries, one different series each",
            primary: None,
            chk: vec![
                vec![lcs(
                    bb(),
                    &[vec![fs(0, 1, 1.0), fs(0, 2, 2.0)], vec![fs(0, 3, 3.0)]],
                )],
                vec![lcs(
                    fb(),
                    &[vec![fs(0, 0, 0.0), fs(0, 1, 1.0)], vec![fs(0, 2, 2.0)]],
                )],
            ],
            extra: vec![],
            expected: vec![
                lcs(
                    bb(),
                    &[vec![fs(0, 1, 1.0), fs(0, 2, 2.0)], vec![fs(0, 3, 3.0)]],
                ),
                lcs(
                    fb(),
                    &[vec![fs(0, 0, 0.0), fs(0, 1, 1.0)], vec![fs(0, 2, 2.0)]],
                ),
            ],
        },
        Case {
            name: "two secondaries, two not in time order series each",
            primary: None,
            chk: vec![
                vec![
                    lcs(bb(), &[vec![fs(0, 5, 5.0)], vec![fs(0, 6, 6.0)]]),
                    lcs(
                        fb(),
                        &[vec![fs(0, 0, 0.0), fs(0, 1, 1.0)], vec![fs(0, 2, 2.0)]],
                    ),
                ],
                vec![
                    lcs(
                        bb(),
                        &[vec![fs(0, 1, 1.0), fs(0, 2, 2.0)], vec![fs(0, 3, 3.0)]],
                    ),
                    lcs(fb(), &[vec![fs(0, 3, 3.0)], vec![fs(0, 4, 4.0)]]),
                ],
            ],
            extra: vec![],
            expected: vec![
                lcs(
                    bb(),
                    &[
                        vec![fs(0, 1, 1.0), fs(0, 2, 2.0)],
                        vec![fs(0, 3, 3.0)],
                        vec![fs(0, 5, 5.0)],
                        vec![fs(0, 6, 6.0)],
                    ],
                ),
                lcs(
                    fb(),
                    &[
                        vec![fs(0, 0, 0.0), fs(0, 1, 1.0)],
                        vec![fs(0, 2, 2.0)],
                        vec![fs(0, 3, 3.0)],
                        vec![fs(0, 4, 4.0)],
                    ],
                ),
            ],
        },
        Case {
            name: "five secondaries, only two have two not in time order series each",
            primary: None,
            chk: vec![
                vec![],
                vec![],
                vec![
                    lcs(bb(), &[vec![fs(0, 5, 5.0)], vec![fs(0, 6, 6.0)]]),
                    lcs(
                        fb(),
                        &[vec![fs(0, 0, 0.0), fs(0, 1, 1.0)], vec![fs(0, 2, 2.0)]],
                    ),
                ],
                vec![
                    lcs(
                        bb(),
                        &[vec![fs(0, 1, 1.0), fs(0, 2, 2.0)], vec![fs(0, 3, 3.0)]],
                    ),
                    lcs(fb(), &[vec![fs(0, 3, 3.0)], vec![fs(0, 4, 4.0)]]),
                ],
                vec![],
            ],
            extra: vec![],
            expected: vec![
                lcs(
                    bb(),
                    &[
                        vec![fs(0, 1, 1.0), fs(0, 2, 2.0)],
                        vec![fs(0, 3, 3.0)],
                        vec![fs(0, 5, 5.0)],
                        vec![fs(0, 6, 6.0)],
                    ],
                ),
                lcs(
                    fb(),
                    &[
                        vec![fs(0, 0, 0.0), fs(0, 1, 1.0)],
                        vec![fs(0, 2, 2.0)],
                        vec![fs(0, 3, 3.0)],
                        vec![fs(0, 4, 4.0)],
                    ],
                ),
            ],
        },
        Case {
            name: "two secondaries, with two not in time order series each, with 3 noop queries and one nil together",
            primary: None,
            chk: vec![
                vec![
                    lcs(bb(), &[vec![fs(0, 5, 5.0)], vec![fs(0, 6, 6.0)]]),
                    lcs(
                        fb(),
                        &[vec![fs(0, 0, 0.0), fs(0, 1, 1.0)], vec![fs(0, 2, 2.0)]],
                    ),
                ],
                vec![
                    lcs(
                        bb(),
                        &[vec![fs(0, 1, 1.0), fs(0, 2, 2.0)], vec![fs(0, 3, 3.0)]],
                    ),
                    lcs(fb(), &[vec![fs(0, 3, 3.0)], vec![fs(0, 4, 4.0)]]),
                ],
            ],
            // adapted: Rust has no nil querier; the nil entry is left out.
            extra: vec![
                noop_chunked_querier(),
                noop_chunked_querier(),
                noop_chunked_querier(),
            ],
            expected: vec![
                lcs(
                    bb(),
                    &[
                        vec![fs(0, 1, 1.0), fs(0, 2, 2.0)],
                        vec![fs(0, 3, 3.0)],
                        vec![fs(0, 5, 5.0)],
                        vec![fs(0, 6, 6.0)],
                    ],
                ),
                lcs(
                    fb(),
                    &[
                        vec![fs(0, 0, 0.0), fs(0, 1, 1.0)],
                        vec![fs(0, 2, 2.0)],
                        vec![fs(0, 3, 3.0)],
                        vec![fs(0, 4, 4.0)],
                    ],
                ),
            ],
        },
        Case {
            name: "two queries, one with NaN samples series",
            primary: None,
            chk: vec![
                vec![lcs(fb(), &[vec![fs(0, 0, f64::NAN)]])],
                vec![lcs(fb(), &[vec![fs(0, 1, 1.0)]])],
            ],
            extra: vec![],
            expected: vec![lcs(fb(), &[vec![fs(0, 0, f64::NAN)], vec![fs(0, 1, 1.0)]])],
        },
    ];

    for tc in cases {
        let mut p: Vec<BoxChunkQuerier> = Vec::new();
        if let Some(s) = tc.primary {
            p.push(Box::new(MockChunkQuerier { to_return: s }));
        }
        let mut qs: Vec<BoxChunkQuerier> = Vec::new();
        for s in tc.chk {
            qs.push(Box::new(MockChunkQuerier { to_return: s }));
        }
        qs.extend(tc.extra);

        // adapted: Go passes a nil series merge function, which is never called here (no chunks overlap).
        let nil_merge: VerticalSeriesMergeFunc = Arc::new(|_| panic!("nil merge function called"));
        let q = new_merge_chunk_querier(p, qs, new_compacting_chunk_series_merger(nil_merge));
        let mut merged = q.select(&Context::background(), false, None, &[]);
        let mut expected = new_mock_chunk_series_set(tc.expected);
        while merged.next() {
            assert!(expected.next(), "{}: Expected Next() to be true", tc.name);
            let actual = merged.at().clone();
            let exp = expected.at().clone();
            assert_eq!(exp.labels(), actual.labels(), "{}", tc.name);
            let exp_chks = expand_chunk_series(&*exp);
            let act_chks = expand_chunk_series(&*actual);
            assert_chunk_results_eq(&exp_chks, &act_chks, tc.name);
        }
        assert!(merged.err().is_none(), "{}", tc.name);
        assert!(!expected.next(), "{}: Expected Next() to be false", tc.name);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Chunk series mergers.

fn check_merged_chunks(
    name: &str,
    merged: &dyn ChunkSeries,
    expected: &dyn ChunkSeries,
    samples_too: bool,
) {
    assert_eq!(expected.labels(), merged.labels(), "{}", name);
    let act = expand_chunk_series(merged);
    let exp = expand_chunk_series(expected);
    assert_chunk_results_eq(&exp, &act, name);
    if samples_too {
        let act_samples = chunk_metas_to_samples(act.as_ref().unwrap());
        let exp_samples = chunk_metas_to_samples(exp.as_ref().unwrap());
        assert_eq!(exp_samples, act_samples, "{}", name);
    }
}

#[test]
fn test_compacting_chunk_series_merger() {
    let m = new_compacting_chunk_series_merger(chained());
    // histogramSample returns a histogram that is unique to the ts.
    let hs = |ts| histogram_sample(ts, UK);
    let fhs = |ts| float_histogram_sample(ts, UK);
    let bb = || ls(&["bar", "baz"]);
    let e: Vec<Sample> = Vec::new();

    let cases: Vec<(&str, Vec<ChunkSeriesArc>, ChunkSeriesArc)> = vec![
        (
            "single empty series",
            vec![lcs(bb(), &[e.clone()])],
            lcs(bb(), &[e.clone()]),
        ),
        (
            "single series",
            vec![lcs(
                bb(),
                &[vec![fs(0, 1, 1.0), fs(0, 2, 2.0)], vec![fs(0, 3, 3.0)]],
            )],
            lcs(
                bb(),
                &[vec![fs(0, 1, 1.0), fs(0, 2, 2.0)], vec![fs(0, 3, 3.0)]],
            ),
        ),
        (
            "two empty series",
            vec![lcs(bb(), &[e.clone()]), lcs(bb(), &[e.clone()])],
            lcs(bb(), &[e.clone()]),
        ),
        (
            "two non overlapping",
            vec![
                lcs(
                    bb(),
                    &[
                        vec![fs(0, 1, 1.0), fs(0, 2, 2.0)],
                        vec![fs(0, 3, 3.0), fs(0, 5, 5.0)],
                    ],
                ),
                lcs(
                    bb(),
                    &[vec![fs(0, 7, 7.0), fs(0, 9, 9.0)], vec![fs(0, 10, 10.0)]],
                ),
            ],
            lcs(
                bb(),
                &[
                    vec![fs(0, 1, 1.0), fs(0, 2, 2.0)],
                    vec![fs(0, 3, 3.0), fs(0, 5, 5.0)],
                    vec![fs(0, 7, 7.0), fs(0, 9, 9.0)],
                    vec![fs(0, 10, 10.0)],
                ],
            ),
        ),
        (
            "two overlapping",
            vec![
                lcs(
                    bb(),
                    &[
                        vec![fs(0, 1, 1.0), fs(0, 2, 2.0)],
                        vec![fs(0, 3, 3.0), fs(0, 8, 8.0)],
                    ],
                ),
                lcs(
                    bb(),
                    &[vec![fs(0, 7, 7.0), fs(0, 9, 9.0)], vec![fs(0, 10, 10.0)]],
                ),
            ],
            lcs(
                bb(),
                &[
                    vec![fs(0, 1, 1.0), fs(0, 2, 2.0)],
                    vec![fs(0, 3, 3.0), fs(0, 7, 7.0), fs(0, 8, 8.0), fs(0, 9, 9.0)],
                    vec![fs(0, 10, 10.0)],
                ],
            ),
        ),
        (
            "two duplicated",
            vec![
                lcs(
                    bb(),
                    &[vec![
                        fs(0, 1, 1.0),
                        fs(0, 2, 2.0),
                        fs(0, 3, 3.0),
                        fs(0, 5, 5.0),
                    ]],
                ),
                lcs(bb(), &[vec![fs(0, 2, 2.0), fs(0, 3, 3.0), fs(0, 5, 5.0)]]),
            ],
            lcs(
                bb(),
                &[vec![
                    fs(0, 1, 1.0),
                    fs(0, 2, 2.0),
                    fs(0, 3, 3.0),
                    fs(0, 5, 5.0),
                ]],
            ),
        ),
        (
            "three overlapping",
            vec![
                lcs(
                    bb(),
                    &[vec![
                        fs(0, 1, 1.0),
                        fs(0, 2, 2.0),
                        fs(0, 3, 3.0),
                        fs(0, 5, 5.0),
                    ]],
                ),
                lcs(bb(), &[vec![fs(0, 2, 2.0), fs(0, 3, 3.0), fs(0, 6, 6.0)]]),
                lcs(bb(), &[vec![fs(0, 0, 0.0), fs(0, 4, 4.0)]]),
            ],
            lcs(
                bb(),
                &[vec![
                    fs(0, 0, 0.0),
                    fs(0, 1, 1.0),
                    fs(0, 2, 2.0),
                    fs(0, 3, 3.0),
                    fs(0, 4, 4.0),
                    fs(0, 5, 5.0),
                    fs(0, 6, 6.0),
                ]],
            ),
        ),
        (
            "three in chained overlap",
            vec![
                lcs(
                    bb(),
                    &[vec![
                        fs(0, 1, 1.0),
                        fs(0, 2, 2.0),
                        fs(0, 3, 3.0),
                        fs(0, 5, 5.0),
                    ]],
                ),
                lcs(bb(), &[vec![fs(0, 4, 4.0), fs(0, 6, 66.0)]]),
                lcs(bb(), &[vec![fs(0, 6, 6.0), fs(0, 10, 10.0)]]),
            ],
            lcs(
                bb(),
                &[vec![
                    fs(0, 1, 1.0),
                    fs(0, 2, 2.0),
                    fs(0, 3, 3.0),
                    fs(0, 4, 4.0),
                    fs(0, 5, 5.0),
                    fs(0, 6, 66.0),
                    fs(0, 10, 10.0),
                ]],
            ),
        ),
        (
            "three in chained overlap complex",
            vec![
                lcs(
                    bb(),
                    &[
                        vec![fs(0, 0, 0.0), fs(0, 5, 5.0)],
                        vec![fs(0, 10, 10.0), fs(0, 15, 15.0)],
                    ],
                ),
                lcs(
                    bb(),
                    &[
                        vec![fs(0, 2, 2.0), fs(0, 20, 20.0)],
                        vec![fs(0, 25, 25.0), fs(0, 30, 30.0)],
                    ],
                ),
                lcs(
                    bb(),
                    &[
                        vec![fs(0, 18, 18.0), fs(0, 26, 26.0)],
                        vec![fs(0, 31, 31.0), fs(0, 35, 35.0)],
                    ],
                ),
            ],
            lcs(
                bb(),
                &[
                    vec![
                        fs(0, 0, 0.0),
                        fs(0, 2, 2.0),
                        fs(0, 5, 5.0),
                        fs(0, 10, 10.0),
                        fs(0, 15, 15.0),
                        fs(0, 18, 18.0),
                        fs(0, 20, 20.0),
                        fs(0, 25, 25.0),
                        fs(0, 26, 26.0),
                        fs(0, 30, 30.0),
                    ],
                    vec![fs(0, 31, 31.0), fs(0, 35, 35.0)],
                ],
            ),
        ),
        (
            "110 overlapping",
            vec![
                lcs(bb(), &[generate_samples(0, 110)]),
                lcs(bb(), &[generate_samples(60, 50)]),
            ],
            lcs(bb(), &[generate_samples(0, 110)]),
        ),
        (
            "150 overlapping samples, split chunk",
            vec![
                lcs(bb(), &[generate_samples(0, 90)]),
                lcs(bb(), &[generate_samples(60, 90)]),
            ],
            lcs(bb(), &[generate_samples(0, 120), generate_samples(120, 30)]),
        ),
        (
            "histogram chunks overlapping",
            vec![
                lcs(bb(), &[vec![hs(0), hs(5)], vec![hs(10), hs(15)]]),
                lcs(bb(), &[vec![hs(2), hs(20)], vec![hs(25), hs(30)]]),
                lcs(bb(), &[vec![hs(18), hs(26)], vec![hs(31), hs(35)]]),
            ],
            lcs(
                bb(),
                &[
                    vec![
                        hs(0),
                        hs(2),
                        hs(5),
                        hs(10),
                        hs(15),
                        hs(18),
                        hs(20),
                        hs(25),
                        hs(26),
                        hs(30),
                    ],
                    vec![hs(31), hs(35)],
                ],
            ),
        ),
        (
            "histogram chunks overlapping with float chunks",
            vec![
                lcs(bb(), &[vec![hs(0), hs(5)], vec![hs(10), hs(15)]]),
                lcs(
                    bb(),
                    &[vec![fs(0, 1, 1.0), fs(0, 12, 12.0)], vec![fs(0, 14, 14.0)]],
                ),
            ],
            lcs(
                bb(),
                &[
                    vec![hs(0)],
                    vec![fs(0, 1, 1.0)],
                    vec![hs(5), hs(10)],
                    vec![fs(0, 12, 12.0), fs(0, 14, 14.0)],
                    vec![hs(15)],
                ],
            ),
        ),
        (
            "float histogram chunks overlapping",
            vec![
                lcs(bb(), &[vec![fhs(0), fhs(5)], vec![fhs(10), fhs(15)]]),
                lcs(bb(), &[vec![fhs(2), fhs(20)], vec![fhs(25), fhs(30)]]),
                lcs(bb(), &[vec![fhs(18), fhs(26)], vec![fhs(31), fhs(35)]]),
            ],
            lcs(
                bb(),
                &[
                    vec![
                        fhs(0),
                        fhs(2),
                        fhs(5),
                        fhs(10),
                        fhs(15),
                        fhs(18),
                        fhs(20),
                        fhs(25),
                        fhs(26),
                        fhs(30),
                    ],
                    vec![fhs(31), fhs(35)],
                ],
            ),
        ),
        (
            "float histogram chunks overlapping with float chunks",
            vec![
                lcs(bb(), &[vec![fhs(0), fhs(5)], vec![fhs(10), fhs(15)]]),
                lcs(
                    bb(),
                    &[vec![fs(0, 1, 1.0), fs(0, 12, 12.0)], vec![fs(0, 14, 14.0)]],
                ),
            ],
            lcs(
                bb(),
                &[
                    vec![fhs(0)],
                    vec![fs(0, 1, 1.0)],
                    vec![fhs(5), fhs(10)],
                    vec![fs(0, 12, 12.0), fs(0, 14, 14.0)],
                    vec![fhs(15)],
                ],
            ),
        ),
        (
            "float histogram chunks overlapping with histogram chunks",
            vec![
                lcs(bb(), &[vec![fhs(0), fhs(5)], vec![fhs(10), fhs(15)]]),
                lcs(bb(), &[vec![hs(1), hs(12)], vec![hs(14)]]),
            ],
            lcs(
                bb(),
                &[
                    vec![fhs(0)],
                    vec![hs(1)],
                    vec![fhs(5), fhs(10)],
                    vec![hs(12), hs(14)],
                    vec![fhs(15)],
                ],
            ),
        ),
    ];

    for (name, input, expected) in cases {
        let merged = m(input);
        check_merged_chunks(name, &*merged, &*expected, true);
    }
}

#[test]
fn test_compacting_chunk_series_merger_float_encoding() {
    let lbls = ls(&["bar", "baz"]);

    // Two series with overlapping XOR chunks, forcing a re-encode on merge.
    let overlapping_input = || {
        vec![
            lcs(lbls.clone(), &[vec![fs(0, 1, 1.0), fs(0, 2, 2.0)]]),
            lcs(lbls.clone(), &[vec![fs(0, 2, 2.0), fs(0, 3, 3.0)]]),
        ]
    };
    let expand = |m: &VerticalChunkSeriesMergeFunc, input: Vec<ChunkSeriesArc>| -> Vec<Meta> {
        let merged = m(input);
        assert_eq!(&lbls, merged.labels());
        expand_chunk_series(&*merged).expect("no error")
    };
    let xor2: FloatEncodingFn = Arc::new(|| chunkenc::ENC_XOR2);

    // nil getter keeps XOR for re-encoded chunks.
    let m = new_compacting_chunk_series_merger_with_float_encoding(chained(), None);
    let act = expand(&m, overlapping_input());
    assert_eq!(act.len(), 1);
    assert_eq!(act[0].chunk().encoding(), chunkenc::ENC_XOR);
    assert_eq!(act[0].chunk().num_samples(), 3);

    // XOR2 getter re-encodes overlapping chunks to XOR2.
    let m = new_compacting_chunk_series_merger_with_float_encoding(chained(), Some(xor2.clone()));
    let act = expand(&m, overlapping_input());
    assert_eq!(act.len(), 1);
    assert_eq!(act[0].chunk().encoding(), chunkenc::ENC_XOR2);
    assert_eq!(act[0].chunk().num_samples(), 3);
    let mut it = act[0].chunk().iterator(None);
    let act_samples = expand_samples(&mut it).expect("no error");
    assert_eq!(
        act_samples,
        vec![fs(0, 1, 1.0), fs(0, 2, 2.0), fs(0, 3, 3.0)]
    );

    // Non-overlapping chunks pass through with source encoding.
    let m = new_compacting_chunk_series_merger_with_float_encoding(chained(), Some(xor2.clone()));
    let input = vec![
        lcs(lbls.clone(), &[vec![fs(0, 1, 1.0), fs(0, 2, 2.0)]]),
        lcs(lbls.clone(), &[vec![fs(0, 3, 3.0), fs(0, 4, 4.0)]]),
    ];
    let act = expand(&m, input);
    assert_eq!(act.len(), 2);
    assert_eq!(act[0].chunk().encoding(), chunkenc::ENC_XOR);
    assert_eq!(act[1].chunk().encoding(), chunkenc::ENC_XOR);

    // The getter is read again for each merged series.
    let enc = Arc::new(Mutex::new(chunkenc::ENC_XOR));
    let enc2 = enc.clone();
    let getter: FloatEncodingFn = Arc::new(move || -> Encoding { *enc2.lock().unwrap() });
    let m = new_compacting_chunk_series_merger_with_float_encoding(chained(), Some(getter));
    let act = expand(&m, overlapping_input());
    assert_eq!(act.len(), 1);
    assert_eq!(act[0].chunk().encoding(), chunkenc::ENC_XOR);
    *enc.lock().unwrap() = chunkenc::ENC_XOR2;
    let act = expand(&m, overlapping_input());
    assert_eq!(act.len(), 1);
    assert_eq!(act[0].chunk().encoding(), chunkenc::ENC_XOR2);

    // 150 overlapping samples split at 120 with XOR2.
    let m = new_compacting_chunk_series_merger_with_float_encoding(chained(), Some(xor2));
    let input = vec![
        lcs(lbls.clone(), &[generate_samples(0, 90)]),
        lcs(lbls.clone(), &[generate_samples(60, 90)]),
    ];
    let act = expand(&m, input);
    assert_eq!(act.len(), 2);
    assert_eq!(act[0].chunk().encoding(), chunkenc::ENC_XOR2);
    assert_eq!(act[0].chunk().num_samples(), 120);
    assert_eq!(act[1].chunk().encoding(), chunkenc::ENC_XOR2);
    assert_eq!(act[1].chunk().num_samples(), 30);
}

type HintSampleFn = fn(i64, CounterResetHint) -> Sample;

fn hint_sample_funcs() -> [(&'static str, HintSampleFn); 2] {
    [
        ("histogram", histogram_sample),
        ("float histogram", float_histogram_sample),
    ]
}

#[test]
fn test_compacting_chunk_series_merger_histogram_counter_reset_hint() {
    let m = new_compacting_chunk_series_merger(chained());
    let bb = || ls(&["bar", "baz"]);
    for (sample_type, sf) in hint_sample_funcs() {
        let cases: Vec<(&str, Vec<ChunkSeriesArc>, ChunkSeriesArc)> = vec![
            (
                "histogram counter reset hint kept in single series",
                vec![lcs(
                    bb(),
                    &[vec![sf(0, CR), sf(5, UK)], vec![sf(10, CR), sf(15, UK)]],
                )],
                lcs(
                    bb(),
                    &[vec![sf(0, CR), sf(5, UK)], vec![sf(10, CR), sf(15, UK)]],
                ),
            ),
            (
                "histogram not counter reset hint kept in single series",
                vec![lcs(
                    bb(),
                    &[vec![sf(0, NR), sf(5, UK)], vec![sf(10, NR), sf(15, UK)]],
                )],
                lcs(
                    bb(),
                    &[vec![sf(0, NR), sf(5, UK)], vec![sf(10, NR), sf(15, UK)]],
                ),
            ),
            (
                "histogram counter reset hint kept in multiple equal series",
                vec![
                    lcs(
                        bb(),
                        &[vec![sf(0, CR), sf(5, UK)], vec![sf(10, CR), sf(15, UK)]],
                    ),
                    lcs(
                        bb(),
                        &[vec![sf(0, CR), sf(5, UK)], vec![sf(10, CR), sf(15, UK)]],
                    ),
                ],
                lcs(
                    bb(),
                    &[vec![sf(0, CR), sf(5, UK)], vec![sf(10, CR), sf(15, UK)]],
                ),
            ),
            (
                "histogram not counter reset hint kept in multiple equal series",
                vec![
                    lcs(
                        bb(),
                        &[vec![sf(0, NR), sf(5, UK)], vec![sf(10, NR), sf(15, UK)]],
                    ),
                    lcs(
                        bb(),
                        &[vec![sf(0, NR), sf(5, UK)], vec![sf(10, NR), sf(15, UK)]],
                    ),
                ],
                lcs(
                    bb(),
                    &[vec![sf(0, NR), sf(5, UK)], vec![sf(10, NR), sf(15, UK)]],
                ),
            ),
            (
                "histogram counter reset hint dropped from differing series",
                vec![
                    lcs(
                        bb(),
                        &[vec![sf(0, CR), sf(5, UK)], vec![sf(10, CR), sf(15, UK)]],
                    ),
                    lcs(
                        bb(),
                        &[
                            vec![sf(0, CR), sf(5, UK)],
                            vec![sf(10, CR), sf(12, UK), sf(15, UK)],
                        ],
                    ),
                ],
                lcs(
                    bb(),
                    &[
                        vec![sf(0, CR), sf(5, UK)],
                        vec![sf(10, UK), sf(12, UK), sf(15, UK)],
                    ],
                ),
            ),
            (
                "histogram counter not reset hint dropped from differing series",
                vec![
                    lcs(
                        bb(),
                        &[vec![sf(0, NR), sf(5, UK)], vec![sf(10, NR), sf(15, UK)]],
                    ),
                    lcs(
                        bb(),
                        &[
                            vec![sf(0, NR), sf(5, UK)],
                            vec![sf(10, NR), sf(12, UK), sf(15, UK)],
                        ],
                    ),
                ],
                lcs(
                    bb(),
                    &[
                        vec![sf(0, NR), sf(5, UK)],
                        vec![sf(10, UK), sf(12, UK), sf(15, UK)],
                    ],
                ),
            ),
        ];
        for (name, input, expected) in cases {
            let merged = m(input);
            check_merged_chunks(
                &format!("{}/{}", sample_type, name),
                &*merged,
                &*expected,
                true,
            );
        }
    }
}

#[test]
fn test_concatenating_chunk_series_merger() {
    let m = new_concatenating_chunk_series_merger();
    let bb = || ls(&["bar", "baz"]);
    let e: Vec<Sample> = Vec::new();
    let cases: Vec<(&str, Vec<ChunkSeriesArc>, ChunkSeriesArc)> = vec![
        (
            "single empty series",
            vec![lcs(bb(), &[e.clone()])],
            lcs(bb(), &[e.clone()]),
        ),
        (
            "single series",
            vec![lcs(
                bb(),
                &[vec![fs(0, 1, 1.0), fs(0, 2, 2.0)], vec![fs(0, 3, 3.0)]],
            )],
            lcs(
                bb(),
                &[vec![fs(0, 1, 1.0), fs(0, 2, 2.0)], vec![fs(0, 3, 3.0)]],
            ),
        ),
        (
            "two empty series",
            vec![lcs(bb(), &[e.clone()]), lcs(bb(), &[e.clone()])],
            lcs(bb(), &[e.clone(), e.clone()]),
        ),
        (
            "two non overlapping",
            vec![
                lcs(
                    bb(),
                    &[
                        vec![fs(0, 1, 1.0), fs(0, 2, 2.0)],
                        vec![fs(0, 3, 3.0), fs(0, 5, 5.0)],
                    ],
                ),
                lcs(
                    bb(),
                    &[vec![fs(0, 7, 7.0), fs(0, 9, 9.0)], vec![fs(0, 10, 10.0)]],
                ),
            ],
            lcs(
                bb(),
                &[
                    vec![fs(0, 1, 1.0), fs(0, 2, 2.0)],
                    vec![fs(0, 3, 3.0), fs(0, 5, 5.0)],
                    vec![fs(0, 7, 7.0), fs(0, 9, 9.0)],
                    vec![fs(0, 10, 10.0)],
                ],
            ),
        ),
        (
            "two overlapping",
            vec![
                lcs(
                    bb(),
                    &[
                        vec![fs(0, 1, 1.0), fs(0, 2, 2.0)],
                        vec![fs(0, 3, 3.0), fs(0, 8, 8.0)],
                    ],
                ),
                lcs(
                    bb(),
                    &[vec![fs(0, 7, 7.0), fs(0, 9, 9.0)], vec![fs(0, 10, 10.0)]],
                ),
            ],
            lcs(
                bb(),
                &[
                    vec![fs(0, 1, 1.0), fs(0, 2, 2.0)],
                    vec![fs(0, 3, 3.0), fs(0, 8, 8.0)],
                    vec![fs(0, 7, 7.0), fs(0, 9, 9.0)],
                    vec![fs(0, 10, 10.0)],
                ],
            ),
        ),
        (
            "two duplicated",
            vec![
                lcs(
                    bb(),
                    &[vec![
                        fs(0, 1, 1.0),
                        fs(0, 2, 2.0),
                        fs(0, 3, 3.0),
                        fs(0, 5, 5.0),
                    ]],
                ),
                lcs(bb(), &[vec![fs(0, 2, 2.0), fs(0, 3, 3.0), fs(0, 5, 5.0)]]),
            ],
            lcs(
                bb(),
                &[
                    vec![fs(0, 1, 1.0), fs(0, 2, 2.0), fs(0, 3, 3.0), fs(0, 5, 5.0)],
                    vec![fs(0, 2, 2.0), fs(0, 3, 3.0), fs(0, 5, 5.0)],
                ],
            ),
        ),
        (
            "three overlapping",
            vec![
                lcs(
                    bb(),
                    &[vec![
                        fs(0, 1, 1.0),
                        fs(0, 2, 2.0),
                        fs(0, 3, 3.0),
                        fs(0, 5, 5.0),
                    ]],
                ),
                lcs(bb(), &[vec![fs(0, 2, 2.0), fs(0, 3, 3.0), fs(0, 6, 6.0)]]),
                lcs(bb(), &[vec![fs(0, 0, 0.0), fs(0, 4, 4.0)]]),
            ],
            lcs(
                bb(),
                &[
                    vec![fs(0, 1, 1.0), fs(0, 2, 2.0), fs(0, 3, 3.0), fs(0, 5, 5.0)],
                    vec![fs(0, 2, 2.0), fs(0, 3, 3.0), fs(0, 6, 6.0)],
                    vec![fs(0, 0, 0.0), fs(0, 4, 4.0)],
                ],
            ),
        ),
        (
            "three in chained overlap",
            vec![
                lcs(
                    bb(),
                    &[vec![
                        fs(0, 1, 1.0),
                        fs(0, 2, 2.0),
                        fs(0, 3, 3.0),
                        fs(0, 5, 5.0),
                    ]],
                ),
                lcs(bb(), &[vec![fs(0, 4, 4.0), fs(0, 6, 66.0)]]),
                lcs(bb(), &[vec![fs(0, 6, 6.0), fs(0, 10, 10.0)]]),
            ],
            lcs(
                bb(),
                &[
                    vec![fs(0, 1, 1.0), fs(0, 2, 2.0), fs(0, 3, 3.0), fs(0, 5, 5.0)],
                    vec![fs(0, 4, 4.0), fs(0, 6, 66.0)],
                    vec![fs(0, 6, 6.0), fs(0, 10, 10.0)],
                ],
            ),
        ),
        (
            "three in chained overlap complex",
            vec![
                lcs(
                    bb(),
                    &[
                        vec![fs(0, 0, 0.0), fs(0, 5, 5.0)],
                        vec![fs(0, 10, 10.0), fs(0, 15, 15.0)],
                    ],
                ),
                lcs(
                    bb(),
                    &[
                        vec![fs(0, 2, 2.0), fs(0, 20, 20.0)],
                        vec![fs(0, 25, 25.0), fs(0, 30, 30.0)],
                    ],
                ),
                lcs(
                    bb(),
                    &[
                        vec![fs(0, 18, 18.0), fs(0, 26, 26.0)],
                        vec![fs(0, 31, 31.0), fs(0, 35, 35.0)],
                    ],
                ),
            ],
            lcs(
                bb(),
                &[
                    vec![fs(0, 0, 0.0), fs(0, 5, 5.0)],
                    vec![fs(0, 10, 10.0), fs(0, 15, 15.0)],
                    vec![fs(0, 2, 2.0), fs(0, 20, 20.0)],
                    vec![fs(0, 25, 25.0), fs(0, 30, 30.0)],
                    vec![fs(0, 18, 18.0), fs(0, 26, 26.0)],
                    vec![fs(0, 31, 31.0), fs(0, 35, 35.0)],
                ],
            ),
        ),
        (
            "110 overlapping",
            vec![
                lcs(bb(), &[generate_samples(0, 110)]),
                lcs(bb(), &[generate_samples(60, 50)]),
            ],
            lcs(bb(), &[generate_samples(0, 110), generate_samples(60, 50)]),
        ),
        (
            "150 overlapping samples, simply concatenated and no splits",
            vec![
                lcs(bb(), &[generate_samples(0, 90)]),
                lcs(bb(), &[generate_samples(60, 90)]),
            ],
            lcs(bb(), &[generate_samples(0, 90), generate_samples(60, 90)]),
        ),
    ];
    for (name, input, expected) in cases {
        let merged = m(input);
        check_merged_chunks(name, &*merged, &*expected, false);
    }
}

#[test]
fn test_concatenating_chunk_iterator() {
    let chunk1 = chunk_from_samples(&[fs(0, 1, 10.0)]).unwrap();
    let chunk2 = chunk_from_samples(&[fs(0, 2, 20.0)]).unwrap();
    let chunk3 = chunk_from_samples(&[fs(0, 3, 30.0)]).unwrap();
    let test_error = Error::msg("something went wrong");

    type Its = Vec<BoxMetaIterator<'static>>;
    let cases: Vec<(&str, Its, Vec<Meta>, Option<Error>)> = vec![
        (
            "many successful iterators",
            vec![
                new_list_chunk_series_iterator(vec![chunk1.clone(), chunk2.clone()]),
                new_list_chunk_series_iterator(vec![chunk3.clone()]),
            ],
            vec![chunk1.clone(), chunk2.clone(), chunk3.clone()],
            None,
        ),
        (
            "single failing iterator",
            vec![Box::new(ErrChunksIterator::new(test_error.clone()))],
            vec![],
            Some(test_error.clone()),
        ),
        (
            "some failing and some successful iterators",
            vec![
                new_list_chunk_series_iterator(vec![chunk1.clone(), chunk2.clone()]),
                Box::new(ErrChunksIterator::new(test_error.clone())),
                new_list_chunk_series_iterator(vec![chunk3.clone()]),
            ],
            // Should stop before advancing to last iterator.
            vec![chunk1.clone(), chunk2.clone()],
            Some(test_error.clone()),
        ),
    ];
    for (name, iterators, expected_chunks, expected_error) in cases {
        let mut it = ConcatenatingChunkIterator::new(iterators);
        let mut chks = Vec::new();
        while it.next() {
            chks.push(it.at().clone());
        }
        assert_metas_eq(&expected_chunks, &chks, name);
        match expected_error {
            None => assert!(it.err().is_none(), "{}", name),
            Some(e) => assert_eq!(
                it.err().map(|e| e.to_string()),
                Some(e.to_string()),
                "{}",
                name
            ),
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Chain sample iterator.

type SampleFn = fn(i64) -> Sample;

fn sample_funcs() -> [(&'static str, SampleFn); 3] {
    [
        ("float", |ts| fs(-ts, ts, ts as f64)),
        ("histogram", |ts| histogram_sample(ts, UK)),
        ("float histogram", |ts| float_histogram_sample(ts, UK)),
    ]
}

fn iterators(inputs: &[Vec<Sample>]) -> Vec<AnyIterator<'_>> {
    inputs.iter().map(|s| new_list_series_iterator(s)).collect()
}

#[test]
fn test_chain_sample_iterator() {
    for (sample_type, sf) in sample_funcs() {
        let cases: Vec<(&str, Vec<Vec<Sample>>, Vec<Sample>)> = vec![
            (
                "single iterator",
                vec![vec![sf(0), sf(1)]],
                vec![sf(0), sf(1)],
            ),
            (
                "non overlapping iterators",
                vec![vec![sf(0), sf(1)], vec![sf(2), sf(3)]],
                vec![sf(0), sf(1), sf(2), sf(3)],
            ),
            (
                "overlapping but distinct iterators",
                vec![vec![sf(0), sf(3)], vec![sf(1), sf(4)], vec![sf(2), sf(5)]],
                vec![sf(0), sf(1), sf(2), sf(3), sf(4), sf(5)],
            ),
            (
                "overlapping iterators",
                vec![
                    vec![sf(0), sf(1)],
                    vec![sf(0), sf(2)],
                    vec![sf(2), sf(3)],
                    vec![],
                    vec![],
                    vec![],
                ],
                vec![sf(0), sf(1), sf(2), sf(3)],
            ),
        ];
        for (name, input, expected) in cases {
            let mut merged = chain_sample_iterator_from_iterators(None, iterators(&input));
            let actual = expand_samples(&mut merged).expect("no error");
            assert_eq!(expected, actual, "{}/{}", sample_type, name);
        }
    }
}

#[test]
fn test_chain_sample_iterator_histogram_counter_reset_hint() {
    for (sample_type, sf) in hint_sample_funcs() {
        let cases: Vec<(&str, Vec<Vec<Sample>>, Vec<Sample>)> = vec![
            (
                "single iterator",
                vec![vec![sf(0, CR), sf(1, CR), sf(2, UK)]],
                vec![sf(0, UK), sf(1, CR), sf(2, UK)],
            ),
            (
                "single iterator gauge",
                vec![vec![sf(0, GA), sf(1, GA), sf(2, GA)]],
                vec![sf(0, GA), sf(1, GA), sf(2, GA)],
            ),
            (
                "overlapping iterators gauge",
                vec![
                    vec![sf(0, GA), sf(1, GA), sf(2, GA), sf(4, GA)],
                    vec![sf(0, GA), sf(1, GA), sf(3, GA), sf(5, GA)],
                ],
                vec![
                    sf(0, GA),
                    sf(1, GA),
                    sf(2, GA),
                    sf(3, GA),
                    sf(4, GA),
                    sf(5, GA),
                ],
            ),
            (
                "non overlapping iterators",
                vec![vec![sf(0, CR), sf(1, UK)], vec![sf(2, CR), sf(3, CR)]],
                vec![sf(0, UK), sf(1, UK), sf(2, UK), sf(3, CR)],
            ),
            (
                "overlapping but distinct iterators",
                vec![
                    vec![sf(0, CR), sf(3, UK), sf(5, CR)],
                    vec![sf(1, UK), sf(2, CR), sf(4, CR)],
                ],
                vec![
                    sf(0, UK),
                    sf(1, UK),
                    sf(2, CR),
                    sf(3, UK),
                    sf(4, UK),
                    sf(5, UK),
                ],
            ),
            (
                "overlapping iterators",
                vec![
                    vec![sf(0, CR), sf(1, CR), sf(2, CR)],
                    vec![sf(0, CR), sf(1, CR), sf(2, CR)],
                ],
                vec![sf(0, UK), sf(1, UK), sf(2, UK)],
            ),
        ];
        for (name, input, expected) in cases {
            let mut merged = chain_sample_iterator_from_iterators(None, iterators(&input));
            let actual = expand_samples(&mut merged).expect("no error");
            assert_eq!(expected, actual, "{}/{}", sample_type, name);
        }
    }
}

/// Seeks, then reads the sample at the position and the rest (as the Go seek tests do).
fn seek_and_expand(merged: &mut AnyIterator<'_>, seek: i64) -> Vec<Sample> {
    let mut actual = Vec::new();
    match merged.seek(seek) {
        ValueType::Float => {
            let (t, f) = merged.at();
            actual.push(Sample::float(merged.at_st(), t, f));
        }
        ValueType::Histogram => {
            let (t, h): (i64, Histogram) = merged.at_histogram(None);
            actual.push(Sample::histogram(merged.at_st(), t, h));
        }
        ValueType::FloatHistogram => {
            let (t, fh): (i64, FloatHistogram) = merged.at_float_histogram(None);
            actual.push(Sample::float_histogram(merged.at_st(), t, fh));
        }
        ValueType::None => {}
    }
    actual.extend(expand_samples(merged).expect("no error"));
    actual
}

#[test]
fn test_chain_sample_iterator_seek() {
    for (sample_type, sf) in sample_funcs() {
        let cases: Vec<(&str, Vec<Vec<Sample>>, i64, Vec<Sample>)> = vec![
            (
                "single iterator",
                vec![vec![sf(0), sf(1), sf(2)]],
                1,
                vec![sf(1), sf(2)],
            ),
            (
                "non overlapping iterators",
                vec![vec![sf(0), sf(1)], vec![sf(2), sf(3)]],
                2,
                vec![sf(2), sf(3)],
            ),
            (
                "overlapping but distinct iterators",
                vec![vec![sf(0), sf(3)], vec![sf(1), sf(4)], vec![sf(2), sf(5)]],
                2,
                vec![sf(2), sf(3), sf(4), sf(5)],
            ),
            (
                "overlapping iterators",
                vec![vec![sf(0), sf(2), sf(3)], vec![sf(0), sf(1), sf(2)]],
                0,
                vec![sf(0), sf(1), sf(2), sf(3)],
            ),
        ];
        for (name, input, seek, expected) in cases {
            let mut merged = chain_sample_iterator_from_iterators(None, iterators(&input));
            let actual = seek_and_expand(&mut merged, seek);
            assert_eq!(expected, actual, "{}/{}", sample_type, name);
        }
    }
}

/// Go `errIterator`.
struct ErrIterator(chunkenc::Error);

impl ChunkIterator for ErrIterator {
    fn next(&mut self) -> ValueType {
        ValueType::None
    }
    fn seek(&mut self, _: i64) -> ValueType {
        ValueType::None
    }
    fn at(&self) -> (i64, f64) {
        (0, 0.0)
    }
    fn at_histogram(&self, _: Option<Histogram>) -> (i64, Histogram) {
        (0, Histogram::default())
    }
    fn at_float_histogram(&self, _: Option<FloatHistogram>) -> (i64, FloatHistogram) {
        (0, FloatHistogram::default())
    }
    fn at_t(&self) -> i64 {
        0
    }
    fn at_st(&self) -> i64 {
        0
    }
    fn err(&self) -> Option<&chunkenc::Error> {
        Some(&self.0)
    }
}

fn err_iterator() -> AnyIterator<'static> {
    AnyIterator::Other(Box::new(ErrIterator(chunkenc::Error::msg(
        "something went wrong",
    ))))
}

#[test]
fn test_chain_sample_iterator_seek_failing_iterator() {
    let s = vec![fs(0, 0, 0.1), fs(0, 1, 1.1), fs(0, 2, 2.1)];
    let mut merged = chain_sample_iterator_from_iterators(
        None,
        vec![new_list_series_iterator(&s), err_iterator()],
    );
    assert_eq!(merged.seek(0), ValueType::None);
    assert_eq!(
        merged.err().map(|e| e.to_string()),
        Some("something went wrong".to_string())
    );
}

#[test]
fn test_chain_sample_iterator_next_immediately_failing_iterator() {
    let s = vec![fs(0, 0, 0.1), fs(0, 1, 1.1), fs(0, 2, 2.1)];
    let mut merged = chain_sample_iterator_from_iterators(
        None,
        vec![new_list_series_iterator(&s), err_iterator()],
    );
    assert_eq!(merged.next(), ValueType::None);
    assert_eq!(
        merged.err().map(|e| e.to_string()),
        Some("something went wrong".to_string())
    );

    // Next() does some special handling for the first iterator, so make sure it handles the first iterator returning
    // an error too.
    let mut merged = chain_sample_iterator_from_iterators(
        None,
        vec![err_iterator(), new_list_series_iterator(&s)],
    );
    assert_eq!(merged.next(), ValueType::None);
    assert_eq!(
        merged.err().map(|e| e.to_string()),
        Some("something went wrong".to_string())
    );
}

#[test]
fn test_chain_sample_iterator_seek_histogram_counter_reset_hint() {
    for (sample_type, sf) in hint_sample_funcs() {
        let cases: Vec<(&str, Vec<Vec<Sample>>, i64, Vec<Sample>)> = vec![
            (
                "single iterator",
                vec![vec![sf(0, CR), sf(1, CR), sf(2, UK)]],
                1,
                vec![sf(1, UK), sf(2, UK)],
            ),
            (
                "non overlapping iterators",
                vec![vec![sf(0, CR), sf(1, UK)], vec![sf(2, CR), sf(3, CR)]],
                2,
                vec![sf(2, UK), sf(3, CR)],
            ),
            (
                "non overlapping iterators seek to internal reset",
                vec![vec![sf(0, CR), sf(1, UK)], vec![sf(2, CR), sf(3, CR)]],
                3,
                vec![sf(3, UK)],
            ),
        ];
        for (name, input, seek, expected) in cases {
            let mut merged = chain_sample_iterator_from_iterators(None, iterators(&input));
            let actual = seek_and_expand(&mut merged, seek);
            assert_eq!(expected, actual, "{}/{}", sample_type, name);
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Merge querier with secondaries.

/// Go `visitMockQueriers`: counts the mock queriers in a merge tree.
// adapted: Go type-switches through the adapters; Rust walks the hidden `children()` of the queriers (a querier
// without children is a leaf). The checks run on the shared states the test keeps.
fn count_leaf_queriers(q: &dyn Querier) -> usize {
    let children = q.children();
    if children.is_empty() {
        return 1;
    }
    children.into_iter().map(count_leaf_queriers).sum()
}

/// Go `require.Subset(list, subset)` for annotations: every annotation of `subset` is in `list`.
fn assert_annotations_subset(list: &Annotations, subset: &Annotations, msg: &str) {
    for e in subset.iter() {
        assert_eq!(
            list.get(&e.to_string()),
            Some(e),
            "{}: unexpected warning {}",
            msg,
            e
        );
    }
}

/// Go `require.ErrorIs(err, target)` (both may be nil).
fn assert_error_is(err: &Option<Error>, target: &Option<Error>, msg: &str) {
    match (err, target) {
        (None, None) => {}
        (Some(e), Some(t)) => assert!(e.is(t), "{}: {} is not {}", msg, e, t),
        _ => panic!("{}: expected error {:?}, got {:?}", msg, target, err),
    }
}

/// Go `requireEqualSlice`: compares slices, ignoring the difference between nil and empty.
fn require_equal_slice(a: &[String], b: &[String], msg: &str) {
    assert_eq!(a, b, "{}", msg);
}

#[test]
fn test_merge_querier_with_secondaries_error_handling() {
    let err_storage = Error::msg("storage error");
    let warn_storage = Error::msg("storage warning");
    let ctx = Context::background();

    struct MockSpec {
        resp: Vec<String>,
        warnings: Annotations,
        err: Option<Error>,
    }
    let spec = |resp: &[&str], warnings: Annotations, err: Option<Error>| MockSpec {
        resp: strs(resp),
        warnings,
        err,
    };
    let w = |e: &Error| Annotations::new().with(e.clone());

    struct Case {
        name: &'static str,
        primaries: Vec<MockSpec>,
        secondaries: Vec<MockSpec>,
        limit: i64,
        expected_selects_series: Vec<Labels>,
        expected_labels: Vec<String>,
        expected_warnings: Annotations,
        expected_errs: [Option<Error>; 4],
    }
    let none4 = || [None, None, None, None];
    let all4 = |e: &Error| {
        [
            Some(e.clone()),
            Some(e.clone()),
            Some(e.clone()),
            Some(e.clone()),
        ]
    };
    let t = |v: &str| ls(&["test", v]);

    let cases = vec![
        Case {
            name: "one successful primary querier",
            primaries: vec![spec(&["a", "b"], Annotations::new(), None)],
            secondaries: vec![],
            limit: 0,
            expected_selects_series: vec![t("a"), t("b")],
            expected_labels: strs(&["a", "b"]),
            expected_warnings: Annotations::new(),
            expected_errs: none4(),
        },
        Case {
            name: "multiple successful primary queriers",
            primaries: vec![
                spec(&["a", "b"], Annotations::new(), None),
                spec(&["b", "c"], Annotations::new(), None),
            ],
            secondaries: vec![],
            limit: 0,
            expected_selects_series: vec![t("a"), t("b"), t("c")],
            expected_labels: strs(&["a", "b", "c"]),
            expected_warnings: Annotations::new(),
            expected_errs: none4(),
        },
        Case {
            name: "one failed primary querier",
            primaries: vec![spec(&[], Annotations::new(), Some(err_storage.clone()))],
            secondaries: vec![],
            limit: 0,
            expected_selects_series: vec![],
            expected_labels: vec![],
            expected_warnings: Annotations::new(),
            expected_errs: all4(&err_storage),
        },
        Case {
            name: "one successful primary querier with successful secondaries",
            primaries: vec![spec(&["a", "b"], Annotations::new(), None)],
            secondaries: vec![
                spec(&["b"], Annotations::new(), None),
                spec(&["c"], Annotations::new(), None),
            ],
            limit: 0,
            expected_selects_series: vec![t("a"), t("b"), t("c")],
            expected_labels: strs(&["a", "b", "c"]),
            expected_warnings: Annotations::new(),
            expected_errs: none4(),
        },
        Case {
            name: "one successful primary querier with empty response and successful secondaries",
            primaries: vec![spec(&[], Annotations::new(), None)],
            secondaries: vec![
                spec(&["b"], Annotations::new(), None),
                spec(&["c"], Annotations::new(), None),
            ],
            limit: 0,
            expected_selects_series: vec![t("b"), t("c")],
            expected_labels: strs(&["b", "c"]),
            expected_warnings: Annotations::new(),
            expected_errs: none4(),
        },
        Case {
            name: "one failed primary querier with successful secondaries",
            primaries: vec![spec(&[], Annotations::new(), Some(err_storage.clone()))],
            secondaries: vec![
                spec(&["b"], Annotations::new(), None),
                spec(&["c"], Annotations::new(), None),
            ],
            limit: 0,
            expected_selects_series: vec![],
            expected_labels: vec![],
            expected_warnings: Annotations::new(),
            expected_errs: all4(&err_storage),
        },
        Case {
            name: "nil primary querier with failed secondary",
            primaries: vec![],
            secondaries: vec![spec(&["b"], Annotations::new(), Some(err_storage.clone()))],
            limit: 0,
            expected_selects_series: vec![],
            expected_labels: vec![],
            expected_warnings: w(&err_storage),
            expected_errs: none4(),
        },
        Case {
            name: "nil primary querier with two failed secondaries",
            primaries: vec![],
            secondaries: vec![
                spec(&["b"], Annotations::new(), Some(err_storage.clone())),
                spec(&["c"], Annotations::new(), Some(err_storage.clone())),
            ],
            limit: 0,
            expected_selects_series: vec![],
            expected_labels: vec![],
            expected_warnings: w(&err_storage),
            expected_errs: none4(),
        },
        Case {
            name: "one successful primary querier with failed secondaries",
            primaries: vec![spec(&["a"], Annotations::new(), None)],
            secondaries: vec![
                spec(&["b"], Annotations::new(), Some(err_storage.clone())),
                spec(&["c"], Annotations::new(), Some(err_storage.clone())),
            ],
            limit: 0,
            expected_selects_series: vec![t("a")],
            expected_labels: strs(&["a"]),
            expected_warnings: w(&err_storage),
            expected_errs: none4(),
        },
        Case {
            name: "successful queriers with warnings",
            primaries: vec![spec(&["a"], w(&warn_storage), None)],
            secondaries: vec![spec(&["b"], w(&warn_storage), None)],
            limit: 0,
            expected_selects_series: vec![t("a"), t("b")],
            expected_labels: strs(&["a", "b"]),
            expected_warnings: w(&warn_storage),
            expected_errs: none4(),
        },
        Case {
            name: "successful queriers with limit",
            primaries: vec![spec(&["a", "d"], w(&warn_storage), None)],
            secondaries: vec![spec(&["b", "c"], w(&warn_storage), None)],
            limit: 2,
            expected_selects_series: vec![t("a"), t("b")],
            expected_labels: strs(&["a", "b"]),
            expected_warnings: w(&warn_storage),
            expected_errs: none4(),
        },
    ];

    for tc in cases {
        let (label_hints, select_hints) = if tc.limit > 0 {
            (
                Some(LabelHints {
                    limit: tc.limit,
                    ..Default::default()
                }),
                Some(SelectHints {
                    limit: tc.limit,
                    ..Default::default()
                }),
            )
        } else {
            (None, None)
        };
        let mut states = Vec::new();
        let mut make = |specs: Vec<MockSpec>| -> Vec<BoxQuerier> {
            specs
                .into_iter()
                .map(|s| {
                    let q = MockQuerier {
                        resp: s.resp,
                        warnings: s.warnings,
                        err: s.err,
                        ..Default::default()
                    };
                    states.push(q.state.clone());
                    Box::new(q) as BoxQuerier
                })
                .collect()
        };
        let n_queriers = tc.primaries.len() + tc.secondaries.len();
        let primaries = make(tc.primaries);
        let secondaries = make(tc.secondaries);
        let first_merge: VerticalSeriesMergeFunc = Arc::new(|s: Vec<SeriesArc>| s[0].clone());
        let q = new_merge_querier(primaries, secondaries, first_merge);
        let name = tc.name;

        // Select.
        {
            let mut res = q.select(&Context::background(), false, select_hints.as_ref(), &[]);
            let mut lbls = Vec::new();
            while res.next() {
                lbls.push(res.at().labels().clone());
            }
            assert_annotations_subset(&tc.expected_warnings, &res.warnings(), name);
            assert_eq!(tc.expected_errs[0], res.err(), "{}", name);
            assert_error_is(&res.err(), &tc.expected_errs[0], name);
            assert_eq!(tc.expected_selects_series, lbls, "{}", name);

            // Single queries should be unsorted; merged queries sorted.
            let exp = n_queriers > 1;
            for s in &states {
                assert_eq!(
                    s.lock().unwrap().sorted_series_requested,
                    vec![exp],
                    "{}",
                    name
                );
            }
            // Check we visited all queriers.
            assert_eq!(count_leaf_queriers(&*q), n_queriers, "{}", name);
        }
        // LabelNames.
        {
            let (res, w, err) = q.label_names(&ctx, label_hints.as_ref(), &[]);
            assert_annotations_subset(&tc.expected_warnings, &w, name);
            assert_error_is(&err, &tc.expected_errs[1], name);
            require_equal_slice(&tc.expected_labels, &res, name);
            if err.is_none() {
                for s in &states {
                    assert_eq!(s.lock().unwrap().label_names_calls, 1, "{}", name);
                }
            }
        }
        // LabelValues.
        let lvs = {
            let (res, w, err) = q.label_values(&ctx, "test", label_hints.as_ref(), &[]);
            assert_annotations_subset(&tc.expected_warnings, &w, name);
            assert_error_is(&err, &tc.expected_errs[2], name);
            require_equal_slice(&tc.expected_labels, &res, name);
            if err.is_none() {
                for s in &states {
                    assert_eq!(
                        s.lock().unwrap().label_names_requested,
                        vec![LabelNameRequest {
                            name: "test".to_string(),
                            matchers: vec![]
                        }],
                        "{}",
                        name
                    );
                }
            }
            err.is_none()
        };
        // LabelValuesWithMatchers.
        if lvs {
            let matcher = Matcher::must_new(MatchType::Equal, "otherLabel", "someValue");
            let (res, w, err) = q.label_values(
                &ctx,
                "test2",
                label_hints.as_ref(),
                std::slice::from_ref(&matcher),
            );
            assert_annotations_subset(&tc.expected_warnings, &w, name);
            assert_error_is(&err, &tc.expected_errs[3], name);
            require_equal_slice(&tc.expected_labels, &res, name);
            if err.is_none() {
                for s in &states {
                    assert_eq!(
                        s.lock().unwrap().label_names_requested,
                        vec![
                            LabelNameRequest {
                                name: "test".to_string(),
                                matchers: vec![]
                            },
                            LabelNameRequest {
                                name: "test2".to_string(),
                                matchers: vec![(
                                    MatchType::Equal,
                                    "otherLabel".to_string(),
                                    "someValue".to_string()
                                )],
                            },
                        ],
                        "{}",
                        name
                    );
                }
            }
        } else {
            // adapted: Go runs this subtest even after a LabelValues error; the error checks are the same.
            let matcher = Matcher::must_new(MatchType::Equal, "otherLabel", "someValue");
            let (res, w, err) = q.label_values(
                &ctx,
                "test2",
                label_hints.as_ref(),
                std::slice::from_ref(&matcher),
            );
            assert_annotations_subset(&tc.expected_warnings, &w, name);
            assert_error_is(&err, &tc.expected_errs[3], name);
            require_equal_slice(&tc.expected_labels, &res, name);
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Search.

/// Go `searchQuerier`: a querier that is also a searcher.
#[derive(Default)]
struct SearchQuerier {
    base: MockQuerier,
    names: Vec<SearchResult>,
    values: Vec<SearchResult>,
    search_warns: Annotations,
    err: Option<Error>,
}

impl LabelQuerier for SearchQuerier {
    fn label_values(
        &self,
        ctx: &Context,
        name: &str,
        h: Option<&LabelHints>,
        m: &[Matcher],
    ) -> LabelResult {
        self.base.label_values(ctx, name, h, m)
    }
    fn label_names(&self, ctx: &Context, h: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.base.label_names(ctx, h, m)
    }
    fn close(&mut self) -> Option<Error> {
        self.base.close()
    }
}

impl Querier for SearchQuerier {
    fn select(
        &self,
        _: &Context,
        sort: bool,
        _: Option<&SelectHints>,
        _: &[Matcher],
    ) -> BoxSeriesSet {
        self.base.do_select(sort)
    }
    fn as_searcher(&self) -> Option<&dyn Searcher> {
        Some(self)
    }
}

impl Searcher for SearchQuerier {
    fn search_label_names<'a>(
        &'a self,
        _: &Context,
        _: Option<&SearchHints>,
        _: &[Matcher],
    ) -> BoxSearchResultSet<'a> {
        if let Some(err) = &self.err {
            return err_search_result_set(err.clone(), vec![]);
        }
        new_search_result_set_from_slice(self.names.clone(), self.search_warns.clone())
    }
    fn search_label_values<'a>(
        &'a self,
        _: &Context,
        _: &str,
        _: Option<&SearchHints>,
        _: &[Matcher],
    ) -> BoxSearchResultSet<'a> {
        if let Some(err) = &self.err {
            return err_search_result_set(err.clone(), vec![]);
        }
        new_search_result_set_from_slice(self.values.clone(), self.search_warns.clone())
    }
}

/// Go `partialErrSearchQuerier`: yields partial results, then an error.
#[derive(Default)]
struct PartialErrSearchQuerier {
    base: MockQuerier,
    names: Vec<SearchResult>,
    values: Vec<SearchResult>,
    err: Option<Error>,
}

impl LabelQuerier for PartialErrSearchQuerier {
    fn label_values(
        &self,
        ctx: &Context,
        name: &str,
        h: Option<&LabelHints>,
        m: &[Matcher],
    ) -> LabelResult {
        self.base.label_values(ctx, name, h, m)
    }
    fn label_names(&self, ctx: &Context, h: Option<&LabelHints>, m: &[Matcher]) -> LabelResult {
        self.base.label_names(ctx, h, m)
    }
    fn close(&mut self) -> Option<Error> {
        self.base.close()
    }
}

impl Querier for PartialErrSearchQuerier {
    fn select(
        &self,
        _: &Context,
        sort: bool,
        _: Option<&SelectHints>,
        _: &[Matcher],
    ) -> BoxSeriesSet {
        self.base.do_select(sort)
    }
    fn as_searcher(&self) -> Option<&dyn Searcher> {
        Some(self)
    }
}

impl Searcher for PartialErrSearchQuerier {
    fn search_label_names<'a>(
        &'a self,
        _: &Context,
        _: Option<&SearchHints>,
        _: &[Matcher],
    ) -> BoxSearchResultSet<'a> {
        new_search_result_set_from_slice_and_error(
            self.names.clone(),
            Annotations::new(),
            self.err.clone(),
        )
    }
    fn search_label_values<'a>(
        &'a self,
        _: &Context,
        _: &str,
        _: Option<&SearchHints>,
        _: &[Matcher],
    ) -> BoxSearchResultSet<'a> {
        new_search_result_set_from_slice_and_error(
            self.values.clone(),
            Annotations::new(),
            self.err.clone(),
        )
    }
}

fn sr(v: &str, s: f64) -> SearchResult {
    SearchResult {
        value: v.to_string(),
        score: s,
    }
}

fn sq(names: Vec<SearchResult>, values: Vec<SearchResult>) -> BoxQuerier {
    Box::new(SearchQuerier {
        names,
        values,
        ..Default::default()
    })
}

fn sq_names(names: Vec<SearchResult>) -> BoxQuerier {
    sq(names, vec![])
}

fn sq_err(err: Error) -> BoxQuerier {
    Box::new(SearchQuerier {
        err: Some(err),
        ..Default::default()
    })
}

fn mock() -> BoxQuerier {
    Box::new(MockQuerier::default())
}

/// Go `collectSearchResults`.
fn collect_search_results(rs: &mut dyn SearchResultSet) -> Vec<SearchResult> {
    let mut got = Vec::new();
    while rs.next() {
        got.push(rs.at().clone());
    }
    assert!(rs.err().is_none(), "unexpected error {:?}", rs.err());
    assert!(rs.close().is_none());
    got
}

/// Go `NewMergeQuerier([]Querier{a, b}, nil, ChainedSeriesMerge)` (forces the merge path).
fn new_merged(a: BoxQuerier, b: BoxQuerier) -> BoxQuerier {
    new_merge_querier(vec![a, b], vec![], chained())
}

fn searcher(q: &dyn Querier) -> &dyn Searcher {
    q.as_searcher().expect("a searcher")
}

#[test]
fn test_merge_querier_search() {
    let ctx = Context::background();

    // Results from both searchers are merged.
    {
        let merged = new_merged(
            sq(vec![sr("env", 0.9)], vec![sr("prod", 1.0)]),
            sq(vec![sr("job", 0.5)], vec![sr("dev", 0.8)]),
        );
        let got =
            collect_search_results(&mut *searcher(&*merged).search_label_names(&ctx, None, &[]));
        assert_eq!(got.len(), 2);
        let got = collect_search_results(&mut *searcher(&*merged).search_label_values(
            &ctx,
            "env",
            None,
            &[],
        ));
        assert_eq!(got.len(), 2);
    }

    // Duplicate values keep max score.
    {
        let merged = new_merged(
            sq_names(vec![sr("env", 0.6), sr("job", 0.9)]),
            sq_names(vec![sr("env", 0.8), sr("region", 0.5)]),
        );
        let got =
            collect_search_results(&mut *searcher(&*merged).search_label_names(&ctx, None, &[]));
        assert_eq!(got.len(), 3);
        let score = |v: &str| got.iter().find(|r| r.value == v).map(|r| r.score);
        // "env" appears in both; max score wins.
        assert_eq!(score("env"), Some(0.8));
        assert_eq!(score("job"), Some(0.9));
        assert_eq!(score("region"), Some(0.5));
    }

    // Natural order is preserved for merged searchers.
    {
        let merged = new_merged(
            sq_names(vec![sr("region", 0.5), sr("zone", 0.4)]),
            sq_names(vec![sr("env", 0.8), sr("job", 0.9)]),
        );
        let got =
            collect_search_results(&mut *searcher(&*merged).search_label_names(&ctx, None, &[]));
        assert_eq!(
            got,
            vec![
                sr("env", 0.8),
                sr("job", 0.9),
                sr("region", 0.5),
                sr("zone", 0.4)
            ]
        );
    }

    // Secondary search errors become warnings.
    {
        let merged = new_merge_querier(
            vec![sq_names(vec![sr("env", 1.0)])],
            vec![sq_err(Error::msg("secondary search failed"))],
            chained(),
        );
        let mut rs = searcher(&*merged).search_label_names(&ctx, None, &[]);
        let got = collect_search_results(&mut *rs);
        assert_eq!(got, vec![sr("env", 1.0)]);
        let warnings = rs.warnings().as_errors();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].to_string().contains("secondary search failed"));
    }

    // Secondary partial label names followed by error become warnings.
    {
        let secondary: BoxQuerier = Box::new(PartialErrSearchQuerier {
            names: vec![sr("zone", 0.5)],
            err: Some(Error::msg("partial secondary failure")),
            ..Default::default()
        });
        let merged = new_merge_querier(
            vec![sq_names(vec![sr("env", 1.0)])],
            vec![secondary],
            chained(),
        );
        let mut rs = searcher(&*merged).search_label_names(&ctx, None, &[]);
        while rs.next() {}
        assert!(rs.err().is_none());
        let warnings = rs.warnings().as_errors();
        assert_eq!(warnings.len(), 1);
        assert!(
            warnings[0]
                .to_string()
                .contains("partial secondary failure")
        );
        assert!(rs.close().is_none());
    }

    // Warnings accessible after early Close: the internal warningsOnErrorSearchSet lifecycle when the caller
    // closes before exhaustion.
    {
        let inner = new_search_result_set_from_slice_and_error(
            vec![sr("zone", 0.5)],
            Annotations::new(),
            Some(Error::msg("early close failure")),
        );
        let mut rs = warnings_on_error_search_result_set(inner);
        assert!(rs.next());
        // Do not call Next again; close early.
        assert!(rs.close().is_none());
        // Close must not panic and Warnings must be callable.
        assert!(rs.err().is_none());
        let _ = rs.warnings();
        // Calling Close a second time must be a no-op.
        assert!(rs.close().is_none());
    }

    // Secondary partial label values followed by error become warnings.
    {
        let secondary: BoxQuerier = Box::new(PartialErrSearchQuerier {
            values: vec![sr("staging", 0.5)],
            err: Some(Error::msg("partial secondary failure")),
            ..Default::default()
        });
        let merged = new_merge_querier(
            vec![sq(vec![], vec![sr("prod", 1.0)])],
            vec![secondary],
            chained(),
        );
        let mut rs = searcher(&*merged).search_label_values(&ctx, "env", None, &[]);
        while rs.next() {}
        assert!(rs.err().is_none());
        let warnings = rs.warnings().as_errors();
        assert_eq!(warnings.len(), 1);
        assert!(
            warnings[0]
                .to_string()
                .contains("partial secondary failure")
        );
        assert!(rs.close().is_none());
    }

    let unsupported = ERR_SECONDARY_SEARCH_UNSUPPORTED.to_string();
    struct Case {
        name: &'static str,
        primaries: Vec<BoxQuerier>,
        secondaries: Vec<BoxQuerier>,
        expected: bool,
        expected_names: Vec<SearchResult>,
        expected_values: Vec<SearchResult>,
        warnings: Vec<String>,
    }
    let case = |name, primaries, secondaries, expected, warnings: Vec<String>| Case {
        name,
        primaries,
        secondaries,
        expected,
        expected_names: vec![],
        expected_values: vec![],
        warnings,
    };
    let empty_sq = || sq(vec![], vec![]);
    let cases = vec![
        case(
            "supported nested primary",
            vec![new_merged(empty_sq(), empty_sq()), empty_sq()],
            vec![],
            true,
            vec![],
        ),
        case(
            "unsupported nested primary",
            vec![new_merged(empty_sq(), mock()), empty_sq()],
            vec![],
            false,
            vec![],
        ),
        case(
            "supported secondary only",
            vec![],
            vec![empty_sq()],
            true,
            vec![],
        ),
        case(
            "unsupported secondary only",
            vec![],
            vec![mock()],
            true,
            vec![unsupported.clone()],
        ),
        case(
            "multiple unsupported secondaries deduplicate warnings",
            vec![],
            vec![mock(), mock()],
            true,
            vec![unsupported.clone()],
        ),
        case(
            "supported nested secondary",
            vec![],
            vec![new_merged(empty_sq(), empty_sq())],
            true,
            vec![],
        ),
        case(
            "unsupported nested primary merge used as a secondary",
            vec![],
            vec![new_merged(empty_sq(), mock())],
            true,
            vec![unsupported.clone()],
        ),
        Case {
            name: "nested secondary-only merge retains supported results",
            primaries: vec![],
            secondaries: vec![new_merge_querier(
                vec![],
                vec![
                    sq(vec![sr("region", 1.0)], vec![sr("staging", 1.0)]),
                    mock(),
                ],
                chained(),
            )],
            expected: true,
            expected_names: vec![sr("region", 1.0)],
            expected_values: vec![sr("staging", 1.0)],
            warnings: vec![unsupported.clone()],
        },
        Case {
            name: "primary results survive nested secondary warnings",
            primaries: vec![sq(vec![sr("env", 1.0)], vec![sr("prod", 1.0)])],
            secondaries: vec![new_merge_querier(
                vec![],
                vec![
                    Box::new(SearchQuerier {
                        names: vec![sr("region", 1.0)],
                        values: vec![sr("staging", 1.0)],
                        search_warns: Annotations::new().with(Error::msg("existing warning")),
                        ..Default::default()
                    }),
                    mock(),
                    sq_err(Error::msg("secondary failed")),
                ],
                chained(),
            )],
            expected: true,
            expected_names: vec![sr("env", 1.0), sr("region", 1.0)],
            expected_values: vec![sr("prod", 1.0), sr("staging", 1.0)],
            warnings: vec![
                unsupported.clone(),
                "existing warning".to_string(),
                "secondary failed".to_string(),
            ],
        },
        case(
            "unsupported nested primary group is omitted as a whole",
            vec![],
            vec![new_merged(
                sq(vec![sr("region", 1.0)], vec![sr("staging", 1.0)]),
                mock(),
            )],
            true,
            vec![unsupported.clone()],
        ),
        case(
            "mixed search support is not advertised",
            vec![empty_sq(), mock()],
            vec![],
            false,
            vec![],
        ),
        case(
            "no searcher queriers is not advertised",
            vec![mock(), mock()],
            vec![],
            false,
            vec![],
        ),
        case(
            "unsupported secondary returns a warning",
            vec![empty_sq()],
            vec![mock()],
            true,
            vec![unsupported.clone()],
        ),
        case(
            "noop secondary does not prevent search capability",
            vec![empty_sq()],
            vec![noop_querier()],
            true,
            vec![],
        ),
        case(
            "empty merge returns a valid empty iterator",
            vec![],
            vec![],
            true,
            vec![],
        ),
        // adapted: Rust has no nil querier; the nil entries are left out.
        case(
            "all noop inputs return a valid empty iterator",
            vec![noop_querier()],
            vec![noop_querier()],
            true,
            vec![],
        ),
    ];

    for tc in cases {
        let merged = new_merge_querier(tc.primaries, tc.secondaries, chained());
        let s = merged.as_searcher();
        assert_eq!(s.is_some(), tc.expected, "{}", tc.name);
        let Some(s) = s else { continue };
        for order in [
            Ordering::ByValueAsc,
            Ordering::ByValueDesc,
            Ordering::ByScoreDesc,
        ] {
            for limit in [0i64, 1] {
                let hints = SearchHints {
                    order_by: order,
                    limit,
                    ..Default::default()
                };
                for method in ["names", "values"] {
                    let msg = format!("{}/order={:?}/limit={}/{}", tc.name, order, limit, method);
                    let (mut rs, exp) = if method == "names" {
                        (
                            s.search_label_names(&Context::background(), Some(&hints), &[]),
                            &tc.expected_names,
                        )
                    } else {
                        (
                            s.search_label_values(&Context::background(), "env", Some(&hints), &[]),
                            &tc.expected_values,
                        )
                    };
                    let mut expected = exp.clone();
                    if order == Ordering::ByValueDesc {
                        expected.reverse();
                    }
                    if limit > 0 && expected.len() as i64 > limit {
                        expected.truncate(limit as usize);
                    }
                    assert_eq!(expected, collect_search_results(&mut *rs), "{}", msg);
                    let mut warnings: Vec<String> =
                        rs.warnings().iter().map(|w| w.to_string()).collect();
                    let mut want = tc.warnings.clone();
                    warnings.sort();
                    want.sort();
                    assert_eq!(want, warnings, "{}", msg);
                }
            }
        }
    }

    // Secondary deadline remains a warning.
    {
        let merged = new_merge_querier(
            vec![sq_names(vec![sr("env", 1.0)])],
            vec![sq_err(ERR_DEADLINE_EXCEEDED)],
            chained(),
        );
        let mut rs = searcher(&*merged).search_label_names(&ctx, None, &[]);
        assert!(rs.next());
        assert_eq!(rs.at().value, "env");
        assert!(!rs.next());
        assert!(rs.err().is_none());
        let warnings = rs.warnings().as_errors();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].is(&ERR_DEADLINE_EXCEEDED));
        assert!(rs.close().is_none());
    }

    // OrderByScoreDesc overrides natural ordering. The inputs are already score-desc within each searcher.
    {
        let merged = new_merged(
            sq_names(vec![sr("job", 0.9), sr("env", 0.3)]),
            sq_names(vec![sr("region", 0.5)]),
        );
        let hints = SearchHints {
            order_by: Ordering::ByScoreDesc,
            ..Default::default()
        };
        let got = collect_search_results(&mut *searcher(&*merged).search_label_names(
            &ctx,
            Some(&hints),
            &[],
        ));
        assert_eq!(got, vec![sr("job", 0.9), sr("region", 0.5), sr("env", 0.3)]);
    }

    // OrderByScoreDesc error preserves warnings.
    {
        let mut ws = Annotations::new();
        ws.add(Error::msg("prior warning"));
        let q1: BoxQuerier = Box::new(SearchQuerier {
            names: vec![sr("env", 1.0)],
            search_warns: ws,
            ..Default::default()
        });
        let merged = new_merged(q1, sq_err(Error::msg("score path failure")));
        let hints = SearchHints {
            order_by: Ordering::ByScoreDesc,
            ..Default::default()
        };
        let mut rs = searcher(&*merged).search_label_names(&ctx, Some(&hints), &[]);
        while rs.next() {}
        let err = rs.err().expect("an error");
        assert!(err.to_string().contains("score path failure"));
        let warnings = rs.warnings().as_errors();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].to_string().contains("prior warning"));
        assert!(rs.close().is_none());
    }

    // OrderByValueDesc reverses natural ordering. Each searcher emits in descending value order.
    {
        let merged = new_merged(
            sq_names(vec![sr("job", 1.0), sr("env", 1.0)]),
            sq_names(vec![sr("region", 1.0), sr("cluster", 1.0)]),
        );
        let hints = SearchHints {
            order_by: Ordering::ByValueDesc,
            ..Default::default()
        };
        let got = collect_search_results(&mut *searcher(&*merged).search_label_names(
            &ctx,
            Some(&hints),
            &[],
        ));
        assert_eq!(
            got,
            vec![
                sr("region", 1.0),
                sr("job", 1.0),
                sr("env", 1.0),
                sr("cluster", 1.0)
            ]
        );
    }

    // Primary error preserves prior results and warnings.
    {
        let mut ws = Annotations::new();
        ws.add(Error::msg("prior warning"));
        let q1: BoxQuerier = Box::new(SearchQuerier {
            names: vec![sr("env", 1.0)],
            search_warns: ws,
            ..Default::default()
        });
        let merged = new_merged(q1, sq_err(Error::msg("primary failure")));
        let mut rs = searcher(&*merged).search_label_names(&ctx, None, &[]);
        // The successful searcher's results come through even though the other primary failed.
        assert!(rs.next());
        assert_eq!(*rs.at(), sr("env", 1.0));
        assert!(!rs.next());
        let err = rs.err().expect("an error");
        assert!(err.to_string().contains("primary failure"));
        // Warnings from the successful first searcher are preserved.
        let warnings = rs.warnings().as_errors();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].to_string().contains("prior warning"));
        assert!(rs.close().is_none());
    }

    // Partial primary failure mid-stream keeps surviving values.
    {
        let q1: BoxQuerier = Box::new(PartialErrSearchQuerier {
            names: vec![sr("alpha", 1.0), sr("beta", 1.0)],
            err: Some(Error::msg("a tail error")),
            ..Default::default()
        });
        let merged = new_merge_querier(
            vec![q1, sq_names(vec![sr("delta", 1.0)])],
            vec![],
            chained(),
        );
        let mut rs = searcher(&*merged).search_label_names(&ctx, None, &[]);
        let mut got = Vec::new();
        while rs.next() {
            got.push(rs.at().value.clone());
        }
        assert_eq!(got, strs(&["alpha", "beta", "delta"]));
        let err = rs.err().expect("an error");
        assert!(err.to_string().contains("a tail error"));
        assert!(rs.close().is_none());
    }

    // Limit is applied at merge level.
    {
        let merged = new_merged(
            sq_names(vec![sr("a", 1.0), sr("b", 0.9)]),
            sq_names(vec![sr("c", 0.8)]),
        );
        let hints = SearchHints {
            limit: 2,
            ..Default::default()
        };
        let got = collect_search_results(&mut *searcher(&*merged).search_label_names(
            &ctx,
            Some(&hints),
            &[],
        ));
        assert_eq!(got.len(), 2);
        // Alphabetical order, limited to 2.
        assert_eq!(got[0].value, "a");
        assert_eq!(got[1].value, "b");
    }

    // Empty searchers.
    {
        let merged = new_merged(empty_sq(), empty_sq());
        let got =
            collect_search_results(&mut *searcher(&*merged).search_label_names(&ctx, None, &[]));
        assert!(got.is_empty());
    }

    // Single searcher passthrough (two queriers, one empty, to force the merge path).
    {
        let merged = new_merged(
            sq_names(vec![sr("a", 1.0), sr("b", 1.0), sr("c", 1.0)]),
            empty_sq(),
        );
        let got =
            collect_search_results(&mut *searcher(&*merged).search_label_names(&ctx, None, &[]));
        assert_eq!(got, vec![sr("a", 1.0), sr("b", 1.0), sr("c", 1.0)]);
    }

    // Overlapping values are deduplicated.
    {
        let merged = new_merged(
            sq_names(vec![sr("a", 1.0), sr("b", 1.0)]),
            sq_names(vec![sr("b", 1.0), sr("c", 1.0)]),
        );
        let got =
            collect_search_results(&mut *searcher(&*merged).search_label_names(&ctx, None, &[]));
        assert_eq!(got, vec![sr("a", 1.0), sr("b", 1.0), sr("c", 1.0)]);
    }

    // Interleaved values with limit.
    {
        let merged = new_merged(
            sq_names(vec![sr("a", 1.0), sr("c", 1.0), sr("e", 1.0)]),
            sq_names(vec![sr("b", 1.0), sr("d", 1.0)]),
        );
        let hints = SearchHints {
            limit: 3,
            ..Default::default()
        };
        let got = collect_search_results(&mut *searcher(&*merged).search_label_names(
            &ctx,
            Some(&hints),
            &[],
        ));
        assert_eq!(got, vec![sr("a", 1.0), sr("b", 1.0), sr("c", 1.0)]);
    }
}
