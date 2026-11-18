//! Port of `promql/promqltest/test_test.go`.

use std::sync::Arc;
use std::time::Duration;

use chunkenc::{ChunkIterator, ValueType};
use model::labels::{self, MatchType, Matcher};
use promql::promqltest::internal::{
    assert_matrix_sorted, lines, parse_load, parse_st_sequence, test_test,
};
use promql::promqltest::{self, LazyLoader, LazyLoaderOpts};
use promql::{FPoint, Series};
use storage::{AppenderTransaction, Context, Storage};

const TEST_DATA: &str = r#"
load 5m
	http_requests{job="api-server", instance="0", group="production"}	0+10x10
	http_requests{job="api-server", instance="1", group="production"}	0+20x10
	http_requests{job="api-server", instance="0", group="canary"}		0+30x10
	http_requests{job="api-server", instance="1", group="canary"}		0+40x10
	{"http.requests", "service.name"="api-server", instance="0", group="canary"}		0+50x10
	{"http.requests", "service.name"="api-server", instance="1", group="canary"}		0+60x10
"#;

fn fpoints(v: &[(i64, f64)]) -> Vec<FPoint> {
    v.iter().map(|&(t, f)| FPoint { t, f }).collect()
}

fn series(name: &str, v: &[(i64, f64)]) -> Series {
    Series {
        metric: labels::from_strings(&["__name__", name]),
        floats: fpoints(v),
        ..Default::default()
    }
}

#[test]
fn test_lazy_loader_with_samples_till() {
    struct TestCase {
        ts: i64,
        series: Vec<Series>,
        check_only_error: bool,
    }
    let p = |n: i64| {
        (0..=n)
            .map(|i| (i * 10000, (i + 1) as f64))
            .collect::<Vec<_>>()
    };
    let cases: Vec<(&str, Vec<TestCase>)> = vec![
        (
            "
				load 10s
					metric1 1+1x10
			",
            vec![
                TestCase {
                    ts: 40_000,
                    series: vec![series("metric1", &p(4))],
                    check_only_error: false,
                },
                TestCase {
                    ts: 10_000,
                    series: vec![series("metric1", &p(4))],
                    check_only_error: false,
                },
                TestCase {
                    ts: 60_000,
                    series: vec![series("metric1", &p(6))],
                    check_only_error: false,
                },
            ],
        ),
        (
            "
				load 10s
					metric1 1+0x5
					metric2 1+1x100
			",
            vec![
                TestCase {
                    ts: 70_000,
                    series: vec![
                        series(
                            "metric1",
                            &(0..=5).map(|i| (i * 10000, 1.0)).collect::<Vec<_>>(),
                        ),
                        series("metric2", &p(7)),
                    ],
                    check_only_error: false,
                },
                TestCase {
                    ts: 300_000,
                    series: vec![],
                    check_only_error: true,
                },
            ],
        ),
    ];

    for (load_string, test_cases) in cases {
        let mut suite = LazyLoader::new(load_string, LazyLoaderOpts::default()).unwrap();
        for tc in test_cases {
            let mut res = None;
            suite.with_samples_till(tc.ts, |r| res = Some(r));
            res.unwrap().unwrap();
            if tc.check_only_error {
                continue;
            }
            let queryable = suite.queryable();
            let querier = queryable.querier(i64::MIN, i64::MAX).unwrap();
            for s in &tc.series {
                let mut matchers = Vec::new();
                s.metric
                    .range(|n, v| matchers.push(Matcher::new(MatchType::Equal, n, v).unwrap()));
                let mut ss = querier.select(&suite.context(), false, None, &matchers);
                assert!(ss.next());
                let storage_series = ss.at().clone();
                assert!(!ss.next(), "Expecting only 1 series");
                let mut got = Series {
                    metric: storage_series.labels().clone(),
                    ..Default::default()
                };
                let mut it = storage_series.iterator(None);
                while it.next() == ValueType::Float {
                    let (t, v) = it.at();
                    got.floats.push(FPoint { t, f: v });
                }
                assert!(it.err().is_none());
                assert_eq!(s, &got);
            }
        }
        suite.close().unwrap();
    }
}

#[test]
fn test_run_test() {
    let test_cases: Vec<(&str, String, String)> = vec![
        ("instant query with expected float result", format!("{}{}", TEST_DATA, r####"
eval instant at 5m sum by (group) (http_requests)
	{group="production"} 30
	{group="canary"} 70
"####), String::new()),
        ("instant query on UTF-8 metric with expected float result", format!("{}{}", TEST_DATA, r####"
eval instant at 5m sum by ("service.name") ({"http.requests"})
	{"service.name"="api-server"} 110
"####), String::new()),
        ("instant query with unexpected float result", format!("{}{}", TEST_DATA, r####"
eval instant at 5m sum by (group) (http_requests)
	{group="production"} 30
	{group="canary"} 80
"####), r####"error in eval sum by (group) (http_requests) (line 10): expected 80 for {group="canary"} but got 70"####.to_string()),
        ("instant query with expected histogram result", r####"
load 5m
	testmetric {{schema:-1 sum:4 count:1 buckets:[1] offset:1}}

eval instant at 0 testmetric
	testmetric {{schema:-1 sum:4 count:1 buckets:[1] offset:1}}
"####.to_string(), String::new()),
        ("instant query with unexpected histogram result", r####"
load 5m
	testmetric {{schema:-1 sum:4 count:1 buckets:[1] offset:1}}

eval instant at 0 testmetric
	testmetric {{schema:-1 sum:6 count:1 buckets:[1] offset:1}}
"####.to_string(), r####"error in eval testmetric (line 5): expected {{schema:-1 count:1 sum:6 offset:1 buckets:[1]}} for {__name__="testmetric"} but got {{schema:-1 count:1 sum:4 offset:1 buckets:[1]}}"####.to_string()),
        ("instant query with float value returned when histogram expected", r####"
load 5m
	testmetric 2

eval instant at 0 testmetric
	testmetric {{}}
"####.to_string(), r####"error in eval testmetric (line 5): expected histogram {{}} for {__name__="testmetric"} but got float value 2"####.to_string()),
        ("instant query with histogram returned when float expected", r####"
load 5m
	testmetric {{}}

eval instant at 0 testmetric
	testmetric 2
"####.to_string(), r####"error in eval testmetric (line 5): expected float value 2.000000 for {__name__="testmetric"} but got histogram {{}}"####.to_string()),
        ("instant query, but result has an unexpected series with a float value", format!("{}{}", TEST_DATA, r####"
eval instant at 5m sum by (group) (http_requests)
	{group="production"} 30
"####), r####"error in eval sum by (group) (http_requests) (line 10): unexpected metric {group="canary"} in result, has value 70"####.to_string()),
        ("instant query, but result has an unexpected series with a histogram value", r####"
load 5m
	testmetric {{}}

eval instant at 0m testmetric
"####.to_string(), r####"error in eval testmetric (line 5): unexpected metric {__name__="testmetric"} in result, has value {{}}"####.to_string()),
        ("instant query, but result is missing a series", format!("{}{}", TEST_DATA, r####"
eval instant at 5m sum by (group) (http_requests)
	{group="production"} 30
	{group="canary"} 70
	{group="test"} 100
"####), r####"error in eval sum by (group) (http_requests) (line 10): expected metric {group="test"} with 3: [100.000000] not found"####.to_string()),
        ("instant query expected to fail, and query fails", r####"
load 5m
	testmetric1{src="a",dst="b"} 0
	testmetric2{src="a",dst="b"} 1

eval_fail instant at 0m ceil({__name__=~'testmetric1|testmetric2'})
"####.to_string(), String::new()),
        ("instant query expected to fail, but query succeeds", r####"eval_fail instant at 0s vector(0)"####.to_string(), r####"expected error evaluating query "vector(0)" (line 1) but got none"####.to_string()),
        ("instant query expected to fail with specific error message, and query fails with that error", r####"
load 5m
	testmetric1{src="a",dst="b"} 0
	testmetric2{src="a",dst="b"} 1

eval_fail instant at 0m ceil({__name__=~'testmetric1|testmetric2'})
	expected_fail_message vector cannot contain metrics with the same labelset
"####.to_string(), String::new()),
        ("instant query expected to fail with specific error message, and query fails with a different error", r####"
load 5m
	testmetric1{src="a",dst="b"} 0
	testmetric2{src="a",dst="b"} 1

eval_fail instant at 0m ceil({__name__=~'testmetric1|testmetric2'})
	expected_fail_message something else went wrong
"####.to_string(), r####"expected error "something else went wrong" evaluating query "ceil({__name__=~'testmetric1|testmetric2'})" (line 6), but got: vector cannot contain metrics with the same labelset"####.to_string()),
        ("instant query expected to fail with error matching pattern, and query fails with that error", r####"
load 5m
	testmetric1{src="a",dst="b"} 0
	testmetric2{src="a",dst="b"} 1

eval_fail instant at 0m ceil({__name__=~'testmetric1|testmetric2'})
	expected_fail_regexp vector .* contain metrics
"####.to_string(), String::new()),
        ("instant query expected to fail with error matching pattern, and query fails with a different error", r####"
load 5m
	testmetric1{src="a",dst="b"} 0
	testmetric2{src="a",dst="b"} 1

eval_fail instant at 0m ceil({__name__=~'testmetric1|testmetric2'})
	expected_fail_regexp something else went wrong
"####.to_string(), r####"expected error matching pattern "something else went wrong" evaluating query "ceil({__name__=~'testmetric1|testmetric2'})" (line 6), but got: vector cannot contain metrics with the same labelset"####.to_string()),
        ("instant query expected to fail with error matching pattern, and pattern is not a valid regexp", r####"
load 5m
	testmetric1{src="a",dst="b"} 0
	testmetric2{src="a",dst="b"} 1

eval_fail instant at 0m ceil({__name__=~'testmetric1|testmetric2'})
	expected_fail_regexp [
"####.to_string(), format!("{}{}", r####"error in eval ceil({__name__=~'testmetric1|testmetric2'}) (line 7): invalid regexp '[' for expected_fail_regexp: error parsing regexp: missing closing ]: "####, r####"`[`"####)),
        ("instant query with results expected to match provided order, and result is in expected order", format!("{}{}", TEST_DATA, r####"
eval_ordered instant at 50m sort(http_requests)
	http_requests{group="production", instance="0", job="api-server"} 100
	http_requests{group="production", instance="1", job="api-server"} 200
	http_requests{group="canary", instance="0", job="api-server"} 300
	http_requests{group="canary", instance="1", job="api-server"} 400
"####), String::new()),
        ("instant query with results expected to match provided order, but result is out of order", format!("{}{}", TEST_DATA, r####"
eval_ordered instant at 50m sort(http_requests)
	http_requests{group="production", instance="0", job="api-server"} 100
	http_requests{group="production", instance="1", job="api-server"} 200
	http_requests{group="canary", instance="1", job="api-server"} 400
	http_requests{group="canary", instance="0", job="api-server"} 300
"####), r####"error in eval sort(http_requests) (line 10): expected metric {__name__="http_requests", group="canary", instance="0", job="api-server"} with [300.000000] at position 4 but was at 3"####.to_string()),
        ("instant query with results expected to match provided order, but result has an unexpected series", format!("{}{}", TEST_DATA, r####"
eval_ordered instant at 50m sort(http_requests)
	http_requests{group="production", instance="0", job="api-server"} 100
	http_requests{group="production", instance="1", job="api-server"} 200
	http_requests{group="canary", instance="0", job="api-server"} 300
"####), r####"error in eval sort(http_requests) (line 10): unexpected metric {__name__="http_requests", group="canary", instance="1", job="api-server"} in result, has value 400"####.to_string()),
        ("instant query with results expected to match provided order, result is in expected order and info annotation is ignored", format!("{}{}", TEST_DATA, r####"
eval_ordered instant at 50m sort(rate(http_requests[10m]))
	{group="production", instance="0", job="api-server"} 0.03333333333333333
	{group="production", instance="1", job="api-server"} 0.06666666666666667
	{group="canary", instance="0", job="api-server"} 0.1
	{group="canary", instance="1", job="api-server"} 0.13333333333333333
"####), String::new()),
        ("instant query with expected info annotation", format!("{}{}", TEST_DATA, r####"
eval_info instant at 50m sort(rate(http_requests[10m]))
	{group="production", instance="0", job="api-server"} 0.03333333333333333
	{group="production", instance="1", job="api-server"} 0.06666666666666667
	{group="canary", instance="0", job="api-server"} 0.1
	{group="canary", instance="1", job="api-server"} 0.13333333333333333
"####), String::new()),
        ("instant query with unexpected info annotation", format!("{}{}", TEST_DATA, r####"
eval instant at 50m sort(rate(http_requests[10m]))
	expect no_info
	{group="production", instance="0", job="api-server"} 0.03333333333333333
	{group="production", instance="1", job="api-server"} 0.06666666666666667
	{group="canary", instance="0", job="api-server"} 0.1
	{group="canary", instance="1", job="api-server"} 0.13333333333333333
"####), r####"unexpected info annotations evaluating query "sort(rate(http_requests[10m]))" (line 10): [PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "http_requests" (1:11)]"####.to_string()),
        ("instant query with unexpectedly missing warn annotation", format!("{}{}", TEST_DATA, r####"
eval_warn instant at 50m sort(rate(http_requests[10m]))
	{group="production", instance="0", job="api-server"} 0.03333333333333333
	{group="production", instance="1", job="api-server"} 0.06666666666666667
	{group="canary", instance="0", job="api-server"} 0.1
	{group="canary", instance="1", job="api-server"} 0.13333333333333333
"####), r####"expected warnings evaluating query "sort(rate(http_requests[10m]))" (line 10) but got none"####.to_string()),
        ("instant query with invalid timestamp", r####"eval instant at abc123 vector(0)"####.to_string(), r####"error in eval vector(0) (line 1): invalid timestamp definition "abc123": not a valid duration string: "abc123""####.to_string()),
        ("range query with expected result", format!("{}{}", TEST_DATA, r####"
eval range from 0 to 10m step 5m sum by (group) (http_requests)
	{group="production"} 0 30 60
	{group="canary"} 0 70 140
"####), String::new()),
        ("range query with unexpected float value", format!("{}{}", TEST_DATA, r####"
eval range from 0 to 10m step 5m sum by (group) (http_requests)
	{group="production"} 0 30 60
	{group="canary"} 0 80 140
"####), r####"error in eval sum by (group) (http_requests) (line 10): expected float value at index 1 (t=300000) for {group="canary"} to be 80, but got 70 (result has 3 float points [0 @[0] 70 @[300000] 140 @[600000]] and 0 histogram points [])"####.to_string()),
        ("range query with expected histogram values", r####"
load 5m
	testmetric {{schema:-1 sum:4 count:1 buckets:[1] offset:1}} {{schema:-1 sum:5 count:1 buckets:[1] offset:1}} {{schema:-1 sum:6 count:1 buckets:[1] offset:1}}

eval range from 0 to 10m step 5m testmetric
	testmetric {{schema:-1 sum:4 count:1 buckets:[1] offset:1}} {{schema:-1 sum:5 count:1 buckets:[1] offset:1}} {{schema:-1 sum:6 count:1 buckets:[1] offset:1}}
"####.to_string(), String::new()),
        ("range query with unexpected histogram value", r####"
load 5m
	testmetric {{schema:-1 sum:4 count:1 buckets:[1] offset:1}} {{schema:-1 sum:5 count:1 buckets:[1] offset:1}} {{schema:-1 sum:6 count:1 buckets:[1] offset:1}}

eval range from 0 to 10m step 5m testmetric
	testmetric {{schema:-1 sum:4 count:1 buckets:[1] offset:1}} {{schema:-1 sum:7 count:1 buckets:[1] offset:1}} {{schema:-1 sum:8 count:1 buckets:[1] offset:1}}
"####.to_string(), r####"error in eval testmetric (line 5): expected histogram value at index 1 (t=300000) for {__name__="testmetric"} to be {{schema:-1 count:1 sum:7 offset:1 buckets:[1]}}, but got {{schema:-1 count:1 sum:5 counter_reset_hint:not_reset offset:1 buckets:[1]}} (result has 0 float points [] and 3 histogram points [{{schema:-1 count:1 sum:4 offset:1 buckets:[1]}} @[0] {{schema:-1 count:1 sum:5 counter_reset_hint:not_reset offset:1 buckets:[1]}} @[300000] {{schema:-1 count:1 sum:6 counter_reset_hint:not_reset offset:1 buckets:[1]}} @[600000]])"####.to_string()),
        ("range query with too many points for query time range", format!("{}{}", TEST_DATA, r####"
eval range from 0 to 10m step 5m sum by (group) (http_requests)
	{group="production"} 0 30 60 90
	{group="canary"} 0 70 140
"####), r####"error in eval sum by (group) (http_requests) (line 10): expected 4 points for {group="production"}, but query time range cannot return this many points"####.to_string()),
        ("range query with missing point in result", r####"
load 5m
	testmetric 5

eval range from 0 to 6m step 6m testmetric
	testmetric 5 10
"####.to_string(), r####"error in eval testmetric (line 5): expected 2 float points and 0 histogram points for {__name__="testmetric"}, but got 1 float point [5 @[0]] and 0 histogram points []"####.to_string()),
        ("range query with extra point in result", format!("{}{}", TEST_DATA, r####"
eval range from 0 to 10m step 5m sum by (group) (http_requests)
	{group="production"} 0 30
	{group="canary"} 0 70 140
"####), r####"error in eval sum by (group) (http_requests) (line 10): expected 2 float points and 0 histogram points for {group="production"}, but got 3 float points [0 @[0] 30 @[300000] 60 @[600000]] and 0 histogram points []"####.to_string()),
        ("range query, but result has an unexpected series", format!("{}{}", TEST_DATA, r####"
eval range from 0 to 10m step 5m sum by (group) (http_requests)
	{group="production"} 0 30 60
"####), r####"error in eval sum by (group) (http_requests) (line 10): unexpected metric {group="canary"} in result, has 3 float points [0 @[0] 70 @[300000] 140 @[600000]] and 0 histogram points []"####.to_string()),
        ("range query, but result is missing a series", format!("{}{}", TEST_DATA, r####"
eval range from 0 to 10m step 5m sum by (group) (http_requests)
	{group="production"} 0 30 60
	{group="canary"} 0 70 140
	{group="test"} 0 100 200
"####), r####"error in eval sum by (group) (http_requests) (line 10): expected metric {group="test"} not found"####.to_string()),
        ("range query expected to fail, and query fails", r####"
load 5m
	testmetric1{src="a",dst="b"} 0
	testmetric2{src="a",dst="b"} 1

eval_fail range from 0 to 10m step 5m ceil({__name__=~'testmetric1|testmetric2'})
"####.to_string(), String::new()),
        ("range query expected to fail, but query succeeds", r####"eval_fail range from 0 to 10m step 5m vector(0)"####.to_string(), r####"expected error evaluating query "vector(0)" (line 1) but got none"####.to_string()),
        ("range query expected to fail with specific error message, and query fails with that error", r####"
load 5m
	testmetric1{src="a",dst="b"} 0
	testmetric2{src="a",dst="b"} 1

eval_fail range from 0 to 10m step 5m ceil({__name__=~'testmetric1|testmetric2'})
	expected_fail_message vector cannot contain metrics with the same labelset
"####.to_string(), String::new()),
        ("range query expected to fail with specific error message, and query fails with a different error", r####"
load 5m
	testmetric1{src="a",dst="b"} 0
	testmetric2{src="a",dst="b"} 1

eval_fail range from 0 to 10m step 5m ceil({__name__=~'testmetric1|testmetric2'})
	expected_fail_message something else went wrong
"####.to_string(), r####"expected error "something else went wrong" evaluating query "ceil({__name__=~'testmetric1|testmetric2'})" (line 6), but got: vector cannot contain metrics with the same labelset"####.to_string()),
        ("range query expected to fail with error matching pattern, and query fails with that error", r####"
load 5m
	testmetric1{src="a",dst="b"} 0
	testmetric2{src="a",dst="b"} 1

eval_fail range from 0 to 10m step 5m ceil({__name__=~'testmetric1|testmetric2'})
	expected_fail_regexp vector .* contain metrics
"####.to_string(), String::new()),
        ("range query expected to fail with error matching pattern, and query fails with a different error", r####"
load 5m
	testmetric1{src="a",dst="b"} 0
	testmetric2{src="a",dst="b"} 1

eval_fail range from 0 to 10m step 5m ceil({__name__=~'testmetric1|testmetric2'})
	expected_fail_regexp something else went wrong
"####.to_string(), r####"expected error matching pattern "something else went wrong" evaluating query "ceil({__name__=~'testmetric1|testmetric2'})" (line 6), but got: vector cannot contain metrics with the same labelset"####.to_string()),
        ("range query expected to fail with error matching pattern, and pattern is not a valid regexp", r####"
load 5m
	testmetric1{src="a",dst="b"} 0
	testmetric2{src="a",dst="b"} 1

eval_fail range from 0 to 10m step 5m ceil({__name__=~'testmetric1|testmetric2'})
	expected_fail_regexp [
"####.to_string(), format!("{}{}", r####"error in eval ceil({__name__=~'testmetric1|testmetric2'}) (line 7): invalid regexp '[' for expected_fail_regexp: error parsing regexp: missing closing ]: "####, r####"`[`"####)),
        ("range query with from and to timestamps in wrong order", r####"eval range from 10m to 9m step 5m vector(0)"####.to_string(), r####"error in eval vector(0) (line 1): invalid test definition, end timestamp (9m) is before start timestamp (10m)"####.to_string()),
        ("range query with sparse output", r####"
load 6m
	testmetric 1 _ 3

eval range from 0 to 18m step 6m testmetric
	testmetric 1 _ 3
"####.to_string(), String::new()),
        ("range query with float value returned when no value expected", r####"
load 6m
	testmetric 1 2 3

eval range from 0 to 18m step 6m testmetric
	testmetric 1 _ 3
"####.to_string(), r####"error in eval testmetric (line 5): expected 2 float points and 0 histogram points for {__name__="testmetric"}, but got 3 float points [1 @[0] 2 @[360000] 3 @[720000]] and 0 histogram points []"####.to_string()),
        ("range query with float value returned when histogram expected", r####"
load 5m
	testmetric 2 3

eval range from 0 to 5m step 5m testmetric
	testmetric {{}} {{}}
"####.to_string(), r####"error in eval testmetric (line 5): expected 0 float points and 2 histogram points for {__name__="testmetric"}, but got 2 float points [2 @[0] 3 @[300000]] and 0 histogram points []"####.to_string()),
        ("range query with histogram returned when float expected", r####"
load 5m
	testmetric {{}} {{}}

eval range from 0 to 5m step 5m testmetric
	testmetric 2 3
"####.to_string(), r####"error in eval testmetric (line 5): expected 2 float points and 0 histogram points for {__name__="testmetric"}, but got 0 float points [] and 2 histogram points [{{}} @[0] {{counter_reset_hint:not_reset}} @[300000]]"####.to_string()),
        ("range query with expected mixed results", r####"
load 6m
	testmetric{group="a"} {{}} _ _
	testmetric{group="b"} _ _ 3

eval range from 0 to 12m step 6m sum(testmetric)
	{} {{}} _ 3
"####.to_string(), String::new()),
        ("range query with mixed results and incorrect values", r####"
load 5m
	testmetric 3 {{}}

eval range from 0 to 5m step 5m testmetric
	testmetric {{}} 3
"####.to_string(), r####"error in eval testmetric (line 5): expected float value at index 0 for {__name__="testmetric"} to have timestamp 300000, but it had timestamp 0 (result has 1 float point [3 @[0]] and 1 histogram point [{{}} @[300000]])"####.to_string()),
        ("instant query with expected scalar result", r####"
				eval instant at 1m 3
					3
			"####.to_string(), String::new()),
        ("instant query with unexpected scalar result", r####"
				eval instant at 1m 3
					2
			"####.to_string(), r####"error in eval 3 (line 2): expected scalar 2 but got 3"####.to_string()),
        ("instant query that returns a scalar but expects a vector", r####"
				eval instant at 1m 3
					{} 3
			"####.to_string(), r####"error in eval 3 (line 2): expected vector or matrix result, but got scalar: 3 @[60000]"####.to_string()),
        ("instant query that returns a vector but expects a scalar", r####"
				eval instant at 1m vector(3)
					3
			"####.to_string(), r####"error in eval vector(3) (line 2): expected scalar result, but got vector {} => 3 @[60000]"####.to_string()),
        ("range query that returns a matrix but expects a scalar", r####"
				eval range from 0 to 1m step 30s vector(3)
					3
			"####.to_string(), r####"error in eval vector(3) (line 2): expected scalar result, but got matrix {} =>
3 @[0]
3 @[30000]
3 @[60000]"####.to_string()),
        ("instant query expected to fail with specific error message, and query fails with that error (with new eval syntax)", r####"
load 5m
	testmetric1{src="a",dst="b"} 0
	testmetric2{src="a",dst="b"} 1

eval_fail instant at 0m ceil({__name__=~'testmetric1|testmetric2'})
	expect fail msg: vector cannot contain metrics with the same labelset
"####.to_string(), String::new()),
        ("instant query expected to fail with specific error message, and query does not fail with that error (with new eval syntax)", r####"
load 5m
	testmetric1{src="a",dst="b"} 0
	testmetric2{src="a",dst="b"} 1

eval_fail instant at 0m ceil({__name__=~'testmetric1|testmetric2'})
	expect fail msg: something went wrong
"####.to_string(), r####"expected error matching "something went wrong" evaluating query "ceil({__name__=~'testmetric1|testmetric2'})" (line 6), but got: vector cannot contain metrics with the same labelset"####.to_string()),
        ("instant query expected to fail with specific error regex, and query fails with that error (with new eval syntax)", r####"
load 5m
	testmetric1{src="a",dst="b"} 0
	testmetric2{src="a",dst="b"} 1

eval_fail instant at 0m ceil({__name__=~'testmetric1|testmetric2'})
	expect fail regex: .*labelset.*
"####.to_string(), String::new()),
        ("instant query expected to fail with specific error regex, and query does not fail with that error (with new eval syntax)", r####"
load 5m
	testmetric1{src="a",dst="b"} 0
	testmetric2{src="a",dst="b"} 1

eval_fail instant at 0m ceil({__name__=~'testmetric1|testmetric2'})
	expect fail regex: something went (wrong|boom)
"####.to_string(), r####"expected error matching "something went (wrong|boom)" evaluating query "ceil({__name__=~'testmetric1|testmetric2'})" (line 6), but got: vector cannot contain metrics with the same labelset"####.to_string()),
        ("instant query expected to only to fail (with new eval syntax)", r####"
load 5m
	testmetric1{src="a",dst="b"} 0
	testmetric2{src="a",dst="b"} 1

eval_fail instant at 0m ceil({__name__=~'testmetric1|testmetric2'})
	expect fail
"####.to_string(), String::new()),
        ("invalid fail syntax with error as token instead of regex or msg(with new eval syntax)", r####"
load 5m
	testmetric1{src="a",dst="b"} 0
	testmetric2{src="a",dst="b"} 1

eval_fail instant at 0m ceil({__name__=~'testmetric1|testmetric2'})
	expect fail error: something went wrong
"####.to_string(), r####"error in eval ceil({__name__=~'testmetric1|testmetric2'}) (line 7): invalid expect statement, must match `expect <type> <match_type>: <string>` format"####.to_string()),
        ("instant query expected not to care about annotations (with new eval syntax)", r####"
load 5m
	metric{src="a"} {{schema:0 sum:1 count:2}}
	metric{src="b"} 1

eval_warn instant at 0m sum(metric)
"####.to_string(), String::new()),
        ("instant query expected to only have warn annotation (with new eval syntax)", r####"
load 5m
	metric{src="a"} {{schema:0 sum:1 count:2}}
	metric{src="b"} 1

eval_warn instant at 0m sum(metric)
	expect warn
"####.to_string(), String::new()),
        ("instant query expected to have warn annotation with specific message (with new eval syntax)", r####"
load 5m
	metric{src="a"} {{schema:0 sum:1 count:2}}
	metric{src="b"} 1

eval_warn instant at 0m sum(metric)
	expect warn msg: PromQL warning: encountered a mix of histograms and floats for aggregation (1:5)
"####.to_string(), String::new()),
        ("instant query expected to have warn but not with the specific message (with new eval syntax)", r####"
load 5m
	metric{src="a"} {{schema:0 sum:1 count:2}}
	metric{src="b"} 1

eval_warn instant at 0m sum(metric)
	expect warn msg: PromQL warning: encountered a mix
"####.to_string(), r####"expected warn annotation matching message "PromQL warning: encountered a mix" but no matching annotation was found for query "sum(metric)" (line 6), found: [PromQL warning: encountered a mix of histograms and floats for aggregation (1:5)]"####.to_string()),
        ("instant query expected to have warn annotation with specific regex (with new eval syntax)", r####"
load 5m
	metric{src="a"} {{schema:0 sum:1 count:2}}
	metric{src="b"} 1

eval_warn instant at 0m sum(metric)
	expect warn regex: PromQL warning: encountered a mix
"####.to_string(), String::new()),
        ("instant query expected to have warn but not with the specific regex (with new eval syntax)", r####"
load 5m
	metric{src="a"} {{schema:0 sum:1 count:2}}
	metric{src="b"} 1

eval_warn instant at 0m sum(metric)
	expect warn regex: PromQL warning: something went (wrong|boom)
"####.to_string(), r####"expected warn annotation matching pattern "PromQL warning: something went (wrong|boom)" but no matching annotation was found for query "sum(metric)" (line 6), found: [PromQL warning: encountered a mix of histograms and floats for aggregation (1:5)]"####.to_string()),
        ("instant query expected to have warn annotation and no info annotation (with new eval syntax)", r####"
load 5m
	metric{src="a"} {{schema:0 sum:1 count:2}}
	metric{src="b"} 1

eval_warn instant at 0m sum(metric)
	expect warn
	expect no_info
"####.to_string(), String::new()),
        ("instant query expected to warn and info annotation but got only warn annotation (with new eval syntax)", r####"
load 5m
	metric{src="a"} {{schema:0 sum:1 count:2}}
	metric{src="b"} 1

eval_warn instant at 0m sum(metric)
	expect warn
	expect info
"####.to_string(), r####"expected info annotations but none were found for query "sum(metric)" (line 6)"####.to_string()),
        ("instant query expected to have no warn but got warn annotation (with new eval syntax)", r####"
load 5m
	metric{src="a"} {{schema:0 sum:1 count:2}}
	metric{src="b"} 1

eval_warn instant at 0m sum(metric)
	expect no_warn
"####.to_string(), r####"unexpected warning annotations evaluating query "sum(metric)" (line 6): [PromQL warning: encountered a mix of histograms and floats for aggregation (1:5)]"####.to_string()),
        ("instant query expected to only have info annotation (with new eval syntax)", r####"
load 5m
	metric{src="a"} {{schema:0 sum:1 count:2}}
	metric{src="b"} 1

eval_info instant at 0m min(metric)
	expect info
	{} 1
"####.to_string(), String::new()),
        ("instant query expected have info annotation with specific message (with new eval syntax)", r####"
load 5m
	metric{src="a"} {{schema:0 sum:1 count:2}}
	metric{src="b"} 1

eval_info instant at 0m min(metric)
	expect info msg: PromQL info: ignored histogram in min aggregation (1:5)
	{} 1
"####.to_string(), String::new()),
        ("instant query expected to have info annotation but not with the specific message (with new eval syntax)", r####"
load 5m
	metric{src="a"} {{schema:0 sum:1 count:2}}
	metric{src="b"} 1

eval_info instant at 0m min(metric)
	expect info msg: something went wrong
	{} 1
"####.to_string(), r####"expected info annotation matching message "something went wrong" but no matching annotation was found for query "min(metric)" (line 6), found: [PromQL info: ignored histogram in min aggregation (1:5)]"####.to_string()),
        ("instant query expected to have info annotation with specific regex (with new eval syntax)", r####"
load 5m
	metric{src="a"} {{schema:0 sum:1 count:2}}
	metric{src="b"} 1

eval_info instant at 0m min(metric)
	expect info regex: PromQL info: ignored histogram
	{} 1
"####.to_string(), String::new()),
        ("instant query expected to only have warn but not with the specific regex (with new eval syntax)", r####"
load 5m
	metric{src="a"} {{schema:0 sum:1 count:2}}
	metric{src="b"} 1

eval_info instant at 0m min(metric)
	expect info regex: something went (wrong|boom)
	{} 1
"####.to_string(), r####"expected info annotation matching pattern "something went (wrong|boom)" but no matching annotation was found for query "min(metric)" (line 6), found: [PromQL info: ignored histogram in min aggregation (1:5)]"####.to_string()),
        ("instant query expected to have info annotation and no warn annotation (with new eval syntax)", r####"
load 5m
	metric{src="a"} {{schema:0 sum:1 count:2}}
	metric{src="b"} 1

eval_info instant at 0m min(metric)
	expect info
	expect no_warn
	{} 1
"####.to_string(), String::new()),
        ("instant query expected to have warn and info annotation but got only info annotation (with new eval syntax)", r####"
load 5m
	metric{src="a"} {{schema:0 sum:1 count:2}}
	metric{src="b"} 1

eval_info instant at 0m min(metric)
	expect info
	expect warn
	{} 1
"####.to_string(), r####"expected warn annotations but none were found for query "min(metric)" (line 6)"####.to_string()),
        ("instant query expected to have no info annotation but got info annotation (with new eval syntax)", r####"
load 5m
	metric{src="a"} {{schema:0 sum:1 count:2}}
	metric{src="b"} 1

eval_info instant at 0m min(metric)
	expect no_info
	{} 1
"####.to_string(), r####"unexpected info annotations evaluating query "min(metric)" (line 6): [PromQL info: ignored histogram in min aggregation (1:5)]"####.to_string()),
        ("instant query with results expected to match provided order, and result is in expected order (with new eval syntax)", format!("{}{}", TEST_DATA, r####"
eval_ordered instant at 50m sort(http_requests)
	expect ordered
	http_requests{group="production", instance="0", job="api-server"} 100
	http_requests{group="production", instance="1", job="api-server"} 200
	http_requests{group="canary", instance="0", job="api-server"} 300
	http_requests{group="canary", instance="1", job="api-server"} 400
"####), String::new()),
        ("instant query with results expected to match provided order, but result is out of order (with new eval syntax)", format!("{}{}", TEST_DATA, r####"
eval_ordered instant at 50m sort(http_requests)
	expect ordered
	http_requests{group="production", instance="0", job="api-server"} 100
	http_requests{group="production", instance="1", job="api-server"} 200
	http_requests{group="canary", instance="1", job="api-server"} 400
	http_requests{group="canary", instance="0", job="api-server"} 300
"####), r####"error in eval sort(http_requests) (line 10): expected metric {__name__="http_requests", group="canary", instance="0", job="api-server"} with [300.000000] at position 4 but was at 3"####.to_string()),
        ("instant query with `expect` as the metric name in `expect{}` format expected not to throw any error, and no error occurred (with the new eval syntax).", r####"
load 5m
	expect 0 1

eval instant at 0m expect
	expect no_info
	expect{} 0
"####.to_string(), String::new()),
        ("instant query with both `expect info` and `expect no_info` lines expected to fail to parse (with the new eval syntax).", format!("{}{}", TEST_DATA, r####"
eval instant at 0m http_requests
	expect no_info
	expect info
	http_requests
"####), r####"error in eval http_requests (line 12): invalid expect lines, info and no_info cannot be used together"####.to_string()),
        ("instant query with both `expect warn` and `expect no_warn` lines expected to fail to parse (with the new eval syntax).", format!("{}{}", TEST_DATA, r####"
eval instant at 0m http_requests
	expect no_warn
	expect warn
	http_requests
"####), r####"error in eval http_requests (line 12): invalid expect lines, warn and no_warn cannot be used together"####.to_string()),
        ("instant query with more than one `expect fail` lines expected to fail to parse (with the new eval syntax).", format!("{}{}", TEST_DATA, r####"
eval instant at 0m http_requests
	expect fail
	expect fail msg: something went wrong
	http_requests
"####), r####"error in eval http_requests (line 12): invalid expect lines, multiple expect fail lines are not allowed"####.to_string()),
        ("instant query with string literal", r####"
				eval instant at 50m ("Foo")
					expect string "Foo"
			"####.to_string(), String::new()),
        ("instant query with string literal with leading space", r####"
				eval instant at 50m (" Foo")
					expect string " Foo"
			"####.to_string(), String::new()),
        ("instant query with string literal with trailing space", r####"
				eval instant at 50m ("Foo ")
					expect string "Foo "
			"####.to_string(), String::new()),
        ("instant query with string literal as space", r####"
				eval instant at 50m (" ")
					expect string " "
			"####.to_string(), String::new()),
        ("instant query with string literal with empty string", r####"
				eval instant at 50m ("")
					expect string
			"####.to_string(), r####"error in eval ("") (line 3): expected string literal not valid - a quoted string literal is required"####.to_string()),
        ("instant query with string literal with correctly quoted empty string", r####"
				eval instant at 50m ("")
					expect string ""
			"####.to_string(), String::new()),
        ("instant query with string literal - not quoted", r####"
				eval instant at 50m ("Foo")
					expect string Foo
			"####.to_string(), r####"error in eval ("Foo") (line 3): expected string literal not valid - check that the string is correctly quoted"####.to_string()),
        ("instant query with empty string literal", r####"
				eval instant at 50m ("Foo")
					expect string ""
			"####.to_string(), r####"error in eval ("Foo") (line 2): expected string "" but got "Foo""####.to_string()),
        ("instant query with error string literal", r####"
				eval instant at 50m ("Foo")
					expect string "Bar"
			"####.to_string(), r####"error in eval ("Foo") (line 2): expected string "Bar" but got "Foo""####.to_string()),
        ("instant query with range result - result does not have a series that is expected", r####"
				load 10s
  					some_metric{env="a"} 1+1x5

				eval instant at 1m some_metric[1m]
					expect range vector from 10s to 1m step 10s
  					some_metric{env="a"} 2 3 4 5 6
  					some_metric{env="b"} 4 6 8 10 12
			"####.to_string(), r####"error in eval some_metric[1m] (line 5): expected metric {__name__="some_metric", env="b"} not found"####.to_string()),
        ("instant query with range result - result has a series which is not expected", r####"
				load 10s
  					some_metric{env="a"} 1+1x5
					some_metric{env="b"} 1+1x5

				eval instant at 1m some_metric[1m]
					expect range vector from 10s to 1m step 10s
  					some_metric{env="a"} 2 3 4 5 6
			"####.to_string(), r####"error in eval some_metric[1m] (line 6): unexpected metric {__name__="some_metric", env="b"} in result, has 5 float points [2 @[10000] 3 @[20000] 4 @[30000] 5 @[40000] 6 @[50000]] and 0 histogram points []"####.to_string()),
        ("instant query with range result - result has a value that is not expected", r####"
				load 10s
  					some_metric{env="a"} 1+1x5

				eval instant at 1m some_metric[1m]
					expect range vector from 10s to 1m step 10s
  					some_metric{env="a"} 9 3 4 5 6
			"####.to_string(), r####"error in eval some_metric[1m] (line 5): expected float value at index 0 (t=10000) for {__name__="some_metric", env="a"} to be 9, but got 2 (result has 5 float points [2 @[10000] 3 @[20000] 4 @[30000] 5 @[40000] 6 @[50000]] and 0 histogram points [])"####.to_string()),
        ("instant query with range result - invalid expect range vector directive", r####"
				load 10s
  					some_metric{env="a"} 1+1x5

				eval instant at 1m some_metric[1m]
					expect range vector from 10s
  					some_metric{env="a"} 2 3 4 5 6
			"####.to_string(), r####"error in eval some_metric[1m] (line 6): invalid range vector definition "expect range vector from 10s""####.to_string()),
        ("instant query with range result - result matches expected value", r####"
				load 1m
				  some_metric{env="1"} 0+1x4
				  some_metric{env="2"} 0+2x4
				
				eval instant at 2m some_metric[2m]
				  expect range vector from 1m to 2m step 60s
					some_metric{env="1"} 1 2
					some_metric{env="2"} 2 4
			"####.to_string(), String::new()),
        ("instant query with range result - result has a is missing a sample", r####"
				load 1m
				  some_metric_with_stale_marker 0 1 stale 3
				
				eval instant at 3m some_metric_with_stale_marker[3m]
					expect range vector from 1m to 3m step 60s
					some_metric_with_stale_marker{} 1 2 3
			"####.to_string(), r####"error in eval some_metric_with_stale_marker[3m] (line 5): expected 3 float points and 0 histogram points for {__name__="some_metric_with_stale_marker"}, but got 2 float points [1 @[60000] 3 @[180000]] and 0 histogram points []"####.to_string()),
        ("instant query with range result - result has a sample where none is expected", r####"
				load 1m
				  some_metric_with_stale_marker 0 1 2 3
				
				eval instant at 3m some_metric_with_stale_marker[3m]
					expect range vector from 1m to 3m step 60s
					some_metric_with_stale_marker{} 1 _ 3
			"####.to_string(), r####"error in eval some_metric_with_stale_marker[3m] (line 5): expected 2 float points and 0 histogram points for {__name__="some_metric_with_stale_marker"}, but got 3 float points [1 @[60000] 2 @[120000] 3 @[180000]] and 0 histogram points []"####.to_string()),
    ];

    let mut failures = Vec::new();
    for (name, input, expected_error) in test_cases {
        let engine = promqltest::new_test_engine(
            false,
            Duration::ZERO,
            promqltest::DEFAULT_MAX_SAMPLES_PER_QUERY,
        );
        let err =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| test_test(&input, &engine)))
                .unwrap_or_else(|p| Err(format!("panic: {}", promqltest::panic_message(&*p))));
        if expected_error.is_empty() {
            if let Err(e) = err {
                failures.push(format!("{}: unexpected error: {}", name, e));
            }
        } else {
            match err {
                Ok(()) => failures.push(format!(
                    "{}: expected error {:?}, got none",
                    name, expected_error
                )),
                Err(e) if e != expected_error => failures.push(format!(
                    "{}:\n  expected: {}\n  actual:   {}",
                    name, expected_error, e
                )),
                _ => {}
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} failing cases:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn test_parse_st_sequence() {
    use promql::parser::SequenceValue;
    let om = || SequenceValue {
        omitted: true,
        ..Default::default()
    };
    let v = |f: f64| SequenceValue {
        value: f,
        ..Default::default()
    };
    let cases: Vec<(&str, Option<Vec<SequenceValue>>)> = vec![
        ("_", Some(vec![om()])),
        ("_x3", Some(vec![om(), om(), om()])),
        ("0s", Some(vec![v(0.0)])),
        ("-1m", Some(vec![v((-60 * 1000) as f64)])),
        ("-1mx2", Some(vec![v(-60000.0), v(-60000.0), v(-60000.0)])),
        (
            "-1m+15sx2",
            Some(vec![
                v(-60000.0),
                v((-60000 + 15000) as f64),
                v((-60000 + 30000) as f64),
            ]),
        ),
        ("30s-10sx2", Some(vec![v(30000.0), v(20000.0), v(10000.0)])),
        ("_ -1m", Some(vec![om(), v(-60000.0)])),
        ("", Some(vec![])),
        ("badunit", None),
        ("_x0", None),
        ("_x-2", None),
        ("-1m+15s", None),
    ];
    for (input, expected) in cases {
        let got = parse_st_sequence(input);
        match expected {
            None => assert!(got.is_err(), "{}", input),
            Some(e) => assert_eq!(got.unwrap(), e, "{}", input),
        }
    }
}

#[test]
fn test_parse_load_st_line() {
    let step_ms = 5 * 60 * 1000;
    let ls = lines(&["load 5m", "  my_counter@st -1mx4", "  my_counter 0+1x4"]);
    let (_, cmd) = parse_load(&ls, 0, 0).unwrap();
    let metric = labels::from_strings(&["__name__", "my_counter"]);
    let smpls = cmd.samples(&metric).unwrap();
    assert_eq!(smpls.len(), 5);
    for (i, s) in smpls.iter().enumerate() {
        let want_t = i as i64 * step_ms;
        let want_st = want_t - 60000;
        assert_eq!(want_t, s.t, "sample {} timestamp", i);
        assert_eq!(want_st, s.st, "sample {} start timestamp", i);
    }
}

#[test]
fn test_parse_load_st_line_mismatch() {
    let cases: Vec<(&str, Vec<&str>)> = vec![
        (
            "count mismatch",
            vec!["load 5m", "  my_counter@st -1mx3", "  my_counter 0+1x4"],
        ),
        (
            "metric mismatch",
            vec!["load 5m", "  other_metric@st -1mx4", "  my_counter 0+1x4"],
        ),
        ("orphan @st line", vec!["load 5m", "  my_counter@st -1mx4"]),
        (
            "double @st line",
            vec![
                "load 5m",
                "  my_counter@st -1mx4",
                "  my_counter@st -1mx4",
                "  my_counter 0+1x4",
            ],
        ),
    ];
    for (name, l) in cases {
        assert!(parse_load(&lines(&l), 0, 0).is_err(), "{}", name);
    }
}

#[test]
fn test_load_st_line_storage_roundtrip() {
    let stores: Vec<(&str, Arc<dyn Storage>)> = vec![
        ("test storage", promqltest::new_test_storage()),
        (
            "built-in test storage",
            Arc::new(promqltest::new_builtin_test_storage()),
        ),
    ];
    for (name, store) in stores {
        let step_ms = 5 * 60 * 1000;
        let ls = lines(&["load 5m", "  my_counter@st -1mx4", "  my_counter 0+1x4"]);
        let (_, cmd) = parse_load(&ls, 0, 0).unwrap();
        let mut app = store.appender_v2(&Context::background());
        cmd.append(&mut *app).unwrap();
        assert!(app.commit().is_none());

        let mut q = store.querier(i64::MIN, i64::MAX).unwrap();
        let mut ss = q.select(
            &Context::background(),
            false,
            None,
            &[Matcher::new(MatchType::Equal, "__name__", "my_counter").unwrap()],
        );
        assert!(ss.next(), "{}: expected one series", name);
        let s = ss.at().clone();
        let mut it = s.iterator(None);
        for i in 0..5 {
            assert_eq!(ValueType::Float, it.next(), "{}: sample {}", name, i);
            let want_t = i as i64 * step_ms;
            let want_st = want_t - 60_000;
            let (got_t, _) = it.at();
            assert_eq!(want_t, got_t, "{}: sample {} timestamp", name, i);
            assert_eq!(
                want_st,
                it.at_st(),
                "{}: sample {} start timestamp",
                name,
                i
            );
        }
        assert_eq!(ValueType::None, it.next(), "expected no more samples");
        assert!(it.err().is_none());
        assert!(!ss.next(), "expected only one series");
        assert!(q.close().is_none());
    }
}

#[test]
fn test_assert_matrix_sorted() {
    let s = |v: &str| Series {
        metric: labels::from_strings(&["the_label", v]),
        ..Default::default()
    };
    let cases: Vec<(&str, Vec<Series>, &str)> = vec![
        ("empty matrix", vec![], ""),
        ("matrix with one series", vec![s("value_1")], ""),
        (
            "matrix with two series, series in sorted order",
            vec![s("value_1"), s("value_2")],
            "",
        ),
        (
            "matrix with two series, series in reverse order",
            vec![s("value_2"), s("value_1")],
            r#"matrix results should always be sorted by labels, but matrix is not sorted: series at index 1 with labels {the_label="value_1"} sorts before series at index 0 with labels {the_label="value_2"}"#,
        ),
        (
            "matrix with three series, series in sorted order",
            vec![s("value_1"), s("value_2"), s("value_3")],
            "",
        ),
        (
            "matrix with three series, series not in sorted order",
            vec![s("value_1"), s("value_3"), s("value_2")],
            r#"matrix results should always be sorted by labels, but matrix is not sorted: series at index 2 with labels {the_label="value_2"} sorts before series at index 1 with labels {the_label="value_3"}"#,
        ),
    ];
    for (name, m, expected) in cases {
        let r = assert_matrix_sorted(&m);
        if expected.is_empty() {
            assert!(r.is_ok(), "{}", name);
        } else {
            assert_eq!(r.unwrap_err(), expected, "{}", name);
        }
    }
}
