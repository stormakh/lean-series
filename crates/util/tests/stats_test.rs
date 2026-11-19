//! Port of `util/stats/stats_test.go`.
//!
//! `BenchmarkMergeTotalSamplesFromSubquery` is a Go benchmark and is not ported.

use std::thread::sleep;
use std::time::Duration;

use regex::Regex;
use util::stats::{QuerySamples, QueryTimers, QueryTiming, Statistics, SubqueryConsumer, TimerGroup, new_query_stats};

fn matches(re: &str, s: &str) -> bool {
    Regex::new(re).unwrap().is_match(s)
}

#[test]
fn test_timer_group_new_timer() {
    let mut tg = TimerGroup::new();
    let timer = tg.get_timer(QueryTiming::ExecTotalTime);
    let duration = timer.duration();
    assert_eq!(0.0, duration, "Expected duration equal 0");
    let minimum = Duration::from_millis(2);
    timer.start();
    sleep(minimum);
    timer.stop();
    let duration = timer.duration();
    assert!(duration > 0.0, "Expected duration greater than 0");
    let elapsed = timer.elapsed_time();
    assert!(elapsed >= minimum, "Expected elapsed time to be greater than time slept.");
}

#[test]
fn test_query_stats_with_timers_and_samples() {
    let mut qt = QueryTimers::new();
    let mut qs = QuerySamples::new(true);
    qs.init_step_tracking(20001000, 25001000, 1000000);
    let timer = qt.get_timer(QueryTiming::ExecTotalTime);
    timer.start();
    sleep(Duration::from_millis(2));
    timer.stop();
    qs.increment_samples_at_timestamp(20001000, 5);
    qs.increment_samples_at_timestamp(25001000, 5);
    qs.increment_samples_read_at_timestamp(20001000, 5);
    qs.increment_samples_read_at_timestamp(25001000, 5);
    qs.total_series = 3;

    let qstats = new_query_stats(&Statistics { timers: qt, samples: Some(qs) });
    let actual = qstats.to_json_string();
    // Timing value is one of multiple fields, unit is seconds (float).
    assert!(matches(r#"[,{]"execTotalTime":\d+\.\d+[,}]"#, &actual), "Expected timings with one non-zero entry.");

    assert!(matches(r#"[,{]"totalQueryableSamples":10[,}]"#, &actual), "expected totalQueryableSamples: {actual}");
    assert!(
        matches(
            r#"[,{]"totalQueryableSamplesPerStep":\[\[20001,5\],\[21001,0\],\[22001,0\],\[23001,0\],\[24001,0\],\[25001,5\]\]"#,
            &actual
        ),
        "expected totalQueryableSamplesPerStep: {actual}"
    );
    assert!(matches(r#"[,{]"samplesRead":10[,}]"#, &actual), "expected samplesRead: {actual}");
    assert!(
        matches(
            r#"[,{]"samplesReadPerStep":\[\[20001,5\],\[21001,0\],\[22001,0\],\[23001,0\],\[24001,0\],\[25001,5\]\]"#,
            &actual
        ),
        "expected samplesReadPerStep: {actual}"
    );

    assert!(matches(r#"[,\{]"seriesTouched":3[,}]"#, &actual), "{actual}");
    assert!(!actual.contains(r#""cost""#), "the statistics object must not carry a cost field");
}

#[test]
fn test_query_stats_with_span_timers() {
    // adapted: Go's span timer also opens a tracing span and feeds a Prometheus summary; neither is ported, so
    // the span timer is the timer group's start/finish.
    let mut qt = QueryTimers::new();
    let qs = QuerySamples::new(false);
    qt.start(QueryTiming::ExecQueueTime);
    sleep(Duration::from_millis(5));
    qt.finish(QueryTiming::ExecQueueTime);
    let qstats = new_query_stats(&Statistics { timers: qt, samples: Some(qs) });
    let actual = qstats.to_json_string();
    // Timing value is one of multiple fields, unit is seconds (float).
    assert!(matches(r#"[,{]"execQueueTime":\d+\.\d+[,}]"#, &actual), "Expected timings with one non-zero entry.");
}

#[test]
fn test_merge_from_subquery() {
    struct StepGrid {
        start: i64,
        end: i64,
        interval: i64,
        per_step: Vec<i64>,
    }
    fn g(start: i64, end: i64, interval: i64, per_step: &[i64]) -> StepGrid {
        StepGrid { start, end, interval, per_step: per_step.to_vec() }
    }
    struct Case {
        name: &'static str,
        parent: StepGrid,
        child: StepGrid,
        outer_offset: i64,
        outer_range: i64,
        at_timestamp: Option<i64>,
        want_total_step: Vec<i64>,
        want_read_step: Vec<i64>,
    }
    let cases = vec![
        Case {
            name: "alignedChildGridAddsToParentSteps",
            parent: g(1000, 3000, 1000, &[1, 2, 3]),
            child: g(1000, 3000, 1000, &[10, 20, 30]),
            outer_offset: 0,
            outer_range: 0,
            at_timestamp: None,
            want_total_step: vec![11, 22, 33],
            want_read_step: vec![11, 22, 33],
        },
        Case {
            name: "offsetChildGridAttributesByTimestamp",
            parent: g(1000, 3000, 1000, &[0, 5, 0]),
            child: g(2000, 4000, 1000, &[0, 100, 0]), // tk=3000 -> parent step 2.
            outer_offset: 0,
            outer_range: 0,
            at_timestamp: None,
            want_total_step: vec![0, 5, 100],
            want_read_step: vec![0, 5, 100],
        },
        Case {
            // Child spans 1000..9000 (9 steps). Steps with tk <= parentStart=5000 (k=0..4) clamp to parent step
            // 0; tk=6000 -> step 1; tk=7000 -> step 2; tk=8000,9000 clamp to the last parent step (also 2).
            name: "childBeforeAndAfterParentWindowClampsToEndpoints",
            parent: g(5000, 7000, 1000, &[0, 0, 0]),
            child: g(1000, 9000, 1000, &[1, 1, 1, 1, 1, 1, 1, 1, 1]),
            outer_offset: 0,
            outer_range: 0,
            at_timestamp: None,
            want_total_step: vec![5, 1, 3],
            want_read_step: vec![5, 1, 3],
        },
        Case {
            // Parent step 60s, outer range 30s: window per step is (parentTs-30, parentTs].
            name: "outerRangeExcludesGapSamples",
            parent: g(30000, 90000, 60000, &[0, 0]),
            child: g(10000, 90000, 10000, &[1, 1, 1, 1, 1, 1, 1, 1, 1]),
            outer_offset: 0,
            outer_range: 30000,
            at_timestamp: None,
            want_total_step: vec![3, 3],
            want_read_step: vec![3, 3],
        },
        Case {
            // Overlapping windows: totals count a child step per window, reads once.
            name: "overlappingWindowsCountTotalsPerWindowAndReadsOnce",
            parent: g(30000, 60000, 10000, &[0, 0, 0, 0]),
            child: g(10000, 60000, 10000, &[1, 1, 1, 1, 1, 1]),
            outer_offset: 0,
            outer_range: 30000,
            at_timestamp: None,
            want_total_step: vec![3, 3, 3, 3],
            want_read_step: vec![3, 1, 1, 1],
        },
        Case {
            // Child steps at 30k and 40k are past the last window (10k,20k].
            name: "childAfterLastParentWindowIsDropped",
            parent: g(10000, 20000, 10000, &[0, 0]),
            child: g(10000, 40000, 10000, &[1, 1, 1, 1]),
            outer_offset: 0,
            outer_range: 10000,
            at_timestamp: None,
            want_total_step: vec![1, 1],
            want_read_step: vec![1, 1],
        },
        Case {
            // outerRange=0 disables filtering even when the parent has a wider step than the child's range.
            name: "outerRangeZeroDisablesGapFilter",
            parent: g(30000, 90000, 60000, &[0, 0]),
            child: g(10000, 90000, 10000, &[1, 1, 1, 1, 1, 1, 1, 1, 1]),
            outer_offset: 0,
            outer_range: 0,
            at_timestamp: None,
            want_total_step: vec![3, 6],
            want_read_step: vec![3, 6],
        },
        Case {
            // Offset 60s shifts child tk forward before matching the parent grid.
            name: "outerOffsetShiftsTkIntoWindow",
            parent: g(240000, 240000, 1, &[0]),
            child: g(170000, 180000, 10000, &[1, 1]),
            outer_offset: 60000,
            outer_range: 20000,
            at_timestamp: None,
            want_total_step: vec![2],
            want_read_step: vec![2],
        },
        Case {
            // @ 200s: every parent step consumes the fixed window (180k,200k].
            name: "atModifierConsumesFixedWindow",
            parent: g(250000, 280000, 10000, &[0, 0, 0, 0]),
            child: g(190000, 230000, 10000, &[1, 1, 1, 1, 1]),
            outer_offset: 0,
            outer_range: 20000,
            at_timestamp: Some(200000),
            want_total_step: vec![2, 2, 2, 2],
            want_read_step: vec![2, 0, 0, 0],
        },
        Case {
            // @ 200s offset 60s: child steps 130k and 140k shift into (180k,200k].
            name: "atModifierWithOffsetConsumesShiftedWindow",
            parent: g(250000, 260000, 10000, &[0, 0]),
            child: g(130000, 170000, 10000, &[1, 1, 1, 1, 1]),
            outer_offset: 60000,
            outer_range: 20000,
            at_timestamp: Some(200000),
            want_total_step: vec![2, 2],
            want_read_step: vec![2, 0],
        },
    ];

    let sum = |s: &[i64]| -> i64 { s.iter().sum() };

    for tc in cases {
        let mut parent = QuerySamples::new(true);
        parent.init_step_tracking(tc.parent.start, tc.parent.end, tc.parent.interval);
        copy(&mut parent.total_samples_per_step, &tc.parent.per_step);
        copy(&mut parent.samples_read_per_step, &tc.parent.per_step);
        parent.total_samples = sum(&tc.parent.per_step);
        parent.samples_read = sum(&tc.parent.per_step);

        let mut child = QuerySamples::new_child_with_step_tracking(tc.child.start, tc.child.end, tc.child.interval);
        copy(&mut child.total_samples_per_step, &tc.child.per_step);
        copy(&mut child.samples_read_per_step, &tc.child.per_step);
        child.total_samples = sum(&tc.child.per_step);
        child.samples_read = sum(&tc.child.per_step);

        let consumer = SubqueryConsumer {
            start: tc.parent.start,
            interval: tc.parent.interval,
            num_steps: tc.parent.per_step.len(),
            offset: tc.outer_offset,
            range: tc.outer_range,
            at_timestamp: tc.at_timestamp,
        };
        parent.merge_total_samples_from_subquery(&child, &consumer);
        parent.merge_samples_read_from_subquery(&child, &consumer);

        assert_eq!(tc.want_total_step, parent.total_samples_per_step, "{}", tc.name);
        assert_eq!(sum(&tc.want_total_step), parent.total_samples, "{}", tc.name);
        assert_eq!(tc.want_read_step, parent.samples_read_per_step, "{}", tc.name);
        assert_eq!(sum(&tc.want_read_step), parent.samples_read, "{}", tc.name);
    }
}

/// Go's built-in `copy`.
fn copy(dst: &mut [i64], src: &[i64]) {
    let n = dst.len().min(src.len());
    dst[..n].copy_from_slice(&src[..n]);
}

#[test]
fn test_timer_group() {
    let mut tg = TimerGroup::new();
    assert_eq!("Exec total time: 0s", tg.get_timer(QueryTiming::ExecTotalTime).to_string());
    assert_eq!("Exec queue wait time: 0s", tg.get_timer(QueryTiming::ExecQueueTime).to_string());
    assert_eq!("Inner eval time: 0s", tg.get_timer(QueryTiming::InnerEvalTime).to_string());
    assert_eq!("Query preparation time: 0s", tg.get_timer(QueryTiming::QueryPreparationTime).to_string());
    assert_eq!("Result sorting time: 0s", tg.get_timer(QueryTiming::ResultSortTime).to_string());
    assert_eq!("Eval total time: 0s", tg.get_timer(QueryTiming::EvalTotalTime).to_string());

    let actual = tg.to_string();
    let expected = "Exec total time: 0s\nExec queue wait time: 0s\nInner eval time: 0s\nQuery preparation time: 0s\nResult sorting time: 0s\nEval total time: 0s\n";
    assert_eq!(expected, actual);
}
