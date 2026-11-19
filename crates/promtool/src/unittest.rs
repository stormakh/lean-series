//! Port of cmd/promtool/unittest.go (`promtool test rules`), with util/junitxml ([`junitxml`]) and the parts of
//! github.com/nsf/jsondiff that `--diff` uses ([`jsondiff`]).
//!
//! Go writes to `os.Stdout` and `os.Stderr`; here the functions take the two streams.

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use model::labels::{self, Labels};
use model::regexp::Regexp;
use promql::parser::Parser;
use promql::promqltest::{histogram_test_expression, LazyLoader, LazyLoaderOpts};
use promql::{Engine, Value};
use rules::{Group, Manager, ManagerOptions};
use storage::{Context, Queryable};
use yaml::{Decoder, Node, Unmarshal};

use crate::{FAILURE_EXIT_CODE, SUCCESS_EXIT_CODE};

type Error = crate::Error;

const NS_PER_MS: i64 = 1_000_000;
const NS_PER_SEC: i64 = 1_000_000_000;

/// Go `RulesUnitTest`: unit tests of rules from the given unit test files. The JUnit XML result is discarded.
#[allow(clippy::too_many_arguments)]
pub fn rules_unit_test(
    query_opts: LazyLoaderOpts,
    p: Parser,
    run_strings: Option<&[String]>,
    diff_flag: bool,
    debug: bool,
    ignore_unknown_fields: bool,
    files: &[String],
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> i32 {
    rules_unit_test_result(
        &mut std::io::sink(),
        query_opts,
        p,
        run_strings,
        diff_flag,
        debug,
        ignore_unknown_fields,
        files,
        stdout,
        stderr,
    )
}

/// Go `RulesUnitTestResult`: like [`rules_unit_test`], and writes the JUnit XML result to `results`.
#[allow(clippy::too_many_arguments)]
pub fn rules_unit_test_result(
    results: &mut dyn Write,
    query_opts: LazyLoaderOpts,
    p: Parser,
    run_strings: Option<&[String]>,
    diff_flag: bool,
    debug: bool,
    ignore_unknown_fields: bool,
    files: &[String],
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> i32 {
    let mut failed = false;
    let mut junit = junitxml::JUnitXml::default();

    let run = run_strings.map(|rs| Regexp::must_compile(&rs.join("|")));

    for f in files {
        let _ = writeln!(stdout, "Processing test file: {f}");
        let errs = rule_unit_test(
            f,
            &query_opts,
            &p,
            run.as_ref(),
            diff_flag,
            debug,
            ignore_unknown_fields,
            junit.suite(f),
            stderr,
        );
        if let Some(errs) = errs {
            let _ = writeln!(stderr, "  FAILED:");
            for e in &errs {
                let _ = writeln!(stderr, "{e}");
                let _ = writeln!(stdout);
            }
            failed = true;
        } else {
            let _ = writeln!(stdout, "  SUCCESS");
        }
        let _ = writeln!(stdout);
    }
    if let Err(err) = junit.write_xml(results) {
        let _ = writeln!(stderr, "failed to write JUnit XML: {err}");
    }
    if failed {
        return FAILURE_EXIT_CODE;
    }
    SUCCESS_EXIT_CODE
}

#[allow(clippy::too_many_arguments)]
fn rule_unit_test(
    filename: &str,
    query_opts: &LazyLoaderOpts,
    p: &Parser,
    run: Option<&Regexp>,
    diff_flag: bool,
    debug: bool,
    ignore_unknown_fields: bool,
    ts: &mut junitxml::TestSuite,
    stderr: &mut dyn Write,
) -> Option<Vec<Error>> {
    let b = match std::fs::read(filename) {
        Ok(b) => b,
        Err(e) => {
            let err: Error = go_io_error("open", filename, &e).into();
            ts.abort(&err);
            return Some(vec![err]);
        }
    };

    let mut unit_test_inp = UnitTestFile::default();
    let text = String::from_utf8_lossy(&b);
    if let Err(e) = yaml::unmarshal_into(&text, &mut unit_test_inp, true) {
        let err: Error = e.to_string().into();
        ts.abort(&err);
        return Some(vec![err]);
    }
    if let Err(e) = resolve_and_glob_filepaths(go_dir(filename), &mut unit_test_inp, stderr) {
        let err: Error = e.into();
        ts.abort(&err);
        return Some(vec![err]);
    }

    if unit_test_inp.evaluation_interval.0 == 0 {
        unit_test_inp.evaluation_interval = yaml::Duration::from_secs(60);
    }

    let eval_interval = unit_test_inp.evaluation_interval.0;
    ts.settime(&local_time_now());
    // Giving number for groups mentioned in the file for ordering.
    // Lower number group should be evaluated before higher number group.
    let mut group_order_map: HashMap<String, usize> = HashMap::new();
    for (i, gn) in unit_test_inp.group_eval_order.iter().enumerate() {
        if group_order_map.contains_key(gn) {
            let err: Error = format!("group name repeated in evaluation order: {gn}").into();
            ts.abort(&err);
            return Some(vec![err]);
        }
        group_order_map.insert(gn.clone(), i);
    }

    // Testing.
    let mut errs: Vec<Error> = Vec::new();
    let fuzzy = unit_test_inp.fuzzy_compare;
    let rule_files = unit_test_inp.rule_files.clone();
    for (i, t) in unit_test_inp.tests.iter_mut().enumerate() {
        if !matches_run(&t.test_group_name, run) {
            continue;
        }
        let testname = if t.test_group_name.is_empty() { format!("unnamed#{i}") } else { t.test_group_name.clone() };
        let tc = ts.case(&testname);
        if t.interval.0 == 0 {
            t.interval = unit_test_inp.evaluation_interval;
        }
        let ers = t.test(
            p,
            &testname,
            eval_interval,
            &group_order_map,
            query_opts.clone(),
            diff_flag,
            debug,
            ignore_unknown_fields,
            fuzzy,
            &rule_files,
            stderr,
        );
        if let Some(ers) = ers {
            for e in &ers {
                tc.fail(&e.to_string());
            }
            errs.extend(ers);
        }
    }

    if !errs.is_empty() {
        return Some(errs);
    }
    None
}

fn matches_run(name: &str, run: Option<&Regexp>) -> bool {
    match run {
        None => true,
        Some(r) => r.is_match(name),
    }
}

// ---------------------------------------------------------------------------------------------------------------
// The unit test file.

/// Go `unitTestFile`: the contents of a single unit test file.
#[derive(Default)]
struct UnitTestFile {
    rule_files: Vec<String>,
    evaluation_interval: yaml::Duration,
    group_eval_order: Vec<String>,
    tests: Vec<TestGroup>,
    fuzzy_compare: bool,
}
yaml::yaml_struct!(UnitTestFile, "main.unitTestFile", [
    (rule_files: "rule_files")
    (evaluation_interval: "evaluation_interval", omitempty)
    (group_eval_order: "group_eval_order")
    (tests: "tests")
    (fuzzy_compare: "fuzzy_compare", omitempty)
]);
yaml::impl_plain_struct!(UnitTestFile);

/// Go `resolveAndGlobFilepaths`: joins all relative paths in a configuration with a given base directory and
/// replaces all globs with matching files.
fn resolve_and_glob_filepaths(base_dir: &str, utf: &mut UnitTestFile, stderr: &mut dyn Write) -> Result<(), String> {
    for rf in utf.rule_files.iter_mut() {
        if !rf.is_empty() && !rf.starts_with('/') {
            *rf = go_join(base_dir, rf);
        }
    }

    let mut globbed_files = Vec::new();
    for rf in &utf.rule_files {
        let m = discovery::glob::glob(rf)?;
        if m.is_empty() {
            let _ = writeln!(stderr, "  WARNING: no file match pattern {rf}");
        }
        globbed_files.extend(m);
    }
    utf.rule_files = globbed_files;
    Ok(())
}

/// Go `testStartTimestamp`: a time that YAML gives as an RFC 3339 string or a Unix timestamp. `None` is Go's zero
/// time. The value is in nanoseconds since the Unix epoch.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct TestStartTimestamp(Option<i64>);

impl Unmarshal for TestStartTimestamp {
    fn unmarshal_yaml(&mut self, n: &Node, d: &mut Decoder) -> yaml::Result<bool> {
        d.custom(|d| {
            let mut s = String::new();
            d.inner(n, &mut s)?;
            self.0 = Some(parse_time(&s).map_err(yaml::Error::Msg)?);
            Ok(())
        })
    }
    fn go_type() -> String {
        "main.testStartTimestamp".to_string()
    }
}
impl yaml::Marshal for TestStartTimestamp {
    fn marshal_yaml(&self) -> Result<yaml::Value, String> {
        Ok(yaml::Value::Map(Vec::new()))
    }
    fn is_zero(&self) -> bool {
        self.0.is_none()
    }
}

/// Go `testGroup`: a group of input series and tests associated with it.
#[derive(Default)]
struct TestGroup {
    interval: yaml::Duration,
    input_series: Vec<Series>,
    alert_rule_tests: Vec<AlertTestCase>,
    promql_expr_tests: Vec<PromqlTestCase>,
    external_labels: Labels,
    external_url: String,
    test_group_name: String,
    start_timestamp: TestStartTimestamp,
}
yaml::yaml_struct!(TestGroup, "main.testGroup", [
    (interval: "interval")
    (input_series: "input_series")
    (alert_rule_tests: "alert_rule_test", omitempty)
    (promql_expr_tests: "promql_expr_test", omitempty)
    (external_labels: "external_labels", omitempty)
    (external_url: "external_url", omitempty)
    (test_group_name: "name", omitempty)
    (start_timestamp: "start_timestamp", omitempty)
]);
yaml::impl_plain_struct!(TestGroup);

#[derive(Default)]
struct Series {
    series: String,
    values: String,
}
yaml::yaml_struct!(Series, "main.series", [
    (series: "series")
    (values: "values")
]);
yaml::impl_plain_struct!(Series);

#[derive(Default, Clone)]
struct AlertTestCase {
    eval_time: yaml::Duration,
    alertname: String,
    exp_alerts: Vec<Alert>,
}
yaml::yaml_struct!(AlertTestCase, "main.alertTestCase", [
    (eval_time: "eval_time")
    (alertname: "alertname")
    (exp_alerts: "exp_alerts")
]);
yaml::impl_plain_struct!(AlertTestCase);

#[derive(Default, Clone)]
struct Alert {
    exp_labels: BTreeMap<String, String>,
    exp_annotations: BTreeMap<String, String>,
}
yaml::yaml_struct!(Alert, "main.alert", [
    (exp_labels: "exp_labels")
    (exp_annotations: "exp_annotations")
]);
yaml::impl_plain_struct!(Alert);

#[derive(Default)]
struct PromqlTestCase {
    expr: String,
    eval_time: yaml::Duration,
    exp_samples: Vec<Sample>,
}
yaml::yaml_struct!(PromqlTestCase, "main.promqlTestCase", [
    (expr: "expr")
    (eval_time: "eval_time")
    (exp_samples: "exp_samples")
]);
yaml::impl_plain_struct!(PromqlTestCase);

#[derive(Default)]
struct Sample {
    labels: String,
    value: f64,
    /// A non-empty string means Value is ignored.
    histogram: String,
}
yaml::yaml_struct!(Sample, "main.sample", [
    (labels: "labels")
    (value: "value")
    (histogram: "histogram")
]);
yaml::impl_plain_struct!(Sample);

/// Go `parsedSample`: a sample with parsed labels.
#[derive(Clone)]
struct ParsedSample {
    labels: Labels,
    value: f64,
    /// TestExpression() of histogram.FloatHistogram.
    histogram: String,
}

impl ParsedSample {
    fn string(&self) -> String {
        if !self.histogram.is_empty() {
            return format!("{} {}", self.labels, self.histogram);
        }
        format!("{} {}", self.labels, format_float_e(self.value))
    }
}

fn parsed_samples_string(pss: &[ParsedSample]) -> String {
    if pss.is_empty() {
        return "nil".to_string();
    }
    let mut s = pss[0].string();
    for ps in &pss[1..] {
        s.push_str(", ");
        s.push_str(&ps.string());
    }
    s
}

#[derive(Clone)]
struct LabelAndAnnotation {
    labels: Labels,
    annotations: Labels,
}

impl LabelAndAnnotation {
    fn string(&self) -> String {
        format!("Labels:{}\nAnnotations:{}", self.labels, self.annotations)
    }
}

/// Go `labelsAndAnnotations.Less`.
fn la_less(a: &LabelAndAnnotation, b: &LabelAndAnnotation) -> bool {
    let diff = labels::compare(&a.labels, &b.labels);
    if diff != 0 {
        return diff < 0;
    }
    labels::compare(&a.annotations, &b.annotations) < 0
}

/// Go `labelsAndAnnotations.String`.
fn las_string(la: &[LabelAndAnnotation]) -> String {
    if la.is_empty() {
        return "[]".to_string();
    }
    let mut s = String::new();
    s.push_str(&format!("[\n0:{}", indent_lines(&format!("\n{}", la[0].string()), "  ")));
    for (i, l) in la[1..].iter().enumerate() {
        s.push_str(&format!(",\n{}:{}", i + 1, indent_lines(&format!("\n{}", l.string()), "  ")));
    }
    s.push_str("\n]");
    s
}

/// Go `json.Marshal(labelsAndAnnotations)` decoded again as the jsondiff value.
fn las_json(la: &[LabelAndAnnotation]) -> jsondiff::Value {
    let lbls = |l: &Labels| {
        let mut m = BTreeMap::new();
        for (k, v) in l.iter() {
            m.insert(k.to_string(), jsondiff::Value::Str(v.to_string()));
        }
        jsondiff::Value::Obj(m)
    };
    jsondiff::Value::Arr(
        la.iter()
            .map(|x| {
                let mut m = BTreeMap::new();
                m.insert("Labels".to_string(), lbls(&x.labels));
                m.insert("Annotations".to_string(), lbls(&x.annotations));
                jsondiff::Value::Obj(m)
            })
            .collect(),
    )
}

/// Go `indentLines`: prefixes each line but the first with `indent`.
fn indent_lines(lines: &str, indent: &str) -> String {
    let mut sb = String::new();
    let n: Vec<&str> = lines.split('\n').collect();
    for (i, l) in n.iter().enumerate() {
        if i > 0 {
            sb.push_str(indent);
        }
        sb.push_str(l);
        if i != n.len() - 1 {
            sb.push('\n');
        }
    }
    sb
}

impl TestGroup {
    /// Go `testGroup.test`: performs the unit tests.
    #[allow(clippy::too_many_arguments)]
    fn test(
        &self,
        p: &Parser,
        testname: &str,
        eval_interval: i64,
        group_order_map: &HashMap<String, usize>,
        query_opts: LazyLoaderOpts,
        diff_flag: bool,
        debug: bool,
        ignore_unknown_fields: bool,
        fuzzy_compare: bool,
        rule_files: &[String],
        stderr: &mut dyn Write,
    ) -> Option<Vec<Error>> {
        let test_start = Instant::now();
        if debug {
            let _ = writeln!(stderr, "DEBUG: Starting test {testname}");
        }
        let r = self.test_inner(
            p,
            eval_interval,
            group_order_map,
            query_opts,
            diff_flag,
            debug,
            ignore_unknown_fields,
            fuzzy_compare,
            rule_files,
            stderr,
        );
        if debug {
            let _ = writeln!(
                stderr,
                "DEBUG: Test {testname} finished, took {}",
                go_duration_string(test_start.elapsed().as_nanos() as i64)
            );
        }
        r
    }

    #[allow(clippy::too_many_arguments)]
    fn test_inner(
        &self,
        p: &Parser,
        eval_interval: i64,
        group_order_map: &HashMap<String, usize>,
        mut query_opts: LazyLoaderOpts,
        diff_flag: bool,
        debug: bool,
        ignore_unknown_fields: bool,
        fuzzy_compare: bool,
        rule_files: &[String],
        stderr: &mut dyn Write,
    ) -> Option<Vec<Error>> {
        // Setup testing suite.
        // Set the start time from the test group.
        query_opts.start_time = self.start_timestamp.0.map(|ns| ns.div_euclid(NS_PER_MS));
        // Evaluate with the same parser the rule files are loaded with, so that
        // syntax enabled by a feature flag survives into evaluation.
        query_opts.parser = Some(p.clone());
        let mut suite = match LazyLoader::new(&self.series_loading_string(), query_opts) {
            Ok(s) => s,
            Err(e) => return Some(vec![e.into()]),
        };
        let mut out = self.run_suite(&mut suite, p, eval_interval, group_order_map, diff_flag, debug, ignore_unknown_fields, fuzzy_compare, rule_files, stderr);
        // Go: deferred suite.Close().
        if let Err(e) = suite.close() {
            out.get_or_insert_with(Vec::new).push(e.into());
        }
        out
    }

    #[allow(clippy::too_many_arguments)]
    fn run_suite(
        &self,
        suite: &mut LazyLoader,
        p: &Parser,
        eval_interval: i64,
        group_order_map: &HashMap<String, usize>,
        diff_flag: bool,
        debug: bool,
        ignore_unknown_fields: bool,
        fuzzy_compare: bool,
        rule_files: &[String],
        stderr: &mut dyn Write,
    ) -> Option<Vec<Error>> {
        suite.subquery_interval.store(eval_interval / NS_PER_MS, std::sync::atomic::Ordering::Relaxed);

        // Load the rule files.
        let engine: Arc<Engine> = Arc::new(suite.query_engine().clone());
        let storage = Arc::new(suite.storage().clone());
        let opts = ManagerOptions {
            query_func: Some(rules::engine_query_func(engine, storage.clone() as Arc<dyn Queryable>)),
            appendable_v2: Some(storage as Arc<dyn storage::AppendableV2>),
            context: Context::background(),
            notify_func: Some(Arc::new(|_: &Context, _: &str, _: Vec<rules::Alert>| {})),
            logger: rules::log::Logger::nop(),
            parser: Some(p.clone()),
            ..ManagerOptions::default()
        };
        let m = Manager::new(opts);
        let groups_map = match m.load_groups(
            std::time::Duration::from_nanos(self.interval.0.max(0) as u64),
            &self.external_labels,
            &self.external_url,
            None,
            ignore_unknown_fields,
            rule_files,
        ) {
            Ok(g) => g,
            Err(ers) => return Some(ers),
        };
        let groups = ordered_groups(groups_map, group_order_map);

        // Bounds for evaluating the rules.
        // Times are i128 nanoseconds: Go's time.Time does not overflow where an i64 of nanoseconds would.
        let mint: i128 = self.start_timestamp.0.unwrap_or(0) as i128;
        let maxt = mint + self.max_eval_time() as i128;

        // Optional floating point compare fuzzing.
        let compare_float64 = |x: f64, y: f64| -> bool {
            if fuzzy_compare {
                x == y || next_after(x, f64::NEG_INFINITY) == y || next_after(x, f64::INFINITY) == y
            } else {
                x == y
            }
        };

        // Pre-processing some data for testing alerts.
        // All this preparation is so that we can test alerts as we evaluate the rules.
        // This avoids storing them in memory, as the number of evals might be high.

        // All the `eval_time` for which we have unit tests for alerts.
        let mut alert_eval_times: Vec<i64> = Vec::new();
        // Map of all the eval_time+alertname combination present in the unit tests.
        let mut alerts_in_test: HashMap<i64, HashMap<String, ()>> = HashMap::new();
        // Map of all the unit tests for given eval_time.
        let mut alert_tests: HashMap<i64, Vec<AlertTestCase>> = HashMap::new();
        for alert in &self.alert_rule_tests {
            if alert.alertname.is_empty() {
                let test_group_log = if !self.test_group_name.is_empty() {
                    format!(" (in TestGroup {})", self.test_group_name)
                } else {
                    String::new()
                };
                return Some(vec![format!(
                    "an item under alert_rule_test misses required attribute alertname at eval_time {}{}",
                    alert.eval_time, test_group_log
                )
                .into()]);
            }
            if !alert_eval_times.contains(&alert.eval_time.0) {
                alert_eval_times.push(alert.eval_time.0);
            }
            alerts_in_test.entry(alert.eval_time.0).or_default().insert(alert.alertname.clone(), ());
            alert_tests.entry(alert.eval_time.0).or_default().push(alert.clone());
        }
        alert_eval_times.sort();

        // Current index in alertEvalTimes what we are looking at.
        let mut curr = 0;

        for g in &groups {
            for r in g.all_rules() {
                if let Some(alert_rule) = r.as_alerting() {
                    // Mark alerting rules as restored, to ensure the ALERTS timeseries is
                    // created when they run.
                    alert_rule.set_restored(true);
                }
            }
        }

        let ctx = suite.context();
        let mut errs: Vec<Error> = Vec::new();
        let mut ts = mint;
        while ts <= maxt {
            // Collects the alerts asked for unit testing.
            let mut eval_errs: Vec<Error> = Vec::new();
            // Go `ts.Sub(time.Unix(0, 0))` saturates.
            let since_epoch = sat(ts);
            let till = since_epoch / NS_PER_MS;
            suite.with_samples_till(till, |r| {
                if let Err(err) = r {
                    errs.push(err.into());
                    return;
                }
                for g in &groups {
                    g.eval(&ctx, rules::Time::from_unix_nano(since_epoch));
                    for r in g.all_rules() {
                        if let Some(e) = r.last_error() {
                            eval_errs.push(
                                format!("    rule: {}, time: {}, err: {}", r.name(), go_duration_string(since_epoch), e).into(),
                            );
                        }
                    }
                }
            });
            let had_eval_errs = !eval_errs.is_empty();
            errs.extend(eval_errs);
            // Only end testing at this point if errors occurred evaluating above,
            // rather than any test failures already collected in errs.
            if had_eval_errs {
                return Some(errs);
            }

            while curr < alert_eval_times.len()
                && sat(ts - mint) <= alert_eval_times[curr]
                && alert_eval_times[curr] < sat(ts + eval_interval as i128 - mint)
            {
                // We need to check alerts for this time.
                // If 'ts <= `eval_time=alertEvalTimes[curr]` < ts+evalInterval'
                // then we compare alerts with the Eval at `ts`.
                let t = alert_eval_times[curr];

                let present_alerts = &alerts_in_test[&t];
                let mut got: HashMap<String, Vec<LabelAndAnnotation>> = HashMap::new();

                // Same Alert name can be present in multiple groups.
                // Hence we collect them all to check against expected alerts.
                for g in &groups {
                    for r in g.all_rules() {
                        let Some(ar) = r.as_alerting() else { continue };
                        if !present_alerts.contains_key(r.name()) {
                            continue;
                        }

                        let mut alerts = Vec::new();
                        for a in ar.active_alerts() {
                            if a.state == rules::AlertState::Firing {
                                alerts.push(LabelAndAnnotation {
                                    labels: a.labels.copy(),
                                    annotations: a.annotations.copy(),
                                });
                            }
                        }

                        got.entry(r.name().to_string()).or_default().extend(alerts);
                    }
                }

                for testcase in &alert_tests[&t] {
                    // Checking alerts.
                    let mut got_alerts = got.get(&testcase.alertname).cloned().unwrap_or_default();

                    let mut exp_alerts = Vec::new();
                    for a in &testcase.exp_alerts {
                        // User gives only the labels from alerting rule, which doesn't
                        // include this label (added by Prometheus during Eval).
                        let mut exp_labels = a.exp_labels.clone();
                        exp_labels.insert(labels::ALERT_NAME.to_string(), testcase.alertname.clone());

                        exp_alerts.push(LabelAndAnnotation {
                            labels: labels::from_map(exp_labels),
                            annotations: labels::from_map(a.exp_annotations.clone()),
                        });
                    }

                    promql::gosort::sort(&mut got_alerts, la_less);
                    promql::gosort::sort(&mut exp_alerts, la_less);

                    let equal = exp_alerts.len() == got_alerts.len()
                        && exp_alerts.iter().zip(&got_alerts).all(|(a, b)| {
                            labels::equal(&a.labels, &b.labels) && labels::equal(&a.annotations, &b.annotations)
                        });
                    if !equal {
                        let test_name = if !self.test_group_name.is_empty() {
                            format!("    name: {},\n", self.test_group_name)
                        } else {
                            String::new()
                        };
                        let exp_string = indent_lines(&las_string(&exp_alerts), "            ");
                        let got_string = indent_lines(&las_string(&got_alerts), "            ");
                        if diff_flag {
                            // If empty, populates an empty value
                            if got_alerts.is_empty() {
                                got_alerts.push(LabelAndAnnotation { labels: Labels::empty(), annotations: Labels::empty() });
                            }
                            // If empty, populates an empty value
                            if exp_alerts.is_empty() {
                                exp_alerts.push(LabelAndAnnotation { labels: Labels::empty(), annotations: Labels::empty() });
                            }

                            let (res, diff) = jsondiff::compare(&las_json(&exp_alerts), &las_json(&got_alerts));
                            if res != jsondiff::Difference::FullMatch {
                                errs.push(
                                    format!(
                                        "{}    alertname: {}, time: {}, \n        diff: {}",
                                        test_name,
                                        testcase.alertname,
                                        testcase.eval_time,
                                        indent_lines(&diff, "            ")
                                    )
                                    .into(),
                                );
                            }
                        } else {
                            errs.push(
                                format!(
                                    "{}    alertname: {}, time: {}, \n        exp:{}, \n        got:{}",
                                    test_name, testcase.alertname, testcase.eval_time, exp_string, got_string
                                )
                                .into(),
                            );
                        }
                    }
                }

                curr += 1;
            }
            ts += eval_interval as i128;
        }

        // Checking promql expressions.
        'outer: for test_case in &self.promql_expr_tests {
            let got = match query(&ctx, &test_case.expr, sat(mint + test_case.eval_time.0 as i128), suite.query_engine(), suite.queryable())
            {
                Ok(g) => g,
                Err(err) => {
                    errs.push(
                        format!(
                            "    expr: {}, time: {}, err: {}",
                            model::unicode::quote(&test_case.expr),
                            test_case.eval_time,
                            err
                        )
                        .into(),
                    );
                    continue;
                }
            };

            let mut got_samples: Vec<ParsedSample> = Vec::new();
            for s in &got {
                got_samples.push(ParsedSample {
                    labels: s.metric.copy(),
                    value: s.f,
                    histogram: histogram_test_expression(s.h.as_deref()),
                });
            }

            let mut exp_samples: Vec<ParsedSample> = Vec::new();
            for s in &test_case.exp_samples {
                let mut res: Result<Labels, String> = p.parse_metric(&s.labels).map_err(|e| e.to_string());
                let mut hist = None;
                if res.is_ok() && !s.histogram.is_empty() {
                    match p.parse_series_desc(&format!("{{}} {}", s.histogram)) {
                        Err(parse_err) => res = Err(parse_err.to_string()),
                        Ok((_, values)) => {
                            if values.len() != 1 {
                                res = Err(format!("expected 1 value, got {}", values.len()));
                            } else if values[0].histogram.is_none() {
                                res = Err(format!("expected histogram, got {}", values[0]));
                            } else {
                                hist = values.into_iter().next().and_then(|v| v.histogram);
                            }
                        }
                    }
                }
                let lb = match res {
                    Ok(lb) => lb,
                    Err(err) => {
                        let err = format!("labels {}: {}", model::unicode::quote(&s.labels), err);
                        errs.push(
                            format!(
                                "    expr: {}, time: {}, err: {}",
                                model::unicode::quote(&test_case.expr),
                                test_case.eval_time,
                                err
                            )
                            .into(),
                        );
                        continue 'outer;
                    }
                };
                exp_samples.push(ParsedSample {
                    labels: lb,
                    value: s.value,
                    histogram: histogram_test_expression(hist.as_deref()),
                });
            }

            promql::gosort::sort(&mut exp_samples, |a, b| labels::compare(&a.labels, &b.labels) <= 0);
            promql::gosort::sort(&mut got_samples, |a, b| labels::compare(&a.labels, &b.labels) <= 0);
            let equal = exp_samples.len() == got_samples.len()
                && exp_samples.iter().zip(&got_samples).all(|(a, b)| {
                    labels::equal(&a.labels, &b.labels) && compare_float64(a.value, b.value) && a.histogram == b.histogram
                });
            if !equal {
                errs.push(
                    format!(
                        "    expr: {}, time: {},\n        exp: {}\n        got: {}",
                        model::unicode::quote(&test_case.expr),
                        test_case.eval_time,
                        parsed_samples_string(&exp_samples),
                        parsed_samples_string(&got_samples)
                    )
                    .into(),
                );
            }
        }

        if debug {
            let mut ts = self.max_eval_time();
            // Potentially a test can be specified at a time with fractional seconds,
            // which PromQL cannot represent, so round up to the next whole second.
            ts = go_truncate(ts + NS_PER_SEC, NS_PER_SEC);
            let expr = format!("{{__name__=~\".+\"}}[{}]", go_duration_string(ts));
            let bg = Context::background();
            let mut q = match suite.query_engine().new_instant_query(&bg, suite.queryable(), None, &expr, sat(mint + ts as i128) / NS_PER_MS) {
                Ok(q) => q,
                Err(err) => {
                    let _ = writeln!(stderr, "DEBUG: Failed querying, expr: {}, err: {}", model::unicode::quote(&expr), err);
                    return if errs.is_empty() { None } else { Some(errs) };
                }
            };
            let res = q.exec(&ctx);
            if let Some(err) = res.err {
                let _ = writeln!(stderr, "DEBUG: Failed query exec, expr: {}, err: {}", model::unicode::quote(&expr), err);
                return if errs.is_empty() { None } else { Some(errs) };
            }
            match res.value {
                Some(Value::Matrix(v)) => {
                    let _ = writeln!(stderr, "DEBUG: Dump of all data (input_series and rules) at {}:", go_duration_string(ts));
                    let _ = writeln!(stderr, "{}", promql::matrix_string(&v));
                }
                other => {
                    let t = match other {
                        Some(Value::Vector(_)) => "promql.Vector",
                        Some(Value::Scalar(_)) => "promql.Scalar",
                        Some(Value::String(_)) => "promql.String",
                        _ => "<nil>",
                    };
                    let _ = writeln!(stderr, "DEBUG: Got unexpected type {t}");
                    return if errs.is_empty() { None } else { Some(errs) };
                }
            }
        }

        if !errs.is_empty() {
            return Some(errs);
        }
        None
    }

    /// Go `seriesLoadingString`: the input series in PromQL notation.
    fn series_loading_string(&self) -> String {
        let mut result = format!("load {}\n", short_duration(self.interval));
        for is in &self.input_series {
            result.push_str(&format!("  {} {}\n", is.series, is.values));
        }
        result
    }

    /// Go `maxEvalTime`: the max eval time among all alert and promql unit tests (nanoseconds).
    fn max_eval_time(&self) -> i64 {
        let mut maxd = 0i64;
        for alert in &self.alert_rule_tests {
            if alert.eval_time.0 > maxd {
                maxd = alert.eval_time.0;
            }
        }
        for pet in &self.promql_expr_tests {
            if pet.eval_time.0 > maxd {
                maxd = pet.eval_time.0;
            }
        }
        maxd
    }
}

fn short_duration(d: yaml::Duration) -> String {
    let mut s = d.go_string();
    if s.ends_with("m0s") {
        s.truncate(s.len() - 2);
    }
    if s.ends_with("h0m") {
        s.truncate(s.len() - 2);
    }
    s
}

/// Go `orderedGroups`: the groups in the order of `group_order_map` (a partial ordering).
fn ordered_groups(groups_map: HashMap<String, Arc<Group>>, group_order_map: &HashMap<String, usize>) -> Vec<Arc<Group>> {
    let mut groups: Vec<Arc<Group>> = groups_map.into_values().collect();
    let key = |g: &Arc<Group>| group_order_map.get(g.name()).copied().unwrap_or(0);
    promql::gosort::sort(&mut groups, |a, b| key(a) < key(b));
    groups
}

/// Go `query`: an instant query whose result must be a vector or a scalar. `t` is in nanoseconds.
fn query(ctx: &Context, qs: &str, t: i64, engine: &Engine, qu: Arc<dyn Queryable>) -> Result<promql::Vector, Error> {
    let mut q = engine.new_instant_query(ctx, qu, None, qs, t / NS_PER_MS)?;
    let res = q.exec(ctx);
    if let Some(err) = res.err {
        return Err(err.into());
    }
    match res.value {
        Some(Value::Vector(v)) => Ok(v),
        Some(Value::Scalar(v)) => {
            Ok(vec![promql::Sample { t: v.t, f: v.v, h: None, metric: Labels::empty(), drop_name: false }])
        }
        _ => Err("rule result is not a vector or scalar".into()),
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Small Go helpers.

/// Go `parseTime` of query.go: a Unix timestamp (float seconds) or an RFC 3339 time, in nanoseconds.
fn parse_time(s: &str) -> Result<i64, String> {
    if let Some(t) = go_parse_float(s) {
        let sec = t.trunc();
        let frac = t - sec;
        return Ok((sec as i64).wrapping_mul(NS_PER_SEC).wrapping_add((frac * NS_PER_SEC as f64) as i64));
    }
    if let Some(t) = parse_rfc3339_nano(s) {
        return Ok(t);
    }
    Err(format!("cannot parse {} to a valid timestamp", model::unicode::quote(s)))
}

/// Go `strconv.ParseFloat(s, 64)` for the decimal forms (and Inf/NaN); out of range values are errors.
fn go_parse_float(s: &str) -> Option<f64> {
    let b = s.as_bytes();
    let body = s.trim_start_matches(['+', '-']);
    if s.len() - body.len() > 1 {
        return None;
    }
    let lower = body.to_ascii_lowercase();
    if lower == "inf" || lower == "infinity" || lower == "nan" {
        return s.parse::<f64>().ok();
    }
    // Only digits, one dot, and an exponent (Rust's parser accepts the same decimal syntax).
    if body.is_empty() || !body.bytes().all(|c| c.is_ascii_digit() || matches!(c, b'.' | b'e' | b'E' | b'+' | b'-')) {
        return None;
    }
    let _ = b;
    let f = s.parse::<f64>().ok()?;
    if f.is_infinite() {
        return None;
    }
    Some(f)
}

/// Go `time.Parse(time.RFC3339Nano, s)`, in nanoseconds since the Unix epoch.
fn parse_rfc3339_nano(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    let num = |a: usize, n: usize| -> Option<i64> {
        let x = b.get(a..a + n)?;
        if !x.iter().all(|c| c.is_ascii_digit()) {
            return None;
        }
        std::str::from_utf8(x).ok()?.parse().ok()
    };
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let (y, mo, d, h, mi, se) = (num(0, 4)?, num(5, 2)?, num(8, 2)?, num(11, 2)?, num(14, 2)?, num(17, 2)?);
    if !(1..=12).contains(&mo) || h >= 24 || mi >= 60 || se >= 60 {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let mdays = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][(mo - 1) as usize];
    if d < 1 || d > mdays {
        return None;
    }
    let mut i = 19;
    let mut nanos = 0i64;
    if b[i] == b'.' || b[i] == b',' {
        let st = i + 1;
        i = st;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == st {
            return None;
        }
        for (k, c) in b[st..i].iter().take(9).enumerate() {
            nanos += (c - b'0') as i64 * 10i64.pow(8 - k as u32);
        }
    }
    let offset = match &b[i..] {
        b"Z" => 0,
        o if o.len() == 6 && (o[0] == b'+' || o[0] == b'-') && o[3] == b':' => {
            let hh = num(i + 1, 2)?;
            let mm = num(i + 4, 2)?;
            if hh >= 24 || mm >= 60 {
                return None;
            }
            let v = hh * 3600 + mm * 60;
            if o[0] == b'-' {
                -v
            } else {
                v
            }
        }
        _ => return None,
    };
    let days = days_from_civil(y, mo, d);
    let secs = days * 86400 + h * 3600 + mi * 60 + se - offset;
    Some(secs * NS_PER_SEC + nanos)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Go `time.Now().Format("2006-01-02T15:04:05")` in the local time zone.
fn local_time_now() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let secs = now + local_offset(now);
    let (y, m, d) = civil_from_days(secs.div_euclid(86400));
    let rem = secs.rem_euclid(86400);
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}", rem / 3600, rem % 3600 / 60, rem % 60)
}

/// The UTC offset (seconds) of the local time zone at `unix` (Go `time.Local`: `$TZ`, else /etc/localtime), read
/// from the TZif transitions. After the last transition, the type of the last transition is used.
fn local_offset(unix: i64) -> i64 {
    let path = match std::env::var("TZ") {
        Err(_) => "/etc/localtime".to_string(),
        Ok(tz) if tz.is_empty() || tz == "UTC" => return 0,
        Ok(tz) => {
            let tz = tz.trim_start_matches(':').to_string();
            if tz.starts_with('/') {
                tz
            } else {
                format!("/usr/share/zoneinfo/{tz}")
            }
        }
    };
    let Ok(data) = std::fs::read(path) else { return 0 };
    tzif_offset(&data, unix).unwrap_or(0)
}

fn tzif_offset(data: &[u8], unix: i64) -> Option<i64> {
    let be32 = |p: usize| -> Option<i64> { Some(i32::from_be_bytes(data.get(p..p + 4)?.try_into().ok()?) as i64) };
    let be64 = |p: usize| -> Option<i64> { Some(i64::from_be_bytes(data.get(p..p + 8)?.try_into().ok()?)) };
    if data.get(0..4)? != b"TZif" {
        return None;
    }
    let counts = |base: usize| -> Option<[usize; 6]> {
        let mut c = [0usize; 6];
        for (i, v) in c.iter_mut().enumerate() {
            *v = be32(base + 20 + 4 * i)? as usize;
        }
        Some(c)
    };
    let c1 = counts(0)?;
    let (mut base, mut tsize, mut c) = (0usize, 4usize, c1);
    if data[4] >= b'2' {
        // Skip the v1 data block; use the 64-bit block.
        let v1len = 44 + c1[3] * 5 + c1[4] * 6 + c1[5] + c1[2] * 8 + c1[1] + c1[0];
        base = v1len;
        tsize = 8;
        c = counts(base)?;
    }
    let [_isut, _isstd, _leap, timecnt, typecnt, _charcnt] = c;
    let trans = base + 44;
    let idxs = trans + timecnt * tsize;
    let types = idxs + timecnt;
    let ttype = |i: usize| -> Option<i64> { be32(types + 6 * i) };
    let mut idx = None;
    for i in 0..timecnt {
        let t = if tsize == 8 { be64(trans + 8 * i)? } else { be32(trans + 4 * i)? };
        if t <= unix {
            idx = Some(*data.get(idxs + i)? as usize);
        } else {
            break;
        }
    }
    let i = match idx {
        Some(i) => i,
        None => (0..typecnt).find(|&k| data.get(types + 6 * k + 4) == Some(&0)).unwrap_or(0),
    };
    ttype(i)
}

/// Clamps nanoseconds to an i64 (Go `time.Time.Sub` and `UnixNano` saturate or wrap; the ranges here saturate).
fn sat(x: i128) -> i64 {
    x.clamp(i64::MIN as i128, i64::MAX as i128) as i64
}

/// Go `time.Duration.String()` for nanoseconds.
fn go_duration_string(d: i64) -> String {
    let mut buf: Vec<u8> = Vec::with_capacity(32);
    let neg = d < 0;
    let mut u = d.unsigned_abs();
    // Built in reverse.
    let fmt_frac = |buf: &mut Vec<u8>, mut v: u64, prec: usize| -> u64 {
        let mut print = false;
        for _ in 0..prec {
            let digit = v % 10;
            print = print || digit != 0;
            if print {
                buf.push(b'0' + digit as u8);
            }
            v /= 10;
        }
        if print {
            buf.push(b'.');
        }
        v
    };
    let fmt_int = |buf: &mut Vec<u8>, mut v: u64| {
        if v == 0 {
            buf.push(b'0');
        } else {
            while v > 0 {
                buf.push(b'0' + (v % 10) as u8);
                v /= 10;
            }
        }
    };
    if u < NS_PER_SEC as u64 {
        buf.push(b's');
        let prec;
        if u == 0 {
            return "0s".to_string();
        } else if u < 1_000 {
            prec = 0;
            buf.push(b'n');
        } else if u < 1_000_000 {
            prec = 3;
            // "µ" (U+00B5) reversed.
            buf.push(0xb5);
            buf.push(0xc2);
        } else {
            prec = 6;
            buf.push(b'm');
        }
        u = fmt_frac(&mut buf, u, prec);
        fmt_int(&mut buf, u);
    } else {
        buf.push(b's');
        u = fmt_frac(&mut buf, u, 9);
        fmt_int(&mut buf, u % 60);
        u /= 60;
        if u > 0 {
            buf.push(b'm');
            fmt_int(&mut buf, u % 60);
            u /= 60;
            if u > 0 {
                buf.push(b'h');
                fmt_int(&mut buf, u);
            }
        }
    }
    if neg {
        buf.push(b'-');
    }
    buf.reverse();
    String::from_utf8(buf).unwrap_or_default()
}

/// Go `time.Duration.Truncate(m)`.
fn go_truncate(d: i64, m: i64) -> i64 {
    if m <= 0 {
        return d;
    }
    d - d % m
}

/// Go `strconv.FormatFloat(f, 'E', -1, 64)`.
fn format_float_e(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "+Inf".to_string() } else { "-Inf".to_string() };
    }
    let s = format!("{f:e}");
    let (mant, exp) = s.split_once('e').unwrap_or((&s, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let sign = if exp < 0 { '-' } else { '+' };
    format!("{mant}E{sign}{:02}", exp.abs())
}

/// Go `math.Nextafter(x, y)`.
fn next_after(x: f64, y: f64) -> f64 {
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == y {
        return x;
    }
    if x == 0.0 {
        return f64::from_bits(1).copysign(y);
    }
    if (y > x) == (x > 0.0) {
        f64::from_bits(x.to_bits() + 1)
    } else {
        f64::from_bits(x.to_bits() - 1)
    }
}

/// Go `filepath.Dir`.
fn go_dir(p: &str) -> &str {
    let d = Path::new(p).parent().map(|d| d.to_str().unwrap_or("")).unwrap_or("");
    if d.is_empty() {
        if p.starts_with('/') {
            "/"
        } else {
            "."
        }
    } else {
        d
    }
}

/// Go `filepath.Join(a, b)` (joins and cleans).
fn go_join(a: &str, b: &str) -> String {
    let j = if a.is_empty() { b.to_string() } else { format!("{a}/{b}") };
    go_clean(&j)
}

/// Go `filepath.Clean`.
fn go_clean(p: &str) -> String {
    if p.is_empty() {
        return ".".to_string();
    }
    let rooted = p.starts_with('/');
    let mut out: Vec<&str> = Vec::new();
    for part in p.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if matches!(out.last(), Some(l) if *l != "..") {
                    out.pop();
                } else if !rooted {
                    out.push("..");
                }
            }
            x => out.push(x),
        }
    }
    let s = out.join("/");
    if rooted {
        format!("/{s}")
    } else if s.is_empty() {
        ".".to_string()
    } else {
        s
    }
}

/// The text of a Go `*fs.PathError` for an I/O error (`open <path>: no such file or directory`).
fn go_io_error(op: &str, path: &str, e: &std::io::Error) -> String {
    let msg = match e.kind() {
        std::io::ErrorKind::NotFound => "no such file or directory".to_string(),
        std::io::ErrorKind::PermissionDenied => "permission denied".to_string(),
        std::io::ErrorKind::IsADirectory => "is a directory".to_string(),
        _ => {
            let s = e.to_string();
            // Rust appends " (os error N)"; Go prints the bare strerror text, lower-cased first letter.
            let s = s.split(" (os error").next().unwrap_or(&s).to_string();
            let mut c = s.chars();
            match c.next() {
                Some(f) => f.to_lowercase().collect::<String>() + c.as_str(),
                None => s,
            }
        }
    };
    format!("{op} {path}: {msg}")
}

// ---------------------------------------------------------------------------------------------------------------

/// Port of util/junitxml.
pub mod junitxml {
    use std::io::Write;

    /// Go `JUnitXML`.
    #[derive(Default, Debug)]
    pub struct JUnitXml {
        pub suites: Vec<TestSuite>,
    }

    /// Go `TestSuite`.
    #[derive(Default, Debug)]
    pub struct TestSuite {
        pub name: String,
        pub test_count: i64,
        pub failure_count: i64,
        pub error_count: i64,
        pub skipped_count: i64,
        pub timestamp: String,
        pub cases: Vec<TestCase>,
    }

    /// Go `TestCase`.
    #[derive(Default, Debug)]
    pub struct TestCase {
        pub name: String,
        pub failures: Vec<String>,
        pub error: String,
    }

    impl JUnitXml {
        /// Go `WriteXML`: the document as `encoding/xml` encodes it (no header, no indentation).
        pub fn write_xml(&self, h: &mut dyn Write) -> std::io::Result<()> {
            let mut s = String::from("<testsuites>");
            for ts in &self.suites {
                s.push_str(&format!(
                    "<testsuite name=\"{}\" tests=\"{}\" failures=\"{}\" errors=\"{}\" skipped=\"{}\" timestamp=\"{}\">",
                    escape(&ts.name),
                    ts.test_count,
                    ts.failure_count,
                    ts.error_count,
                    ts.skipped_count,
                    escape(&ts.timestamp)
                ));
                for c in &ts.cases {
                    s.push_str(&format!("<testcase name=\"{}\">", escape(&c.name)));
                    for f in &c.failures {
                        s.push_str(&format!("<failure>{}</failure>", escape(f)));
                    }
                    if !c.error.is_empty() {
                        s.push_str(&format!("<error>{}</error>", escape(&c.error)));
                    }
                    s.push_str("</testcase>");
                }
                s.push_str("</testsuite>");
            }
            s.push_str("</testsuites>");
            h.write_all(s.as_bytes())?;
            h.flush()
        }

        /// Go `Suite`: appends a new suite and returns it.
        pub fn suite(&mut self, name: &str) -> &mut TestSuite {
            self.suites.push(TestSuite { name: name.to_string(), ..Default::default() });
            self.suites.last_mut().unwrap()
        }

        /// Parses a document written by [`JUnitXml::write_xml`] (Go `xml.Unmarshal` into `JUnitXML`, for tests).
        pub fn parse(text: &str) -> Result<JUnitXml, String> {
            let mut j = JUnitXml::default();
            let mut rest = text.trim();
            rest = rest.strip_prefix("<testsuites>").ok_or("missing <testsuites>")?;
            loop {
                if let Some(r) = rest.strip_prefix("</testsuites>") {
                    if !r.trim().is_empty() {
                        return Err("trailing data".into());
                    }
                    return Ok(j);
                }
                let r = rest.strip_prefix("<testsuite ").ok_or("expected <testsuite>")?;
                let end = r.find('>').ok_or("unterminated <testsuite>")?;
                let attrs = &r[..end];
                let mut ts = TestSuite {
                    name: attr(attrs, "name")?,
                    test_count: attr(attrs, "tests")?.parse().map_err(|_| "bad tests")?,
                    failure_count: attr(attrs, "failures")?.parse().map_err(|_| "bad failures")?,
                    error_count: attr(attrs, "errors")?.parse().map_err(|_| "bad errors")?,
                    skipped_count: attr(attrs, "skipped")?.parse().map_err(|_| "bad skipped")?,
                    timestamp: attr(attrs, "timestamp")?,
                    cases: Vec::new(),
                };
                rest = &r[end + 1..];
                while let Some(r) = rest.strip_prefix("<testcase ") {
                    let end = r.find('>').ok_or("unterminated <testcase>")?;
                    let mut c = TestCase { name: attr(&r[..end], "name")?, ..Default::default() };
                    rest = &r[end + 1..];
                    loop {
                        if let Some(r) = rest.strip_prefix("<failure>") {
                            let e = r.find("</failure>").ok_or("unterminated <failure>")?;
                            c.failures.push(unescape(&r[..e]));
                            rest = &r[e + "</failure>".len()..];
                        } else if let Some(r) = rest.strip_prefix("<error>") {
                            let e = r.find("</error>").ok_or("unterminated <error>")?;
                            c.error = unescape(&r[..e]);
                            rest = &r[e + "</error>".len()..];
                        } else {
                            break;
                        }
                    }
                    rest = rest.strip_prefix("</testcase>").ok_or("expected </testcase>")?;
                    ts.cases.push(c);
                }
                rest = rest.strip_prefix("</testsuite>").ok_or("expected </testsuite>")?;
                j.suites.push(ts);
            }
        }
    }

    fn attr(attrs: &str, name: &str) -> Result<String, String> {
        let key = format!("{name}=\"");
        let i = attrs.find(&key).ok_or(format!("missing attribute {name}"))? + key.len();
        let e = attrs[i..].find('"').ok_or("unterminated attribute")?;
        Ok(unescape(&attrs[i..i + e]))
    }

    fn unescape(s: &str) -> String {
        s.replace("&#34;", "\"")
            .replace("&#39;", "'")
            .replace("&lt;", "<")
            .replace("&gt;", ">")
            .replace("&#x9;", "\t")
            .replace("&#xA;", "\n")
            .replace("&#xD;", "\r")
            .replace("&amp;", "&")
    }

    /// Go `xml` `EscapeString`.
    fn escape(s: &str) -> String {
        let mut o = String::with_capacity(s.len());
        for c in s.chars() {
            match c {
                '"' => o.push_str("&#34;"),
                '\'' => o.push_str("&#39;"),
                '&' => o.push_str("&amp;"),
                '<' => o.push_str("&lt;"),
                '>' => o.push_str("&gt;"),
                '\t' => o.push_str("&#x9;"),
                '\n' => o.push_str("&#xA;"),
                '\r' => o.push_str("&#xD;"),
                c if !in_character_range(c) => o.push('\u{FFFD}'),
                c => o.push(c),
            }
        }
        o
    }

    fn in_character_range(r: char) -> bool {
        let r = r as u32;
        r == 0x09 || r == 0x0A || r == 0x0D || (0x20..=0xD7FF).contains(&r) || (0xE000..=0xFFFD).contains(&r) || (0x10000..=0x10FFFF).contains(&r)
    }

    impl TestSuite {
        /// Go `Fail`.
        pub fn fail(&mut self, f: &str) {
            self.failure_count += 1;
            self.last_case().failures.push(f.to_string());
        }

        fn last_case(&mut self) -> &mut TestCase {
            if self.cases.is_empty() {
                self.case("unknown");
            }
            self.cases.last_mut().unwrap()
        }

        /// Go `Case`: appends a new test case and returns the suite.
        pub fn case(&mut self, name: &str) -> &mut TestSuite {
            self.cases.push(TestCase { name: name.to_string(), ..Default::default() });
            self.test_count += 1;
            self
        }

        /// Go `Settime`.
        pub fn settime(&mut self, name: &str) {
            self.timestamp = name.to_string();
        }

        /// Go `Abort`.
        pub fn abort(&mut self, e: &dyn std::fmt::Display) {
            self.error_count += 1;
            self.last_case().error = e.to_string();
        }
    }
}

/// The parts of github.com/nsf/jsondiff that `--diff` uses: `Compare` with `DefaultConsoleOptions`, on decoded
/// JSON values.
pub mod jsondiff {
    use std::collections::BTreeMap;

    /// A decoded JSON value (`encoding/json` with `UseNumber`).
    #[derive(Clone, Debug, PartialEq)]
    pub enum Value {
        Null,
        Bool(bool),
        Num(String),
        Str(String),
        Arr(Vec<Value>),
        Obj(BTreeMap<String, Value>),
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Difference {
        FullMatch,
        SupersetMatch,
        NoMatch,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Tag {
        Normal,
        Added,
        Removed,
        Changed,
        Skipped,
    }

    impl Tag {
        fn begin(self) -> &'static str {
            match self {
                Tag::Normal => "",
                Tag::Added => "\x1b[0;32m",
                Tag::Removed => "\x1b[0;31m",
                Tag::Changed => "\x1b[0;33m",
                Tag::Skipped => "\x1b[0;90m",
            }
        }
        fn end(self) -> &'static str {
            match self {
                Tag::Normal => "",
                _ => "\x1b[0m",
            }
        }
    }

    const INDENT: &str = "    ";
    const CHANGED_SEPARATOR: &str = " => ";

    fn skipped_array_element(n: usize) -> String {
        if n == 1 {
            "...skipped 1 array element...".to_string()
        } else {
            format!("...skipped {n} array elements...")
        }
    }

    fn skipped_object_property(n: usize) -> String {
        if n == 1 {
            "...skipped 1 object property...".to_string()
        } else {
            format!("...skipped {n} object properties...")
        }
    }

    struct Ctx {
        level: usize,
        last_tag: Option<Tag>,
        diff: Difference,
    }

    fn quote(s: &str) -> String {
        model::unicode::quote(s)
    }

    impl Ctx {
        fn terminate_tag(&mut self, buf: &mut String) {
            if let Some(t) = self.last_tag.take() {
                buf.push_str(t.end());
            }
        }

        fn newline(&mut self, buf: &mut String, s: &str) {
            buf.push_str(s);
            if let Some(t) = self.last_tag {
                buf.push_str(t.end());
            }
            buf.push('\n');
            for _ in 0..self.level {
                buf.push_str(INDENT);
            }
            if let Some(t) = self.last_tag {
                buf.push_str(t.begin());
            }
        }

        fn key(&mut self, buf: &mut String, k: &str) {
            buf.push_str(&quote(k));
            buf.push_str(": ");
        }

        fn write_value(&mut self, buf: &mut String, v: &Value, full: bool) {
            match v {
                Value::Bool(b) => buf.push_str(if *b { "true" } else { "false" }),
                Value::Num(n) => buf.push_str(n),
                Value::Str(s) => buf.push_str(&quote(s)),
                Value::Arr(vv) => {
                    if full {
                        if vv.is_empty() {
                            buf.push('[');
                        } else {
                            self.level += 1;
                            self.newline(buf, "[");
                        }
                        for (i, v) in vv.iter().enumerate() {
                            self.write_value(buf, v, true);
                            if i != vv.len() - 1 {
                                self.newline(buf, ",");
                            } else {
                                self.level -= 1;
                                self.newline(buf, "");
                            }
                        }
                        buf.push(']');
                    } else {
                        buf.push_str("[]");
                    }
                }
                Value::Obj(vv) => {
                    if full {
                        if vv.is_empty() {
                            buf.push('{');
                        } else {
                            self.level += 1;
                            self.newline(buf, "{");
                        }
                        for (i, (k, v)) in vv.iter().enumerate() {
                            self.key(buf, k);
                            self.write_value(buf, v, true);
                            if i != vv.len() - 1 {
                                self.newline(buf, ",");
                            } else {
                                self.level -= 1;
                                self.newline(buf, "");
                            }
                        }
                        buf.push('}');
                    } else {
                        buf.push_str("{}");
                    }
                }
                Value::Null => buf.push_str("null"),
            }
        }

        fn write_mismatch(&mut self, buf: &mut String, a: &Value, b: &Value) {
            self.write_value(buf, a, false);
            buf.push_str(CHANGED_SEPARATOR);
            self.write_value(buf, b, false);
        }

        fn tag(&mut self, buf: &mut String, tag: Tag) {
            if self.last_tag == Some(tag) {
                return;
            } else if let Some(t) = self.last_tag {
                buf.push_str(t.end());
            }
            buf.push_str(tag.begin());
            self.last_tag = Some(tag);
        }

        fn result(&mut self, d: Difference) {
            if d == Difference::NoMatch {
                self.diff = Difference::NoMatch;
            } else if d == Difference::SupersetMatch && self.diff != Difference::NoMatch {
                self.diff = Difference::SupersetMatch;
            } else if self.diff != Difference::NoMatch && self.diff != Difference::SupersetMatch {
                self.diff = Difference::FullMatch;
            }
        }

        fn print_mismatch(&mut self, buf: &mut String, a: &Value, b: &Value) {
            self.tag(buf, Tag::Changed);
            self.write_mismatch(buf, a, b);
        }

        fn print_skipped(&mut self, buf: &mut String, n: &mut usize, strfunc: fn(usize) -> String, last: bool) {
            if *n == 0 {
                return;
            }
            self.tag(buf, Tag::Skipped);
            buf.push_str(&strfunc(*n));
            if !last {
                self.tag(buf, Tag::Normal);
                self.newline(buf, ",");
            }
            *n = 0;
        }

        fn finalize(&mut self, mut buf: String) -> String {
            self.terminate_tag(&mut buf);
            buf
        }

        /// The items of a collection pair: (key, a, b).
        fn collect_diffs(&mut self, items: &[(Option<String>, Option<&Value>, Option<&Value>)]) -> (Vec<String>, isize) {
            self.level += 1;
            let mut last = -1isize;
            let mut diffs = Vec::with_capacity(items.len());
            for (i, (_, a, b)) in items.iter().enumerate() {
                let mut diff = String::new();
                if let (Some(a), Some(b)) = (a, b) {
                    diff = self.print_diff(a, b);
                }
                if !diff.is_empty() || a.is_some() != b.is_some() {
                    last = i as isize;
                }
                diffs.push(diff);
            }
            self.level -= 1;
            (diffs, last)
        }

        fn print_collection_diff(
            &mut self,
            open: &str,
            close: &str,
            skipped: fn(usize) -> String,
            items: Vec<(Option<String>, Option<&Value>, Option<&Value>)>,
        ) -> String {
            let mut buf = String::new();
            let (diffs, _last_diff) = self.collect_diffs(&items);

            // some diffs or empty collection
            self.tag(&mut buf, Tag::Normal);
            if items.is_empty() {
                buf.push_str(open);
                buf.push_str(close);
                return self.finalize(buf);
            } else {
                self.level += 1;
                self.newline(&mut buf, open);
            }

            let mut no_diff_span = 0usize;
            let count = items.len();
            for (i, (key, va, vb)) in items.iter().enumerate() {
                let write_key = |ctx: &mut Ctx, buf: &mut String| {
                    if let Some(k) = key {
                        ctx.key(buf, k);
                    }
                };
                match (va, vb) {
                    (Some(_), Some(_)) => {
                        let diff = &diffs[i];
                        if !diff.is_empty() {
                            self.print_skipped(&mut buf, &mut no_diff_span, skipped, false);
                            write_key(self, &mut buf);
                            buf.push_str(diff);
                        }
                    }
                    (Some(va), None) => {
                        self.print_skipped(&mut buf, &mut no_diff_span, skipped, false);
                        self.tag(&mut buf, Tag::Removed);
                        write_key(self, &mut buf);
                        self.write_value(&mut buf, va, true);
                        self.result(Difference::SupersetMatch);
                    }
                    (None, Some(vb)) => {
                        self.print_skipped(&mut buf, &mut no_diff_span, skipped, false);
                        self.tag(&mut buf, Tag::Added);
                        write_key(self, &mut buf);
                        self.write_value(&mut buf, vb, true);
                        self.result(Difference::NoMatch);
                    }
                    (None, None) => {}
                }

                // SkipMatches is false in the console options: every item is written.
                let will_write_more_items = i < count - 1;
                if will_write_more_items {
                    self.tag(&mut buf, Tag::Normal);
                    self.newline(&mut buf, ",");
                }
                if i == count - 1 {
                    // we're done
                    self.print_skipped(&mut buf, &mut no_diff_span, skipped, true);
                    self.level -= 1;
                    self.tag(&mut buf, Tag::Normal);
                    self.newline(&mut buf, "");
                    break;
                }
            }

            buf.push_str(close);
            self.finalize(buf)
        }

        fn print_diff(&mut self, a: &Value, b: &Value) -> String {
            let mut buf = String::new();

            if matches!(a, Value::Null) || matches!(b, Value::Null) {
                if matches!(a, Value::Null) && matches!(b, Value::Null) {
                    self.tag(&mut buf, Tag::Normal);
                    self.write_value(&mut buf, a, false);
                    self.result(Difference::FullMatch);
                } else {
                    self.print_mismatch(&mut buf, a, b);
                    self.result(Difference::NoMatch);
                }
                return self.finalize(buf);
            }

            // Go compares the reflect kinds: json.Number and string are both strings.
            let kind = |v: &Value| match v {
                Value::Bool(_) => 1,
                Value::Num(_) | Value::Str(_) => 2,
                Value::Arr(_) => 3,
                Value::Obj(_) => 4,
                Value::Null => 0,
            };
            if kind(a) != kind(b) {
                self.print_mismatch(&mut buf, a, b);
                self.result(Difference::NoMatch);
                return self.finalize(buf);
            }

            match (a, b) {
                (Value::Bool(x), Value::Bool(y)) if x != y => {
                    self.print_mismatch(&mut buf, a, b);
                    self.result(Difference::NoMatch);
                    return self.finalize(buf);
                }
                (Value::Num(x), _) => {
                    if !matches!(b, Value::Num(y) if y == x) {
                        self.print_mismatch(&mut buf, a, b);
                        self.result(Difference::NoMatch);
                        return self.finalize(buf);
                    }
                }
                (Value::Str(x), _) => {
                    if !matches!(b, Value::Str(y) if y == x) {
                        self.print_mismatch(&mut buf, a, b);
                        self.result(Difference::NoMatch);
                        return self.finalize(buf);
                    }
                }
                (Value::Arr(sa), Value::Arr(sb)) => {
                    let max = sa.len().max(sb.len());
                    let items = (0..max).map(|i| (None, sa.get(i), sb.get(i))).collect();
                    return self.print_collection_diff("[", "]", skipped_array_element, items);
                }
                (Value::Obj(ma), Value::Obj(mb)) => {
                    let mut keys: Vec<&String> = ma.keys().chain(mb.keys()).collect();
                    keys.sort();
                    keys.dedup();
                    let items = keys.into_iter().map(|k| (Some(k.clone()), ma.get(k), mb.get(k))).collect();
                    return self.print_collection_diff("{", "}", skipped_object_property, items);
                }
                _ => {}
            }
            self.tag(&mut buf, Tag::Normal);
            self.write_value(&mut buf, a, true);
            self.result(Difference::FullMatch);
            self.finalize(buf)
        }
    }

    /// Go `jsondiff.Compare(a, b, &DefaultConsoleOptions())` on decoded values.
    pub fn compare(a: &Value, b: &Value) -> (Difference, String) {
        let mut ctx = Ctx { level: 0, last_tag: None, diff: Difference::FullMatch };
        let s = ctx.print_diff(a, b);
        (ctx.diff, s)
    }
}
