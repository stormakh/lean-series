//! Port of `promql/promqltest`: the PromQL test scripting language (`load`, `eval`, `expect`, `clear`), the test
//! engine options, the lazy loader for rule tests, and the test data migration tool.
//!
//! Errors are strings with Go's texts (the Go code compares `err.Error()`).
//!
//! Rust notes:
//! - Go runs each `eval` command as a subtest (`t.Run`) and goes on after a failure; [`run_test_with_storage`]
//!   reports each eval result to a callback instead, and catches panics per eval. A failed `load` or `clear`
//!   stops the script, as in Go.
//! - Go iterates the series of a `load` command in map order (random); here they are appended in file order.
//! - The storage is [`teststorage::TestStorage`] (an in-memory port of the TSDB head) until the `tsdb` crate exists.

pub mod convertnhcb;
pub mod migrate;
pub mod teststorage;

use std::collections::HashMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use model::histogram::{FloatHistogram, Span, custom_bucket_bounds_match};
use model::labels::{self, Labels};
use model::regexp::Regexp;
use model::value::is_stale_nan;
use storage::annotations::{PROMQL_INFO, PROMQL_WARNING};
use storage::{
    Annotations, AppendV2Options, AppenderTransaction, AppenderV2, Context, Error as StorageError,
    Queryable, Storage,
};

use crate::engine::{Engine, EngineOpts};
use crate::parser::duration::parse_duration;
use crate::parser::quote::{go_quote, parse_float, parse_int_base0, unquote};
use crate::parser::{self, Expr, ParseErr, Parser, PositionRange, SequenceValue};
use crate::value::{FPoint, HPoint, Matrix, Sample, Scalar, Series, Value, Vector};

pub use teststorage::TestStorage;

/// Relative error allowed for sample values (Go `defaultEpsilon`).
pub const DEFAULT_EPSILON: f64 = 0.000001;
/// Go `DefaultMaxSamplesPerQuery`.
pub const DEFAULT_MAX_SAMPLES_PER_QUERY: usize = 10000;
const RANGE_VECTOR_PREFIX: &str = "expect range vector";
const EXPECT_STRING_PREFIX: &str = "expect string";
/// Go's zero `time.Time` (0001-01-01 UTC) in milliseconds since the epoch.
const GO_ZERO_TIME_MS: i64 = -62_135_596_800_000;
/// Go `testStartTime` (the Unix epoch) in milliseconds.
const TEST_START_TIME: i64 = 0;
const MS: i64 = 1_000_000;

fn re(s: &str) -> Regexp {
    Regexp::must_compile(s)
}

struct Patterns {
    load: Regexp,
    eval_instant: Regexp,
    eval_range: Regexp,
    expect: Regexp,
    match_any: Regexp,
    expect_range: Regexp,
    query_context_funcs: Regexp,
}

fn patterns() -> &'static Patterns {
    static P: OnceLock<Patterns> = OnceLock::new();
    P.get_or_init(|| Patterns {
        load: re(r"^load(?:_(with_nhcb))?\s+(.+?)$"),
        eval_instant: re(
            r"^eval(?:_(fail|warn|ordered|info))?\s+instant\s+(?:at\s+(.+?))?\s+(.+)$",
        ),
        eval_range: re(
            r"^eval(?:_(fail|warn|info))?\s+range\s+from\s+(.+)\s+to\s+(.+)\s+step\s+(.+?)\s+(.+)$",
        ),
        expect: re(r"^expect\s+(ordered|fail|warn|no_warn|info|no_info)(?:\s+(regex|msg):(.+))?$"),
        match_any: re(r"^.*$"),
        expect_range: re(&format!(
            r"^{}\s+from\s+(.+)\s+to\s+(.+)\s+step\s+(.+)$",
            RANGE_VECTOR_PREFIX
        )),
        query_context_funcs: re(r"(start|end|range|step)\(\)"),
    })
}

/// Go `FindStringSubmatch`: the submatches ("" for groups that did not match), or `None`.
fn submatches<'a>(re: &Regexp, s: &'a str) -> Option<Vec<&'a str>> {
    let idx = re.find_string_submatch_index(s)?;
    Some(
        idx.chunks(2)
            .map(|p| {
                if p[0] < 0 {
                    ""
                } else {
                    &s[p[0] as usize..p[1] as usize]
                }
            })
            .collect(),
    )
}

/// Go `TestParserOpts`: the parser options of all built-in test engines.
pub fn test_parser_opts() -> parser::Options {
    parser::Options {
        enable_experimental_functions: true,
        enable_binop_fill_modifiers: true,
        enable_unquoted_utf8_names: true,
        ..Default::default()
    }
}

/// Go `parserForBuiltinTests`.
fn test_parser() -> &'static Parser {
    static P: OnceLock<Parser> = OnceLock::new();
    P.get_or_init(|| Parser::new(test_parser_opts()))
}

/// Go `NewTestEngine`.
pub fn new_test_engine(
    enable_per_step_stats: bool,
    lookback_delta: Duration,
    max_samples: usize,
) -> Engine {
    new_test_engine_with_opts(EngineOpts {
        max_samples,
        timeout: Duration::from_secs(100),
        no_step_subquery_interval_fn: Some(Arc::new(|_| 60_000)),
        enable_at_modifier: true,
        enable_negative_offset: true,
        enable_per_step_stats,
        lookback_delta,
        enable_delayed_name_removal: true,
        use_start_timestamps: true,
        parser: Some(Parser::new(test_parser_opts())),
        ..Default::default()
    })
}

/// Go `NewTestEngineWithOpts`.
pub fn new_test_engine_with_opts(opts: EngineOpts) -> Engine {
    Engine::new(opts)
}

/// Go `NewBuiltinTestStorage`.
pub fn new_builtin_test_storage() -> TestStorage {
    TestStorage::new_builtin()
}

/// A function that opens a new, empty storage.
pub type NewStorageFn<'a> = &'a dyn Fn() -> Arc<dyn Storage>;

/// Go `newTestStorage` (as the promqltest package's own tests configure it: ST storage and XOR2 floats).
pub fn new_test_storage() -> Arc<dyn Storage> {
    Arc::new(TestStorage::new_builtin())
}

/// Go `LoadedStorage`: a storage with the data of the `load` commands of `input`.
pub fn loaded_storage(input: &str) -> Result<TestStorage, String> {
    loaded_storage_with_options(input, teststorage::Options::default())
}

/// Go `LoadedStorage(t, input, opts...)` with storage options.
pub fn loaded_storage_with_options(input: &str, opts: teststorage::Options) -> Result<TestStorage, String> {
    let st = TestStorage::with_options(opts);
    let st2 = st.clone();
    let open = move || -> Arc<dyn Storage> { Arc::new(st2.clone()) };
    let mut test = Test::new(input, false, &open)?;
    for cmd in std::mem::take(&mut test.cmds) {
        match cmd {
            TestCommand::Load(l) => test.exec_load(&l)?,
            _ => {
                return Err(format!(
                    "only 'load' commands accepted, got '{}'",
                    cmd.name()
                ));
            }
        }
    }
    Ok(st)
}

/// The test data directory of upstream (`promql/promqltest/testdata`).
pub fn builtin_test_dir() -> String {
    format!(
        "{}/../../upstream/promql/promqltest/testdata",
        env!("CARGO_MANIFEST_DIR")
    )
}

/// The built-in test files (Go `testsFs`, `*/*.test`): (file name, content), sorted by name.
pub fn builtin_test_files() -> Result<Vec<(String, String)>, String> {
    let dir = builtin_test_dir();
    let mut out = Vec::new();
    for e in std::fs::read_dir(&dir).map_err(|e| format!("read {}: {}", dir, e))? {
        let e = e.map_err(|e| e.to_string())?;
        let name = e.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".test") {
            continue;
        }
        let content = std::fs::read_to_string(e.path()).map_err(|e| e.to_string())?;
        out.push((name, content));
    }
    out.sort();
    Ok(out)
}

/// Go `GetBuiltInExprs`: the expressions of all eval commands of the built-in test files.
pub fn get_builtin_exprs() -> Result<Vec<String>, String> {
    let mut exprs = Vec::new();
    for (_, content) in builtin_test_files()? {
        let mut t = Test {
            testing_mode: false,
            cmds: Vec::new(),
            storage: None,
        };
        t.parse(&content)?;
        for c in &t.cmds {
            if let TestCommand::Eval(e) = c {
                exprs.push(e.expr.clone());
            }
        }
    }
    Ok(exprs)
}

/// The result of one eval command of a script.
pub struct EvalOutcome<'a> {
    pub cmd: &'a EvalCmd,
    pub result: Result<(), String>,
}

/// Go `RunBuiltinTests`: runs every built-in test file on the engine; `on_eval` gets the file name and each eval
/// result. Returns the errors that stopped a file (parse or load errors), keyed by file name.
pub fn run_builtin_tests(
    engine: &Engine,
    on_eval: &mut dyn FnMut(&str, EvalOutcome<'_>),
) -> Vec<(String, String)> {
    run_builtin_tests_with_storage(engine, &|| Arc::new(new_builtin_test_storage()), on_eval)
}

/// Go `RunBuiltinTestsWithStorage`.
pub fn run_builtin_tests_with_storage(
    engine: &Engine,
    new_storage: NewStorageFn<'_>,
    on_eval: &mut dyn FnMut(&str, EvalOutcome<'_>),
) -> Vec<(String, String)> {
    let mut errs = Vec::new();
    let files = match builtin_test_files() {
        Ok(f) => f,
        Err(e) => return vec![(String::new(), e)],
    };
    for (name, content) in files {
        if let Err(e) =
            run_test_with_storage(&content, engine, new_storage, &mut |o| on_eval(&name, o))
        {
            errs.push((name, e));
        }
    }
    errs
}

/// Go `RunTest`: runs the script and fails with all eval failures (one per line) or the error that stopped it.
pub fn run_test(input: &str, engine: &Engine) -> Result<(), String> {
    let mut fails = Vec::new();
    let r = run_test_with_storage(input, engine, &new_test_storage, &mut |o| {
        if let Err(e) = o.result {
            fails.push(format!("line {}/{}: {}", o.cmd.line, o.cmd.expr, e));
        }
    });
    if let Err(e) = r {
        fails.push(e);
    }
    if fails.is_empty() {
        Ok(())
    } else {
        Err(fails.join("\n"))
    }
}

/// Go `RunTestWithStorage` (non-testing mode): each eval is reported to `on_eval`, panics are caught per eval.
/// Returns the parse, load or clear error that stopped the script.
pub fn run_test_with_storage(
    input: &str,
    engine: &Engine,
    new_storage: NewStorageFn<'_>,
    on_eval: &mut dyn FnMut(EvalOutcome<'_>),
) -> Result<(), String> {
    run(input, engine, new_storage, false, on_eval)
}

/// Go `testTest`: runs the script in test-the-test mode and returns the first error.
pub fn test_test(input: &str, engine: &Engine) -> Result<(), String> {
    run(input, engine, &new_test_storage, true, &mut |_| {})
}

fn run(
    input: &str,
    engine: &Engine,
    new_storage: NewStorageFn<'_>,
    testing_mode: bool,
    on_eval: &mut dyn FnMut(EvalOutcome<'_>),
) -> Result<(), String> {
    let mut test = Test::new(input, testing_mode, new_storage)?;
    let cmds = std::mem::take(&mut test.cmds);
    for cmd in &cmds {
        match cmd {
            TestCommand::Clear => test.storage = Some(new_storage()),
            TestCommand::Load(l) => test.exec_load(l)?,
            TestCommand::Eval(e) => {
                if testing_mode {
                    test.exec_eval(e, engine)?;
                } else {
                    let result = match catch_unwind(AssertUnwindSafe(|| test.exec_eval(e, engine)))
                    {
                        Ok(r) => r,
                        Err(p) => Err(format!("panic: {}", panic_message(&*p))),
                    };
                    on_eval(EvalOutcome { cmd: e, result });
                }
            }
        }
    }
    Ok(())
}

/// The message of a panic payload.
pub fn panic_message(p: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = p.downcast_ref::<String>() {
        return s.clone();
    }
    if let Some(s) = p.downcast_ref::<&str>() {
        return s.to_string();
    }
    "unknown panic".to_string()
}

/// A sequence of test commands run against a test storage (Go `test`).
pub struct Test {
    /// Go `testingMode` (test-the-test mode): the first eval error stops the script.
    pub testing_mode: bool,
    cmds: Vec<TestCommand>,
    storage: Option<Arc<dyn Storage>>,
}

/// A test command (Go `testCommand`).
pub enum TestCommand {
    Clear,
    Load(LoadCmd),
    Eval(EvalCmd),
}

impl TestCommand {
    fn name(&self) -> &'static str {
        match self {
            TestCommand::Clear => "clear",
            TestCommand::Load(_) => "load",
            TestCommand::Eval(_) => "eval",
        }
    }
}

/// Go `raise`: a parse error with a line offset.
fn raise(line: usize, msg: String) -> String {
    ParseErr {
        position_range: PositionRange::default(),
        err: msg,
        query: String::new(),
        line_offset: line as i32,
    }
    .to_string()
}

impl Test {
    /// Go `newTest`: parses the script and opens the storage.
    pub fn new(
        input: &str,
        testing_mode: bool,
        new_storage: NewStorageFn<'_>,
    ) -> Result<Test, String> {
        let mut t = Test {
            testing_mode,
            cmds: Vec::new(),
            storage: None,
        };
        let r = t.parse(input);
        t.storage = Some(new_storage());
        r.map(|_| t)
    }

    /// The parsed commands.
    pub fn commands(&self) -> &[TestCommand] {
        &self.cmds
    }

    fn storage(&self) -> Arc<dyn Storage> {
        self.storage.clone().expect("storage is open")
    }

    /// Go `parse`.
    fn parse(&mut self, input: &str) -> Result<(), String> {
        let lines = get_lines(input);
        let mut i = 0;
        while i < lines.len() {
            let l = &lines[i];
            if l.is_empty() {
                i += 1;
                continue;
            }
            let first = l.split([' ', '\t']).next().unwrap_or("").to_lowercase();
            let cmd = if first == "clear" {
                TestCommand::Clear
            } else if first.starts_with("load") {
                let (ni, c) = parse_load(&lines, i, TEST_START_TIME)?;
                i = ni;
                TestCommand::Load(c)
            } else if first.starts_with("eval") {
                let (ni, c) = parse_eval(&lines, i)?;
                i = ni;
                TestCommand::Eval(c)
            } else {
                return Err(raise(i, format!("invalid command {}", go_quote(l))));
            };
            self.cmds.push(cmd);
            i += 1;
        }
        Ok(())
    }

    fn exec_load(&mut self, cmd: &LoadCmd) -> Result<(), String> {
        let st = self.storage();
        let mut app = st.appender_v2(&Context::background());
        if let Err(e) = cmd.append(&mut *app) {
            let _ = app.rollback();
            return Err(e);
        }
        if let Some(e) = app.commit() {
            return Err(e.to_string());
        }
        Ok(())
    }

    fn exec_eval(&self, cmd: &EvalCmd, engine: &Engine) -> Result<(), String> {
        if cmd.is_range {
            self.exec_range_eval(cmd, engine)
        } else {
            self.exec_instant_eval(cmd, engine)
        }
    }

    fn queryable(&self) -> Arc<dyn Queryable> {
        self.storage()
    }

    fn exec_range_eval(&self, cmd: &EvalCmd, engine: &Engine) -> Result<(), String> {
        let ctx = Context::background();
        let mut q = match engine.new_range_query(
            &ctx,
            self.queryable(),
            None,
            &cmd.expr,
            cmd.start,
            cmd.end,
            cmd.step,
        ) {
            Ok(q) => q,
            Err(err) => {
                if cmd.is_fail() && cmd.check_expected_failure(&err.to_string()).is_ok() {
                    return Ok(());
                }
                return Err(format!(
                    "error creating range query for {} (line {}): {}",
                    go_quote(&cmd.expr),
                    cmd.line,
                    err
                ));
            }
        };
        let res = q.exec(&ctx);
        q.close();
        if let Some(err) = res.err {
            if cmd.is_fail() {
                return cmd.check_expected_failure(&err.to_string());
            }
            return Err(format!(
                "error evaluating query {} (line {}): {}",
                go_quote(&cmd.expr),
                cmd.line,
                err
            ));
        }
        if cmd.is_fail() {
            return Err(format!(
                "expected error evaluating query {} (line {}) but got none",
                go_quote(&cmd.expr),
                cmd.line
            ));
        }
        cmd.check_annotations(&cmd.expr, &res.warnings)?;
        let value = res
            .value
            .ok_or_else(|| "query returned no value".to_string())?;
        cmd.compare_result(value)
            .map_err(|e| format!("error in eval {} (line {}): {}", cmd.expr, cmd.line, e))
    }

    fn exec_instant_eval(&self, cmd: &EvalCmd, engine: &Engine) -> Result<(), String> {
        let mut queries = vec![AtModifierTestCase {
            expr: cmd.expr.clone(),
            eval_time: cmd.eval,
        }];
        queries.extend(at_modifier_test_cases(&cmd.expr, cmd.eval)?);
        for iq in &queries {
            self.run_instant_query(iq, cmd, engine)?;
        }
        Ok(())
    }

    fn run_instant_query(
        &self,
        iq: &AtModifierTestCase,
        cmd: &EvalCmd,
        engine: &Engine,
    ) -> Result<(), String> {
        let ctx = Context::background();
        let mut q =
            match engine.new_instant_query(&ctx, self.queryable(), None, &iq.expr, iq.eval_time) {
                Ok(q) => q,
                Err(err) => {
                    if cmd.is_fail() && cmd.check_expected_failure(&err.to_string()).is_ok() {
                        return Ok(());
                    }
                    return Err(format!(
                        "error creating instant query for {} (line {}): {}",
                        go_quote(&cmd.expr),
                        cmd.line,
                        err
                    ));
                }
            };
        let res = q.exec(&ctx);
        q.close();
        if let Some(err) = res.err {
            if cmd.is_fail() {
                return cmd.check_expected_failure(&err.to_string());
            }
            return Err(format!(
                "error evaluating query {} (line {}): {}",
                go_quote(&iq.expr),
                cmd.line,
                err
            ));
        }
        if cmd.is_fail() {
            return Err(format!(
                "expected error evaluating query {} (line {}) but got none",
                go_quote(&iq.expr),
                cmd.line
            ));
        }
        cmd.check_annotations(&iq.expr, &res.warnings)?;
        let value = res
            .value
            .ok_or_else(|| "query returned no value".to_string())?;
        let is_scalar = matches!(value, Value::Scalar(_));
        cmd.compare_result(value)
            .map_err(|e| format!("error in eval {} (line {}): {}", iq.expr, cmd.line, e))?;

        if cmd.exclude_from_range_query {
            return Ok(());
        }
        if patterns().query_context_funcs.is_match(&iq.expr) {
            return Ok(());
        }
        let mut q = engine
            .new_range_query(
                &ctx,
                self.queryable(),
                None,
                &iq.expr,
                iq.eval_time - 60_000,
                iq.eval_time + 60_000,
                60_000,
            )
            .map_err(|err| {
                format!(
                    "error creating range query for {} (line {}): {}",
                    go_quote(&cmd.expr),
                    cmd.line,
                    err
                )
            })?;
        let range_res = q.exec(&ctx);
        q.close();
        if let Some(err) = range_res.err {
            return Err(format!(
                "error evaluating query {} (line {}) in range mode: {}",
                go_quote(&iq.expr),
                cmd.line,
                err
            ));
        }
        if cmd.is_ordered() {
            return Ok(());
        }
        let mat = match range_res.value {
            Some(Value::Matrix(m)) => m,
            other => panic!(
                "interface conversion: parser.Value is {}, not promql.Matrix",
                other
                    .map(|v| format!("{}", v.value_type()))
                    .unwrap_or_else(|| "nil".into())
            ),
        };
        assert_matrix_sorted(&mat)?;
        let mut vec: Vector = Vec::with_capacity(mat.len());
        for series in &mat {
            for p in &series.floats {
                if p.t == iq.eval_time {
                    vec.push(Sample {
                        metric: series.metric.clone(),
                        t: p.t,
                        f: p.f,
                        h: None,
                        drop_name: false,
                    });
                    break;
                }
            }
            for p in &series.histograms {
                if p.t == iq.eval_time {
                    vec.push(Sample {
                        metric: series.metric.clone(),
                        t: p.t,
                        f: 0.0,
                        h: Some(p.h.clone()),
                        drop_name: false,
                    });
                    break;
                }
            }
        }
        let r = if is_scalar {
            cmd.compare_result(Value::Scalar(Scalar { t: 0, v: vec[0].f }))
        } else {
            cmd.compare_result(Value::Vector(vec))
        };
        r.map_err(|e| {
            format!(
                "error in eval {} (line {}) range mode: {}",
                iq.expr, cmd.line, e
            )
        })
    }
}

/// Go `getLines`: trimmed lines with comments removed.
fn get_lines(input: &str) -> Vec<String> {
    input
        .split('\n')
        .map(|l| {
            let l = l.trim_matches(|c: char| c.is_whitespace());
            if l.starts_with('#') {
                String::new()
            } else {
                l.to_string()
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------------------------
// load.

/// A sample with an optional start timestamp (Go `sampleST`; `st` 0 means unknown).
#[derive(Clone, Debug, PartialEq)]
pub struct SampleSt {
    pub t: i64,
    pub f: f64,
    pub h: Option<FloatHistogram>,
    pub st: i64,
}

/// Loads sequences of sample values for metrics into the storage (Go `loadCmd`).
#[derive(Clone, Debug)]
pub struct LoadCmd {
    /// The step between samples in nanoseconds.
    pub gap: i64,
    /// The series in file order; a later definition of the same series replaces the samples.
    defs: Vec<(Labels, Vec<SampleSt>)>,
    pos: HashMap<u64, usize>,
    pub with_nhcb: bool,
    /// The time of the first sample in milliseconds.
    pub start_time: i64,
}

impl LoadCmd {
    fn new(gap: i64, with_nhcb: bool) -> LoadCmd {
        LoadCmd {
            gap,
            defs: Vec::new(),
            pos: HashMap::new(),
            with_nhcb,
            start_time: TEST_START_TIME,
        }
    }

    /// The samples defined for the series with these labels (Go `cmd.defs[m.Hash()]`).
    pub fn samples(&self, m: &Labels) -> Option<&[SampleSt]> {
        self.pos.get(&m.hash()).map(|&i| self.defs[i].1.as_slice())
    }

    /// Go `set`: stores the samples of a metric, with optional start timestamps (same length as `vals`).
    fn set(&mut self, m: Labels, vals: &[SequenceValue], st_vals: Option<&[SequenceValue]>) {
        let mut samples = Vec::with_capacity(vals.len());
        let mut ts_ns = self.start_time * MS;
        for (i, v) in vals.iter().enumerate() {
            let ts_ms = ts_ns / MS;
            if !v.omitted {
                let mut s = SampleSt {
                    t: ts_ms,
                    f: v.value,
                    h: v.histogram.as_deref().cloned(),
                    st: 0,
                };
                if let Some(st) = st_vals {
                    if !st.is_empty() && !st[i].omitted {
                        s.st = ts_ms + st[i].value as i64;
                    }
                }
                samples.push(s);
            }
            ts_ns += self.gap;
        }
        let h = m.hash();
        match self.pos.get(&h) {
            Some(&i) => self.defs[i] = (m, samples),
            None => {
                self.pos.insert(h, self.defs.len());
                self.defs.push((m, samples));
            }
        }
    }

    /// Go `append`: appends the defined series to the storage.
    pub fn append(&self, a: &mut dyn AppenderV2) -> Result<(), String> {
        for (m, smpls) in &self.defs {
            for s in smpls {
                append_sample(a, s, m)?;
            }
        }
        if self.with_nhcb {
            return self.append_custom_histogram(a);
        }
        Ok(())
    }

    /// Go `appendCustomHistogram`: converts the classic histograms to NHCB and appends them.
    fn append_custom_histogram(&self, a: &mut dyn AppenderV2) -> Result<(), String> {
        use convertnhcb::{SuffixType, TempHistogram};
        // Base metric hash -> (metric, histograms by timestamp), in first-seen order.
        let mut map: Vec<(Labels, Vec<(i64, TempHistogram)>)> = Vec::new();
        let mut map_pos: HashMap<u64, usize> = HashMap::new();
        for (m, smpls) in &self.defs {
            let m_name = m.get(labels::METRIC_NAME);
            let (suffix, name) = convertnhcb::get_histogram_metric_base_name(m_name);
            let le = match suffix {
                SuffixType::None => continue,
                SuffixType::Bucket => {
                    if !m.has(labels::BUCKET_LABEL) {
                        panic!("expected bucket label in metric {}", m);
                    }
                    match parse_float(m.get(labels::BUCKET_LABEL)) {
                        Ok(le) if !le.is_nan() => le,
                        _ => continue,
                    }
                }
                _ => 0.0,
            };
            let m2 = convertnhcb::get_histogram_metric_base(m, name);
            let h = m2.hash();
            let idx = *map_pos.entry(h).or_insert_with(|| {
                map.push((m2.clone(), Vec::new()));
                map.len() - 1
            });
            map[idx].0 = m2;
            let by_ts = &mut map[idx].1;
            for s in smpls {
                if s.h.is_some() {
                    continue;
                }
                let th = match by_ts.iter().position(|(t, _)| *t == s.t) {
                    Some(p) => &mut by_ts[p].1,
                    None => {
                        by_ts.push((s.t, TempHistogram::new()));
                        &mut by_ts.last_mut().unwrap().1
                    }
                };
                let _ = match suffix {
                    SuffixType::Bucket => th.set_bucket_count(le, s.f),
                    SuffixType::Count => th.set_count(s.f),
                    SuffixType::Sum => th.set_sum(s.f),
                    SuffixType::None => Ok(()),
                };
            }
        }
        for (metric, by_ts) in map {
            let mut samples: Vec<(i64, FloatHistogram)> = Vec::with_capacity(by_ts.len());
            for (t, th) in by_ts {
                let (h, fh) = th.convert().map_err(|e| e.to_string())?;
                let fh = match fh {
                    Some(fh) => fh,
                    None => {
                        let h = h.expect("integer histogram");
                        h.validate().map_err(|e| e.to_string())?;
                        h.to_float()
                    }
                };
                fh.validate().map_err(|e| e.to_string())?;
                samples.push((t, fh));
            }
            samples.sort_by_key(|s| s.0);
            for (t, fh) in samples {
                append_sample(
                    a,
                    &SampleSt {
                        t,
                        f: 0.0,
                        h: Some(fh),
                        st: 0,
                    },
                    &metric,
                )?;
            }
        }
        Ok(())
    }
}

/// Go `appendSample`.
fn append_sample(a: &mut dyn AppenderV2, s: &SampleSt, m: &Labels) -> Result<(), String> {
    let opts = AppendV2Options::default();
    let r = match &s.h {
        Some(h) => a.append(0, m, s.st, s.t, 0.0, None, Some(h), &opts),
        None => a.append(0, m, s.st, s.t, s.f, None, None, &opts),
    };
    r.map(|_| ()).map_err(|e| e.to_string())
}

/// Go `parseSeries`.
fn parse_series(def_line: &str, line: usize) -> Result<(Labels, Vec<SequenceValue>), String> {
    test_parser()
        .parse_series_desc(def_line)
        .map_err(|mut err| {
            parser::enrich_parse_error(&mut err, |e| e.line_offset = line as i32);
            err.to_string()
        })
}

/// Go `parseLoad`: parses a load command at line `i`; returns the index of its last line.
pub fn parse_load(
    lines: &[String],
    mut i: usize,
    start_time: i64,
) -> Result<(usize, LoadCmd), String> {
    let p = patterns();
    let Some(parts) = submatches(&p.load, &lines[i]) else {
        return Err(raise(
            i,
            "invalid load command. (load[_with_nhcb] <step:duration>)".into(),
        ));
    };
    let with_nhcb = parts[1] == "with_nhcb";
    let step = parts[2];
    let gap = parse_duration(step).map_err(|e| {
        raise(
            i,
            format!("invalid step definition {}: {}", go_quote(step), e),
        )
    })?;
    let mut cmd = LoadCmd::new(gap, with_nhcb);
    cmd.start_time = start_time;
    let mut pending: Option<(Labels, Vec<SequenceValue>, usize)> = None;
    while i + 1 < lines.len() {
        i += 1;
        let def_line = &lines[i];
        if def_line.is_empty() {
            i -= 1;
            break;
        }
        if is_st_line(def_line) {
            if let Some((_, _, pl)) = &pending {
                return Err(raise(*pl, "@st line has no following sample line".into()));
            }
            let (m, vals) = parse_st_line(def_line, i)?;
            pending = Some((m, vals, i));
            continue;
        }
        let (metric, vals) = parse_series(def_line, i)?;
        if let Some((pm, pv, pl)) = &pending {
            if !labels::equal(&metric, pm) {
                return Err(raise(
                    *pl,
                    "@st metric does not match the following sample line metric".into(),
                ));
            }
            if pv.len() != vals.len() {
                return Err(raise(
                    *pl,
                    format!(
                        "@st line has {} values but sample line has {}",
                        pv.len(),
                        vals.len()
                    ),
                ));
            }
        }
        let st = pending.take().map(|p| p.1);
        cmd.set(metric, &vals, st.as_deref());
    }
    if let Some((_, _, pl)) = pending {
        return Err(raise(pl, "@st line has no following sample line".into()));
    }
    Ok((i, cmd))
}

/// Go `isSTLine`.
fn is_st_line(def_line: &str) -> bool {
    let l = def_line.trim();
    match l.find([' ', '\t']) {
        Some(i) => l[..i].ends_with("@st"),
        None => false,
    }
}

/// Go `parseSTLine`.
fn parse_st_line(def_line: &str, line: usize) -> Result<(Labels, Vec<SequenceValue>), String> {
    let l = def_line.trim();
    let Some(space) = l.find([' ', '\t']) else {
        return Err(raise(
            line,
            "invalid @st line: missing value sequence".into(),
        ));
    };
    let metric_part = l[..space].strip_suffix("@st").unwrap_or(&l[..space]);
    let vals_part = l[space + 1..].trim();
    let (metric, _) = parse_series(&format!("{} _", metric_part), line).map_err(|e| {
        raise(
            line,
            format!("invalid @st line metric {}: {}", go_quote(metric_part), e),
        )
    })?;
    let vals = parse_st_sequence(vals_part)
        .map_err(|e| raise(line, format!("invalid @st sequence: {}", e)))?;
    Ok((metric, vals))
}

/// Go `parseSTSequence`: start-timestamp offsets in milliseconds.
pub fn parse_st_sequence(input: &str) -> Result<Vec<SequenceValue>, String> {
    let mut out = Vec::new();
    for item in input.split_whitespace() {
        let vals = parse_st_item(item)
            .map_err(|e| format!("invalid ST item {}: {}", go_quote(item), e))?;
        out.extend(vals);
    }
    Ok(out)
}

fn omitted() -> SequenceValue {
    SequenceValue {
        omitted: true,
        ..Default::default()
    }
}

fn value(v: f64) -> SequenceValue {
    SequenceValue {
        value: v,
        ..Default::default()
    }
}

/// Go `strconv.ParseUint(s, 10, 64)` (only success matters).
fn parse_uint10(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

fn parse_st_item(item: &str) -> Result<Vec<SequenceValue>, String> {
    if item == "_" {
        return Ok(vec![omitted()]);
    }
    if let Some(n) = item.strip_prefix("_x") {
        return match parse_uint10(n) {
            Some(n) if n > 0 => Ok((0..n).map(|_| omitted()).collect()),
            _ => Err("invalid repeat count".into()),
        };
    }
    let (base, rest) = parse_duration_prefix(item)?;
    if rest.is_empty() {
        return Ok(vec![value(base as f64)]);
    }
    if let Some(n) = rest.strip_prefix('x') {
        let n = parse_uint10(n).ok_or("invalid repeat count")?;
        return Ok((0..=n).map(|_| value(base as f64)).collect());
    }
    let negative = match rest.as_bytes()[0] {
        b'+' => false,
        b'-' => true,
        c => {
            return Err(format!(
                "unexpected character {} after duration",
                go_quote_byte(c)
            ));
        }
    };
    let (mut delta, rest2) =
        parse_duration_prefix(&rest[1..]).map_err(|e| format!("invalid step duration: {}", e))?;
    if negative {
        delta = -delta;
    }
    let Some(n) = rest2.strip_prefix('x') else {
        return Err("expected 'x<count>' after step duration".into());
    };
    let n = parse_uint10(n).ok_or_else(|| {
        format!(
            "invalid repeat count: strconv.ParseUint: parsing {}: invalid syntax",
            go_quote(n)
        )
    })?;
    let mut offset = base;
    let mut out = Vec::with_capacity(n as usize + 1);
    for _ in 0..=n {
        out.push(value(offset as f64));
        offset += delta;
    }
    Ok(out)
}

/// Go `%q` of a byte (a rune literal).
fn go_quote_byte(c: u8) -> String {
    crate::parser::quote::go_quote_rune(Some(c as char))
}

/// Go `parseDurationPrefix`: a duration with an optional sign at the start of `s`, in milliseconds, and the rest.
fn parse_duration_prefix(s: &str) -> Result<(i64, &str), String> {
    if s.is_empty() {
        return Err("empty duration".into());
    }
    let b = s.as_bytes();
    let mut i = 0;
    let mut negative = false;
    match b[0] {
        b'-' => {
            negative = true;
            i += 1;
        }
        b'+' => i += 1,
        _ => {}
    }
    let start = i;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    if i == start {
        return Err(format!("expected digits in duration {}", go_quote(s)));
    }
    while i < b.len() && b[i].is_ascii_alphabetic() && b[i] != b'x' {
        i += 1;
    }
    let d = parse_duration(&s[start..i])
        .map_err(|e| format!("invalid duration {}: {}", go_quote(&s[start..i]), e))?;
    let mut ms = d / MS;
    if negative {
        ms = -ms;
    }
    Ok((ms, &s[i..]))
}

// ---------------------------------------------------------------------------------------------------------------
// eval.

/// The kinds of `expect` lines (Go `expectCmdType`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ExpectCmdType {
    Ordered,
    Fail,
    Warn,
    NoWarn,
    Info,
    NoInfo,
}

/// An `expect` line: a message or a pattern (Go `expectCmd`).
#[derive(Clone, Debug)]
pub struct ExpectCmd {
    pub message: String,
    pub regex: Option<Regexp>,
}

impl ExpectCmd {
    /// Go `CheckMatch`.
    pub fn check_match(&self, s: &str) -> bool {
        match &self.regex {
            None => self.message == s,
            Some(r) => r.is_match(s),
        }
    }

    fn string(&self) -> &str {
        match &self.regex {
            None => &self.message,
            Some(r) => r.as_str(),
        }
    }

    fn typ(&self) -> &'static str {
        if self.regex.is_none() {
            "message"
        } else {
            "pattern"
        }
    }
}

/// Expected values of one metric (Go `entry`).
#[derive(Clone, Debug)]
pub struct Entry {
    pub pos: usize,
    pub vals: Vec<SequenceValue>,
}

/// Go `%v`/`%s` of a `[]parser.SequenceValue`.
fn seq_string(vals: &[SequenceValue]) -> String {
    let mut s = String::from("[");
    for (i, v) in vals.iter().enumerate() {
        if i > 0 {
            s.push(' ');
        }
        s.push_str(&v.to_string());
    }
    s.push(']');
    s
}

/// Evaluates an expression and checks the result (Go `evalCmd`). Times are in milliseconds.
#[derive(Clone, Debug)]
pub struct EvalCmd {
    pub expr: String,
    pub start: i64,
    pub end: i64,
    pub step: i64,
    /// The line of the command (1-based).
    pub line: usize,
    pub eval: i64,
    pub is_range: bool,
    pub fail: bool,
    pub warn: bool,
    pub ordered: bool,
    pub info: bool,
    pub expected_fail_message: String,
    pub expected_fail_regexp: Option<Regexp>,
    pub expected_cmds: HashMap<ExpectCmdType, Vec<ExpectCmd>>,
    pub metrics: HashMap<u64, Labels>,
    pub expect_scalar: bool,
    pub expected: HashMap<u64, Entry>,
    pub expected_string: String,
    pub exclude_from_range_query: bool,
}

impl EvalCmd {
    fn new_instant(expr: String, start: i64, line: usize) -> EvalCmd {
        EvalCmd {
            expr,
            start,
            end: GO_ZERO_TIME_MS,
            step: 0,
            line,
            eval: start,
            is_range: false,
            fail: false,
            warn: false,
            ordered: false,
            info: false,
            expected_fail_message: String::new(),
            expected_fail_regexp: None,
            expected_cmds: HashMap::new(),
            metrics: HashMap::new(),
            expect_scalar: false,
            expected: HashMap::new(),
            expected_string: String::new(),
            exclude_from_range_query: false,
        }
    }

    fn new_range(expr: String, start: i64, end: i64, step: i64, line: usize) -> EvalCmd {
        let mut c = EvalCmd::new_instant(expr, start, line);
        c.end = end;
        c.step = step;
        c.eval = GO_ZERO_TIME_MS;
        c.is_range = true;
        c
    }

    fn is_ordered(&self) -> bool {
        self.ordered
            || self
                .expected_cmds
                .get(&ExpectCmdType::Ordered)
                .is_some_and(|v| !v.is_empty())
    }

    fn is_fail(&self) -> bool {
        self.fail
            || self
                .expected_cmds
                .get(&ExpectCmdType::Fail)
                .is_some_and(|v| !v.is_empty())
    }

    fn expect(&mut self, pos: usize, v: SequenceValue) {
        self.expect_scalar = true;
        self.expected.insert(0, Entry { pos, vals: vec![v] });
    }

    fn expect_metric(&mut self, pos: usize, m: Labels, vals: Vec<SequenceValue>) {
        self.expect_scalar = false;
        let h = m.hash();
        self.metrics.insert(h, m);
        self.expected.insert(h, Entry { pos, vals });
    }

    fn metric(&self, h: u64) -> String {
        self.metrics
            .get(&h)
            .map(|m| m.to_string())
            .unwrap_or_else(|| "{}".into())
    }

    /// Go `checkAnnotations`.
    fn check_annotations(&self, expr: &str, annos: &Annotations) -> Result<(), String> {
        let (count_warnings, count_info) = annos.count_warnings_and_info();
        if self.warn && count_warnings == 0 {
            return Err(format!(
                "expected warnings evaluating query {} (line {}) but got none",
                go_quote(expr),
                self.line
            ));
        }
        if self.info && count_info == 0 {
            return Err(format!(
                "expected info annotations evaluating query {} (line {}) but got none",
                go_quote(expr),
                self.line
            ));
        }
        for err in annos.iter() {
            if !err.is(&StorageError::Sentinel(&PROMQL_WARNING))
                && !err.is(&StorageError::Sentinel(&PROMQL_INFO))
            {
                return Err(format!(
                    "unexpected annotation type, must be either info or warn but got: {}",
                    err
                ));
            }
        }
        let (warnings, infos) = annos.as_strings(expr, 0, 0);
        let none = Vec::new();
        validate_expected_annotations_of_type(
            expr,
            self.expected_cmds
                .get(&ExpectCmdType::Warn)
                .unwrap_or(&none),
            &warnings,
            self.line,
            "warn",
        )?;
        validate_expected_annotations_of_type(
            expr,
            self.expected_cmds
                .get(&ExpectCmdType::Info)
                .unwrap_or(&none),
            &infos,
            self.line,
            "info",
        )?;
        // Go's AsStrings set the query on the annotation errors (pointers), so AsErrors prints positions.
        let as_errors = || {
            let v: Vec<String> = annos
                .iter()
                .map(|e| match e {
                    StorageError::Annotation(a) => a.error(expr),
                    other => other.to_string(),
                })
                .collect();
            format!("[{}]", v.join(" "))
        };
        if self.expected_cmds.contains_key(&ExpectCmdType::NoWarn) && !warnings.is_empty() {
            return Err(format!(
                "unexpected warning annotations evaluating query {} (line {}): {}",
                go_quote(expr),
                self.line,
                as_errors()
            ));
        }
        if self.expected_cmds.contains_key(&ExpectCmdType::NoInfo) && !infos.is_empty() {
            return Err(format!(
                "unexpected info annotations evaluating query {} (line {}): {}",
                go_quote(expr),
                self.line,
                as_errors()
            ));
        }
        Ok(())
    }

    /// Go `compareResult`.
    pub fn compare_result(&self, result: Value) -> Result<(), String> {
        let g = model::histogram::gofmt::g;
        match result {
            Value::Matrix(mut val) => {
                if self.is_ordered() {
                    return Err("expected ordered result, but query returned a matrix".into());
                }
                if self.expect_scalar {
                    return Err(format!(
                        "expected scalar result, but got matrix {}",
                        crate::value::matrix_string(&val)
                    ));
                }
                assert_matrix_sorted(&val)?;
                let mut seen: HashMap<u64, bool> = HashMap::new();
                for s in val.iter_mut() {
                    let hash = s.metric.hash();
                    if !self.metrics.contains_key(&hash) {
                        return Err(format!(
                            "unexpected metric {} in result, has {}",
                            s.metric,
                            format_series_result(s)
                        ));
                    }
                    seen.insert(hash, true);
                    let empty = Entry {
                        pos: 0,
                        vals: Vec::new(),
                    };
                    let exp = self.expected.get(&hash).unwrap_or(&empty);
                    let mut expected_floats: Vec<FPoint> = Vec::new();
                    let mut expected_histograms: Vec<(i64, FloatHistogram, bool)> = Vec::new();
                    for (i, e) in exp.vals.iter().enumerate() {
                        let ts = self.start + i as i64 * self.step;
                        if ts > self.end {
                            return Err(format!(
                                "expected {} points for {}, but query time range cannot return this many points",
                                exp.vals.len(),
                                self.metric(hash)
                            ));
                        }
                        if let Some(h) = &e.histogram {
                            expected_histograms.push((ts, (**h).clone(), e.counter_reset_hint_set));
                        } else if !e.omitted {
                            expected_floats.push(FPoint { t: ts, f: e.value });
                        }
                    }
                    if expected_floats.len() != s.floats.len()
                        || expected_histograms.len() != s.histograms.len()
                    {
                        return Err(format!(
                            "expected {} float points and {} histogram points for {}, but got {}",
                            expected_floats.len(),
                            expected_histograms.len(),
                            self.metric(hash),
                            format_series_result(s)
                        ));
                    }
                    for (i, expected) in expected_floats.iter().enumerate() {
                        let actual = s.floats[i];
                        if expected.t != actual.t {
                            return Err(format!(
                                "expected float value at index {} for {} to have timestamp {}, but it had timestamp {} (result has {})",
                                i,
                                self.metric(hash),
                                expected.t,
                                actual.t,
                                format_series_result(s)
                            ));
                        }
                        if !almost_equal(actual.f, expected.f, DEFAULT_EPSILON) {
                            return Err(format!(
                                "expected float value at index {} (t={}) for {} to be {}, but got {} (result has {})",
                                i,
                                actual.t,
                                self.metric(hash),
                                g(expected.f),
                                g(actual.f),
                                format_series_result(s)
                            ));
                        }
                    }
                    for (i, (et, eh, hint_set)) in expected_histograms.iter_mut().enumerate() {
                        let at = s.histograms[i].t;
                        if *et != at {
                            return Err(format!(
                                "expected histogram value at index {} for {} to have timestamp {}, but it had timestamp {} (result has {})",
                                i,
                                self.metric(hash),
                                et,
                                at,
                                format_series_result(s)
                            ));
                        }
                        eh.compact(0);
                        let ah = std::sync::Arc::make_mut(&mut s.histograms[i].h);
                        ah.compact(0);
                        if !compare_native_histogram(eh, ah, *hint_set) {
                            let a_expr = ah.test_expression();
                            return Err(format!(
                                "expected histogram value at index {} (t={}) for {} to be {}, but got {} (result has {})",
                                i,
                                at,
                                self.metric(hash),
                                eh.test_expression(),
                                a_expr,
                                format_series_result(s)
                            ));
                        }
                    }
                }
                for hash in self.expected.keys() {
                    if !seen.contains_key(hash) {
                        return Err(format!("expected metric {} not found", self.metric(*hash)));
                    }
                }
            }
            Value::Vector(mut val) => {
                if self.expect_scalar {
                    return Err(format!(
                        "expected scalar result, but got vector {}",
                        crate::value::vector_string(&val)
                    ));
                }
                let mut seen: HashMap<u64, bool> = HashMap::new();
                for (pos, v) in val.iter_mut().enumerate() {
                    let fp = v.metric.hash();
                    if !self.metrics.contains_key(&fp) {
                        if let Some(h) = &v.h {
                            return Err(format!(
                                "unexpected metric {} in result, has value {}",
                                v.metric,
                                h.test_expression()
                            ));
                        }
                        return Err(format!(
                            "unexpected metric {} in result, has value {}",
                            v.metric,
                            g(v.f)
                        ));
                    }
                    let exp = &self.expected[&fp];
                    if self.is_ordered() && exp.pos != pos + 1 {
                        return Err(format!(
                            "expected metric {} with {} at position {} but was at {}",
                            v.metric,
                            seq_string(&exp.vals),
                            exp.pos,
                            pos + 1
                        ));
                    }
                    let exp0 = &exp.vals[0];
                    let mut exp_h = exp0.histogram.as_deref().cloned();
                    if exp_h.is_none() {
                        if let Some(h) = &v.h {
                            return Err(format!(
                                "expected float value {} for {} but got histogram {}",
                                exp0,
                                v.metric,
                                h.test_expression()
                            ));
                        }
                    }
                    if let Some(eh) = &exp_h {
                        if v.h.is_none() {
                            return Err(format!(
                                "expected histogram {} for {} but got float value {}",
                                eh.test_expression(),
                                v.metric,
                                g(v.f)
                            ));
                        }
                    }
                    if let Some(eh) = exp_h.as_mut() {
                        eh.compact(0);
                        let vh = std::sync::Arc::make_mut(v.h.as_mut().unwrap());
                        vh.compact(0);
                        if !compare_native_histogram(eh, vh, exp0.counter_reset_hint_set) {
                            return Err(format!(
                                "expected {} for {} but got {}",
                                eh.test_expression(),
                                v.metric,
                                vh.test_expression()
                            ));
                        }
                    }
                    if !almost_equal(exp0.value, v.f, DEFAULT_EPSILON) {
                        return Err(format!(
                            "expected {} for {} but got {}",
                            g(exp0.value),
                            v.metric,
                            g(v.f)
                        ));
                    }
                    seen.insert(fp, true);
                }
                for (fp, exp_vals) in &self.expected {
                    if !seen.contains_key(fp) {
                        return Err(format!(
                            "expected metric {} with {}: {} not found",
                            self.metric(*fp),
                            exp_vals.pos,
                            seq_string(&exp_vals.vals)
                        ));
                    }
                }
            }
            Value::Scalar(val) => {
                if !self.expect_scalar {
                    return Err(format!("expected vector or matrix result, but got {}", val));
                }
                let exp0 = &self.expected[&0].vals[0];
                if let Some(h) = &exp0.histogram {
                    return Err(format!(
                        "expected histogram {} but got {}",
                        h.test_expression(),
                        val
                    ));
                }
                if !almost_equal(exp0.value, val.v, DEFAULT_EPSILON) {
                    return Err(format!(
                        "expected scalar {} but got {}",
                        g(exp0.value),
                        g(val.v)
                    ));
                }
            }
            Value::String(val) => {
                if self.expected_string != val.v {
                    return Err(format!(
                        "expected string \"{}\" but got \"{}\"",
                        self.expected_string, val.v
                    ));
                }
            }
        }
        Ok(())
    }

    /// Go `checkExpectedFailure`.
    fn check_expected_failure(&self, actual: &str) -> Result<(), String> {
        if !self.expected_fail_message.is_empty() && self.expected_fail_message != actual {
            return Err(format!(
                "expected error {} evaluating query {} (line {}), but got: {}",
                go_quote(&self.expected_fail_message),
                go_quote(&self.expr),
                self.line,
                actual
            ));
        }
        if let Some(r) = &self.expected_fail_regexp {
            if !r.is_match(actual) {
                return Err(format!(
                    "expected error matching pattern {} evaluating query {} (line {}), but got: {}",
                    go_quote(r.as_str()),
                    go_quote(&self.expr),
                    self.line,
                    actual
                ));
            }
        }
        if let Some(exp_fail) = self.expected_cmds.get(&ExpectCmdType::Fail) {
            if let Some(e) = exp_fail.first() {
                if !e.check_match(actual) {
                    return Err(format!(
                        "expected error matching {} evaluating query {} (line {}), but got: {}",
                        go_quote(e.string()),
                        go_quote(&self.expr),
                        self.line,
                        actual
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Go `validateExpectedAnnotationsOfType`.
fn validate_expected_annotations_of_type(
    expr: &str,
    expected: &[ExpectCmd],
    actual: &[String],
    line: usize,
    annotation_type: &str,
) -> Result<(), String> {
    if expected.is_empty() {
        return Ok(());
    }
    if actual.is_empty() {
        return Err(format!(
            "expected {} annotations but none were found for query {} (line {})",
            annotation_type,
            go_quote(expr),
            line
        ));
    }
    for e in expected {
        if !actual.iter().any(|a| e.check_match(a)) {
            return Err(format!(
                "expected {} annotation matching {} {} but no matching annotation was found for query {} (line {}), found: [{}]",
                annotation_type,
                e.typ(),
                go_quote(e.string()),
                go_quote(expr),
                line,
                actual.join(" ")
            ));
        }
    }
    for a in actual {
        if !expected.iter().any(|e| e.check_match(a)) {
            return Err(format!(
                "unexpected {} annotation {} found for query {} (line {})",
                annotation_type,
                go_quote(a),
                expr,
                line
            ));
        }
    }
    Ok(())
}

/// Go `parseExpect`.
fn parse_expect(def_line: &str) -> Result<(ExpectCmdType, ExpectCmd), String> {
    let p = patterns();
    let Some(parts) = submatches(&p.expect, def_line.trim()) else {
        return Err(
            "invalid expect statement, must match `expect <type> <match_type>: <string>` format"
                .into(),
        );
    };
    let mode = parts[1];
    let typ = match mode {
        "fail" => ExpectCmdType::Fail,
        "ordered" => ExpectCmdType::Ordered,
        "warn" => ExpectCmdType::Warn,
        "no_warn" => ExpectCmdType::NoWarn,
        "info" => ExpectCmdType::Info,
        "no_info" => ExpectCmdType::NoInfo,
        _ => return Err(format!("invalid expected error/annotation type {}", mode)),
    };
    let mut e = ExpectCmd {
        message: String::new(),
        regex: None,
    };
    if !parts[2].is_empty() {
        match parts[2] {
            "msg" => e.message = parts[3].trim().to_string(),
            "regex" => {
                let pattern = parts[3].trim();
                e.regex = Some(
                    Regexp::compile(pattern)
                        .map_err(|_| format!("invalid regex {} for {}", pattern, mode))?,
                );
            }
            other => return Err(format!("invalid token {} after {}", other, mode)),
        }
    } else {
        e.regex = Some(p.match_any.clone());
    }
    Ok((typ, e))
}

/// Go `validateExpectedCmds`.
fn validate_expected_cmds(cmd: &EvalCmd) -> Result<(), String> {
    let n = |t| {
        cmd.expected_cmds
            .get(&t)
            .map_or(0, |v: &Vec<ExpectCmd>| v.len())
    };
    if n(ExpectCmdType::Info) > 0 && n(ExpectCmdType::NoInfo) > 0 {
        return Err("invalid expect lines, info and no_info cannot be used together".into());
    }
    if n(ExpectCmdType::Warn) > 0 && n(ExpectCmdType::NoWarn) > 0 {
        return Err("invalid expect lines, warn and no_warn cannot be used together".into());
    }
    if n(ExpectCmdType::Fail) > 1 {
        return Err("invalid expect lines, multiple expect fail lines are not allowed".into());
    }
    Ok(())
}

/// Go `parseDurations`: from, to and step in nanoseconds.
fn parse_durations(from: &str, to: &str, step: &str) -> Result<(i64, i64, i64), String> {
    let f = parse_duration(from).map_err(|e| {
        format!(
            "invalid start timestamp definition {}: {}",
            go_quote(from),
            e
        )
    })?;
    let t = parse_duration(to)
        .map_err(|e| format!("invalid end timestamp definition {}: {}", go_quote(to), e))?;
    if t < f {
        return Err(format!(
            "invalid test definition, end timestamp ({}) is before start timestamp ({})",
            to, from
        ));
    }
    let s = parse_duration(step)
        .map_err(|e| format!("invalid step definition {}: {}", go_quote(step), e))?;
    Ok((f, t, s))
}

/// Go `parseExpectRangeVector`: start, end and step in milliseconds.
fn parse_expect_range_vector(line: &str) -> Result<(i64, i64, i64), String> {
    let parts = submatches(&patterns().expect_range, line);
    let Some(parts) = parts.filter(|p| p.len() == 4) else {
        return Err(format!(
            "invalid range vector definition {}",
            go_quote(line)
        ));
    };
    let (f, t, s) = parse_durations(parts[1], parts[2], parts[3])?;
    Ok((TEST_START_TIME + f / MS, TEST_START_TIME + t / MS, s / MS))
}

/// Go `parseAsStringLiteral`.
fn parse_as_string_literal(line: &str) -> Result<String, String> {
    if line == EXPECT_STRING_PREFIX {
        return Err(
            "expected string literal not valid - a quoted string literal is required".into(),
        );
    }
    let s = line
        .strip_prefix(&format!("{} ", EXPECT_STRING_PREFIX))
        .unwrap_or(line);
    if s.is_empty() {
        return Err(
            "expected string literal not valid - a quoted string literal is required".into(),
        );
    }
    go_unquote(s).ok_or_else(|| {
        "expected string literal not valid - check that the string is correctly quoted".into()
    })
}

/// Go `strconv.Unquote` (single-quoted strings must hold exactly one character).
fn go_unquote(s: &str) -> Option<String> {
    let r = unquote(s).ok()?;
    if s.starts_with('\'') && r.chars().count() != 1 {
        return None;
    }
    Some(r)
}

/// Go `parseNumber`.
fn parse_number(s: &str) -> Result<f64, String> {
    if let Some(n) = parse_int_base0(s) {
        return Ok(n as f64);
    }
    parse_float(s).map_err(|(_, e)| format!("error parsing number: {}", e))
}

/// Go `(*test).parseEval`: parses an eval command at line `i`; returns the index of its last line.
fn parse_eval(lines: &[String], mut i: usize) -> Result<(usize, EvalCmd), String> {
    let p = patterns();
    let instant_parts = submatches(&p.eval_instant, &lines[i]);
    let range_parts = submatches(&p.eval_range, &lines[i]);
    if instant_parts.is_none() && range_parts.is_none() {
        return Err(raise(
            i,
            "invalid evaluation command. Must be either 'eval[_fail|_warn|_ordered] instant [at <offset:duration>] <query>' or 'eval[_fail|_warn] range from <from> to <to> step <step> <query>'".into(),
        ));
    }
    let is_instant = instant_parts.is_some();
    let (mode, expr) = match &instant_parts {
        Some(ip) => (ip[1], ip[3]),
        None => {
            let rp = range_parts.as_ref().unwrap();
            (rp[1], rp[5])
        }
    };
    if let Err(mut err) = test_parser().parse_expr(expr) {
        let line = &lines[i];
        let off = line.find(expr).map(|x| x as i64).unwrap_or(-1);
        parser::enrich_parse_error(&mut err, |e| {
            e.line_offset = i as i32;
            e.position_range.start += off as parser::Pos;
            e.position_range.end += off as parser::Pos;
            e.query = line.clone();
        });
        return Err(err.to_string());
    }
    let line_no = i + 1;
    // Go formatErr reads the loop variable i when it is called: the line being parsed.
    let format_err =
        |line: usize, msg: String| format!("error in eval {} (line {}): {}", expr, line + 1, msg);

    let mut cmd = if let Some(ip) = &instant_parts {
        let at = ip[2];
        let offset = parse_duration(at).map_err(|e| {
            format_err(
                i,
                format!("invalid timestamp definition {}: {}", go_quote(at), e),
            )
        })?;
        EvalCmd::new_instant(expr.to_string(), TEST_START_TIME + offset / MS, line_no)
    } else {
        let rp = range_parts.as_ref().unwrap();
        let (f, t, s) = parse_durations(rp[2], rp[3], rp[4]).map_err(|e| format_err(i, e))?;
        EvalCmd::new_range(
            expr.to_string(),
            TEST_START_TIME + f / MS,
            TEST_START_TIME + t / MS,
            s / MS,
            line_no,
        )
    };
    match mode {
        "ordered" => cmd.ordered = true,
        "fail" => cmd.fail = true,
        "warn" => cmd.warn = true,
        "info" => cmd.info = true,
        _ => {}
    }

    let mut expect_range_vector = false;
    let mut j: i64 = 1;
    while i + 1 < lines.len() {
        i += 1;
        let def_line = &lines[i];
        if def_line.is_empty() {
            i -= 1;
            break;
        }
        if cmd.fail && def_line.starts_with("expected_fail_message") {
            cmd.expected_fail_message =
                def_line["expected_fail_message".len()..].trim().to_string();
            break;
        }
        if cmd.fail && def_line.starts_with("expected_fail_regexp") {
            let pattern = def_line["expected_fail_regexp".len()..].trim();
            cmd.expected_fail_regexp = Some(Regexp::compile(pattern).map_err(|e| {
                format_err(
                    i,
                    format!(
                        "invalid regexp '{}' for expected_fail_regexp: {}",
                        pattern, e
                    ),
                )
            })?);
            break;
        }
        if def_line.starts_with(RANGE_VECTOR_PREFIX) {
            let (start, end, step) =
                parse_expect_range_vector(def_line).map_err(|e| format_err(i, e))?;
            expect_range_vector = true;
            cmd.start = start;
            cmd.end = end;
            cmd.step = step;
            cmd.eval = end;
            cmd.exclude_from_range_query = true;
            j += 1;
            continue;
        }
        if def_line.starts_with(EXPECT_STRING_PREFIX) {
            cmd.expected_string =
                parse_as_string_literal(def_line).map_err(|e| format_err(i, e))?;
            cmd.exclude_from_range_query = true;
            j += 1;
            continue;
        }
        if def_line.split(' ').next() == Some("expect") {
            let (typ, e) = parse_expect(def_line).map_err(|e| format_err(i, e))?;
            cmd.expected_cmds.entry(typ).or_default().push(e);
            validate_expected_cmds(&cmd).map_err(|e| format_err(i, e))?;
            continue;
        }
        if let Ok(f) = parse_number(def_line) {
            cmd.expect(0, value(f));
            break;
        }
        let (metric, vals) = parse_series(def_line, i)?;
        if vals.len() > 1 && is_instant && !expect_range_vector {
            return Err(format_err(i,
                "expecting multiple values in instant evaluation not allowed. consider using 'expect range vector' directive to enable a range vector result for an instant query".into(),
            ));
        }
        cmd.expect_metric(j as usize, metric, vals);
        j += 1;
    }
    Ok((i, cmd))
}

// ---------------------------------------------------------------------------------------------------------------
// Result comparison helpers.

/// Go `almost.Equal`: true if a and b differ by less than their sum times epsilon, or if both are StaleNaN, or
/// both are any other NaN.
pub fn almost_equal(a: f64, b: f64, epsilon: f64) -> bool {
    let min_normal = f64::from_bits(0x0010000000000000);
    if is_stale_nan(a) || is_stale_nan(b) {
        return is_stale_nan(a) && is_stale_nan(b);
    }
    if a.is_nan() && b.is_nan() {
        return true;
    }
    if a == b {
        return true;
    }
    let abs_sum = a.abs() + b.abs();
    let diff = (a - b).abs();
    if a == 0.0 || b == 0.0 || abs_sum < min_normal {
        return diff < epsilon * min_normal;
    }
    diff / abs_sum.min(f64::MAX) < epsilon
}

/// Go `compareNativeHistogram`: compares two histograms with a tolerance for float fields; the counter reset hint
/// only counts when it was set explicitly in the expectation.
pub fn compare_native_histogram(
    exp: &FloatHistogram,
    cur: &FloatHistogram,
    counter_reset_hint_set: bool,
) -> bool {
    if exp.schema != cur.schema
        || !almost_equal(exp.count, cur.count, DEFAULT_EPSILON)
        || !almost_equal(exp.sum, cur.sum, DEFAULT_EPSILON)
    {
        return false;
    }
    if exp.uses_custom_buckets()
        && !custom_bucket_bounds_match(exp.custom_values_slice(), cur.custom_values_slice())
    {
        return false;
    }
    if exp.zero_threshold != cur.zero_threshold
        || !almost_equal(exp.zero_count, cur.zero_count, DEFAULT_EPSILON)
    {
        return false;
    }
    if !spans_match(&exp.negative_spans, &cur.negative_spans) {
        return false;
    }
    if !float_buckets_match(&exp.negative_buckets, &cur.negative_buckets) {
        return false;
    }
    if !spans_match(&exp.positive_spans, &cur.positive_spans) {
        return false;
    }
    if !float_buckets_match(&exp.positive_buckets, &cur.positive_buckets) {
        return false;
    }
    if counter_reset_hint_set && exp.counter_reset_hint != cur.counter_reset_hint {
        return false;
    }
    true
}

fn float_buckets_match(b1: &[f64], b2: &[f64]) -> bool {
    b1.len() == b2.len()
        && b1
            .iter()
            .zip(b2)
            .all(|(a, b)| almost_equal(*a, *b, DEFAULT_EPSILON))
}

fn all_empty_spans(s: &[Span]) -> bool {
    s.iter().all(|s| s.length == 0)
}

/// Go `spansMatch` of promqltest (zero-length spans are merged into the next span).
fn spans_match(s1: &[Span], s2: &[Span]) -> bool {
    if s1.is_empty() && s2.is_empty() {
        return true;
    }
    let (mut i1, mut i2) = (0, 0);
    loop {
        if i1 >= s1.len() {
            return all_empty_spans(&s2[i2..]);
        }
        if i2 >= s2.len() {
            return all_empty_spans(&s1[i1..]);
        }
        let (mut c1, mut c2) = (s1[i1], s2[i2]);
        i1 += 1;
        i2 += 1;
        if c1.length == 0 {
            while i1 < s1.len() && s1[i1].length == 0 {
                c1.offset += s1[i1].offset;
                i1 += 1;
            }
            if i1 < s1.len() {
                c1.offset += s1[i1].offset;
                c1.length = s1[i1].length;
                i1 += 1;
            }
        }
        if c2.length == 0 {
            while i2 < s2.len() && s2[i2].length == 0 {
                c2.offset += s2[i2].offset;
                i2 += 1;
            }
            if i2 < s2.len() {
                c2.offset += s2[i2].offset;
                c2.length = s2[i2].length;
                i2 += 1;
            }
        }
        if c1.length == 0 && c2.length == 0 {
            return true;
        }
        if c1.offset != c2.offset || c1.length != c2.length {
            return false;
        }
    }
}

/// Go `formatSeriesResult`.
fn format_series_result(s: &Series) -> String {
    let fp = if s.floats.len() == 1 { "" } else { "s" };
    let hp = if s.histograms.len() == 1 { "" } else { "s" };
    let floats: Vec<String> = s.floats.iter().map(|p| p.to_string()).collect();
    let hists: Vec<String> = s
        .histograms
        .iter()
        .map(|p: &HPoint| format!("{} @[{}]", p.h.test_expression(), p.t))
        .collect();
    format!(
        "{} float point{} [{}] and {} histogram point{} [{}]",
        s.floats.len(),
        fp,
        floats.join(" "),
        s.histograms.len(),
        hp,
        hists.join(" ")
    )
}

/// Go `HistogramTestExpression`: the test expression of the histogram, or "" for none.
pub fn histogram_test_expression(h: Option<&FloatHistogram>) -> String {
    h.map(|h| h.test_expression()).unwrap_or_default()
}

/// Go `assertMatrixSorted`.
pub fn assert_matrix_sorted(m: &Matrix) -> Result<(), String> {
    if m.len() <= 1 {
        return Ok(());
    }
    for i in 0..m.len() - 1 {
        let next = &m[i + 1].metric;
        if labels::compare(&m[i].metric, next) > 0 {
            return Err(format!(
                "matrix results should always be sorted by labels, but matrix is not sorted: series at index {} with labels {} sorts before series at index {} with labels {}",
                i + 1,
                next,
                i,
                m[i].metric
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------------------------
// @ modifier test cases.

/// An instant query with its evaluation time in milliseconds (Go `atModifierTestCase`).
#[derive(Clone, Debug, PartialEq)]
pub struct AtModifierTestCase {
    pub expr: String,
    pub eval_time: i64,
}

/// Go `atModifierTestCases`: the query with `@ <evalTime>` on every selector, at other evaluation times.
pub fn at_modifier_test_cases(
    expr_str: &str,
    eval_time: i64,
) -> Result<Vec<AtModifierTestCase>, String> {
    let mut expr = test_parser()
        .parse_expr(expr_str)
        .map_err(|e| e.to_string())?;
    let ts = eval_time;
    let mut contains_non_step_invariant = false;
    set_at(&mut expr, ts, false, &mut contains_non_step_invariant);
    if contains_non_step_invariant {
        return Ok(Vec::new());
    }
    let new_expr = expr.to_string();
    let times: Vec<i64> = if ts == 0 {
        vec![-1000, -ts, 1000]
    } else {
        vec![-10 * ts, 0, ts / 5, ts, 10 * ts]
    };
    Ok(times
        .into_iter()
        .map(|et| AtModifierTestCase {
            expr: new_expr.clone(),
            eval_time: et,
        })
        .collect())
}

/// The visitor of Go `atModifierTestCases` (`parser.Inspect` with the path of ancestors).
fn set_at(node: &mut Expr, ts: i64, under_at_subquery: bool, unsafe_fn: &mut bool) {
    if !under_at_subquery {
        match node {
            Expr::VectorSelector(n) => {
                if n.timestamp.is_none() {
                    n.timestamp = Some(ts);
                }
            }
            Expr::MatrixSelector(n) => {
                if let Some(vs) = n.vector_selector.as_vector_selector_mut() {
                    if vs.timestamp.is_none() {
                        vs.timestamp = Some(ts);
                    }
                }
            }
            Expr::Subquery(n) => {
                if n.timestamp.is_none() {
                    n.timestamp = Some(ts);
                }
            }
            Expr::Call(n) => {
                *unsafe_fn = *unsafe_fn || crate::functions::is_at_modifier_unsafe(&n.func.name);
            }
            _ => {}
        }
    }
    let child_under =
        under_at_subquery || matches!(node, Expr::Subquery(n) if n.timestamp.is_some());
    match node {
        Expr::Aggregate(n) => {
            if let Some(e) = n.expr.as_mut() {
                set_at(e, ts, child_under, unsafe_fn);
            }
            if let Some(p) = n.param.as_mut() {
                set_at(p, ts, child_under, unsafe_fn);
            }
        }
        Expr::Binary(n) => {
            set_at(&mut n.lhs, ts, child_under, unsafe_fn);
            set_at(&mut n.rhs, ts, child_under, unsafe_fn);
        }
        Expr::Call(n) => {
            for a in n.args.iter_mut() {
                set_at(a, ts, child_under, unsafe_fn);
            }
        }
        Expr::Subquery(n) => set_at(&mut n.expr, ts, child_under, unsafe_fn),
        Expr::Paren(n) => set_at(&mut n.expr, ts, child_under, unsafe_fn),
        Expr::Unary(n) => set_at(&mut n.expr, ts, child_under, unsafe_fn),
        Expr::MatrixSelector(n) => set_at(&mut n.vector_selector, ts, child_under, unsafe_fn),
        Expr::StepInvariant(n) => set_at(&mut n.expr, ts, child_under, unsafe_fn),
        Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::VectorSelector(_)
        | Expr::Duration(_) => {}
    }
}

// ---------------------------------------------------------------------------------------------------------------
// LazyLoader.

/// Options of the lazy loader (Go `LazyLoaderOpts`).
#[derive(Clone, Debug, Default)]
pub struct LazyLoaderOpts {
    pub enable_at_modifier: bool,
    pub enable_negative_offset: bool,
    pub enable_delayed_name_removal: bool,
    /// The start time of the test in milliseconds; `None` is the Unix epoch.
    pub start_time: Option<i64>,
    /// The parser of the engine; `None` uses the engine's default.
    pub parser: Option<Parser>,
}

/// Lazily loads samples into a storage, for rule unit tests (Go `LazyLoader`).
pub struct LazyLoader {
    load_cmd: LoadCmd,
    storage: TestStorage,
    /// The default subquery step (Go `SubqueryInterval`), in milliseconds; read by the engine at query time.
    pub subquery_interval: Arc<AtomicI64>,
    query_engine: Engine,
    opts: LazyLoaderOpts,
}

impl LazyLoader {
    /// Go `NewLazyLoader`.
    pub fn new(input: &str, opts: LazyLoaderOpts) -> Result<LazyLoader, String> {
        let load_cmd = Self::parse(input, &opts)?;
        let subquery_interval = Arc::new(AtomicI64::new(0));
        let (storage, query_engine) = Self::fresh(&opts, &subquery_interval);
        Ok(LazyLoader {
            load_cmd,
            storage,
            subquery_interval,
            query_engine,
            opts,
        })
    }

    fn parse(input: &str, opts: &LazyLoaderOpts) -> Result<LoadCmd, String> {
        let lines = get_lines(input);
        for (i, l) in lines.iter().enumerate() {
            if l.is_empty() {
                continue;
            }
            let first = l.split([' ', '\t']).next().unwrap_or("").to_lowercase();
            if first.starts_with("load") {
                let start = opts.start_time.unwrap_or(TEST_START_TIME);
                let (_, cmd) = parse_load(&lines, i, start)?;
                return Ok(cmd);
            }
            return Err(raise(i, format!("invalid command {}", go_quote(l))));
        }
        Err("no \"load\" command found".into())
    }

    fn fresh(opts: &LazyLoaderOpts, sqi: &Arc<AtomicI64>) -> (TestStorage, Engine) {
        let sqi = sqi.clone();
        let engine = Engine::new(EngineOpts {
            max_samples: 10000,
            timeout: Duration::from_secs(100),
            no_step_subquery_interval_fn: Some(Arc::new(move |_| sqi.load(Ordering::Relaxed))),
            enable_at_modifier: opts.enable_at_modifier,
            enable_negative_offset: opts.enable_negative_offset,
            enable_delayed_name_removal: opts.enable_delayed_name_removal,
            parser: opts.parser.clone(),
            ..Default::default()
        });
        (TestStorage::new(), engine)
    }

    /// Go `appendTill`: appends the samples up to `ts` (milliseconds).
    fn append_till(&mut self, ts: i64) -> Result<(), String> {
        let mut app = self.storage.appender_v2();
        for (m, smpls) in self.load_cmd.defs.iter_mut() {
            let mut cut = smpls.len();
            for (i, s) in smpls.iter().enumerate() {
                if s.t > ts {
                    cut = i;
                    break;
                }
                append_sample(&mut app, s, m)?;
            }
            smpls.drain(..cut);
        }
        match app.commit() {
            Some(e) => Err(e.to_string()),
            None => Ok(()),
        }
    }

    /// Go `WithSamplesTill`: loads the samples up to `ts` (milliseconds) and calls `f` with the result.
    pub fn with_samples_till(&mut self, ts: i64, f: impl FnOnce(Result<(), String>)) {
        let r = self.append_till(ts);
        f(r)
    }

    /// Go `QueryEngine`.
    pub fn query_engine(&self) -> &Engine {
        &self.query_engine
    }

    /// Go `Queryable`.
    pub fn queryable(&self) -> Arc<dyn Queryable> {
        Arc::new(self.storage.clone())
    }

    /// Go `Storage`.
    pub fn storage(&self) -> &TestStorage {
        &self.storage
    }

    /// Go `Context`.
    pub fn context(&self) -> Context {
        Context::background()
    }

    /// The options the loader was made with.
    pub fn opts(&self) -> &LazyLoaderOpts {
        &self.opts
    }

    /// Go `Close`.
    pub fn close(&self) -> Result<(), String> {
        self.query_engine.close().map_err(|e| e.to_string())
    }
}

#[doc(hidden)]
pub mod internal {
    //! Unexported Go functions that the ported Go tests call.
    pub use super::{LoadCmd, SampleSt};
    pub use super::{assert_matrix_sorted, parse_load, parse_st_sequence, test_test};

    /// Go `getLines` for tests.
    pub fn lines(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }
}
