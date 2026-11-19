//! Port of cmd/promtool/main.go: the command line, `check config|web-config|rules|healthy|ready`, the rule
//! duplicate lint, `promql format|label-matchers` and the dispatch to the other modules.

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;

use config::ScrapeConfig;
use discovery::Group;
use model::labels::{Labels, MatchType, Matcher, ValidationScheme};
use promql::parser::{Expr, Parser};
use rules::rulefmt::{RuleGroup, RuleGroups};
use yaml::url::GoUrl;

use crate::cli::{App, Arg, Cmd, Flag, Kind, Outcome, Parsed};
use crate::query::Printer;
use crate::{FAILURE_EXIT_CODE, Io, LINT_ERR_EXIT_CODE, SUCCESS_EXIT_CODE};

pub const LINT_OPTION_ALL: &str = "all";
pub const LINT_OPTION_DUPLICATE_RULES: &str = "duplicate-rules";
pub const LINT_OPTION_TOO_LONG_SCRAPE_INTERVAL: &str = "too-long-scrape-interval";
pub const LINT_OPTION_NONE: &str = "none";
pub const CHECK_HEALTH: &str = "/-/healthy";
pub const CHECK_READINESS: &str = "/-/ready";

const HTTP_CONFIG_FILE_DESCRIPTION: &str =
    "HTTP client configuration file, see details at https://prometheus.io/docs/prometheus/latest/configuration/promtool";

const CHECK_METRICS_USAGE: &str = "Pass Prometheus metrics over stdin to lint them for consistency and correctness, and optionally perform cardinality analysis.

examples:

$ cat metrics.prom | promtool check metrics

$ curl -s http://localhost:9090/metrics | promtool check metrics --extended

$ curl -s http://localhost:9100/metrics | promtool check metrics --extended --lint=none";

/// The version text (Go `version.Print("promtool")`).
pub fn version_string() -> String {
    "promtool, version 3.16.0 (branch: HEAD, revision: e2e28adeee2c5883d000fc82aca6008cfbf7e96d)
  build user:       prometheus-rs
  build date:
  go version:       rust
  platform:         linux/amd64
  tags:             netgo,builtinassets"
        .to_string()
}

fn lint_rules_options() -> Vec<&'static str> {
    vec![LINT_OPTION_ALL, LINT_OPTION_DUPLICATE_RULES, LINT_OPTION_NONE]
}

fn lint_config_options() -> Vec<&'static str> {
    let mut v = lint_rules_options();
    v.push(LINT_OPTION_TOO_LONG_SCRAPE_INTERVAL);
    v
}

/// The kingpin application of main.go.
pub fn build_app(name: &str) -> App {
    use Kind::*;
    let http_config = || Flag::new("http.config.file", HTTP_CONFIG_FILE_DESCRIPTION, ExistingFile).placeholder("<filename>");
    let default_db = "data/";
    let bench_file = "../../tsdb/testdata/20kseries.json";

    let check = Cmd::new("check", "Check the resources for validity.")
        .flag(Flag::new("query.lookback-delta", "The server's maximum query lookback duration.", Duration).default("5m"))
        .cmd(
            Cmd::new(
                "service-discovery",
                "Perform service discovery for the given job name and report the results, including relabeling.",
            )
            .arg(Arg::new("config-file", "The prometheus config file.", ExistingFile).required())
            .arg(Arg::new("job", "The job to run service discovery for.", String).required())
            .flag(Flag::new("timeout", "The time to wait for discovery results.", Duration).default("30s")),
        )
        .cmd(
            Cmd::new("config", "Check if the config files are valid or not.")
                .arg(Arg::new("config-files", "The config files to check.", ExistingFiles).required())
                .flag(Flag::new(
                    "syntax-only",
                    "Only check the config file syntax, ignoring file and content validation referenced in the config",
                    Bool,
                ))
                .flag(
                    Flag::new(
                        "lint",
                        format!(
                            "Linting checks to apply to the rules/scrape configs specified in the config. Available options are: {}. Use --lint=none to disable linting",
                            lint_config_options().join(", ")
                        ),
                        String,
                    )
                    .default(LINT_OPTION_DUPLICATE_RULES),
                )
                .flag(Flag::new("lint-fatal", "Make lint errors exit with exit code 3.", Bool).default("false"))
                .flag(Flag::new("ignore-unknown-fields", "Ignore unknown fields in the rule groups read by the config files. This is useful when you want to extend rule files with custom metadata. Ensure that those fields are removed before loading them into the Prometheus server as it performs strict checks by default.", Bool).default("false"))
                .flag(Flag::new("agent", "Check config file for Prometheus in Agent mode.", Bool)),
        )
        .cmd(
            Cmd::new("web-config", "Check if the web config files are valid or not.")
                .arg(Arg::new("web-config-files", "The config files to check.", ExistingFiles).required()),
        )
        .cmd(
            Cmd::new("healthy", "Check if the Prometheus server is healthy.")
                .flag(http_config())
                .flag(Flag::new("url", "The URL for the Prometheus server.", Url).default("http://localhost:9090")),
        )
        .cmd(
            Cmd::new("ready", "Check if the Prometheus server is ready.")
                .flag(http_config())
                .flag(Flag::new("url", "The URL for the Prometheus server.", Url).default("http://localhost:9090")),
        )
        .cmd(
            Cmd::new("rules", "Check if the rule files are valid or not.")
                .arg(Arg::new("rule-files", "The rule files to check, default is read from standard input.", ExistingFiles))
                .flag(
                    Flag::new(
                        "lint",
                        format!(
                            "Linting checks to apply. Available options are: {}. Use --lint=none to disable linting",
                            lint_rules_options().join(", ")
                        ),
                        String,
                    )
                    .default(LINT_OPTION_DUPLICATE_RULES),
                )
                .flag(Flag::new("lint-fatal", "Make lint errors exit with exit code 3.", Bool).default("false"))
                .flag(Flag::new("ignore-unknown-fields", "Ignore unknown fields in the rule files. This is useful when you want to extend rule files with custom metadata. Ensure that those fields are removed before loading them into the Prometheus server as it performs strict checks by default.", Bool).default("false")),
        )
        .cmd(
            Cmd::new("metrics", CHECK_METRICS_USAGE)
                .flag(Flag::new("extended", "Print extended information related to the cardinality of the metrics.", Bool))
                .flag(
                    Flag::new(
                        "lint",
                        "Linting checks to apply for metrics. Available options are: all, none. Use --lint=none to disable metrics linting.",
                        String,
                    )
                    .default(LINT_OPTION_ALL),
                ),
        );

    let start_end = |c: Cmd, start_help: &str, end_help: &str| {
        c.flag(Flag::new("start", start_help, String)).flag(Flag::new("end", end_help, String))
    };
    let query = Cmd::new("query", "Run query against a Prometheus server.")
        .flag(Flag::new("format", "Output format of the query.", Enum(vec!["promql", "json"])).short('o').default("promql"))
        .flag(http_config())
        .cmd(
            Cmd::new("instant", "Run instant query.")
                .arg(Arg::new("server", "Prometheus server to query.", Url).required())
                .arg(Arg::new("expr", "PromQL query expression.", String).required())
                .flag(Flag::new("time", "Query evaluation time (RFC3339 or Unix timestamp).", String))
                .flag(Flag::new("header", "Extra headers to send to server.", StringMap)),
        )
        .cmd(
            Cmd::new("range", "Run range query.")
                .arg(Arg::new("server", "Prometheus server to query.", Url).required())
                .arg(Arg::new("expr", "PromQL query expression.", String).required())
                .flag(Flag::new("header", "Extra headers to send to server.", StringMap))
                .flag(Flag::new("start", "Query range start time (RFC3339 or Unix timestamp).", String))
                .flag(Flag::new("end", "Query range end time (RFC3339 or Unix timestamp).", String))
                .flag(Flag::new("step", "Query step size (duration).", Duration)),
        )
        .cmd(start_end(
            Cmd::new("series", "Run series query.")
                .arg(Arg::new("server", "Prometheus server to query.", Url).required())
                .flag(Flag::new("match", "Series selector. Can be specified multiple times.", Strings).required()),
            "Start time (RFC3339 or Unix timestamp).",
            "End time (RFC3339 or Unix timestamp).",
        ))
        .cmd(
            start_end(
                Cmd::new("labels", "Run labels query.")
                    .arg(Arg::new("server", "Prometheus server to query.", Url).required())
                    .arg(Arg::new("name", "Label name to provide label values for.", String).required()),
                "Start time (RFC3339 or Unix timestamp).",
                "End time (RFC3339 or Unix timestamp).",
            )
            .flag(Flag::new("match", "Series selector. Can be specified multiple times.", Strings)),
        )
        .cmd(
            Cmd::new("analyze", "Run queries against your Prometheus to analyze the usage pattern of certain metrics.")
                .flag(Flag::new("server", "Prometheus server to query.", Url).required())
                .flag(Flag::new("type", "Type of metric: histogram.", String).required())
                .flag(Flag::new("duration", "Time frame to analyze.", Duration).default("1h"))
                .flag(Flag::new("time", "Query time (RFC3339 or Unix timestamp), defaults to now.", String))
                .flag(Flag::new("match", "Series selector. Can be specified multiple times.", Strings).required()),
        );

    let debug = Cmd::new("debug", "Fetch debug information.")
        .cmd(
            Cmd::new("pprof", "Fetch profiling debug information.")
                .arg(Arg::new("server", "Prometheus server to get pprof files from.", String).required()),
        )
        .cmd(
            Cmd::new("metrics", "Fetch metrics debug information.")
                .arg(Arg::new("server", "Prometheus server to get metrics from.", String).required()),
        )
        .cmd(
            Cmd::new("all", "Fetch all debug information.")
                .arg(Arg::new("server", "Prometheus server to get all debug information from.", String).required()),
        );

    let push = Cmd::new("push", "Push to a Prometheus server.").flag(http_config()).cmd(
        Cmd::new("metrics", "Push metrics to a prometheus remote write (for testing purpose only).")
            .arg(Arg::new("remote-write-url", "Prometheus remote write url to push metrics.", Url).required())
            .arg(Arg::new("metric-files", "The metric files to push, default is read from standard input.", ExistingFiles))
            .flag(Flag::new("label", "Label to attach to metrics. Can be specified multiple times.", StringMap).default("job=promtool"))
            .flag(Flag::new("timeout", "The time to wait for pushing metrics.", Duration).default("30s"))
            .flag(Flag::new("header", "Prometheus remote write header.", StringMap))
            .flag(
                Flag::new(
                    "protobuf_message",
                    "Protobuf message to use when writing (prometheus.WriteRequest or io.prometheus.write.v2.Request).",
                    String,
                )
                .default("prometheus.WriteRequest"),
            )
            .flag(Flag::new("remote-write.path", "Override the default remote write API path.", String).default("/api/v1/write")),
    );

    let test = Cmd::new("test", "Unit testing.")
        .flag(Flag::new("junit", "File path to store JUnit XML test results.", OpenFile))
        .cmd(
            Cmd::new("rules", "Unit tests for rules.")
                .flag(Flag::new("run", "If set, will only run test groups whose names match the regular expression. Can be specified multiple times.", Strings))
                .arg(Arg::new("test-rule-file", "The unit test file.", ExistingFiles).required())
                .flag(Flag::new("debug", "Enable unit test debugging.", Bool).default("false"))
                .flag(Flag::new("diff", "[Experimental] Print colored differential output between expected & received output.", Bool).default("false"))
                .flag(Flag::new("ignore-unknown-fields", "Ignore unknown fields in the test files. This is useful when you want to extend rule files with custom metadata. Ensure that those fields are removed before loading them into the Prometheus server as it performs strict checks by default.", Bool).default("false")),
        );

    let db_arg = || Arg::new("db path", format!("Database path (default is {default_db})."), String).default(default_db);
    let sandbox = || Flag::new("sandbox-dir-root", "Root directory where a sandbox directory will be created, this sandbox is used in case WAL replay generates chunks (default is the database path). The sandbox is cleaned up at the end.", String);
    let min_time = || Flag::new("min-time", "Minimum timestamp to dump, in milliseconds since the Unix epoch.", Int64).default(&i64::MIN.to_string());
    let max_time = || Flag::new("max-time", "Maximum timestamp to dump, in milliseconds since the Unix epoch.", Int64).default(&i64::MAX.to_string());
    let dump_match = || Flag::new("match", "Series selector. Can be specified multiple times.", Strings).default("{__name__=~'(?s:.*)'}");
    let tsdb = Cmd::new("tsdb", "Run tsdb commands.")
        .cmd(
            Cmd::new("bench", "Run benchmarks.").cmd(
                Cmd::new("write", "Run a write performance benchmark.")
                    .flag(Flag::new("out", "Set the output path.", String).default("benchout"))
                    .flag(Flag::new("metrics", "Number of metrics to read.", Int).default("10000"))
                    .flag(Flag::new("scrapes", "Number of scrapes to simulate.", Int).default("3000"))
                    .arg(Arg::new("file", format!("Input file with samples data, default is ({bench_file})."), String).default(bench_file)),
            ),
        )
        .cmd(
            Cmd::new("analyze", "Analyze churn, label pair cardinality and compaction efficiency.")
                .arg(db_arg())
                .arg(Arg::new("block id", "Block to analyze (default is the last block).", String))
                .flag(Flag::new("limit", "How many items to show in each list.", Int).default("20"))
                .flag(Flag::new("extended", "Run extended analysis.", Bool))
                .flag(Flag::new("match", "Series selector to analyze. Only 1 set of matchers is supported now.", String)),
        )
        .cmd(
            Cmd::new("list", "List tsdb blocks.")
                .flag(Flag::new("human-readable", "Print human readable values.", Bool).short('r'))
                .arg(db_arg()),
        )
        .cmd(
            Cmd::new("dump", "Dump data (series+samples or optionally just series) from a TSDB.")
                .arg(db_arg())
                .flag(sandbox())
                .flag(min_time())
                .flag(max_time())
                .flag(dump_match())
                .flag(Flag::new("format", "Output format of the dump (prom (default) or seriesjson).", Enum(vec!["prom", "seriesjson"])).default("prom")),
        )
        .cmd(
            Cmd::new("dump-openmetrics", "[Experimental] Dump samples from a TSDB into OpenMetrics text format, excluding native histograms and staleness markers, which are not representable in OpenMetrics.")
                .arg(db_arg())
                .flag(sandbox())
                .flag(min_time())
                .flag(max_time())
                .flag(dump_match()),
        )
        .cmd(
            Cmd::new("create-blocks-from", "[Experimental] Import samples from input and produce TSDB blocks. Please refer to the storage docs for more details.")
                .flag(Flag::new("human-readable", "Print human readable values.", Bool).short('r'))
                .flag(Flag::new("quiet", "Do not print created blocks.", Bool).short('q'))
                .flag(Flag::new("max-block-duration", "Maximum duration created blocks may span. Anything less than 2h is ignored.", Duration).hidden().placeholder("<duration>"))
                .flag(Flag::new("allow-incompatible-block-duration", "Use --max-block-duration as is, instead of rounding it down to a Prometheus compaction range.", Bool).hidden())
                .cmd(
                    Cmd::new("openmetrics", "Import samples from OpenMetrics input and produce TSDB blocks. Please refer to the storage docs for more details.")
                        .flag(Flag::new("label", "Label to attach to metrics. Can be specified multiple times. Example --label=label_name=label_value", StringMap))
                        .arg(Arg::new("input file", "OpenMetrics file to read samples from.", String).required())
                        .arg(Arg::new("output directory", "Output directory for generated blocks.", String).default(default_db)),
                )
                .cmd(
                    Cmd::new("rules", "Create blocks of data for new recording rules.")
                        .flag(http_config())
                        .flag(Flag::new("url", "The URL for the Prometheus API with the data where the rule will be backfilled from.", Url).default("http://localhost:9090"))
                        .flag(Flag::new("start", "The time to start backfilling the new rule from. Must be a RFC3339 formatted date or Unix timestamp. Required.", String).required())
                        .flag(Flag::new("end", "If an end time is provided, all recording rules in the rule files provided will be backfilled to the end time. Default will backfill up to 3 hours ago. Must be a RFC3339 formatted date or Unix timestamp.", String))
                        .flag(Flag::new("output-dir", "Output directory for generated blocks.", String).default("data/"))
                        .flag(Flag::new("eval-interval", "How frequently to evaluate rules when backfilling if a value is not set in the recording rule files.", Duration).default("60s"))
                        .arg(Arg::new("rule-files", "A list of one or more files containing recording rules to be backfilled. All recording rules listed in the files will be backfilled. Alerting rules are not evaluated.", ExistingFiles).required()),
                ),
        );

    let promql_cmd = Cmd::new("promql", "PromQL formatting and editing. Requires the --experimental flag.")
        .cmd(Cmd::new("format", "Format PromQL query to pretty printed form.").arg(Arg::new("query", "PromQL query.", String).required()))
        .cmd(
            Cmd::new("label-matchers", "Edit label matchers contained within an existing PromQL query.")
                .cmd(
                    Cmd::new("set", "Set a label matcher in the query.")
                        .flag(Flag::new("type", "Type of the label matcher to set.", Enum(vec!["=", "!=", "=~", "!~"])).short('t').default("="))
                        .arg(Arg::new("query", "PromQL query.", String).required())
                        .arg(Arg::new("name", "Name of the label matcher to set.", String).required())
                        .arg(Arg::new("value", "Value of the label matcher to set.", String).required()),
                )
                .cmd(
                    Cmd::new("delete", "Delete a label from the query.")
                        .arg(Arg::new("query", "PromQL query.", String).required())
                        .arg(Arg::new("name", "Name of the label to delete.", String).required()),
                ),
        );

    let flags = vec![
        Flag::new("experimental", "Enable experimental commands.", Bool),
        Flag::new("enable-feature", "Comma separated feature names to enable. Valid options: promql-experimental-functions, promql-delayed-name-removal, promql-unquoted-utf8-names. See https://prometheus.io/docs/prometheus/latest/feature_flags/ for more details", Strings).default(""),
    ];
    let documentation = Cmd::new("write-documentation", "Generate command line documentation. Internal use.").hidden();
    App::new(
        name,
        "Tooling for the Prometheus monitoring system.",
        &version_string(),
        flags,
        vec![check, query, debug, push, test, tsdb, promql_cmd, documentation],
    )
}

/// Runs promtool with the process streams. `args[0]` is the program name.
pub fn run(args: Vec<String>) -> i32 {
    let stdin = std::io::stdin();
    let mut stdin = stdin.lock();
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    let stderr = std::io::stderr();
    let mut stderr = stderr.lock();
    let mut io = Io { stdin: &mut stdin, stdout: &mut stdout, stderr: &mut stderr };
    let code = run_with(&args, &mut io);
    let _ = io.stdout.flush();
    code
}

fn load_round_tripper(p: &Parsed, server_url: Option<&GoUrl>, err: &mut dyn Write, app: &App) -> Result<Arc<dyn net::RoundTripper>, i32> {
    let path = p.str("http.config.file");
    if path.is_empty() {
        return Ok(crate::api::default_round_tripper());
    }
    if let Some(u) = server_url {
        if u.user.as_ref().is_some_and(|ui| !ui.username.is_empty()) {
            let _ = writeln!(err, "{}: error: Cannot set base auth in the server URL and use a http.config.file at the same time", app.name);
            return Err(1);
        }
    }
    let cfg = match net::load_http_config_file(&path) {
        Ok((c, _)) => c,
        Err(e) => {
            let _ = writeln!(err, "{}: error: Failed to load HTTP config file: {e}", app.name);
            return Err(1);
        }
    };
    let opts = net::HttpClientOptions::default().with_user_agent("promtool/3.16.0");
    net::new_round_tripper_from_config(&cfg, &opts).map_err(|e| {
        let _ = writeln!(err, "{}: error: Failed to create a new HTTP round tripper: {e}", app.name);
        1
    })
}

fn parse_url_value(s: &str) -> GoUrl {
    yaml::url::parse(s).unwrap_or_default()
}

/// Runs promtool with the given streams.
pub fn run_with(args: &[String], io: &mut Io) -> i32 {
    let name = args
        .first()
        .map(|a| std::path::Path::new(a).file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default())
        .unwrap_or_else(|| "promtool".to_string());
    let app = build_app(&name);
    let parsed = match app.parse(&args[1.min(args.len())..], io.stdout, io.stderr) {
        Outcome::Run(p) => p,
        Outcome::Exit(c) => return c,
    };
    let cmd = parsed.command.clone();

    let printer = if parsed.str("format") == "json" && cmd.starts_with("query") { Printer::Json } else { Printer::Promql };

    // The server URL flag/argument of the selected command.
    let server_url_name = match cmd.as_str() {
        "check healthy" | "check ready" | "tsdb create-blocks-from rules" => Some("url"),
        "query instant" | "query range" | "query series" | "query labels" | "query analyze" => Some("server"),
        _ => None,
    };
    let server_url = server_url_name.map(|n| parse_url_value(&parsed.str(n)));
    let rt = match load_round_tripper(&parsed, server_url.as_ref(), io.stderr, &app) {
        Ok(rt) => rt,
        Err(c) => return c,
    };

    // Block duration of create-blocks-from (milliseconds).
    let max_block_duration = parsed.duration("max-block-duration");
    let mut block_duration_ns = max_block_duration;
    if cmd.starts_with("tsdb create-blocks-from") {
        let compatible = crate::pending::backfill::get_compatible_block_duration(max_block_duration / 1_000_000) * 1_000_000;
        if compatible != max_block_duration {
            if parsed.bool("allow-incompatible-block-duration") && max_block_duration / 1_000_000 > 0 {
                let logger = crate::promslog::stderr_logger();
                logger.info(
                    "Block duration does not match a default Prometheus compaction range.",
                    &[("block_duration", &scrape::util::go_duration_string_nanos(max_block_duration))],
                );
            } else {
                block_duration_ns = compatible;
            }
        }
    }

    let mut opts = promql::parser::Options::default();
    let mut delayed_name_removal = false;
    for f in parsed.strings("enable-feature") {
        for o in f.split(',') {
            match o {
                "promql-unquoted-utf8-names" => opts.enable_unquoted_utf8_names = true,
                "promql-experimental-functions" => opts.enable_experimental_functions = true,
                "promql-delayed-name-removal" => delayed_name_removal = true,
                "promql-duration-expr" | "promql-extended-range-selectors" => {}
                "promql-binop-fill-modifiers" => opts.enable_binop_fill_modifiers = true,
                "" => continue,
                _ => {
                    let _ = write!(io.stdout, "  WARNING: Unknown feature passed to --enable-feature: {o}");
                }
            }
        }
    }
    let p = Parser::new(opts);
    let logger = crate::promslog::stderr_logger();

    match cmd.as_str() {
        "check service-discovery" => crate::sd::check_sd(
            &parsed.str("config-file"),
            &parsed.str("job"),
            parsed.duration("timeout"),
            io.stdout,
            io.stderr,
        ),
        "check config" => {
            let ls = new_config_lint_config(
                &parsed.str("lint"),
                parsed.bool("lint-fatal"),
                parsed.bool("ignore-unknown-fields"),
                ValidationScheme::Utf8,
                parsed.duration("query.lookback-delta"),
                io.stdout,
            );
            check_config(parsed.bool("agent"), parsed.bool("syntax-only"), &ls, &p, &parsed.strings("config-files"), io)
        }
        "check healthy" | "check ready" => {
            let ep = if cmd == "check healthy" { CHECK_HEALTH } else { CHECK_READINESS };
            let mut u = server_url.unwrap_or_default();
            crate::check_err(check_server_status(&mut u, ep, rt, io.stderr), io.stderr)
        }
        "check web-config" => crate::web_config::check_web_config(&parsed.strings("web-config-files"), io),
        "check rules" => {
            let ls = new_rules_lint_config(
                &parsed.str("lint"),
                parsed.bool("lint-fatal"),
                parsed.bool("ignore-unknown-fields"),
                ValidationScheme::Utf8,
                io.stdout,
            );
            check_rules_cmd(&ls, &p, &parsed.strings("rule-files"), io)
        }
        "check metrics" => crate::check_metrics::check_metrics(parsed.bool("extended"), &parsed.str("lint"), io),
        "push metrics" => {
            let u = parse_url_value(&parsed.str("remote-write-url"));
            crate::metrics::push_metrics(
                &u,
                rt,
                &parsed.map("header"),
                std::time::Duration::from_nanos(parsed.duration("timeout").max(0) as u64),
                &parsed.str("protobuf_message"),
                &parsed.str("remote-write.path"),
                &parsed.map("label"),
                &parsed.strings("metric-files"),
                io,
            )
        }
        "query instant" => crate::query::query_instant(
            &server_url.unwrap_or_default(),
            rt,
            &parsed.map("header"),
            &parsed.str("expr"),
            &parsed.str("time"),
            printer,
            io.stdout,
            io.stderr,
        ),
        "query range" => crate::query::query_range(
            &server_url.unwrap_or_default(),
            rt,
            &parsed.map("header"),
            &parsed.str("expr"),
            &parsed.str("start"),
            &parsed.str("end"),
            parsed.duration("step"),
            printer,
            io.stdout,
            io.stderr,
        ),
        "query series" => crate::query::query_series(
            &server_url.unwrap_or_default(),
            rt,
            &parsed.strings("match"),
            &parsed.str("start"),
            &parsed.str("end"),
            printer,
            io.stdout,
            io.stderr,
        ),
        "debug pprof" => crate::debug::debug_pprof(&parsed.str("server"), io.stdout, io.stderr),
        "debug metrics" => crate::debug::debug_metrics(&parsed.str("server"), io.stdout, io.stderr),
        "debug all" => crate::debug::debug_all(&parsed.str("server"), io.stdout, io.stderr),
        "query labels" => crate::query::query_labels(
            &server_url.unwrap_or_default(),
            rt,
            &parsed.strings("match"),
            &parsed.str("name"),
            &parsed.str("start"),
            &parsed.str("end"),
            printer,
            io.stdout,
            io.stderr,
        ),
        "test rules" => {
            let junit = parsed.str("junit");
            let lazy_opts = promql::promqltest::LazyLoaderOpts {
                enable_at_modifier: true,
                enable_negative_offset: true,
                enable_delayed_name_removal: delayed_name_removal,
                ..Default::default()
            };
            let run = parsed.strings("run");
            let run_strings = if parsed.is_set("run") { Some(run.as_slice()) } else { None };
            let mut sink = std::io::sink();
            let mut junit_file = if junit.is_empty() {
                None
            } else {
                use std::os::unix::fs::OpenOptionsExt;
                std::fs::OpenOptions::new().write(true).create(true).truncate(true).mode(0o644).open(&junit).ok()
            };
            let results: &mut dyn Write = match junit_file.as_mut() {
                Some(f) => f,
                None => &mut sink,
            };
            crate::unittest::rules_unit_test_result(
                results,
                lazy_opts,
                p.clone(),
                run_strings,
                parsed.bool("diff"),
                parsed.bool("debug"),
                parsed.bool("ignore-unknown-fields"),
                &parsed.strings("test-rule-file"),
                io.stdout,
                io.stderr,
            )
        }
        "tsdb bench write" => crate::check_err(
            crate::pending::tsdb_cmd::benchmark_write(
                &parsed.str("out"),
                &parsed.str("file"),
                parsed.int("metrics") as i64,
                parsed.int("scrapes") as i64,
                io.stdout,
            ),
            io.stderr,
        ),
        "tsdb analyze" => crate::check_err(
            crate::pending::tsdb_cmd::analyze_block(
                &parsed.str("db path"),
                &parsed.str("block id"),
                parsed.int("limit") as i64,
                parsed.bool("extended"),
                &parsed.str("match"),
                &p,
                io.stdout,
            ),
            io.stderr,
        ),
        "tsdb list" => crate::check_err(
            crate::pending::tsdb_cmd::list_blocks(&parsed.str("db path"), parsed.bool("human-readable"), io.stdout),
            io.stderr,
        ),
        "tsdb dump" | "tsdb dump-openmetrics" => {
            let format = if cmd == "tsdb dump-openmetrics" {
                crate::pending::tsdb_cmd::SeriesSetFormatter::OpenMetrics
            } else if parsed.str("format") == "seriesjson" {
                crate::pending::tsdb_cmd::SeriesSetFormatter::LabelsToJson
            } else {
                crate::pending::tsdb_cmd::SeriesSetFormatter::SeriesSet
            };
            crate::check_err(
                crate::pending::tsdb_cmd::dump_tsdb_data(
                    &parsed.str("db path"),
                    &parsed.str("sandbox-dir-root"),
                    parsed.int64("min-time"),
                    parsed.int64("max-time"),
                    &parsed.strings("match"),
                    format,
                    &p,
                    io.stdout,
                ),
                io.stderr,
            )
        }
        "tsdb create-blocks-from openmetrics" => crate::pending::tsdb_cmd::backfill_open_metrics(
            &parsed.str("input file"),
            &parsed.str("output directory"),
            parsed.bool("human-readable"),
            parsed.bool("quiet"),
            block_duration_ns,
            &parsed.map("label"),
            io.stdout,
            io.stderr,
        ),
        "tsdb create-blocks-from rules" => crate::check_err(
            import_rules(
                &server_url.unwrap_or_default(),
                rt,
                &parsed.str("start"),
                &parsed.str("end"),
                &parsed.str("output-dir"),
                parsed.duration("eval-interval"),
                block_duration_ns,
                &parsed.strings("rule-files"),
                &logger,
                io.stderr,
            ),
            io.stderr,
        ),
        "query analyze" => {
            let cfg = crate::analyze::QueryAnalyzeConfig {
                metric_type: parsed.str("type"),
                duration: parsed.duration("duration"),
                time: parsed.str("time"),
                matchers: parsed.strings("match"),
            };
            crate::check_err(cfg.run(&server_url.unwrap_or_default(), rt, io.stdout), io.stderr)
        }
        "write-documentation" => {
            let _ = io.stdout.write_all(app.generate_markdown().as_bytes());
            SUCCESS_EXIT_CODE
        }
        "promql format" => {
            if let Some(c) = check_experimental(parsed.bool("experimental"), io.stderr) {
                return c;
            }
            crate::check_err(format_promql(&parsed.str("query"), &p, io.stdout), io.stderr)
        }
        "promql label-matchers set" => {
            if let Some(c) = check_experimental(parsed.bool("experimental"), io.stderr) {
                return c;
            }
            crate::check_err(
                labels_set_promql(&parsed.str("query"), &parsed.str("type"), &parsed.str("name"), &parsed.str("value"), &p, io.stdout),
                io.stderr,
            )
        }
        "promql label-matchers delete" => {
            if let Some(c) = check_experimental(parsed.bool("experimental"), io.stderr) {
                return c;
            }
            crate::check_err(labels_delete_promql(&parsed.str("query"), &parsed.str("name"), &p, io.stdout), io.stderr)
        }
        _ => SUCCESS_EXIT_CODE,
    }
}

/// Go `checkExperimental`: `Some(1)` when the flag is not set.
fn check_experimental(f: bool, err: &mut dyn Write) -> Option<i32> {
    if !f {
        let _ = writeln!(err, "This command is experimental and requires the --experimental flag to be set.");
        return Some(1);
    }
    None
}

/// Go `rulesLintConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RulesLintConfig {
    pub all: bool,
    pub duplicate_rules: bool,
    pub fatal: bool,
    pub ignore_unknown_fields: bool,
    pub name_validation_scheme: ValidationScheme,
}

/// Go `newRulesLintConfig`. Unknown options print a warning to `out` (Go: stdout).
pub fn new_rules_lint_config(
    string_val: &str,
    fatal: bool,
    ignore_unknown_fields: bool,
    scheme: ValidationScheme,
    out: &mut dyn Write,
) -> RulesLintConfig {
    let mut ls = RulesLintConfig { fatal, ignore_unknown_fields, name_validation_scheme: scheme, ..Default::default() };
    if string_val.is_empty() {
        return ls;
    }
    for setting in string_val.split(',') {
        match setting {
            LINT_OPTION_ALL => ls.all = true,
            LINT_OPTION_DUPLICATE_RULES => ls.duplicate_rules = true,
            LINT_OPTION_NONE => {}
            _ => {
                let _ = writeln!(out, "WARNING: unknown lint option: {}", promql::parser::quote::go_quote(setting));
            }
        }
    }
    ls
}

impl RulesLintConfig {
    pub fn lint_duplicate_rules(&self) -> bool {
        self.all || self.duplicate_rules
    }
}

/// Go `configLintConfig`. `lookback_delta` is in nanoseconds.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConfigLintConfig {
    pub rules: RulesLintConfig,
    pub lookback_delta: i64,
}

/// Go `newConfigLintConfig`.
pub fn new_config_lint_config(
    options_str: &str,
    fatal: bool,
    ignore_unknown_fields: bool,
    scheme: ValidationScheme,
    lookback_delta: i64,
    out: &mut dyn Write,
) -> ConfigLintConfig {
    let mut c = ConfigLintConfig { rules: RulesLintConfig { fatal, ..Default::default() }, lookback_delta: 0 };
    let mut lint_none = false;
    let mut rules_options: Vec<&str> = Vec::new();
    for option in options_str.split(',') {
        match option {
            LINT_OPTION_ALL | LINT_OPTION_TOO_LONG_SCRAPE_INTERVAL => {
                c.lookback_delta = lookback_delta;
                if option == LINT_OPTION_ALL {
                    rules_options.push(LINT_OPTION_ALL);
                }
            }
            LINT_OPTION_NONE => lint_none = true,
            _ => rules_options.push(option),
        }
    }
    if lint_none {
        c.lookback_delta = 0;
        rules_options.clear();
    }
    c.rules = new_rules_lint_config(&rules_options.join(","), fatal, ignore_unknown_fields, scheme, out);
    c
}

/// Go `CheckServerStatus`.
pub fn check_server_status(
    server_url: &mut GoUrl,
    check_endpoint: &str,
    rt: Arc<dyn net::RoundTripper>,
    err: &mut dyn Write,
) -> Result<(), crate::Error> {
    if server_url.scheme.is_empty() {
        server_url.scheme = "http".to_string();
    }
    let address = crate::api::url_join_path(server_url, &[check_endpoint]).to_url_string();
    if let Err(e) = yaml::url::parse(&address) {
        let _ = writeln!(err, "error creating API client: {e}");
        return Err(e.into());
    }
    let mut req = net::Request::get(&address)?;
    req.timeout = Some(std::time::Duration::from_secs(10));
    req.decompress = true;
    let resp = net::Client::new(rt).do_request(req)?;
    if resp.status != 200 {
        return Err(format!("check failed: URL={}, status={}", server_url.to_url_string(), resp.status).into());
    }
    let _ = writeln!(err, "  SUCCESS:  {}", String::from_utf8_lossy(&resp.body));
    Ok(())
}

/// Go `CheckConfig`.
pub fn check_config(
    agent_mode: bool,
    check_syntax_only: bool,
    lint_settings: &ConfigLintConfig,
    p: &Parser,
    files: &[String],
    io: &mut Io,
) -> i32 {
    let mut failed = false;
    let mut has_errors = false;
    let logger = crate::promslog::stderr_logger();
    for f in files {
        let (rule_files, scrape_configs) = match check_config_file(agent_mode, f, check_syntax_only, io.stdout) {
            Err(e) => {
                let _ = writeln!(io.stderr, "  FAILED: {e}");
                has_errors = true;
                failed = true;
                (Vec::new(), Vec::new())
            }
            Ok((rf, sc)) => {
                if !rf.is_empty() {
                    let _ = writeln!(io.stdout, "  SUCCESS: {} rule files found", rf.len());
                }
                let _ = writeln!(io.stdout, " SUCCESS: {f} is valid prometheus config file syntax");
                (rf, sc)
            }
        };
        let _ = writeln!(io.stdout);
        if !check_syntax_only {
            let scrape_failed = lint_scrape_configs(&scrape_configs, lint_settings, io.stderr);
            failed = failed || scrape_failed;
            let (rules_failed, rules_have_errors) = check_rules(&rule_files, &lint_settings.rules, p, &logger, io.stdout, io.stderr);
            failed = failed || rules_failed;
            has_errors = has_errors || rules_have_errors;
        }
    }
    if failed && has_errors {
        return FAILURE_EXIT_CODE;
    }
    if failed && lint_settings.rules.fatal {
        return LINT_ERR_EXIT_CODE;
    }
    SUCCESS_EXIT_CODE
}

/// Go `checkFileExists`.
fn check_file_exists(f: &str) -> Result<(), String> {
    if f.is_empty() {
        return Ok(());
    }
    std::fs::metadata(f).map(|_| ()).map_err(|e| format!("stat {f}: {}", crate::io_err_text(&e)))
}

/// Go `checkConfig`: loads a config file and checks the files it references. Prints "Checking <file>" and
/// warnings to `out`.
pub fn check_config_file(
    agent_mode: bool,
    filename: &str,
    check_syntax_only: bool,
    out: &mut dyn Write,
) -> Result<(Vec<String>, Vec<ScrapeConfig>), String> {
    let _ = writeln!(out, "Checking {filename}");
    let logger = crate::promslog::stderr_logger();
    let cfg = config::load_file(filename, agent_mode, &logger)?;

    let mut rule_files = Vec::new();
    if !check_syntax_only {
        for rf in cfg.rule_files.iter() {
            let rfs = discovery::glob::glob(rf)?;
            if !rf.contains('*') {
                if rfs.is_empty() {
                    return Err(format!("{} does not point to an existing file", go_q(rf)));
                }
                check_file_exists(&rfs[0]).map_err(|e| format!("error checking rule file {}: {e}", go_q(&rfs[0])))?;
            }
            rule_files.extend(rfs);
        }
    }

    let scfgs = if check_syntax_only {
        cfg.scrape_configs.clone()
    } else {
        cfg.get_scrape_configs().map_err(|e| format!("error loading scrape configs: {e}"))?
    };

    for scfg in scfgs.iter() {
        if !check_syntax_only {
            if let Some(a) = &scfg.http_client_config.authorization {
                check_file_exists(&a.credentials_file).map_err(|e| {
                    format!("error checking authorization credentials or bearer token file {}: {e}", go_q(&a.credentials_file))
                })?;
            }
        }
        check_tls_config(&scfg.http_client_config.tls_config, check_syntax_only)?;
        for c in scfg.service_discovery_configs.iter() {
            let any = c.as_any();
            if let Some(k) = any.downcast_ref::<discovery::kubernetes::SdConfig>() {
                check_tls_config(&k.http_client_config.tls_config, check_syntax_only)?;
            } else if let Some(fc) = any.downcast_ref::<discovery::file::SdConfig>() {
                if check_syntax_only {
                    continue;
                }
                for file in fc.files.iter() {
                    let files = discovery::glob::glob(file)?;
                    if !files.is_empty() {
                        for f in files {
                            let tgs = check_sd_file(&f).map_err(|e| format!("checking SD file {}: {e}", go_q(file)))?;
                            check_target_groups_for_scrape_config(&tgs, scfg)?;
                        }
                        continue;
                    }
                    let _ = writeln!(out, "  WARNING: file {} for file_sd in scrape job {} does not exist", go_q(file), go_q(&scfg.job_name));
                }
            } else if let Some(sc) = any.downcast_ref::<discovery::StaticConfig>() {
                check_target_groups_for_scrape_config(&sc.0, scfg)?;
            }
        }
    }

    for amcfg in cfg.alerting_config.alertmanager_configs.iter() {
        for c in amcfg.service_discovery_configs.iter() {
            let any = c.as_any();
            if let Some(fc) = any.downcast_ref::<discovery::file::SdConfig>() {
                if check_syntax_only {
                    continue;
                }
                for file in fc.files.iter() {
                    let files = discovery::glob::glob(file)?;
                    if !files.is_empty() {
                        for f in files {
                            let tgs = check_sd_file(&f).map_err(|e| format!("checking SD file {}: {e}", go_q(file)))?;
                            for tg in tgs.iter() {
                                notifier::alertmanager_from_group(tg, amcfg)?;
                            }
                        }
                        continue;
                    }
                    let _ = writeln!(out, "  WARNING: file {} for file_sd in alertmanager config does not exist", go_q(file));
                }
            } else if let Some(sc) = any.downcast_ref::<discovery::StaticConfig>() {
                for tg in sc.0.iter() {
                    notifier::alertmanager_from_group(tg, amcfg)?;
                }
            }
        }
    }
    Ok((rule_files, scfgs))
}

fn go_q(s: &str) -> String {
    promql::parser::quote::go_quote(s)
}

/// Go `checkTLSConfig`.
pub fn check_tls_config(tls: &net::TlsConfig, check_syntax_only: bool) -> Result<(), String> {
    if !tls.cert_file.is_empty() && tls.key_file.is_empty() {
        return Err(format!("client cert file {} specified without client key file", go_q(&tls.cert_file)));
    }
    if !tls.key_file.is_empty() && tls.cert_file.is_empty() {
        return Err(format!("client key file {} specified without client cert file", go_q(&tls.key_file)));
    }
    if check_syntax_only {
        return Ok(());
    }
    check_file_exists(&tls.cert_file).map_err(|e| format!("error checking client cert file {}: {e}", go_q(&tls.cert_file)))?;
    check_file_exists(&tls.key_file).map_err(|e| format!("error checking client key file {}: {e}", go_q(&tls.key_file)))?;
    Ok(())
}

/// Go `checkSDFile`.
pub fn check_sd_file(filename: &str) -> Result<Vec<Group>, String> {
    let content = std::fs::read(filename).map_err(|e| format!("open {filename}: {}", crate::io_err_text(&e)))?;
    let ext = std::path::Path::new(filename)
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .and_then(|n| n.rfind('.').map(|i| n[i..].to_string()))
        .unwrap_or_default();
    let groups: Vec<Option<Group>> = match ext.to_lowercase().as_str() {
        ".json" => Group::list_from_json(&content)?,
        ".yml" | ".yaml" => {
            yaml::unmarshal_strict::<Vec<Option<Group>>>(&String::from_utf8_lossy(&content)).map_err(|e| e.to_string())?
        }
        _ => return Err(format!("invalid file extension: {}", go_q(&ext))),
    };
    let mut out = Vec::with_capacity(groups.len());
    for (i, g) in groups.into_iter().enumerate() {
        match g {
            Some(g) => out.push(g),
            None => return Err(format!("nil target group item found (index {i})")),
        }
    }
    Ok(out)
}

fn check_target_groups_for_scrape_config(tgs: &[Group], scfg: &ScrapeConfig) -> Result<(), String> {
    let cfg = Arc::new(scfg.clone());
    let mut targets = Vec::new();
    let mut lb = model::labels::Builder::new(Labels::default());
    for tg in tgs {
        let failures = scrape::targets_from_group(tg, &cfg, &mut targets, &mut lb);
        if let Some(first) = failures.into_iter().next() {
            return Err(first.to_string());
        }
    }
    Ok(())
}

/// Go `CheckRules` (reads stdin when no files are given).
pub fn check_rules_cmd(ls: &RulesLintConfig, p: &Parser, files: &[String], io: &mut Io) -> i32 {
    let logger = crate::promslog::stderr_logger();
    let (failed, has_errors) = if files.is_empty() {
        check_rules_from_stdin(ls, p, &logger, io)
    } else {
        check_rules(files, ls, p, &logger, io.stdout, io.stderr)
    };
    if failed && has_errors {
        return FAILURE_EXIT_CODE;
    }
    if failed && ls.fatal {
        return LINT_ERR_EXIT_CODE;
    }
    SUCCESS_EXIT_CODE
}

/// An error of the rule checks: the text and whether it wraps Go's `errLint`.
struct RuleErr {
    text: String,
    lint: bool,
}

fn report_rule_groups(rgs: Option<RuleGroups>, errs: Vec<rules::rulefmt::BoxError>, ls: &RulesLintConfig, out: &mut dyn Write, err: &mut dyn Write) -> (bool, bool) {
    let mut failed = false;
    let mut has_errors = false;
    if !errs.is_empty() {
        failed = true;
        let _ = writeln!(err, "  FAILED:");
        for e in errs.iter() {
            let _ = writeln!(err, "{e}");
            has_errors = true;
        }
        if has_errors {
            return (failed, has_errors);
        }
    }
    let rgs = rgs.unwrap_or_default();
    match check_rule_groups(&rgs, ls) {
        Err(errs) => {
            let _ = writeln!(err, "  FAILED:");
            for e in errs.iter() {
                let _ = writeln!(err, "{}", e.text);
            }
            failed = true;
            for e in errs.iter() {
                has_errors = has_errors || !e.lint;
            }
        }
        Ok(n) => {
            let _ = writeln!(out, "  SUCCESS: {n} rules found");
        }
    }
    let _ = writeln!(out);
    (failed, has_errors)
}

/// Go `checkRulesFromStdin`.
pub fn check_rules_from_stdin(ls: &RulesLintConfig, p: &Parser, logger: &discovery::Logger, io: &mut Io) -> (bool, bool) {
    let _ = writeln!(io.stdout, "Checking standard input");
    let mut data = Vec::new();
    if let Err(e) = io.stdin.read_to_end(&mut data) {
        let _ = writeln!(io.stderr, "  FAILED: {e}");
        return (true, true);
    }
    let (rgs, errs) = rules::rulefmt::parse(&data, ls.ignore_unknown_fields, ls.name_validation_scheme, p, logger);
    report_rule_groups(rgs, errs, ls, io.stdout, io.stderr)
}

/// Go `checkRules`: validates rule files; returns (failed, hasErrors).
pub fn check_rules(
    files: &[String],
    ls: &RulesLintConfig,
    p: &Parser,
    logger: &discovery::Logger,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> (bool, bool) {
    let mut failed = false;
    let mut has_errors = false;
    for f in files {
        let _ = writeln!(out, "Checking {f}");
        let (rgs, errs) = rules::rulefmt::parse_file(f, ls.ignore_unknown_fields, ls.name_validation_scheme, p, logger);
        if !errs.is_empty() {
            failed = true;
            let _ = writeln!(err, "  FAILED:");
            for e in errs.iter() {
                let _ = writeln!(err, "{e}");
                has_errors = true;
            }
            continue;
        }
        let (fa, he) = report_rule_groups(rgs, Vec::new(), ls, out, err);
        failed = failed || fa;
        has_errors = has_errors || he;
    }
    (failed, has_errors)
}

/// Go `checkRuleGroups`: the number of rules, or the lint errors.
fn check_rule_groups(rgs: &RuleGroups, ls: &RulesLintConfig) -> Result<usize, Vec<RuleErr>> {
    let num_rules: usize = rgs.groups.iter().map(|g| g.rules.len()).sum();
    if ls.lint_duplicate_rules() {
        let d = check_duplicates(&rgs.groups);
        if !d.is_empty() {
            let mut msg = format!("{} duplicate rule(s) found.\n", d.len());
            for n in d.iter() {
                msg.push_str(&format!("Metric: {}\nLabel(s):\n", n.metric));
                n.label.range(|ln, lv| msg.push_str(&format!("\t{ln}: {lv}\n")));
            }
            msg.push_str("Might cause inconsistency while recording expressions");
            return Err(vec![RuleErr { text: format!("lint error {msg}"), lint: true }]);
        }
    }
    Ok(num_rules)
}

/// Go `lintScrapeConfigs`.
pub fn lint_scrape_configs(scrape_configs: &[ScrapeConfig], ls: &ConfigLintConfig, err: &mut dyn Write) -> bool {
    for scfg in scrape_configs {
        if ls.lookback_delta > 0 && scfg.scrape_interval.0 >= ls.lookback_delta {
            let _ = writeln!(
                err,
                "  FAILED: too long scrape interval found, data point will be marked as stale - job: {}, interval: {}",
                scfg.job_name,
                scfg.scrape_interval.go_string()
            );
            return true;
        }
    }
    false
}

/// Go `compareRuleType`.
#[derive(Clone, Debug, PartialEq)]
pub struct CompareRuleType {
    pub metric: String,
    pub label: Labels,
}

fn compare(a: &CompareRuleType, b: &CompareRuleType) -> std::cmp::Ordering {
    a.metric.cmp(&b.metric).then_with(|| model::labels::compare(&a.label, &b.label).cmp(&0))
}

/// Go `checkDuplicates`.
pub fn check_duplicates(groups: &[RuleGroup]) -> Vec<CompareRuleType> {
    let mut duplicates: Vec<CompareRuleType> = Vec::new();
    let mut c_rules: Vec<CompareRuleType> = Vec::new();
    for g in groups {
        for r in g.rules.iter() {
            c_rules.push(CompareRuleType {
                metric: if !r.alert.is_empty() { r.alert.clone() } else { r.record.clone() },
                label: rules::from_maps(&[&g.labels, &r.labels]),
            });
        }
    }
    if c_rules.len() < 2 {
        return duplicates;
    }
    c_rules.sort_by(compare);
    let mut last = 0;
    for i in 1..c_rules.len() {
        if compare(&c_rules[last], &c_rules[i]).is_eq()
            && (duplicates.is_empty() || !compare(&c_rules[last], duplicates.last().unwrap()).is_eq())
        {
            duplicates.push(c_rules[i].clone());
        }
        last = i;
    }
    duplicates
}

/// Go `importRules`. `eval_interval` and `block_duration` are in nanoseconds.
#[allow(clippy::too_many_arguments)]
pub fn import_rules(
    url: &GoUrl,
    rt: Arc<dyn net::RoundTripper>,
    start: &str,
    end: &str,
    output_dir: &str,
    eval_interval: i64,
    block_duration: i64,
    files: &[String],
    logger: &discovery::Logger,
    err: &mut dyn Write,
) -> Result<(), crate::Error> {
    let mut u = url.clone();
    let api = crate::api::new_api(&mut u, rt, &BTreeMap::new()).map_err(|e| format!("new api client error: {e}"))?;
    let d = |n: i64| std::time::Duration::from_nanos(n.max(0) as u64);
    crate::rules_cmd::import_rules(
        logger.clone(),
        Box::new(api),
        start,
        end,
        output_dir,
        d(eval_interval),
        d(block_duration),
        ValidationScheme::Utf8,
        files,
        err,
    )
}

/// Go `formatPromQL`.
pub fn format_promql(query: &str, p: &Parser, out: &mut dyn Write) -> Result<(), crate::Error> {
    let expr = p.parse_expr(query)?;
    writeln!(out, "{}", expr.pretty(0))?;
    Ok(())
}

/// Calls `f` for every vector selector of the expression (the nodes Go's `parser.Inspect` visits).
fn for_each_vector_selector(e: &mut Expr, f: &mut dyn FnMut(&mut promql::parser::VectorSelector)) {
    match e {
        Expr::VectorSelector(vs) => f(vs),
        Expr::Aggregate(n) => {
            if let Some(x) = n.expr.as_mut() {
                for_each_vector_selector(x, f);
            }
            if let Some(x) = n.param.as_mut() {
                for_each_vector_selector(x, f);
            }
        }
        Expr::Binary(n) => {
            for_each_vector_selector(&mut n.lhs, f);
            for_each_vector_selector(&mut n.rhs, f);
        }
        Expr::Call(n) => {
            for a in n.args.iter_mut() {
                for_each_vector_selector(a, f);
            }
        }
        Expr::Subquery(n) => for_each_vector_selector(&mut n.expr, f),
        Expr::Paren(n) => for_each_vector_selector(&mut n.expr, f),
        Expr::Unary(n) => for_each_vector_selector(&mut n.expr, f),
        Expr::MatrixSelector(n) => for_each_vector_selector(&mut n.vector_selector, f),
        Expr::StepInvariant(n) => for_each_vector_selector(&mut n.expr, f),
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Duration(_) => {}
    }
}

/// Go `labelsSetPromQL`.
pub fn labels_set_promql(query: &str, label_match_type: &str, name: &str, value: &str, p: &Parser, out: &mut dyn Write) -> Result<(), crate::Error> {
    let mut expr = p.parse_expr(query)?;
    let match_type = match label_match_type {
        "=" => MatchType::Equal,
        "!=" => MatchType::NotEqual,
        "=~" => MatchType::Regexp,
        "!~" => MatchType::NotRegexp,
        _ => return Err(format!("invalid label match type: {label_match_type}").into()),
    };
    for_each_vector_selector(&mut expr, &mut |n| {
        let mut found = false;
        for m in n.label_matchers.iter_mut() {
            if m.name == name {
                m.typ = match_type;
                m.value = value.to_string();
                found = true;
            }
        }
        if !found {
            // Go builds the matcher without compiling the regexp; only its text is printed.
            let m = match Matcher::new(match_type, name, value) {
                Ok(m) => m,
                Err(_) => {
                    let mut m = Matcher::must_new(MatchType::Equal, name, value);
                    m.typ = match_type;
                    m
                }
            };
            n.label_matchers.push(m);
        }
    });
    writeln!(out, "{}", expr.pretty(0))?;
    Ok(())
}

/// Go `labelsDeletePromQL`. Go deletes from the slice while ranging over it, so a matcher right after a deleted
/// one is skipped, and a second delete past the shortened length panics; both are reproduced.
pub fn labels_delete_promql(query: &str, name: &str, p: &Parser, out: &mut dyn Write) -> Result<(), crate::Error> {
    let mut expr = p.parse_expr(query)?;
    let mut panic_msg: Option<String> = None;
    for_each_vector_selector(&mut expr, &mut |n| {
        if panic_msg.is_some() {
            return;
        }
        let mut backing = n.label_matchers.clone();
        let orig_len = backing.len();
        let mut cur_len = orig_len;
        for i in 0..orig_len {
            if backing[i].name == name {
                if i + 1 > cur_len {
                    panic_msg = Some(format!("panic: runtime error: slice bounds out of range [{}:{}]", i + 1, cur_len));
                    return;
                }
                for j in i..cur_len - 1 {
                    backing[j] = backing[j + 1].clone();
                }
                cur_len -= 1;
            }
        }
        backing.truncate(cur_len);
        n.label_matchers = backing;
    });
    if let Some(m) = panic_msg {
        let _ = std::io::stderr().write_all(format!("{m}\n").as_bytes());
        std::process::exit(2);
    }
    writeln!(out, "{}", expr.pretty(0))?;
    Ok(())
}
