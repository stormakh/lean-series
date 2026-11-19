//! Port of cmd/promtool/rules.go: the rule importer of `promtool tsdb create-blocks-from rules`, which evaluates
//! recording rules through the HTTP API (`query_range`) and writes the results to new TSDB blocks.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use model::labels::{self, Labels, ValidationScheme};
use rules::log::Logger;
use rules::{Group, Manager, ManagerOptions};
use storage::{AppendV2Options, AppenderV2, Context};
use tsdb::blockwriter::{new_block_writer, BlockWriter};

use crate::api::{QueryRangeApi, Range, Time};
use crate::client_model::Value;
use crate::Error;

const MAX_SAMPLES_IN_MEMORY: usize = 5000;

/// Go `ruleImporterConfig`.
#[derive(Clone, Debug)]
pub struct RuleImporterConfig {
    pub output_dir: String,
    pub start: Time,
    pub end: Time,
    pub eval_interval: Duration,
    pub block_duration: Duration,
    pub name_validation_scheme: ValidationScheme,
}

/// Go `ruleImporter`.
pub struct RuleImporter {
    logger: Logger,
    config: RuleImporterConfig,
    api_client: Box<dyn QueryRangeApi>,
    /// The loaded groups by group key (`file;name`).
    pub groups: HashMap<String, Arc<Group>>,
    rule_manager: Manager,
}

/// Go `newRuleImporter`: a rule importer that parses and evaluates recording rule files and writes the new series
/// to disk in blocks.
pub fn new_rule_importer(logger: Logger, config: RuleImporterConfig, api_client: Box<dyn QueryRangeApi>) -> RuleImporter {
    logger.info(
        "new rule importer",
        &[("component", "backfiller"), ("start", &format_rfc822(config.start)), ("end", &format_rfc822(config.end))],
    );
    let rule_manager =
        Manager::new(ManagerOptions { name_validation_scheme: config.name_validation_scheme, ..ManagerOptions::default() });
    RuleImporter { logger, config, api_client, groups: HashMap::new(), rule_manager }
}

impl RuleImporter {
    /// Go `loadGroups`: parses groups from a list of recording rule files.
    pub fn load_groups(&mut self, filenames: &[String]) -> Vec<Error> {
        match self.rule_manager.load_groups(self.config.eval_interval, &Labels::empty(), "", None, false, filenames) {
            Ok(groups) => {
                self.groups = groups;
                Vec::new()
            }
            Err(errs) => errs,
        }
    }

    /// Go `importAll`. The block duration is used as is; it is not validated against the durations that
    /// Prometheus supports.
    pub fn import_all(&self) -> Vec<Error> {
        let mut errs = Vec::new();
        let ctx = Context::background();
        for (name, group) in &self.groups {
            self.logger.info("processing group", &[("component", "backfiller"), ("name", name)]);

            for (i, r) in group.all_rules().iter().enumerate() {
                self.logger.info(
                    "processing rule",
                    &[("component", "backfiller"), ("id", &i.to_string()), ("name", r.name())],
                );
                let expr = r.query().map(|q| q.to_string()).unwrap_or_default();
                if let Err(err) = self.import_rule(
                    &ctx,
                    &expr,
                    r.name(),
                    r.labels(),
                    self.config.start,
                    self.config.end,
                    (self.config.block_duration.as_nanos() / 1_000_000) as i64,
                    group,
                ) {
                    errs.push(err);
                }
            }
        }
        errs
    }

    /// Go `importRule`. `block_duration` is in milliseconds.
    #[allow(clippy::too_many_arguments)]
    pub fn import_rule(
        &self,
        ctx: &Context,
        rule_expr: &str,
        rule_name: &str,
        rule_labels: &Labels,
        start: Time,
        end: Time,
        block_duration: i64,
        grp: &Group,
    ) -> Result<(), Error> {
        let start_in_ms = start.unix() * 1000;
        let end_in_ms = end.unix() * 1000;

        let mut start_of_block = block_duration * (start_in_ms / block_duration);
        while start_of_block <= end_in_ms {
            let end_of_block = start_of_block + block_duration - 1;

            let curr_start = (start_of_block / 1000).max(start.unix());
            let mut start_with_alignment = grp.eval_timestamp(curr_start * 1_000_000_000);
            while start_with_alignment.unix_secs() < curr_start {
                start_with_alignment = start_with_alignment.add(grp.interval());
            }
            let block_end = Time::from_unix((end_of_block / 1000).min(end.unix()), 0);
            let aligned = Time(start_with_alignment.unix_nano());
            if block_end < aligned {
                break;
            }
            let (val, warnings, infos) = self
                .api_client
                .query_range(rule_expr, Range { start: aligned, end: block_end, step: grp.interval() })
                .map_err(|e| -> Error { format!("query range: {e}").into() })?;
            // adapted: Go logs when the slices are non-nil; the trait gives no nil, so this logs when non-empty.
            if !warnings.is_empty() {
                self.logger.warn("Range query returned warnings.", &[("warnings", &go_strings(&warnings))]);
            }
            if !infos.is_empty() {
                self.logger.info("Range query returned infos.", &[("infos", &go_strings(&infos))]);
            }

            // To prevent races with compaction, a block writer only allows appending samples
            // that are at most half a block size older than the most recent sample appended so far.
            // However, in the way we use the block writer here, compaction doesn't happen, while we
            // also need to append samples throughout the whole block range. To allow that, we
            // pretend that the block is twice as large here, but only really add sample in the
            // original interval later.
            let w = new_block_writer(tsdb::Logger::default(), Path::new(&self.config.output_dir), 2 * block_duration)
                .map_err(|e| -> Error { format!("new block writer: {e}").into() })?;
            let r = write_block(ctx, &w, &val, rule_labels, rule_name);
            // Go: the error of Close is joined into a variable that is not returned, so it is dropped.
            let _ = w.close();
            r?;
            start_of_block += block_duration;
        }

        Ok(())
    }
}

/// The body of the block loop of Go `importRule` after the block writer is made.
fn write_block(ctx: &Context, w: &BlockWriter, val: &Value, rule_labels: &Labels, rule_name: &str) -> Result<(), Error> {
    let mut app = new_multiple_appender(ctx, w);
    match val {
        Value::Matrix(matrix) => {
            for sample in matrix {
                let mut lb = labels::Builder::new(Labels::empty());

                for (name, value) in &sample.metric {
                    lb.set(name, value);
                }

                // Setting the rule labels after the output of the query,
                // so they can override query output.
                rule_labels.range(|n, v| {
                    lb.set(n, v);
                });

                lb.set(labels::METRIC_NAME, rule_name);
                let lbls = lb.labels();

                for value in &sample.values {
                    app.add(ctx, &lbls, value.timestamp, value.value).map_err(|e| -> Error { format!("add: {e}").into() })?;
                }
            }
        }
        other => return Err(format!("rule result is wrong type {}", other.type_name()).into()),
    }

    app.flush_and_commit(ctx).map_err(|e| -> Error { format!("flush and commit: {e}").into() })
}

/// Go `multipleAppender`: keeps track of how many samples have been added to the current appender. If the max
/// samples have been added, then all series are committed and a new appender is created.
struct MultipleAppender<'a> {
    max_samples_in_memory: usize,
    current_sample_count: usize,
    writer: &'a BlockWriter,
    appender: Box<dyn AppenderV2>,
}

fn new_multiple_appender<'a>(ctx: &Context, block_writer: &'a BlockWriter) -> MultipleAppender<'a> {
    MultipleAppender {
        max_samples_in_memory: MAX_SAMPLES_IN_MEMORY,
        current_sample_count: 0,
        writer: block_writer,
        appender: block_writer.appender_v2(ctx),
    }
}

impl MultipleAppender<'_> {
    fn add(&mut self, ctx: &Context, l: &Labels, t: i64, v: f64) -> Result<(), String> {
        if let Err(err) = self.appender.append(0, l, 0, t, v, None, None, &AppendV2Options::default()) {
            return Err(format!("multiappender append: {err}"));
        }
        self.current_sample_count += 1;
        if self.current_sample_count >= self.max_samples_in_memory {
            return self.commit(ctx);
        }
        Ok(())
    }

    fn commit(&mut self, ctx: &Context) -> Result<(), String> {
        if self.current_sample_count == 0 {
            return Ok(());
        }
        if let Some(err) = self.appender.commit() {
            return Err(format!("multiappender commit: {err}"));
        }
        self.appender = self.writer.appender_v2(ctx);
        self.current_sample_count = 0;
        Ok(())
    }

    fn flush_and_commit(&mut self, ctx: &Context) -> Result<(), String> {
        self.commit(ctx)?;
        if let Err(err) = self.writer.flush(ctx) {
            return Err(format!("multiappender flush: {err}"));
        }
        Ok(())
    }
}

/// Go `importRules` of main.go without the API client construction: the caller makes the client from `--url`
/// and `--http.config.file` (Go `newAPI`) and passes it. `start` and `end` are the flag texts (`end` may be
/// empty); `stderr` gets the per-rule import errors.
#[allow(clippy::too_many_arguments)]
pub fn import_rules(
    logger: Logger,
    api_client: Box<dyn QueryRangeApi>,
    start: &str,
    end: &str,
    output_dir: &str,
    eval_interval: Duration,
    block_duration: Duration,
    name_validation_scheme: ValidationScheme,
    files: &[String],
    stderr: &mut dyn std::io::Write,
) -> Result<(), Error> {
    let etime = if end.is_empty() {
        Time::now().sub_dur(Duration::from_secs(3 * 3600))
    } else {
        Time(crate::unittest::parse_time(end).map_err(|e| -> Error { format!("error parsing end time: {e}").into() })?)
    };

    let stime =
        Time(crate::unittest::parse_time(start).map_err(|e| -> Error { format!("error parsing start time: {e}").into() })?);

    if stime >= etime {
        return Err("start time is not before end time".into());
    }

    let cfg = RuleImporterConfig {
        output_dir: output_dir.to_string(),
        start: stime,
        end: etime,
        eval_interval,
        block_duration,
        name_validation_scheme,
    };

    let mut rule_importer = new_rule_importer(logger, cfg, api_client);
    let errs = rule_importer.load_groups(files);
    if let Some(err) = errs.into_iter().next() {
        return Err(format!("rule importer parse error: {err}").into());
    }

    let errs = rule_importer.import_all();
    for err in &errs {
        let _ = writeln!(stderr, "rule importer error: {err}");
    }
    if !errs.is_empty() {
        return Err("error importing rules".into());
    }

    Ok(())
}

/// Go `%v` of a `[]string`.
fn go_strings(v: &[String]) -> String {
    format!("[{}]", v.join(" "))
}

/// Go `t.Format(time.RFC822)` ("02 Jan 06 15:04 MST") of a UTC time.
fn format_rfc822(t: Time) -> String {
    const MONTHS: [&str; 12] = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let secs = t.unix();
    let z = secs.div_euclid(86400) + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    let rem = secs.rem_euclid(86400);
    format!("{d:02} {} {:02} {:02}:{:02} UTC", MONTHS[(m - 1) as usize], y.rem_euclid(100), rem / 3600, rem % 3600 / 60)
}
