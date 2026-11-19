//! Temporary stand-ins for the helper modules until they are merged (removed at integration).
#![allow(unused_variables, clippy::too_many_arguments)]

use std::collections::BTreeMap;
use std::io::Write;

fn missing() -> crate::Error {
    "not integrated yet".into()
}

pub mod backfill {
    pub fn get_compatible_block_duration(ms: i64) -> i64 {
        ms
    }
}

pub mod tsdb_cmd {
    use super::*;
    pub enum SeriesSetFormatter {
        SeriesSet,
        LabelsToJson,
        OpenMetrics,
    }
    pub fn benchmark_write(out: &str, file: &str, m: i64, s: i64, o: &mut dyn Write) -> Result<(), crate::Error> {
        Err(missing())
    }
    pub fn analyze_block(p: &str, b: &str, l: i64, e: bool, m: &str, pr: &promql::parser::Parser, o: &mut dyn Write) -> Result<(), crate::Error> {
        Err(missing())
    }
    pub fn list_blocks(p: &str, h: bool, o: &mut dyn Write) -> Result<(), crate::Error> {
        Err(missing())
    }
    pub fn dump_tsdb_data(p: &str, s: &str, a: i64, b: i64, m: &[String], f: SeriesSetFormatter, pr: &promql::parser::Parser, o: &mut dyn Write) -> Result<(), crate::Error> {
        Err(missing())
    }
    pub fn backfill_open_metrics(p: &str, o: &str, h: bool, q: bool, d: i64, l: &BTreeMap<String, String>, out: &mut dyn Write, err: &mut dyn Write) -> i32 {
        1
    }
}

pub mod unittest {
    use super::*;
    pub fn run_from_cli(junit: &str, o: promql::promqltest::LazyLoaderOpts, p: &promql::parser::Parser, run: &[String], diff: bool, debug: bool, ign: bool, files: &[String], io: &mut crate::Io) -> i32 {
        1
    }
}

pub mod rules_cmd {
    use super::*;
    pub struct RuleImporterConfig {
        pub output_dir: String,
        pub start: crate::api::Time,
        pub end: crate::api::Time,
        pub eval_interval: i64,
        pub block_duration: i64,
        pub name_validation_scheme: model::labels::ValidationScheme,
    }
    pub struct RuleImporter;
    impl RuleImporter {
        pub fn new(l: discovery::Logger, c: RuleImporterConfig, a: Box<dyn crate::api::QueryRangeApi>) -> RuleImporter {
            RuleImporter
        }
        pub fn load_groups(&mut self, f: &[String]) -> Vec<crate::Error> {
            vec![missing()]
        }
        pub fn import_all(&mut self) -> Vec<crate::Error> {
            Vec::new()
        }
    }
}

pub mod check_metrics {
    pub fn check_metrics(e: bool, l: &str, io: &mut crate::Io) -> i32 {
        1
    }
}

pub mod metrics {
    use super::*;
    pub fn push_metrics(u: &yaml::url::GoUrl, rt: std::sync::Arc<dyn net::RoundTripper>, h: &BTreeMap<String, String>, t: i64, pm: &str, ap: &str, l: &BTreeMap<String, String>, f: &[String], io: &mut crate::Io) -> i32 {
        1
    }
}

pub mod web_config {
    pub fn check_web_config(f: &[String], io: &mut crate::Io) -> i32 {
        1
    }
}
