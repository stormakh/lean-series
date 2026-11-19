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

