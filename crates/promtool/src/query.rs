//! Port of cmd/promtool/query.go (`promtool query instant|range|series|labels`) and the printers of main.go.

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;

use yaml::url::GoUrl;

use crate::api::{self, ApiError, Range, Time};
use crate::client_model::{self, LabelSet, Value};
use crate::{FAILURE_EXIT_CODE, SUCCESS_EXIT_CODE};

/// Go `printer` (main.go): `promqlPrinter` or `jsonPrinter`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Printer {
    Promql,
    Json,
}

impl Printer {
    pub fn print_value(&self, v: &Value, out: &mut dyn Write) {
        let _ = match self {
            Printer::Promql => writeln!(out, "{v}"),
            Printer::Json => writeln!(out, "{}", client_model::value_to_json(v)),
        };
    }
    pub fn print_series(&self, v: &[LabelSet], out: &mut dyn Write) {
        match self {
            Printer::Promql => {
                for s in v {
                    let _ = writeln!(out, "{}", client_model::labelset_string(s));
                }
            }
            Printer::Json => {
                let _ = writeln!(out, "{}", client_model::labelsets_to_json(v));
            }
        }
    }
    pub fn print_label_values(&self, v: &[String], out: &mut dyn Write) {
        match self {
            Printer::Promql => {
                for s in v {
                    let _ = writeln!(out, "{s}");
                }
            }
            Printer::Json => {
                let _ = writeln!(out, "{}", client_model::strings_to_json(v));
            }
        }
    }
}

type Headers = BTreeMap<String, String>;

/// Go `QueryInstant`.
pub fn query_instant(
    url: &GoUrl,
    rt: Arc<dyn net::RoundTripper>,
    headers: &Headers,
    query: &str,
    eval_time: &str,
    p: Printer,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> i32 {
    let mut url = url.clone();
    let api = match api::new_api(&mut url, rt, headers) {
        Ok(a) => a,
        Err(e) => {
            let _ = writeln!(err, "error creating API client: {e}");
            return FAILURE_EXIT_CODE;
        }
    };
    let mut e_time = Time::now();
    if !eval_time.is_empty() {
        match parse_time(eval_time) {
            Ok(t) => e_time = t,
            Err(e) => {
                let _ = writeln!(err, "error parsing evaluation time: {e}");
                return FAILURE_EXIT_CODE;
            }
        }
    }
    match api.query(query, Some(e_time)) {
        Ok((v, _, _)) => {
            p.print_value(&v, out);
            SUCCESS_EXIT_CODE
        }
        Err(e) => handle_api_error(&e, err),
    }
}

/// Go `QueryRange`. `step` is in nanoseconds.
#[allow(clippy::too_many_arguments)]
pub fn query_range(
    url: &GoUrl,
    rt: Arc<dyn net::RoundTripper>,
    headers: &Headers,
    query: &str,
    start: &str,
    end: &str,
    step: i64,
    p: Printer,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> i32 {
    let mut url = url.clone();
    let api = match api::new_api(&mut url, rt, headers) {
        Ok(a) => a,
        Err(e) => {
            let _ = writeln!(err, "error creating API client: {e}");
            return FAILURE_EXIT_CODE;
        }
    };
    let etime = if end.is_empty() {
        Time::now()
    } else {
        match parse_time(end) {
            Ok(t) => t,
            Err(e) => {
                let _ = writeln!(err, "error parsing end time: {e}");
                return FAILURE_EXIT_CODE;
            }
        }
    };
    let stime = if start.is_empty() {
        etime.add_nanos(-5 * 60 * 1_000_000_000)
    } else {
        match parse_time(start) {
            Ok(t) => t,
            Err(e) => {
                let _ = writeln!(err, "error parsing start time: {e}");
                return FAILURE_EXIT_CODE;
            }
        }
    };
    if stime >= etime {
        let _ = writeln!(err, "start time is not before end time");
        return FAILURE_EXIT_CODE;
    }
    let mut step = step;
    if step == 0 {
        let secs = (etime.0 - stime.0) as f64 / 1e9;
        let resolution = (secs / 250.0).floor().max(1.0);
        step = resolution as i64 * 1_000_000_000;
    }
    let r = Range { start: stime, end: etime, step: std::time::Duration::from_nanos(step.max(0) as u64) };
    match api.query_range_full(query, r) {
        Ok((v, _, _)) => {
            p.print_value(&v, out);
            SUCCESS_EXIT_CODE
        }
        Err(e) => handle_api_error(&e, err),
    }
}

/// Go `QuerySeries`.
pub fn query_series(
    url: &GoUrl,
    rt: Arc<dyn net::RoundTripper>,
    matchers: &[String],
    start: &str,
    end: &str,
    p: Printer,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> i32 {
    let mut url = url.clone();
    let api = match api::new_api(&mut url, rt, &Headers::new()) {
        Ok(a) => a,
        Err(e) => {
            let _ = writeln!(err, "error creating API client: {e}");
            return FAILURE_EXIT_CODE;
        }
    };
    let (stime, etime) = match parse_start_time_and_end_time(start, end) {
        Ok(t) => t,
        Err(e) => {
            let _ = writeln!(err, "{e}");
            return FAILURE_EXIT_CODE;
        }
    };
    match api.series(matchers, Some(stime), Some(etime)) {
        Ok((v, _)) => {
            p.print_series(&v, out);
            SUCCESS_EXIT_CODE
        }
        Err(e) => handle_api_error(&e, err),
    }
}

/// Go `QueryLabels`.
#[allow(clippy::too_many_arguments)]
pub fn query_labels(
    url: &GoUrl,
    rt: Arc<dyn net::RoundTripper>,
    matchers: &[String],
    name: &str,
    start: &str,
    end: &str,
    p: Printer,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> i32 {
    let mut url = url.clone();
    let api = match api::new_api(&mut url, rt, &Headers::new()) {
        Ok(a) => a,
        Err(e) => {
            let _ = writeln!(err, "error creating API client: {e}");
            return FAILURE_EXIT_CODE;
        }
    };
    let (stime, etime) = match parse_start_time_and_end_time(start, end) {
        Ok(t) => t,
        Err(e) => {
            let _ = writeln!(err, "{e}");
            return FAILURE_EXIT_CODE;
        }
    };
    let (val, warn, r) = api.label_values(name, matchers, Some(stime), Some(etime));
    for w in warn {
        let _ = writeln!(err, "query warning: {w}");
    }
    if let Err(e) = r {
        return handle_api_error(&e, err);
    }
    p.print_label_values(&val, out);
    SUCCESS_EXIT_CODE
}

/// Go `handleAPIError`.
pub fn handle_api_error(e: &crate::Error, err: &mut dyn Write) -> i32 {
    match e.downcast_ref::<ApiError>() {
        Some(a) if !a.detail.is_empty() => {
            let _ = writeln!(err, "query error: {a} (detail: {})", a.detail.trim());
        }
        _ => {
            let _ = writeln!(err, "query error: {e}");
        }
    }
    FAILURE_EXIT_CODE
}

/// Go `parseStartTimeAndEndTime`.
pub fn parse_start_time_and_end_time(start: &str, end: &str) -> Result<(Time, Time), String> {
    const H9999: i64 = 9999 * 3600 * 1_000_000_000;
    let now = Time::now();
    let mut stime = now.add_nanos(-H9999);
    let mut etime = now.add_nanos(H9999);
    if !start.is_empty() {
        stime = parse_time(start).map_err(|e| format!("error parsing start time: {e}"))?;
    }
    if !end.is_empty() {
        etime = parse_time(end).map_err(|e| format!("error parsing end time: {e}"))?;
    }
    Ok((stime, etime))
}

/// Go `parseTime`: a Unix timestamp (float seconds) or RFC3339Nano.
pub fn parse_time(s: &str) -> Result<Time, String> {
    if let Ok(t) = crate::cli::parse_go_float(s) {
        let sec = t.trunc();
        let ns = t - sec;
        return Ok(Time::from_unix(sec as i64, (ns * 1e9) as i64));
    }
    if let Some(t) = parse_rfc3339_nano(s) {
        return Ok(t);
    }
    Err(format!("cannot parse {} to a valid timestamp", promql::parser::quote::go_quote(s)))
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Go `time.Parse(time.RFC3339Nano, s)` (strict layout "2006-01-02T15:04:05.999999999Z07:00").
pub fn parse_rfc3339_nano(s: &str) -> Option<Time> {
    let b = s.as_bytes();
    let num = |from: usize, n: usize| -> Option<i64> {
        let part = b.get(from..from + n)?;
        if !part.iter().all(|c| c.is_ascii_digit()) {
            return None;
        }
        std::str::from_utf8(part).ok()?.parse().ok()
    };
    if b.len() < 20 {
        return None;
    }
    let year = num(0, 4)?;
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let (mon, day, hour, min, sec) = (num(5, 2)?, num(8, 2)?, num(11, 2)?, num(14, 2)?, num(17, 2)?);
    if !(1..=12).contains(&mon) || day < 1 || hour > 23 || min > 59 || sec > 59 {
        return None;
    }
    let mdays = [31, if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if day > mdays[(mon - 1) as usize] {
        return None;
    }
    let mut i = 19;
    let mut nanos: i64 = 0;
    if i < b.len() && (b[i] == b'.' || b[i] == b',') {
        i += 1;
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        let digits = &s[start..i];
        if digits.is_empty() || digits.len() > 9 {
            return None;
        }
        nanos = digits.parse::<i64>().ok()? * 10i64.pow(9 - digits.len() as u32);
    }
    let rest = &s[i..];
    let offset: i64 = if rest == "Z" {
        0
    } else {
        let rb = rest.as_bytes();
        if rb.len() != 6 || !(rb[0] == b'+' || rb[0] == b'-') || rb[3] != b':' {
            return None;
        }
        let oh: i64 = rest[1..3].parse().ok()?;
        let om: i64 = rest[4..6].parse().ok()?;
        if oh > 24 || om > 59 {
            return None;
        }
        let o = oh * 3600 + om * 60;
        if rb[0] == b'-' { -o } else { o }
    };
    let days = days_from_civil(year, mon, day);
    let secs = days * 86400 + hour * 3600 + min * 60 + sec - offset;
    Some(Time::from_unix(secs, nanos))
}
