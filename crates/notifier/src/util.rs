//! Port of `notifier/util.go`: alerts to the Alertmanager API v2 form (`models.PostableAlerts`) and its JSON body.

use std::collections::BTreeMap;
use std::time::UNIX_EPOCH;

use model::labels::Labels;

use crate::alert::{Alert, Time};

/// Go `models.LabelSet` (a map; JSON keys come out sorted, as Go marshals maps).
pub type LabelSet = BTreeMap<String, String>;

/// Go `models.PostableAlert`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PostableAlert {
    pub annotations: LabelSet,
    pub ends_at: Time,
    pub starts_at: Time,
    pub generator_url: String,
    pub labels: LabelSet,
}

/// Go `alertsToOpenAPIAlerts`.
pub fn alerts_to_open_api_alerts<A: AsRef<Alert>>(alerts: &[A]) -> Vec<PostableAlert> {
    alerts
        .iter()
        .map(|a| {
            let a = a.as_ref();
            PostableAlert {
                annotations: labels_to_open_api_label_set(&a.annotations),
                ends_at: a.ends_at,
                starts_at: a.starts_at,
                generator_url: a.generator_url.clone(),
                labels: labels_to_open_api_label_set(&a.labels),
            }
        })
        .collect()
}

/// Go `labelsToOpenAPILabelSet`.
pub fn labels_to_open_api_label_set(ls: &Labels) -> LabelSet {
    ls.iter().map(|(n, v)| (n.to_string(), v.to_string())).collect()
}

/// The JSON body Go's `json.Marshal(models.PostableAlerts)` produces: per alert
/// `{"annotations":{..},"endsAt":"..","startsAt":"..","generatorURL":"..","labels":{..}}`
/// (`annotations` omitted when empty, `generatorURL` omitted when empty, times always present because
/// `omitempty` does not apply to `strfmt.DateTime`).
pub fn marshal_postable_alerts(alerts: &[PostableAlert]) -> String {
    let mut out = String::with_capacity(64 + alerts.len() * 160);
    out.push('[');
    for (i, a) in alerts.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('{');
        if !a.annotations.is_empty() {
            out.push_str("\"annotations\":");
            write_label_set(&a.annotations, &mut out);
            out.push(',');
        }
        out.push_str("\"endsAt\":\"");
        out.push_str(&format_date_time(a.ends_at));
        out.push_str("\",\"startsAt\":\"");
        out.push_str(&format_date_time(a.starts_at));
        out.push('"');
        // swag.ConcatJSON joins the two halves with a comma.
        out.push(',');
        if !a.generator_url.is_empty() {
            out.push_str("\"generatorURL\":");
            json::write_str(&a.generator_url, &mut out);
            out.push(',');
        }
        out.push_str("\"labels\":");
        write_label_set(&a.labels, &mut out);
        out.push('}');
    }
    out.push(']');
    out
}

fn write_label_set(ls: &LabelSet, out: &mut String) {
    out.push('{');
    for (i, (k, v)) in ls.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        json::write_str(k, out);
        out.push(':');
        json::write_str(v, out);
    }
    out.push('}');
}

/// Go `strfmt.DateTime` JSON text: `2006-01-02T15:04:05.000Z07:00` (milliseconds, truncated), in UTC.
/// The zero time prints as `0001-01-01T00:00:00.000Z`.
pub fn format_date_time(t: Time) -> String {
    let (secs, ms) = match t {
        // Go zero time: January 1, year 1, 00:00:00 UTC = -62135596800 s from the Unix epoch.
        None => (-62_135_596_800i64, 0i64),
        Some(t) => match t.duration_since(UNIX_EPOCH) {
            Ok(d) => (d.as_secs() as i64, (d.subsec_millis()) as i64),
            Err(e) => {
                let d = e.duration();
                let mut s = -(d.as_secs() as i64);
                let mut ns = -(d.subsec_nanos() as i64);
                if ns < 0 {
                    s -= 1;
                    ns += 1_000_000_000;
                }
                (s, ns / 1_000_000)
            }
        },
    };
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z", y, m, d, rem / 3600, rem % 3600 / 60, rem % 60, ms)
}

/// Days since 1970-01-01 to (year, month, day) in the proleptic Gregorian calendar.
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}
