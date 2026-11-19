// Port of notifier/util_test.go.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use model::labels::{from_strings, Labels};
use notifier::util::{alerts_to_open_api_alerts, format_date_time, labels_to_open_api_label_set, marshal_postable_alerts};
use notifier::Alert;

fn ls(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

#[test]
fn test_labels_to_open_api_label_set() {
    assert_eq!(ls(&[("aaa", "111"), ("bbb", "222")]), labels_to_open_api_label_set(&from_strings(&["aaa", "111", "bbb", "222"])));
}

#[test]
fn test_labels_to_open_api_label_set_empty() {
    let result = labels_to_open_api_label_set(&Labels::empty());
    assert!(result.is_empty());
}

#[test]
fn test_labels_to_open_api_label_set_special_characters() {
    let result = labels_to_open_api_label_set(&from_strings(&["special/chars", "value with spaces", "unicode", "αβγ", "empty", ""]));
    let expected = ls(&[("special/chars", "value with spaces"), ("unicode", "αβγ"), ("empty", "")]);
    assert_eq!(expected, result);
}

#[test]
fn test_alerts_to_open_api_alerts_empty() {
    let result = alerts_to_open_api_alerts::<Alert>(&[]);
    assert!(result.is_empty());
    // adapted: Go marshals the empty models.PostableAlerts{} as "[]".
    assert_eq!("[]", marshal_postable_alerts(&result));
}

#[test]
fn test_alerts_to_open_api_alerts_nil() {
    // adapted: Rust has no nil slice; an empty slice is the closest input.
    let nil: Vec<Alert> = Vec::new();
    let result = alerts_to_open_api_alerts(&nil);
    assert!(result.is_empty());
}

#[test]
fn test_alerts_to_open_api_alerts_single() {
    let now = SystemTime::now();
    let alert = Alert {
        labels: from_strings(&["alertname", "test", "severity", "critical"]),
        annotations: from_strings(&["summary", "Test alert"]),
        starts_at: Some(now),
        ends_at: Some(now + Duration::from_secs(3600)),
        generator_url: "http://prometheus:9090/graph".to_string(),
    };

    let result = alerts_to_open_api_alerts(&[alert]);
    assert_eq!(result.len(), 1);

    let api_alert = &result[0];
    assert_eq!("test", api_alert.labels["alertname"]);
    assert_eq!("critical", api_alert.labels["severity"]);
    assert_eq!("Test alert", api_alert.annotations["summary"]);
    assert_eq!("http://prometheus:9090/graph", api_alert.generator_url);
}

#[test]
fn test_alerts_to_open_api_alerts_multiple() {
    let now = SystemTime::now();
    let alerts = vec![
        Alert {
            labels: from_strings(&["alertname", "alert1"]),
            annotations: from_strings(&["desc", "First alert"]),
            starts_at: Some(now),
            ends_at: Some(now + Duration::from_secs(3600)),
            ..Default::default()
        },
        Alert {
            labels: from_strings(&["alertname", "alert2"]),
            annotations: from_strings(&["desc", "Second alert"]),
            starts_at: Some(now + Duration::from_secs(60)),
            ends_at: Some(now + Duration::from_secs(7200)),
            ..Default::default()
        },
    ];

    let result = alerts_to_open_api_alerts(&alerts);
    assert_eq!(result.len(), 2);

    assert_eq!("alert1", result[0].labels["alertname"]);
    assert_eq!("alert2", result[1].labels["alertname"]);
    assert_eq!("First alert", result[0].annotations["desc"]);
    assert_eq!("Second alert", result[1].annotations["desc"]);
}

#[test]
fn test_alerts_to_open_api_alerts_empty_fields() {
    let alert = Alert { labels: Labels::empty(), annotations: Labels::empty(), starts_at: None, ends_at: None, generator_url: String::new() };

    let result = alerts_to_open_api_alerts(&[alert]);
    assert_eq!(result.len(), 1);

    let api_alert = &result[0];
    assert!(api_alert.labels.is_empty());
    assert!(api_alert.annotations.is_empty());
    assert!(api_alert.generator_url.is_empty());

    // adapted: the JSON body Go sends for this alert (checked against json.Marshal of models.PostableAlerts).
    assert_eq!(r#"[{"endsAt":"0001-01-01T00:00:00.000Z","startsAt":"0001-01-01T00:00:00.000Z","labels":{}}]"#, marshal_postable_alerts(&result));
}

// Extra check of the wire format (not an upstream test): field order, omitted fields, HTML escaping and the
// millisecond time format, as produced by Go's json.Marshal(models.PostableAlerts).
#[test]
fn postable_alerts_json_matches_go() {
    let t = SystemTime::UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_789);
    let alert = Alert {
        labels: from_strings(&["alertname", "a<b>&c", "z", "1"]),
        annotations: from_strings(&["summary", "x"]),
        starts_at: Some(t),
        ends_at: Some(t + Duration::from_secs(3600)),
        generator_url: "http://h:9090/graph?g0.expr=up&x=<".to_string(),
    };
    assert_eq!(
        r#"[{"annotations":{"summary":"x"},"endsAt":"2023-11-14T23:13:20.123Z","startsAt":"2023-11-14T22:13:20.123Z","generatorURL":"http://h:9090/graph?g0.expr=up\u0026x=\u003c","labels":{"alertname":"a\u003cb\u003e\u0026c","z":"1"}}]"#,
        marshal_postable_alerts(&alerts_to_open_api_alerts(&[alert]))
    );
    assert_eq!("1969-12-31T23:59:59.500Z", format_date_time(Some(SystemTime::UNIX_EPOCH - Duration::from_millis(500))));
}
