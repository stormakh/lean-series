// Port of model/relabel/relabel_test.go.

use model::labels::{self, Builder, Labels, ValidationScheme};
use model::relabel::{default_relabel_config, process_builder, relabel_target_legacy, Action, Config, Regexp};

#[derive(Default)]
struct Case {
    input: Labels,
    relabel: Vec<Config>,
    output: Labels,
    drop: bool,
}

fn lm(pairs: &[(&str, &str)]) -> Labels {
    labels::from_map(pairs.iter().copied())
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[test]
fn test_relabel() {
    let tests = vec![
        Case {
            input: lm(&[
                ("a", "foo"),
                ("b", "bar"),
                ("c", "baz"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new("f(.*)"),
                    target_label: "d".to_string(),
                    separator: ";".to_string(),
                    replacement: "ch${1}-ch${1}".to_string(),
                    action: Action::Replace,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "foo"),
                ("b", "bar"),
                ("c", "baz"),
                ("d", "choo-choo"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "foo"),
                ("b", "bar"),
                ("c", "baz"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a", "b"])),
                    regex: Regexp::must_new("f(.*);(.*)r"),
                    target_label: "a".to_string(),
                    separator: ";".to_string(),
                    replacement: "b${1}${2}m".to_string(), // boobam
                    action: Action::Replace,
                    ..Config::default()
                },
                Config {
                    source_labels: Some(strs(&["c", "a"])),
                    regex: Regexp::must_new("(b).*b(.*)ba(.*)"),
                    target_label: "d".to_string(),
                    separator: ";".to_string(),
                    replacement: "$1$2$2$3".to_string(),
                    action: Action::Replace,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "boobam"),
                ("b", "bar"),
                ("c", "baz"),
                ("d", "boooom"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "foo"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new(".*o.*"),
                    action: Action::Drop,
                    ..Config::default()
                },
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new("f(.*)"),
                    target_label: "d".to_string(),
                    separator: ";".to_string(),
                    replacement: "ch$1-ch$1".to_string(),
                    action: Action::Replace,
                    ..Config::default()
                },
            ],
            drop: true,
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "foo"),
                ("b", "bar"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new(".*o.*"),
                    action: Action::Drop,
                    ..Config::default()
                },
            ],
            drop: true,
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "abc"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new(".*(b).*"),
                    target_label: "d".to_string(),
                    separator: ";".to_string(),
                    replacement: "$1".to_string(),
                    action: Action::Replace,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "abc"),
                ("d", "b"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "foo"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new("no-match"),
                    action: Action::Drop,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "foo"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "foo"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new("f|o"),
                    action: Action::Drop,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "foo"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "foo"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new("no-match"),
                    action: Action::Keep,
                    ..Config::default()
                },
            ],
            drop: true,
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "foo"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new("f.*"),
                    action: Action::Keep,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "foo"),
            ]),
            ..Case::default()
        },
        Case {
            // No replacement must be applied if there is no match.
            input: lm(&[
                ("a", "boo"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new("f"),
                    target_label: "b".to_string(),
                    replacement: "bar".to_string(),
                    action: Action::Replace,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "boo"),
            ]),
            ..Case::default()
        },
        Case {
            // Blank replacement should delete the label.
            input: lm(&[
                ("a", "foo"),
                ("f", "baz"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new("(f).*"),
                    target_label: "$1".to_string(),
                    replacement: "$2".to_string(),
                    action: Action::Replace,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "foo"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "foo"),
                ("b", "bar"),
                ("c", "baz"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["c"])),
                    target_label: "d".to_string(),
                    separator: ";".to_string(),
                    action: Action::HashMod,
                    modulus: 1000,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "foo"),
                ("b", "bar"),
                ("c", "baz"),
                ("d", "976"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "foo\nbar"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a"])),
                    target_label: "b".to_string(),
                    separator: ";".to_string(),
                    action: Action::HashMod,
                    modulus: 1000,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "foo\nbar"),
                ("b", "734"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "foo"),
                ("b1", "bar"),
                ("b2", "baz"),
            ]),
            relabel: vec![
                Config {
                    regex: Regexp::must_new("(b.*)"),
                    replacement: "bar_${1}".to_string(),
                    action: Action::LabelMap,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "foo"),
                ("b1", "bar"),
                ("b2", "baz"),
                ("bar_b1", "bar"),
                ("bar_b2", "baz"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "foo"),
                ("__meta_my_bar", "aaa"),
                ("__meta_my_baz", "bbb"),
                ("__meta_other", "ccc"),
            ]),
            relabel: vec![
                Config {
                    regex: Regexp::must_new("__meta_(my.*)"),
                    replacement: "${1}".to_string(),
                    action: Action::LabelMap,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "foo"),
                ("__meta_my_bar", "aaa"),
                ("__meta_my_baz", "bbb"),
                ("__meta_other", "ccc"),
                ("my_bar", "aaa"),
                ("my_baz", "bbb"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "some-name-value"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new("some-([^-]+)-([^,]+)"),
                    action: Action::Replace,
                    replacement: "${2}".to_string(),
                    target_label: "${1}".to_string(),
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "some-name-value"),
                ("name", "value"),
            ]),
            ..Case::default()
        },
        // invalid replacement ""
        Case {
            input: lm(&[
                ("a", "some-name-value"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new("some-([^-]+)-([^,]+)"),
                    action: Action::Replace,
                    replacement: "${3}".to_string(),
                    target_label: "${1}".to_string(),
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "some-name-value"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "some-name-0"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new("some-([^-]+)-([^,]+)"),
                    action: Action::Replace,
                    replacement: "${1}".to_string(),
                    target_label: "${3}".to_string(),
                    ..Config::default()
                },
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new("some-([^-]+)-([^,]+)"),
                    action: Action::Replace,
                    replacement: "${1}".to_string(),
                    target_label: "${3}".to_string(),
                    ..Config::default()
                },
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new("some-([^-]+)(-[^,]+)"),
                    action: Action::Replace,
                    replacement: "${1}".to_string(),
                    target_label: "${3}".to_string(),
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "some-name-0"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("__meta_sd_tags", "path:/secret,job:some-job,label:foo=bar"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["__meta_sd_tags"])),
                    regex: Regexp::must_new("(?:.+,|^)path:(/[^,]+).*"),
                    action: Action::Replace,
                    replacement: "${1}".to_string(),
                    target_label: "__metrics_path__".to_string(),
                    ..Config::default()
                },
                Config {
                    source_labels: Some(strs(&["__meta_sd_tags"])),
                    regex: Regexp::must_new("(?:.+,|^)job:([^,]+).*"),
                    action: Action::Replace,
                    replacement: "${1}".to_string(),
                    target_label: "job".to_string(),
                    ..Config::default()
                },
                Config {
                    source_labels: Some(strs(&["__meta_sd_tags"])),
                    regex: Regexp::must_new("(?:.+,|^)label:([^=]+)=([^,]+).*"),
                    action: Action::Replace,
                    replacement: "${2}".to_string(),
                    target_label: "${1}".to_string(),
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("__meta_sd_tags", "path:/secret,job:some-job,label:foo=bar"),
                ("__metrics_path__", "/secret"),
                ("job", "some-job"),
                ("foo", "bar"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("__meta_kubernetes_pod_container_port_name", "foo"),
                ("__meta_kubernetes_pod_annotation_XXX_metrics_port", "9091"),
            ]),
            relabel: vec![
                Config {
                    regex: Regexp::must_new("^__meta_kubernetes_pod_container_port_name$"),
                    action: Action::LabelDrop,
                    ..Config::default()
                },
                Config {
                    source_labels: Some(strs(&["__meta_kubernetes_pod_annotation_XXX_metrics_port"])),
                    regex: Regexp::must_new("(.+)"),
                    action: Action::Replace,
                    replacement: "metrics".to_string(),
                    target_label: "__meta_kubernetes_pod_container_port_name".to_string(),
                    ..Config::default()
                },
                Config {
                    source_labels: Some(strs(&["__meta_kubernetes_pod_container_port_name"])),
                    regex: Regexp::must_new("^metrics$"),
                    action: Action::Keep,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("__meta_kubernetes_pod_annotation_XXX_metrics_port", "9091"),
                ("__meta_kubernetes_pod_container_port_name", "metrics"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "foo"),
                ("b1", "bar"),
                ("b2", "baz"),
            ]),
            relabel: vec![
                Config {
                    regex: Regexp::must_new("(b.*)"),
                    action: Action::LabelKeep,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("b1", "bar"),
                ("b2", "baz"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "foo"),
                ("b1", "bar"),
                ("b2", "baz"),
            ]),
            relabel: vec![
                Config {
                    regex: Regexp::must_new("(b.*)"),
                    action: Action::LabelDrop,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "foo"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("foo", "bAr123Foo"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["foo"])),
                    action: Action::Uppercase,
                    target_label: "foo_uppercase".to_string(),
                    ..Config::default()
                },
                Config {
                    source_labels: Some(strs(&["foo"])),
                    action: Action::Lowercase,
                    target_label: "foo_lowercase".to_string(),
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("foo", "bAr123Foo"),
                ("foo_lowercase", "bar123foo"),
                ("foo_uppercase", "BAR123FOO"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("__tmp_port", "1234"),
                ("__port1", "1234"),
                ("__port2", "5678"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["__tmp_port"])),
                    action: Action::KeepEqual,
                    target_label: "__port1".to_string(),
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("__tmp_port", "1234"),
                ("__port1", "1234"),
                ("__port2", "5678"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("__tmp_port", "1234"),
                ("__port1", "1234"),
                ("__port2", "5678"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["__tmp_port"])),
                    action: Action::DropEqual,
                    target_label: "__port1".to_string(),
                    ..Config::default()
                },
            ],
            drop: true,
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("__tmp_port", "1234"),
                ("__port1", "1234"),
                ("__port2", "5678"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["__tmp_port"])),
                    action: Action::DropEqual,
                    target_label: "__port2".to_string(),
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("__tmp_port", "1234"),
                ("__port1", "1234"),
                ("__port2", "5678"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("__tmp_port", "1234"),
                ("__port1", "1234"),
                ("__port2", "5678"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["__tmp_port"])),
                    action: Action::KeepEqual,
                    target_label: "__port2".to_string(),
                    ..Config::default()
                },
            ],
            drop: true,
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "line1\nline2"),
                ("b", "bar"),
                ("c", "baz"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new("line1.*line2"),
                    target_label: "d".to_string(),
                    separator: ";".to_string(),
                    replacement: "match${1}".to_string(),
                    action: Action::Replace,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "line1\nline2"),
                ("b", "bar"),
                ("c", "baz"),
                ("d", "match"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("utf-8.label", "line1\nline2"),
                ("b", "bar"),
                ("c", "baz"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["utf-8.label"])),
                    regex: Regexp::must_new("line1.*line2"),
                    target_label: "d".to_string(),
                    separator: ";".to_string(),
                    replacement: "match${1}".to_string(),
                    action: Action::Replace,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("utf-8.label", "line1\nline2"),
                ("b", "bar"),
                ("c", "baz"),
                ("d", "match"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "line1\nline2"),
                ("b", "bar"),
                ("c", "baz"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new("line1.*line2"),
                    target_label: "utf-8.label".to_string(),
                    separator: ";".to_string(),
                    replacement: "match${1}".to_string(),
                    action: Action::Replace,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "line1\nline2"),
                ("b", "bar"),
                ("c", "baz"),
                ("utf-8.label", "match"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "line1\nline2"),
                ("b", "bar"),
                ("c", "baz"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new("line1.*line2"),
                    target_label: "utf-8.label${1}".to_string(),
                    separator: ";".to_string(),
                    replacement: "match${1}".to_string(),
                    action: Action::Replace,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "line1\nline2"),
                ("b", "bar"),
                ("c", "baz"),
                ("utf-8.label", "match"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("a", "line1\nline2"),
                ("b", "bar"),
                ("c", "baz"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new("(line1).*(?<second>line2)"),
                    target_label: "label1_${1}".to_string(),
                    separator: ";".to_string(),
                    replacement: "val_${second}".to_string(),
                    action: Action::Replace,
                    ..Config::default()
                },
                Config {
                    source_labels: Some(strs(&["a"])),
                    regex: Regexp::must_new("(line1).*(?<second>line2)"),
                    target_label: "label2_$1".to_string(),
                    separator: ";".to_string(),
                    replacement: "val_$second".to_string(),
                    action: Action::Replace,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("a", "line1\nline2"),
                ("b", "bar"),
                ("c", "baz"),
                ("label1_line1", "val_line2"),
                ("label2_line1", "val_line2"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("__name__", "http_requests_total"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["__name__"])),
                    regex: Regexp::must_new(".*_total$"),
                    target_label: "__type__".to_string(),
                    replacement: "counter".to_string(),
                    action: Action::Replace,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("__name__", "http_requests_total"),
                ("__type__", "counter"),
            ]),
            ..Case::default()
        },
        Case {
            input: lm(&[
                ("__name__", "disk_usage_bytes"),
            ]),
            relabel: vec![
                Config {
                    source_labels: Some(strs(&["__name__"])),
                    regex: Regexp::must_new(".*_bytes$"),
                    target_label: "__unit__".to_string(),
                    replacement: "bytes".to_string(),
                    action: Action::Replace,
                    ..Config::default()
                },
            ],
            output: lm(&[
                ("__name__", "disk_usage_bytes"),
                ("__unit__", "bytes"),
            ]),
            ..Case::default()
        },
    ];

    for mut test in tests {
        // Setting default fields, mimicking the behaviour in Prometheus.
        for cfg in test.relabel.iter_mut() {
            let def = default_relabel_config();
            if cfg.action == Action::Unset {
                cfg.action = def.action;
            }
            if cfg.separator.is_empty() {
                cfg.separator = def.separator;
            }
            if cfg.regex.0.is_none() || cfg.regex.string().is_empty() {
                cfg.regex = def.regex;
            }
            if cfg.replacement.is_empty() {
                cfg.replacement = def.replacement;
            }
            cfg.name_validation_scheme = ValidationScheme::Utf8;
            cfg.validate(ValidationScheme::Utf8).unwrap();
        }

        let mut lb = Builder::new(test.input.clone());
        let keep = process_builder(&mut lb, &test.relabel);
        assert_eq!(!test.drop, keep, "{}", test.input);
        if keep {
            assert_eq!(test.output, lb.labels(), "{}", test.input);
        }
    }
}

#[test]
fn test_relabel_validate() {
    let utf8 = ValidationScheme::Utf8;
    let legacy = ValidationScheme::Legacy;
    let def_repl = || default_relabel_config().replacement;
    let tests: Vec<(Config, &str)> = vec![
        (Config { name_validation_scheme: utf8, ..Default::default() }, "relabel action cannot be empty"),
        (Config { action: Action::Replace, name_validation_scheme: utf8, ..Default::default() }, "requires 'target_label' value"),
        (Config { action: Action::Lowercase, name_validation_scheme: utf8, ..Default::default() }, "requires 'target_label' value"),
        (
            Config {
                action: Action::Lowercase,
                replacement: def_repl(),
                target_label: "${3}".to_string(), // With UTF-8 naming, this is now a legal relabel rule.
                name_validation_scheme: utf8,
                ..Default::default()
            },
            "",
        ),
        (
            Config {
                action: Action::Lowercase,
                replacement: def_repl(),
                target_label: "${3}".to_string(), // Fails with legacy validation
                name_validation_scheme: legacy,
                ..Default::default()
            },
            "\"${3}\" is invalid 'target_label' for lowercase action",
        ),
        (
            Config {
                source_labels: Some(strs(&["a"])),
                regex: Regexp::must_new("some-([^-]+)-([^,]+)"),
                action: Action::Replace,
                replacement: "${1}".to_string(),
                target_label: "${3}".to_string(),
                name_validation_scheme: utf8,
                ..Default::default()
            },
            "",
        ),
        (
            Config {
                source_labels: Some(strs(&["a"])),
                regex: Regexp::must_new("some-([^-]+)-([^,]+)"),
                action: Action::Replace,
                replacement: "${1}".to_string(),
                target_label: "0${3}".to_string(), // With UTF-8 naming this targets a valid label.
                name_validation_scheme: utf8,
                ..Default::default()
            },
            "",
        ),
        (
            Config {
                source_labels: Some(strs(&["a"])),
                regex: Regexp::must_new("some-([^-]+)-([^,]+)"),
                action: Action::Replace,
                replacement: "${1}".to_string(),
                target_label: "-${3}".to_string(), // With UTF-8 naming this targets a valid label.
                name_validation_scheme: utf8,
                ..Default::default()
            },
            "",
        ),
        (
            Config {
                regex: Regexp::must_new("__meta_kubernetes_pod_label_(strimzi_io_.+)"),
                action: Action::LabelMap,
                replacement: "$1".to_string(),
                name_validation_scheme: legacy,
                ..Default::default()
            },
            "",
        ),
        (
            Config {
                regex: Regexp::must_new("__meta_(.+)"),
                action: Action::LabelMap,
                replacement: "${1}".to_string(),
                name_validation_scheme: legacy,
                ..Default::default()
            },
            "",
        ),
    ];
    for (i, (mut config, expected)) in tests.into_iter().enumerate() {
        let err = config.validate(ValidationScheme::Utf8);
        if expected.is_empty() {
            assert!(err.is_ok(), "{}: {:?}", i, err);
        } else {
            let e = err.unwrap_err();
            assert!(e.contains(expected), "{}: {:?} does not contain {:?}", i, e, expected);
        }
    }
}

#[test]
fn test_target_label_legacy_validity() {
    let tests = [
        ("-label", false),
        ("label", true),
        ("label${1}", true),
        ("${1}label", true),
        ("${1}", true),
        ("${1}label", true),
        ("${", false),
        ("$", false),
        ("${}", false),
        ("foo${", false),
        ("$1", true),
        ("asd$2asd", true),
        ("-foo${1}bar-", false),
        ("bar.foo${1}bar", false),
        ("_${1}_", true),
        ("foo${bar}foo", true),
    ];
    for (s, valid) in tests {
        assert_eq!(valid, relabel_target_legacy().is_match(s), "Expected {:?} to be {}", s, valid);
    }
}

#[test]
fn test_regexp_json_unmarshal_then_marshal() {
    let tests = [
        ("Empty regex", r#"{"separator":"","regex":"","replacement":""}"#),
        ("string literal", r#"{"separator":"","regex":"foo","replacement":""}"#),
        ("regex", r#"{"separator":"","regex":".*foo.*","replacement":""}"#),
    ];
    for (name, input) in tests {
        let unmarshalled = Config::from_json(&json::Json::parse(input).unwrap()).unwrap();
        let marshalled = unmarshalled.to_json().stringify();
        assert_eq!(input, marshalled, "{}", name);
    }
}

#[test]
fn test_regexp_should_marshal_and_unmarshal_zero_value() {
    let zero = Regexp::default();
    let marshalled = zero.marshal_yaml();
    assert_eq!("null\n", marshalled);
    let unmarshalled = Regexp::unmarshal_yaml(&marshalled).unwrap();
    assert!(unmarshalled.0.is_none());
}

#[test]
fn test_config_unmarshal_then_marshal() {
    let tests = [
        (
            "Values provided",
            r"source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_port]
separator: ;
regex: \\d+
target_label: __meta_kubernetes_pod_container_port_number
replacement: $1
action: replace
",
        ),
        (
            "No regex provided",
            r"source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_port]
separator: ;
target_label: __meta_kubernetes_pod_container_port_number
replacement: $1
action: keepequal
",
        ),
        (
            "Default regex provided",
            r"source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_port]
separator: ;
regex: (.*)
target_label: __meta_kubernetes_pod_container_port_number
replacement: $1
action: replace
",
        ),
        (
            // https://github.com/prometheus/prometheus/issues/18652:
            // explicit empty separator/replacement/regex must survive a YAML round-trip
            // because they are distinct from the defaults (";", "$1", "(.*)").
            "Explicit empty separator and replacement",
            r#"source_labels: [namespace, k8s_app]
separator: ""
regex: ""
target_label: target_cluster
replacement: ""
action: replace
"#,
        ),
    ];
    for (name, input_yaml) in tests {
        let unmarshalled: Config = yaml::unmarshal(input_yaml).unwrap_or_else(|e| panic!("{name}: {e}"));
        let marshalled = yaml::marshal(&unmarshalled).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(input_yaml, marshalled, "{name}");
    }
}
