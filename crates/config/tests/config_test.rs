//! Port of upstream/config/config_test.go.
//!
//! Go runs these tests one after another with the working directory upstream/config. Here `setup()` sets the
//! same working directory and returns a lock that every test holds, so the tests also run one at a time
//! (some change process-global state: `set_default_runtime_config`, the TEST environment variable).

use std::sync::{Mutex, MutexGuard, Once};

use config::{
    Config, EscapingScheme, GlobalConfig, LogLevel, ScrapeConfig, ScrapeProtocol, TsdbConfig, TsdbRetentionConfig, default_config,
    default_global_config, default_proto_first_scrape_protocols, default_runtime_config, default_scrape_config, default_scrape_protocols,
    load, load_file, set_default_runtime_config,
};
use discovery::{Configs, Group, Logger, StaticConfig};
use model::labels::{ValidationScheme, from_strings};
use model::labelset::{ADDRESS_LABEL, LabelSet};
use net::{HttpClientConfig, TlsConfig};
use yaml::{Base2Bytes, Duration};

#[path = "config_test/expected_conf.rs"]
mod expected_conf;

/// Sets the working directory to upstream/config (once) and serializes the tests. Use `let _g = setup();`.
fn setup() -> MutexGuard<'static, ()> {
    static ONCE: Once = Once::new();
    static LOCK: Mutex<()> = Mutex::new(());
    ONCE.call_once(|| {
        std::env::set_current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../../upstream/config")).expect("chdir upstream/config");
    });
    LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

fn nop() -> Logger {
    Logger::nop()
}

/// True when every named SD provider is registered (rows that need unported providers switch on by themselves).
fn registered(providers: &[&str]) -> bool {
    let names = discovery::registered_config_names();
    providers.iter().all(|p| names.iter().any(|n| n == p))
}

/// testify `require.ErrorContains`.
#[track_caller]
fn require_error_contains<T>(r: &Result<T, String>, want: &str) {
    match r {
        Ok(_) => panic!("expected an error containing {want:?}, got no error"),
        Err(e) => assert!(e.contains(want), "error {e:?} does not contain {want:?}"),
    }
}

#[test]
fn test_yaml_not_longer_supported_am_api() {
    let _g = setup();
    let r = load_file("testdata/config_with_no_longer_supported_am_api_config.yml", false, &nop());
    assert!(r.is_err());
}

#[test]
fn test_yaml_roundtrip() {
    let _g = setup();
    let want = load_file("testdata/roundtrip.good.yml", false, &nop()).unwrap();
    let out = yaml::marshal(&want).unwrap();
    let got = load(&out, &nop()).unwrap();
    assert_eq!(want, got);
}

#[test]
fn test_query_cost_limits() {
    let _g = setup();
    // defaults are unlimited
    {
        let c = load("", &nop()).unwrap();
        assert_eq!(c.global_config.query_max_series, 0);
        assert_eq!(c.global_config.query_max_samples_read, 0);
        assert_eq!(c.global_config.query_max_duration, Duration(0));

        let d = default_global_config();
        assert_eq!(d.query_max_series, 0);
        assert_eq!(d.query_max_samples_read, 0);
        assert_eq!(d.query_max_duration, Duration(0));
    }

    // explicit zero is unlimited
    {
        let c = load("global:\n  query_max_series: 0\n  query_max_samples_read: 0\n  query_max_duration: 0s\n", &nop()).unwrap();
        assert_eq!(c.global_config.query_max_series, 0);
        assert_eq!(c.global_config.query_max_samples_read, 0);
        assert_eq!(c.global_config.query_max_duration, Duration(0));
    }

    // parses from file
    {
        let c = load_file("testdata/query_cost_limits.good.yml", false, &nop()).unwrap();
        assert_eq!(c.global_config.query_max_series, 1000000u64);
        assert_eq!(c.global_config.query_max_samples_read, 50000000u64);
        assert_eq!(c.global_config.query_max_duration, Duration::from_secs(45));
    }

    // round-trips through YAML
    {
        let want = load_file("testdata/query_cost_limits.good.yml", false, &nop()).unwrap();
        let out = yaml::marshal(&want).unwrap();
        assert!(out.contains("query_max_series: 1000000"), "{out}");
        assert!(out.contains("query_max_samples_read: 50000000"), "{out}");
        assert!(out.contains("query_max_duration: 45s"), "{out}");
        let got = load(&out, &nop()).unwrap();
        assert_eq!(want, got);
    }

    // rejects bad values
    for (input, err_msg) in [
        ("global:\n  query_max_series: -1\n", "cannot unmarshal !!int `-1` into uint64"),
        ("global:\n  query_max_samples_read: -1\n", "cannot unmarshal !!int `-1` into uint64"),
        ("global:\n  query_max_duration: -1s\n", r#"not a valid duration string: "-1s""#),
        ("global:\n  query_max_series: many\n", "cannot unmarshal !!str `many` into uint64"),
        ("global:\n  query_max_duration: 45\n", r#"not a valid duration string: "45""#),
        ("global:\n  query_max_duration: forever\n", r#"not a valid duration string: "forever""#),
    ] {
        require_error_contains(&load(input, &nop()), err_msg);
    }

    // limits change on reload
    {
        let before = load_file("testdata/query_cost_limits.good.yml", false, &nop()).unwrap();
        let after = load("global:\n  query_max_series: 7\n  query_max_samples_read: 8\n  query_max_duration: 9s\n", &nop()).unwrap();
        assert_ne!(before.global_config.query_max_series, after.global_config.query_max_series);
        assert_eq!(after.global_config.query_max_series, 7u64);
        assert_eq!(after.global_config.query_max_samples_read, 8u64);
        assert_eq!(after.global_config.query_max_duration, Duration::from_secs(9));
    }
}

#[test]
fn test_remote_write_retry_on_rate_limit() {
    let _g = setup();
    let want = load_file("testdata/remote_write_retry_on_rate_limit.good.yml", false, &nop()).unwrap();
    let out = yaml::marshal(&want).unwrap();
    let got = load(&out, &nop()).unwrap();
    assert!(got.remote_write_configs[0].queue_config.retry_on_rate_limit);
    assert!(!got.remote_write_configs[1].queue_config.retry_on_rate_limit);
}

#[test]
fn test_remote_write_failed_request_logging() {
    let _g = setup();
    let cfg_yaml = "
remote_write:
  - url: http://remote1/api/v1/write
    failed_request_logging: true
  - url: http://remote2/api/v1/write
";
    let cfg = load(cfg_yaml, &nop()).unwrap();
    assert_eq!(cfg.remote_write_configs.len(), 2);
    assert!(cfg.remote_write_configs[0].failed_request_logging);
    assert!(!cfg.remote_write_configs[1].failed_request_logging);
}

/// Go: LoadFile, yaml.Marshal, then yaml.UnmarshalStrict into a fresh Config.
fn load_marshal_unmarshal_strict(path: &str) -> Config {
    let want = load_file(path, false, &nop()).unwrap();
    let out = yaml::marshal(&want).unwrap();
    yaml::unmarshal_strict::<Config>(&out).unwrap()
}

fn strings(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[test]
fn test_otlp_sanitize_resource_attributes() {
    let _g = setup();
    // good config - default resource attributes
    {
        let got = load_marshal_unmarshal_strict("testdata/otlp_sanitize_default_resource_attributes.good.yml");
        assert!(!got.otlp_config.promote_all_resource_attributes);
        assert!(got.otlp_config.ignore_resource_attributes.is_empty());
        assert!(got.otlp_config.promote_resource_attributes.is_empty());
    }

    // good config - promote resource attributes
    {
        let got = load_marshal_unmarshal_strict("testdata/otlp_sanitize_promote_resource_attributes.good.yml");
        assert!(!got.otlp_config.promote_all_resource_attributes);
        assert!(got.otlp_config.ignore_resource_attributes.is_empty());
        assert_eq!(got.otlp_config.promote_resource_attributes, strings(&["k8s.cluster.name", "k8s.job.name", "k8s.namespace.name"]));
    }

    // bad config - promote resource attributes
    {
        let r = load_file("testdata/otlp_sanitize_promote_resource_attributes.bad.yml", false, &nop());
        require_error_contains(&r, "invalid 'promote_resource_attributes'");
        require_error_contains(&r, r#"duplicated promoted OTel resource attribute "k8s.job.name""#);
        require_error_contains(&r, "empty promoted OTel resource attribute");
    }

    // good config - promote all resource attributes
    {
        let got = load_marshal_unmarshal_strict("testdata/otlp_sanitize_resource_attributes_promote_all.good.yml");
        assert!(got.otlp_config.promote_all_resource_attributes);
        assert!(got.otlp_config.promote_resource_attributes.is_empty());
        assert!(got.otlp_config.ignore_resource_attributes.is_empty());
    }

    // good config - ignore resource attributes
    {
        let got = load_marshal_unmarshal_strict("testdata/otlp_sanitize_ignore_resource_attributes.good.yml");
        assert!(got.otlp_config.promote_all_resource_attributes);
        assert!(got.otlp_config.promote_resource_attributes.is_empty());
        assert_eq!(got.otlp_config.ignore_resource_attributes, strings(&["k8s.cluster.name", "k8s.job.name", "k8s.namespace.name"]));
    }

    // bad config - ignore resource attributes
    {
        let r = load_file("testdata/otlp_sanitize_ignore_resource_attributes.bad.yml", false, &nop());
        require_error_contains(&r, "invalid 'ignore_resource_attributes'");
        require_error_contains(&r, r#"duplicated ignored OTel resource attribute "k8s.job.name""#);
        require_error_contains(&r, "empty ignored OTel resource attribute");
    }

    // bad config - conflict between promote all and promote specific resource attributes
    {
        let r = load_file("testdata/otlp_promote_all_resource_attributes.bad.yml", false, &nop());
        require_error_contains(&r, "'promote_all_resource_attributes' and 'promote_resource_attributes' cannot be configured simultaneously");
    }

    // bad config - configuring ignoring of resource attributes without also enabling promotion of all resource attributes
    {
        let r = load_file("testdata/otlp_ignore_resource_attributes_without_promote_all.bad.yml", false, &nop());
        require_error_contains(&r, "'ignore_resource_attributes' cannot be configured unless 'promote_all_resource_attributes' is true");
    }
}

#[test]
fn test_otlp_allow_service_name_in_target_info() {
    let _g = setup();
    // good config
    let got = load_marshal_unmarshal_strict("testdata/otlp_allow_keep_identifying_resource_attributes.good.yml");
    assert!(got.otlp_config.keep_identifying_resource_attributes);
}

#[test]
fn test_otlp_convert_histograms_to_nhcb() {
    let _g = setup();
    // good config
    let got = load_marshal_unmarshal_strict("testdata/otlp_convert_histograms_to_nhcb.good.yml");
    assert!(got.otlp_config.convert_histograms_to_nhcb);
}

#[test]
fn test_otlp_promote_scope_metadata() {
    let _g = setup();
    // good config
    let got = load_marshal_unmarshal_strict("testdata/otlp_promote_scope_metadata.good.yml");
    assert!(got.otlp_config.promote_scope_metadata);
}

#[test]
fn test_otlp_label_underscore_sanitization() {
    let _g = setup();
    // defaults to true
    {
        let conf = load_file("testdata/otlp_label_underscore_sanitization_defaults.good.yml", false, &nop()).unwrap();
        assert!(conf.otlp_config.label_name_underscore_sanitization);
        assert!(conf.otlp_config.label_name_preserve_multiple_underscores);
    }

    // explicitly enabled
    {
        let got = load_marshal_unmarshal_strict("testdata/otlp_label_underscore_sanitization_enabled.good.yml");
        assert!(got.otlp_config.label_name_underscore_sanitization);
        assert!(got.otlp_config.label_name_preserve_multiple_underscores);
    }

    // explicitly disabled
    {
        let conf = load_file("testdata/otlp_label_underscore_sanitization_disabled.good.yml", false, &nop()).unwrap();
        assert!(!conf.otlp_config.label_name_underscore_sanitization);
        assert!(!conf.otlp_config.label_name_preserve_multiple_underscores);
    }

    // empty config uses defaults
    {
        let conf = load_file("testdata/otlp_empty.yml", false, &nop()).unwrap();
        assert!(conf.otlp_config.label_name_underscore_sanitization);
        assert!(conf.otlp_config.label_name_preserve_multiple_underscores);
    }
}

#[test]
fn test_otlp_allow_utf8() {
    let _g = setup();
    // Each case runs LoadFile and Load (of the file content), as the Go subtests do.
    let load_both = |fpath: &str| -> [Result<Config, String>; 2] {
        let content = std::fs::read_to_string(fpath).unwrap();
        [load_file(fpath, false, &nop()), load(&content, &nop())]
    };

    // good config - NoUTF8EscapingWithSuffixes
    for r in load_both("testdata/otlp_allow_utf8.good.yml") {
        assert_eq!(r.unwrap().otlp_config.translation_strategy, config::NO_UTF8_ESCAPING_WITH_SUFFIXES);
    }

    // incompatible config - NoUTF8EscapingWithSuffixes
    for r in load_both("testdata/otlp_allow_utf8.incompatible.yml") {
        require_error_contains(&r, r#"OTLP translation strategy "NoUTF8EscapingWithSuffixes" is not allowed when UTF8 is disabled"#);
    }

    // good config - NoTranslation
    for r in load_both("testdata/otlp_no_translation.good.yml") {
        assert_eq!(r.unwrap().otlp_config.translation_strategy, config::NO_TRANSLATION);
    }

    // incompatible config - NoTranslation
    for r in load_both("testdata/otlp_no_translation.incompatible.yml") {
        require_error_contains(&r, r#"OTLP translation strategy "NoTranslation" is not allowed when UTF8 is disabled"#);
    }

    // bad config
    for r in load_both("testdata/otlp_allow_utf8.bad.yml") {
        require_error_contains(&r, r#"unsupported OTLP translation strategy "Invalid""#);
    }

    // good config - missing otlp config uses default
    for r in load_both("testdata/otlp_empty.yml") {
        assert_eq!(r.unwrap().otlp_config.translation_strategy, config::UNDERSCORE_ESCAPING_WITH_SUFFIXES);
    }
}

#[test]
fn test_scrape_interval_larger() {
    let _g = setup();
    let c = load_file("testdata/scrape_interval_larger.good.yml", false, &nop()).unwrap();
    assert_eq!(c.scrape_configs.len(), 1);
    for sc in &c.scrape_configs {
        assert!(sc.scrape_interval >= sc.scrape_timeout);
    }
}

#[test]
fn test_kubernetes_empty_api_server() {
    let _g = setup();
    load_file("testdata/kubernetes_empty_apiserver.good.yml", false, &nop()).unwrap();
}

#[test]
fn test_kubernetes_with_kube_config() {
    let _g = setup();
    load_file("testdata/kubernetes_kubeconfig_without_apiserver.good.yml", false, &nop()).unwrap();
}

#[test]
fn test_kubernetes_selectors() {
    let _g = setup();
    load_file("testdata/kubernetes_selectors_endpoints.good.yml", false, &nop()).unwrap();
    load_file("testdata/kubernetes_selectors_node.good.yml", false, &nop()).unwrap();
    load_file("testdata/kubernetes_selectors_ingress.good.yml", false, &nop()).unwrap();
    load_file("testdata/kubernetes_selectors_pod.good.yml", false, &nop()).unwrap();
    load_file("testdata/kubernetes_selectors_service.good.yml", false, &nop()).unwrap();
}

/// Go `expectedErrors`: (file in testdata/, error text it must contain, SD providers the file uses besides
/// static, file and http).
const EXPECTED_ERRORS: &[(&str, &str, &[&str])] = &[
    (r#"jobname.bad.yml"#, r#"job_name is empty"#, &[]),
    (r#"jobname_dup.bad.yml"#, r#"found multiple scrape configs with job name "prometheus""#, &[]),
    (r#"scrape_interval.bad.yml"#, r#"scrape timeout greater than scrape interval"#, &[]),
    (r#"labelname.bad.yml"#, r#""\xff" is not a valid label name"#, &[]),
    (r#"labelvalue.bad.yml"#, r#""\xff" is not a valid label value"#, &[]),
    (r#"regex.bad.yml"#, r#"error parsing regexp"#, &[]),
    (r#"modulus_missing.bad.yml"#, r#"relabel configuration for hashmod requires non-zero modulus"#, &[]),
    (r#"labelkeep.bad.yml"#, r#"labelkeep action requires only 'regex', and no other fields"#, &[]),
    (r#"labelkeep2.bad.yml"#, r#"labelkeep action requires only 'regex', and no other fields"#, &[]),
    (r#"labelkeep3.bad.yml"#, r#"labelkeep action requires only 'regex', and no other fields"#, &[]),
    (r#"labelkeep4.bad.yml"#, r#"labelkeep action requires only 'regex', and no other fields"#, &[]),
    (r#"labelkeep5.bad.yml"#, r#"labelkeep action requires only 'regex', and no other fields"#, &[]),
    (r#"labeldrop.bad.yml"#, r#"labeldrop action requires only 'regex', and no other fields"#, &[]),
    (r#"labeldrop2.bad.yml"#, r#"labeldrop action requires only 'regex', and no other fields"#, &[]),
    (r#"labeldrop3.bad.yml"#, r#"labeldrop action requires only 'regex', and no other fields"#, &[]),
    (r#"labeldrop4.bad.yml"#, r#"labeldrop action requires only 'regex', and no other fields"#, &[]),
    (r#"labeldrop5.bad.yml"#, r#"labeldrop action requires only 'regex', and no other fields"#, &[]),
    (r#"dropequal.bad.yml"#, r#"relabel configuration for dropequal action requires 'target_label' value"#, &[]),
    (r#"dropequal1.bad.yml"#, r#"dropequal action requires only 'source_labels' and `target_label`, and no other fields"#, &[]),
    (r#"keepequal.bad.yml"#, r#"relabel configuration for keepequal action requires 'target_label' value"#, &[]),
    (r#"keepequal1.bad.yml"#, r#"keepequal action requires only 'source_labels' and `target_label`, and no other fields"#, &[]),
    (r#"labelmap.bad.yml"#, r#"!!binary value contains invalid base64 data"#, &[]),
    (r#"lowercase.bad.yml"#, r#"relabel configuration for lowercase action requires 'target_label' value"#, &[]),
    (r#"lowercase3.bad.yml"#, r#"'replacement' can not be set for lowercase action"#, &[]),
    (r#"uppercase.bad.yml"#, r#"relabel configuration for uppercase action requires 'target_label' value"#, &[]),
    (r#"uppercase3.bad.yml"#, r#"'replacement' can not be set for uppercase action"#, &[]),
    (r#"rules.bad.yml"#, r#"invalid rule file path"#, &[]),
    (r#"unknown_attr.bad.yml"#, r#"field consult_sd_configs not found in type"#, &[]),
    (r#"bearertoken.bad.yml"#, r#"at most one of bearer_token & bearer_token_file must be configured"#, &[]),
    (r#"bearertoken_basicauth.bad.yml"#, r#"at most one of basic_auth, oauth2, bearer_token & bearer_token_file must be configured"#, &[]),
    (r#"kubernetes_http_config_without_api_server.bad.yml"#, r#"to use custom HTTP client configuration please provide the 'api_server' URL explicitly"#, &["kubernetes"]),
    (r#"kubernetes_kubeconfig_with_own_namespace.bad.yml"#, r#"cannot use 'kubeconfig_file' and 'namespaces.own_namespace' simultaneously"#, &["kubernetes"]),
    (r#"kubernetes_api_server_with_own_namespace.bad.yml"#, r#"cannot use 'api_server' and 'namespaces.own_namespace' simultaneously"#, &["kubernetes"]),
    (r#"kubernetes_kubeconfig_with_apiserver.bad.yml"#, r#"cannot use 'kubeconfig_file' and 'api_server' simultaneously"#, &["kubernetes"]),
    (r#"kubernetes_kubeconfig_with_http_config.bad.yml"#, r#"cannot use a custom HTTP client configuration together with 'kubeconfig_file'"#, &["kubernetes"]),
    (r#"kubernetes_bearertoken.bad.yml"#, r#"at most one of bearer_token & bearer_token_file must be configured"#, &["kubernetes"]),
    (r#"kubernetes_role.bad.yml"#, r#"role"#, &["kubernetes"]),
    (r#"kubernetes_selectors_endpoints.bad.yml"#, r#"endpoints role supports only pod, service, endpoints selectors"#, &["kubernetes"]),
    (r#"kubernetes_selectors_ingress.bad.yml"#, r#"ingress role supports only ingress selectors"#, &["kubernetes"]),
    (r#"kubernetes_selectors_node.bad.yml"#, r#"node role supports only node selectors"#, &["kubernetes"]),
    (r#"kubernetes_selectors_pod.bad.yml"#, r#"pod role supports only pod, node selectors"#, &["kubernetes"]),
    (r#"kubernetes_selectors_service.bad.yml"#, r#"service role supports only service selectors"#, &["kubernetes"]),
    (r#"kubernetes_namespace_discovery.bad.yml"#, r#"field foo not found in type kubernetes.plain"#, &["kubernetes"]),
    (r#"kubernetes_selectors_duplicated_role.bad.yml"#, r#"duplicated selector role: pod"#, &["kubernetes"]),
    (r#"kubernetes_selectors_incorrect_selector.bad.yml"#, r#"invalid selector: 'metadata.status-Running'; can't understand 'metadata.status-Running'"#, &["kubernetes"]),
    (r#"kubernetes_bearertoken_basicauth.bad.yml"#, r#"at most one of basic_auth, oauth2, bearer_token & bearer_token_file must be configured"#, &["kubernetes"]),
    (r#"kubernetes_authorization_basicauth.bad.yml"#, r#"at most one of basic_auth, oauth2 & authorization must be configured"#, &["kubernetes"]),
    (r#"marathon_no_servers.bad.yml"#, r#"marathon_sd: must contain at least one Marathon server"#, &["marathon"]),
    (r#"marathon_authtoken_authtokenfile.bad.yml"#, r#"marathon_sd: at most one of auth_token & auth_token_file must be configured"#, &["marathon"]),
    (r#"marathon_authtoken_basicauth.bad.yml"#, r#"marathon_sd: at most one of basic_auth, auth_token & auth_token_file must be configured"#, &["marathon"]),
    (r#"marathon_authtoken_bearertoken.bad.yml"#, r#"marathon_sd: at most one of bearer_token, bearer_token_file, auth_token & auth_token_file must be configured"#, &["marathon"]),
    (r#"marathon_authtoken_authorization.bad.yml"#, r#"marathon_sd: at most one of auth_token, auth_token_file & authorization must be configured"#, &["marathon"]),
    (r#"openstack_role.bad.yml"#, r#"unknown OpenStack SD role"#, &["openstack"]),
    (r#"openstack_availability.bad.yml"#, r#"unknown availability invalid, must be one of admin, internal or public"#, &["openstack"]),
    (r#"url_in_targetgroup.bad.yml"#, r#""http://bad" is not a valid hostname"#, &[]),
    (r#"target_label_missing.bad.yml"#, r#"relabel configuration for replace action requires 'target_label' value"#, &[]),
    (r#"target_label_hashmod_missing.bad.yml"#, r#"relabel configuration for hashmod action requires 'target_label' value"#, &[]),
    (r#"unknown_global_attr.bad.yml"#, r#"field nonexistent_field not found in type config.plain"#, &[]),
    (r#"remote_read_url_missing.bad.yml"#, r#"url for remote_read is empty"#, &[]),
    (r#"remote_write_header.bad.yml"#, r#"x-prometheus-remote-write-version is a reserved header. It must not be changed"#, &[]),
    (r#"remote_read_header.bad.yml"#, r#"x-prometheus-remote-write-version is a reserved header. It must not be changed"#, &[]),
    (r#"remote_write_authorization_header.bad.yml"#, r#"authorization header must be changed via the basic_auth, authorization, oauth2, sigv4, azuread or google_iam parameter"#, &[]),
    (r#"remote_write_wrong_msg.bad.yml"#, r#"invalid protobuf_message value: unknown type for remote write protobuf message io.prometheus.writet.v2.Request, supported: prometheus.WriteRequest, io.prometheus.write.v2.Request"#, &[]),
    (r#"remote_write_url_missing.bad.yml"#, r#"url for remote_write is empty"#, &[]),
    (r#"remote_write_dup.bad.yml"#, r#"found multiple remote write configs with job name "queue1""#, &[]),
    (r#"remote_write_queue_max_samples_per_send_zero.bad.yml"#, r#"remote write queue max_samples_per_send must be positive"#, &[]),
    (r#"remote_write_queue_max_shards_zero.bad.yml"#, r#"remote write queue max_shards must be positive"#, &[]),
    (r#"remote_write_queue_min_shards_zero.bad.yml"#, r#"remote write queue min_shards must be positive"#, &[]),
    (r#"remote_write_queue_capacity_zero.bad.yml"#, r#"remote write queue capacity must be positive"#, &[]),
    (r#"remote_write_queue_min_shards_greater_than_max.bad.yml"#, r#"remote write queue min_shards must not be greater than max_shards"#, &[]),
    (r#"remote_write_queue_max_backoff_less_than_min.bad.yml"#, r#"remote write queue max_backoff must not be less than min_backoff"#, &[]),
    (r#"remote_read_dup.bad.yml"#, r#"found multiple remote read configs with job name "queue1""#, &[]),
    (r#"ec2_filters_empty_values.bad.yml"#, r#"EC2 SD configuration filter values cannot be empty"#, &["ec2"]),
    (r#"ec2_token_file.bad.yml"#, r#"at most one of bearer_token & bearer_token_file must be configured"#, &["ec2"]),
    (r#"lightsail_token_file.bad.yml"#, r#"at most one of bearer_token & bearer_token_file must be configured"#, &["lightsail"]),
    (r#"section_key_dup.bad.yml"#, r#"field scrape_configs already set in type config.plain"#, &[]),
    (r#"azure_client_id_missing.bad.yml"#, r#"azure SD configuration requires a client_id"#, &["azure"]),
    (r#"azure_client_secret_missing.bad.yml"#, r#"azure SD configuration requires a client_secret"#, &["azure"]),
    (r#"azure_subscription_id_missing.bad.yml"#, r#"azure SD configuration requires a subscription_id"#, &["azure"]),
    (r#"azure_tenant_id_missing.bad.yml"#, r#"azure SD configuration requires a tenant_id"#, &["azure"]),
    (r#"azure_authentication_method.bad.yml"#, r#"unknown authentication_type "invalid". Supported types are "OAuth", "ManagedIdentity", "SDK" or "WorkloadIdentity""#, &["azure"]),
    (r#"oci_authentication_method.bad.yml"#, r#"oci_sd: unknown auth method "invalid", expected "api_key" or "instance_principal""#, &["oci"]),
    (r#"azure_bearertoken_basicauth.bad.yml"#, r#"at most one of basic_auth, oauth2, bearer_token & bearer_token_file must be configured"#, &["azure"]),
    (r#"empty_scrape_config.bad.yml"#, r#"empty or null scrape config section"#, &[]),
    (r#"empty_rw_config.bad.yml"#, r#"empty or null remote write config section"#, &[]),
    (r#"empty_rr_config.bad.yml"#, r#"empty or null remote read config section"#, &[]),
    (r#"empty_target_relabel_config.bad.yml"#, r#"empty or null target relabeling rule"#, &[]),
    (r#"empty_metric_relabel_config.bad.yml"#, r#"empty or null metric relabeling rule"#, &[]),
    (r#"empty_alert_relabel_config.bad.yml"#, r#"empty or null alert relabeling rule"#, &[]),
    (r#"empty_alertmanager_relabel_config.bad.yml"#, r#"empty or null Alertmanager target relabeling rule"#, &[]),
    (r#"empty_rw_relabel_config.bad.yml"#, r#"empty or null relabeling rule in remote write config"#, &[]),
    (r#"empty_static_config.bad.yml"#, r#"empty or null section in static_configs"#, &[]),
    (r#"puppetdb_no_query.bad.yml"#, r#"query missing"#, &["puppetdb"]),
    (r#"puppetdb_no_url.bad.yml"#, r#"URL is missing"#, &["puppetdb"]),
    (r#"puppetdb_bad_url.bad.yml"#, r#"host is missing in URL"#, &["puppetdb"]),
    (r#"puppetdb_no_scheme.bad.yml"#, r#"URL scheme must be 'http' or 'https'"#, &["puppetdb"]),
    (r#"puppetdb_token_file.bad.yml"#, r#"at most one of bearer_token & bearer_token_file must be configured"#, &["puppetdb"]),
    (r#"hetzner_role.bad.yml"#, r#"unknown role"#, &["hetzner"]),
    (r#"eureka_no_server.bad.yml"#, r#"empty or null eureka server"#, &["eureka"]),
    (r#"eureka_invalid_server.bad.yml"#, r#"invalid eureka server URL"#, &["eureka"]),
    (r#"scaleway_role.bad.yml"#, r#"unknown role "invalid""#, &["scaleway"]),
    (r#"scaleway_no_secret.bad.yml"#, r#"one of secret_key & secret_key_file must be configured"#, &["scaleway"]),
    (r#"scaleway_two_secrets.bad.yml"#, r#"at most one of secret_key & secret_key_file must be configured"#, &["scaleway"]),
    (r#"outscale_no_region.bad.yml"#, r#"outscale SD configuration requires a region"#, &["outscale"]),
    (r#"outscale_no_secret.bad.yml"#, r#"one of secret_key & secret_key_file must be configured"#, &["outscale"]),
    (r#"outscale_two_secrets.bad.yml"#, r#"at most one of secret_key & secret_key_file must be configured"#, &["outscale"]),
    (r#"scrape_body_size_limit.bad.yml"#, r#"units: unknown unit  in 100"#, &[]),
    (r#"http_url_no_scheme.bad.yml"#, r#"URL scheme must be 'http' or 'https'"#, &[]),
    (r#"http_url_no_host.bad.yml"#, r#"host is missing in URL"#, &[]),
    (r#"http_token_file.bad.yml"#, r#"at most one of bearer_token & bearer_token_file must be configured"#, &[]),
    (r#"http_url_bad_scheme.bad.yml"#, r#"URL scheme must be 'http' or 'https'"#, &[]),
    (r#"empty_scrape_config_action.bad.yml"#, r#"relabel action cannot be empty"#, &[]),
    (r#"tracing_missing_endpoint.bad.yml"#, r#"tracing endpoint must be set"#, &[]),
    (r#"tracing_invalid_header.bad.yml"#, r#"x-prometheus-remote-write-version is a reserved header. It must not be changed"#, &[]),
    (r#"tracing_invalid_authorization_header.bad.yml"#, r#"authorization header configuration is not yet supported"#, &[]),
    (r#"tracing_invalid_compression.bad.yml"#, r#"invalid compression type foo provided, valid options: gzip"#, &[]),
    (r#"uyuni_no_server.bad.yml"#, r#"Uyuni SD configuration requires server host"#, &["uyuni"]),
    (r#"uyuni_token_file.bad.yml"#, r#"at most one of bearer_token & bearer_token_file must be configured"#, &["uyuni"]),
    (r#"ionos_datacenter.bad.yml"#, r#"datacenter id can't be empty"#, &["ionos"]),
    (r#"ovhcloud_no_secret.bad.yml"#, r#"application secret can not be empty"#, &["ovhcloud"]),
    (r#"ovhcloud_bad_service.bad.yml"#, r#"unknown service: fakeservice"#, &["ovhcloud"]),
    (r#"scrape_config_files_glob.bad.yml"#, r#"parsing YAML file testdata/scrape_config_files_glob.bad.yml: invalid scrape config file path "scrape_configs/*/*""#, &[]),
    (r#"scrape_config_files_scrape_protocols.bad.yml"#, r#"parsing YAML file testdata/scrape_config_files_scrape_protocols.bad.yml: scrape_protocols: unknown scrape protocol prometheusproto, supported: [OpenMetricsText0.0.1 OpenMetricsText1.0.0 OpenMetricsText2.0.0 PrometheusProto PrometheusText0.0.4 PrometheusText1.0.0] for scrape config with job name "node""#, &[]),
    (r#"scrape_config_files_scrape_protocols2.bad.yml"#, r#"parsing YAML file testdata/scrape_config_files_scrape_protocols2.bad.yml: duplicated protocol in scrape_protocols, got [OpenMetricsText1.0.0 PrometheusProto OpenMetricsText1.0.0] for scrape config with job name "node""#, &[]),
    (r#"scrape_config_files_fallback_scrape_protocol1.bad.yml"#, r#"parsing YAML file testdata/scrape_config_files_fallback_scrape_protocol1.bad.yml: invalid fallback_scrape_protocol for scrape config with job name "node": unknown scrape protocol prometheusproto, supported: [OpenMetricsText0.0.1 OpenMetricsText1.0.0 OpenMetricsText2.0.0 PrometheusProto PrometheusText0.0.4 PrometheusText1.0.0]"#, &[]),
    (r#"scrape_config_files_fallback_scrape_protocol2.bad.yml"#, r#"unmarshal errors"#, &[]),
    (r#"scrape_config_utf8_conflicting.bad.yml"#, r#"utf8 metric names requested but validation scheme is not set to UTF8"#, &[]),
    (r#"stackit_endpoint.bad.yml"#, r#"invalid endpoint"#, &["stackit"]),
    (r#"tsdb_retention_time.bad.yml"#, r#"not a valid duration string: "-1h""#, &[]),
    (r#"tsdb_retention_size.bad.yml"#, r#"'storage.tsdb.retention.size' must be greater than or equal to 0"#, &[]),
    (r#"tsdb_retention_percentage.bad.yml"#, r#"'storage.tsdb.retention.percentage' must be in the range [0, 100]"#, &[]),
    (r#"tsdb_retention_percentage_negative.bad.yml"#, r#"'storage.tsdb.retention.percentage' must be in the range [0, 100]"#, &[]),
    (r#"tsdb_chunk_encoding_floats.bad.yml"#, r#"'storage.tsdb.chunk_encoding.floats' must be 'xor' or 'xor2', or the field must be omitted entirely, got "xor3""#, &[]),
    (r#"tsdb_chunk_encoding_floats_wrong_case.bad.yml"#, r#"'storage.tsdb.chunk_encoding.floats' must be 'xor' or 'xor2', or the field must be omitted entirely, got "XOR""#, &[]),
    (r#"tsdb_chunk_encoding_floats_wrong_case_xor2.bad.yml"#, r#"'storage.tsdb.chunk_encoding.floats' must be 'xor' or 'xor2', or the field must be omitted entirely, got "XOR2""#, &[]),
    (r#"metric_name_validation_scheme.bad.yml"#, r#"unrecognized ValidationScheme: "invalid_scheme""#, &[]),
    (r#"query_max_series.bad.yml"#, r#"cannot unmarshal !!int `-1` into uint64"#, &[]),
    (r#"query_max_samples_read.bad.yml"#, r#"cannot unmarshal !!str `many` into uint64"#, &[]),
    (r#"query_max_duration.bad.yml"#, r#"not a valid duration string: "-1s""#, &[]),
];

#[test]
fn test_bad_configs() {
    let _g = setup();
    for (filename, err_msg, _) in EXPECTED_ERRORS {
        let r = load_file(&format!("testdata/{filename}"), false, &nop());
        match &r {
            Ok(_) => panic!("Expected error for {filename} to contain {err_msg:?} but got no error"),
            Err(e) => assert!(e.contains(err_msg), "Expected error for {filename} to contain {err_msg:?} but got: {e}"),
        }
    }
}

/// The rows of `test_bad_configs` whose SD providers are registered. Rows that wait for providers (see the
/// third column of EXPECTED_ERRORS) are skipped until the provider is registered.
#[test]
fn test_bad_configs_ported_providers() {
    let _g = setup();
    let mut ran = 0;
    for (filename, err_msg, providers) in EXPECTED_ERRORS {
        if !registered(providers) {
            continue;
        }
        ran += 1;
        let r = load_file(&format!("testdata/{filename}"), false, &nop());
        match &r {
            Ok(_) => panic!("Expected error for {filename} to contain {err_msg:?} but got no error"),
            Err(e) => assert!(e.contains(err_msg), "Expected error for {filename} to contain {err_msg:?} but got: {e}"),
        }
    }
    assert!(ran > 0);
}

#[test]
fn test_tsdb_retention_percentage_float() {
    let _g = setup();
    let c = load_file("testdata/tsdb_retention_percentage_float.good.yml", false, &nop()).unwrap();
    assert_eq!(c.storage_config.tsdb_config.unwrap().retention.unwrap().percentage, 0.5);
}

#[test]
fn test_tsdb_chunk_encoding() {
    let _g = setup();
    for (filename, encoding) in [
        ("tsdb_chunk_encoding_floats_xor.good.yml", config::FLOAT_CHUNK_ENCODING_XOR),
        ("tsdb_chunk_encoding_floats_xor2.good.yml", config::FLOAT_CHUNK_ENCODING_XOR2),
    ] {
        let c = load_file(&format!("testdata/{filename}"), false, &nop()).unwrap();
        assert_eq!(c.storage_config.tsdb_config.unwrap().chunk_encoding.floats, encoding);
    }

    // Empty/absent value is also valid.
    let c = load("", &nop()).unwrap();
    assert!(c.storage_config.tsdb_config.unwrap().chunk_encoding.floats.is_empty());
}

// The only error in static_config.bad.yml is the label name `!!binary "/w=="` (the byte 0xff, invalid UTF-8;
// the yaml decoder keeps it, see yaml::rawbytes).
#[test]
fn test_bad_static_configs_yml() {
    let _g = setup();
    let content = std::fs::read("testdata/static_config.bad.yml").unwrap();
    let r = yaml::unmarshal_strict::<Group>(&String::from_utf8_lossy(&content));
    assert!(r.is_err());
    // the error is Go's (and not some other decoding failure)
    assert_eq!(r.unwrap_err().to_string(), r#""\xff" is not a valid label name"#);
}

/// Go: `exp := DefaultConfig; exp.loaded = true; exp.StorageConfig.TSDBConfig = &TSDBConfig{Retention: &retention}`.
fn expected_default_loaded_config() -> Config {
    let mut exp = default_config();
    exp.loaded = true;
    exp.storage_config.tsdb_config = Some(TsdbConfig { retention: Some(TsdbRetentionConfig::default()), ..Default::default() });
    exp
}

#[test]
fn test_empty_config() {
    let _g = setup();
    let c = load("", &nop()).unwrap();
    assert_eq!(expected_default_loaded_config(), c);
    assert_eq!(c.runtime.gogc, 75);
    assert_eq!(c.runtime.log_level, LogLevel("info".to_string()));
}

#[test]
fn test_runtime_log_level_config() {
    let _g = setup();
    for (name, cfg, expected, err_string) in [
        ("default", "", "info", ""),
        ("configured", "runtime:\n  log_level: warn\n", "warn", ""),
        ("normalized", "runtime:\n  log_level: DEBUG\n", "debug", ""),
        ("invalid", "runtime:\n  log_level: verbose\n", "", "unrecognized log level verbose"),
    ] {
        let r = load(cfg, &nop());
        if !err_string.is_empty() {
            require_error_contains(&r, err_string);
            continue;
        }
        let c = r.unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(c.runtime.log_level, LogLevel(expected.to_string()), "{name}");
    }

    // configured process default
    {
        /// Go t.Cleanup: restores DefaultRuntimeConfig (and with it DefaultConfig.Runtime), also on panic.
        struct Restore(config::RuntimeConfig);
        impl Drop for Restore {
            fn drop(&mut self) {
                let prev = self.0.clone();
                set_default_runtime_config(|r| *r = prev);
            }
        }
        let _restore = Restore(default_runtime_config());

        // adapted: Go assigns DefaultRuntimeConfig.LogLevel and then DefaultConfig.Runtime = DefaultRuntimeConfig;
        // Rust default_config() reads the runtime default, so one call sets both.
        set_default_runtime_config(|r| r.log_level = LogLevel("debug".to_string()));
        assert_eq!(default_config().runtime, default_runtime_config());
        let c = load("runtime:\n  gogc: 77\n", &nop()).unwrap();
        assert_eq!(c.runtime.log_level, LogLevel("debug".to_string()));
    }
}

#[test]
fn test_expand_external_labels() {
    let _g = setup();
    // Cleanup ant TEST env variable that could exist on the system.
    // SAFETY: the tests of this file hold the setup() lock, and nothing else in this process reads TEST.
    unsafe { std::env::set_var("TEST", "") };

    let c = load_file("testdata/external_labels.good.yml", false, &nop()).unwrap();
    assert_eq!(c.global_config.external_labels, from_strings(&["bar", "foo", "baz", "foobar", "foo", "", "qux", "foo${TEST}", "xyz", "foo$bar"]));

    // SAFETY: as above.
    unsafe { std::env::set_var("TEST", "TestValue") };
    let c = load_file("testdata/external_labels.good.yml", false, &nop()).unwrap();
    assert_eq!(
        c.global_config.external_labels,
        from_strings(&["bar", "foo", "baz", "fooTestValuebar", "foo", "TestValue", "qux", "foo${TEST}", "xyz", "foo$bar"])
    );
}

#[test]
fn test_agent_mode() {
    let _g = setup();
    require_error_contains(&load_file("testdata/agent_mode.with_alert_manager.yml", true, &nop()), "field alerting is not allowed in agent mode");
    require_error_contains(&load_file("testdata/agent_mode.with_alert_relabels.yml", true, &nop()), "field alerting is not allowed in agent mode");
    require_error_contains(&load_file("testdata/agent_mode.with_rule_files.yml", true, &nop()), "field rule_files is not allowed in agent mode");
    require_error_contains(&load_file("testdata/agent_mode.with_remote_reads.yml", true, &nop()), "field remote_read is not allowed in agent mode");

    let c = load_file("testdata/agent_mode.without_remote_writes.yml", true, &nop()).unwrap();
    assert!(c.remote_write_configs.is_empty());

    let c = load_file("testdata/agent_mode.good.yml", true, &nop()).unwrap();
    assert_eq!(c.remote_write_configs.len(), 1);
    assert_eq!("http://remote1/push", c.remote_write_configs[0].url.to_url_string());
}

#[test]
fn test_global_config() {
    let _g = setup();
    // empty block restores defaults
    {
        let c = load("global:\n", &nop()).unwrap();
        // TSDBConfig is always injected by Config.UnmarshalYAML even when no storage.tsdb section is present, so the
        // expected config must include it.
        assert_eq!(expected_default_loaded_config(), c);
    }

    // isZero
    let z = GlobalConfig::default;
    for (name, cfg, expect_zero) in [
        ("empty GlobalConfig", z(), true),
        ("ScrapeInterval set", GlobalConfig { scrape_interval: Duration::from_secs(30), ..z() }, false),
        ("BodySizeLimit set", GlobalConfig { body_size_limit: Base2Bytes(1 << 20), ..z() }, false),
        ("SampleLimit set", GlobalConfig { sample_limit: 1000, ..z() }, false),
        ("TargetLimit set", GlobalConfig { target_limit: 500, ..z() }, false),
        ("LabelLimit set", GlobalConfig { label_limit: 100, ..z() }, false),
        ("LabelNameLengthLimit set", GlobalConfig { label_name_length_limit: 50, ..z() }, false),
        ("LabelValueLengthLimit set", GlobalConfig { label_value_length_limit: 200, ..z() }, false),
        ("KeepDroppedTargets set", GlobalConfig { keep_dropped_targets: 10, ..z() }, false),
        ("MetricNameValidationScheme set", GlobalConfig { metric_name_validation_scheme: ValidationScheme::Legacy, ..z() }, false),
        ("MetricNameEscapingScheme set", GlobalConfig { metric_name_escaping_scheme: config::ESCAPE_UNDERSCORES.to_string(), ..z() }, false),
    ] {
        assert_eq!(cfg.is_zero(), expect_zero, "{name}");
    }
}

/// Go `ScrapeConfigOptions`.
#[derive(Default)]
struct ScrapeConfigOptions {
    job_name: &'static str,
    scrape_interval: Duration,
    scrape_timeout: Duration,
    /// None: DefaultScrapeProtocols.
    scrape_protocols: Option<Vec<ScrapeProtocol>>,
    scrape_native_histograms: bool,
    always_scrape_classic_histograms: bool,
    convert_classic_hist_to_nhcb: bool,
    extra_scrape_metrics: bool,
}

fn static_localhost_8080() -> Configs {
    Configs(vec![Box::new(StaticConfig(vec![Group {
        targets: vec![LabelSet::from_pairs(&[(ADDRESS_LABEL, "localhost:8080")])],
        labels: LabelSet::default(),
        source: "0".to_string(),
    }]))])
}

/// The `sc` helper of Go TestGetScrapeConfigs.
fn sc(opts: ScrapeConfigOptions) -> ScrapeConfig {
    ScrapeConfig {
        job_name: opts.job_name.to_string(),
        honor_timestamps: true,
        scrape_interval: opts.scrape_interval,
        scrape_timeout: opts.scrape_timeout,
        scrape_protocols: Some(opts.scrape_protocols.unwrap_or_else(default_scrape_protocols)),
        metric_name_validation_scheme: ValidationScheme::Utf8,
        metric_name_escaping_scheme: config::ALLOW_UTF8.to_string(),

        metrics_path: "/metrics".to_string(),
        scheme: "http".to_string(),
        enable_compression: true,
        http_client_config: HttpClientConfig::default_config(),
        service_discovery_configs: static_localhost_8080(),
        scrape_native_histograms: Some(opts.scrape_native_histograms),
        always_scrape_classic_histograms: Some(opts.always_scrape_classic_histograms),
        convert_classic_histograms_to_nhcb: Some(opts.convert_classic_hist_to_nhcb),
        extra_scrape_metrics: Some(opts.extra_scrape_metrics),
        ..Default::default()
    }
}

struct GetScrapeConfigsCase {
    name: &'static str,
    config_file: &'static str,
    /// SD providers the case needs besides static, file and http.
    providers: &'static [&'static str],
    /// Built only when the case runs (the vultr config needs the vultr provider). None: Go's nil.
    expected_result: Box<dyn Fn() -> Option<Vec<ScrapeConfig>>>,
    expected_error: &'static str,
}

fn get_scrape_configs_cases() -> Vec<GetScrapeConfigsCase> {
    let s60 = Duration::from_secs(60);
    let s10 = Duration::from_secs(10);
    // One prometheus job with 60s interval and 10s timeout (most cases).
    let one = move |f: fn(&mut ScrapeConfigOptions)| -> Box<dyn Fn() -> Option<Vec<ScrapeConfig>>> {
        Box::new(move || {
            let mut o = ScrapeConfigOptions { job_name: "prometheus", scrape_interval: s60, scrape_timeout: s10, ..Default::default() };
            f(&mut o);
            Some(vec![sc(o)])
        })
    };
    let case = |name, config_file, expected_result| GetScrapeConfigsCase { name, config_file, providers: &[], expected_result, expected_error: "" };
    let err_case =
        |name, config_file, expected_error| GetScrapeConfigsCase { name, config_file, providers: &[], expected_result: Box::new(|| None), expected_error };
    fn text_0_0_4() -> Option<Vec<ScrapeProtocol>> {
        Some(vec![ScrapeProtocol::new(config::PROMETHEUS_TEXT_0_0_4)])
    }

    vec![
        case("An included config file should be a valid global config.", "testdata/scrape_config_files.good.yml", one(|_| {})),
        case("A global config that only include a scrape config file.", "testdata/scrape_config_files_only.good.yml", one(|_| {})),
        case(
            "A global config that combine scrape config files and scrape configs.",
            "testdata/scrape_config_files_combined.good.yml",
            Box::new(move || {
                let mk = |job_name| sc(ScrapeConfigOptions { job_name, scrape_interval: s60, scrape_timeout: s10, ..Default::default() });
                Some(vec![mk("node"), mk("prometheus"), mk("alertmanager")])
            }),
        ),
        GetScrapeConfigsCase {
            name: "A global config that includes a scrape config file with globs",
            config_file: "testdata/scrape_config_files_glob.good.yml",
            providers: &["vultr"],
            expected_result: Box::new(|| {
                let base = |job_name: &str, interval, cert_file: &str, key_file: &str, sd| ScrapeConfig {
                    job_name: job_name.to_string(),
                    honor_timestamps: true,
                    scrape_interval: interval,
                    scrape_timeout: default_global_config().scrape_timeout,
                    scrape_protocols: Some(default_scrape_protocols()),
                    metric_name_validation_scheme: ValidationScheme::Utf8,
                    metric_name_escaping_scheme: config::ALLOW_UTF8.to_string(),
                    scrape_native_histograms: Some(false),
                    always_scrape_classic_histograms: Some(false),
                    convert_classic_histograms_to_nhcb: Some(false),
                    extra_scrape_metrics: Some(false),
                    metrics_path: default_scrape_config().metrics_path,
                    scheme: default_scrape_config().scheme,
                    enable_compression: true,
                    http_client_config: HttpClientConfig {
                        tls_config: TlsConfig { cert_file: cert_file.to_string(), key_file: key_file.to_string(), ..Default::default() },
                        follow_redirects: true,
                        enable_http2: true,
                        ..Default::default()
                    },
                    service_discovery_configs: sd,
                    ..Default::default()
                };
                // adapted: vultr.SDConfig is not ported as a Rust type yet, so the expected Go literal
                // `&vultr.SDConfig{HTTPClientConfig: {Authorization: {Type: "Bearer", Credentials: "abcdef"},
                // FollowRedirects: true, EnableHTTP2: true}, Port: 80, RefreshInterval: 60s}` is built from YAML
                // that names every one of its fields.
                let vultr: Configs = yaml::unmarshal_strict(
                    "vultr_sd_configs:\n- authorization:\n    type: Bearer\n    credentials: abcdef\n  follow_redirects: true\n  enable_http2: true\n  port: 80\n  refresh_interval: 60s\n",
                )
                .unwrap();
                Some(vec![
                    base(
                        "prometheus",
                        Duration::from_secs(60),
                        "testdata/scrape_configs/valid_cert_file",
                        "testdata/scrape_configs/valid_key_file",
                        static_localhost_8080(),
                    ),
                    base("node", Duration::from_secs(15), "testdata/valid_cert_file", "testdata/valid_key_file", vultr),
                ])
            }),
            expected_error: "",
        },
        err_case(
            "A global config that includes twice the same scrape configs.",
            "testdata/scrape_config_files_double_import.bad.yml",
            r#"found multiple scrape configs with job name "prometheus""#,
        ),
        err_case(
            "A global config that includes a scrape config identical to a scrape config in the main file.",
            "testdata/scrape_config_files_duplicate.bad.yml",
            r#"found multiple scrape configs with job name "prometheus""#,
        ),
        err_case(
            "A global config that includes a scrape config file with errors.",
            "testdata/scrape_config_files_global.bad.yml",
            r#"scrape timeout greater than scrape interval for scrape config with job name "prometheus""#,
        ),
        case("A global config that enables convert classic histograms to nhcb.", "testdata/global_convert_classic_hist_to_nhcb.good.yml", one(|o| o.convert_classic_hist_to_nhcb = true)),
        case(
            "A global config that enables convert classic histograms to nhcb and scrape config that disables the conversion",
            "testdata/local_disable_convert_classic_hist_to_nhcb.good.yml",
            one(|_| {}),
        ),
        case(
            "A global config that disables convert classic histograms to nhcb and scrape config that enables the conversion",
            "testdata/local_convert_classic_hist_to_nhcb.good.yml",
            one(|o| o.convert_classic_hist_to_nhcb = true),
        ),
        case(
            "A global config that enables always scrape classic histograms",
            "testdata/global_enable_always_scrape_classic_hist.good.yml",
            one(|o| o.always_scrape_classic_histograms = true),
        ),
        case("A global config that disables always scrape classic histograms", "testdata/global_disable_always_scrape_classic_hist.good.yml", one(|_| {})),
        case(
            "A global config that disables always scrape classic histograms and scrape config that enables it",
            "testdata/local_enable_always_scrape_classic_hist.good.yml",
            one(|o| o.always_scrape_classic_histograms = true),
        ),
        case(
            "A global config that enables always scrape classic histograms and scrape config that disables it",
            "testdata/local_disable_always_scrape_classic_hist.good.yml",
            one(|_| {}),
        ),
        case(
            "A global config that enables scrape native histograms",
            "testdata/global_enable_scrape_native_hist.good.yml",
            one(|o| {
                o.scrape_native_histograms = true;
                o.scrape_protocols = Some(default_proto_first_scrape_protocols());
            }),
        ),
        case(
            "A global config that enables scrape native histograms and sets scrape protocols explicitly",
            "testdata/global_enable_scrape_native_hist_and_scrape_protocols.good.yml",
            one(|o| {
                o.scrape_native_histograms = true;
                o.scrape_protocols = Some(vec![ScrapeProtocol::new(config::PROMETHEUS_TEXT_0_0_4)]);
            }),
        ),
        case(
            "A local config that enables scrape native histograms",
            "testdata/local_enable_scrape_native_hist.good.yml",
            one(|o| {
                o.scrape_native_histograms = true;
                o.scrape_protocols = Some(default_proto_first_scrape_protocols());
            }),
        ),
        case(
            "A local config that enables scrape native histograms and sets scrape protocols explicitly",
            "testdata/local_enable_scrape_native_hist_and_scrape_protocols.good.yml",
            one(|o| {
                o.scrape_native_histograms = true;
                o.scrape_protocols = text_0_0_4();
            }),
        ),
        case(
            "A global config that enables scrape native histograms and scrape config that disables it",
            "testdata/local_disable_scrape_native_hist.good.yml",
            one(|o| o.scrape_protocols = Some(default_scrape_protocols())),
        ),
        case(
            "A global config that enables scrape native histograms and scrape protocols and scrape config that disables scrape native histograms but does not change scrape protocols",
            "testdata/global_scrape_protocols_and_local_disable_scrape_native_hist.good.yml",
            one(|o| o.scrape_protocols = text_0_0_4()),
        ),
        case("A global config that enables extra scrape metrics", "testdata/global_enable_extra_scrape_metrics.good.yml", one(|o| o.extra_scrape_metrics = true)),
        case("A global config that disables extra scrape metrics", "testdata/global_disable_extra_scrape_metrics.good.yml", one(|_| {})),
        case(
            "A global config that disables extra scrape metrics and scrape config that enables it",
            "testdata/local_enable_extra_scrape_metrics.good.yml",
            one(|o| o.extra_scrape_metrics = true),
        ),
        case(
            "A global config that enables extra scrape metrics and scrape config that disables it",
            "testdata/local_disable_extra_scrape_metrics.good.yml",
            one(|_| {}),
        ),
    ]
}

fn run_get_scrape_configs_case(tc: &GetScrapeConfigsCase) {
    let c = load_file(tc.config_file, false, &nop()).unwrap_or_else(|e| panic!("{}: {e}", tc.name));
    let r = c.get_scrape_configs();
    if !tc.expected_error.is_empty() {
        require_error_contains(&r, tc.expected_error);
    }
    // Go returns a nil slice with the error.
    assert_eq!((tc.expected_result)(), r.ok(), "{}", tc.name);
}

#[test]
fn test_get_scrape_configs() {
    let _g = setup();
    for tc in get_scrape_configs_cases() {
        run_get_scrape_configs_case(&tc);
    }
}

/// The cases of `test_get_scrape_configs` whose SD providers are registered: all but "A global config that
/// includes a scrape config file with globs", which waits for vultr.
#[test]
fn test_get_scrape_configs_ported_providers() {
    let _g = setup();
    let mut ran = 0;
    for tc in get_scrape_configs_cases() {
        if registered(tc.providers) {
            ran += 1;
            run_get_scrape_configs_case(&tc);
        }
    }
    assert!(ran > 0);
}

#[test]
fn test_extra_scrape_metrics() {
    let _g = setup();
    for (name, cfg, expect_global, expect_enabled) in [
        (
            "default values (not set)",
            "
scrape_configs:
  - job_name: test
    static_configs:
      - targets: ['localhost:9090']
",
            Some(false), // inherits from DefaultGlobalConfig
            false,
        ),
        (
            "global enabled",
            "
global:
  extra_scrape_metrics: true
scrape_configs:
  - job_name: test
    static_configs:
      - targets: ['localhost:9090']
",
            Some(true),
            true,
        ),
        (
            "global disabled",
            "
global:
  extra_scrape_metrics: false
scrape_configs:
  - job_name: test
    static_configs:
      - targets: ['localhost:9090']
",
            Some(false),
            false,
        ),
        (
            "scrape override enabled",
            "
global:
  extra_scrape_metrics: false
scrape_configs:
  - job_name: test
    extra_scrape_metrics: true
    static_configs:
      - targets: ['localhost:9090']
",
            Some(false),
            true,
        ),
        (
            "scrape override disabled",
            "
global:
  extra_scrape_metrics: true
scrape_configs:
  - job_name: test
    extra_scrape_metrics: false
    static_configs:
      - targets: ['localhost:9090']
",
            Some(true),
            false,
        ),
    ] {
        let c = load(cfg, &nop()).unwrap_or_else(|e| panic!("{name}: {e}"));

        // Check global config
        assert_eq!(c.global_config.extra_scrape_metrics, expect_global, "{name}");

        // Check scrape config
        let scfgs = c.get_scrape_configs().unwrap();
        assert_eq!(scfgs.len(), 1, "{name}");

        // Check the effective value via the helper method
        assert_eq!(scfgs[0].extra_scrape_metrics_enabled(), expect_enabled, "{name}");
    }
}

#[test]
fn test_scrape_config_disable_compression() {
    let _g = setup();
    let got = load_marshal_unmarshal_strict("testdata/scrape_config_disable_compression.good.yml");
    assert!(!got.scrape_configs[0].enable_compression);
}

#[test]
fn test_scrape_config_name_validation_settings() {
    let _g = setup();
    for (name, input_file, expect_scheme, expect_escaping) in [
        ("blank config implies default", "scrape_config_default_validation_mode", ValidationScheme::Utf8, EscapingScheme::NoEscaping),
        ("global setting implies local settings", "scrape_config_global_validation_mode", ValidationScheme::Legacy, EscapingScheme::DotsEscaping),
        ("local setting", "scrape_config_local_validation_mode", ValidationScheme::Legacy, EscapingScheme::ValueEncodingEscaping),
        ("local setting overrides global setting", "scrape_config_local_global_validation_mode", ValidationScheme::Utf8, EscapingScheme::DotsEscaping),
        ("local validation implies underscores escaping", "scrape_config_local_infer_escaping", ValidationScheme::Legacy, EscapingScheme::UnderscoreEscaping),
    ] {
        let got = load_marshal_unmarshal_strict(&format!("testdata/{input_file}.yml"));
        assert_eq!(got.scrape_configs[0].metric_name_validation_scheme, expect_scheme, "{name}");

        // adapted: Go model.ToEscapingScheme(s) fails on ""; config::to_escaping_scheme maps "" from the
        // validation scheme, and with Unset it fails on "" the same way.
        let escaping = config::to_escaping_scheme(&got.scrape_configs[0].metric_name_escaping_scheme, ValidationScheme::Unset)
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(escaping, expect_escaping, "{name}");
    }
}

#[test]
fn test_scrape_config_name_escaping_settings() {
    let _g = setup();
    for (name, input_file, expect_validation_scheme, expect_escaping_scheme) in [
        ("blank config implies default", "scrape_config_default_validation_mode", ValidationScheme::Utf8, "allow-utf-8"),
        ("global setting implies local settings", "scrape_config_global_validation_mode", ValidationScheme::Legacy, "dots"),
        ("local setting", "scrape_config_local_validation_mode", ValidationScheme::Legacy, "values"),
        ("local setting overrides global setting", "scrape_config_local_global_validation_mode", ValidationScheme::Utf8, "dots"),
    ] {
        let got = load_marshal_unmarshal_strict(&format!("testdata/{input_file}.yml"));
        assert_eq!(got.scrape_configs[0].metric_name_validation_scheme, expect_validation_scheme, "{name}");
        assert_eq!(got.scrape_configs[0].metric_name_escaping_scheme, expect_escaping_scheme, "{name}");
    }
}

#[test]
fn test_scrape_protocol_header() {
    let _g = setup();
    for (name, proto, expected_value) in [
        ("blank", ScrapeProtocol::new(""), ""),
        ("invalid", ScrapeProtocol::new("invalid"), ""),
        ("prometheus protobuf", ScrapeProtocol::new(config::PROMETHEUS_PROTO), "application/vnd.google.protobuf"),
        ("prometheus text 0.0.4", ScrapeProtocol::new(config::PROMETHEUS_TEXT_0_0_4), "text/plain"),
        ("prometheus text 1.0.0", ScrapeProtocol::new(config::PROMETHEUS_TEXT_1_0_0), "text/plain"),
        ("openmetrics 0.0.1", ScrapeProtocol::new(config::OPEN_METRICS_TEXT_0_0_1), "application/openmetrics-text"),
        ("openmetrics 1.0.0", ScrapeProtocol::new(config::OPEN_METRICS_TEXT_1_0_0), "application/openmetrics-text"),
        ("openmetrics 2.0.0", ScrapeProtocol::new(config::OPEN_METRICS_TEXT_2_0_0), "application/openmetrics-text"),
    ] {
        assert_eq!(proto.header_media_type(), expected_value, "{name}");
    }
}

// Regression test against https://github.com/prometheus/prometheus/issues/15538
#[test]
fn test_get_scrape_configs_loaded() {
    let _g = setup();
    // without load
    {
        let c = Config::default();
        let r = c.get_scrape_configs();
        assert_eq!(r.err().as_deref(), Some("scrape config cannot be fetched, main config was not validated and loaded correctly; should not happen"));
    }
    // with load
    {
        let c = load("", &nop()).unwrap();
        c.get_scrape_configs().unwrap();
    }
}

// ---- cfg-expected: TestLoadConfig, TestElideSecrets, TestLoadConfigRuleFilesAbsolutePath ----

#[test]
fn test_load_config() {
    let _g = setup();
    // Parse a valid file that sets a global scrape timeout. This tests whether parsing
    // an overwritten default field in the global config permanently changes the default.
    load_file("testdata/global_timeout.good.yml", false, &Logger::nop()).unwrap();

    let c = load_file("testdata/conf.good.yml", false, &Logger::nop()).unwrap();

    // adapted: Go ignores the unexported fields (Config.loaded, regexp and proxy internals); Rust Regexp `==`
    // compares the patterns and `loaded` is true on both sides.
    assert_eq!(expected_conf::expected_conf(), c);
}

// YAML marshaling must not reveal authentication credentials.
#[test]
fn test_elide_secrets() {
    let _g = setup();
    let c = load_file("testdata/conf.good.yml", false, &Logger::nop()).unwrap();

    // Go regexp `\\u003csecret\\u003e|<secret>`: the two alternatives cannot overlap.
    let config = yaml::marshal(&c).unwrap();
    let yaml_config = config.as_str();

    let matches = yaml_config.matches(r"\u003csecret\u003e").count() + yaml_config.matches("<secret>").count();
    assert_eq!(matches, 29, "wrong number of secret matches found");
    assert!(!yaml_config.contains("mysecret"), "yaml marshal reveals authentication credentials.");
}

#[cfg(not(windows))]
#[test]
fn test_load_config_rule_files_absolute_path() {
    let _g = setup();
    // From config_default_test.go (build tag !windows).
    let rule_files_config_file = "testdata/rules_abs_path.good.yml";
    let rule_files_expected_conf = Config {
        loaded: true,

        global_config: default_global_config(),
        runtime: default_runtime_config(),
        otlp_config: config::default_otlp_config(),
        storage_config: config::StorageConfig {
            tsdb_config: Some(TsdbConfig { retention: Some(TsdbRetentionConfig::default()), ..Default::default() }),
            ..Default::default()
        },
        rule_files: vec!["testdata/first.rules".to_string(), "testdata/rules/second.rules".to_string(), "/absolute/third.rules".to_string()],
        ..Default::default()
    };

    // Parse a valid file that sets a rule files with an absolute path
    let c = load_file(rule_files_config_file, false, &Logger::nop()).unwrap();
    assert_eq!(rule_files_expected_conf, c);
}

// ---- end cfg-expected ----
