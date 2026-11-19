//! Go `expectedConf` (upstream/config/config_test.go): the Config that testdata/conf.good.yml loads to.
//!
//! The SD configs of providers that are not ported yet stay as the Go literal in a comment that starts with
//! `// waits for <provider>:`; the provider porter translates them in place.

use std::collections::BTreeMap;

use config::*;
use discovery::{Configs, Group, StaticConfig};
use model::labels::{self, ValidationScheme};
use model::labelset::{ADDRESS_LABEL, LabelSet};
use model::relabel::{self, Action, Regexp};
use net::config::Header;
use net::{Authorization, BasicAuth, Headers, HttpClientConfig, OAuth2, TlsConfig, TlsVersion};
use yaml::{Base2Bytes, Duration, Secret, Url};

const MIB: i64 = 1 << 20;
const GIB: i64 = 1 << 30;

// Go consts globBodySizeLimit .. globalGoGC.
const GLOB_BODY_SIZE_LIMIT: Base2Bytes = Base2Bytes(15 * MIB);
const GLOB_SAMPLE_LIMIT: usize = 1500;
const GLOB_TARGET_LIMIT: usize = 30;
const GLOB_LABEL_LIMIT: usize = 30;
const GLOB_LABEL_NAME_LENGTH_LIMIT: usize = 200;
const GLOB_LABEL_VALUE_LENGTH_LIMIT: usize = 200;
const GLOBAL_GO_GC: i64 = 42;

/// Go `globScrapeFailureLogFile` (filepath.FromSlash is the identity on Linux).
fn glob_scrape_failure_log_file() -> String {
    "testdata/fail.log".to_string()
}

/// Go `mustParseURL`.
fn must_parse_url(u: &str) -> Url {
    Url::parse(u).unwrap()
}

/// Go `kubernetesSDHostURL`.
#[allow(dead_code)]
fn kubernetes_sd_host_url() -> Url {
    Url::parse("https://localhost:1234").unwrap()
}

fn s(v: &str) -> String {
    v.to_string()
}

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|x| x.to_string()).collect()
}

fn names(v: &[&str]) -> Option<Vec<String>> {
    Some(strs(v))
}

fn cert_key_tls() -> TlsConfig {
    TlsConfig { cert_file: s("testdata/valid_cert_file"), key_file: s("testdata/valid_key_file"), ..Default::default() }
}

fn addr(a: &str) -> LabelSet {
    LabelSet::from_pairs(&[(ADDRESS_LABEL, a)])
}

fn sd(v: Vec<Box<dyn discovery::Config>>) -> Configs {
    Configs(v)
}

fn static_localhost_9090() -> Configs {
    sd(vec![Box::new(StaticConfig(vec![Group { targets: vec![addr("localhost:9090")], labels: LabelSet::default(), source: s("0") }]))])
}

/// The fields that (almost) every scrape config literal of Go `expectedConf` repeats with the same value:
/// HonorTimestamps true, 15s interval, the global default timeout, compression on, the glob* limits,
/// DefaultScrapeProtocols, globScrapeFailureLogFile, the default name validation and escaping schemes,
/// boolPtr(false) for the four histogram/extra-metrics flags, the default metrics path and scheme, and
/// config.DefaultHTTPClientConfig. Each job below overrides what its Go literal sets differently.
fn job(name: &str) -> ScrapeConfig {
    let g = default_global_config();
    let d = default_scrape_config();
    ScrapeConfig {
        job_name: s(name),
        honor_timestamps: true,
        scrape_interval: Duration::from_secs(15),
        scrape_timeout: g.scrape_timeout,
        enable_compression: true,
        body_size_limit: GLOB_BODY_SIZE_LIMIT,
        sample_limit: GLOB_SAMPLE_LIMIT,
        target_limit: GLOB_TARGET_LIMIT,
        label_limit: GLOB_LABEL_LIMIT,
        label_name_length_limit: GLOB_LABEL_NAME_LENGTH_LIMIT,
        label_value_length_limit: GLOB_LABEL_VALUE_LENGTH_LIMIT,
        scrape_protocols: Some(default_scrape_protocols()),
        scrape_failure_log_file: glob_scrape_failure_log_file(),
        metric_name_validation_scheme: g.metric_name_validation_scheme,
        metric_name_escaping_scheme: g.metric_name_escaping_scheme.clone(),
        scrape_native_histograms: Some(false),
        always_scrape_classic_histograms: Some(false),
        convert_classic_histograms_to_nhcb: Some(false),
        extra_scrape_metrics: Some(false),
        metrics_path: d.metrics_path.clone(),
        scheme: d.scheme.clone(),
        http_client_config: HttpClientConfig::default_config(),
        ..Default::default()
    }
}

/// A relabel config with the default regex, replacement and separator (fields the Go literals spell out).
fn rc(source_labels: Option<Vec<String>>, target_label: &str, action: Action) -> relabel::Config {
    let d = relabel::default_relabel_config();
    relabel::Config {
        source_labels,
        target_label: s(target_label),
        separator: d.separator,
        regex: d.regex,
        replacement: d.replacement,
        action,
        name_validation_scheme: ValidationScheme::Utf8,
        ..Default::default()
    }
}

pub fn expected_conf() -> Config {
    let dg = default_global_config();
    let dr = relabel::default_relabel_config();
    Config {
        loaded: true,
        global_config: GlobalConfig {
            scrape_interval: Duration::from_secs(15),
            scrape_timeout: dg.scrape_timeout,
            evaluation_interval: Duration::from_secs(30),
            query_log_file: s("testdata/query.log"),
            scrape_failure_log_file: glob_scrape_failure_log_file(),

            external_labels: labels::from_strings(&["foo", "bar", "monitor", "codelab"]),

            body_size_limit: GLOB_BODY_SIZE_LIMIT,
            sample_limit: GLOB_SAMPLE_LIMIT,
            target_limit: GLOB_TARGET_LIMIT,
            label_limit: GLOB_LABEL_LIMIT,
            label_name_length_limit: GLOB_LABEL_NAME_LENGTH_LIMIT,
            label_value_length_limit: GLOB_LABEL_VALUE_LENGTH_LIMIT,
            scrape_native_histograms: Some(false),
            always_scrape_classic_histograms: false,
            convert_classic_histograms_to_nhcb: false,
            extra_scrape_metrics: Some(false),
            metric_name_validation_scheme: ValidationScheme::Utf8,
            ..Default::default()
        },

        runtime: RuntimeConfig { gogc: GLOBAL_GO_GC, log_level: LogLevel(s("info")) },

        rule_files: strs(&["testdata/first.rules", "testdata/my/*.rules"]),

        remote_write_configs: vec![
            RemoteWriteConfig {
                url: must_parse_url("http://remote1/push"),
                protobuf_message: WriteMessageType(s(WRITE_V1_MESSAGE_TYPE)),
                remote_timeout: Duration::from_secs(30),
                name: s("drop_expensive"),
                write_relabel_configs: vec![relabel::Config {
                    source_labels: names(&["__name__"]),
                    separator: s(";"),
                    regex: Regexp::must_new("expensive.*"),
                    replacement: s("$1"),
                    action: Action::Drop,
                    name_validation_scheme: ValidationScheme::Utf8,
                    ..Default::default()
                }],
                queue_config: default_queue_config(),
                metadata_config: default_metadata_config(),
                http_client_config: HttpClientConfig {
                    oauth2: Some(OAuth2 {
                        client_id: s("123"),
                        client_secret: Secret::new("456"),
                        token_url: s("http://remote1/auth"),
                        tls_config: cert_key_tls(),
                        ..Default::default()
                    }),
                    follow_redirects: true,
                    enable_http2: false,
                    ..Default::default()
                },
                ..Default::default()
            },
            RemoteWriteConfig {
                url: must_parse_url("http://remote2/push"),
                protobuf_message: WriteMessageType(s(WRITE_V2_MESSAGE_TYPE)),
                remote_timeout: Duration::from_secs(30),
                queue_config: default_queue_config(),
                metadata_config: default_metadata_config(),
                name: s("rw_tls"),
                http_client_config: HttpClientConfig {
                    tls_config: cert_key_tls(),
                    follow_redirects: true,
                    enable_http2: false,
                    ..Default::default()
                },
                headers: BTreeMap::from([(s("name"), s("value"))]),
                convert_nhcb_to_classic: true,
                ..Default::default()
            },
        ],

        otlp_config: OtlpConfig {
            promote_resource_attributes: strs(&["k8s.cluster.name", "k8s.job.name", "k8s.namespace.name"]),
            translation_strategy: s(UNDERSCORE_ESCAPING_WITH_SUFFIXES),
            label_name_underscore_sanitization: true,
            label_name_preserve_multiple_underscores: true,
            ..Default::default()
        },

        remote_read_configs: vec![
            RemoteReadConfig {
                url: must_parse_url("http://remote1/read"),
                remote_timeout: Duration::from_secs(60),
                chunked_read_limit: DEFAULT_CHUNKED_READ_LIMIT,
                read_recent: true,
                name: s("default"),
                http_client_config: HttpClientConfig { follow_redirects: true, enable_http2: false, ..Default::default() },
                filter_external_labels: true,
                ..Default::default()
            },
            RemoteReadConfig {
                url: must_parse_url("http://remote3/read"),
                remote_timeout: Duration::from_secs(60),
                chunked_read_limit: DEFAULT_CHUNKED_READ_LIMIT,
                read_recent: false,
                name: s("read_special"),
                required_matchers: LabelSet::from_pairs(&[("job", "special")]),
                http_client_config: HttpClientConfig {
                    tls_config: cert_key_tls(),
                    follow_redirects: true,
                    enable_http2: true,
                    ..Default::default()
                },
                filter_external_labels: true,
                ..Default::default()
            },
        ],

        scrape_configs: vec![
            ScrapeConfig {
                honor_labels: true,
                scrape_fallback_protocol: ScrapeProtocol::new(PROMETHEUS_TEXT_0_0_4),
                scrape_failure_log_file: s("testdata/fail_prom.log"),

                http_client_config: HttpClientConfig {
                    authorization: Some(Authorization {
                        type_: s("Bearer"),
                        credentials_file: s("testdata/valid_token_file"),
                        ..Default::default()
                    }),
                    follow_redirects: true,
                    enable_http2: true,
                    tls_config: TlsConfig { min_version: TlsVersion(0x0301), ..Default::default() }, // tls.VersionTLS10
                    http_headers: Some(Headers {
                        headers: BTreeMap::from([(
                            s("foo"),
                            Header {
                                values: strs(&["foobar"]),
                                secrets: vec![Secret::new("bar"), Secret::new("foo")],
                                files: strs(&["testdata/valid_password_file"]),
                            },
                        )]),
                    }),
                    ..Default::default()
                },

                service_discovery_configs: sd(vec![
                    Box::new(discovery::file::SdConfig {
                        files: strs(&["testdata/foo/*.slow.json", "testdata/foo/*.slow.yml", "testdata/single/file.yml"]),
                        refresh_interval: Duration::from_secs(10 * 60),
                    }),
                    Box::new(discovery::file::SdConfig {
                        files: strs(&["testdata/bar/*.yaml"]),
                        refresh_interval: Duration::from_secs(5 * 60),
                    }),
                    Box::new(StaticConfig(vec![Group {
                        targets: vec![addr("localhost:9090"), addr("localhost:9191")],
                        labels: LabelSet::from_pairs(&[("my", "label"), ("your", "label")]),
                        source: s("0"),
                    }])),
                ]),

                relabel_configs: vec![
                    relabel::Config {
                        source_labels: names(&["job", "__meta_dns_name"]),
                        target_label: s("job"),
                        separator: s(";"),
                        regex: Regexp::must_new("(.*)some-[regex]"),
                        replacement: s("foo-${1}"),
                        action: Action::Replace,
                        name_validation_scheme: ValidationScheme::Utf8,
                        ..Default::default()
                    },
                    relabel::Config {
                        source_labels: names(&["abc"]),
                        target_label: s("cde"),
                        separator: s(";"),
                        regex: dr.regex.clone(),
                        replacement: dr.replacement.clone(),
                        action: Action::Replace,
                        name_validation_scheme: ValidationScheme::Utf8,
                        ..Default::default()
                    },
                    relabel::Config {
                        target_label: s("abc"),
                        separator: s(";"),
                        regex: dr.regex.clone(),
                        replacement: s("static"),
                        action: Action::Replace,
                        name_validation_scheme: ValidationScheme::Utf8,
                        ..Default::default()
                    },
                    relabel::Config {
                        target_label: s("abc"),
                        separator: s(";"),
                        regex: Regexp::must_new(""),
                        replacement: s("static"),
                        action: Action::Replace,
                        name_validation_scheme: ValidationScheme::Utf8,
                        ..Default::default()
                    },
                    rc(names(&["foo"]), "abc", Action::KeepEqual),
                    rc(names(&["foo"]), "abc", Action::DropEqual),
                ],
                ..job("prometheus")
            },
            ScrapeConfig {
                scrape_interval: Duration::from_secs(50),
                scrape_timeout: Duration::from_secs(5),
                body_size_limit: Base2Bytes(10 * MIB),
                sample_limit: 1000,
                target_limit: 35,
                label_limit: 35,
                label_name_length_limit: 210,
                label_value_length_limit: 210,
                scrape_protocols: Some(vec![ScrapeProtocol::new(PROMETHEUS_TEXT_0_0_4)]),

                http_client_config: HttpClientConfig {
                    basic_auth: Some(BasicAuth {
                        username: s("admin_name"),
                        password: Secret::new("multiline\nmysecret\ntest"),
                        ..Default::default()
                    }),
                    follow_redirects: true,
                    enable_http2: true,
                    ..Default::default()
                },
                metrics_path: s("/my_path"),
                scheme: s("https"),

                service_discovery_configs: sd(vec![
                    Box::new(discovery::dns::SdConfig {
                        names: strs(&["first.dns.address.domain.com", "second.dns.address.domain.com"]),
                        refresh_interval: Duration::from_secs(15),
                        r#type: s("SRV"),
                        ..Default::default()
                    }),
                    Box::new(discovery::dns::SdConfig {
                        names: strs(&["first.dns.address.domain.com"]),
                        refresh_interval: Duration::from_secs(30),
                        r#type: s("SRV"),
                        ..Default::default()
                    }),
                ]),

                relabel_configs: vec![
                    relabel::Config {
                        source_labels: names(&["job"]),
                        regex: Regexp::must_new("(.*)some-[regex]"),
                        separator: s(";"),
                        replacement: dr.replacement.clone(),
                        action: Action::Drop,
                        name_validation_scheme: ValidationScheme::Utf8,
                        ..Default::default()
                    },
                    relabel::Config {
                        source_labels: names(&["__address__"]),
                        target_label: s("__tmp_hash"),
                        regex: dr.regex.clone(),
                        replacement: dr.replacement.clone(),
                        modulus: 8,
                        separator: s(";"),
                        action: Action::HashMod,
                        name_validation_scheme: ValidationScheme::Utf8,
                    },
                    relabel::Config {
                        source_labels: names(&["__tmp_hash"]),
                        regex: Regexp::must_new("1"),
                        separator: s(";"),
                        replacement: dr.replacement.clone(),
                        action: Action::Keep,
                        name_validation_scheme: ValidationScheme::Utf8,
                        ..Default::default()
                    },
                    relabel::Config {
                        regex: Regexp::must_new("1"),
                        separator: s(";"),
                        replacement: dr.replacement.clone(),
                        action: Action::LabelMap,
                        name_validation_scheme: ValidationScheme::Utf8,
                        ..Default::default()
                    },
                    relabel::Config {
                        regex: Regexp::must_new("d"),
                        separator: s(";"),
                        replacement: dr.replacement.clone(),
                        action: Action::LabelDrop,
                        name_validation_scheme: ValidationScheme::Utf8,
                        ..Default::default()
                    },
                    relabel::Config {
                        regex: Regexp::must_new("k"),
                        separator: s(";"),
                        replacement: dr.replacement.clone(),
                        action: Action::LabelKeep,
                        name_validation_scheme: ValidationScheme::Utf8,
                        ..Default::default()
                    },
                ],
                metric_relabel_configs: vec![relabel::Config {
                    source_labels: names(&["__name__"]),
                    regex: Regexp::must_new("expensive_metric.*"),
                    separator: s(";"),
                    replacement: dr.replacement.clone(),
                    action: Action::Drop,
                    name_validation_scheme: ValidationScheme::Utf8,
                    ..Default::default()
                }],
                ..job("service-x")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    Box::new(discovery::consul::SdConfig {
                        server: s("localhost:1234"),
                        path_prefix: s("/consul"),
                        token: Secret::new("mysecret"),
                        services: strs(&["nginx", "cache", "mysql"]),
                        health_filter: s(r#"Service.Tags contains "canary""#),
                        service_tags: strs(&["canary", "v1"]),
                        node_meta: BTreeMap::from([(s("rack"), s("123"))]),
                        tag_separator: discovery::consul::default_sd_config().tag_separator,
                        scheme: s("https"),
                        refresh_interval: discovery::consul::default_sd_config().refresh_interval,
                        allow_stale: true,
                        http_client_config: HttpClientConfig {
                            tls_config: TlsConfig {
                                cert_file: s("testdata/valid_cert_file"),
                                key_file: s("testdata/valid_key_file"),
                                ca_file: s("testdata/valid_ca_file"),
                                insecure_skip_verify: false,
                                ..Default::default()
                            },
                            follow_redirects: true,
                            enable_http2: true,
                            ..Default::default()
                        },
                        ..Default::default()
                    }),
                ]),

                relabel_configs: vec![relabel::Config {
                    source_labels: names(&["__meta_sd_consul_tags"]),
                    regex: Regexp::must_new("label:([^=]+)=([^,]+)"),
                    separator: s(","),
                    target_label: s("${1}"),
                    replacement: s("${2}"),
                    action: Action::Replace,
                    name_validation_scheme: ValidationScheme::Utf8,
                    ..Default::default()
                }],
                ..job("service-y")
            },
            ScrapeConfig {
                scrape_timeout: Duration::from_secs(10),

                metrics_path: s("/metrics"),
                scheme: s("http"),

                http_client_config: HttpClientConfig {
                    tls_config: cert_key_tls(),

                    authorization: Some(Authorization {
                        type_: s("Bearer"),
                        credentials: Secret::new("mysecret"),
                        ..Default::default()
                    }),

                    follow_redirects: true,
                    enable_http2: true,
                    ..Default::default()
                },
                ..job("service-z")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    // waits for kubernetes:
                    // &kubernetes.SDConfig{
                    //     APIServer: kubernetesSDHostURL(),
                    //     Role:      kubernetes.RoleEndpoint,
                    //     HTTPClientConfig: config.HTTPClientConfig{
                    //         BasicAuth: &config.BasicAuth{
                    //             Username: "myusername",
                    //             Password: "mysecret",
                    //         },
                    //         TLSConfig: config.TLSConfig{
                    //             CertFile: filepath.FromSlash("testdata/valid_cert_file"),
                    //             KeyFile:  filepath.FromSlash("testdata/valid_key_file"),
                    //         },
                    //         FollowRedirects: true,
                    //         EnableHTTP2:     true,
                    //     },
                    //     NamespaceDiscovery: kubernetes.NamespaceDiscovery{},
                    // },
                ]),
                ..job("service-kubernetes")
            },
            ScrapeConfig {
                http_client_config: HttpClientConfig {
                    basic_auth: Some(BasicAuth {
                        username: s("myusername"),
                        password_file: s("testdata/valid_password_file"),
                        ..Default::default()
                    }),
                    follow_redirects: true,
                    enable_http2: true,
                    ..Default::default()
                },

                service_discovery_configs: sd(vec![
                    // waits for kubernetes:
                    // &kubernetes.SDConfig{
                    //     APIServer: kubernetesSDHostURL(),
                    //     Role:      kubernetes.RoleEndpoint,
                    //     NamespaceDiscovery: kubernetes.NamespaceDiscovery{
                    //         Names: []string{
                    //             "default",
                    //         },
                    //     },
                    //     HTTPClientConfig: config.DefaultHTTPClientConfig,
                    // },
                ]),
                ..job("service-kubernetes-namespaces")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    // waits for kuma:
                    // &xds.KumaSDConfig{
                    //     Server:           "http://kuma-control-plane.kuma-system.svc:5676",
                    //     ClientID:         "main-prometheus",
                    //     HTTPClientConfig: config.DefaultHTTPClientConfig,
                    //     RefreshInterval:  model.Duration(15 * time.Second),
                    //     FetchTimeout:     model.Duration(2 * time.Minute),
                    // },
                ]),
                ..job("service-kuma")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    Box::new(discovery::marathon::SdConfig {
                        servers: strs(&["https://marathon.example.com:443"]),
                        refresh_interval: Duration::from_secs(30),
                        auth_token: Secret::new("mysecret"),
                        http_client_config: HttpClientConfig { tls_config: cert_key_tls(), follow_redirects: true, enable_http2: true, ..Default::default() },
                        ..Default::default()
                    }),
                ]),
                ..job("service-marathon")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    Box::new(discovery::nomad::SdConfig {
                        allow_stale: true,
                        namespace: s("default"),
                        refresh_interval: Duration::from_secs(60),
                        region: s("global"),
                        server: s("http://localhost:4646"),
                        tag_separator: s(","),
                        http_client_config: HttpClientConfig { follow_redirects: true, enable_http2: true, ..Default::default() },
                    }),
                ]),
                ..job("service-nomad")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![Box::new(discovery::aws::ec2::Ec2SdConfig {
                    region: s("us-east-1"),
                    access_key: s("access"),
                    secret_key: Secret(s("mysecret")),
                    profile: s("profile"),
                    refresh_interval: Duration::from_secs(60),
                    port: 80,
                    filters: vec![
                        discovery::aws::Filter { name: s("tag:environment"), values: vec![s("prod")] },
                        discovery::aws::Filter { name: s("tag:service"), values: vec![s("web"), s("db")] },
                    ],
                    http_client_config: HttpClientConfig::default_config(),
                    ..Default::default()
                })]),
                ..job("service-ec2")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![Box::new(discovery::aws::lightsail::LightsailSdConfig {
                    region: s("us-east-1"),
                    access_key: s("access"),
                    secret_key: Secret(s("mysecret")),
                    profile: s("profile"),
                    refresh_interval: Duration::from_secs(60),
                    port: 80,
                    http_client_config: HttpClientConfig::default_config(),
                    ..Default::default()
                })]),
                ..job("service-lightsail")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![Box::new(discovery::azure::SdConfig {
                    environment: s("AzurePublicCloud"),
                    subscription_id: s("11AAAA11-A11A-111A-A111-1111A1111A11"),
                    resource_group: s("my-resource-group"),
                    tenant_id: s("BBBB222B-B2B2-2B22-B222-2BB2222BB2B2"),
                    client_id: s("333333CC-3C33-3333-CCC3-33C3CCCCC33C"),
                    client_secret: Secret(s("mysecret")),
                    authentication_method: s("OAuth"),
                    refresh_interval: Duration::from_secs(300),
                    port: 9100,
                    http_client_config: HttpClientConfig::default_config(),
                })]),
                ..job("service-azure")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    Box::new(discovery::zookeeper::NerveSdConfig {
                        servers: strs(&["localhost"]),
                        paths: strs(&["/monitoring"]),
                        timeout: Duration::from_secs(10),
                    }),
                ]),
                ..job("service-nerve")
            },
            ScrapeConfig { service_discovery_configs: static_localhost_9090(), ..job("0123service-xxx") },
            ScrapeConfig {
                honor_timestamps: false,
                metrics_path: s("/federate"),
                service_discovery_configs: static_localhost_9090(),
                ..job("badfederation")
            },
            ScrapeConfig { service_discovery_configs: static_localhost_9090(), ..job("測試") },
            ScrapeConfig {
                service_discovery_configs: sd(vec![Box::new(discovery::http::SdConfig {
                    http_client_config: HttpClientConfig::default_config(),
                    url: s("http://example.com/prometheus"),
                    refresh_interval: Duration::from_secs(60),
                })]),
                ..job("httpsd")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    Box::new(discovery::triton::SdConfig {
                        account: s("testAccount"),
                        role: s("container"),
                        dns_suffix: s("triton.example.com"),
                        endpoint: s("triton.example.com"),
                        port: 9163,
                        refresh_interval: Duration::from_secs(60),
                        version: 1,
                        tls_config: cert_key_tls(),
                        ..Default::default()
                    }),
                ]),
                ..job("service-triton")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    Box::new(discovery::digitalocean::SdConfig {
                        http_client_config: HttpClientConfig {
                            authorization: Some(Authorization { type_: s("Bearer"), credentials: Secret::new("abcdef"), ..Default::default() }),
                            follow_redirects: true,
                            enable_http2: true,
                            ..Default::default()
                        },
                        port: 80,
                        refresh_interval: Duration::from_secs(60),
                        role: discovery::digitalocean::Role(s("droplets")),
                        ..Default::default()
                    }),
                ]),
                ..job("digitalocean-droplets")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    Box::new(discovery::moby::DockerSdConfig {
                        filters: vec![],
                        host: s("unix:///var/run/docker.sock"),
                        port: 80,
                        host_networking_host: s("localhost"),
                        refresh_interval: Duration::from_secs(60),
                        http_client_config: HttpClientConfig::default_config(),
                        match_first_network: true,
                    }),
                ]),
                ..job("docker")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    Box::new(discovery::moby::DockerSwarmSdConfig {
                        filters: vec![],
                        host: s("http://127.0.0.1:2375"),
                        role: s("nodes"),
                        port: 80,
                        refresh_interval: Duration::from_secs(60),
                        http_client_config: HttpClientConfig::default_config(),
                    }),
                ]),
                ..job("dockerswarm")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![Box::new(discovery::oci::SdConfig {
                    auth: s("api_key"),
                    region: s("us-ashburn-1"),
                    tenancy: s("ocid1.tenancy.oc1..tenancy001"),
                    user: s("ocid1.user.oc1..user001"),
                    fingerprint: s("aa:bb:cc:dd:ee:ff"),
                    key_file: s("testdata/valid_key_file"),
                    key_passphrase: Secret(s("mysecret")),
                    compartments: vec![s("ocid1.compartment.oc1..comp001")],
                    port: 9100,
                    refresh_interval: Duration::from_secs(60),
                    http_client_config: HttpClientConfig::default_config(),
                    ..Default::default()
                })]),
                ..job("service-oci")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![Box::new(discovery::openstack::SdConfig {
                    role: discovery::openstack::Role(s("instance")),
                    region: s("RegionOne"),
                    port: 80,
                    availability: s("public"),
                    refresh_interval: Duration::from_secs(60),
                    tls_config: TlsConfig { ca_file: s("testdata/valid_ca_file"), ..cert_key_tls() },
                    ..Default::default()
                })]),
                ..job("service-openstack")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    Box::new(discovery::puppetdb::SdConfig {
                        url: s("https://puppetserver/"),
                        query: s("resources { type = \"Package\" and title = \"httpd\" }"),
                        include_parameters: true,
                        port: 80,
                        refresh_interval: Duration::from_secs(60),
                        http_client_config: HttpClientConfig {
                            follow_redirects: true,
                            enable_http2: true,
                            tls_config: TlsConfig {
                                ca_file: s("testdata/valid_ca_file"),
                                cert_file: s("testdata/valid_cert_file"),
                                key_file: s("testdata/valid_key_file"),
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                    }),
                ]),
                ..job("service-puppetdb")
            },
            ScrapeConfig {
                scrape_protocols: Some(default_proto_first_scrape_protocols()),
                scrape_native_histograms: Some(true),

                relabel_configs: vec![rc(names(&["instance"]), "instance", Action::Uppercase)],
                service_discovery_configs: sd(vec![
                    Box::new(discovery::hetzner::SdConfig {
                        http_client_config: HttpClientConfig {
                            authorization: Some(Authorization { type_: s("Bearer"), credentials: Secret::new("abcdef"), ..Default::default() }),
                            follow_redirects: true,
                            enable_http2: true,
                            ..Default::default()
                        },
                        port: 80,
                        refresh_interval: Duration::from_secs(60),
                        role: discovery::hetzner::Role(s("hcloud")),
                        ..Default::default()
                    }),
                    Box::new(discovery::hetzner::SdConfig {
                        http_client_config: HttpClientConfig {
                            basic_auth: Some(BasicAuth { username: s("abcdef"), password: Secret::new("abcdef"), ..Default::default() }),
                            follow_redirects: true,
                            enable_http2: true,
                            ..Default::default()
                        },
                        port: 80,
                        refresh_interval: Duration::from_secs(60),
                        role: discovery::hetzner::Role(s("robot")),
                        ..Default::default()
                    }),
                ]),
                ..job("hetzner")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    Box::new(discovery::eureka::SdConfig {
                        server: s("http://eureka.example.com:8761/eureka"),
                        refresh_interval: Duration::from_secs(30),
                        http_client_config: HttpClientConfig::default_config(),
                    }),
                ]),
                ..job("service-eureka")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    Box::new(discovery::ovhcloud::SdConfig {
                        endpoint: s("ovh-eu"),
                        application_key: s("testAppKey"),
                        application_secret: Secret::new("testAppSecret"),
                        consumer_key: Secret::new("testConsumerKey"),
                        refresh_interval: Duration::from_secs(60),
                        service: s("vps"),
                    }),
                    Box::new(discovery::ovhcloud::SdConfig {
                        endpoint: s("ovh-eu"),
                        application_key: s("testAppKey"),
                        application_secret: Secret::new("testAppSecret"),
                        consumer_key: Secret::new("testConsumerKey"),
                        refresh_interval: Duration::from_secs(60),
                        service: s("dedicated_server"),
                    }),
                ]),
                ..job("ovhcloud")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    Box::new(discovery::scaleway::SdConfig {
                        api_url: s("https://api.scaleway.com"),
                        access_key: s("SCWXXXXXXXXXXXXXXXXX"),
                        http_client_config: HttpClientConfig::default_config(),
                        port: 80,
                        project: s("11111111-1111-1111-1111-111111111112"),
                        refresh_interval: Duration::from_secs(60),
                        role: discovery::scaleway::Role(s("instance")),
                        secret_key: Secret::new("11111111-1111-1111-1111-111111111111"),
                        zone: s("fr-par-1"),
                        ..Default::default()
                    }),
                    Box::new(discovery::scaleway::SdConfig {
                        api_url: s("https://api.scaleway.com"),
                        access_key: s("SCWXXXXXXXXXXXXXXXXX"),
                        http_client_config: HttpClientConfig::default_config(),
                        port: 80,
                        project: s("11111111-1111-1111-1111-111111111112"),
                        refresh_interval: Duration::from_secs(60),
                        role: discovery::scaleway::Role(s("baremetal")),
                        secret_key: Secret::new("11111111-1111-1111-1111-111111111111"),
                        zone: s("fr-par-1"),
                        ..Default::default()
                    }),
                ]),
                ..job("scaleway")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    Box::new(discovery::linode::SdConfig {
                        http_client_config: HttpClientConfig {
                            authorization: Some(Authorization { type_: s("Bearer"), credentials: Secret::new("abcdef"), ..Default::default() }),
                            follow_redirects: true,
                            enable_http2: true,
                            ..Default::default()
                        },
                        port: 80,
                        tag_separator: discovery::linode::default_sd_config().tag_separator,
                        refresh_interval: Duration::from_secs(60),
                        ..Default::default()
                    }),
                ]),
                ..job("linode-instances")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    Box::new(discovery::stackit::SdConfig {
                        project: s("11111111-1111-1111-1111-111111111111"),
                        service_account_key: Secret::new("mysecret_sa_key"),
                        private_key: Secret::new("mysecret_private_key"),
                        region: s("eu01"),
                        http_client_config: HttpClientConfig {
                            authorization: Some(Authorization { type_: s("Bearer"), credentials: Secret::new("abcdef"), ..Default::default() }),
                            follow_redirects: true,
                            enable_http2: true,
                            ..Default::default()
                        },
                        port: 80,
                        refresh_interval: Duration::from_secs(60),
                        ..Default::default()
                    }),
                ]),
                ..job("stackit-servers")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    Box::new(discovery::uyuni::SdConfig {
                        server: s("https://localhost:1234"),
                        username: s("gopher"),
                        password: Secret::new("hole"),
                        entitlement: s("monitoring_entitled"),
                        separator: s(","),
                        refresh_interval: Duration::from_secs(60),
                        http_client_config: HttpClientConfig::default_config(),
                    }),
                ]),
                ..job("uyuni")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    Box::new(discovery::ionos::SdConfig {
                        datacenter_id: s("8feda53f-15f0-447f-badf-ebe32dad2fc0"),
                        http_client_config: HttpClientConfig {
                            authorization: Some(Authorization { type_: s("Bearer"), credentials: Secret::new("abcdef"), ..Default::default() }),
                            follow_redirects: true,
                            enable_http2: true,
                            ..Default::default()
                        },
                        port: 80,
                        refresh_interval: Duration::from_secs(60),
                        ..Default::default()
                    }),
                ]),
                ..job("ionos")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    Box::new(discovery::vultr::SdConfig {
                        http_client_config: HttpClientConfig {
                            authorization: Some(Authorization { type_: s("Bearer"), credentials: Secret::new("abcdef"), ..Default::default() }),
                            follow_redirects: true,
                            enable_http2: true,
                            ..Default::default()
                        },
                        port: 80,
                        refresh_interval: Duration::from_secs(60),
                    }),
                ]),
                ..job("vultr")
            },
            ScrapeConfig {
                service_discovery_configs: sd(vec![
                    Box::new(discovery::outscale::SdConfig {
                        endpoint: s("https://api.eu-west-2.outscale.com/api/v1"),
                        region: s("eu-west-2"),
                        access_key: s("A1B2C3D4E5F6G7H8I9J0"),
                        secret_key: Secret::new("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"),
                        port: 80,
                        refresh_interval: Duration::from_secs(60),
                        http_client_config: HttpClientConfig::default_config(),
                        ..Default::default()
                    }),
                ]),
                ..job("outscale")
            },
        ],
        alerting_config: AlertingConfig {
            alertmanager_configs: vec![AlertmanagerConfig {
                scheme: s("https"),
                timeout: Duration::from_secs(10),
                api_version: AlertmanagerApiVersion(s(ALERTMANAGER_API_VERSION_V2)),
                http_client_config: HttpClientConfig::default_config(),
                service_discovery_configs: sd(vec![Box::new(StaticConfig(vec![Group {
                    targets: vec![addr("1.2.3.4:9093"), addr("1.2.3.5:9093"), addr("1.2.3.6:9093")],
                    labels: LabelSet::default(),
                    source: s("0"),
                }]))]),
                ..Default::default()
            }],
            ..Default::default()
        },
        storage_config: StorageConfig {
            tsdb_config: Some(TsdbConfig {
                out_of_order_time_window: 30 * 60 * 1000, // 30 * time.Minute.Milliseconds()
                out_of_order_time_window_flag: Duration::from_secs(30 * 60),
                stale_series_compaction_threshold: 0.5,
                retention: Some(TsdbRetentionConfig { time: Duration::from_secs(24 * 3600), size: Base2Bytes(GIB), percentage: 28.0 }),
                ..Default::default()
            }),
            ..Default::default()
        },
        tracing_config: TracingConfig {
            endpoint: s("localhost:4317"),
            client_type: TracingClientType(s(TRACING_CLIENT_GRPC)),
            insecure: false,
            compression: s("gzip"),
            timeout: Duration::from_secs(5),
            headers: BTreeMap::from([(s("foo"), s("bar"))]),
            tls_config: TlsConfig { insecure_skip_verify: true, ..cert_key_tls() },
            ..Default::default()
        },
        ..Default::default()
    }
}
