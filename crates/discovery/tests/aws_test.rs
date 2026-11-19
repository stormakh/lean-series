//! Port of discovery/aws/aws_test.go (the `aws_sd_configs` umbrella config).
use discovery::Config;
use discovery::aws::ecs::{EcsSdConfig, default_ecs_sd_config};
use discovery::aws::elasticache::{ElasticacheSdConfig, default_elasticache_sd_config};
use discovery::aws::msk::{MskSdConfig, default_msk_sd_config};
use discovery::aws::rds::{RdsSdConfig, default_rds_sd_config};
use discovery::aws::{ROLE_EC2, ROLE_ECS, ROLE_ELASTICACHE, ROLE_LIGHTSAIL, ROLE_MSK, ROLE_RDS, Role, SdConfig, default_sd_config};

fn role(s: &str) -> Role {
    Role(s.to_string())
}

#[test]
fn test_role_unmarshal_yaml() {
    for (name, input, expected, want_err) in [("EC2Role", "ec2", ROLE_EC2, false), ("LightsailRole", "lightsail", ROLE_LIGHTSAIL, false), ("ECSRole", "ecs", ROLE_ECS, false), ("InvalidRole", "invalid", "invalid", true)] {
        let r: Result<Role, _> = yaml::unmarshal(input);
        if want_err {
            assert!(r.is_err(), "{name}: expected error for input {input:?}");
        } else {
            assert_eq!(role(expected), r.unwrap_or_else(|e| panic!("{name}: unexpected error for input {input:?}: {e}")), "{name}");
        }
    }
}

#[test]
fn test_role_string() {
    for (r, expected) in [(ROLE_EC2, "ec2"), (ROLE_LIGHTSAIL, "lightsail"), (ROLE_ECS, "ecs")] {
        assert_eq!(expected, role(r).to_string());
    }
}

#[test]
fn test_sd_config_name() {
    assert_eq!("aws", SdConfig::default().name());
}

#[test]
fn test_default_sd_config() {
    let d = default_sd_config();
    assert_eq!(role(""), d.role);
    assert_eq!(yaml::Duration::from_secs(60), d.refresh_interval);
}

fn strings(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[test]
fn test_sd_config_unmarshal_yaml() {
    let cfg: SdConfig = yaml::unmarshal("role: ec2\nregion: us-west-2\nport: 9100\nfilters:\n  - name: instance-state-name\n    values: [running]").unwrap();
    assert_eq!(role(ROLE_EC2), cfg.role);
    let e = cfg.ec2.as_ref().expect("EC2SDConfig");
    assert_eq!("us-west-2", e.region);
    assert_eq!(9100, e.port);
    assert_eq!(1, e.filters.len());
    assert_eq!("instance-state-name", e.filters[0].name);
    assert_eq!(strings(&["running"]), e.filters[0].values);

    let cfg: SdConfig = yaml::unmarshal("role: ecs\nregion: us-east-1\nport: 9200\nclusters: [\"some-cluster\"]").unwrap();
    assert_eq!(role(ROLE_ECS), cfg.role);
    let e = cfg.ecs.as_ref().expect("ECSSDConfig");
    assert_eq!("us-east-1", e.region);
    assert_eq!(9200, e.port);
    assert_eq!(strings(&["some-cluster"]), e.clusters);

    let cfg: SdConfig = yaml::unmarshal("role: lightsail\nregion: eu-central-1\nport: 9300").unwrap();
    assert_eq!(role(ROLE_LIGHTSAIL), cfg.role);
    let e = cfg.lightsail.as_ref().expect("LightsailSDConfig");
    assert_eq!("eu-central-1", e.region);
    assert_eq!(9300, e.port);

    let cfg: SdConfig = yaml::unmarshal("role: rds\nregion: us-east-1\nport: 9400\nfilters:\n  - name: engine\n    values: [aurora-postgresql]").unwrap();
    assert_eq!(role(ROLE_RDS), cfg.role);
    let e = cfg.rds.as_ref().expect("RDSSDConfig");
    assert_eq!("us-east-1", e.region);
    assert_eq!(9400, e.port);
    assert_eq!(1, e.filters.len());
    assert_eq!("engine", e.filters[0].name);
    assert_eq!(strings(&["aurora-postgresql"]), e.filters[0].values);
}

#[test]
fn test_multiple_sd_configs_do_not_share_state() {
    // adapted: Go's require.NotSame checks that the two role configs are different pointers; in Rust each
    // decoded SdConfig owns its role config, so the check is that changing one leaves the other unchanged.
    let parse = |y: &str| -> (SdConfig, SdConfig) {
        let mut v: Vec<SdConfig> = yaml::unmarshal(y).unwrap();
        assert_eq!(2, v.len());
        let b = v.pop().unwrap();
        (v.pop().unwrap(), b)
    };

    let (c1, c2) = parse("\n- role: ec2\n  region: us-west-2\n  port: 9100\n  filters:\n    - name: tag:Name\n      values: [host-1]\n- role: ec2\n  region: us-west-2\n  port: 9101\n  filters:\n    - name: tag:Name\n      values: [host-2]");
    assert_eq!(role(ROLE_EC2), c1.role);
    assert_eq!(role(ROLE_EC2), c2.role);
    let (e1, e2) = (c1.ec2.clone().unwrap(), c2.ec2.clone().unwrap());
    assert_eq!(9100, e1.port);
    assert_eq!(9101, e2.port);
    assert_eq!(1, e1.filters.len());
    assert_eq!(1, e2.filters.len());
    assert_eq!(strings(&["host-1"]), e1.filters[0].values);
    assert_eq!(strings(&["host-2"]), e2.filters[0].values);
    let mut m = c1.clone();
    m.ec2.as_mut().unwrap().port = 1;
    assert_eq!(9101, c2.ec2.as_ref().unwrap().port, "EC2SDConfig objects should not share state");

    let (c1, c2) = parse("\n- role: ecs\n  region: us-east-1\n  port: 8080\n  clusters: [cluster-a]\n- role: ecs\n  region: us-east-1\n  port: 8081\n  clusters: [cluster-b]");
    assert_eq!(role(ROLE_ECS), c1.role);
    assert_eq!(role(ROLE_ECS), c2.role);
    let (e1, e2) = (c1.ecs.clone().unwrap(), c2.ecs.clone().unwrap());
    assert_eq!(8080, e1.port);
    assert_eq!(8081, e2.port);
    assert_eq!(strings(&["cluster-a"]), e1.clusters);
    assert_eq!(strings(&["cluster-b"]), e2.clusters);
    assert_ne!(e1, e2, "ECSSDConfig objects should not share state");

    let (c1, c2) = parse("\n- role: lightsail\n  region: eu-west-1\n  port: 7070\n- role: lightsail\n  region: eu-west-1\n  port: 7071");
    assert_eq!(role(ROLE_LIGHTSAIL), c1.role);
    assert_eq!(role(ROLE_LIGHTSAIL), c2.role);
    assert_eq!(7070, c1.lightsail.as_ref().unwrap().port);
    assert_eq!(7071, c2.lightsail.as_ref().unwrap().port);
    assert_ne!(c1.lightsail, c2.lightsail, "LightsailSDConfig objects should not share state");

    let (c1, c2) = parse("\n- role: msk\n  region: ap-south-1\n  port: 6060\n  clusters: [\"cluster-1\"]\n- role: msk\n  region: ap-south-1\n  port: 6061\n  clusters: [\"cluster-2\"]");
    assert_eq!(role(ROLE_MSK), c1.role);
    assert_eq!(role(ROLE_MSK), c2.role);
    let (e1, e2) = (c1.msk.clone().unwrap(), c2.msk.clone().unwrap());
    assert_eq!(6060, e1.port);
    assert_eq!(strings(&["cluster-1"]), e1.clusters);
    assert_eq!(6061, e2.port);
    assert_eq!(strings(&["cluster-2"]), e2.clusters);
    assert_ne!(e1, e2, "MSKSDConfig objects should not share state");
}

#[test]
fn test_awssd_config_unmarshal_yaml_no_region_resolution() {
    // Parsing must not resolve the region (no IMDS call); the region stays empty. Rust's decoder never reads
    // the environment, so this holds without changing the process environment as the Go test does.
    for r in ["ec2", "ecs", "rds", "msk", "elasticache", "lightsail"] {
        let cfg: SdConfig = yaml::unmarshal(&format!("role: {r}\nregion: \"\"\n")).unwrap_or_else(|e| panic!("{r}: config parsing must not make network calls: {e}"));
        assert!(cfg.region.is_empty(), "{r}: region must remain empty after parse; resolution is deferred to SD init");
    }
}

#[test]
fn test_request_concurrency_unmarshal_yaml() {
    fn parse_ecs(s: &str) -> Result<i64, String> {
        yaml::unmarshal::<EcsSdConfig>(s).map(|c| c.request_concurrency).map_err(|e| e.to_string())
    }
    fn parse_elasticache(s: &str) -> Result<i64, String> {
        yaml::unmarshal::<ElasticacheSdConfig>(s).map(|c| c.request_concurrency).map_err(|e| e.to_string())
    }
    fn parse_msk(s: &str) -> Result<i64, String> {
        yaml::unmarshal::<MskSdConfig>(s).map(|c| c.request_concurrency).map_err(|e| e.to_string())
    }
    fn parse_rds(s: &str) -> Result<i64, String> {
        yaml::unmarshal::<RdsSdConfig>(s).map(|c| c.request_concurrency).map_err(|e| e.to_string())
    }
    let configs: [(&str, i64, fn(&str) -> Result<i64, String>); 4] = [
        ("ecs_sd", default_ecs_sd_config().request_concurrency, parse_ecs),
        ("elasticache_sd", default_elasticache_sd_config().request_concurrency, parse_elasticache),
        ("msk_sd", default_msk_sd_config().request_concurrency, parse_msk),
        ("rds_sd", default_rds_sd_config().request_concurrency, parse_rds),
    ];
    let cases: [(&str, &str, &str, i64, bool); 4] = [
        ("Zero", "request_concurrency: 0", "request_concurrency must be positive, got 0", 0, false),
        ("Negative", "request_concurrency: -1", "request_concurrency must be positive, got -1", 0, false),
        ("Positive", "request_concurrency: 5", "", 5, false),
        ("Omitted", "", "", 0, true),
    ];
    for (cname, default, parse) in configs {
        for (name, y, expected_err, expected, want_default) in cases {
            let got = parse(&format!("region: us-west-2\n{y}\n"));
            if !expected_err.is_empty() {
                assert_eq!(Err(format!("{cname}: {expected_err}")), got, "{cname}/{name}");
                continue;
            }
            let exp = if want_default { default } else { expected };
            assert_eq!(Ok(exp), got, "{cname}/{name}");
        }
    }
}

#[test]
fn test_sd_config_set_directory() {
    let tmp = std::env::temp_dir();
    let dir = tmp.to_str().unwrap();
    for (name, y, r) in [
        ("EC2", "\nrole: ec2\nregion: us-east-1\n", ROLE_EC2),
        ("ECS", "\nrole: ecs\nregion: us-west-2\nclusters: [test-cluster]\n", ROLE_ECS),
        ("Elasticache", "\nrole: elasticache\nregion: eu-west-1\n", ROLE_ELASTICACHE),
        ("Lightsail", "\nrole: lightsail\nregion: ap-south-1\n", ROLE_LIGHTSAIL),
        ("MSK", "\nrole: msk\nregion: us-east-2\n", ROLE_MSK),
        ("RDS", "\nrole: rds\nregion: us-west-1\n", ROLE_RDS),
    ] {
        let mut cfg: SdConfig = yaml::unmarshal(y).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(role(r), cfg.role, "{name}");
        // Go: require.NotPanics
        cfg.set_directory(dir);
    }
    // SetDirectoryWithNilConfigs: no role config set (the original Go bug was a nil dereference)
    let mut cfg = SdConfig { role: role(ROLE_EC2), ..Default::default() };
    cfg.set_directory(dir);
}
