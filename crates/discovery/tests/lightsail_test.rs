//! Port of discovery/aws/lightsail_test.go. The Go tests mock the SDK client; here the same instances are
//! served as a Lightsail GetInstances (awsJson1.1) response by a local server.
mod cloud_mock;

use std::sync::Arc;

use cloud_mock::{MockRequest, MockResponse, MockServer};
use discovery::aws::lightsail::*;
use discovery::{CancelToken, DiscovererOptions, Group, Registry};
use json::Json;

/// The Go `types.Instance` members the tests set (None = a nil pointer).
#[derive(Clone, Debug, Default)]
struct Instance {
    private_ip_address: Option<String>,
    blueprint_id: Option<String>,
    bundle_id: Option<String>,
    name: Option<String>,
    support_code: Option<String>,
    /// `Some(None)` = a location without an availability zone.
    location: Option<Option<String>>,
    /// `Some(None)` = a state without a name.
    state: Option<Option<String>>,
}

fn s(v: &str) -> Option<String> {
    Some(v.to_string())
}

fn instance_json(i: &Instance) -> Json {
    let mut o = Json::obj();
    let opt = |o: &mut Json, k: &str, v: &Option<String>| {
        if let Some(v) = v {
            o.set(k, Json::Str(v.clone()));
        }
    };
    opt(&mut o, "name", &i.name);
    opt(&mut o, "supportCode", &i.support_code);
    if let Some(l) = &i.location {
        let mut lo = Json::obj();
        opt(&mut lo, "availabilityZone", l);
        lo.set("regionName", Json::Str("us-east-1".into()));
        o.set("location", lo);
    }
    opt(&mut o, "blueprintId", &i.blueprint_id);
    opt(&mut o, "bundleId", &i.bundle_id);
    opt(&mut o, "privateIpAddress", &i.private_ip_address);
    if let Some(st) = &i.state {
        let mut so = Json::obj().with("code", Json::Int(16));
        opt(&mut so, "name", st);
        o.set("state", so);
    }
    o
}

/// The GetInstances response body.
fn get_instances_body(instances: &[Instance]) -> String {
    let mut arr = Json::arr();
    for i in instances {
        arr.push(instance_json(i));
    }
    Json::obj().with("instances", arr).stringify()
}

/// Go `lightsailTestDiscovery`: port 8080, region us-east-1, GetInstances answered with `instances`.
fn lightsail_test_discovery(instances: Vec<Instance>) -> (LightsailDiscovery, MockServer) {
    let body = get_instances_body(&instances);
    let srv = MockServer::from_fn(move |req: &MockRequest| {
        if req.header("X-Amz-Target") == "Lightsail_20161128.GetInstances" {
            MockResponse::new(200, "application/x-amz-json-1.1", body.clone())
        } else {
            MockResponse::new(400, "application/x-amz-json-1.1", r#"{"__type":"InvalidInputException","message":"unknown operation"}"#)
        }
    });
    let cfg = LightsailSdConfig {
        port: 8080,
        region: "us-east-1".to_string(),
        endpoint: srv.url.clone(),
        access_key: "AKIDEXAMPLE".to_string(),
        secret_key: yaml::Secret("secret".to_string()),
        http_client_config: net::HttpClientConfig::default_config(),
        ..Default::default()
    };
    let reg = Registry::new();
    let rmi = discovery::new_refresh_metrics(&reg);
    let d = LightsailDiscovery::new(&cfg, DiscovererOptions { logger: discovery::Logger::nop(), metrics: Some(LightsailMetrics::new(rmi)), set_name: "lightsail".to_string(), ..Default::default() }).unwrap();
    (d, srv)
}

/// Go `fullyPopulatedLightsailInstance`.
fn fully_populated_lightsail_instance() -> Instance {
    Instance {
        private_ip_address: s("10.0.0.1"),
        blueprint_id: s("ubuntu_22_04"),
        bundle_id: s("nano_2_0"),
        name: s("instance-1"),
        support_code: s("support-code-1"),
        location: Some(s("us-east-1a")),
        state: Some(s("running")),
    }
}

fn refresh(instances: Vec<Instance>) -> Vec<Arc<Group>> {
    let (mut d, _srv) = lightsail_test_discovery(instances);
    d.refresh(&CancelToken::new()).unwrap()
}

fn label<'a>(g: &'a [Arc<Group>], k: &str) -> Option<&'a str> {
    g[0].targets[0].get(k).map(String::as_str)
}

#[test]
fn test_lightsail_refresh_fully_populated() {
    // The recorded response set for end-to-end comparisons with Prometheus (written only on request).
    if let Ok(dir) = std::env::var("AWS_SD_RECORD_DIR") {
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(format!("{dir}/lightsail_GetInstances.json"), get_instances_body(&[fully_populated_lightsail_instance()])).unwrap();
    }
    let tgs = refresh(vec![fully_populated_lightsail_instance()]);
    assert_eq!(1, tgs.len());
    assert_eq!(1, tgs[0].targets.len());
    assert_eq!(Some("10.0.0.1:8080"), label(&tgs, "__address__"));
    assert_eq!(Some("us-east-1a"), label(&tgs, LIGHTSAIL_LABEL_AZ));
    assert_eq!(Some("ubuntu_22_04"), label(&tgs, LIGHTSAIL_LABEL_BLUEPRINT_ID));
    assert_eq!(Some("nano_2_0"), label(&tgs, LIGHTSAIL_LABEL_BUNDLE_ID));
    assert_eq!(Some("instance-1"), label(&tgs, LIGHTSAIL_LABEL_INSTANCE_NAME));
    assert_eq!(Some("running"), label(&tgs, LIGHTSAIL_LABEL_INSTANCE_STATE));
    assert_eq!(Some("support-code-1"), label(&tgs, LIGHTSAIL_LABEL_INSTANCE_SUPPORT_CODE));
}

#[test]
fn test_lightsail_refresh_nil_optional_fields() {
    type Mutate = fn(&mut Instance);
    let cases: Vec<(&str, Mutate, &str)> = vec![
        ("NilLocation", |i| i.location = None, LIGHTSAIL_LABEL_AZ),
        ("NilLocationAvailabilityZone", |i| i.location = Some(None), LIGHTSAIL_LABEL_AZ),
        ("NilState", |i| i.state = None, LIGHTSAIL_LABEL_INSTANCE_STATE),
        ("NilStateName", |i| i.state = Some(None), LIGHTSAIL_LABEL_INSTANCE_STATE),
        ("NilBlueprintId", |i| i.blueprint_id = None, LIGHTSAIL_LABEL_BLUEPRINT_ID),
        ("NilBundleId", |i| i.bundle_id = None, LIGHTSAIL_LABEL_BUNDLE_ID),
        ("NilName", |i| i.name = None, LIGHTSAIL_LABEL_INSTANCE_NAME),
        ("NilSupportCode", |i| i.support_code = None, LIGHTSAIL_LABEL_INSTANCE_SUPPORT_CODE),
    ];
    for (name, mutate, absent) in cases {
        let mut inst = fully_populated_lightsail_instance();
        mutate(&mut inst);
        let tgs = refresh(vec![inst]);
        assert_eq!(1, tgs.len(), "{name}");
        assert_eq!(1, tgs[0].targets.len(), "{name}: instance must still be discovered when an optional field is absent");
        assert!(!tgs[0].targets[0].contains_key(absent), "{name}: label sourced from the absent field should be omitted");
        // The instance is still usable: the address is what makes it a target.
        assert_eq!(Some("10.0.0.1:8080"), label(&tgs, "__address__"), "{name}");
    }
}

#[test]
fn test_lightsail_refresh_all_optional_fields_nil() {
    let tgs = refresh(vec![Instance { private_ip_address: s("10.0.0.9"), ..Default::default() }]);
    assert_eq!(1, tgs.len());
    assert_eq!(1, tgs[0].targets.len());
    assert_eq!(Some("10.0.0.9:8080"), label(&tgs, "__address__"));
    assert_eq!(Some("us-east-1"), label(&tgs, LIGHTSAIL_LABEL_REGION));
    for absent in [LIGHTSAIL_LABEL_AZ, LIGHTSAIL_LABEL_BLUEPRINT_ID, LIGHTSAIL_LABEL_BUNDLE_ID, LIGHTSAIL_LABEL_INSTANCE_NAME, LIGHTSAIL_LABEL_INSTANCE_STATE, LIGHTSAIL_LABEL_INSTANCE_SUPPORT_CODE] {
        assert!(!tgs[0].targets[0].contains_key(absent), "{absent}");
    }
}
