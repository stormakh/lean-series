//! Port of discovery/oci/oci_test.go. The Go tests stub the closures that wrap the OCI SDK clients; here the
//! same data is served as OCI REST responses (wire JSON) from a local mock server, so the request, signature,
//! paging and parse path runs too. Closures are replaced directly only where Go injects an error that no API
//! response can produce (a failing compartment lister) or a list that changes between refreshes.
mod cloud_mock;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use cloud_mock::{MockRequest, MockResponse, MockServer};
use discovery::oci::client::{Client, Limiter, Signer};
use discovery::oci::*;
use discovery::{CancelToken, Config, DiscovererOptions, Logger, Registry};
use json::Json;
use model::labelset::{ADDRESS_LABEL, LabelSet};

/// A 2048-bit RSA key for the request signer (ring signs only with keys of 2048 bits or more).
const TEST_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAsmnVzQN1oIo9YhCB656/fEQVDmbCduLBl28deBCYv3UtkbVR
5NVep/g9gpcVzCrJkFclDCTlk2nPeYOFjTtmLALyI5g2ErnSnFN3assyTnfdbe+7
lQQD18DvsyV2/mcKg5/ObUVybsT25a7zdAuaTFbt/Ae3L/R0csntwp5d0bzljF3q
s7t1K46ZVHGrXymV43jHTj68Sz/lN3gLBkS8Qltw10KFuCA9bw3Etop3WuoxdTWW
ZFDNpcMYaKCrLND3Av1tV/ehehdtgl7BvWfBnt5ffUnSVHmZ3au96Y29ZGqdcArW
6qZwiRogyYilZ3EwyCBA4SBA4819sNdqysGD5QIDAQABAoIBAAbVQtj9Pu4NHVi6
JR+vDsplmSDaEqyW9q8kFE7a497BUCN6fE51VZC9Uy3DN5SANwQOMl8Ksrg4nzea
L+0+zyT0X+25EKka8NQH5lTyv1R+Br8FJXwJkVQcEZIc5zfLHn34CuHofP99D6+w
SaayIxC1a5vxd7Sgei1DSpXfltxHfVFoVVMihaIT7mszgsJIWSXW1V5os/aPjHvj
vmLBfWmPW/ezzUF1A35WytsHnnfheUiXctI/T/6Th9udSPAbCaVFCN33mNXYzf2L
UzK1dgZGHtAo/vN8ezO+JFSvYGFTHVIu+8x5tEdjDa3lROljki/FIFabIY3iP0E1
X8FJTUECgYEA71z4VbVfImavgLPzevYh2YO0yOM9mQthenqPDDRZNi9RYfhvAP9T
9qmklFDvZTf+uGh95DJM1JRKdz/YP38XsXxnlM6kxbWvtn3aoYKSxQgDHj3UkC13
bDNHbLTH1Op6ja4FlxS6f1pN/8wCSjLsdj6eubxqh/7/uyL03pUlRKkCgYEAvtBg
NCfI9+9YNksH4eXBa1+QQzzlj9gXONGxe8+wWegfuoRH5Ugr2Rf+yqCCPGmBFBIm
hPEii75/hUz5unqGtXtL/Vr40dQmDHONoy8jPeQm0nS3pNTcXwHDkf0dgzqM3AR8
/WRabK+UVR0LrfSfSjWI3Dtn/OvCbLuDZJG0Dt0CgYEAmRsevbQF5jtIh0qTs1M8
06UrPlQcUoi1EWLTnTre1YnuxHvkhP7M9joMy9H5TwVCKTKtVAnYmyE/gIeWCweA
YEby+nwzaUON4W0fK7AZsW4ukGl8TbzHZq5mQWJhcA+L1JQ7fygDIvLgcX5M+Kka
F4h2ftfGTLcPXOQ/PSCpdgECgYBYn+G+96eQ+HqpJOoxUowmeclAYX1h9QS8IDDm
B42qBHUQJTZbWMz81PdDV4+FzZ7lGpUHdBQOgdkqqZjnstb0XaLqtqEQnA9cAyOK
4m7k5R3e69UL2s3L1r7mBID7nWslGXSQIFcbWeV0Lrxo/FAroHfDilf/EeVIqUMa
p83QhQKBgEW4x5q+oezWjSJk3nzkEtRq62I1nsuVi5wjDMECA/4ws1w8IX99mbC1
5mGy0BA/O1VM5CNbCDOjfpsrlChXNBwuhCFjkl6hAasL3ETXA2FP4LQJC8NmrI+G
BQmQjWQfnDMAOkY1v8maFjkmiBB1CTGMEFC632MExAvnLwr0oFgf
-----END RSA PRIVATE KEY-----
";

const TEST_COMPARTMENT: &str = "ocid1.compartment.oc1..comp001";
const TEST_TENANCY: &str = "ocid1.tenancy.oc1..test001";

fn temp_dir() -> String {
    static N: AtomicUsize = AtomicUsize::new(0);
    let d = format!("{}/oci_test-{}-{}", env!("CARGO_TARGET_TMPDIR"), std::process::id(), N.fetch_add(1, Ordering::Relaxed));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn write_file(path: &str, content: &str) {
    std::fs::write(path, content).unwrap();
}

fn l(v: &LabelSet, k: &str) -> String {
    v.0.get(k).cloned().unwrap_or_default()
}

// ---- test helpers -----------------------------------------------------------

fn s(v: &str) -> Option<String> {
    Some(v.to_string())
}

/// makeInstance: a core.Instance with sensible defaults.
fn make_instance() -> Instance {
    Instance {
        id: s("ocid1.instance.oc1.iad.aaa001"),
        display_name: s("test-instance-1"),
        shape: s("VM.Standard.E4.Flex"),
        availability_domain: s("US-ASHBURN-AD-1"),
        fault_domain: s("FAULT-DOMAIN-1"),
        image_id: s("ocid1.image.oc1.iad.img001"),
        region: s("us-ashburn-1"),
        compartment_id: s("ocid1.compartment.oc1..comp001"),
        lifecycle_state: "RUNNING".to_string(),
        freeform_tags: Vec::new(),
        defined_tags: Vec::new(),
    }
}

/// The wire JSON of an instance (as ListInstances returns it).
fn instance_json(i: &Instance) -> Json {
    let mut o = Json::obj();
    let put = |o: &mut Json, k: &str, v: &Option<String>| {
        if let Some(v) = v {
            o.set(k, v.as_str());
        }
    };
    put(&mut o, "id", &i.id);
    put(&mut o, "displayName", &i.display_name);
    put(&mut o, "shape", &i.shape);
    put(&mut o, "availabilityDomain", &i.availability_domain);
    put(&mut o, "faultDomain", &i.fault_domain);
    put(&mut o, "imageId", &i.image_id);
    put(&mut o, "region", &i.region);
    put(&mut o, "compartmentId", &i.compartment_id);
    o.set("lifecycleState", i.lifecycle_state.as_str());
    o.set("timeCreated", "2026-01-01T00:00:00.000Z");
    o.set("freeformTags", Json::Obj(i.freeform_tags.iter().map(|(k, v)| (k.clone(), Json::Str(v.clone()))).collect()));
    o.set("definedTags", Json::Obj(i.defined_tags.iter().map(|(k, v)| (k.clone(), Json::Obj(v.clone()))).collect()));
    o
}

/// The wire JSON of a VNIC.
fn vnic_json(id: &str, primary: bool, private_ip: Option<&str>, public_ip: Option<&str>, ipv6: &[&str], hostname: Option<&str>) -> Json {
    let mut o = Json::obj().with("id", id).with("isPrimary", primary).with("lifecycleState", "AVAILABLE");
    if let Some(v) = private_ip {
        o.set("privateIp", v);
    }
    if let Some(v) = public_ip {
        o.set("publicIp", v);
    }
    if !ipv6.is_empty() {
        o.set("ipv6Addresses", Json::Arr(ipv6.iter().map(|a| Json::Str(a.to_string())).collect()));
    }
    if let Some(v) = hostname {
        o.set("hostnameLabel", v);
    }
    o
}

/// makeVnic: a primary VNIC; public_ip may be empty.
fn make_vnic(id: &str, private_ip: &str, public_ip: &str) -> Json {
    vnic_json(id, true, Some(private_ip), (!public_ip.is_empty()).then_some(public_ip), &[], None)
}

/// makeAttachment: a single ATTACHED VnicAttachment.
fn make_attachment(instance_id: &str, vnic_id: &str) -> Json {
    Json::obj().with("id", "ocid1.vnicattachment.oc1..att001").with("instanceId", instance_id).with("vnicId", vnic_id).with("lifecycleState", "ATTACHED")
}

/// primaryOnly.
fn primary_only(id: &str, private_ip: &str, public_ip: &str) -> InstanceVnics {
    InstanceVnics { primary: VnicInfo { id: id.to_string(), private_ip: private_ip.to_string(), public_ip: public_ip.to_string(), ..Default::default() } }
}

fn json_resp(v: Json) -> MockResponse {
    MockResponse::json(v.stringify())
}

fn arr(v: Vec<Json>) -> Json {
    Json::Arr(v)
}

/// An OCI error response that the SDK does not retry.
fn not_found(msg: &str) -> MockResponse {
    MockResponse::new(404, "application/json", Json::obj().with("code", "NotAuthorizedOrNotFound").with("message", msg).stringify()).header("opc-request-id", "req-404")
}

const API: &str = "/20160918";

/// The mock OCI API: a handler per operation. `None` handlers return an empty list.
type Handler = Arc<dyn Fn(&str) -> MockResponse + Send + Sync>;

#[derive(Default, Clone)]
struct Api {
    instances: Option<Handler>,
    attachments: Option<Handler>,
    vnic: Option<Handler>,
}

fn serve(api: Api) -> MockServer {
    MockServer::from_fn(move |r: &MockRequest| {
        let p = r.path.as_str();
        if p == format!("{API}/instances") {
            let c = r.param("compartmentId").unwrap_or_default();
            return api.instances.as_ref().map(|f| f(&c)).unwrap_or_else(|| json_resp(arr(vec![])));
        }
        if p == format!("{API}/vnicAttachments") {
            let i = r.param("instanceId").unwrap_or_default();
            return api.attachments.as_ref().map(|f| f(&i)).unwrap_or_else(|| json_resp(arr(vec![])));
        }
        if let Some(id) = p.strip_prefix(&format!("{API}/vnics/")) {
            return match &api.vnic {
                Some(f) => f(id),
                None => not_found("vnic not found"),
            };
        }
        MockResponse::status(404, "404 page not found\n")
    })
}

/// singleInstanceDiscovery: serves exactly one instance and VNIC.
fn single_instance_api(inst: Instance, vnic: Json) -> Api {
    let att = make_attachment(inst.id.as_deref().unwrap_or(""), vnic.str_of("id"));
    let ij = instance_json(&inst);
    Api {
        instances: Some(Arc::new(move |_| json_resp(arr(vec![ij.clone()])))),
        attachments: Some(Arc::new(move |_| json_resp(arr(vec![att.clone()])))),
        vnic: Some(Arc::new(move |_| json_resp(vnic.clone()))),
    }
}

fn key_file() -> String {
    let p = format!("{}/oci.pem", temp_dir());
    write_file(&p, TEST_KEY);
    p
}

fn base_config(compartments: &[&str]) -> SdConfig {
    SdConfig {
        auth: AUTH_API_KEY.to_string(),
        region: "us-ashburn-1".to_string(),
        tenancy: TEST_TENANCY.to_string(),
        user: "ocid1.user.oc1..u001".to_string(),
        fingerprint: "aa:bb:cc".to_string(),
        key_file: key_file(),
        compartments: compartments.iter().map(|c| c.to_string()).collect(),
        port: 9100,
        refresh_interval: yaml::Duration::from_secs(60),
        http_client_config: net::HttpClientConfig::default_config(),
        ..Default::default()
    }
}

/// baseDiscovery: a Discovery against the mock server, compartments [testCompartment] unless given.
fn mock_discovery(server: &MockServer, compartments: &[&str]) -> Discovery {
    let cfg = base_config(compartments);
    let reg = Registry::new();
    let refresh_metrics = discovery::new_refresh_metrics(&reg);
    let metrics = cfg.new_discoverer_metrics(&reg, refresh_metrics);
    metrics.register().unwrap();
    let opts = DiscovererOptions { logger: Logger::nop(), metrics: Some(metrics), set_name: "oci".to_string(), ..Default::default() };
    Discovery::new_with_endpoints(&cfg, opts, Some(Endpoints { identity: server.url.clone(), iaas: server.url.clone() })).unwrap()
}

fn base_discovery(server: &MockServer) -> Discovery {
    mock_discovery(server, &[TEST_COMPARTMENT])
}

fn count(server: &MockServer, prefix: &str) -> usize {
    server.requests().iter().filter(|r| r.path.starts_with(&format!("{API}{prefix}"))).count()
}

/// A client of the mock server (for the compartment walk).
fn mock_client(server: &MockServer) -> Client {
    let key = discovery::oci::key::private_key_from_bytes(TEST_KEY.as_bytes(), None).unwrap();
    Client {
        http: net::new_client_from_config(&net::HttpClientConfig::default_config(), &net::HttpClientOptions::default()).unwrap(),
        signer: Arc::new(Signer { key_id: format!("{TEST_TENANCY}/ocid1.user.oc1..u001/aa:bb:cc"), key }),
        endpoints: Endpoints { identity: server.url.clone(), iaas: server.url.clone() },
        user_agent: "Prometheus/3.16.0".to_string(),
        limiter: Arc::new(Limiter::new(RATE_LIMIT_RPS, RATE_LIMIT_BURST)),
    }
}

// ---- instanceLabels ---------------------------------------------------------

#[test]
fn test_instance_labels_core_labels() {
    let inst = make_instance();
    let labels = instance_labels(&inst, &primary_only("ocid1.vnic.oc1..v001", "10.0.0.5", "203.0.113.1"), TEST_TENANCY, 9100);
    let expected = [
        (ADDRESS_LABEL, "10.0.0.5:9100"),
        (OCI_LABEL_INSTANCE_ID, "ocid1.instance.oc1.iad.aaa001"),
        (OCI_LABEL_INSTANCE_NAME, "test-instance-1"),
        (OCI_LABEL_INSTANCE_STATE, "RUNNING"),
        (OCI_LABEL_INSTANCE_SHAPE, "VM.Standard.E4.Flex"),
        (OCI_LABEL_AVAILABILITY_DOMAIN, "US-ASHBURN-AD-1"),
        (OCI_LABEL_FAULT_DOMAIN, "FAULT-DOMAIN-1"),
        (OCI_LABEL_REGION, "us-ashburn-1"),
        (OCI_LABEL_TENANCY_ID, TEST_TENANCY),
        (OCI_LABEL_COMPARTMENT_ID, "ocid1.compartment.oc1..comp001"),
        (OCI_LABEL_IMAGE_ID, "ocid1.image.oc1.iad.img001"),
        (OCI_LABEL_VNIC_ID, "ocid1.vnic.oc1..v001"),
        (OCI_LABEL_PRIVATE_IP, "10.0.0.5"),
        (OCI_LABEL_PUBLIC_IP, "203.0.113.1"),
    ];
    for (k, want) in expected {
        assert_eq!(want, l(&labels, k), "label {k}");
    }
}

#[test]
fn test_instance_labels_address_is_private_ip_when_both_present() {
    let labels = instance_labels(&make_instance(), &primary_only("v", "10.0.0.5", "203.0.113.1"), "tenancy", 80);
    assert_eq!("10.0.0.5:80", l(&labels, ADDRESS_LABEL));
}

#[test]
fn test_instance_labels_address_falls_back_to_public_ip() {
    let labels = instance_labels(&make_instance(), &primary_only("v", "", "203.0.113.1"), "tenancy", 80);
    assert_eq!("203.0.113.1:80", l(&labels, ADDRESS_LABEL));
}

#[test]
fn test_instance_labels_freeform_tags_sanitized() {
    // Sanitization replaces invalid characters with underscores but preserves case so that case-sensitive
    // OCI tag keys do not collide (e.g. Env vs env).
    let mut inst = make_instance();
    inst.freeform_tags = [("environment", "production"), ("My-Tag.Key", "value"), ("123numeric", "v"), ("Env", "upper"), ("env", "lower")]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    let labels = instance_labels(&inst, &primary_only("v", "10.0.0.1", ""), "tenancy", 80);
    assert_eq!("production", l(&labels, &format!("{OCI_LABEL_FREEFORM_TAG_PREFIX}environment")));
    assert_eq!("value", l(&labels, &format!("{OCI_LABEL_FREEFORM_TAG_PREFIX}My_Tag_Key")));
    assert_eq!("v", l(&labels, &format!("{OCI_LABEL_FREEFORM_TAG_PREFIX}123numeric")));
    assert_eq!("upper", l(&labels, &format!("{OCI_LABEL_FREEFORM_TAG_PREFIX}Env")));
    assert_eq!("lower", l(&labels, &format!("{OCI_LABEL_FREEFORM_TAG_PREFIX}env")));
}

#[test]
fn test_instance_labels_defined_tags_stringify_scalars() {
    // OCI defined tag values may be strings, numbers, or booleans. The SDK decodes JSON numbers as float64.
    // All scalars are stringified so that keep/drop relabel rules on numeric or boolean tags match correctly.
    let mut inst = make_instance();
    inst.defined_tags = vec![(
        "Oracle-Tags".to_string(),
        vec![
            ("CreatedBy".to_string(), Json::Str("user@example.com".to_string())),
            ("CostCenter".to_string(), Json::Str("42".to_string())),
            ("FloatValue".to_string(), Json::Float(100.0)),
            ("DecimalRate".to_string(), Json::Float(0.5)),
            ("BoolValue".to_string(), Json::Bool(true)),
            ("IntValue".to_string(), Json::Int(7)),
            ("NilValue".to_string(), Json::Null),
            ("SliceValue".to_string(), Json::Arr(vec![Json::Str("a".to_string())])),
        ],
    )];
    let labels = instance_labels(&inst, &primary_only("v", "10.0.0.1", ""), "tenancy", 80);
    let p = OCI_LABEL_DEFINED_TAG_PREFIX;
    assert_eq!("user@example.com", l(&labels, &format!("{p}Oracle_Tags_CreatedBy")));
    assert_eq!("42", l(&labels, &format!("{p}Oracle_Tags_CostCenter")));
    assert_eq!("100", l(&labels, &format!("{p}Oracle_Tags_FloatValue")));
    assert_eq!("0.5", l(&labels, &format!("{p}Oracle_Tags_DecimalRate")));
    assert_eq!("true", l(&labels, &format!("{p}Oracle_Tags_BoolValue")));
    assert_eq!("7", l(&labels, &format!("{p}Oracle_Tags_IntValue")));
    assert!(!labels.0.contains_key(&format!("{p}Oracle_Tags_NilValue")), "nil defined tag must not be emitted");
    assert!(!labels.0.contains_key(&format!("{p}Oracle_Tags_SliceValue")), "non-scalar defined tag must not be emitted");
}

#[test]
#[allow(clippy::approx_constant)]
fn test_stringify_defined_tag() {
    // adapted: Go's int, int32, int64 and float32 cases have no JSON form of their own; JSON integers are
    // Json::Int and the float32 case is the same value as a float64.
    let cases: Vec<(&str, Json, &str, bool)> = vec![
        ("string", Json::Str("abc".to_string()), "abc", true),
        ("empty string", Json::Str(String::new()), "", true),
        ("bool true", Json::Bool(true), "true", true),
        ("bool false", Json::Bool(false), "false", true),
        ("float64 whole", Json::Float(42.0), "42", true),
        ("float64 fraction", Json::Float(3.14), "3.14", true),
        ("float32", Json::Float(1.5), "1.5", true),
        ("int", Json::Int(7), "7", true),
        ("int64", Json::Int(-9), "-9", true),
        ("int32", Json::Int(3), "3", true),
        ("nil", Json::Null, "", false),
        ("slice", Json::Arr(vec![Json::Str("a".to_string())]), "", false),
        ("map", Json::obj().with("a", Json::Int(1)), "", false),
    ];
    for (name, input, want, ok) in cases {
        let got = stringify_defined_tag(&input);
        assert_eq!(ok, got.is_some(), "{name}");
        assert_eq!(want, got.unwrap_or_default(), "{name}");
    }
}

#[test]
fn test_instance_labels_nil_optional_fields() {
    let inst = Instance { lifecycle_state: "RUNNING".to_string(), ..Default::default() };
    let labels = instance_labels(&inst, &primary_only("", "10.0.0.1", ""), "tenancy", 80);
    assert_eq!("10.0.0.1:80", l(&labels, ADDRESS_LABEL));
    assert_eq!(Some(&String::new()), labels.0.get(OCI_LABEL_INSTANCE_ID));
    assert_eq!(Some(&String::new()), labels.0.get(OCI_LABEL_IMAGE_ID));
}

#[test]
fn test_instance_labels_port_in_address() {
    let inst = make_instance();
    for port in [80, 9100, 9182, 443] {
        let labels = instance_labels(&inst, &primary_only("v", "10.0.0.1", ""), "tenancy", port);
        assert_eq!(format!("10.0.0.1:{port}"), l(&labels, ADDRESS_LABEL), "port {port}");
    }
}

#[test]
fn test_instance_labels_i_pv6_and_hostname() {
    let vnics = InstanceVnics {
        primary: VnicInfo {
            id: "ocid1.vnic.oc1..v001".to_string(),
            private_ip: "10.0.0.5".to_string(),
            ipv6_addresses: vec!["2001:db8::1".to_string(), "2001:db8::2".to_string()],
            hostname_label: "web-1".to_string(),
            ..Default::default()
        },
    };
    let labels = instance_labels(&make_instance(), &vnics, TEST_TENANCY, 9100);
    assert_eq!(",2001:db8::1,2001:db8::2,", l(&labels, OCI_LABEL_IPV6_ADDRESSES));
    assert_eq!("web-1", l(&labels, OCI_LABEL_HOSTNAME_LABEL));
}

#[test]
fn test_instance_labels_i_pv6_and_hostname_empty_when_absent() {
    // Both labels are set unconditionally, empty when the VNIC has no IPv6 address or hostname label.
    let labels = instance_labels(&make_instance(), &primary_only("v", "10.0.0.1", ""), "tenancy", 80);
    assert_eq!(Some(&String::new()), labels.0.get(OCI_LABEL_IPV6_ADDRESSES));
    assert_eq!(Some(&String::new()), labels.0.get(OCI_LABEL_HOSTNAME_LABEL));
}

#[test]
fn test_join_i_pv6() {
    assert!(join_ipv6(&[]).is_empty());
    assert!(join_ipv6(&Vec::new()).is_empty());
    assert_eq!(",2001:db8::1,", join_ipv6(&["2001:db8::1".to_string()]));
    assert_eq!(",2001:db8::1,2001:db8::2,", join_ipv6(&["2001:db8::1".to_string(), "2001:db8::2".to_string()]));
}

#[test]
fn test_join_i_pv6_matchable_by_regex() {
    // The list is comma-wrapped so a relabel regex can match a whole address with `.*,addr,.*`, including the
    // first and last entries (require.Regexp is an unanchored search).
    let value = join_ipv6(&["2001:db8::1".to_string(), "2001:db8::2".to_string()]);
    for addr in ["2001:db8::1", "2001:db8::2"] {
        assert!(model::regexp::Regexp::must_compile(&format!(".*,{addr},.*")).is_match(&value));
    }
    assert!(!model::regexp::Regexp::must_compile(".*,2001:db8::3,.*").is_match(&value));
}

#[test]
fn test_instance_labels_i_pv6_only_address() {
    // With no IPv4 address the target falls back to the first (sorted) IPv6 address, bracketed by JoinHostPort.
    let vnics = InstanceVnics {
        primary: VnicInfo { id: "ocid1.vnic.oc1..v001".to_string(), ipv6_addresses: vec!["2001:db8::1".to_string(), "2001:db8::2".to_string()], ..Default::default() },
    };
    let labels = instance_labels(&make_instance(), &vnics, TEST_TENANCY, 9100);
    assert_eq!("[2001:db8::1]:9100", l(&labels, ADDRESS_LABEL));
}

// ---- refresh ----------------------------------------------------------------

#[test]
fn test_refresh_single_instance() {
    let inst = make_instance();
    let vnic = make_vnic("ocid1.vnic.oc1..v001", "10.0.0.5", "203.0.113.10");
    let server = serve(single_instance_api(inst, vnic));
    let mut d = base_discovery(&server);

    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());
    assert_eq!("OCI", tgs[0].source);
    assert_eq!(1, tgs[0].targets.len());

    let labels = &tgs[0].targets[0];
    assert_eq!("10.0.0.5:9100", l(labels, ADDRESS_LABEL));
    assert_eq!("ocid1.instance.oc1.iad.aaa001", l(labels, OCI_LABEL_INSTANCE_ID));
    assert_eq!("ocid1.vnic.oc1..v001", l(labels, OCI_LABEL_VNIC_ID));
    assert_eq!("10.0.0.5", l(labels, OCI_LABEL_PRIVATE_IP));
    assert_eq!("203.0.113.10", l(labels, OCI_LABEL_PUBLIC_IP));
    assert_eq!(TEST_TENANCY, l(labels, OCI_LABEL_TENANCY_ID));

    // The SDK request shapes and signature (oci-go-sdk ListInstances, ListVnicAttachments, GetVnic).
    let reqs = server.requests();
    let targets: Vec<String> = reqs.iter().map(|r| format!("{} {}?{}", r.method, r.path, r.query)).collect();
    assert_eq!(
        vec![
            format!("GET {API}/instances?compartmentId={TEST_COMPARTMENT}&limit=100"),
            format!("GET {API}/vnicAttachments?compartmentId={TEST_COMPARTMENT}&instanceId=ocid1.instance.oc1.iad.aaa001&limit=100"),
            format!("GET {API}/vnics/ocid1.vnic.oc1..v001?"),
        ],
        targets
    );
    let key = discovery::oci::key::private_key_from_bytes(TEST_KEY.as_bytes(), None).unwrap();
    let public = ring::signature::UnparsedPublicKey::new(&ring::signature::RSA_PKCS1_2048_8192_SHA256, ring::signature::KeyPair::public_key(&key).as_ref().to_vec());
    for r in &reqs {
        assert_eq!("Prometheus/3.16.0", r.header("user-agent"));
        assert_eq!("Oracle-GoSDK/65.127.0", r.header("opc-client-info"));
        assert_eq!("true", r.header("opc-client-retries"));
        assert_eq!(32, r.header("opc-request-id").len());
        let auth = r.header("authorization");
        let prefix = format!("Signature version=\"1\",headers=\"date (request-target) host\",keyId=\"{TEST_TENANCY}/ocid1.user.oc1..u001/aa:bb:cc\",algorithm=\"rsa-sha256\",signature=\"");
        assert!(auth.starts_with(&prefix), "{auth}");
        let sig = crypto::base64_decode(auth[prefix.len()..].trim_end_matches('"')).unwrap();
        let target = if r.query.is_empty() { r.path.clone() } else { format!("{}?{}", r.path, r.query) };
        let signing = format!("date: {}\n(request-target): get {}\nhost: {}", r.header("date"), target, r.header("host"));
        public.verify(signing.as_bytes(), &sig).unwrap();
    }
}

#[test]
fn test_refresh_primary_vnic_i_pv6_sorted_and_hostname() {
    // The primary VNIC's IPv6 addresses are exposed sorted for deterministic output even though OCI returns
    // them in an unspecified order.
    let vnic = vnic_json("ocid1.vnic.oc1..v001", true, Some("10.0.0.5"), None, &["2001:db8::2", "2001:db8::1"], Some("web-1"));
    let server = serve(single_instance_api(make_instance(), vnic));
    let mut d = base_discovery(&server);

    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());
    assert_eq!(1, tgs[0].targets.len());
    let labels = &tgs[0].targets[0];
    assert_eq!(",2001:db8::1,2001:db8::2,", l(labels, OCI_LABEL_IPV6_ADDRESSES));
    assert_eq!("web-1", l(labels, OCI_LABEL_HOSTNAME_LABEL));
}

#[test]
fn test_refresh_i_pv6_only_instance_not_skipped() {
    // An IPv6-only primary VNIC (no IPv4 private or public IP) is still discovered, with its first IPv6
    // address used as the target address.
    let vnic = vnic_json("ocid1.vnic.oc1..v001", true, None, None, &["2001:db8::2", "2001:db8::1"], None);
    let server = serve(single_instance_api(make_instance(), vnic));
    let mut d = base_discovery(&server);

    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());
    assert_eq!(1, tgs[0].targets.len());
    let labels = &tgs[0].targets[0];
    assert_eq!("[2001:db8::1]:9100", l(labels, ADDRESS_LABEL));
    assert_eq!(",2001:db8::1,2001:db8::2,", l(labels, OCI_LABEL_IPV6_ADDRESSES));
}

#[test]
fn test_resolve_vnics_does_not_mutate_sdk_slice() {
    // adapted: Go checks that sorting does not reorder the slice owned by the SDK response. Here the VNIC as
    // GetVnic returns it keeps the API order after its labels were built from a sorted copy.
    let vnic = vnic_json("ocid1.vnic.oc1..v001", true, Some("10.0.0.5"), None, &["2001:db8::2", "2001:db8::1"], None);
    let server = serve(single_instance_api(make_instance(), vnic));
    let mut d = base_discovery(&server);
    d.refresh(&CancelToken::new()).unwrap();

    let fetched = mock_client(&server).get_vnic(&CancelToken::new(), "ocid1.vnic.oc1..v001").unwrap();
    let info = primary_vnic_info(&fetched);
    assert_eq!(vec!["2001:db8::1".to_string(), "2001:db8::2".to_string()], info.ipv6_addresses);
    assert_eq!(vec!["2001:db8::2".to_string(), "2001:db8::1".to_string()], fetched.ipv6_addresses);
}

#[test]
fn test_refresh_resolves_primary_vnic_and_stops() {
    // Only the primary VNIC is resolved: once it is found the walk stops so a multi-VNIC instance does not
    // fan out a GetVnic call per attachment.
    let inst = make_instance();
    let iid = inst.id.clone().unwrap();
    let primary = vnic_json("ocid1.vnic.oc1..p", true, Some("10.0.0.1"), Some("203.0.113.1"), &[], None);
    let secondary = vnic_json("ocid1.vnic.oc1..s", false, Some("10.0.0.2"), Some("203.0.113.2"), &[], None);
    let ij = instance_json(&inst);
    let atts = arr(vec![make_attachment(&iid, "ocid1.vnic.oc1..p"), make_attachment(&iid, "ocid1.vnic.oc1..s")]);
    let server = serve(Api {
        instances: Some(Arc::new(move |_| json_resp(arr(vec![ij.clone()])))),
        attachments: Some(Arc::new(move |_| json_resp(atts.clone()))),
        vnic: Some(Arc::new(move |id| match id {
            "ocid1.vnic.oc1..p" => json_resp(primary.clone()),
            "ocid1.vnic.oc1..s" => json_resp(secondary.clone()),
            _ => not_found("unexpected VNIC ID"),
        })),
    });
    let mut d = base_discovery(&server);

    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());
    assert_eq!(1, tgs[0].targets.len());
    let labels = &tgs[0].targets[0];
    assert_eq!("10.0.0.1:9100", l(labels, ADDRESS_LABEL));
    assert_eq!("ocid1.vnic.oc1..p", l(labels, OCI_LABEL_VNIC_ID));
    assert_eq!("10.0.0.1", l(labels, OCI_LABEL_PRIVATE_IP));
    assert_eq!("203.0.113.1", l(labels, OCI_LABEL_PUBLIC_IP));
    assert_eq!(1, count(&server, "/vnics/"), "must stop after resolving the primary VNIC");
}

#[test]
fn test_refresh_multiple_compartments() {
    let (comp1, comp2, comp3) = ("ocid1.compartment.oc1..c001", "ocid1.compartment.oc1..c002", "ocid1.compartment.oc1..c003");
    let mut inst1 = make_instance();
    inst1.id = s("ocid1.instance.oc1..i001");
    inst1.compartment_id = s(comp1);
    let mut inst2 = make_instance();
    inst2.id = s("ocid1.instance.oc1..i002");
    inst2.compartment_id = s(comp2);
    let (i1, i2) = (instance_json(&inst1), instance_json(&inst2));
    let vnic = make_vnic("ocid1.vnic.oc1..v001", "10.0.0.1", "");
    let server = serve(Api {
        instances: Some(Arc::new(move |c| match c {
            "ocid1.compartment.oc1..c001" => json_resp(arr(vec![i1.clone()])),
            "ocid1.compartment.oc1..c002" => json_resp(arr(vec![i2.clone()])),
            _ => json_resp(arr(vec![])), // comp3 is empty
        })),
        attachments: Some(Arc::new(|iid| json_resp(arr(vec![make_attachment(iid, "ocid1.vnic.oc1..v001")])))),
        vnic: Some(Arc::new(move |_| json_resp(vnic.clone()))),
    });
    let mut d = mock_discovery(&server, &[comp1, comp2, comp3]);

    let tgs = d.refresh(&CancelToken::new()).unwrap();
    // All compartments are merged into a single group; the empty comp3 contributes no targets.
    assert_eq!(1, tgs.len());
    assert_eq!("OCI", tgs[0].source);
    assert_eq!(2, tgs[0].targets.len());
    let compartments: std::collections::BTreeSet<String> = tgs[0].targets.iter().map(|t| l(t, OCI_LABEL_COMPARTMENT_ID)).collect();
    assert_eq!(std::collections::BTreeSet::from([comp1.to_string(), comp2.to_string()]), compartments);
}

#[test]
fn test_refresh_disappearing_compartment_cleared() {
    let (comp1, comp2) = ("ocid1.compartment.oc1..c001", "ocid1.compartment.oc1..c002");
    let vnic = make_vnic("ocid1.vnic.oc1..v001", "10.0.0.1", "");
    let server = serve(Api {
        instances: Some(Arc::new(|c| {
            let mut inst = make_instance();
            inst.id = Some(format!("ocid1.instance.oc1..{c}"));
            inst.compartment_id = s(c);
            json_resp(arr(vec![instance_json(&inst)]))
        })),
        attachments: Some(Arc::new(|iid| json_resp(arr(vec![make_attachment(iid, "ocid1.vnic.oc1..v001")])))),
        vnic: Some(Arc::new(move |_| json_resp(vnic.clone()))),
    });
    let mut d = base_discovery(&server);
    // The compartment list changes between refreshes (Go replaces the listCompartments closure).
    let compartments = Arc::new(Mutex::new(vec![comp1.to_string(), comp2.to_string()]));
    let c = compartments.clone();
    d.refresher().list_compartments = Box::new(move |_ctx: &CancelToken| Ok(c.lock().unwrap().clone()));

    // First refresh: both compartments yield a target in the single group.
    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());
    assert_eq!("OCI", tgs[0].source);
    assert_eq!(2, tgs[0].targets.len());

    // comp2 disappears: only comp1's target remains, comp2's is gone.
    *compartments.lock().unwrap() = vec![comp1.to_string()];
    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());
    assert_eq!("OCI", tgs[0].source);
    assert_eq!(1, tgs[0].targets.len());
    assert_eq!(comp1, l(&tgs[0].targets[0], OCI_LABEL_COMPARTMENT_ID));

    // Every compartment disappears: the group is emitted empty so the manager clears the remaining targets.
    compartments.lock().unwrap().clear();
    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());
    assert_eq!("OCI", tgs[0].source);
    assert!(tgs[0].targets.is_empty());
}

#[test]
fn test_refresh_compartment_list_error_propagates() {
    // Go injects the error through the listCompartments closure: the compartment walk itself fails only on a
    // cancelled context.
    let server = serve(Api::default());
    let mut d = base_discovery(&server);
    d.refresher().list_compartments = Box::new(|_ctx: &CancelToken| Err("identity API unavailable".to_string()));
    let err = d.refresh(&CancelToken::new()).unwrap_err();
    assert_eq!("oci_sd: failed to list compartments: identity API unavailable", err);
}

#[test]
fn test_refresh_list_instances_error_propagates_error() {
    // A ListInstances error fails the refresh as a whole so the refresh framework keeps the last known good
    // targets rather than flapping the compartment's targets down for a cycle.
    let server = serve(Api { instances: Some(Arc::new(|_| not_found("OCI API unavailable"))), ..Default::default() });
    let mut d = base_discovery(&server);
    let err = d.refresh(&CancelToken::new()).unwrap_err();
    let want = format!(
        "listing OCI instances in compartment {TEST_COMPARTMENT}: Error returned by Compute Service. Http Status Code: 404. Error Code: NotAuthorizedOrNotFound. Opc request id: req-404. Message: OCI API unavailable\nOperation Name: ListInstances\n"
    );
    assert!(err.starts_with(&want), "{err}");
    assert!(err.contains(&format!("\nRequest Endpoint: GET {}{API}/instances?compartmentId={TEST_COMPARTMENT}&limit=100\n", server.url)), "{err}");
    assert!(err.contains("\nAlso see https://docs.oracle.com/iaas/api/#/en/iaas/20160918/Instance/ListInstances for details"), "{err}");
}

#[test]
fn test_refresh_empty_compartment() {
    let server = serve(Api::default());
    let mut d = base_discovery(&server);
    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());
    assert!(tgs[0].targets.is_empty());
}

#[test]
fn test_refresh_vnic_attachment_error_instance_skipped() {
    let ij = instance_json(&make_instance());
    let server = serve(Api {
        instances: Some(Arc::new(move |_| json_resp(arr(vec![ij.clone()])))),
        attachments: Some(Arc::new(|_| not_found("VNIC list failed"))),
        ..Default::default()
    });
    let mut d = base_discovery(&server);
    // Instance is skipped, compartment group is still returned (empty targets).
    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());
    assert!(tgs[0].targets.is_empty());
}

#[test]
fn test_refresh_get_vnic_error_instance_skipped() {
    let inst = make_instance();
    let att = make_attachment(inst.id.as_deref().unwrap(), "ocid1.vnic.oc1..v001");
    let ij = instance_json(&inst);
    let server = serve(Api {
        instances: Some(Arc::new(move |_| json_resp(arr(vec![ij.clone()])))),
        attachments: Some(Arc::new(move |_| json_resp(arr(vec![att.clone()])))),
        vnic: Some(Arc::new(|_| not_found("GetVnic failed"))),
    });
    let mut d = base_discovery(&server);
    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());
    assert!(tgs[0].targets.is_empty());
}

#[test]
fn test_refresh_no_primary_vnic_instance_dropped() {
    // An instance with no resolvable primary VNIC has no usable IP, so the SD drops it rather than emit an
    // unscrapeable :port target.
    let non_primary = vnic_json("ocid1.vnic.oc1..v001", false, Some("10.0.0.5"), None, &[], None);
    let server = serve(single_instance_api(make_instance(), non_primary));
    let mut d = base_discovery(&server);
    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());
    assert!(tgs[0].targets.is_empty(), "instance with no usable IP must be dropped");
}

#[test]
fn test_refresh_instance_with_nil_id_dropped() {
    // An instance without an OCID is skipped with a warning.
    let mut inst = make_instance();
    inst.id = None;
    let ij = instance_json(&inst);
    let server = serve(Api { instances: Some(Arc::new(move |_| json_resp(arr(vec![ij.clone()])))), ..Default::default() });
    let mut d = base_discovery(&server);
    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(1, tgs.len());
    assert!(tgs[0].targets.is_empty());
    assert_eq!(0, count(&server, "/vnicAttachments"), "instance with nil Id must not reach VNIC lookup");
}

#[test]
fn test_refresh_context_canceled() {
    // Auto-discovery: the compartment walk sees the cancelled context before any call.
    let server = serve(Api::default());
    let mut d = mock_discovery(&server, &[]);
    let ctx = CancelToken::new();
    ctx.cancel();
    let err = d.refresh(&ctx).unwrap_err();
    assert_eq!("oci_sd: failed to list compartments: context canceled", err);
    assert!(server.requests().is_empty());
}

// ---- listAllCompartments ----------------------------------------------------

/// fakeCompartmentTree, served as Identity ListCompartments responses: `children` of each parent, paged by
/// `page_size` (page tokens are start indices, sent back in `opc-next-page`), and parents whose listing
/// fails with 404 NotAuthorizedOrNotFound (a permission gap).
fn compartment_tree(children: &[(&str, Vec<(&str, &str)>)], page_size: usize, denied: &[&str]) -> MockServer {
    let children: Vec<(String, Vec<Json>)> = children
        .iter()
        .map(|(p, cs)| (p.to_string(), cs.iter().map(|(id, state)| Json::obj().with("id", *id).with("lifecycleState", *state).with("name", *id)).collect()))
        .collect();
    let denied: Vec<String> = denied.iter().map(|s| s.to_string()).collect();
    MockServer::from_fn(move |r: &MockRequest| {
        if r.path != format!("{API}/compartments")
            || r.param("accessLevel").as_deref() != Some("ACCESSIBLE")
            || r.param("compartmentIdInSubtree").as_deref() != Some("false")
            || r.param("limit").as_deref() != Some("100")
        {
            return MockResponse::status(400, "unexpected request\n");
        }
        let parent = r.param("compartmentId").unwrap_or_default();
        if denied.contains(&parent) {
            return not_found("permission denied");
        }
        let all = children.iter().find(|(p, _)| *p == parent).map(|(_, c)| c.clone()).unwrap_or_default();
        if page_size == 0 || page_size >= all.len() {
            return json_resp(arr(all));
        }
        let start: usize = r.param("page").map(|p| p.parse().unwrap()).unwrap_or(0);
        let end = start + page_size;
        if end >= all.len() {
            return json_resp(arr(all[start..].to_vec()));
        }
        json_resp(arr(all[start..end].to_vec())).header("opc-next-page", &end.to_string())
    })
}

fn calls(server: &MockServer, parent: &str) -> usize {
    server.requests().iter().filter(|r| r.param("compartmentId").as_deref() == Some(parent)).count()
}

fn walk(server: &MockServer, ctx: &CancelToken) -> Result<Vec<String>, String> {
    let c = mock_client(server);
    let mut paginate = |ctx: &CancelToken, parent: &str, page: Option<&str>| c.list_compartments_page(ctx, parent, page);
    list_all_compartments(ctx, &mut paginate, "root", &Logger::nop())
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

fn strs(v: &[&str]) -> Vec<String> {
    sorted(v.iter().map(|s| s.to_string()).collect())
}

#[test]
fn test_list_all_compartments_bfs_includes_root() {
    let tree = compartment_tree(&[("root", vec![("c1", "ACTIVE"), ("c2", "ACTIVE")]), ("c1", vec![("c1a", "ACTIVE")])], 0, &[]);
    let got = walk(&tree, &CancelToken::new()).unwrap();
    assert_eq!(strs(&["root", "c1", "c2", "c1a"]), sorted(got));
}

#[test]
fn test_list_all_compartments_paged_results() {
    let tree = compartment_tree(&[("root", vec![("c1", "ACTIVE"), ("c2", "ACTIVE"), ("c3", "ACTIVE"), ("c4", "ACTIVE"), ("c5", "ACTIVE")])], 2, &[]);
    let got = walk(&tree, &CancelToken::new()).unwrap();
    assert_eq!(strs(&["root", "c1", "c2", "c3", "c4", "c5"]), sorted(got));
    assert_eq!(3, calls(&tree, "root"), "5 children with page size 2 must take 3 calls");
}

#[test]
fn test_list_all_compartments_inactive_compartment_skipped() {
    let tree = compartment_tree(&[("root", vec![("c1", "ACTIVE"), ("c2-dead", "INACTIVE"), ("c3", "ACTIVE")])], 0, &[]);
    let got = walk(&tree, &CancelToken::new()).unwrap();
    assert_eq!(strs(&["root", "c1", "c3"]), sorted(got));
}

#[test]
fn test_list_all_compartments_cycle_visited_once() {
    // A child that references back to root must not cause an infinite loop.
    let tree = compartment_tree(&[("root", vec![("c1", "ACTIVE")]), ("c1", vec![("root", "ACTIVE")])], 0, &[]);
    let got = walk(&tree, &CancelToken::new()).unwrap();
    assert_eq!(strs(&["root", "c1"]), sorted(got));
    assert_eq!(1, calls(&tree, "root"), "root must be listed only once even when referenced as a child");
}

#[test]
fn test_list_all_compartments_permission_error_skips_subtree() {
    // c2 denied; its subtree must not abort the walk.
    let tree = compartment_tree(&[("root", vec![("c1", "ACTIVE"), ("c2", "ACTIVE")]), ("c1", vec![("c1a", "ACTIVE")])], 0, &["c2"]);
    let got = walk(&tree, &CancelToken::new()).unwrap();
    assert_eq!(strs(&["root", "c1", "c2", "c1a"]), sorted(got));
}

#[test]
fn test_list_all_compartments_canceled_context_aborts() {
    // A cancelled context must abort the walk immediately rather than be silently treated as a permission gap.
    let tree = compartment_tree(&[("root", vec![("c1", "ACTIVE")])], 0, &[]);
    let ctx = CancelToken::new();
    ctx.cancel();
    assert_eq!(Err("context canceled".to_string()), walk(&tree, &ctx));
}

// ---- SDConfig validation ----------------------------------------------------

fn valid_api_key() -> SdConfig {
    SdConfig {
        auth: AUTH_API_KEY.to_string(),
        region: "us-ashburn-1".to_string(),
        tenancy: "ocid1.tenancy.oc1..t001".to_string(),
        user: "ocid1.user.oc1..u001".to_string(),
        fingerprint: "aa:bb:cc".to_string(),
        key_file: "/etc/oci/key.pem".to_string(),
        refresh_interval: yaml::Duration::from_secs(60),
        http_client_config: net::HttpClientConfig::default_config(),
        ..Default::default()
    }
}

#[test]
fn test_sd_config_validation() {
    type Mutate = fn(&mut SdConfig);
    let cases: Vec<(&str, Mutate, &str)> = vec![
        ("missing region", |c| c.region.clear(), "region is required"),
        ("api_key missing tenancy", |c| c.tenancy.clear(), "requires tenancy"),
        ("api_key missing user", |c| c.user.clear(), "requires user"),
        ("api_key missing fingerprint", |c| c.fingerprint.clear(), "requires fingerprint"),
        ("api_key missing key_file", |c| c.key_file.clear(), "requires key_file"),
        (
            "api_key with both key_passphrase and key_passphrase_file",
            |c| {
                c.key_passphrase = yaml::Secret::new("secret");
                c.key_passphrase_file = "/etc/oci/passphrase".to_string();
            },
            "at most one of key_passphrase and key_passphrase_file",
        ),
        (
            "instance_principal with api_key fields",
            |c| c.auth = AUTH_INSTANCE_PRINCIPAL.to_string(), // Tenancy still set from validAPIKey.
            "must not have tenancy",
        ),
        (
            "instance_principal with key_passphrase_file",
            |c| {
                c.auth = AUTH_INSTANCE_PRINCIPAL.to_string();
                c.tenancy.clear();
                c.user.clear();
                c.fingerprint.clear();
                c.key_file.clear();
                c.key_passphrase_file = "/etc/oci/passphrase".to_string();
            },
            "must not have",
        ),
        (
            "unknown auth method",
            |c| {
                c.auth = "magic".to_string();
                c.tenancy.clear();
            },
            "unknown auth method",
        ),
        ("empty string in compartments list", |c| c.compartments = vec!["ocid1.compartment.oc1..good".to_string(), String::new()], "compartments[1] must not be empty"),
        ("refresh_interval zero", |c| c.refresh_interval = yaml::Duration(0), "refresh_interval must be positive"),
        ("refresh_interval negative", |c| c.refresh_interval = yaml::Duration::from_secs(-1), "refresh_interval must be positive"),
    ];
    for (name, mutate, want) in cases {
        let mut cfg = valid_api_key();
        mutate(&mut cfg);
        let err = cfg.validate().unwrap_err();
        assert!(err.contains(want), "{name}: {err}");
    }
}

#[test]
fn test_sd_config_valid_instance_principal() {
    let mut cfg = SdConfig {
        auth: AUTH_INSTANCE_PRINCIPAL.to_string(),
        region: "us-ashburn-1".to_string(),
        refresh_interval: yaml::Duration::from_secs(60),
        http_client_config: net::HttpClientConfig::default_config(),
        ..Default::default()
    };
    cfg.validate().unwrap();
}

#[test]
fn test_sd_config_valid_with_explicit_compartments() {
    let mut cfg = valid_api_key();
    cfg.compartments = vec!["ocid1.compartment.oc1..c001".to_string(), "ocid1.compartment.oc1..c002".to_string()];
    cfg.validate().unwrap();
}

#[test]
fn test_sd_config_valid_auto_discovery() {
    // Compartments empty -> auto-discover. Should be valid.
    let mut cfg = valid_api_key();
    cfg.validate().unwrap();
}

#[test]
fn test_sd_config_valid_with_key_passphrase_file() {
    let mut cfg = valid_api_key();
    cfg.key_passphrase_file = "/etc/oci/passphrase".to_string();
    cfg.validate().unwrap();
}

// ---- resolveKeyPassphrase ---------------------------------------------------

#[test]
fn test_resolve_key_passphrase_inline_preferred_when_set_alone() {
    let got = resolve_key_passphrase(&SdConfig { key_passphrase: yaml::Secret::new("topsecret"), ..Default::default() }).unwrap();
    assert_eq!("topsecret", got);
}

#[test]
fn test_resolve_key_passphrase_file_trims_trailing_newline() {
    let path = format!("{}/pass", temp_dir());
    write_file(&path, "topsecret\n");
    let got = resolve_key_passphrase(&SdConfig { key_passphrase_file: path, ..Default::default() }).unwrap();
    assert_eq!("topsecret", got);
}

#[test]
fn test_resolve_key_passphrase_file_missing() {
    let path = format!("{}/nope", temp_dir());
    assert!(resolve_key_passphrase(&SdConfig { key_passphrase_file: path, ..Default::default() }).is_err());
}

// ---- SetDirectory -----------------------------------------------------------

#[test]
fn test_sd_config_set_directory_relative_path() {
    let mut cfg = SdConfig { key_file: "keys/oci.pem".to_string(), ..Default::default() };
    cfg.set_directory("/etc/prometheus");
    assert_eq!("/etc/prometheus/keys/oci.pem", cfg.key_file);
}

#[test]
fn test_sd_config_set_directory_absolute_path_unchanged() {
    let abs = format!("{}/oci.pem", temp_dir());
    let mut cfg = SdConfig { key_file: abs.clone(), ..Default::default() };
    cfg.set_directory("/etc/prometheus");
    assert_eq!(abs, cfg.key_file);
}

#[test]
fn test_sd_config_set_directory_empty_key_file() {
    let mut cfg = SdConfig::default();
    cfg.set_directory("/etc/prometheus");
    assert!(cfg.key_file.is_empty());
}

#[test]
fn test_sd_config_set_directory_key_passphrase_file_joined() {
    let mut cfg = SdConfig { key_passphrase_file: "keys/oci.pass".to_string(), ..Default::default() };
    cfg.set_directory("/etc/prometheus");
    assert_eq!("/etc/prometheus/keys/oci.pass", cfg.key_passphrase_file);
}
