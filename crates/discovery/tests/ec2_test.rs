//! Port of discovery/aws/ec2_test.go. The Go tests mock the SDK client with Go structs; here the same data is
//! served as EC2 Query API XML responses by a local server, and the discovery talks to it through its
//! `endpoint` with static credentials.
mod cloud_mock;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use cloud_mock::{MockRequest, MockResponse, MockServer};
use discovery::aws::Filter;
use discovery::aws::ec2::*;
use discovery::aws::xml::escape;
use discovery::{CancelToken, DiscovererOptions, Group, Registry};
use model::labelset::LabelSet;

// ---- the Go ec2Types the tests fill in (None = a nil pointer, "" = an empty enum) ----

#[derive(Clone, Debug, Default)]
struct Ipv6 {
    ipv6_address: Option<String>,
    is_primary_ipv6: Option<bool>,
}

#[derive(Clone, Debug, Default)]
struct Eni {
    /// `Some(None)` = an attachment without a device index.
    attachment: Option<Option<i32>>,
    ipv6_addresses: Vec<Ipv6>,
    subnet_id: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct Tag {
    key: Option<String>,
    value: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct Instance {
    architecture: String,
    image_id: Option<String>,
    instance_id: Option<String>,
    instance_lifecycle: String,
    instance_type: String,
    /// `Some(None)` = a placement without an availability zone.
    placement: Option<Option<String>>,
    platform: String,
    private_dns_name: Option<String>,
    private_ip_address: Option<String>,
    public_dns_name: Option<String>,
    public_ip_address: Option<String>,
    state: Option<String>,
    subnet_id: Option<String>,
    vpc_id: Option<String>,
    network_interfaces: Vec<Eni>,
    tags: Vec<Tag>,
}

/// Go `ec2DataStore`.
#[derive(Clone, Debug, Default)]
struct Ec2DataStore {
    region: String,
    az_to_az_id: HashMap<String, String>,
    owner_id: String,
    instances: Vec<Instance>,
}

fn s(v: &str) -> Option<String> {
    Some(v.to_string())
}

fn el(out: &mut String, name: &str, v: &Option<String>) {
    if let Some(v) = v {
        out.push_str(&format!("<{name}>{}</{name}>", escape(v)));
    }
}

fn enum_el(out: &mut String, name: &str, v: &str) {
    if !v.is_empty() {
        out.push_str(&format!("<{name}>{}</{name}>", escape(v)));
    }
}

/// The instance as an `instancesSet` item of a DescribeInstances response.
fn instance_xml(i: &Instance) -> String {
    let mut o = String::from("<item>");
    el(&mut o, "instanceId", &i.instance_id);
    el(&mut o, "imageId", &i.image_id);
    if let Some(st) = &i.state {
        o.push_str(&format!("<instanceState><code>16</code><name>{}</name></instanceState>", escape(st)));
    }
    el(&mut o, "privateDnsName", &i.private_dns_name);
    el(&mut o, "dnsName", &i.public_dns_name);
    enum_el(&mut o, "instanceType", &i.instance_type);
    if let Some(p) = &i.placement {
        o.push_str("<placement>");
        el(&mut o, "availabilityZone", p);
        o.push_str("<tenancy>default</tenancy></placement>");
    }
    enum_el(&mut o, "platform", &i.platform);
    el(&mut o, "subnetId", &i.subnet_id);
    el(&mut o, "vpcId", &i.vpc_id);
    el(&mut o, "privateIpAddress", &i.private_ip_address);
    el(&mut o, "ipAddress", &i.public_ip_address);
    enum_el(&mut o, "architecture", &i.architecture);
    enum_el(&mut o, "instanceLifecycle", &i.instance_lifecycle);
    if !i.tags.is_empty() {
        o.push_str("<tagSet>");
        for t in &i.tags {
            o.push_str("<item>");
            el(&mut o, "key", &t.key);
            el(&mut o, "value", &t.value);
            o.push_str("</item>");
        }
        o.push_str("</tagSet>");
    }
    if !i.network_interfaces.is_empty() {
        o.push_str("<networkInterfaceSet>");
        for n in &i.network_interfaces {
            o.push_str("<item>");
            el(&mut o, "subnetId", &n.subnet_id);
            if let Some(a) = &n.attachment {
                o.push_str("<attachment><attachmentId>eni-attach-1</attachmentId>");
                el(&mut o, "deviceIndex", &a.map(|d| d.to_string()));
                o.push_str("<status>attached</status></attachment>");
            }
            o.push_str("<ipv6AddressesSet>");
            for a in &n.ipv6_addresses {
                o.push_str("<item>");
                el(&mut o, "ipv6Address", &a.ipv6_address);
                el(&mut o, "isPrimaryIpv6", &a.is_primary_ipv6.map(|b| b.to_string()));
                o.push_str("</item>");
            }
            o.push_str("</ipv6AddressesSet></item>");
        }
        o.push_str("</networkInterfaceSet>");
    }
    o.push_str("</item>");
    o
}

/// The DescribeAvailabilityZones response (an API error without zones, as the Go mock's "No AZs found").
fn describe_azs(d: &Ec2DataStore) -> MockResponse {
    if d.az_to_az_id.is_empty() {
        return MockResponse::new(400, "text/xml;charset=UTF-8", "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Response><Errors><Error><Code>InvalidParameterValue</Code><Message>No AZs found</Message></Error></Errors><RequestID>req-azs</RequestID></Response>");
    }
    let mut azs: Vec<_> = d.az_to_az_id.iter().collect();
    azs.sort();
    let mut o = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DescribeAvailabilityZonesResponse xmlns=\"http://ec2.amazonaws.com/doc/2016-11-15/\"><requestId>req-azs</requestId><availabilityZoneInfo>");
    for (name, id) in azs {
        o.push_str(&format!("<item><zoneState>available</zoneState><regionName>{}</regionName><zoneName>{}</zoneName><zoneId>{}</zoneId><zoneType>availability-zone</zoneType></item>", escape(&d.region), escape(name), escape(id)));
    }
    o.push_str("</availabilityZoneInfo></DescribeAvailabilityZonesResponse>");
    MockResponse::xml(o)
}

/// The DescribeInstances response: one reservation with the instances that pass an `instance-state-name`
/// filter (as the Go mock).
fn describe_instances(d: &Ec2DataStore, req: &MockRequest) -> MockResponse {
    let params = req.form_params();
    let mut names: BTreeMap<String, String> = BTreeMap::new();
    let mut values: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (k, v) in &params {
        let parts: Vec<&str> = k.split('.').collect();
        match parts.as_slice() {
            ["Filter", n, "Name"] => {
                names.insert(n.to_string(), v.clone());
            }
            ["Filter", n, "Value", _] => values.entry(n.to_string()).or_default().push(v.clone()),
            _ => {}
        }
    }
    let mut allowed: Vec<String> = Vec::new();
    let mut has_state_filter = false;
    for (n, name) in &names {
        if name != "instance-state-name" {
            continue;
        }
        has_state_filter = true;
        allowed.extend(values.get(n).cloned().unwrap_or_default());
    }
    let mut o = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DescribeInstancesResponse xmlns=\"http://ec2.amazonaws.com/doc/2016-11-15/\"><requestId>req-1</requestId><reservationSet><item><reservationId>r-1</reservationId>");
    o.push_str(&format!("<ownerId>{}</ownerId><groupSet/><instancesSet>", escape(&d.owner_id)));
    for i in &d.instances {
        if has_state_filter {
            match &i.state {
                Some(st) if allowed.contains(st) => {}
                _ => continue,
            }
        }
        o.push_str(&instance_xml(i));
    }
    o.push_str("</instancesSet></item></reservationSet></DescribeInstancesResponse>");
    MockResponse::xml(o)
}

fn handler(d: Ec2DataStore) -> impl Fn(&MockRequest) -> MockResponse + Send + Sync + 'static {
    move |req: &MockRequest| match req.param("Action").as_deref() {
        Some("DescribeAvailabilityZones") => describe_azs(&d),
        Some("DescribeInstances") => describe_instances(&d, req),
        _ => MockResponse::new(400, "text/xml;charset=UTF-8", "<Response><Errors><Error><Code>InvalidAction</Code><Message>unknown action</Message></Error></Errors><RequestID>x</RequestID></Response>"),
    }
}

/// Go `ec2TestDiscovery`: a discovery whose client talks to the mock server for `data` (port 4242, the data's
/// region).
fn ec2_test_discovery(data: &Ec2DataStore, filters: Vec<Filter>) -> (Ec2Discovery, MockServer) {
    let srv = MockServer::from_fn(handler(data.clone()));
    let cfg = Ec2SdConfig {
        port: 4242,
        region: data.region.clone(),
        filters,
        endpoint: srv.url.clone(),
        access_key: "AKIDEXAMPLE".to_string(),
        secret_key: yaml::Secret("secret".to_string()),
        http_client_config: net::HttpClientConfig::default_config(),
        ..Default::default()
    };
    let reg = Registry::new();
    let rmi = discovery::new_refresh_metrics(&reg);
    let d = Ec2Discovery::new(&cfg, DiscovererOptions { logger: discovery::Logger::nop(), metrics: Some(Ec2Metrics::new(rmi)), set_name: "ec2".to_string(), ..Default::default() }).unwrap();
    (d, srv)
}

fn azs(p: &[(&str, &str)]) -> HashMap<String, String> {
    p.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
}

fn ls(p: &[(&str, &str)]) -> LabelSet {
    LabelSet::from_pairs(p)
}

fn groups(tgs: Vec<Arc<Group>>) -> Vec<Group> {
    tgs.iter().map(|g| (**g).clone()).collect()
}

fn three_azs() -> HashMap<String, String> {
    azs(&[("azname-a", "azid-1"), ("azname-b", "azid-2"), ("azname-c", "azid-3")])
}

#[test]
fn test_ec2_discovery_refresh_azi_ds() {
    let ctx = CancelToken::new();
    for (name, should_fail, data) in [
        ("Normal", false, Ec2DataStore { az_to_az_id: three_azs(), region: "region-azids".to_string(), ..Default::default() }),
        ("HandleError", true, Ec2DataStore { region: "region-azids".to_string(), ..Default::default() }),
    ] {
        let (mut d, _srv) = ec2_test_discovery(&data, vec![]);
        let r = d.refresh_az_ids(&ctx);
        if should_fail {
            assert!(r.is_err(), "{name}");
        } else {
            assert!(r.is_ok(), "{name}: {r:?}");
            assert_eq!(Some(data.az_to_az_id.clone()), d.az_to_az_id(), "{name}");
        }
    }
}

#[test]
fn test_ec2_discovery_refresh() {
    let ctx = CancelToken::new();
    let i32p = |v: i32| Some(Some(v));
    let v6 = |a: &str, p: bool| Ipv6 { ipv6_address: s(a), is_primary_ipv6: Some(p) };
    struct Case {
        name: &'static str,
        data: Ec2DataStore,
        filters: Vec<Filter>,
        expected: Vec<Group>,
    }
    let cases = vec![
        Case {
            name: "NoPrivateIpOrIpv6",
            data: Ec2DataStore { region: "region-noprivateip".into(), az_to_az_id: three_azs(), instances: vec![Instance { instance_id: s("instance-id-noprivateip"), ..Default::default() }], ..Default::default() },
            filters: vec![],
            expected: vec![Group { source: "region-noprivateip".into(), ..Default::default() }],
        },
        Case {
            name: "NoVpc",
            data: Ec2DataStore {
                region: "region-novpc".into(),
                az_to_az_id: three_azs(),
                owner_id: "owner-id-novpc".into(),
                instances: vec![Instance {
                    // set every possible options and test them here
                    architecture: "architecture-novpc".into(),
                    image_id: s("ami-novpc"),
                    instance_id: s("instance-id-novpc"),
                    instance_lifecycle: "instance-lifecycle-novpc".into(),
                    instance_type: "instance-type-novpc".into(),
                    placement: Some(s("azname-b")),
                    platform: "platform-novpc".into(),
                    private_dns_name: s("private-dns-novpc"),
                    private_ip_address: s("1.2.3.4"),
                    public_dns_name: s("public-dns-novpc"),
                    public_ip_address: s("42.42.42.2"),
                    state: s("running"),
                    // test tags once and for all
                    tags: vec![
                        Tag { key: s("tag-1-key"), value: s("tag-1-value") },
                        Tag { key: s("tag-2-key"), value: s("tag-2-value") },
                        Tag::default(),
                        Tag { key: None, value: s("tag-4-value") },
                        Tag { key: s("tag-5-key"), value: None },
                    ],
                    ..Default::default()
                }],
            },
            filters: vec![],
            expected: vec![Group {
                source: "region-novpc".into(),
                targets: vec![ls(&[
                    ("__address__", "1.2.3.4:4242"),
                    ("__meta_ec2_ami", "ami-novpc"),
                    ("__meta_ec2_architecture", "architecture-novpc"),
                    ("__meta_ec2_availability_zone", "azname-b"),
                    ("__meta_ec2_availability_zone_id", "azid-2"),
                    ("__meta_ec2_instance_id", "instance-id-novpc"),
                    ("__meta_ec2_instance_lifecycle", "instance-lifecycle-novpc"),
                    ("__meta_ec2_instance_type", "instance-type-novpc"),
                    ("__meta_ec2_instance_state", "running"),
                    ("__meta_ec2_owner_id", "owner-id-novpc"),
                    ("__meta_ec2_platform", "platform-novpc"),
                    ("__meta_ec2_private_dns_name", "private-dns-novpc"),
                    ("__meta_ec2_private_ip", "1.2.3.4"),
                    ("__meta_ec2_public_dns_name", "public-dns-novpc"),
                    ("__meta_ec2_public_ip", "42.42.42.2"),
                    ("__meta_ec2_region", "region-novpc"),
                    ("__meta_ec2_tag_tag_1_key", "tag-1-value"),
                    ("__meta_ec2_tag_tag_2_key", "tag-2-value"),
                ])],
                ..Default::default()
            }],
        },
        Case {
            name: "Ipv4",
            data: Ec2DataStore {
                region: "region-ipv4".into(),
                az_to_az_id: three_azs(),
                instances: vec![Instance {
                    // just the minimum needed for the refresh work
                    image_id: s("ami-ipv4"),
                    instance_id: s("instance-id-ipv4"),
                    instance_type: "instance-type-ipv4".into(),
                    placement: Some(s("azname-c")),
                    private_ip_address: s("5.6.7.8"),
                    state: s("running"),
                    subnet_id: s("azid-3"),
                    vpc_id: s("vpc-ipv4"),
                    network_interfaces: vec![
                        // interface without subnet -> should be ignored
                        Eni { ipv6_addresses: vec![v6("2001:db8:1::1", true)], ..Default::default() },
                        // interface with subnet, no IPv6
                        Eni { subnet_id: s("azid-3"), ..Default::default() },
                        // interface with another subnet, no IPv6
                        Eni { subnet_id: s("azid-1"), ..Default::default() },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            },
            filters: vec![],
            expected: vec![Group {
                source: "region-ipv4".into(),
                targets: vec![ls(&[
                    ("__address__", "5.6.7.8:4242"),
                    ("__meta_ec2_ami", "ami-ipv4"),
                    ("__meta_ec2_availability_zone", "azname-c"),
                    ("__meta_ec2_availability_zone_id", "azid-3"),
                    ("__meta_ec2_instance_id", "instance-id-ipv4"),
                    ("__meta_ec2_instance_state", "running"),
                    ("__meta_ec2_instance_type", "instance-type-ipv4"),
                    ("__meta_ec2_owner_id", ""),
                    ("__meta_ec2_primary_subnet_id", "azid-3"),
                    ("__meta_ec2_private_ip", "5.6.7.8"),
                    ("__meta_ec2_region", "region-ipv4"),
                    ("__meta_ec2_subnet_id", ",azid-3,azid-1,"),
                    ("__meta_ec2_vpc_id", "vpc-ipv4"),
                ])],
                ..Default::default()
            }],
        },
        Case {
            name: "Ipv6",
            data: Ec2DataStore {
                region: "region-ipv6".into(),
                az_to_az_id: three_azs(),
                instances: vec![Instance {
                    // just the minimum needed for the refresh work
                    image_id: s("ami-ipv6"),
                    instance_id: s("instance-id-ipv6"),
                    instance_type: "instance-type-ipv6".into(),
                    placement: Some(s("azname-b")),
                    private_ip_address: s("9.10.11.12"),
                    state: s("running"),
                    subnet_id: s("azid-2"),
                    vpc_id: s("vpc-ipv6"),
                    network_interfaces: vec![
                        // interface without primary IPv6, index 2
                        Eni { attachment: i32p(3), ipv6_addresses: vec![v6("2001:db8:2::1:1", false)], subnet_id: s("azid-2") },
                        // interface with primary IPv6, index 1
                        Eni { attachment: i32p(1), ipv6_addresses: vec![v6("2001:db8:2::2:1", false), v6("2001:db8:2::2:2", true)], subnet_id: s("azid-2") },
                        // interface with primary IPv6, index 3
                        Eni { attachment: i32p(3), ipv6_addresses: vec![v6("2001:db8:2::3:1", true)], subnet_id: s("azid-1") },
                        // interface without primary IPv6, index 0
                        Eni { attachment: i32p(0), ipv6_addresses: vec![], subnet_id: s("azid-3") },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            },
            filters: vec![],
            expected: vec![Group {
                source: "region-ipv6".into(),
                targets: vec![ls(&[
                    ("__address__", "9.10.11.12:4242"),
                    ("__meta_ec2_ami", "ami-ipv6"),
                    ("__meta_ec2_availability_zone", "azname-b"),
                    ("__meta_ec2_availability_zone_id", "azid-2"),
                    ("__meta_ec2_instance_id", "instance-id-ipv6"),
                    ("__meta_ec2_instance_state", "running"),
                    ("__meta_ec2_instance_type", "instance-type-ipv6"),
                    ("__meta_ec2_ipv6_addresses", ",2001:db8:2::1:1,2001:db8:2::2:1,2001:db8:2::2:2,2001:db8:2::3:1,"),
                    ("__meta_ec2_owner_id", ""),
                    ("__meta_ec2_default_ipv6_address", "2001:db8:2::2:2"),
                    ("__meta_ec2_primary_ipv6_addresses", ",,2001:db8:2::2:2,,2001:db8:2::3:1,"),
                    ("__meta_ec2_primary_subnet_id", "azid-2"),
                    ("__meta_ec2_private_ip", "9.10.11.12"),
                    ("__meta_ec2_region", "region-ipv6"),
                    ("__meta_ec2_subnet_id", ",azid-2,azid-1,azid-3,"),
                    ("__meta_ec2_vpc_id", "vpc-ipv6"),
                ])],
                ..Default::default()
            }],
        },
        Case {
            name: "Ipv6-Only",
            data: Ec2DataStore {
                region: "region-ipv6-only".into(),
                az_to_az_id: three_azs(),
                instances: vec![Instance {
                    // just the minimum needed for the refresh work
                    image_id: s("ami-ipv6-only"),
                    instance_id: s("instance-id-ipv6-only"),
                    instance_type: "instance-type-ipv6-only".into(),
                    placement: Some(s("azname-b")),
                    state: s("running"),
                    subnet_id: s("azid-2"),
                    vpc_id: s("vpc-ipv6-only"),
                    network_interfaces: vec![
                        // interface without primary IPv6, index 0
                        Eni { attachment: i32p(0), ipv6_addresses: vec![v6("2001:db8:2::1:1", false)], subnet_id: s("azid-2") },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            },
            filters: vec![],
            expected: vec![Group {
                source: "region-ipv6-only".into(),
                targets: vec![ls(&[
                    ("__address__", "[2001:db8:2::1:1]:4242"),
                    ("__meta_ec2_ami", "ami-ipv6-only"),
                    ("__meta_ec2_availability_zone", "azname-b"),
                    ("__meta_ec2_availability_zone_id", "azid-2"),
                    ("__meta_ec2_instance_id", "instance-id-ipv6-only"),
                    ("__meta_ec2_instance_state", "running"),
                    ("__meta_ec2_instance_type", "instance-type-ipv6-only"),
                    ("__meta_ec2_ipv6_addresses", ",2001:db8:2::1:1,"),
                    ("__meta_ec2_owner_id", ""),
                    ("__meta_ec2_default_ipv6_address", "2001:db8:2::1:1"),
                    ("__meta_ec2_primary_subnet_id", "azid-2"),
                    ("__meta_ec2_region", "region-ipv6-only"),
                    ("__meta_ec2_subnet_id", ",azid-2,"),
                    ("__meta_ec2_vpc_id", "vpc-ipv6-only"),
                ])],
                ..Default::default()
            }],
        },
        Case {
            name: "FiltersMatchSingleInstance",
            data: Ec2DataStore {
                region: "region-filter-match".into(),
                az_to_az_id: azs(&[("azname-a", "azid-1")]),
                instances: vec![
                    Instance { image_id: s("ami-filter-1"), instance_id: s("instance-filter-1"), instance_type: "instance-type-filter".into(), placement: Some(s("azname-a")), private_ip_address: s("10.0.0.1"), state: s("running"), ..Default::default() },
                    Instance { image_id: s("ami-filter-2"), instance_id: s("instance-filter-2"), instance_type: "instance-type-filter".into(), placement: Some(s("azname-a")), private_ip_address: s("10.0.0.2"), state: s("stopped"), ..Default::default() },
                ],
                ..Default::default()
            },
            filters: vec![Filter { name: "instance-state-name".into(), values: vec!["running".into()] }],
            expected: vec![Group {
                source: "region-filter-match".into(),
                targets: vec![ls(&[
                    ("__address__", "10.0.0.1:4242"),
                    ("__meta_ec2_ami", "ami-filter-1"),
                    ("__meta_ec2_availability_zone", "azname-a"),
                    ("__meta_ec2_availability_zone_id", "azid-1"),
                    ("__meta_ec2_instance_id", "instance-filter-1"),
                    ("__meta_ec2_instance_state", "running"),
                    ("__meta_ec2_instance_type", "instance-type-filter"),
                    ("__meta_ec2_owner_id", ""),
                    ("__meta_ec2_private_ip", "10.0.0.1"),
                    ("__meta_ec2_region", "region-filter-match"),
                ])],
                ..Default::default()
            }],
        },
        Case {
            name: "FiltersMatchNoInstances",
            data: Ec2DataStore {
                region: "region-filter-none".into(),
                az_to_az_id: azs(&[("azname-a", "azid-1")]),
                instances: vec![
                    Instance { image_id: s("ami-filter-1"), instance_id: s("instance-filter-1"), instance_type: "instance-type-filter".into(), placement: Some(s("azname-a")), private_ip_address: s("10.0.1.1"), state: s("running"), ..Default::default() },
                    Instance { image_id: s("ami-filter-2"), instance_id: s("instance-filter-2"), instance_type: "instance-type-filter".into(), placement: Some(s("azname-a")), private_ip_address: s("10.0.1.2"), state: s("stopped"), ..Default::default() },
                ],
                ..Default::default()
            },
            filters: vec![Filter { name: "instance-state-name".into(), values: vec!["terminated".into()] }],
            expected: vec![Group { source: "region-filter-none".into(), ..Default::default() }],
        },
    ];
    for c in cases {
        let (mut d, _srv) = ec2_test_discovery(&c.data, c.filters);
        let g = d.refresh(&ctx).unwrap_or_else(|e| panic!("{}: {e}", c.name));
        assert_eq!(c.expected, groups(g), "{}", c.name);
    }
}

/// Go `fullyPopulatedEC2Instance`.
fn fully_populated_ec2_instance() -> Instance {
    Instance {
        architecture: "x86_64".into(),
        image_id: s("ami-full"),
        instance_id: s("i-full"),
        instance_lifecycle: "spot".into(),
        instance_type: "t3.micro".into(),
        placement: Some(s("azname-a")),
        platform: "windows".into(),
        private_dns_name: s("ip-10-0-0-1.ec2.internal"),
        private_ip_address: s("10.0.0.1"),
        public_dns_name: s("ec2-42-42-42-42.compute-1.amazonaws.com"),
        public_ip_address: s("42.42.42.42"),
        state: s("running"),
        subnet_id: s("subnet-full"),
        vpc_id: s("vpc-full"),
        network_interfaces: vec![Eni { attachment: Some(Some(0)), ipv6_addresses: vec![Ipv6 { ipv6_address: s("2001:db8::1"), is_primary_ipv6: Some(true) }], subnet_id: s("subnet-full") }],
        tags: vec![],
    }
}

/// Go `fullyPopulatedEC2DataStore`.
fn fully_populated_ec2_data_store() -> Ec2DataStore {
    Ec2DataStore { region: "region-full".into(), az_to_az_id: azs(&[("azname-a", "azid-1")]), owner_id: "owner-id-full".into(), instances: vec![fully_populated_ec2_instance()] }
}

#[test]
fn test_ec2_discovery_refresh_fully_populated() {
    let data = fully_populated_ec2_data_store();
    // The recorded response set for end-to-end comparisons with Prometheus (written only on request).
    if let Ok(dir) = std::env::var("AWS_SD_RECORD_DIR") {
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(format!("{dir}/ec2_DescribeAvailabilityZones.xml"), describe_azs(&data).body).unwrap();
        std::fs::write(format!("{dir}/ec2_DescribeInstances.xml"), describe_instances(&data, &MockRequest::default()).body).unwrap();
    }
    let (mut d, _srv) = ec2_test_discovery(&data, vec![]);
    let tgs = d.refresh(&CancelToken::new()).unwrap();
    assert_eq!(
        vec![Group {
            source: "region-full".into(),
            targets: vec![ls(&[
                ("__address__", "10.0.0.1:4242"),
                ("__meta_ec2_ami", "ami-full"),
                ("__meta_ec2_architecture", "x86_64"),
                ("__meta_ec2_availability_zone", "azname-a"),
                ("__meta_ec2_availability_zone_id", "azid-1"),
                ("__meta_ec2_default_ipv6_address", "2001:db8::1"),
                ("__meta_ec2_instance_id", "i-full"),
                ("__meta_ec2_instance_lifecycle", "spot"),
                ("__meta_ec2_instance_state", "running"),
                ("__meta_ec2_instance_type", "t3.micro"),
                ("__meta_ec2_ipv6_addresses", ",2001:db8::1,"),
                ("__meta_ec2_owner_id", "owner-id-full"),
                ("__meta_ec2_platform", "windows"),
                ("__meta_ec2_primary_ipv6_addresses", ",2001:db8::1,"),
                ("__meta_ec2_primary_subnet_id", "subnet-full"),
                ("__meta_ec2_private_dns_name", "ip-10-0-0-1.ec2.internal"),
                ("__meta_ec2_private_ip", "10.0.0.1"),
                ("__meta_ec2_public_dns_name", "ec2-42-42-42-42.compute-1.amazonaws.com"),
                ("__meta_ec2_public_ip", "42.42.42.42"),
                ("__meta_ec2_region", "region-full"),
                ("__meta_ec2_subnet_id", ",subnet-full,"),
                ("__meta_ec2_vpc_id", "vpc-full"),
            ])],
            ..Default::default()
        }],
        groups(tgs)
    );
}

#[test]
fn test_ec2_discovery_refresh_instance_nil_optional_fields() {
    type Mutate = fn(&mut Instance);
    let cases: Vec<(&str, Mutate, Vec<&str>)> = vec![
        ("NilImageId", |i| i.image_id = None, vec![EC2_LABEL_AMI]),
        ("NilInstanceId", |i| i.instance_id = None, vec![EC2_LABEL_INSTANCE_ID]),
        ("NilPlacement", |i| i.placement = None, vec![EC2_LABEL_AZ, EC2_LABEL_AZ_ID]),
        ("NilAvailabilityZone", |i| i.placement = Some(None), vec![EC2_LABEL_AZ, EC2_LABEL_AZ_ID]),
        ("NilState", |i| i.state = None, vec![EC2_LABEL_INSTANCE_STATE]),
        ("NilSubnetId", |i| i.subnet_id = None, vec![EC2_LABEL_PRIMARY_SUBNET_ID]),
        ("NilPublicDnsName", |i| i.public_dns_name = None, vec![EC2_LABEL_PUBLIC_DNS]),
    ];
    for (name, mutate, absent) in cases {
        let mut data = fully_populated_ec2_data_store();
        mutate(&mut data.instances[0]);
        let (mut d, _srv) = ec2_test_discovery(&data, vec![]);
        let tgs = d.refresh(&CancelToken::new()).unwrap();
        assert_eq!(1, tgs.len(), "{name}");
        assert_eq!(1, tgs[0].targets.len(), "{name}: instance must still be discovered when an optional field is absent");
        let target = &tgs[0].targets[0];
        for label in absent {
            assert!(!target.contains_key(label), "{name}: label sourced from the absent field should be omitted: {label}");
        }
        // The instance is still usable: the address is what makes it a target.
        assert_eq!(Some("10.0.0.1:4242"), target.get("__address__").map(String::as_str), "{name}");
    }
}

#[test]
fn test_ec2_discovery_refresh_i_pv6_nil_optional_fields() {
    type Mutate = fn(&mut Eni);
    let addrs = || ls(&[(EC2_LABEL_IPV6_ADDRESSES, ",2001:db8::1,"), (EC2_LABEL_DEFAULT_IPV6_ADDRESS, "2001:db8::1")]);
    let cases: Vec<(&str, Mutate, LabelSet)> = vec![
        // Nothing identifies the address, so it drops out entirely.
        ("NilIpv6Address", |e| e.ipv6_addresses[0].ipv6_address = None, LabelSet::new()),
        // An absent flag means the address is not primary.
        ("NilIsPrimaryIpv6", |e| e.ipv6_addresses[0].is_primary_ipv6 = None, addrs()),
        // Without an attachment there is no device index to record the primary address at, but the address
        // itself is still discovered.
        ("NilAttachment", |e| e.attachment = None, addrs()),
        ("NilDeviceIndex", |e| e.attachment = Some(None), addrs()),
        // A negative index has no slot in the positional list.
        ("NegativeDeviceIndex", |e| e.attachment = Some(Some(-1)), addrs()),
    ];
    for (name, mutate, expected) in cases {
        let mut data = fully_populated_ec2_data_store();
        mutate(&mut data.instances[0].network_interfaces[0]);
        let (mut d, _srv) = ec2_test_discovery(&data, vec![]);
        let tgs = d.refresh(&CancelToken::new()).unwrap();
        assert_eq!(1, tgs.len(), "{name}");
        assert_eq!(1, tgs[0].targets.len(), "{name}: instance must still be discovered when an optional field is absent");
        let target = &tgs[0].targets[0];
        for label in [EC2_LABEL_IPV6_ADDRESSES, EC2_LABEL_PRIMARY_IPV6_ADDRESSES, EC2_LABEL_DEFAULT_IPV6_ADDRESS] {
            if let Some(want) = expected.get(label) {
                assert_eq!(Some(want), target.get(label), "{name}: {label}");
                continue;
            }
            assert!(!target.contains_key(label), "{name}: label sourced from the absent field should be omitted: {label}");
        }
        // The instance keeps its IPv4 address either way.
        assert_eq!(Some("10.0.0.1:4242"), target.get("__address__").map(String::as_str), "{name}");
    }
}
