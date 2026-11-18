//! Port of discovery/eureka/client_test.go.
use discovery::CancelToken;
use discovery::eureka::fetch_apps;

use crate::common::{Resp, TestServer};

const APPS_XML: &str = r#"<applications>
  <versions__delta>1</versions__delta>
  <apps__hashcode>UP_4_</apps__hashcode>
  <application>
    <name>CONFIG-SERVICE</name>
    <instance>
      <instanceId>config-service001.test.com:config-service:8080</instanceId>
      <hostName>config-service001.test.com</hostName>
      <app>CONFIG-SERVICE</app>
      <ipAddr>192.133.83.31</ipAddr>
      <status>UP</status>
      <overriddenstatus>UNKNOWN</overriddenstatus>
      <port enabled="true">8080</port>
      <securePort enabled="false">8080</securePort>
      <countryId>1</countryId>
      <dataCenterInfo class="com.netflix.appinfo.InstanceInfo$DefaultDataCenterInfo">
        <name>MyOwn</name>
      </dataCenterInfo>
      <leaseInfo>
        <renewalIntervalInSecs>30</renewalIntervalInSecs>
        <durationInSecs>90</durationInSecs>
        <registrationTimestamp>1596003469304</registrationTimestamp>
        <lastRenewalTimestamp>1596110179310</lastRenewalTimestamp>
        <evictionTimestamp>0</evictionTimestamp>
        <serviceUpTimestamp>1547190033103</serviceUpTimestamp>
      </leaseInfo>
      <metadata>
        <instanceId>config-service001.test.com:config-service:8080</instanceId>
      </metadata>
      <homePageUrl>http://config-service001.test.com:8080/</homePageUrl>
      <statusPageUrl>http://config-service001.test.com:8080/info</statusPageUrl>
      <healthCheckUrl>http://config-service001.test.com 8080/health</healthCheckUrl>
      <vipAddress>config-service</vipAddress>
      <isCoordinatingDiscoveryServer>false</isCoordinatingDiscoveryServer>
      <lastUpdatedTimestamp>1596003469304</lastUpdatedTimestamp>
      <lastDirtyTimestamp>1596003469304</lastDirtyTimestamp>
      <actionType>ADDED</actionType>
    </instance>
    <instance>
      <instanceId>config-service002.test.com:config-service:8080</instanceId>
      <hostName>config-service002.test.com</hostName>
      <app>CONFIG-SERVICE</app>
      <ipAddr>192.133.83.31</ipAddr>
      <status>UP</status>
      <overriddenstatus>UNKNOWN</overriddenstatus>
      <port enabled="true">8080</port>
      <securePort enabled="false">8080</securePort>
      <countryId>1</countryId>
      <dataCenterInfo class="com.netflix.appinfo.InstanceInfo$DefaultDataCenterInfo">
        <name>MyOwn</name>
      </dataCenterInfo>
      <leaseInfo>
        <renewalIntervalInSecs>30</renewalIntervalInSecs>
        <durationInSecs>90</durationInSecs>
        <registrationTimestamp>1596003469304</registrationTimestamp>
        <lastRenewalTimestamp>1596110179310</lastRenewalTimestamp>
        <evictionTimestamp>0</evictionTimestamp>
        <serviceUpTimestamp>1547190033103</serviceUpTimestamp>
      </leaseInfo>
      <metadata>
        <instanceId>config-service002.test.com:config-service:8080</instanceId>
      </metadata>
      <homePageUrl>http://config-service002.test.com:8080/</homePageUrl>
      <statusPageUrl>http://config-service002.test.com:8080/info</statusPageUrl>
      <healthCheckUrl>http://config-service002.test.com:8080/health</healthCheckUrl>
      <vipAddress>config-service</vipAddress>
      <isCoordinatingDiscoveryServer>false</isCoordinatingDiscoveryServer>
      <lastUpdatedTimestamp>1596003469304</lastUpdatedTimestamp>
      <lastDirtyTimestamp>1596003469304</lastDirtyTimestamp>
      <actionType>ADDED</actionType>
    </instance>
  </application>
  <application>
    <name>META-SERVICE</name>
    <instance>
      <instanceId>meta-service002.test.com:meta-service:8080</instanceId>
      <hostName>meta-service002.test.com</hostName>
      <app>META-SERVICE</app>
      <ipAddr>192.133.87.237</ipAddr>
      <status>UP</status>
      <overriddenstatus>UNKNOWN</overriddenstatus>
      <port enabled="true">8080</port>
      <securePort enabled="false">443</securePort>
      <countryId>1</countryId>
      <dataCenterInfo class="com.netflix.appinfo.InstanceInfo$DefaultDataCenterInfo">
        <name>MyOwn</name>
      </dataCenterInfo>
      <leaseInfo>
        <renewalIntervalInSecs>30</renewalIntervalInSecs>
        <durationInSecs>90</durationInSecs>
        <registrationTimestamp>1535444352472</registrationTimestamp>
        <lastRenewalTimestamp>1596110168846</lastRenewalTimestamp>
        <evictionTimestamp>0</evictionTimestamp>
        <serviceUpTimestamp>1535444352472</serviceUpTimestamp>
      </leaseInfo>
      <metadata>
        <project>meta-service</project>
        <management.port>8090</management.port>
      </metadata>
      <homePageUrl>http://meta-service002.test.com:8080/</homePageUrl>
      <statusPageUrl>http://meta-service002.test.com:8080/info</statusPageUrl>
      <healthCheckUrl>http://meta-service002.test.com:8080/health</healthCheckUrl>
      <vipAddress>meta-service</vipAddress>
      <secureVipAddress>meta-service</secureVipAddress>
      <isCoordinatingDiscoveryServer>false</isCoordinatingDiscoveryServer>
      <lastUpdatedTimestamp>1535444352472</lastUpdatedTimestamp>
      <lastDirtyTimestamp>1535444352398</lastDirtyTimestamp>
      <actionType>ADDED</actionType>
    </instance>
    <instance>
      <instanceId>meta-service001.test.com:meta-service:8080</instanceId>
      <hostName>meta-service001.test.com</hostName>
      <app>META-SERVICE</app>
      <ipAddr>192.133.87.236</ipAddr>
      <status>UP</status>
      <overriddenstatus>UNKNOWN</overriddenstatus>
      <port enabled="true">8080</port>
      <securePort enabled="false">443</securePort>
      <countryId>1</countryId>
      <dataCenterInfo class="com.netflix.appinfo.InstanceInfo$DefaultDataCenterInfo">
        <name>MyOwn</name>
      </dataCenterInfo>
      <leaseInfo>
        <renewalIntervalInSecs>30</renewalIntervalInSecs>
        <durationInSecs>90</durationInSecs>
        <registrationTimestamp>1535444352472</registrationTimestamp>
        <lastRenewalTimestamp>1596110168846</lastRenewalTimestamp>
        <evictionTimestamp>0</evictionTimestamp>
        <serviceUpTimestamp>1535444352472</serviceUpTimestamp>
      </leaseInfo>
      <metadata>
        <project>meta-service</project>
        <management.port>8090</management.port>
      </metadata>
      <homePageUrl>http://meta-service001.test.com:8080/</homePageUrl>
      <statusPageUrl>http://meta-service001.test.com:8080/info</statusPageUrl>
      <healthCheckUrl>http://meta-service001.test.com:8080/health</healthCheckUrl>
      <vipAddress>meta-service</vipAddress>
      <secureVipAddress>meta-service</secureVipAddress>
      <isCoordinatingDiscoveryServer>false</isCoordinatingDiscoveryServer>
      <lastUpdatedTimestamp>1535444352472</lastUpdatedTimestamp>
      <lastDirtyTimestamp>1535444352398</lastDirtyTimestamp>
      <actionType>ADDED</actionType>
    </instance>
  </application>
</applications>"#;

/// Go `&http.Client{}`.
fn plain_client() -> net::Client {
    net::Client::new(net::new_round_tripper_from_config(&Default::default(), &Default::default()).unwrap())
}

#[test]
fn test_fetch_apps() {
    // Simulate apps with a valid XML response.
    let ts = TestServer::new(|_| Resp::new(200, APPS_XML).header("Content-Type", "application/xml"));
    let apps = fetch_apps(&CancelToken::new(), &ts.url, &plain_client()).unwrap();

    assert_eq!(2, apps.applications.len());
    assert_eq!("CONFIG-SERVICE", apps.applications[0].name);
    assert_eq!("META-SERVICE", apps.applications[1].name);

    assert_eq!(2, apps.applications[1].instances.len());
    assert_eq!("meta-service002.test.com:meta-service:8080", apps.applications[1].instances[0].instance_id);
    let md = apps.applications[1].instances[0].metadata.as_ref().unwrap();
    assert_eq!("project", md.items[0].xml_name.local);
    assert_eq!("meta-service", md.items[0].content);
    assert_eq!("management.port", md.items[1].xml_name.local);
    assert_eq!("8090", md.items[1].content);
    assert_eq!("meta-service001.test.com:meta-service:8080", apps.applications[1].instances[1].instance_id);
}

#[test]
fn test500_error_http_response() {
    // Simulate 500 error.
    let ts = TestServer::new(|_| Resp::new(500, "").header("Content-Type", "application/xml"));
    let r = fetch_apps(&CancelToken::new(), &ts.url, &plain_client());
    assert!(r.is_err(), "5xx HTTP response");
}

/// Extra (not in the ledger): Go's encoding/xml details the decoder follows.
#[test]
fn extra_decode_applications() {
    use discovery::eureka::client::decode_applications;
    let x = r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<!DOCTYPE x [<!ENTITY a "b">]><!-- c --><apps><application><name>A&amp;B<x>skip</x>&#65;<![CDATA[<c>]]></name>
<instance><hostName>h</hostName><port enabled="t">  81 </port><securePort/><metadata><k a="1">v<i>w</i></k><e/></metadata>
<dataCenterInfo class="c"><name>n</name><metadata><z>1</z></metadata></dataCenterInfo></instance></application></apps>trailing"#;
    let apps = decode_applications(x.as_bytes()).unwrap();
    let a = &apps.applications[0];
    assert_eq!("A&BA<c>", a.name);
    let i = &a.instances[0];
    assert_eq!(81, i.port.as_ref().unwrap().port);
    assert!(i.port.as_ref().unwrap().enabled);
    assert_eq!(0, i.secure_port.as_ref().unwrap().port);
    let md = &i.metadata.as_ref().unwrap().items;
    assert_eq!(("k", "v<i>w</i>"), (md[0].xml_name.local.as_str(), md[0].content.as_str()));
    assert_eq!(("e", ""), (md[1].xml_name.local.as_str(), md[1].content.as_str()));
    let dc = i.data_center_info.as_ref().unwrap();
    assert_eq!(("n", "c"), (dc.name.as_str(), dc.class.as_str()));
    assert_eq!("1", dc.metadata.as_ref().unwrap().items[0].content);

    let errs = [
        ("", "EOF"),
        ("<a>", "XML syntax error on line 1: unexpected EOF"),
        ("<a>\n</b>", "XML syntax error on line 2: element <a> closed by </b>"),
        ("<a><application><instance><countryId>x</countryId></instance></application></a>", "strconv.ParseInt: parsing \"x\": invalid syntax"),
        ("<a><application><instance><port enabled=\"yes\">1</port></instance></application></a>", "strconv.ParseBool: parsing \"yes\": invalid syntax"),
        ("<a>&foo;</a>", "XML syntax error on line 1: invalid character entity &foo;"),
        ("<a>&foo</a>", "XML syntax error on line 1: invalid character entity &foo (no semicolon)"),
        ("<a b=c></a>", "XML syntax error on line 1: unquoted or missing attribute value in element"),
        ("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><a/>", "xml: encoding \"ISO-8859-1\" declared but Decoder.CharsetReader is nil"),
    ];
    for (input, want) in errs {
        assert_eq!(Err(want.to_string()), decode_applications(input.as_bytes()), "{input}");
    }
}
