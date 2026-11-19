//! Port of discovery/eureka/eureka_test.go.
mod common;

use std::sync::Arc;

use common::{Resp, TestServer, new_options};
use discovery::eureka::{Discovery, SdConfig};
use discovery::{CancelToken, Group};

fn test_update_services(resp: impl Fn() -> Resp + Send + Sync + 'static) -> Result<Vec<Arc<Group>>, String> {
    // Create a test server with mock HTTP handler.
    let ts = TestServer::new(move |_| resp());
    let conf = SdConfig { server: ts.url.clone(), ..Default::default() };
    let (mut opts, _reg) = new_options(&conf);
    opts.set_name = "eureka".to_string();
    let mut md = Discovery::new(&conf, opts)?;
    md.refresh(&CancelToken::new())
}

#[test]
fn test_eureka_sd_handle_error() {
    let err_testing = "non 2xx status '500' response during eureka service discovery";
    let tgs = test_update_services(|| Resp::new(500, "").header("Content-Type", "application/xml"));
    assert_eq!(Err(err_testing.to_string()), tgs);
}

#[test]
fn test_eureka_sd_empty_list() {
    let apps_xml = r#"<applications>
<versions__delta>1</versions__delta>
<apps__hashcode/>
</applications>"#;
    let tgs = test_update_services(move || Resp::new(200, apps_xml).header("Content-Type", "application/xml")).unwrap();
    assert_eq!(1, tgs.len());
}

#[test]
fn test_eureka_sd_send_group() {
    let apps_xml = r#"<applications>
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
    let tgs = test_update_services(move || Resp::new(200, apps_xml).header("Content-Type", "application/xml")).unwrap();
    assert_eq!(1, tgs.len());

    let tg = &tgs[0];
    assert_eq!("eureka", tg.source);
    assert_eq!(4, tg.targets.len());

    let tgt = &tg.targets[0];
    assert_eq!(tgt.get("__address__").map(String::as_str), Some("config-service001.test.com:8080"));

    let tgt = &tg.targets[2];
    assert_eq!(tgt.get("__address__").map(String::as_str), Some("meta-service002.test.com:8080"));
}

/// Extra (not in the ledger): every label of an instance.
#[test]
fn extra_eureka_labels() {
    let apps_xml = r#"<applications>
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
    let tgs = test_update_services(move || Resp::new(200, apps_xml).header("Content-Type", "application/xml")).unwrap();
    let want = common::ls(&[
        ("__address__", "meta-service002.test.com:8080"),
        ("instance", "meta-service002.test.com:meta-service:8080"),
        ("__meta_eureka_app_name", "META-SERVICE"),
        ("__meta_eureka_app_instance_hostname", "meta-service002.test.com"),
        ("__meta_eureka_app_instance_homepage_url", "http://meta-service002.test.com:8080/"),
        ("__meta_eureka_app_instance_statuspage_url", "http://meta-service002.test.com:8080/info"),
        ("__meta_eureka_app_instance_healthcheck_url", "http://meta-service002.test.com:8080/health"),
        ("__meta_eureka_app_instance_ip_addr", "192.133.87.237"),
        ("__meta_eureka_app_instance_vip_address", "meta-service"),
        ("__meta_eureka_app_instance_secure_vip_address", "meta-service"),
        ("__meta_eureka_app_instance_status", "UP"),
        ("__meta_eureka_app_instance_country_id", "1"),
        ("__meta_eureka_app_instance_id", "meta-service002.test.com:meta-service:8080"),
        ("__meta_eureka_app_instance_port", "8080"),
        ("__meta_eureka_app_instance_port_enabled", "true"),
        ("__meta_eureka_app_instance_secure_port", "443"),
        ("__meta_eureka_app_instance_secure_port_enabled", "false"),
        ("__meta_eureka_app_instance_datacenterinfo_name", "MyOwn"),
        ("__meta_eureka_app_instance_metadata_project", "meta-service"),
        ("__meta_eureka_app_instance_metadata_management_port", "8090"),
    ]);
    assert_eq!(want, tgs[0].targets[2]);
}
