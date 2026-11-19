//! Port of `discovery/eureka/client.go`: the `/apps` request and Go's `encoding/xml` decoding of its answer
//! into the structs below.
use super::xml::{self, Decoder, StartElement, Token};
use crate::cancel::CancelToken;

/// Go `userAgent` (`version.PrometheusUserAgent()`).
pub const USER_AGENT: &str = crate::http::USER_AGENT;

/// Go `appListPath`.
pub const APP_LIST_PATH: &str = "/apps";

/// Go `Applications`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Applications {
    pub versions_delta: i64,
    pub apps_hashcode: String,
    pub applications: Vec<Application>,
}

/// Go `Application`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Application {
    pub name: String,
    pub instances: Vec<Instance>,
}

/// Go `Port`: the port number (character data) and its `enabled` attribute.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Port {
    pub port: i64,
    pub enabled: bool,
}

/// Go `Instance`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Instance {
    pub host_name: String,
    pub home_page_url: String,
    pub status_page_url: String,
    pub health_check_url: String,
    pub app: String,
    pub ip_addr: String,
    pub vip_address: String,
    pub secure_vip_address: String,
    pub status: String,
    pub port: Option<Port>,
    pub secure_port: Option<Port>,
    pub data_center_info: Option<DataCenterInfo>,
    pub metadata: Option<MetaData>,
    pub is_coordinating_discovery_server: bool,
    pub action_type: String,
    pub country_id: i64,
    pub instance_id: String,
}

/// Go `MetaData`: every child element (`xml:",any"`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetaData {
    pub items: Vec<Tag>,
}

/// Go `Tag`: the element name and its inner XML.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Tag {
    pub xml_name: xml::Name,
    pub content: String,
}

/// Go `DataCenterInfo`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DataCenterInfo {
    pub name: String,
    pub class: String,
    pub metadata: Option<MetaData>,
}

/// The struct part of Go's `Decoder.unmarshal`: reads the content of the element whose start was just read,
/// hands child elements to `child` (which returns whether it consumed the element; the others are skipped)
/// and collects the character data into `chardata` when the struct has a `,chardata` field.
fn decode_struct(d: &mut Decoder, mut chardata: Option<&mut String>, mut child: impl FnMut(&mut Decoder, &StartElement) -> Result<bool, String>) -> Result<(), String> {
    loop {
        match d.token()? {
            Token::Start(t) => {
                if !child(d, &t)? {
                    d.skip()?;
                }
            }
            Token::End(_) => return Ok(()),
            Token::CharData(s) => {
                if let Some(c) = chardata.as_deref_mut() {
                    c.push_str(&s);
                }
            }
            _ => {}
        }
    }
}

fn string_field(d: &mut Decoder, f: &mut String) -> Result<bool, String> {
    *f = d.chardata()?;
    Ok(true)
}

fn int_field(d: &mut Decoder, f: &mut i64) -> Result<bool, String> {
    *f = xml::parse_int(&d.chardata()?)?;
    Ok(true)
}

fn bool_field(d: &mut Decoder, f: &mut bool) -> Result<bool, String> {
    *f = xml::parse_bool(&d.chardata()?)?;
    Ok(true)
}

impl Applications {
    fn decode(&mut self, d: &mut Decoder) -> Result<(), String> {
        decode_struct(d, None, |d, t| match t.name.local.as_str() {
            "versions__delta" => int_field(d, &mut self.versions_delta),
            "apps__hashcode" => string_field(d, &mut self.apps_hashcode),
            "application" => {
                let mut a = Application::default();
                a.decode(d)?;
                self.applications.push(a);
                Ok(true)
            }
            _ => Ok(false),
        })
    }
}

impl Application {
    fn decode(&mut self, d: &mut Decoder) -> Result<(), String> {
        decode_struct(d, None, |d, t| match t.name.local.as_str() {
            "name" => string_field(d, &mut self.name),
            "instance" => {
                let mut i = Instance::default();
                i.decode(d)?;
                self.instances.push(i);
                Ok(true)
            }
            _ => Ok(false),
        })
    }
}

impl Port {
    fn decode(&mut self, d: &mut Decoder, start: &StartElement) -> Result<(), String> {
        for a in &start.attr {
            if a.name.local == "enabled" {
                self.enabled = xml::parse_bool(&a.value)?;
            }
        }
        let mut data = String::new();
        decode_struct(d, Some(&mut data), |_, _| Ok(false))?;
        self.port = xml::parse_int(&data)?;
        Ok(())
    }
}

impl MetaData {
    fn decode(&mut self, d: &mut Decoder) -> Result<(), String> {
        decode_struct(d, None, |d, t| {
            let content = d.inner_xml()?;
            self.items.push(Tag { xml_name: t.name.clone(), content });
            Ok(true)
        })
    }
}

impl DataCenterInfo {
    fn decode(&mut self, d: &mut Decoder, start: &StartElement) -> Result<(), String> {
        for a in &start.attr {
            if a.name.local == "class" {
                self.class = a.value.clone();
            }
        }
        decode_struct(d, None, |d, t| match t.name.local.as_str() {
            "name" => string_field(d, &mut self.name),
            "metadata" => {
                self.metadata.get_or_insert_with(MetaData::default).decode(d)?;
                Ok(true)
            }
            _ => Ok(false),
        })
    }
}

impl Instance {
    fn decode(&mut self, d: &mut Decoder) -> Result<(), String> {
        decode_struct(d, None, |d, t| match t.name.local.as_str() {
            "hostName" => string_field(d, &mut self.host_name),
            "homePageUrl" => string_field(d, &mut self.home_page_url),
            "statusPageUrl" => string_field(d, &mut self.status_page_url),
            "healthCheckUrl" => string_field(d, &mut self.health_check_url),
            "app" => string_field(d, &mut self.app),
            "ipAddr" => string_field(d, &mut self.ip_addr),
            "vipAddress" => string_field(d, &mut self.vip_address),
            "secureVipAddress" => string_field(d, &mut self.secure_vip_address),
            "status" => string_field(d, &mut self.status),
            "port" => {
                self.port.get_or_insert_with(Port::default).decode(d, t)?;
                Ok(true)
            }
            "securePort" => {
                self.secure_port.get_or_insert_with(Port::default).decode(d, t)?;
                Ok(true)
            }
            "dataCenterInfo" => {
                self.data_center_info.get_or_insert_with(DataCenterInfo::default).decode(d, t)?;
                Ok(true)
            }
            "metadata" => {
                self.metadata.get_or_insert_with(MetaData::default).decode(d)?;
                Ok(true)
            }
            "isCoordinatingDiscoveryServer" => bool_field(d, &mut self.is_coordinating_discovery_server),
            "actionType" => string_field(d, &mut self.action_type),
            "countryId" => int_field(d, &mut self.country_id),
            "instanceId" => string_field(d, &mut self.instance_id),
            _ => Ok(false),
        })
    }
}

/// Go `xml.NewDecoder(r).Decode(&apps)`: the first element (whatever its name) into `Applications`.
pub fn decode_applications(b: &[u8]) -> Result<Applications, String> {
    let mut d = Decoder::new(b);
    d.first_start()?;
    let mut apps = Applications::default();
    apps.decode(&mut d)?;
    Ok(apps)
}

/// Go `fetchApps`: GET `<server>/apps` and decode the answer.
pub fn fetch_apps(_ctx: &CancelToken, server: &str, client: &net::Client) -> Result<Applications, String> {
    let url = format!("{server}{APP_LIST_PATH}");
    let mut req = net::Request::get(&url).map_err(|e| e.to_string())?;
    req.header.add("User-Agent", USER_AGENT);
    let resp = client.do_request(req).map_err(|e| e.to_string())?;
    if resp.status / 100 != 2 {
        return Err(format!("non 2xx status '{}' response during eureka service discovery", resp.status));
    }
    decode_applications(&resp.body).map_err(|e| format!("{}: {e}", yaml::go_quote(&url)))
}
