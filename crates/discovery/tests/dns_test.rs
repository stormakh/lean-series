//! Port of discovery/dns/dns_test.go.
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Arc;

use discovery::dns::{Discovery, LookupFn, Msg, Rr, RrData, SdConfig};
use discovery::{CancelToken, Config, DiscovererOptions, Group, Registry};

mod common;
use common::ls;

fn lookup(f: impl Fn() -> Result<Msg, String> + Send + Sync + 'static) -> LookupFn {
    Arc::new(move |_: &str, _: u16, _: &discovery::Logger| f())
}

fn answer(rrs: Vec<RrData>) -> Msg {
    Msg { answer: rrs.into_iter().map(Rr::new).collect(), ..Default::default() }
}

fn srv(port: u16, target: &str) -> RrData {
    RrData::Srv { priority: 0, weight: 0, port, target: target.to_string() }
}

fn minute() -> yaml::Duration {
    yaml::Duration::from_secs(60)
}

#[test]
fn test_dns() {
    struct Case {
        name: &'static str,
        config: SdConfig,
        lookup: LookupFn,
        expected: Vec<Group>,
    }
    let target = |addr: &str, name: &str, srv_t: &str, srv_p: &str, mx_t: &str| {
        ls(&[
            ("__address__", addr),
            ("__meta_dns_name", name),
            ("__meta_dns_srv_record_target", srv_t),
            ("__meta_dns_srv_record_port", srv_p),
            ("__meta_dns_mx_record_target", mx_t),
            ("__meta_dns_ns_record_target", ""),
        ])
    };
    let names = |n: &[&str]| n.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let cases = vec![
        Case {
            name: "A record query with error",
            config: SdConfig { names: names(&["web.example.com."]), refresh_interval: minute(), port: 80, r#type: "A".into() },
            lookup: lookup(|| Err("some error".to_string())),
            expected: vec![],
        },
        Case {
            name: "A record query",
            config: SdConfig { names: names(&["web.example.com."]), refresh_interval: minute(), port: 80, r#type: "A".into() },
            // Go net.IPv4 returns the 16-byte form.
            lookup: lookup(|| Ok(answer(vec![RrData::A(Ipv4Addr::new(192, 0, 2, 2).to_ipv6_mapped().octets().to_vec())]))),
            expected: vec![Group { source: "web.example.com.".into(), targets: vec![target("192.0.2.2:80", "web.example.com.", "", "", "")], ..Default::default() }],
        },
        Case {
            name: "AAAA record query",
            config: SdConfig { names: names(&["web.example.com."]), refresh_interval: minute(), port: 80, r#type: "AAAA".into() },
            lookup: lookup(|| Ok(answer(vec![RrData::Aaaa(Ipv6Addr::LOCALHOST.octets().to_vec())]))),
            expected: vec![Group { source: "web.example.com.".into(), targets: vec![target("[::1]:80", "web.example.com.", "", "", "")], ..Default::default() }],
        },
        Case {
            name: "SRV record query",
            config: SdConfig { names: names(&["_mysql._tcp.db.example.com."]), r#type: "SRV".into(), refresh_interval: minute(), ..Default::default() },
            lookup: lookup(|| Ok(answer(vec![srv(3306, "db1.example.com."), srv(3306, "db2.example.com.")]))),
            expected: vec![Group {
                source: "_mysql._tcp.db.example.com.".into(),
                targets: vec![
                    target("db1.example.com:3306", "_mysql._tcp.db.example.com.", "db1.example.com.", "3306", ""),
                    target("db2.example.com:3306", "_mysql._tcp.db.example.com.", "db2.example.com.", "3306", ""),
                ],
                ..Default::default()
            }],
        },
        Case {
            name: "SRV record query with unsupported resource records",
            config: SdConfig { names: names(&["_mysql._tcp.db.example.com."]), refresh_interval: minute(), ..Default::default() },
            lookup: lookup(|| Ok(answer(vec![srv(3306, "db1.example.com."), RrData::Txt(vec!["this should be discarded".into()])]))),
            expected: vec![Group {
                source: "_mysql._tcp.db.example.com.".into(),
                targets: vec![target("db1.example.com:3306", "_mysql._tcp.db.example.com.", "db1.example.com.", "3306", "")],
                ..Default::default()
            }],
        },
        Case {
            name: "SRV record query with empty answer (NXDOMAIN)",
            config: SdConfig { names: names(&["_mysql._tcp.db.example.com."]), refresh_interval: minute(), ..Default::default() },
            lookup: lookup(|| Ok(Msg::default())),
            expected: vec![Group { source: "_mysql._tcp.db.example.com.".into(), ..Default::default() }],
        },
        Case {
            name: "MX record query",
            config: SdConfig { names: names(&["example.com."]), r#type: "MX".into(), port: 25, refresh_interval: minute() },
            lookup: lookup(|| Ok(answer(vec![RrData::Mx { preference: 0, mx: "smtp1.example.com.".into() }, RrData::Mx { preference: 10, mx: "smtp2.example.com.".into() }]))),
            expected: vec![Group {
                source: "example.com.".into(),
                targets: vec![
                    target("smtp1.example.com:25", "example.com.", "", "", "smtp1.example.com."),
                    target("smtp2.example.com:25", "example.com.", "", "", "smtp2.example.com."),
                ],
                ..Default::default()
            }],
        },
    ];

    // adapted: Go runs the cases as parallel subtests; here they run in turn, each with its own registry.
    for tc in cases {
        let reg = Registry::new();
        let refresh_metrics = discovery::new_refresh_metrics(&reg);
        let metrics = tc.config.new_discoverer_metrics(&reg, refresh_metrics);
        metrics.register().unwrap();

        let mut sd =
            Discovery::new(&tc.config, DiscovererOptions { logger: discovery::Logger::nop(), metrics: Some(metrics.clone()), set_name: "dns".into(), ..Default::default() })
                .unwrap();
        sd.set_lookup_fn(tc.lookup);

        let tgs = sd.refresh(&CancelToken::new()).unwrap_or_else(|e| panic!("{}: {e}", tc.name));
        let expected: Vec<Arc<Group>> = tc.expected.into_iter().map(Arc::new).collect();
        assert_eq!(expected, tgs, "{}", tc.name);

        metrics.unregister();
    }
}

#[test]
fn test_sd_config_unmarshal_yaml() {
    let marshal = |c: &SdConfig| yaml::marshal(c).unwrap();
    let unmarshal = |d: &str| yaml::unmarshal::<SdConfig>(d);

    let names = |n: &[&str]| n.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let cases = [
        ("valid srv", SdConfig { names: names(&["a.example.com", "b.example.com"]), r#type: "SRV".into(), ..Default::default() }, false),
        ("valid a", SdConfig { names: names(&["a.example.com", "b.example.com"]), r#type: "A".into(), port: 5300, ..Default::default() }, false),
        ("valid aaaa", SdConfig { names: names(&["a.example.com", "b.example.com"]), r#type: "AAAA".into(), port: 5300, ..Default::default() }, false),
        ("invalid a without port", SdConfig { names: names(&["a.example.com", "b.example.com"]), r#type: "A".into(), ..Default::default() }, true),
        ("invalid aaaa without port", SdConfig { names: names(&["a.example.com", "b.example.com"]), r#type: "AAAA".into(), ..Default::default() }, true),
        ("invalid empty names", SdConfig { names: vec![], r#type: "AAAA".into(), ..Default::default() }, true),
        ("invalid unknown dns type", SdConfig { names: names(&["a.example.com", "b.example.com"]), r#type: "PTR".into(), ..Default::default() }, true),
    ];

    for (name, input, expect_err) in cases {
        let d = marshal(&input);
        let err = unmarshal(&d).err();
        assert_eq!(expect_err, err.is_some(), "{name}: {err:?}");
    }
}

/// Not an upstream test: the in-house DNS client against the system resolver and a public one, and the
/// YAML validation errors. Run with `cargo test -p discovery --test dns_test -- --ignored --nocapture`.
#[test]
#[ignore = "needs network"]
fn real_resolver_check() {
    use discovery::dns::*;
    let logger = discovery::Logger::nop();
    for (name, qtype) in [
        ("_imaps._tcp.gmail.com", TYPE_SRV),
        ("prometheus.io", TYPE_A),
        ("prometheus.io", TYPE_AAAA),
        ("gmail.com", TYPE_MX),
        ("prometheus.io", TYPE_NS),
        ("www.github.com", TYPE_A),
        ("_xmpp-client._tcp.jabber.org", TYPE_SRV),
        ("LastSun", TYPE_A),
        ("nonexistent.invalid", TYPE_A),
    ] {
        let m = lookup_with_search_path(name, qtype, &logger).unwrap();
        println!("{name} {qtype}: rcode {} answers:", m.rcode);
        for r in &m.answer {
            println!("  {r}");
        }
    }
    let conf = ClientConfig::from_reader(b"nameserver 8.8.8.8\n");
    let m = lookup_from_any_server("_imaps._tcp.gmail.com.", TYPE_SRV, &conf, &logger).unwrap();
    println!("8.8.8.8 udp: {} answers", m.answer.len());
    let mut c = Client { net: "tcp".into() };
    let m = ask_server_for_name("_imaps._tcp.gmail.com", TYPE_SRV, &mut c, "8.8.8.8:53", true).unwrap();
    println!("8.8.8.8 tcp: {} answers, first {}", m.answer.len(), m.answer[0]);
    // Without EDNS0 the UDP answer is limited to 512 bytes: the TXT records of google.com are truncated, and
    // the client asks again over TCP.
    let mut c = Client::default();
    let m = ask_server_for_name("google.com", TYPE_TXT, &mut c, "8.8.8.8:53", false).unwrap();
    println!("google.com TXT without EDNS0: net {:?}, {} answers, truncated {}", c.net, m.answer.len(), m.truncated);
    // Error texts: nothing listens on 127.0.0.9:53 (UDP and TCP), and 10.255.255.1 does not answer (2s timeout).
    for (net, addr) in [("", "127.0.0.9:53"), ("tcp", "127.0.0.9:53"), ("", "10.255.255.1:53")] {
        let mut c = Client { net: net.into() };
        println!("{:?}", ask_server_for_name("example.com", TYPE_A, &mut c, addr, true).err());
    }
    // The whole discoverer with the real lookup.
    let cfg: SdConfig = yaml::unmarshal("names: [_imaps._tcp.gmail.com, _xmpp-client._tcp.jabber.org]\n").unwrap();
    let (opts, _reg) = common::new_options(&cfg);
    let mut sd = Discovery::new(&cfg, opts).unwrap();
    for g in sd.refresh(&CancelToken::new()).unwrap() {
        println!("{:?}", g);
    }
    for y in ["names: [a]\ntype: A\n", "names: []\n", "names: [a]\ntype: ptr\n"] {
        println!("{:?}", yaml::unmarshal::<SdConfig>(y).err().map(|e| e.to_string()));
    }
    println!("{}", yaml::marshal(&SdConfig { names: vec!["a".into()], r#type: "A".into(), port: 1, ..Default::default() }).unwrap());
}
