use std::time::{Duration, Instant};

const TD: &str = "/home/kamrot/code/prometheus-rs/tools/gopath/pkg/mod/github.com/prometheus/common@v0.72.1/config/testdata";

struct Case {
    name: &'static str,
    cfg: String,
    method: &'static str,
    url: &'static str,
    body: &'static str,
    decompress: bool,
    limit: Option<usize>,
    /// "status:<n>" or a substring of the body, or "err:<substring>"
    expect: Vec<&'static str>,
}

fn c(name: &'static str, cfg: &str, url: &'static str, expect: &[&'static str]) -> Case {
    Case { name, cfg: cfg.replace("$TD", TD), method: "GET", url, body: "", decompress: false, limit: None, expect: expect.to_vec() }
}

pub fn run() -> bool {
    let tls = "tls_config:\n  ca_file: $TD/tls-ca-chain.pem\n  cert_file: $TD/client.crt\n  key_file: $TD/client.key\n";
    let mut cases = vec![
        c("tls client cert", tls, "https://127.0.0.1:9600/hello", &["status:200", "hello https HTTP/1.1 cn=localhost", "ua=Go-http-client/1.1"]),
        c("tls via localhost name", tls, "https://localhost:9600/hello", &["status:200", "cn=localhost"]),
        c("tls inline ca", &format!("tls_config:\n  ca: |\n{}  cert_file: $TD/client.crt\n  key_file: $TD/client.key\n", std::fs::read_to_string(format!("{TD}/tls-ca-chain.pem")).unwrap().lines().map(|l| format!("    {l}\n")).collect::<String>()), "https://127.0.0.1:9600/hello", &["status:200", "cn=localhost"]),
        c("tls no client cert", "tls_config:\n  ca_file: $TD/tls-ca-chain.pem\n", "https://127.0.0.1:9600/hello", &["err:remote error: tls"]),
        c("tls wrong ca", "tls_config:\n  ca_file: $TD/client.crt\n  cert_file: $TD/client.crt\n  key_file: $TD/client.key\n", "https://127.0.0.1:9600/hello", &["err:certificate signed by unknown authority"]),
        c("tls wrong server_name", "tls_config:\n  ca_file: $TD/tls-ca-chain.pem\n  cert_file: $TD/client.crt\n  key_file: $TD/client.key\n  server_name: other.example\n", "https://127.0.0.1:9600/hello", &["err:not valid for"]),
        c("tls right server_name", "tls_config:\n  ca_file: $TD/tls-ca-chain.pem\n  cert_file: $TD/client.crt\n  key_file: $TD/client.key\n  server_name: localhost\n", "https://127.0.0.1:9600/hello", &["status:200"]),
        c("tls insecure", "tls_config:\n  insecure_skip_verify: true\n  cert_file: $TD/client.crt\n  key_file: $TD/client.key\n", "https://127.0.0.1:9600/hello", &["status:200", "cn=localhost"]),
        c("tls system roots reject", "", "https://127.0.0.1:9603/hello", &["err:certificate signed by unknown authority"]),
        c("tls max TLS12 server", "tls_config:\n  ca_file: $TD/tls-ca-chain.pem\n  max_version: TLS12\n", "https://127.0.0.1:9603/hello", &["status:200", "hello https12"]),
        c("tls min TLS13 vs TLS12 server", "tls_config:\n  ca_file: $TD/tls-ca-chain.pem\n  min_version: TLS13\n", "https://127.0.0.1:9603/hello", &["err:remote error: tls: protocol version not supported"]),
        c("basic auth", &format!("{tls}basic_auth:\n  username: alice\n  password: wonderland\n"), "https://127.0.0.1:9600/basic", &["status:200", "basic ok"]),
        c("basic auth wrong", &format!("{tls}basic_auth:\n  username: alice\n  password: nope\n"), "https://127.0.0.1:9600/basic", &["status:401"]),
        c("bearer authorization", &format!("{tls}authorization:\n  credentials: tok-123\n"), "https://127.0.0.1:9600/bearer", &["status:200", "bearer ok"]),
        c("bearer legacy", "bearer_token: tok-123\n", "http://127.0.0.1:9601/bearer", &["status:200", "bearer ok"]),
        c("oauth2 over tls", &format!("{tls}oauth2:\n  client_id: client\n  client_secret: s3cret\n  scopes: [read, write]\n  endpoint_params:\n    extra: \"1\"\n  token_url: https://127.0.0.1:9604/token\n  tls_config:\n    ca_file: $TD/tls-ca-chain.pem\n    cert_file: $TD/client.crt\n    key_file: $TD/client.key\n"), "https://127.0.0.1:9600/oauth-data", &["status:200", "oauth ok Bearer oauth-tok-"]),
        c("oauth2 bad secret", "oauth2:\n  client_id: client\n  client_secret: wrong\n  scopes: [read, write]\n  endpoint_params:\n    extra: \"1\"\n  token_url: http://127.0.0.1:9601/token\n", "http://127.0.0.1:9601/oauth-data", &["err:oauth2: cannot fetch token: 400 Bad Request"]),
        c("proxy http absolute-form", "proxy_url: http://puser:ppass@127.0.0.1:9602\n", "http://127.0.0.1:9601/hello", &["status:200", "hello http", "header:X-Via-Proxy"]),
        c("proxy https CONNECT", &format!("{tls}proxy_url: http://puser:ppass@127.0.0.1:9602\nproxy_connect_header:\n  X-Proxy-Secret: [s3]\n"), "https://127.0.0.1:9600/hello", &["status:200", "cn=localhost"]),
        c("proxy CONNECT without header", &format!("{tls}proxy_url: http://puser:ppass@127.0.0.1:9602\n"), "https://127.0.0.1:9600/hello", &["err:403 Forbidden"]),
        c("proxy wrong auth", "proxy_url: http://puser:bad@127.0.0.1:9602\n", "http://127.0.0.1:9601/hello", &["status:407"]),
        c("no_proxy bypass", "proxy_url: http://puser:ppass@127.0.0.1:9602\nno_proxy: 127.0.0.1\n", "http://127.0.0.1:9601/hello", &["status:200", "noheader:X-Via-Proxy"]),
        c("redirect followed", "follow_redirects: true\n", "http://127.0.0.1:9601/redirect", &["status:200", "hello http"]),
        c("redirect not followed", "follow_redirects: false\n", "http://127.0.0.1:9601/redirect", &["status:302"]),
        c("cross-host redirect drops credentials", "authorization:\n  credentials: tok-123\n", "http://127.0.0.1:9601/redirect-cross", &["status:401", "bad bearer \"\""]),
        c("redirect loop", "follow_redirects: true\n", "http://127.0.0.1:9601/loop", &["err:stopped after 10 redirects"]),
        c("http_headers", "http_headers:\n  X-Custom:\n    values: [abc]\n", "http://127.0.0.1:9601/echo", &["status:200", "GET abc"]),
        c("chunked body", "", "http://127.0.0.1:9601/chunked", &["status:200", "part0;part1;part2;part3;part4;"]),
        c("connection refused", "", "http://127.0.0.1:9649/x", &["err:connect: connection refused"]),
    ];
    cases.push(c("url user info is basic auth", "", "http://alice:wonderland@127.0.0.1:9601/basic", &["status:200", "basic ok"]));
    let mut slow = c("deadline covers a slow body", "", "http://127.0.0.1:9601/slow", &["err:context deadline exceeded"]);
    slow.limit = None;
    cases.push(slow);
    let mut post = c("307 keeps POST body", "follow_redirects: true\n", "http://127.0.0.1:9601/post307", &["status:200", "POST  payload"]);
    post.method = "POST";
    post.body = "payload";
    cases.push(post);
    let mut gz = c("gzip decoded", "", "http://127.0.0.1:9601/gzip", &["status:200", "metric_a 1\nmetric_a 1"]);
    gz.decompress = true;
    cases.push(gz);
    let mut gzl = c("gzip decoded over limit", "", "http://127.0.0.1:9601/gzip", &["err:body size limit exceeded"]);
    gzl.decompress = true;
    gzl.limit = Some(5000);
    cases.push(gzl);
    let mut big = c("body limit", "", "http://127.0.0.1:9601/big", &["err:body size limit exceeded"]);
    big.limit = Some(1000);
    cases.push(big);

    let mut all_ok = true;
    for cs in &cases {
        let cfg = match net::load_http_config(&cs.cfg) {
            Ok(c) => c,
            Err(e) => {
                println!("FAIL {}: config: {e}", cs.name);
                all_ok = false;
                continue;
            }
        };
        let client = match net::new_client_from_config(&cfg, &net::HttpClientOptions::default()) {
            Ok(c) => c,
            Err(e) => {
                println!("FAIL {}: client: {e}", cs.name);
                all_ok = false;
                continue;
            }
        };
        let mut req = net::Request::new(cs.method, cs.url, cs.body.as_bytes().to_vec()).unwrap();
        req.timeout = Some(if cs.url.ends_with("/slow") { Duration::from_secs(1) } else { Duration::from_secs(5) });
        req.decompress = cs.decompress;
        req.max_body_bytes = cs.limit;
        let r = client.do_request(req);
        let mut ok = true;
        let mut detail = String::new();
        for e in &cs.expect {
            let pass = match (e.split_once(':'), &r) {
                (Some(("err", s)), Err(err)) => err.msg.contains(s),
                (Some(("err", _)), Ok(_)) => false,
                (_, Err(_)) => false,
                (Some(("status", s)), Ok(resp)) => resp.status.to_string() == s,
                (Some(("header", h)), Ok(resp)) => resp.header.has(h),
                (Some(("noheader", h)), Ok(resp)) => !resp.header.has(h),
                (_, Ok(resp)) => String::from_utf8_lossy(&resp.body).contains(e),
            };
            if !pass {
                ok = false;
                detail = match &r {
                    Ok(resp) => format!("status {} body {:?}", resp.status, String::from_utf8_lossy(&resp.body).chars().take(200).collect::<String>()),
                    Err(e) => format!("error {}", e.msg),
                };
            }
        }
        let shown = match &r {
            Ok(resp) => format!("{} {:?}", resp.status, String::from_utf8_lossy(&resp.body).chars().take(60).collect::<String>()),
            Err(e) => format!("error: {}", e.msg),
        };
        if ok {
            println!("PASS {}: {}", cs.name, shown);
        } else {
            println!("FAIL {}: expected {:?}, got {}", cs.name, cs.expect, detail);
            all_ok = false;
        }
    }

    // keep-alive reuse and token caching: 200 requests over one TLS client
    let cfg = net::load_http_config(&format!("{tls}oauth2:\n  client_id: client\n  client_secret: s3cret\n  scopes: [read, write]\n  endpoint_params:\n    extra: \"1\"\n  token_url: http://127.0.0.1:9601/token\n").replace("$TD", TD)).unwrap();
    let client = net::new_client_from_config(&cfg, &net::HttpClientOptions::default()).unwrap();
    let t0 = Instant::now();
    let mut bodies = std::collections::BTreeSet::new();
    for _ in 0..200 {
        let r = client.get("https://127.0.0.1:9600/oauth-data").unwrap();
        bodies.insert(String::from_utf8_lossy(&r.body).into_owned());
    }
    let one_token = bodies.len() == 1;
    println!("{} 200 keep-alive TLS requests with one cached OAuth2 token in {:?} ({} distinct bodies: {:?})", if one_token { "PASS" } else { "FAIL" }, t0.elapsed(), bodies.len(), bodies.iter().next());
    all_ok &= one_token;
    all_ok
}
