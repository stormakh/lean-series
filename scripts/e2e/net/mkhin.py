import os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
p = 'rcmp/src/main.rs'
s = open(p).read()
s = s.replace('''            // HTTP_MODE
''', '''            "http" => match net::load_http_config(&b) {
                Err(e) => println!("ERR: {e}"),
                Ok(c) => print!("{}", c.to_yaml_string()),
            },
''')
open(p, 'w').write(s)
p = 'rcmp/Cargo.toml'
s = open(p).read()
if 'net =' not in s:
    s += 'net = { path = "../../../../crates/net" }\n'
open(p, 'w').write(s)
os.makedirs('hin', exist_ok=True)
cases = {
 'unknown': "basic_auth:\n  username: a\n  passwd: b\nfoo: 1\ntls_config:\n  bad: x\n",
 'types': "follow_redirects: maybe\nenable_http2: [1]\nbasic_auth: str\n",
 'tlsver': "tls_config:\n  min_version: TLS12\n  max_version: TLS13\n  insecure_skip_verify: true\n  server_name: foo\n",
 'tlsbadver': "tls_config:\n  min_version: TLS9\n",
 'full': """basic_auth:
  username: u
  password: p
tls_config:
  ca_file: /x/ca.pem
  cert: |
    -----BEGIN CERT-----
    abc
  key: secret
proxy_url: http://user:pass@proxy:3128/x
no_proxy: a.b,c
proxy_connect_header:
  X-B: [b1]
  A: [a1, a2]
follow_redirects: false
http_headers:
  X-Z:
    values: [v]
    secrets: [s]
    files: [f]
  X-A:
    values: ["needs: quoting", "true", ""]
""",
 'oauth': """oauth2:
  client_id: id
  client_secret: s
  scopes: [a, b]
  token_url: http://t/token
  endpoint_params: {z: 1, a: "2"}
  claims:
    n: 1
    s: x
    l: [1, two]
  tls_config:
    insecure_skip_verify: true
  proxy_url: http://p
enable_http2: false
""",
 'authz': "authorization:\n  type: '  Token '\n  credentials_file: /t\n",
 'bearer': "bearer_token: abc\n",
 'dupfield': "follow_redirects: true\nfollow_redirects: false\n",
 'null': "basic_auth:\ntls_config:\nhttp_headers:\n",
 'anchors': "x: &a\n  username: u\nbasic_auth: *a\n",
 'merge': "basic_auth:\n  <<: {username: u, password: p}\n  username: v\n",
 'badurl': "proxy_url: '::bad'\n",
 'headers_dup': "http_headers:\n  A:\n    values: [x]\n  A:\n    values: [y]\n",
 'proxy_env': "proxy_from_environment: true\n",
 'quotednull': "bearer_token_file: \"null\"\nbasic_auth: ~\n",
 'seqitems': "oauth2:\n  client_id: x\n  token_url: y\n  scopes: [a, {b: 1}, c]\n",
}
for k, v in cases.items():
    open('hin/' + k + '.yml', 'w').write(v)
