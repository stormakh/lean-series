//! Go `clientcmd.BuildConfigFromFlags("", path)`: the API server and credentials of the current context of a
//! kubeconfig file.
//!
//! Supported: clusters (server, certificate-authority(-data), insecure-skip-tls-verify, tls-server-name,
//! proxy-url), users (client-certificate(-data), client-key(-data), token, tokenFile, username/password) and
//! contexts. Not supported (an error, so that a config never silently loses its credentials): `exec` credential
//! plugins, `auth-provider` plugins (gcp, oidc, azure), and impersonation (`as`, `as-groups`, `as-uid`,
//! `as-user-extra`). Relative file paths are resolved against the directory of the kubeconfig file, as
//! client-go does.
use std::rc::Rc;

use yaml::node::{Kind as NodeKind, Node};

use super::client::RestConfig;

fn deref(n: &Rc<Node>) -> &Rc<Node> {
    match (&n.kind, &n.alias) {
        (NodeKind::Alias, Some(a)) => a,
        _ => n,
    }
}

fn get<'a>(n: Option<&'a Rc<Node>>, key: &str) -> Option<&'a Rc<Node>> {
    let n = deref(n?);
    if n.kind != NodeKind::Mapping {
        return None;
    }
    n.pairs().find(|(k, _)| deref(k).value == key).map(|(_, v)| deref(v)).filter(|v| !v.is_null())
}

fn s(n: Option<&Rc<Node>>, key: &str) -> String {
    get(n, key).filter(|v| v.kind == NodeKind::Scalar).map(|v| v.value.clone()).unwrap_or_default()
}

fn b(n: Option<&Rc<Node>>, key: &str) -> bool {
    matches!(s(n, key).as_str(), "true" | "True" | "TRUE" | "yes" | "on")
}

/// The item named `name` of a list of `{name, <field>: {...}}` entries.
fn named<'a>(root: Option<&'a Rc<Node>>, list: &str, field: &str, name: &str) -> Option<&'a Rc<Node>> {
    let l = get(root, list)?;
    l.children.iter().map(deref).find(|i| s(Some(i), "name") == name).and_then(|i| get(Some(i), field))
}

fn resolve(dir: &str, p: &str) -> String {
    if p.is_empty() || p.starts_with('/') { p.to_string() } else { format!("{dir}/{p}") }
}

fn data(n: Option<&Rc<Node>>, key: &str) -> Result<String, String> {
    let v = s(n, key);
    if v.is_empty() {
        return Ok(v);
    }
    let raw: String = v.chars().filter(|c| !c.is_whitespace()).collect();
    let d = crypto::base64_decode(&raw).ok_or_else(|| format!("illegal base64 data in {key}"))?;
    String::from_utf8(d).map_err(|_| format!("invalid {key}: not PEM text"))
}

/// Loads the kubeconfig file `path`.
pub fn load(path: &str) -> Result<RestConfig, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("stat {path}: {}", net::types::go_io_error(&e)))?;
    let dir = std::path::Path::new(path).parent().map(|p| p.to_string_lossy().to_string()).filter(|d| !d.is_empty()).unwrap_or_else(|| ".".to_string());
    parse(&text, &dir)
}

/// Parses kubeconfig text; relative paths are resolved against `dir`.
pub fn parse(text: &str, dir: &str) -> Result<RestConfig, String> {
    let root = yaml::node::parse(text).map_err(|e| format!("error loading config: {e}"))?;
    let root = root.as_ref();
    let ctx_name = s(root, "current-context");
    if ctx_name.is_empty() {
        return Err("invalid configuration: no configuration has been provided, try setting KUBERNETES_MASTER environment variable".to_string());
    }
    let ctx = named(root, "contexts", "context", &ctx_name).ok_or_else(|| format!("invalid configuration: context was not found for specified context: {ctx_name}"))?;
    let cluster_name = s(Some(ctx), "cluster");
    let user_name = s(Some(ctx), "user");
    let cluster = named(root, "clusters", "cluster", &cluster_name);
    if cluster.is_none() && !cluster_name.is_empty() {
        return Err(format!("invalid configuration: cluster {} was not found for context {}", yaml::go_quote(&cluster_name), yaml::go_quote(&ctx_name)));
    }
    let server = s(cluster, "server");
    if server.is_empty() {
        return Err("invalid configuration: no server found for cluster ".to_string() + &yaml::go_quote(&cluster_name));
    }
    let user = named(root, "users", "user", &user_name);
    if !user_name.is_empty() && user.is_none() {
        return Err(format!("invalid configuration: user {} was not found for context {}", yaml::go_quote(&user_name), yaml::go_quote(&ctx_name)));
    }
    for (k, what) in [("exec", "exec credential plugins"), ("auth-provider", "auth-provider plugins"), ("as", "impersonation"), ("as-groups", "impersonation"), ("as-uid", "impersonation"), ("as-user-extra", "impersonation")] {
        if get(user, k).is_some() {
            return Err(format!("kubeconfig user {}: {what} are not supported", yaml::go_quote(&user_name)));
        }
    }

    let mut http = net::HttpClientConfig::default_config();
    let tls = &mut http.tls_config;
    let ca_data = data(cluster, "certificate-authority-data")?;
    if !ca_data.is_empty() {
        tls.ca = ca_data;
    } else {
        tls.ca_file = resolve(dir, &s(cluster, "certificate-authority"));
    }
    tls.insecure_skip_verify = b(cluster, "insecure-skip-tls-verify");
    tls.server_name = s(cluster, "tls-server-name");
    let cert_data = data(user, "client-certificate-data")?;
    if !cert_data.is_empty() {
        tls.cert = cert_data;
    } else {
        tls.cert_file = resolve(dir, &s(user, "client-certificate"));
    }
    let key_data = data(user, "client-key-data")?;
    if !key_data.is_empty() {
        tls.key = yaml::Secret(key_data);
    } else {
        tls.key_file = resolve(dir, &s(user, "client-key"));
    }
    let token = s(user, "token");
    let token_file = resolve(dir, &s(user, "tokenFile"));
    if !token_file.is_empty() {
        http.authorization = Some(net::Authorization { type_: "Bearer".to_string(), credentials_file: token_file, ..Default::default() });
    } else if !token.is_empty() {
        http.authorization = Some(net::Authorization { type_: "Bearer".to_string(), credentials: yaml::Secret(token), ..Default::default() });
    }
    let username = s(user, "username");
    let password = s(user, "password");
    if http.authorization.is_none() && (!username.is_empty() || !password.is_empty()) {
        http.basic_auth = Some(net::BasicAuth { username, password: yaml::Secret(password), ..Default::default() });
    }
    let proxy = s(cluster, "proxy-url");
    if proxy.is_empty() {
        http.proxy_config.proxy_from_environment = true;
    } else {
        http.proxy_config.proxy_url = yaml::Url::parse(&proxy).map_err(|e| format!("invalid proxy-url: {e}"))?;
    }
    Ok(RestConfig { host: server, http })
}
