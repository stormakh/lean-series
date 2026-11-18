//! The HTTP client side of prometheus/common config (http_config.go) and prometheus/sigv4: the
//! HTTPClientConfig YAML type with Go's validation messages, and a blocking HTTP/1.1 + TLS (rustls) client built
//! from it with the same layers (authorization, basic auth, OAuth2, extra headers, TLS file reloading, proxies,
//! redirects).
//!
//! ```ignore
//! let cfg: net::HttpClientConfig = yaml::unmarshal_strict(text)?;          // or net::load_http_config_file(path)
//! let client = net::new_client_from_config(&cfg, &net::HttpClientOptions::default().with_user_agent("Prometheus/3.16.0"))?;
//! let mut req = net::Request::get("https://host:9100/metrics")?;
//! req.timeout = Some(std::time::Duration::from_secs(10));
//! let resp = client.do_request(req)?;                                         // resp.status, resp.header, resp.body
//! ```
pub mod client;
pub mod config;
pub mod oauth2;
pub mod proxy;
pub mod roundtrip;
pub mod secret;
pub mod sigv4;
pub mod tls;
pub mod transport;
pub mod types;

pub use client::{Client, HttpClientOptions, new_client_from_config, new_round_tripper_from_config};
pub use config::{Authorization, BasicAuth, Headers, HttpClientConfig, OAuth2, ProxyConfig, TlsConfig, TlsVersion, load_http_config, load_http_config_file};
pub use roundtrip::RoundTripper;
pub use secret::{SecretManager, SecretReader};
pub use sigv4::{SigV4Config, SigV4RoundTripper};
pub use transport::{Transport, TransportOptions};
pub use types::{Error, ErrorKind, Header, Request, Response};
