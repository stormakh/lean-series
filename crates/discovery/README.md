# discovery

Port of Prometheus `discovery/`: the `Config` and `Discoverer` traits, the registry of SD config types,
`Configs` (the `<name>_sd_configs` YAML keys), the discovery `Manager`, the `refresh` helper and the `static`,
`file` and `http` providers. Blocking threads, no async runtime.

- Go `context.Context` -> `CancelToken` (`cancel`, `is_cancelled`, `wait_timeout`, `child`).
- Go `chan<- []*targetgroup.Group` -> `chan::Sender`: `send(Vec<Arc<Group>>) -> bool` blocks until the
  receiver took the update and returns false when the discoverer was cancelled (then return from `run`).
  Returning from `run` drops the sender, which closes the channel (Go `close(ch)`).
- Metrics: `metrics::Registry` with counters, gauges, summaries and histograms (upstream names and labels).
- Logging: `Logger` (no-op unless a sink is installed).

## Adding a provider

One module and one registry line.

1. Create `src/<name>.rs` (or `src/<name>/mod.rs`) with the port of `discovery/<name>/`:
   - `pub struct SdConfig { ... }` with `#[derive(Clone, Debug, Default, PartialEq)]`; `Default` must be
     the Go zero value. Describe the YAML fields with `yaml::yaml_struct!(SdConfig, "<pkg>.plain", [...])`
     (use `(inline http_client_config)` for an inline `config.HTTPClientConfig`).
   - `impl yaml::Unmarshal for SdConfig`: the Go `UnmarshalYAML`, usually
     `d.custom(|d| { *self = default_sd_config(); d.inner_struct(n, self)?; <validation, same error texts> })`.
   - `impl yaml::Marshal for SdConfig` (`yaml::marshal_struct(self)`; secrets are `yaml::Secret`).
   - `impl SdConfigType for SdConfig { const GO_TYPE: &'static str = "<pkg>.SDConfig"; }`
   - `impl Config for SdConfig`: `name()` (the YAML key is `<name>_sd_configs`), `new_discoverer`,
     `new_discoverer_metrics` (if the Go provider has metrics; otherwise the no-op default), `set_directory`
     (if it has file paths; `net::config::join_dir`).
   - The discoverer: implement `Discoverer::run(&mut self, ctx, up)`. For a periodic refresh, write a
     `refresh::Refresher` and wrap it in `refresh::Discovery` (see `http.rs`); the refresh metrics come from
     `opts.metrics` (downcast with `downcast_metrics::<YourMetrics>()`) holding the `RefreshMetricsInstantiator`.
   - HTTP: `net::new_client_from_config(&cfg.http_client_config, &opts.http_client_options)`.
2. Add `pub mod <name>;` to `src/lib.rs` and one line to `builtin()` in `src/registry.rs`:
   `registration::<crate::<name>::SdConfig>(),`
3. Port the Go tests to `tests/<file>.rs` (ledger names). Config tests in `crates/config/tests` that wait for
   the provider run automatically once it is registered (they check `registered_config_names()`); remove the
   `#[ignore = "waits for SD providers: ..."]` of tests whose providers are now all ported, and translate the
   commented Go literals marked `// waits for <name>:` in `crates/config/tests/config_test/expected_conf.rs`.

## Differences from upstream

- `file_sd_configs` watches by polling (every 250ms: a stat of the watched directories and of the files read
  last time; mode-only changes are ignored as Go ignores chmod events) instead of inotify, plus the full
  refresh at `refresh_interval`.
- The manager's sender sleeps until a provider sends an update instead of ticking every 100ms-5s; when it
  wakes after an idle period it waits 0-150ms (Go: until its next tick, 50-150ms apart when idle).
- `http_sd_configs`: a request in flight is not aborted on cancel; it ends at its timeout (the refresh
  interval, as Go's client timeout).
- Summaries have count and sum only (no quantiles).
- Cloud providers (aws/ec2/ecs/elasticache/lightsail/msk/rds, azure, gce, oci, openstack) talk to the APIs over
  plain HTTP + JSON/XML (no SDKs): `aws::client` (SigV4, Query/JSON protocols, SDK-style errors and retries),
  `azure/arm.rs` + `azure/identity.rs`, `oci/client.rs` (HTTP signatures), `openstack/client.rs` (Keystone v3).
  Not supported (clear error): AWS IAM Identity Center (SSO) and `login_session` profiles; Azure PKCS#12
  certificates, username/password, PowerShell and the Arc/ML/Cloud Shell/Service Fabric managed identities;
  GCE external_account, impersonated and OAuth client credential files; OCI instance_principal (ring cannot
  generate the session RSA key), DES/3DES/GCM/scrypt encrypted keys and RSA keys under 2048 bits; OpenStack
  Keystone v2.0. A request in flight is not aborted on cancel; it ends at its timeout.
- `bench/sd-cloud-e2e/run.sh` compares the discovered labels of all cloud providers with real Prometheus 3.16.0
  on recorded API responses.

## Providers ported from upstream (beyond static, file, http)

consul, digitalocean, dns, docker and dockerswarm (`moby`), eureka, hetzner, ionos, linode, marathon, nerve and
serverset (`zookeeper`), nomad, outscale, ovhcloud, puppetdb, scaleway, stackit, triton, uyuni, vultr. No SDK
crates: each module ports the part of the Go API client that upstream uses (paths, query, headers, paging, auth,
error texts). Clients written in house: DNS (`dns.rs`, miekg/dns subset with resolv.conf, search list, UDP with
TCP fallback), ZooKeeper (`zookeeper/zk.rs`, go-zookeeper/zk subset with watches), XML-RPC (`uyuni/xmlrpc.rs`),
XML (`eureka/xml.rs`), Docker over a Unix socket (`moby/client.rs`).

Shared test helpers: `tests/common/` (a Go httptest-like server). Go test files with the same name in several
packages (`instance_test`, `server_test`, `client_test`) are one test file with one module per package.

Differences that apply to these providers:
- A request in flight is not aborted on cancel (except consul, which abandons it); it ends at its timeout.
- Go-style typed JSON decoding checks only the fields a provider reads; numbers in type errors print from the
  parsed value; some syntax error texts differ. There are several copies of this decoder (`consul/goutil.rs`,
  `gojson.rs`, `linode/gojson.rs`, `marathon/gojson.rs`, `sd_json.rs`); they should become one.
- Where Go iterates a map in random order, the port uses sorted or response order.
- Where Go panics on a null pointer in an API response, the port uses the zero value.
- The ZooKeeper tree cache metrics are process-wide (`zookeeper::treecache::treecache_metrics()`); the server must
  register them (Go: default registry).
