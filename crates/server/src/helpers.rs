//! The small functions of `cmd/prometheus/main.go`, `upload.go` (delayed compaction file) and `warnings.go`.

use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use discovery::Logger;
use yaml::url::GoUrl;

/// Go `startsOrEndsWithQuote`.
pub fn starts_or_ends_with_quote(s: &str) -> bool {
    s.starts_with('"') || s.starts_with('\'') || s.ends_with('"') || s.ends_with('\'')
}

/// Go `net.SplitHostPort` (the port part).
pub fn split_host_port(hostport: &str) -> Result<(String, String), String> {
    let missing = || format!("address {hostport}: missing port in address");
    let i = hostport.rfind(':').ok_or_else(missing)?;
    let (host, port) = (&hostport[..i], &hostport[i + 1..]);
    if let Some(h) = host.strip_prefix('[') {
        let h = h.strip_suffix(']').ok_or_else(|| format!("address {hostport}: missing ']' in address"))?;
        return Ok((h.to_string(), port.to_string()));
    }
    if host.contains(':') {
        return Err(format!("address {hostport}: too many colons in address"));
    }
    Ok((host.to_string(), port.to_string()))
}

/// Go `computeExternalURL`: the sanitized external URL. Unset parts come from the host name and the listen
/// address; with an empty `u` the scheme is https when `web_config_file` configures TLS.
pub fn compute_external_url(u: &str, listen_addr: &str, web_config_file: &str) -> Result<GoUrl, String> {
    let mut u = u.to_string();
    if u.is_empty() {
        let hostname = crate::sys::hostname()?;
        let (_, port) = split_host_port(listen_addr)?;
        let scheme = if web_config_has_tls_server_config(web_config_file) { "https" } else { "http" };
        u = format!("{scheme}://{hostname}:{port}/");
    }
    if starts_or_ends_with_quote(&u) {
        return Err("URL must not begin or end with quotes".to_string());
    }
    let mut eu = yaml::url::parse(&u)?;
    let mut ppref = eu.path.trim_end_matches('/').to_string();
    if !ppref.is_empty() && !ppref.starts_with('/') {
        ppref = format!("/{ppref}");
    }
    eu.path = ppref;
    Ok(eu)
}

/// Go `webConfigHasTLSServerConfig`: whether the web config file has a `tls_server_config` with a field that
/// enables TLS (exporter-toolkit `TLSConfig.IsEnabled`). Unreadable or unparsable files do not.
pub fn web_config_has_tls_server_config(path: &str) -> bool {
    if path.is_empty() {
        return false;
    }
    let Ok(data) = std::fs::read_to_string(path) else {
        return false;
    };
    let Ok(docs) = yaml_rust2::YamlLoader::load_from_str(&data) else {
        return false;
    };
    let Some(doc) = docs.first() else {
        return false;
    };
    let tls = &doc["tls_server_config"];
    if tls.as_hash().is_none() {
        return false;
    }
    ["cert_file", "cert", "key_file", "key", "client_ca_file", "client_ca", "client_auth_type"]
        .iter()
        .any(|k| tls[*k].as_str().is_some_and(|s| !s.is_empty()))
}

/// Go `storagePathFsSize`: the file system size of `path` or of its closest existing parent.
pub fn storage_path_fs_size(path: &str) -> u64 {
    let mut path = std::path::PathBuf::from(path);
    loop {
        let size = crate::sys::fs_size(&path.to_string_lossy());
        if size > 0 {
            return size;
        }
        match std::fs::metadata(&path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            _ => return 0,
        }
        let parent = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
            // Go filepath.Dir("data") is ".".
            Some(_) if path.as_os_str() != "." => std::path::PathBuf::from("."),
            _ => return 0,
        };
        if parent == path {
            return 0;
        }
        path = parent;
    }
}

/// Go `safePromQLNoStepSubqueryInterval`.
#[derive(Default)]
pub struct SafeNoStepSubqueryInterval(AtomicI64);

impl SafeNoStepSubqueryInterval {
    /// Sets the interval (a `model.Duration` in nanoseconds; stored in milliseconds).
    pub fn set(&self, ev_nanos: i64) {
        self.0.store(ev_nanos / 1_000_000, Ordering::SeqCst);
    }
    pub fn get(&self, _: i64) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Go `UploadMeta` (the Thanos shipper file).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct UploadMeta {
    pub uploaded: Vec<String>,
}

/// Go's cache of the last read upload meta (`tsdbDelayCompactLastMeta`, `tsdbDelayCompactLastMetaTime`).
pub static TSDB_DELAY_COMPACT_LAST_META: Mutex<Option<(UploadMeta, SystemTime)>> = Mutex::new(None);

fn parse_upload_meta(data: &str) -> Result<UploadMeta, String> {
    let v = json::Json::parse(data).map_err(|e| e.to_string())?;
    let mut m = UploadMeta::default();
    if let Some(u) = v.get("uploaded") {
        if let json::Json::Arr(arr) = u {
            for x in arr {
                match x.as_str() {
                    Some(s) => m.uploaded.push(s.to_string()),
                    None => return Err("json: cannot unmarshal into Go struct field UploadMeta.uploaded of type string".to_string()),
                }
            }
        } else if !u.is_null() {
            return Err("json: cannot unmarshal into Go struct field UploadMeta.uploaded of type []string".to_string());
        }
    }
    Ok(m)
}

/// Go `exludeBlocksPendingUpload`: excludes level-1 blocks that the upload meta file does not list as uploaded.
/// The file is read at most once a minute.
pub fn exclude_blocks_pending_upload(logger: Logger, upload_meta_path: String) -> tsdb::compact::BlockExcludeFilterFunc {
    Arc::new(move |meta: &tsdb::BlockMeta| -> bool {
        if meta.compaction.level > 1 {
            // Blocks with level > 1 are assumed to be not uploaded.
            return false;
        }
        let id = meta.ulid.to_string();
        {
            let cache = TSDB_DELAY_COMPACT_LAST_META.lock().unwrap_or_else(|p| p.into_inner());
            if let Some((m, t)) = cache.as_ref() {
                if *t > SystemTime::now() - Duration::from_secs(60) {
                    return !m.uploaded.contains(&id);
                }
            }
        }
        let data = match std::fs::read_to_string(&upload_meta_path) {
            Ok(d) => d,
            Err(e) => {
                logger.warn("cannot open TSDB upload meta file", &[("path", &upload_meta_path), ("err", &net::types::go_io_error(&e))]);
                return false;
            }
        };
        let m = match parse_upload_meta(&data) {
            Ok(m) => m,
            Err(e) => {
                logger.warn("cannot parse TSDB upload meta file", &[("path", &upload_meta_path), ("err", &e)]);
                return false;
            }
        };
        let excluded = !m.uploaded.contains(&id);
        *TSDB_DELAY_COMPACT_LAST_META.lock().unwrap_or_else(|p| p.into_inner()) = Some((m, SystemTime::now()));
        excluded
    })
}

/// Go `rest.WarningHandlerWithContext`.
pub trait WarningHandler: Send {
    fn handle_warning_header(&mut self, code: i32, agent: &str, message: &str);
}

/// Go `deprecationRegex`: `\S+ \S+ is deprecated in v\d+\.\d+\+`.
fn is_deprecation(message: &str) -> bool {
    static RE: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    RE.get_or_init(|| regex::Regex::new(r"[^\s]+ [^\s]+ is deprecated in v[0-9]+\.[0-9]+\+").expect("valid regex")).is_match(message)
}

/// Go `maxDeprecationWarnings`.
pub const MAX_DEPRECATION_WARNINGS: usize = 32;

/// Go `dedupDeprecationWarningLogger`: logs each Kubernetes API deprecation warning once (up to 32 distinct
/// ones); other warnings always pass.
pub struct DedupDeprecationWarningLogger {
    pub logger: Box<dyn WarningHandler>,
    pub logged: Mutex<HashSet<String>>,
}

impl DedupDeprecationWarningLogger {
    pub fn new(logger: Box<dyn WarningHandler>) -> DedupDeprecationWarningLogger {
        DedupDeprecationWarningLogger { logger, logged: Mutex::new(HashSet::new()) }
    }

    /// Go `HandleWarningHeaderWithContext`.
    pub fn handle_warning_header(&mut self, code: i32, agent: &str, message: &str) {
        if code != 299 || message.is_empty() {
            return;
        }
        let mut logged = self.logged.lock().unwrap_or_else(|p| p.into_inner());
        if logged.contains(message) {
            return;
        }
        if is_deprecation(message) && logged.len() < MAX_DEPRECATION_WARNINGS {
            logged.insert(message.to_string());
        }
        drop(logged);
        self.logger.handle_warning_header(code, agent, message);
    }
}

/// Go `time.Duration.String()` of nanoseconds.
pub fn go_duration_string(ns: i64) -> String {
    if ns < 0 {
        return format!("-{}", util::logging::go_duration(Duration::from_nanos(ns.unsigned_abs())));
    }
    util::logging::go_duration(Duration::from_nanos(ns as u64))
}

/// Go `strconv.FormatFloat(f, 'g', -1, 64)` (kingpin's float value `String()`).
pub fn format_float_g(f: f64) -> String {
    yaml::format_float_g(f)
}
