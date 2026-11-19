//! Port of `notifier/alertmanager.go`: Alertmanager endpoints from service discovery target groups.

use config::AlertmanagerConfig;
use discovery::Group;
use model::labels::{Builder, Labels};
use model::labelset::ADDRESS_LABEL;
use model::relabel;
use yaml::url::GoUrl;

/// Go `model.SchemeLabel`.
pub const SCHEME_LABEL: &str = "__scheme__";
/// Go `pathLabel`.
pub const PATH_LABEL: &str = "__alerts_path__";

/// Alertmanager endpoint information (Go `alertmanager` interface): its URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Alertmanager {
    url: GoUrl,
}

impl Alertmanager {
    /// Go `alertmanagerLabels.url()`: scheme, address and path from the labels.
    pub fn from_labels(ls: &Labels) -> Alertmanager {
        Alertmanager {
            url: GoUrl { scheme: ls.get(SCHEME_LABEL).to_string(), host: ls.get(ADDRESS_LABEL).to_string(), path: ls.get(PATH_LABEL).to_string(), ..Default::default() },
        }
    }

    /// An endpoint from a URL string (the test mock `alertmanagerMock`). Panics on a bad URL, as the mock does.
    pub fn from_url(u: &str) -> Alertmanager {
        match yaml::url::parse(u) {
            Ok(url) => Alertmanager { url },
            Err(e) => panic!("{e}"),
        }
    }

    pub fn url(&self) -> &GoUrl {
        &self.url
    }

    /// Go `url().String()`.
    pub fn url_string(&self) -> String {
        self.url.to_url_string()
    }
}

/// Go `AlertmanagerFromGroup`: the Alertmanagers of a target group, and the ones dropped by relabeling.
pub fn alertmanager_from_group(tg: &Group, cfg: &AlertmanagerConfig) -> Result<(Vec<Alertmanager>, Vec<Alertmanager>), String> {
    let mut res = Vec::new();
    let mut dropped = Vec::new();
    let mut lb = Builder::new(Labels::empty());
    let path = post_path(&cfg.path_prefix, &cfg.api_version.0);

    for tlset in &tg.targets {
        lb.reset(Labels::empty());
        for (ln, lv) in &tlset.0 {
            lb.set(ln, lv);
        }
        // Set configured scheme as the initial scheme label for overwrite.
        lb.set(SCHEME_LABEL, &cfg.scheme);
        lb.set(PATH_LABEL, &path);

        // Combine target labels with target group labels.
        for (ln, lv) in &tg.labels.0 {
            if !tlset.0.contains_key(ln) {
                lb.set(ln, lv);
            }
        }

        let pre_relabel = lb.labels();
        if !relabel::process_builder(&mut lb, &cfg.relabel_configs) {
            dropped.push(Alertmanager::from_labels(&pre_relabel));
            continue;
        }

        let addr = lb.get(ADDRESS_LABEL).to_string();
        config::check_target_address(&addr)?;

        res.push(Alertmanager::from_labels(&lb.labels()));
    }
    Ok((res, dropped))
}

/// Go `postPath`: `path.Join("/", pre, "/api/<v>/alerts")`.
pub fn post_path(pre: &str, v: &str) -> String {
    let joined = format!("/{pre}/api/{v}/alerts");
    clean_path(&joined)
}

/// Go `path.Clean` for an absolute path.
fn clean_path(p: &str) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            s => parts.push(s),
        }
    }
    let mut out = String::with_capacity(p.len());
    for s in &parts {
        out.push('/');
        out.push_str(s);
    }
    if out.is_empty() {
        out.push('/');
    }
    out
}
