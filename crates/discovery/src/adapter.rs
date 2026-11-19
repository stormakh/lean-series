//! Port of `documentation/examples/custom-sd/adapter`: runs a custom service discovery implementation and
//! writes its target groups to a file in the `file_sd` JSON format.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::manager::TargetSets;
use crate::{CancelToken, Discoverer, Group, Logger, Manager, ManagerOptions, Registry, SdMetrics};

/// Go `customSD`: one target group in the output file.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CustomSd {
    pub targets: Vec<String>,
    pub labels: BTreeMap<String, String>,
}

impl CustomSd {
    fn to_json(&self) -> json::Json {
        let mut o = json::Json::obj();
        o.set("targets", json::Json::Arr(self.targets.iter().map(|t| json::Json::Str(t.clone())).collect()));
        o.set("labels", json::Json::Obj(self.labels.iter().map(|(k, v)| (k.clone(), json::Json::Str(v.clone()))).collect()));
        o
    }
}

/// Go `fingerprint`: the XOR of the fingerprints of the targets and the group labels.
fn fingerprint(group: &Group) -> u64 {
    let mut group_fingerprint = model::labelset::LabelSet::new().fingerprint();
    for targets in &group.targets {
        group_fingerprint ^= targets.fingerprint();
    }
    group_fingerprint ^ group.labels.fingerprint()
}

/// Go `generateTargetGroups`: the groups by a unique key `<sd name>:<source>:<fingerprint>` (the fingerprint in
/// case the SD name and the source are not unique). Targets are sorted.
pub fn generate_target_groups(all_target_groups: &TargetSets) -> BTreeMap<String, CustomSd> {
    let mut groups = BTreeMap::new();
    for (k, sd_target_groups) in all_target_groups {
        for group in sd_target_groups {
            let mut new_targets: Vec<String> = Vec::new();
            for targets in &group.targets {
                for target in targets.values() {
                    new_targets.push(target.clone());
                }
            }
            new_targets.sort();
            let new_labels: BTreeMap<String, String> = group.labels.iter().map(|(n, v)| (n.clone(), v.clone())).collect();
            let key = format!("{}:{}:{:016x}", k, group.source, fingerprint(group));
            groups.insert(key, CustomSd { targets: new_targets, labels: new_labels });
        }
    }
    groups
}

/// Go `Adapter`: runs an unknown service discovery implementation and converts its target groups to JSON in a
/// file for `file_sd`.
pub struct Adapter {
    ctx: CancelToken,
    disc: Mutex<Option<Box<dyn Discoverer>>>,
    groups: Mutex<BTreeMap<String, CustomSd>>,
    manager: Manager,
    output: String,
    name: String,
    logger: Logger,
}

impl Adapter {
    /// Go `NewAdapter`. Go's manager constructor returns nil when its metrics cannot be registered; this returns
    /// that error.
    pub fn new(
        ctx: &CancelToken,
        file: &str,
        name: &str,
        d: Option<Box<dyn Discoverer>>,
        logger: Logger,
        sd_metrics: &SdMetrics,
        registerer: &Arc<Registry>,
    ) -> Result<Arc<Adapter>, String> {
        let manager = Manager::new(ctx, logger.clone(), registerer, sd_metrics, ManagerOptions::default())?;
        Ok(Arc::new(Adapter {
            ctx: ctx.clone(),
            disc: Mutex::new(d),
            groups: Mutex::new(BTreeMap::new()),
            manager,
            output: file.to_string(),
            name: name.to_string(),
            logger,
        }))
    }

    /// Go `refreshTargetGroups`: writes the output file when the groups changed.
    fn refresh_target_groups(&self, all_target_groups: &TargetSets) {
        let temp_groups = generate_target_groups(all_target_groups);
        let mut groups = self.groups.lock().unwrap_or_else(|e| e.into_inner());
        if *groups != temp_groups {
            *groups = temp_groups;
            if let Err(err) = write_output(&groups, &self.output) {
                self.logger.with(&[("component", "sd-adapter")]).error("failed to write output", &[("err", &err)]);
            }
        }
    }

    /// Go `writeOutput`: writes the groups as JSON to the output file (through a temporary file and a rename).
    /// Go writes the groups in map order (random); this writes them sorted by key.
    pub fn write_output(&self) -> Result<(), String> {
        let groups = self.groups.lock().unwrap_or_else(|e| e.into_inner());
        write_output(&groups, &self.output)
    }

    /// Go `runCustomSD`: refreshes the output on every update until the manager stops.
    fn run_custom_sd(&self) {
        let updates = self.manager.sync_ch();
        // The channel closes when the manager stops (context cancelled).
        while let Some(all_target_groups) = updates.recv() {
            self.refresh_target_groups(&all_target_groups);
        }
    }

    /// Go `Run`: starts the discovery manager and the custom service discovery implementation.
    pub fn run(self: &Arc<Self>) {
        let m = self.manager.clone();
        let _ = std::thread::Builder::new().name("sd-adapter-manager".into()).spawn(move || {
            let _ = m.run();
        });
        if let Some(d) = self.disc.lock().unwrap_or_else(|e| e.into_inner()).take() {
            self.manager.start_custom_provider(&self.ctx, &self.name, d);
        }
        let a = self.clone();
        let _ = std::thread::Builder::new().name("sd-adapter".into()).spawn(move || a.run_custom_sd());
    }
}

/// Go `mapToArray` + `json.MarshalIndent(arr, "", "    ")` + the temporary file and rename.
fn write_output(groups: &BTreeMap<String, CustomSd>, output: &str) -> Result<(), String> {
    let arr = json::Json::Arr(groups.values().map(CustomSd::to_json).collect());
    let b = arr.stringify_pretty("    ");

    // Go `filepath.Split`: the directory part, with its trailing separator; empty means the temp directory.
    let dir = match output.rfind('/') {
        Some(i) => &output[..=i],
        None => "",
    };
    let dir = if dir.is_empty() { std::env::temp_dir() } else { Path::new(dir).to_path_buf() };
    let (tmp_path, mut tmpfile) = create_temp(&dir, "sd-adapter")?;
    tmpfile.write_all(b.as_bytes()).map_err(|e| format!("write {}: {}", tmp_path.display(), e))?;
    // Close the file before the rename, for platforms that cannot move an open file.
    drop(tmpfile);
    std::fs::rename(&tmp_path, output).map_err(|e| format!("rename {} {}: {}", tmp_path.display(), output, e))
}

/// Go `os.CreateTemp(dir, pattern)`: a new file named pattern + random digits.
fn create_temp(dir: &Path, pattern: &str) -> Result<(std::path::PathBuf, std::fs::File), String> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    for _ in 0..10000 {
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.subsec_nanos()).unwrap_or(0);
        let n = (std::process::id() as u64) ^ ((nanos as u64) << 16) ^ COUNTER.fetch_add(1, Ordering::Relaxed).wrapping_mul(0x9e37_79b9);
        let p = dir.join(format!("{pattern}{}", n % 4_294_967_296));
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
        match opts.open(&p) {
            Ok(f) => return Ok((p, f)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(format!("open {}: {}", p.display(), e)),
        }
    }
    Err(format!("open {}: too many attempts", dir.join(pattern).display()))
}
