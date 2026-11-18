//! Port of `config/reload.go`: a checksum of the config file and the rule and scrape config files it names.

/// Go `ExternalFilesConfig`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExternalFilesConfig {
    pub rule_files: Vec<String>,
    pub scrape_config_files: Vec<String>,
}
yaml::yaml_struct!(ExternalFilesConfig, "config.ExternalFilesConfig", [
    (rule_files: "rule_files")
    (scrape_config_files: "scrape_config_files")
]);
yaml::impl_plain_struct!(ExternalFilesConfig);

/// Go `GenerateChecksum`: sha256 (hex) of the YAML file, then for rule files ("r") and scrape config files
/// ("s"): the prefix, NUL, the path, NUL, the content and NUL of each file matching the patterns.
pub fn generate_checksum(yaml_file_path: &str) -> Result<String, String> {
    let mut hash = crypto::Sha256::new();
    let content = std::fs::read(yaml_file_path).map_err(|e| format!("error reading YAML file: open {}: {}", yaml_file_path, net::types::go_io_error(&e)))?;
    hash.update(&content);
    let mut cfg: ExternalFilesConfig = yaml::unmarshal(&String::from_utf8_lossy(&content)).map_err(|e| format!("error unmarshalling YAML: {e}"))?;
    let dir = net::config::dir_of(yaml_file_path);
    for f in cfg.rule_files.iter_mut() {
        *f = net::config::join_dir(&dir, f);
    }
    for f in cfg.scrape_config_files.iter_mut() {
        *f = net::config::join_dir(&dir, f);
    }
    for (prefix, patterns) in [("r", &cfg.rule_files), ("s", &cfg.scrape_config_files)] {
        for pattern in patterns {
            let files = discovery::glob::glob(pattern).map_err(|e| format!("error finding files with pattern {}: {e}", yaml::go_quote(pattern)))?;
            for file in files {
                hash.update(format!("{prefix}\x00{file}\x00").as_bytes());
                let content = std::fs::read(&file).map_err(|e| format!("error reading file {file}: open {file}: {}", net::types::go_io_error(&e)))?;
                hash.update(&content);
                hash.update(b"\x00");
            }
        }
    }
    Ok(crypto::hex(&hash.finish()))
}
