//! Port of upstream/config/reload_test.go.

use std::path::{Path, PathBuf};

/// Go `t.TempDir()`: a new empty directory, removed when dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> TempDir {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let p =
            std::env::temp_dir().join(format!("config_reload_test_{}_{nanos}", std::process::id()));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn write(path: &Path, content: &str) {
    std::fs::write(path, content).unwrap();
}

/// Go `calculateChecksum`.
#[track_caller]
fn calculate_checksum(yaml_file_path: &Path) -> String {
    let checksum = config::generate_checksum(yaml_file_path.to_str().unwrap()).unwrap();
    assert!(!checksum.is_empty());
    checksum
}

#[test]
fn test_generate_checksum() {
    let tmp_dir = TempDir::new();

    // Define paths for the temporary files.
    let yaml_file_path = tmp_dir.0.join("test.yml");
    let rule_file = "rule_file.yml";
    let rule_file_path = tmp_dir.0.join(rule_file);
    let scrape_config_file = "scrape_config.yml";
    let scrape_config_file_path = tmp_dir.0.join(scrape_config_file);

    // Define initial and modified content for the files.
    let original_rule_content = "groups:\n- name: example\n  rules:\n  - alert: ExampleAlert";
    let modified_rule_content = "groups:\n- name: example\n  rules:\n  - alert: ModifiedAlert";

    let original_scrape_config_content = "scrape_configs:\n- job_name: example";
    let modified_scrape_config_content = "scrape_configs:\n- job_name: modified_example";

    let rule_file_abs = rule_file_path.to_str().unwrap().to_string();
    let scrape_config_file_abs = scrape_config_file_path.to_str().unwrap().to_string();
    let test_cases = [
        (
            "Auto reload using relative path.",
            rule_file.to_string(),
            scrape_config_file.to_string(),
        ),
        (
            "Auto reload using absolute path.",
            rule_file_abs,
            scrape_config_file_abs,
        ),
    ];

    for (name, tc_rule_file_path, tc_scrape_config_file_path) in &test_cases {
        eprintln!("case: {name}");
        // Define YAML content referencing the rule and scrape config files.
        let yaml_content = format!(
            "\nrule_files:\n  - {tc_rule_file_path}\nscrape_config_files:\n  - {tc_scrape_config_file_path}\n"
        );

        // Write initial content to files.
        write(&rule_file_path, original_rule_content);
        write(&scrape_config_file_path, original_scrape_config_content);
        write(&yaml_file_path, &yaml_content);

        // Generate the original checksum.
        let original_checksum = calculate_checksum(&yaml_file_path);

        // Rule File Change
        {
            // Modify the rule file.
            write(&rule_file_path, modified_rule_content);

            // Checksum should change.
            let modified_checksum = calculate_checksum(&yaml_file_path);
            assert_ne!(original_checksum, modified_checksum);

            // Revert the rule file.
            write(&rule_file_path, original_rule_content);

            // Checksum should return to the original.
            let reverted_checksum = calculate_checksum(&yaml_file_path);
            assert_eq!(original_checksum, reverted_checksum);
        }

        // Scrape Config Change
        {
            // Modify the scrape config file.
            write(&scrape_config_file_path, modified_scrape_config_content);

            // Checksum should change.
            let modified_checksum = calculate_checksum(&yaml_file_path);
            assert_ne!(original_checksum, modified_checksum);

            // Revert the scrape config file.
            write(&scrape_config_file_path, original_scrape_config_content);

            // Checksum should return to the original.
            let reverted_checksum = calculate_checksum(&yaml_file_path);
            assert_eq!(original_checksum, reverted_checksum);
        }

        // Rule File Deletion
        {
            // Delete the rule file.
            std::fs::remove_file(&rule_file_path).unwrap();

            // Checksum should change.
            let deleted_checksum = calculate_checksum(&yaml_file_path);
            assert_ne!(original_checksum, deleted_checksum);

            // Restore the rule file.
            write(&rule_file_path, original_rule_content);

            // Checksum should return to the original.
            let reverted_checksum = calculate_checksum(&yaml_file_path);
            assert_eq!(original_checksum, reverted_checksum);
        }

        // Scrape Config Deletion
        {
            // Delete the scrape config file.
            std::fs::remove_file(&scrape_config_file_path).unwrap();

            // Checksum should change.
            let deleted_checksum = calculate_checksum(&yaml_file_path);
            assert_ne!(original_checksum, deleted_checksum);

            // Restore the scrape config file.
            write(&scrape_config_file_path, original_scrape_config_content);

            // Checksum should return to the original.
            let reverted_checksum = calculate_checksum(&yaml_file_path);
            assert_eq!(original_checksum, reverted_checksum);
        }

        // Main File Change
        {
            // Modify the main YAML file.
            let modified_yaml_content = format!(
                "\nglobal:\n  scrape_interval: 3s\nrule_files:\n  - {tc_rule_file_path}\nscrape_config_files:\n  - {tc_scrape_config_file_path}\n"
            );
            write(&yaml_file_path, &modified_yaml_content);

            // Checksum should change.
            let modified_checksum = calculate_checksum(&yaml_file_path);
            assert_ne!(original_checksum, modified_checksum);

            // Revert the main YAML file.
            write(&yaml_file_path, &yaml_content);

            // Checksum should return to the original.
            let reverted_checksum = calculate_checksum(&yaml_file_path);
            assert_eq!(original_checksum, reverted_checksum);
        }

        // Rule File Removed from YAML Config
        {
            // Modify the YAML content to remove the rule file.
            let modified_yaml_content =
                format!("\nscrape_config_files:\n  - {tc_scrape_config_file_path}\n");
            write(&yaml_file_path, &modified_yaml_content);

            // Checksum should change.
            let modified_checksum = calculate_checksum(&yaml_file_path);
            assert_ne!(original_checksum, modified_checksum);

            // Revert the YAML content.
            write(&yaml_file_path, &yaml_content);

            // Checksum should return to the original.
            let reverted_checksum = calculate_checksum(&yaml_file_path);
            assert_eq!(original_checksum, reverted_checksum);
        }

        // Scrape Config Removed from YAML Config
        {
            // Modify the YAML content to remove the scrape config file.
            let modified_yaml_content = format!("\nrule_files:\n  - {tc_rule_file_path}\n");
            write(&yaml_file_path, &modified_yaml_content);

            // Checksum should change.
            let modified_checksum = calculate_checksum(&yaml_file_path);
            assert_ne!(original_checksum, modified_checksum);

            // Revert the YAML content.
            write(&yaml_file_path, &yaml_content);

            // Checksum should return to the original.
            let reverted_checksum = calculate_checksum(&yaml_file_path);
            assert_eq!(original_checksum, reverted_checksum);
        }

        // Empty Rule File
        {
            // Write an empty rule file.
            write(&rule_file_path, "");

            // Checksum should change.
            let empty_checksum = calculate_checksum(&yaml_file_path);
            assert_ne!(original_checksum, empty_checksum);

            // Restore the rule file.
            write(&rule_file_path, original_rule_content);

            // Checksum should return to the original.
            let reverted_checksum = calculate_checksum(&yaml_file_path);
            assert_eq!(original_checksum, reverted_checksum);
        }

        // Empty Scrape Config File
        {
            // Write an empty scrape config file.
            write(&scrape_config_file_path, "");

            // Checksum should change.
            let empty_checksum = calculate_checksum(&yaml_file_path);
            assert_ne!(original_checksum, empty_checksum);

            // Restore the scrape config file.
            write(&scrape_config_file_path, original_scrape_config_content);

            // Checksum should return to the original.
            let reverted_checksum = calculate_checksum(&yaml_file_path);
            assert_eq!(original_checksum, reverted_checksum);
        }
    }
}
