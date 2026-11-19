//! Port of `storage/remote/storage_test.go`.

use std::path::PathBuf;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use config::{Config, GlobalConfig, RemoteReadConfig, RemoteWriteConfig, default_global_config, default_remote_read_config, default_remote_write_config};
use model::labels;
use remote::storage::Storage;
use remote::write::{WriteStorage, new_storage};
use yaml::Url;

const DEFAULT_FLUSH_DEADLINE: Duration = Duration::from_secs(60);

fn temp_dir(name: &str) -> PathBuf {
    let d = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../bench/tmp/w3-remote/qm")
        .join(format!("storage-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn base_remote_write_config(host: &str) -> RemoteWriteConfig {
    let mut cfg = default_remote_write_config();
    cfg.url = Url::parse(host).unwrap();
    cfg
}

fn base_remote_read_config(host: &str) -> RemoteReadConfig {
    let mut cfg = default_remote_read_config();
    cfg.url = Url::parse(host).unwrap();
    cfg
}

fn new_test_storage(dir: &PathBuf) -> Storage {
    new_storage(None, None, None, dir, DEFAULT_FLUSH_DEADLINE, None, false, None)
}

fn close(s: &Storage) {
    assert!(storage::Storage::close(s).is_none());
}

#[test]
fn test_storage_lifecycle() {
    let dir = temp_dir("lifecycle");
    // adapted: Go reads `s.rws.queues`; the write storage is made here and handed to the storage.
    let rws = WriteStorage::new(None, None, &dir, DEFAULT_FLUSH_DEADLINE, None, false, None);
    let s = Storage::with_write_storage(None, rws.clone(), remote::storage::default_read_client_factory());
    let conf = Config {
        global_config: default_global_config(),
        // We need to set URL's so that metric creation doesn't panic.
        remote_write_configs: vec![base_remote_write_config("http://test-storage.com")],
        remote_read_configs: vec![base_remote_read_config("http://test-storage.com")],
        ..Default::default()
    };

    s.apply_config(&conf).unwrap();

    // make sure remote write has a queue.
    assert_eq!(1, rws.queues_len());

    // make sure remote write has a queue.
    assert_eq!(1, s.queryables().len());

    close(&s);
}

#[test]
fn test_update_remote_read_configs() {
    let dir = temp_dir("update");
    let s = new_test_storage(&dir);

    let mut conf = Config { global_config: GlobalConfig::default(), ..Default::default() };
    s.apply_config(&conf).unwrap();
    assert!(s.queryables().is_empty());

    conf.remote_read_configs = vec![base_remote_read_config("http://test-storage.com")];
    s.apply_config(&conf).unwrap();
    assert_eq!(1, s.queryables().len());

    close(&s);
}

#[test]
fn test_filter_external_labels() {
    let dir = temp_dir("filter");
    let s = new_test_storage(&dir);

    let mut conf = Config {
        global_config: GlobalConfig { external_labels: labels::from_strings(&["foo", "bar"]), ..Default::default() },
        ..Default::default()
    };
    s.apply_config(&conf).unwrap();
    assert!(s.queryables().is_empty());

    conf.remote_read_configs = vec![base_remote_read_config("http://test-storage.com")];

    s.apply_config(&conf).unwrap();
    assert_eq!(1, s.queryables().len());
    assert_eq!(1, s.queryables()[0].external_labels.len());

    close(&s);
}

#[test]
fn test_ignore_external_labels() {
    let dir = temp_dir("ignore");
    let s = new_test_storage(&dir);

    let mut conf = Config {
        global_config: GlobalConfig { external_labels: labels::from_strings(&["foo", "bar"]), ..Default::default() },
        ..Default::default()
    };
    s.apply_config(&conf).unwrap();
    assert!(s.queryables().is_empty());

    conf.remote_read_configs = vec![base_remote_read_config("http://test-storage.com")];
    conf.remote_read_configs[0].filter_external_labels = false;

    s.apply_config(&conf).unwrap();
    assert_eq!(1, s.queryables().len());
    assert_eq!(0, s.queryables()[0].external_labels.len());

    close(&s);
}

/// TestWriteStorageApplyConfigsDuringCommit helps detecting races when
/// ApplyConfig runs concurrently with Notify
/// See https://github.com/prometheus/prometheus/issues/12747
#[test]
fn test_write_storage_apply_configs_during_commit() {
    let dir = temp_dir("during-commit");
    let s = Arc::new(new_test_storage(&dir));

    let start = Arc::new(Barrier::new(2000));
    let mut handles = Vec::with_capacity(2000);
    for i in 0..1000 {
        let (s, start) = (s.clone(), start.clone());
        handles.push(std::thread::spawn(move || {
            start.wait();
            let conf = Config {
                global_config: default_global_config(),
                remote_write_configs: vec![base_remote_write_config(&format!("http://test-{i}.com"))],
                ..Default::default()
            };
            s.apply_config(&conf).unwrap();
        }));
    }
    for _ in 0..1000 {
        let (s, start) = (s.clone(), start.clone());
        handles.push(std::thread::spawn(move || {
            start.wait();
            s.notify();
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    close(&s);
}
