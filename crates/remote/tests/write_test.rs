//! Port of `storage/remote/write_test.go`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use config::{
    Config, GlobalConfig, RemoteWriteConfig, WRITE_V1_MESSAGE_TYPE, WriteMessageType, default_global_config,
    default_queue_config, default_remote_write_config,
};
use discovery::metrics::Registry;
use model::exemplar::Exemplar;
use model::labels::{self, Label};
use model::relabel::{self, Regexp};
use remote::write::{WriteStorage, exemplars_in_total, to_hash};
use storage::{AppendV2Options, AppendableV2, Context};
use yaml::{Duration as YDuration, Secret, Url};

const DEFAULT_FLUSH_DEADLINE: Duration = Duration::from_secs(60);

fn temp_dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("rw-write-test-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn localhost() -> Url {
    Url::parse("http://localhost").unwrap()
}

/// Go `baseRemoteWriteConfig` (storage_test.go).
fn base_remote_write_config(host: &str) -> RemoteWriteConfig {
    let mut cfg = default_remote_write_config();
    cfg.url = Url::parse(host).unwrap();
    cfg
}

fn test_remote_write_config() -> RemoteWriteConfig {
    RemoteWriteConfig {
        name: "dev".to_string(),
        url: localhost(),
        queue_config: default_queue_config(),
        protobuf_message: WriteMessageType(WRITE_V1_MESSAGE_TYPE.to_string()),
        ..Default::default()
    }
}

fn new_ws(dir: &PathBuf, reg: Option<Arc<Registry>>, flush_deadline: Duration) -> Arc<WriteStorage> {
    WriteStorage::new(None, reg, dir, flush_deadline, None, false, None)
}

#[test]
fn test_write_storage_apply_config_no_duplicate_write_configs() {
    let dir = temp_dir("nodup");

    let cfg1 = RemoteWriteConfig { name: "write-1".to_string(), ..test_remote_write_config() };
    let cfg2 = RemoteWriteConfig { name: "write-2".to_string(), ..test_remote_write_config() };
    let cfg3 = RemoteWriteConfig { name: String::new(), ..test_remote_write_config() };

    let dup = "duplicate remote write configs are not allowed, found duplicate for URL: http://localhost";
    for (cfgs, expected_err) in [
        // Two duplicates, we should get an error.
        (vec![cfg1.clone(), cfg1.clone()], Some(dup)),
        // Duplicates but with different names, we should not get an error.
        (vec![cfg1.clone(), cfg2.clone()], None),
        // Duplicates but one with no name, we should not get an error.
        (vec![cfg1.clone(), cfg3.clone()], None),
        // Duplicates both with no name, we should get an error.
        (vec![cfg3.clone(), cfg3.clone()], Some(dup)),
    ] {
        let s = new_ws(&dir, None, Duration::from_millis(1));
        let conf = Config { global_config: default_global_config(), remote_write_configs: cfgs, ..Default::default() };
        let err = s.apply_config(&conf);
        match expected_err {
            None => assert!(err.is_ok(), "{err:?}"),
            Some(e) => assert_eq!(Err(e.to_string()), err),
        }
        assert!(s.close().is_ok());
    }
}

#[test]
fn test_write_storage_apply_config_restart_on_name_change() {
    let dir = temp_dir("restart");
    let cfg = test_remote_write_config();
    let hash = to_hash(&cfg).unwrap();

    let s = new_ws(&dir, None, Duration::from_millis(1));
    let mut conf =
        Config { global_config: default_global_config(), remote_write_configs: vec![cfg.clone()], ..Default::default() };
    s.apply_config(&conf).unwrap();
    assert_eq!(s.queue(&hash).unwrap().client().name(), cfg.name);

    // Change the queues name, ensure the queue has been restarted.
    conf.remote_write_configs[0].name = "dev-2".to_string();
    s.apply_config(&conf).unwrap();
    let hash = to_hash(&conf.remote_write_configs[0]).unwrap();
    assert_eq!(s.queue(&hash).unwrap().client().name(), conf.remote_write_configs[0].name);

    assert!(s.close().is_ok());
}

#[test]
fn test_write_storage_apply_config_update_with_registerer() {
    let dir = temp_dir("registerer");
    let s = new_ws(&dir, Some(Registry::new()), Duration::from_millis(1));
    let c1 = RemoteWriteConfig { name: "named".to_string(), ..test_remote_write_config() };
    let c2 = RemoteWriteConfig { name: String::new(), ..test_remote_write_config() };
    let mut conf = Config { global_config: default_global_config(), remote_write_configs: vec![c1, c2], ..Default::default() };
    s.apply_config(&conf).unwrap();

    conf.remote_write_configs[0].queue_config.max_shards = 10;
    conf.remote_write_configs[1].queue_config.max_shards = 10;
    s.apply_config(&conf).unwrap();
    for queue in s.queues() {
        assert_eq!(10, queue.cfg.max_shards);
    }

    assert!(s.close().is_ok());
}

#[test]
fn test_write_storage_apply_config_lifecycle() {
    let dir = temp_dir("lifecycle");
    let s = new_ws(&dir, None, DEFAULT_FLUSH_DEADLINE);
    let conf = Config {
        global_config: default_global_config(),
        remote_write_configs: vec![base_remote_write_config("http://test-storage.com")],
        ..Default::default()
    };
    s.apply_config(&conf).unwrap();
    assert_eq!(1, s.queues_len());

    assert!(s.close().is_ok());
}

#[test]
fn test_write_storage_apply_config_update_external_labels() {
    let dir = temp_dir("extlabels");
    let s = new_ws(&dir, Some(Registry::new()), Duration::from_secs(1));

    let external_labels = labels::from_strings(&["external", "true"]);
    let mut conf = Config {
        global_config: GlobalConfig::default(),
        remote_write_configs: vec![test_remote_write_config()],
        ..Default::default()
    };
    let hash = to_hash(&conf.remote_write_configs[0]).unwrap();
    s.apply_config(&conf).unwrap();
    assert_eq!(1, s.queues_len());
    assert!(s.queue(&hash).unwrap().external_labels.is_empty());

    conf.global_config.external_labels = external_labels;
    let hash = to_hash(&conf.remote_write_configs[0]).unwrap();
    s.apply_config(&conf).unwrap();
    assert_eq!(1, s.queues_len());
    assert_eq!(vec![Label::new("external", "true")], s.queue(&hash).unwrap().external_labels);

    assert!(s.close().is_ok());
}

#[test]
fn test_write_storage_apply_config_idempotent() {
    let dir = temp_dir("idempotent");
    let s = new_ws(&dir, None, DEFAULT_FLUSH_DEADLINE);
    let conf = Config {
        global_config: GlobalConfig::default(),
        remote_write_configs: vec![base_remote_write_config("http://test-storage.com")],
        ..Default::default()
    };
    let hash = to_hash(&conf.remote_write_configs[0]).unwrap();

    s.apply_config(&conf).unwrap();
    assert_eq!(1, s.queues_len());

    s.apply_config(&conf).unwrap();
    assert_eq!(1, s.queues_len());
    assert!(s.queue(&hash).is_some(), "Queue pointer should have remained the same");

    assert!(s.close().is_ok());
}

fn relabel_cfg(re: &str) -> relabel::Config {
    relabel::Config {
        regex: Regexp::must_new(re),
        name_validation_scheme: labels::ValidationScheme::Utf8,
        ..Default::default()
    }
}

#[test]
fn test_write_storage_apply_config_partial_update() {
    let dir = temp_dir("partial");
    let s = new_ws(&dir, None, DEFAULT_FLUSH_DEADLINE);

    let v1 = WriteMessageType(WRITE_V1_MESSAGE_TYPE.to_string());
    let mut c0 = RemoteWriteConfig {
        remote_timeout: YDuration::from_secs(10),
        queue_config: default_queue_config(),
        write_relabel_configs: vec![relabel_cfg(".+")],
        protobuf_message: v1.clone(),
        ..Default::default()
    };
    let mut c1 = RemoteWriteConfig {
        remote_timeout: YDuration::from_secs(20),
        queue_config: default_queue_config(),
        protobuf_message: v1.clone(),
        ..Default::default()
    };
    c1.http_client_config.bearer_token = Secret("foo".to_string());
    let mut c2 = RemoteWriteConfig {
        remote_timeout: YDuration::from_secs(30),
        queue_config: default_queue_config(),
        protobuf_message: v1.clone(),
        ..Default::default()
    };
    // We need to set URL's so that metric creation doesn't panic.
    for c in [&mut c0, &mut c1, &mut c2] {
        c.url = Url::parse("http://test-storage.com").unwrap();
    }

    let mut conf = Config {
        global_config: GlobalConfig::default(),
        remote_write_configs: vec![c0.clone(), c1.clone(), c2.clone()],
        ..Default::default()
    };
    s.apply_config(&conf).unwrap();
    assert_eq!(3, s.queues_len());

    let store_hashes = |conf: &Config| {
        let hashes: Vec<String> = conf.remote_write_configs.iter().map(|c| to_hash(c).unwrap()).collect();
        let queues: Vec<_> = hashes.iter().map(|h| s.queue(h)).collect();
        (hashes, queues)
    };

    let (hashes, queues) = store_hashes(&conf);
    // Update c0 and c2.
    c0.write_relabel_configs[0] = relabel_cfg("foo");
    c2.remote_timeout = YDuration::from_secs(50);
    conf = Config {
        global_config: GlobalConfig::default(),
        remote_write_configs: vec![c0.clone(), c1.clone(), c2.clone()],
        ..Default::default()
    };
    s.apply_config(&conf).unwrap();
    assert_eq!(3, s.queues_len());

    assert!(
        s.queue(&hashes[0]).is_none(),
        "The queue for the first remote write configuration should have been restarted because the relabel configuration has changed."
    );
    let q = s.queue(&hashes[1]);
    assert!(q.is_some(), "Hash of unchanged queue should have remained the same");
    assert!(Arc::ptr_eq(q.as_ref().unwrap(), queues[1].as_ref().unwrap()), "Pointer of unchanged queue should have remained the same");
    assert!(
        s.queue(&hashes[2]).is_none(),
        "The queue for the third remote write configuration should have been restarted because the timeout has changed."
    );

    let (hashes, _) = store_hashes(&conf);
    let second_client = s.queue(&hashes[1]).unwrap().client();
    // Update c1.
    c1.http_client_config.bearer_token = Secret("bar".to_string());
    conf.remote_write_configs[1] = c1.clone();
    s.apply_config(&conf).unwrap();
    assert_eq!(3, s.queues_len());

    assert!(s.queue(&hashes[0]).is_some(), "Pointer of unchanged queue should have remained the same");
    let q = s.queue(&hashes[1]);
    assert!(q.is_some(), "Hash of queue with secret change should have remained the same");
    assert!(
        !Arc::ptr_eq(&second_client, &q.unwrap().client()),
        "Pointer of a client with a secret change should not be the same"
    );
    assert!(s.queue(&hashes[2]).is_some(), "Pointer of unchanged queue should have remained the same");

    let (hashes, _) = store_hashes(&conf);
    // Delete c0.
    conf = Config {
        global_config: GlobalConfig::default(),
        remote_write_configs: vec![c1.clone(), c2.clone()],
        ..Default::default()
    };
    s.apply_config(&conf).unwrap();
    assert_eq!(2, s.queues_len());

    assert!(s.queue(&hashes[0]).is_none(), "If a config is removed, the queue should be stopped and recreated.");
    assert!(s.queue(&hashes[1]).is_some(), "Pointer of unchanged queue should have remained the same");
    assert!(s.queue(&hashes[2]).is_some(), "Pointer of unchanged queue should have remained the same");

    assert!(s.close().is_ok());
}

#[test]
fn test_write_storage_can_register_metrics_after_closing() {
    let dir = temp_dir("register");
    let reg = Registry::new();

    let s = new_ws(&dir, Some(reg.clone()), Duration::from_millis(1));
    assert!(s.close().is_ok());
    // Go `require.NotPanics`.
    let r = std::panic::catch_unwind(|| new_ws(&dir, Some(reg.clone()), Duration::from_millis(1)));
    assert!(r.is_ok());
    let _ = r.unwrap().close();
}

#[test]
fn test_write_storage_appender_v2_append_exemplars() {
    let dir = temp_dir("appenderv2");
    let reg = Registry::new();

    let s = new_ws(&dir, Some(reg), Duration::from_millis(1));

    let mut app = s.appender_v2(&Context::background());

    let before = exemplars_in_total().get().get();

    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let future = now + 3_600_000;
    let lbls = labels::from_strings(&["__name__", "test_metric"]);
    let ex = |id: &str, v: f64| Exemplar { labels: labels::from_strings(&["trace_id", id]), value: v, ts: future, has_ts: true };
    let opts_ex = [ex("1", 1.0)];
    let r = app
        .append(0, &lbls, 0, now, 1.0, None, None, &AppendV2Options { exemplars: &opts_ex, ..Default::default() })
        .unwrap();

    {
        let eapp = app.as_exemplar_appender_v2();
        assert!(eapp.is_some());
        eapp.unwrap().append_exemplars(r, &lbls, &[ex("2", 2.0), ex("3", 3.0)]).unwrap();
    }
    assert!(app.commit().is_none());

    // adapted: exemplars_in_total is process-wide (as in Go) and no other test of this binary appends exemplars.
    assert_eq!(before + 3.0, exemplars_in_total().get().get());
    assert_eq!((now / 1000) as f64, s.highest_timestamp.get());
    assert!(s.close().is_ok());
}
