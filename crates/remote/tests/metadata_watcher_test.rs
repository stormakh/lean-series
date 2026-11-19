//! Port of `storage/remote/metadata_watcher_test.go`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use model::labels::Labels;
use model::labelset::LabelSet;
use model::metadata::MetricType;
use remote::metadata_watcher::{MetadataAppender, MetadataWatcher, ReadyScrapeManager, Watchable};
use remote::queue_manager::Ctx;
use scrape::target::{MetricMetadata, MetricMetadataStore, Target};

const INTERVAL: Duration = Duration::from_millis(1);
const DEADLINE: Duration = Duration::from_millis(1);

/// Go `TestMetaStore`: satisfies the MetricMetadataStore interface.
struct TestMetaStore {
    metadata: Vec<MetricMetadata>,
}

impl MetricMetadataStore for TestMetaStore {
    fn list_metadata(&self) -> Vec<MetricMetadata> {
        self.metadata.clone()
    }
    fn get_metadata(&self, mf_name: &str) -> Option<MetricMetadata> {
        self.metadata.iter().find(|m| m.metric_family == mf_name).cloned()
    }
    fn size_metadata(&self) -> usize {
        0
    }
    fn length_metadata(&self) -> usize {
        0
    }
}

#[derive(Default)]
struct WriteMetadataToMock {
    metadata_appended: AtomicUsize,
}

impl MetadataAppender for WriteMetadataToMock {
    fn append_watcher_metadata(&self, _ctx: &Ctx, m: &[MetricMetadata]) {
        self.metadata_appended.fetch_add(m.len(), Ordering::SeqCst);
    }
}

struct ScrapeManagerMock {
    manager: Option<Arc<dyn Watchable>>,
    ready: bool,
}

impl ReadyScrapeManager for ScrapeManagerMock {
    fn get(&self) -> Result<Arc<dyn Watchable>, String> {
        if self.ready
            && let Some(m) = &self.manager
        {
            return Ok(m.clone());
        }
        // adapted: Go returns a nil *scrape.Manager with a nil error when ready; the test then replaces it.
        if self.ready {
            return Ok(Arc::new(FakeManager { active_targets: HashMap::new() }));
        }
        Err("not ready".to_string())
    }
}

struct FakeManager {
    active_targets: HashMap<String, Vec<Arc<Target>>>,
}

impl Watchable for FakeManager {
    fn targets_active(&self) -> HashMap<String, Vec<Arc<Target>>> {
        self.active_targets.clone()
    }
}

fn new_target() -> Arc<Target> {
    Arc::new(Target::new(Labels::empty(), Arc::new(Default::default()), LabelSet::default(), LabelSet::default()))
}

#[test]
fn test_watch_scrape_manager_not_ready() {
    let wt = Arc::new(WriteMetadataToMock::default());
    let smm = Arc::new(ScrapeManagerMock { manager: None, ready: false });

    let mw = MetadataWatcher::new(None, Some(smm), "", wt.clone(), INTERVAL, DEADLINE);
    assert!(!mw.ready());

    mw.collect();

    assert_eq!(0, wt.metadata_appended.load(Ordering::SeqCst));
}

#[test]
fn test_watch_scrape_manager_ready_for_collection() {
    let wt = Arc::new(WriteMetadataToMock::default());

    let mm = |name: &str| MetricMetadata {
        metric_family: name.to_string(),
        typ: MetricType::Counter,
        help: "Total number".to_string(),
        unit: String::new(),
    };
    let metadata = Arc::new(TestMetaStore {
        metadata: vec![mm("prometheus_tsdb_head_chunks_created"), mm("prometheus_remote_storage_retried_samples")],
    });
    let metadata_dup = Arc::new(TestMetaStore { metadata: vec![mm("prometheus_tsdb_head_chunks_created")] });

    let target = new_target();
    target.set_metadata_store(metadata);
    let target_with_dup = new_target();
    target_with_dup.set_metadata_store(metadata_dup);

    let manager = Arc::new(FakeManager {
        active_targets: HashMap::from([("job".to_string(), vec![target]), ("dup".to_string(), vec![target_with_dup])]),
    });

    let smm = Arc::new(ScrapeManagerMock { manager: None, ready: true });

    let mw = MetadataWatcher::new(None, Some(smm), "", wt.clone(), INTERVAL, DEADLINE);
    mw.set_manager(manager);

    mw.collect();

    assert_eq!(2, wt.metadata_appended.load(Ordering::SeqCst));
}
