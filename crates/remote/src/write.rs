//! Port of `storage/remote/write.go`: `WriteStorage`, which runs one queue manager per remote write config,
//! and the timestamp tracking appenders it gives to the fanout storage.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use config::{Config, RemoteWriteConfig};
use discovery::log::Logger;
use discovery::metrics::{CounterVec, MetricVec, Opts, Registry};
use model::exemplar::Exemplar;
use model::histogram::{FloatHistogram, Histogram};
use model::labels::{self, Labels};
use model::metadata::Metadata;
use storage::{
    AppendOptions, AppendV2Options, Appendable, AppendableV2, Appender, AppenderTransaction, AppenderV2, Context,
    Error, ExemplarAppenderV2, SeriesRef,
};
use tsdb_format::record::BuffersPool;
use tsdb_format::wlog::{LiveReaderMetrics, WatcherMetrics};

use crate::client::{Client, ClientConfig, WriteClient};
use crate::ewma::EwmaRate;
use crate::intern::Pool;
use crate::max_timestamp::MaxTimestamp;
use crate::metadata_watcher::ReadyScrapeManager;
use crate::queue_manager::{EWMA_WEIGHT, NAMESPACE, ProtoMsg, QueueManager, QueueManagerMetrics, SHARD_UPDATE_DURATION, SUBSYSTEM, Signal};

fn global_counter(cell: &'static OnceLock<Arc<CounterVec>>, name: &str, help: &str) -> &'static Arc<CounterVec> {
    cell.get_or_init(|| MetricVec::new_counter(Opts::new(&format!("{NAMESPACE}_{SUBSYSTEM}_{name}"), help)))
}

/// Go `samplesIn` (`prometheus_remote_storage_samples_in_total`).
pub fn samples_in_total() -> &'static Arc<CounterVec> {
    static C: OnceLock<Arc<CounterVec>> = OnceLock::new();
    global_counter(&C, "samples_in_total", "Samples in to remote storage, compare to samples out for queue managers. Deprecated, check prometheus_wal_watcher_records_read_total and prometheus_remote_storage_samples_dropped_total")
}

/// Go `exemplarsIn`.
pub fn exemplars_in_total() -> &'static Arc<CounterVec> {
    static C: OnceLock<Arc<CounterVec>> = OnceLock::new();
    global_counter(&C, "exemplars_in_total", "Exemplars in to remote storage, compare to exemplars out for queue managers. Deprecated, check prometheus_wal_watcher_records_read_total and prometheus_remote_storage_exemplars_dropped_total")
}

/// Go `histogramsIn`.
pub fn histograms_in_total() -> &'static Arc<CounterVec> {
    static C: OnceLock<Arc<CounterVec>> = OnceLock::new();
    global_counter(&C, "histograms_in_total", "HistogramSamples in to remote storage, compare to histograms out for queue managers. Deprecated, check prometheus_wal_watcher_records_read_total and prometheus_remote_storage_histograms_dropped_total")
}

/// Registers the package-level metrics that Go registers with `promauto` (samples/exemplars/histograms in and
/// the interner's zero reference releases). The server calls this once with its registry.
pub fn register_global_metrics(reg: &Registry) -> Result<(), String> {
    reg.register(samples_in_total().clone())?;
    reg.register(exemplars_in_total().clone())?;
    reg.register(histograms_in_total().clone())?;
    reg.register(crate::intern::no_reference_releases().clone())?;
    Ok(())
}

/// Go `toHash`: the MD5 (hex) of the YAML of a remote write config.
pub fn to_hash(c: &RemoteWriteConfig) -> Result<String, String> {
    let bytes = yaml::marshal(c)?;
    Ok(crypto::hex(&crypto::md5(bytes.as_bytes())))
}

/// Makes the write client of a queue (Go `NewWriteClient` with a `ClientConfig` made from the remote write
/// config). Tests replace it.
pub type WriteClientFactory = Arc<dyn Fn(&str, &RemoteWriteConfig) -> Result<Arc<dyn WriteClient>, String> + Send + Sync>;

/// The real factory: `Client::new_write_client`.
pub fn default_write_client_factory() -> WriteClientFactory {
    Arc::new(|name: &str, rw: &RemoteWriteConfig| {
        let conf = ClientConfig {
            url: rw.url.to_url_string(),
            write_proto_msg: rw.protobuf_message.0.clone(),
            timeout: rw.remote_timeout.to_std(),
            http_client_config: rw.http_client_config.clone(),
            sigv4_config: rw.sigv4_config.clone(),
            azure_ad_config: rw.azure_ad_config.clone(),
            google_iam_config: rw.google_iam_config.clone(),
            headers: rw.headers.clone(),
            retry_on_rate_limit: rw.queue_config.retry_on_rate_limit,
            round_robin_dns: rw.round_robin_dns,
            ..Default::default()
        };
        Client::new_write_client(name, &conf).map(|c| Arc::new(c) as Arc<dyn WriteClient>).map_err(|e| e.to_string())
    })
}

struct State {
    external_labels: Labels,
    queues: HashMap<String, Arc<QueueManager>>,
}

/// Go `WriteStorage`: all the remote write storage.
pub struct WriteStorage {
    logger: Logger,
    reg: Option<Arc<Registry>>,
    mtx: Mutex<State>,

    watcher_metrics: Arc<WatcherMetrics>,
    live_reader_metrics: Arc<LiveReaderMetrics>,
    dir: PathBuf,
    pub samples_in: Arc<EwmaRate>,
    flush_deadline: Duration,
    interner: Arc<Pool>,
    scraper: Option<Arc<dyn ReadyScrapeManager>>,
    quit: Arc<Signal>,
    run: Mutex<Option<JoinHandle<()>>>,

    record_buf: Arc<BuffersPool>,

    /// For timestampTracker.
    pub highest_timestamp: Arc<MaxTimestamp>,
    enable_type_and_unit_labels: bool,
    client_factory: WriteClientFactory,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl WriteStorage {
    /// Go `NewWriteStorage`: creates and runs a `WriteStorage`. `client_factory` None uses
    /// `Client::new_write_client`.
    pub fn new(
        logger: Option<Logger>,
        reg: Option<Arc<Registry>>,
        dir: &Path,
        flush_deadline: Duration,
        sm: Option<Arc<dyn ReadyScrapeManager>>,
        enable_type_and_unit_labels: bool,
        client_factory: Option<WriteClientFactory>,
    ) -> Arc<WriteStorage> {
        let highest_timestamp = Arc::new(MaxTimestamp::new(Opts::new(
            &format!("{NAMESPACE}_{SUBSYSTEM}_highest_timestamp_in_seconds"),
            "Highest timestamp that has come into the remote storage via the Appender interface, in seconds since epoch. Initialized to 0 when no data has been received yet. Deprecated, check prometheus_remote_storage_queue_highest_timestamp_seconds which is more accurate.",
        )));
        if let Some(r) = &reg
            && let Err(e) = r.register(highest_timestamp.collector())
        {
            panic!("{e}");
        }
        let rws = Arc::new(WriteStorage {
            logger: logger.unwrap_or_default(),
            reg,
            mtx: Mutex::new(State { external_labels: Labels::empty(), queues: HashMap::new() }),
            watcher_metrics: WatcherMetrics::new(),
            live_reader_metrics: LiveReaderMetrics::new(),
            dir: dir.to_path_buf(),
            samples_in: Arc::new(EwmaRate::new(EWMA_WEIGHT, SHARD_UPDATE_DURATION)),
            flush_deadline,
            interner: Arc::new(Pool::new()),
            scraper: sm,
            quit: Arc::new(Signal::new()),
            run: Mutex::new(None),
            record_buf: Arc::new(BuffersPool::new()),
            highest_timestamp,
            enable_type_and_unit_labels,
            client_factory: client_factory.unwrap_or_else(default_write_client_factory),
        });
        let (samples_in, quit) = (rws.samples_in.clone(), rws.quit.clone());
        let h = std::thread::Builder::new()
            .name("rw-samples-in".into())
            .spawn(move || {
                let mut next = Instant::now() + SHARD_UPDATE_DURATION;
                loop {
                    let now = Instant::now();
                    if now < next && quit.wait_timeout(next - now) {
                        return;
                    }
                    if quit.is_set() {
                        return;
                    }
                    next += SHARD_UPDATE_DURATION;
                    samples_in.tick();
                }
            })
            .expect("spawn");
        *lock(&rws.run) = Some(h);
        rws
    }

    /// Go `Notify`: wakes the WAL watchers of all queues.
    pub fn notify(&self) {
        let st = lock(&self.mtx);
        for q in st.queues.values() {
            // These should all be non blocking
            q.watcher().notify();
        }
    }

    /// Go `ApplyConfig`: updates the state as the new config requires. Only stops and creates queues which
    /// have changes.
    pub fn apply_config(&self, conf: &Config) -> Result<(), String> {
        let mut st = lock(&self.mtx);

        // Remote write queues only need to change if the remote write config or
        // external labels change.
        let external_label_unchanged = labels::equal(&conf.global_config.external_labels, &st.external_labels);
        st.external_labels = conf.global_config.external_labels.clone();

        let mut new_queues: HashMap<String, Arc<QueueManager>> = HashMap::new();
        let mut new_hashes: Vec<String> = Vec::new();
        for rw_conf in &conf.remote_write_configs {
            let hash = to_hash(rw_conf)?;

            // Don't allow duplicate remote write configs.
            if new_queues.contains_key(&hash) {
                return Err(format!(
                    "duplicate remote write configs are not allowed, found duplicate for URL: {}",
                    rw_conf.url.to_url_string()
                ));
            }

            // Set the queue name to the config hash if the user has not set
            // a name in their remote write config so we can still differentiate
            // between queues that have the same remote write endpoint.
            let name = if rw_conf.name.is_empty() { hash[..6].to_string() } else { rw_conf.name.clone() };

            let c = (self.client_factory)(&name, rw_conf)?;

            if external_label_unchanged && let Some(queue) = st.queues.remove(&hash) {
                // Update the client in case any secret configuration has changed.
                queue.set_client(c);
                new_queues.insert(hash, queue);
                continue;
            }

            // Redacted to remove any passwords in the URL (that are
            // technically accepted but not recommended) since this is
            // only used for metric labels.
            let endpoint = rw_conf.url.redacted();
            let q = QueueManager::new(
                QueueManagerMetrics::new(self.reg.clone(), &name, &endpoint),
                Some(self.watcher_metrics.clone()),
                Some(self.live_reader_metrics.clone()),
                Some(self.logger.clone()),
                &self.dir,
                self.samples_in.clone(),
                rw_conf.queue_config.clone(),
                rw_conf.metadata_config.clone(),
                &conf.global_config.external_labels,
                rw_conf.write_relabel_configs.clone(),
                c,
                self.flush_deadline,
                self.interner.clone(),
                self.highest_timestamp.clone(),
                self.scraper.clone(),
                rw_conf.send_exemplars,
                rw_conf.send_native_histograms,
                self.enable_type_and_unit_labels,
                ProtoMsg::from_config(&rw_conf.protobuf_message.0),
                Some(self.record_buf.clone()),
                rw_conf.failed_request_logging,
                rw_conf.convert_nhcb_to_classic,
            );
            new_queues.insert(hash.clone(), q);
            // Keep track of which queues are new so we know which to start.
            new_hashes.push(hash);
        }

        // Anything remaining in rws.queues is a queue who's config has
        // changed or was removed from the overall remote write config.
        for q in st.queues.values() {
            q.stop();
        }

        for hash in &new_hashes {
            new_queues[hash].start();
        }

        st.queues = new_queues;
        Ok(())
    }

    /// The queue of a config hash (Go tests read `rws.queues[hash]`).
    pub fn queue(&self, hash: &str) -> Option<Arc<QueueManager>> {
        lock(&self.mtx).queues.get(hash).cloned()
    }

    /// The number of queues.
    pub fn queues_len(&self) -> usize {
        lock(&self.mtx).queues.len()
    }

    /// All queues.
    pub fn queues(&self) -> Vec<Arc<QueueManager>> {
        lock(&self.mtx).queues.values().cloned().collect()
    }

    /// Go `LowestSentTimestamp`: the lowest sent timestamp across all queues.
    pub fn lowest_sent_timestamp(&self) -> i64 {
        let st = lock(&self.mtx);
        if st.queues.is_empty() {
            return 0;
        }
        let mut lowest_ts = i64::MAX;
        for q in st.queues.values() {
            let ts = (q.metrics.highest_sent_timestamp.get() * 1000.0) as i64;
            if ts < lowest_ts {
                lowest_ts = ts;
            }
        }
        lowest_ts
    }

    /// Go `Close`.
    pub fn close(&self) -> Result<(), String> {
        let st = lock(&self.mtx);
        for q in st.queues.values() {
            q.stop();
        }
        self.quit.set();
        if let Some(h) = lock(&self.run).take() {
            let _ = h.join();
        }
        // TODO(metrics): Go also unregisters the watcher and live reader metrics (not registered here yet).
        if let Some(r) = &self.reg {
            r.unregister(&self.highest_timestamp.collector());
        }
        Ok(())
    }

    fn tracker(&self) -> BaseTimestampTracker {
        BaseTimestampTracker {
            samples_in: self.samples_in.clone(),
            highest_recv_timestamp: self.highest_timestamp.clone(),
            samples: 0,
            exemplars: 0,
            histograms: 0,
            highest_timestamp: 0,
        }
    }
}

impl Appendable for WriteStorage {
    /// Go `Appender`.
    fn appender(&self, _ctx: &Context) -> Box<dyn Appender> {
        Box::new(TimestampTracker { base: self.tracker() })
    }
}

impl AppendableV2 for WriteStorage {
    /// Go `AppenderV2`.
    fn appender_v2(&self, _ctx: &Context) -> Box<dyn AppenderV2> {
        Box::new(TimestampTrackerV2 { base: self.tracker() })
    }
}

/// Go `baseTimestampTracker`.
struct BaseTimestampTracker {
    samples_in: Arc<EwmaRate>,
    highest_recv_timestamp: Arc<MaxTimestamp>,
    samples: i64,
    exemplars: i64,
    histograms: i64,
    highest_timestamp: i64,
}

impl BaseTimestampTracker {
    /// Go `Commit`.
    fn commit(&mut self) -> Option<Error> {
        self.samples_in.incr(self.samples + self.exemplars + self.histograms);
        samples_in_total().get().add(self.samples as f64);
        exemplars_in_total().get().add(self.exemplars as f64);
        histograms_in_total().get().add(self.histograms as f64);
        self.highest_recv_timestamp.set((self.highest_timestamp / 1000) as f64);
        None
    }
}

/// Go `timestampTracker`.
struct TimestampTracker {
    base: BaseTimestampTracker,
}

impl AppenderTransaction for TimestampTracker {
    fn commit(&mut self) -> Option<Error> {
        self.base.commit()
    }
    fn rollback(&mut self) -> Option<Error> {
        None
    }
}

impl Appender for TimestampTracker {
    fn append(&mut self, _r: SeriesRef, _l: &Labels, t: i64, _v: f64) -> Result<SeriesRef, Error> {
        self.base.samples += 1;
        if t > self.base.highest_timestamp {
            self.base.highest_timestamp = t;
        }
        Ok(0)
    }
    fn set_options(&mut self, _opts: Option<&AppendOptions>) {}
    fn append_exemplar(&mut self, _r: SeriesRef, _l: &Labels, _e: &Exemplar) -> Result<SeriesRef, Error> {
        // highestTimestamp excludes exemplars, as not all queues send them.
        self.base.exemplars += 1;
        Ok(0)
    }
    fn append_histogram(
        &mut self,
        _r: SeriesRef,
        _l: &Labels,
        t: i64,
        _h: Option<&Histogram>,
        _fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.base.histograms += 1;
        if t > self.base.highest_timestamp {
            self.base.highest_timestamp = t;
        }
        Ok(0)
    }
    fn append_histogram_st_zero_sample(
        &mut self,
        _r: SeriesRef,
        _l: &Labels,
        _t: i64,
        st: i64,
        _h: Option<&Histogram>,
        _fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.base.histograms += 1;
        if st > self.base.highest_timestamp {
            // Theoretically, we should never see a ST zero sample with a timestamp higher than the highest timestamp we've seen so far.
            // However, we're not going to enforce that here, as it is not the responsibility of the tracker to enforce this.
            self.base.highest_timestamp = st;
        }
        Ok(0)
    }
    fn update_metadata(&mut self, _r: SeriesRef, _l: &Labels, _m: &Metadata) -> Result<SeriesRef, Error> {
        // Intentionally a no-op. dataIn measures records that create work for the
        // sharded queue and is used to estimate queue load and backlog. A metadata
        // update only changes cached per-series state; it does not enqueue an item.
        Ok(0)
    }
    fn append_st_zero_sample(&mut self, _r: SeriesRef, _l: &Labels, _t: i64, st: i64) -> Result<SeriesRef, Error> {
        self.base.samples += 1;
        if st > self.base.highest_timestamp {
            self.base.highest_timestamp = st;
        }
        Ok(0)
    }
}

/// Go `timestampTrackerV2`.
struct TimestampTrackerV2 {
    base: BaseTimestampTracker,
}

impl AppenderTransaction for TimestampTrackerV2 {
    fn commit(&mut self) -> Option<Error> {
        self.base.commit()
    }
    fn rollback(&mut self) -> Option<Error> {
        None
    }
}

impl AppenderV2 for TimestampTrackerV2 {
    fn append(
        &mut self,
        r: SeriesRef,
        _ls: &Labels,
        _st: i64,
        t: i64,
        _v: f64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
        opts: &AppendV2Options<'_>,
    ) -> Result<SeriesRef, Error> {
        if fh.is_some() || h.is_some() {
            self.base.histograms += 1;
        } else {
            self.base.samples += 1;
        }
        if t > self.base.highest_timestamp {
            self.base.highest_timestamp = t;
        }
        self.base.exemplars += opts.exemplars.len() as i64;
        Ok(r)
    }

    fn as_exemplar_appender_v2(&mut self) -> Option<&mut dyn ExemplarAppenderV2> {
        Some(self)
    }
}

impl ExemplarAppenderV2 for TimestampTrackerV2 {
    fn append_exemplars(&mut self, r: SeriesRef, _ls: &Labels, exemplars: &[Exemplar]) -> Result<SeriesRef, Error> {
        // highestTimestamp excludes exemplars, as not all queues send them.
        self.base.exemplars += exemplars.len() as i64;
        Ok(r)
    }
}
