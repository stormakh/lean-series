//! Port of `storage/remote/storage.go`: the remote storage. Reads fan out to every configured remote read
//! endpoint (merged with the chained series merge); writes go to the remote write storage (queue manager).
//!
//! The write side is behind [`RemoteWriteStorage`] (implemented by the queue manager's `WriteStorage`).
//! Not ported: the log deduper (`logging.Dedupe`, one minute): logs pass through unchanged.

use std::sync::{Arc, Mutex};

use config::{Config, RemoteReadConfig};
use model::labels::Labels;
use model::labelset::LabelSet;
use model::matcher::{MatchType, Matcher};
use storage::{
    chained_series_merge, new_compacting_chunk_series_merger, new_merge_chunk_querier, new_merge_querier, Appendable,
    AppendableV2, Appender, AppenderV2, BoxChunkQuerier, BoxQuerier, ChunkQueryable, Context, CostSamplingQueryable,
    Error, Queryable, SampleAndChunkQueryable,
};

use crate::client::{Client, ClientConfig, ReadClient};
use crate::read::{new_sample_and_chunk_queryable_client, StartTimeCallback};

/// The write side of the remote storage (Go `*WriteStorage` as `Storage` uses it).
pub trait RemoteWriteStorage: Appendable + AppendableV2 + Send + Sync {
    /// Go `ApplyConfig`.
    fn apply_config(&self, conf: &Config) -> Result<(), Error>;
    /// Go `Notify`: new WAL data is available.
    fn notify(&self);
    /// Go `LowestSentTimestamp`.
    fn lowest_sent_timestamp(&self) -> i64;
    /// Go `Close`.
    fn close(&self) -> Option<Error>;
}

/// Makes a read client for a remote read config (Go `NewReadClient`). Tests replace it.
pub type ReadClientFactory = Arc<dyn Fn(&str, &RemoteReadConfig) -> Result<Arc<dyn ReadClient>, Error> + Send + Sync>;

/// Go `NewReadClient(name, &ClientConfig{...})` from a remote read config.
pub fn default_read_client_factory() -> ReadClientFactory {
    Arc::new(|name: &str, rr: &RemoteReadConfig| {
        let c = Client::new_read_client(
            name,
            &ClientConfig {
                url: rr.url.to_url_string(),
                timeout: std::time::Duration::from_nanos(rr.remote_timeout.nanos().max(0) as u64),
                chunked_read_limit: rr.chunked_read_limit,
                http_client_config: rr.http_client_config.clone(),
                headers: rr.headers.clone(),
                ..Default::default()
            },
        )?;
        Ok(Arc::new(c) as Arc<dyn ReadClient>)
    })
}

/// A configured remote read endpoint: the queryable and, for tests, its external labels.
#[derive(Clone)]
pub struct RemoteQueryable {
    pub queryable: Arc<dyn SampleAndChunkQueryable>,
    /// The external labels the queryable filters (empty with `filter_external_labels: false`).
    pub external_labels: Labels,
}

/// Go `Storage`: all remote read and write endpoints; a `storage::Storage`.
pub struct Storage {
    rws: Arc<dyn RemoteWriteStorage>,
    read_client_factory: ReadClientFactory,
    // For reads.
    queryables: Mutex<Arc<Vec<RemoteQueryable>>>,
    local_start_time_callback: Option<StartTimeCallback>,
    /// Go `s.mtx`: serializes ApplyConfig and Close.
    mtx: Mutex<()>,
}

impl Storage {
    /// Go `NewStorage`, with the write storage made by the caller (the queue manager's `WriteStorage`).
    pub fn with_write_storage(
        st_callback: Option<StartTimeCallback>,
        rws: Arc<dyn RemoteWriteStorage>,
        read_client_factory: ReadClientFactory,
    ) -> Storage {
        Storage {
            rws,
            read_client_factory,
            queryables: Mutex::new(Arc::new(Vec::new())),
            local_start_time_callback: st_callback,
            mtx: Mutex::new(()),
        }
    }

    /// Go `Notify`.
    pub fn notify(&self) {
        self.rws.notify();
    }

    /// The write storage.
    pub fn write_storage(&self) -> &Arc<dyn RemoteWriteStorage> {
        &self.rws
    }

    /// The configured read endpoints (Go `s.queryables`, for tests).
    pub fn queryables(&self) -> Arc<Vec<RemoteQueryable>> {
        self.queryables.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// Sets the read endpoints directly (Go tests build `&Storage{queryables: ...}`).
    pub fn set_queryables(&self, q: Vec<RemoteQueryable>) {
        *self.queryables.lock().unwrap_or_else(|p| p.into_inner()) = Arc::new(q);
    }

    /// Go `ApplyConfig`.
    pub fn apply_config(&self, conf: &Config) -> Result<(), Error> {
        let _g = self.mtx.lock().unwrap_or_else(|p| p.into_inner());
        self.rws.apply_config(conf)?;

        // Update the read clients.
        let mut read_hashes = std::collections::HashSet::new();
        let mut queryables = Vec::with_capacity(conf.remote_read_configs.len());
        for rr_conf in &conf.remote_read_configs {
            let hash = to_hash(rr_conf)?;
            // Duplicate remote read configs are not allowed.
            if !read_hashes.insert(hash.clone()) {
                return Err(Error::msg(format!(
                    "duplicate remote read configs are not allowed, found duplicate for URL: {}",
                    rr_conf.url.to_url_string()
                )));
            }
            // Without a configured name, the config hash tells queues with the same endpoint apart.
            let name = if rr_conf.name.is_empty() { hash[..6].to_string() } else { rr_conf.name.clone() };
            let c = (self.read_client_factory)(&name, rr_conf)?;

            let external_labels =
                if rr_conf.filter_external_labels { conf.global_config.external_labels.clone() } else { Labels::empty() };
            let q = new_sample_and_chunk_queryable_client(
                c,
                external_labels.clone(),
                labels_to_equality_matchers(&rr_conf.required_matchers),
                rr_conf.read_recent,
                self.local_start_time_callback.clone(),
            );
            queryables.push(RemoteQueryable { queryable: Arc::new(q), external_labels });
        }
        *self.queryables.lock().unwrap_or_else(|p| p.into_inner()) = Arc::new(queryables);
        Ok(())
    }

    /// Go `LowestSentTimestamp`.
    pub fn lowest_sent_timestamp(&self) -> i64 {
        self.rws.lowest_sent_timestamp()
    }
}

impl Queryable for Storage {
    /// Go `Querier`: a merge querier over every remote read endpoint. Remote reads are best effort.
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        let queryables = self.queryables();
        let mut queriers = Vec::with_capacity(queryables.len());
        for q in queryables.iter() {
            queriers.push(q.queryable.querier(mint, maxt)?);
        }
        Ok(new_merge_querier(Vec::new(), queriers, Arc::new(chained_series_merge)))
    }
}

impl ChunkQueryable for Storage {
    /// Go `ChunkQuerier`.
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        let queryables = self.queryables();
        let mut queriers = Vec::with_capacity(queryables.len());
        for q in queryables.iter() {
            queriers.push(q.queryable.chunk_querier(mint, maxt)?);
        }
        Ok(new_merge_chunk_querier(
            Vec::new(),
            queriers,
            new_compacting_chunk_series_merger(Arc::new(chained_series_merge)),
        ))
    }
}

impl CostSamplingQueryable for Storage {
    /// Reports an empty source only when no remote readers are configured.
    fn cost_sampling_queryable(&self) -> (Option<&dyn SampleAndChunkQueryable>, bool) {
        (None, self.queryables().is_empty())
    }
}

impl Appendable for Storage {
    fn appender(&self, ctx: &Context) -> Box<dyn Appender> {
        self.rws.appender(ctx)
    }
}

impl AppendableV2 for Storage {
    fn appender_v2(&self, ctx: &Context) -> Box<dyn AppenderV2> {
        self.rws.appender_v2(ctx)
    }
}

impl storage::Storage for Storage {
    /// Go `StartTime`: `model.Latest`.
    fn start_time(&self) -> Result<i64, Error> {
        Ok(i64::MAX)
    }

    /// Go `Close`: stops the write queues.
    fn close(&self) -> Option<Error> {
        let _g = self.mtx.lock().unwrap_or_else(|p| p.into_inner());
        self.rws.close()
    }

    fn as_cost_sampling_queryable(&self) -> Option<&dyn CostSamplingQueryable> {
        Some(self)
    }
}

/// Go `labelsToEqualityMatchers`.
pub fn labels_to_equality_matchers(ls: &LabelSet) -> Vec<Matcher> {
    ls.iter().map(|(k, v)| Matcher::new(MatchType::Equal, k, v).expect("equality matcher")).collect()
}

/// Go `toHash`: the hex md5 of the YAML form of a config.
pub fn to_hash<T: yaml::Marshal + ?Sized>(data: &T) -> Result<String, Error> {
    let bytes = yaml::marshal(data).map_err(Error::msg)?;
    Ok(crypto::hex(&crypto::md5(bytes.as_bytes())))
}
