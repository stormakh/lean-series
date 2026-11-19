//! Port of Prometheus `prompb` and `storage/remote`: remote write 1.0 and 2.0 (client, queue manager, WAL
//! watcher integration, write handler), remote read (client, handler, codecs), the OTLP write handler and
//! translator, and Azure AD / SigV4 authentication for remote write.

pub mod azuread;
pub mod chunked;
pub mod client;
pub mod codec;
pub mod dial_context;
pub mod ewma;
pub mod intern;
pub mod max_timestamp;
pub mod metadata_watcher;
pub mod prompb;
pub mod queue_manager;
pub mod read;
pub mod read_handler;
pub mod remoteapi;
pub mod stats;
pub mod storage;
pub mod write;
pub mod write_handler;
