//! Port of Prometheus `prompb` and `storage/remote`: remote write 1.0 and 2.0 (client, queue manager, WAL
//! watcher integration, write handler), remote read (client, handler, codecs), the OTLP write handler and
//! translator, and Azure AD / SigV4 authentication for remote write.

pub mod client;
pub mod prompb;
pub mod stats;
pub mod otlptranslator;
pub mod write_otlp_handler;
