//! Port of `prompb`: remote write 1.0 / remote read messages (`types.proto`, `remote.proto`), remote write 2.0
//! (`io/prometheus/write/v2`), and their conversions to model types. Hand-written protobuf, byte-identical to
//! the gogo-generated Go code.

pub mod codec;
pub mod proto;
pub mod types;
pub mod writev2;

pub use codec::*;
pub use proto::DecodeError;
pub use types::*;
