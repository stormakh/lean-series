//! Port of Prometheus `model/` packages: labels, matchers, the fast regex matcher (with a Go `regexp/syntax`
//! compatible parser), relabeling, exemplars, histograms, metadata, values and timestamps.

pub mod exemplar;
pub mod histogram;
pub mod labels;
pub mod matcher;
pub mod metadata;
pub mod regexp;
pub mod relabel;
pub mod schema;
pub mod timestamp;
pub mod unicode;
pub mod value;
