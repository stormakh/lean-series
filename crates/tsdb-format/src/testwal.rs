//! Port of `util/testwal`: generates WAL records for tests and benchmarks.
//!
//! The ledger lists its test under crate `util`, but the code lives here: it builds `record` types, and `util`
//! must not depend on the TSDB crates (they will depend on `util`).

use std::sync::Arc;

use model::histogram::{Histogram, Span};
use model::labels::{self, Label, Labels, ScratchBuilder};

use crate::chunks::HeadSeriesRef;
use crate::record::{
    MetricType, RefExemplar, RefFloatHistogramSample, RefHistogramSample, RefMetadata, RefSample, RefSeries,
};

/// Go `RecordsCase`: the record generation options.
///
/// Generated series have refs that are monotonic and deterministic, in `[ref_padding, ref_padding + series)`.
#[derive(Default)]
pub struct RecordsCase {
    pub name: String,

    pub series: usize,
    pub samples_per_series: usize,
    pub histograms_per_series: usize,
    pub float_histograms_per_series: usize,
    pub exemplars_per_series: usize,

    pub extra_labels: Vec<Label>,

    /// A padding to add to series refs.
    pub ref_padding: usize,
    /// Custom labels; by default `test_metric_<ref>` with the extra labels.
    pub labels_fn: Option<Box<dyn Fn(&mut ScratchBuilder, usize) -> Labels>>,
    /// Custom sample timestamps; `j` is the sample index within the series. By default `j`.
    pub ts_fn: Option<Box<dyn Fn(usize, usize) -> i64>>,
    /// The source histogram for histogram and float histogram records; by default an exponential histogram.
    pub histogram_fn: Option<Box<dyn Fn(usize) -> Histogram>>,
    /// If true, samples have no start timestamp (0).
    pub no_st: bool,
}

/// Go `Records`: batches of generated WAL records.
#[derive(Clone, Debug, Default)]
pub struct Records {
    pub series: Vec<RefSeries>,
    pub samples: Vec<RefSample>,
    pub histograms: Vec<RefHistogramSample>,
    pub float_histograms: Vec<RefFloatHistogramSample>,
    pub exemplars: Vec<RefExemplar>,
    pub metadata: Vec<RefMetadata>,
}

/// Go `newTestHist`.
fn new_test_hist(i: usize) -> Histogram {
    Histogram {
        schema: 2,
        zero_threshold: 1e-128,
        zero_count: 0,
        count: 2,
        sum: 0.0,
        positive_spans: vec![Span { offset: 0, length: 1 }],
        positive_buckets: vec![i as i64 + 1],
        negative_spans: vec![Span { offset: 0, length: 1 }],
        negative_buckets: vec![-(i as i64) - 1],
        ..Default::default()
    }
}

/// Go `GenerateRecords`: the records of a case, with the given number of samples, histograms and so on per
/// series ref.
pub fn generate_records(c: &RecordsCase) -> Records {
    let default_labels = |lb: &mut ScratchBuilder, i: usize| -> Labels {
        // Labels with the name of the series plus any extra labels supplied.
        lb.reset();
        lb.add(labels::METRIC_NAME, &format!("test_metric_{i}"));
        for l in &c.extra_labels {
            lb.add(&l.name, &l.value);
        }
        lb.sort();
        lb.labels()
    };
    let ts_fn = |r: usize, j: usize| -> i64 {
        match &c.ts_fn {
            Some(f) => f(r, j),
            None => j as i64,
        }
    };
    // Keep the start timestamp simple; the exact semantics are not tested.
    let st_for_ts = |ts: i64| if c.no_st { 0 } else { ts - 1 };

    let mut ret = Records {
        series: Vec::with_capacity(c.series),
        metadata: Vec::with_capacity(c.series),
        samples: Vec::with_capacity(c.series * c.samples_per_series),
        histograms: Vec::with_capacity(c.series * c.histograms_per_series),
        float_histograms: Vec::with_capacity(c.series * c.float_histograms_per_series),
        exemplars: Vec::with_capacity(c.series * c.exemplars_per_series),
    };
    let mut lb = ScratchBuilder::new(1 + c.extra_labels.len());
    for i in 0..c.series {
        let r = c.ref_padding + i;
        let hr = r as HeadSeriesRef;
        let lset = match &c.labels_fn {
            Some(f) => f(&mut lb, r),
            None => default_labels(&mut lb, r),
        };
        ret.series.push(RefSeries { r#ref: hr, labels: lset });
        ret.metadata.push(RefMetadata {
            r#ref: hr,
            r#type: MetricType::Counter as u8,
            unit: "unit text".to_string(),
            help: format!("help text for {r}"),
        });
        for j in 0..c.samples_per_series {
            let ts = ts_fn(r, j);
            ret.samples.push(RefSample { r#ref: hr, st: st_for_ts(ts), t: ts, v: r as f64 });
        }
        let h = Arc::new(match &c.histogram_fn {
            Some(f) => f(r),
            None => new_test_hist(r),
        });
        for j in 0..c.histograms_per_series {
            let ts = ts_fn(r, j);
            ret.histograms.push(RefHistogramSample { r#ref: hr, st: st_for_ts(ts), t: ts, h: h.clone() });
        }
        for j in 0..c.float_histograms_per_series {
            let ts = ts_fn(r, j);
            ret.float_histograms.push(RefFloatHistogramSample { r#ref: hr, st: st_for_ts(ts), t: ts, fh: Arc::new(h.to_float()) });
        }
        for j in 0..c.exemplars_per_series {
            ret.exemplars.push(RefExemplar {
                r#ref: hr,
                t: ts_fn(r, j),
                v: r as f64,
                labels: labels::from_strings(&["trace_id", &format!("trace-{r}")]),
            });
        }
    }
    ret
}
