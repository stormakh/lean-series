//! Port of `tsdb/agent/db_append_v2.go`: the V2 appender of the agent.

use std::sync::Arc;

use model::exemplar::Exemplar;
use model::histogram::{CounterResetHint, FloatHistogram, Histogram};
use model::labels::Labels;
use model::metadata;
use model::value::is_stale_nan;
use storage::{
    AppendPartialError, AppendV2Options, AppenderTransaction, ERR_DUPLICATE_EXEMPLAR, ERR_NOT_FOUND,
    ERR_OUT_OF_ORDER_EXEMPLAR, ERR_OUT_OF_ORDER_SAMPLE, ERR_OUT_OF_ORDER_ST, ERR_ST_NEWER_THAN_SAMPLE, Error,
    ExemplarAppenderV2, ExemplarError, SeriesRef,
};
use tsdb_format::record::{self, RefExemplar, RefFloatHistogramSample, RefHistogramSample, RefMetadata, RefSample};

use crate::db::AppenderBase;
use crate::series::MemSeries;

/// Go `appenderV2`: implements `storage::AppenderV2`.
pub struct AppenderV2 {
    pub base: AppenderBase,
}

impl AppenderTransaction for AppenderV2 {
    fn commit(&mut self) -> Option<Error> {
        self.base.commit().err()
    }
    fn rollback(&mut self) -> Option<Error> {
        self.base.rollback().err()
    }
}

impl storage::AppenderV2 for AppenderV2 {
    fn append(
        &mut self,
        r: SeriesRef,
        ls: &Labels,
        st: i64,
        t: i64,
        v: f64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
        opts: &AppendV2Options<'_>,
    ) -> Result<SeriesRef, Error> {
        // Fail fast on incorrect histograms.
        let mut histogram_type = false;
        if let Some(fh) = fh {
            histogram_type = true;
            fh.validate()?;
        } else if let Some(h) = h {
            histogram_type = true;
            h.validate()?;
        }

        // Series references and chunk references are identical for agent mode.
        let s = self.base.get_or_create(r, ls)?;

        let last_ts = s.lock().last_ts;

        if self.base.db.opts.enable_st_as_zero_sample && st != 0 {
            self.best_effort_append_st_zero_sample(&s, ls, last_ts, st, t, h, fh);
        }

        let db = self.base.db.clone();
        if t <= self.base.min_valid_time(last_ts) {
            db.metrics.total_out_of_order_samples.inc();
            return Err(ERR_OUT_OF_ORDER_SAMPLE);
        }

        let sref = s.r#ref;
        let is_stale;
        if let Some(fh) = fh {
            is_stale = is_stale_nan(fh.sum);
            self.base.pending_float_histograms.push(RefFloatHistogramSample {
                r#ref: sref,
                st,
                t,
                fh: Arc::new(fh.clone()),
            });
            self.base.float_histogram_series.push(s.clone());
        } else if let Some(h) = h {
            is_stale = is_stale_nan(h.sum);
            self.base.pending_histograms.push(RefHistogramSample {
                r#ref: sref,
                st,
                t,
                h: Arc::new(h.clone()),
            });
            self.base.histogram_series.push(s.clone());
        } else {
            is_stale = is_stale_nan(v);
            self.base.pending_samples.push(RefSample { r#ref: sref, st, t, v });
            self.base.sample_series.push(s.clone());
        }
        if histogram_type {
            db.metrics.total_appended_samples.histogram.inc();
        } else {
            db.metrics.total_appended_samples.float.inc();
        }
        if is_stale {
            // For stale values we never attempt to process metadata/exemplars, claim the success.
            return Ok(sref);
        }

        // Append exemplars if any. (Upstream TODO: the agent has no equivalent of the head's exemplar storage
        // switch.)
        let mut partial = Ok(());
        if !opts.exemplars.is_empty() {
            // Currently only exemplars can return partial errors.
            partial = self.append_exemplars_to(&s, opts.exemplars);
        }
        if db.opts.enable_metadata_wal_records
            && let Some(m) = opts.metadata
            && !m.is_empty()
        {
            let meta_changed = {
                let g = s.lock();
                metadata::is_zero_handle(&g.meta) || !g.meta.value().equals(m)
            };
            if meta_changed {
                self.base.pending_metadata.push(RefMetadata {
                    r#ref: sref,
                    r#type: record::get_metric_type(m.typ),
                    unit: m.unit.clone(),
                    help: m.help.clone(),
                });
                self.base.metadata_series.push(s);
            }
        }
        partial.map(|_| sref)
    }

    fn as_exemplar_appender_v2(&mut self) -> Option<&mut dyn ExemplarAppenderV2> {
        Some(self)
    }

    fn type_name(&self) -> &'static str {
        "*agent.appenderV2"
    }
}

impl ExemplarAppenderV2 for AppenderV2 {
    /// Go `AppendExemplars`: the series identified by `r` and/or `ls` MUST already exist.
    fn append_exemplars(&mut self, r: SeriesRef, ls: &Labels, exemplars: &[Exemplar]) -> Result<SeriesRef, Error> {
        // Series references and chunk references are identical for agent mode.
        let mut s = self.base.db.series.get_by_id(r);
        if s.is_none() {
            let ls = ls.without_empty();
            s = self.base.db.series.get_by_hash(ls.hash(), &ls);
        }
        let Some(s) = s else {
            return Err(Error::wrap(
                format!("unknown series ref when trying to add exemplars: {r}"),
                ERR_NOT_FOUND,
            ));
        };
        if exemplars.is_empty() {
            return Ok(s.r#ref);
        }
        self.append_exemplars_to(&s, exemplars).map(|_| s.r#ref)
    }
}

impl AppenderV2 {
    /// Go `appendExemplars`.
    fn append_exemplars_to(&mut self, s: &Arc<MemSeries>, exemplars: &[Exemplar]) -> Result<(), Error> {
        let mut errs: Vec<ExemplarError> = Vec::new();
        for (i, e) in exemplars.iter().enumerate() {
            // Ensure no empty labels have gotten through.
            let mut e = e.clone();
            e.labels = e.labels.without_empty();

            if let Err(err) = self.base.validate_exemplar(s.r#ref, &e) {
                if !err.is(&ERR_DUPLICATE_EXEMPLAR) {
                    // Except duplicates, return partial errors.
                    errs.push(ExemplarError { index: i, err });
                    continue;
                }
                if !err.is(&ERR_OUT_OF_ORDER_EXEMPLAR) {
                    // Go logs the exemplar as the "err" value too.
                    self.base.db.logger.debug(
                        "Error while adding an exemplar on AppendSample",
                        &[("exemplars", &format!("{e:?}")), ("err", &format!("{e:?}"))],
                    );
                }
                continue;
            }

            self.base.db.series.set_latest_exemplar(s.r#ref, &e);
            self.base.pending_exemplars.push(RefExemplar {
                r#ref: s.r#ref,
                t: e.ts,
                v: e.value,
                labels: e.labels,
            });
            self.base.db.metrics.total_appended_exemplars.inc();
        }
        if !errs.is_empty() {
            return Err(Error::AppendPartial(Arc::new(AppendPartialError {
                series_ref: s.r#ref,
                exemplar_errors: errs,
            })));
        }
        Ok(())
    }

    /// Go `bestEffortAppendSTZeroSample`: an experimental feature; errors only give a debug log.
    #[allow(clippy::too_many_arguments)]
    fn best_effort_append_st_zero_sample(
        &mut self,
        s: &Arc<MemSeries>,
        ls: &Labels,
        last_ts: i64,
        st: i64,
        t: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) {
        let logger = &self.base.db.logger;
        if st >= t {
            logger.debug(
                "Error when appending ST",
                &[("series", ls), ("st", &st), ("t", &t), ("err", &ERR_ST_NEWER_THAN_SAMPLE)],
            );
            return;
        }
        if st <= last_ts {
            logger.debug(
                "Error when appending ST",
                &[("series", ls), ("st", &st), ("t", &t), ("err", &ERR_OUT_OF_ORDER_ST)],
            );
            return;
        }

        let metrics = &self.base.db.metrics;
        if let Some(fh) = fh {
            let zero = FloatHistogram {
                // The ST zero sample represents a counter reset by definition.
                counter_reset_hint: CounterResetHint::CounterReset,
                // Replicate other fields to avoid needless chunk creation.
                schema: fh.schema,
                zero_threshold: fh.zero_threshold,
                custom_values: fh.custom_values.clone(),
                ..Default::default()
            };
            self.base.pending_float_histograms.push(RefFloatHistogramSample {
                r#ref: s.r#ref,
                st: 0,
                t: st,
                fh: Arc::new(zero),
            });
            self.base.float_histogram_series.push(s.clone());
            metrics.total_appended_samples.histogram.inc();
        } else if let Some(h) = h {
            let zero = Histogram {
                counter_reset_hint: CounterResetHint::CounterReset,
                schema: h.schema,
                zero_threshold: h.zero_threshold,
                custom_values: h.custom_values.clone(),
                ..Default::default()
            };
            self.base.pending_histograms.push(RefHistogramSample {
                r#ref: s.r#ref,
                st: 0,
                t: st,
                h: Arc::new(zero),
            });
            self.base.histogram_series.push(s.clone());
            metrics.total_appended_samples.histogram.inc();
        } else {
            self.base.pending_samples.push(RefSample {
                r#ref: s.r#ref,
                st: 0,
                t: st,
                v: 0.0,
            });
            self.base.sample_series.push(s.clone());
            metrics.total_appended_samples.float.inc();
        }
    }
}
