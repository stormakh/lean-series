//! The V2 appender of the head (port of `tsdb/head_append_v2.go`).

use std::sync::atomic::Ordering;
use std::sync::Arc;

use model::exemplar::Exemplar;
use model::histogram::{CounterResetHint, FloatHistogram, Histogram};
use model::labels::Labels;
use model::metadata;
use model::value::is_stale_nan;
use storage::{
    AppendPartialError, AppendV2Options, AppenderTransaction, AppenderV2, Context, Error, ExemplarAppenderV2,
    ExemplarError, GetRef, SeriesRef, ERR_DUPLICATE_EXEMPLAR, ERR_EXEMPLARS_DISABLED, ERR_NOT_FOUND,
    ERR_OUT_OF_BOUNDS, ERR_OUT_OF_ORDER_EXEMPLAR, ERR_OUT_OF_ORDER_SAMPLE, ERR_ST_NEWER_THAN_SAMPLE,
    ERR_TOO_OLD_SAMPLE,
};
use tsdb_format::record::{self, RefFloatHistogramSample, RefHistogramSample, RefMetadata, RefSample};

use crate::head::{Head, MemSeries, SAMPLE_METRIC_TYPE_FLOAT, SAMPLE_METRIC_TYPE_HISTOGRAM};
use crate::head_append::{
    float_histogram_sample_type, histogram_sample_type, ExemplarWithSeriesRef, HeadAppenderBase, SampleType,
};

/// Go `headAppenderV2`.
pub struct HeadAppenderV2 {
    pub base: HeadAppenderBase,
}

impl HeadAppenderV2 {
    pub(crate) fn new(head: Arc<Head>) -> HeadAppenderV2 {
        HeadAppenderV2 {
            base: HeadAppenderBase::new(head),
        }
    }

    /// Go `appendFloat`: returns the series the sample was appended to, which may differ from `s` if `s` was
    /// garbage-collected in the meantime.
    fn append_float(&mut self, s: Arc<MemSeries>, st: i64, t: i64, v: f64, fast_reject_ooo: bool) -> Result<Arc<MemSeries>, Error> {
        let a = &mut self.base;
        let (head_maxt, min_valid_time, ooo_time_window) = (a.head_maxt, a.min_valid_time, a.ooo_time_window);
        let (s, (rejected, delta, err)) = a.lock_for_append(s, |_, g| {
            let (is_ooo, delta, err) = g.appendable(t, v, head_maxt, min_valid_time, ooo_time_window);
            if is_ooo && fast_reject_ooo {
                return (true, delta, err);
            }
            if err.is_none() {
                g.mark_pending_commit();
            }
            (false, delta, err)
        })?;
        if rejected {
            return Err(ERR_OUT_OF_ORDER_SAMPLE);
        }
        if delta > 0 {
            a.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
        }
        if let Some(err) = err {
            return Err(err);
        }
        let b = a.get_current_batch(SampleType::Float, s.r#ref);
        b.floats.push(RefSample {
            r#ref: s.r#ref,
            st,
            t,
            v,
        });
        b.float_series.push(s.clone());
        Ok(s)
    }

    /// Go `appendHistogram`.
    fn append_histogram(
        &mut self,
        s: Arc<MemSeries>,
        st: i64,
        t: i64,
        h: Arc<Histogram>,
        fast_reject_ooo: bool,
    ) -> Result<Arc<MemSeries>, Error> {
        let a = &mut self.base;
        let (head_maxt, min_valid_time, ooo_time_window) = (a.head_maxt, a.min_valid_time, a.ooo_time_window);
        let (s, (rejected, delta, err)) = a.lock_for_append(s, |_, g| {
            let (is_ooo, delta, err) = g.appendable_histogram(t, &h, head_maxt, min_valid_time, ooo_time_window);
            if is_ooo && fast_reject_ooo {
                return (true, delta, err);
            }
            if err.is_none() {
                g.mark_pending_commit();
            }
            (false, delta, err)
        })?;
        if rejected {
            return Err(ERR_OUT_OF_ORDER_SAMPLE);
        }
        if delta > 0 {
            a.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
        }
        if let Some(err) = err {
            return Err(err);
        }
        let b = a.get_current_batch(histogram_sample_type(&h), s.r#ref);
        b.histograms.push(RefHistogramSample {
            r#ref: s.r#ref,
            st,
            t,
            h,
        });
        b.histogram_series.push(s.clone());
        Ok(s)
    }

    /// Go `appendFloatHistogram`.
    fn append_float_histogram(
        &mut self,
        s: Arc<MemSeries>,
        st: i64,
        t: i64,
        fh: Arc<FloatHistogram>,
        fast_reject_ooo: bool,
    ) -> Result<Arc<MemSeries>, Error> {
        let a = &mut self.base;
        let (head_maxt, min_valid_time, ooo_time_window) = (a.head_maxt, a.min_valid_time, a.ooo_time_window);
        let (s, (rejected, delta, err)) = a.lock_for_append(s, |_, g| {
            let (is_ooo, delta, err) =
                g.appendable_float_histogram(t, &fh, head_maxt, min_valid_time, ooo_time_window);
            if is_ooo && fast_reject_ooo {
                return (true, delta, err);
            }
            if err.is_none() {
                g.mark_pending_commit();
            }
            (false, delta, err)
        })?;
        if rejected {
            return Err(ERR_OUT_OF_ORDER_SAMPLE);
        }
        if delta > 0 {
            a.head.metrics.ooo_histogram.observe(delta as f64 / 1000.0);
        }
        if let Some(err) = err {
            return Err(err);
        }
        let b = a.get_current_batch(float_histogram_sample_type(&fh), s.r#ref);
        b.float_histograms.push(RefFloatHistogramSample {
            r#ref: s.r#ref,
            st,
            t,
            fh,
        });
        b.float_histogram_series.push(s.clone());
        Ok(s)
    }

    /// Go `appendExemplars`: failed exemplars (except duplicates) give a partial error.
    fn append_exemplars_to(&mut self, s: &Arc<MemSeries>, exemplars: &[Exemplar]) -> Result<(), Error> {
        let mut errs: Vec<ExemplarError> = Vec::new();
        let es = self.base.head.exemplars();
        for (i, e) in exemplars.iter().enumerate() {
            // Ensure no empty labels have gotten through.
            let mut e = e.clone();
            e.labels = e.labels.without_empty();
            if let Err(err) = es.validate_exemplar(&s.lset, &e) {
                if !err.is(&ERR_DUPLICATE_EXEMPLAR) && !err.is(&ERR_EXEMPLARS_DISABLED) {
                    // Except duplicates, return partial errors.
                    errs.push(ExemplarError { index: i, err });
                    continue;
                }
                if !err.is(&ERR_OUT_OF_ORDER_EXEMPLAR) {
                    self.base.head.logger.debug(
                        "Error while adding an exemplar on AppendSample",
                        &[("exemplars", &format!("{:?}", e)), ("err", &err)],
                    );
                }
                continue;
            }
            let b = self.base.get_current_batch(SampleType::None, s.r#ref);
            // Protect the series from compaction before the exemplar is committed.
            s.update_exemplar_timestamp(e.ts);
            b.exemplars.push(ExemplarWithSeriesRef {
                r#ref: s.r#ref,
                exemplar: e,
            });
        }
        if !errs.is_empty() {
            return Err(Error::AppendPartial(Arc::new(AppendPartialError {
                series_ref: s.r#ref,
                exemplar_errors: errs,
            })));
        }
        Ok(())
    }

    /// Go `bestEffortAppendSTZeroSample`: errors are only logged. Returns the series the zero sample was
    /// appended to.
    #[allow(clippy::too_many_arguments)]
    fn best_effort_append_st_zero_sample(
        &mut self,
        s: Arc<MemSeries>,
        ls: &Labels,
        st: i64,
        t: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Arc<MemSeries> {
        let logger = self.base.head.logger.clone();
        if st >= t {
            logger.debug(
                "Error when appending ST",
                &[("series", ls), ("st", &st), ("t", &t), ("err", &ERR_ST_NEWER_THAN_SAMPLE)],
            );
            return s;
        }
        if st < self.base.min_valid_time {
            logger.debug(
                "Error when appending ST",
                &[("series", ls), ("st", &st), ("t", &t), ("err", &ERR_OUT_OF_BOUNDS)],
            );
            return s;
        }
        let res = if let Some(fh) = fh {
            let zero = FloatHistogram {
                // The STZeroSample represents a counter reset by definition.
                counter_reset_hint: CounterResetHint::CounterReset,
                // Replicate other fields to avoid needless chunk creation.
                schema: fh.schema,
                zero_threshold: fh.zero_threshold,
                custom_values: fh.custom_values.clone(),
                ..Default::default()
            };
            self.append_float_histogram(s.clone(), 0, st, Arc::new(zero), true)
        } else if let Some(h) = h {
            let zero = Histogram {
                counter_reset_hint: CounterResetHint::CounterReset,
                schema: h.schema,
                zero_threshold: h.zero_threshold,
                custom_values: h.custom_values.clone(),
                ..Default::default()
            };
            self.append_histogram(s.clone(), 0, st, Arc::new(zero), true)
        } else {
            self.append_float(s.clone(), 0, st, 0.0, true)
        };
        match res {
            Ok(appended) => appended,
            Err(err) => {
                if err.is(&ERR_OUT_OF_ORDER_SAMPLE) {
                    // OOO errors are common and expected (cumulative). Explicitly ignored.
                    return s;
                }
                logger.debug(
                    "Error when appending ST",
                    &[("series", &s.lset), ("st", &st), ("t", &t), ("err", &err)],
                );
                s
            }
        }
    }

    /// Go `GetRef`.
    pub fn get_ref(&self, lset: &Labels, hash: u64) -> (SeriesRef, Labels) {
        self.base.get_ref(lset, hash)
    }
}

impl AppenderTransaction for HeadAppenderV2 {
    fn commit(&mut self) -> Option<Error> {
        self.base.commit().err()
    }
    fn rollback(&mut self) -> Option<Error> {
        self.base.rollback().err()
    }
}

impl GetRef for HeadAppenderV2 {
    fn get_ref(&mut self, lset: &Labels, hash: u64) -> (SeriesRef, Labels) {
        self.base.get_ref(lset, hash)
    }
}

impl AppenderV2 for HeadAppenderV2 {
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
        let mut sample_metric_type = SAMPLE_METRIC_TYPE_FLOAT;
        // Fail fast on incorrect histograms.
        if let Some(fh) = fh {
            sample_metric_type = SAMPLE_METRIC_TYPE_HISTOGRAM;
            fh.validate()?;
        } else if let Some(h) = h {
            sample_metric_type = SAMPLE_METRIC_TYPE_HISTOGRAM;
            h.validate()?;
        }
        let head = self.base.head.clone();
        // Fail fast if OOO is disabled and the sample is out of bounds.
        if self.base.ooo_time_window == 0 && t < self.base.min_valid_time {
            head.metrics.out_of_bound_samples.with_label_values(sample_metric_type).inc();
            return Err(ERR_OUT_OF_BOUNDS);
        }
        let mut s = head.series.get_by_id(r);
        head.run_test_after_series_lookup(s.as_ref());
        if s.is_none() {
            s = Some(self.base.get_or_create(ls)?.0);
        }
        let mut s = s.expect("series");
        if head.opts.enable_st_as_zero_sample && st != 0 {
            s = self.best_effort_append_st_zero_sample(s, ls, st, t, h, fh);
        }
        let is_stale;
        let res = if let Some(fh) = fh {
            is_stale = is_stale_nan(fh.sum);
            self.append_float_histogram(s, st, t, Arc::new(fh.clone()), opts.reject_out_of_order)
        } else if let Some(h) = h {
            is_stale = is_stale_nan(h.sum);
            self.append_histogram(s, st, t, Arc::new(h.clone()), opts.reject_out_of_order)
        } else {
            is_stale = is_stale_nan(v);
            if is_stale {
                // If we have added a sample before with this same appender, turn a stale float sample into a
                // stale histogram sample or stale float histogram sample as appropriate.
                let o = AppendV2Options {
                    reject_out_of_order: opts.reject_out_of_order,
                    ..Default::default()
                };
                match self.base.types_in_batch.get(&s.r#ref).copied().unwrap_or_default() {
                    SampleType::Histogram | SampleType::CustomBucketHistogram => {
                        let h = Histogram {
                            sum: v,
                            ..Default::default()
                        };
                        return self.append(s.r#ref, ls, st, t, 0.0, Some(&h), None, &o);
                    }
                    SampleType::FloatHistogram | SampleType::CustomBucketFloatHistogram => {
                        let fh = FloatHistogram {
                            sum: v,
                            ..Default::default()
                        };
                        return self.append(s.r#ref, ls, st, t, 0.0, None, Some(&fh), &o);
                    }
                    _ => {}
                }
            }
            self.append_float(s, st, t, v, opts.reject_out_of_order)
        };
        // Handle append error, if any.
        let s = match res {
            Ok(s) => s,
            Err(err) => {
                if err.is(&ERR_OUT_OF_ORDER_SAMPLE) {
                    head.metrics.out_of_order_samples.with_label_values(sample_metric_type).inc();
                } else if err.is(&ERR_TOO_OLD_SAMPLE) {
                    head.metrics.too_old_samples.with_label_values(sample_metric_type).inc();
                }
                return Err(err);
            }
        };
        if is_stale {
            // For stale values we never attempt to process metadata/exemplars, claim the success.
            return Ok(s.r#ref);
        }
        // Append exemplars if any and if storage was configured for it.
        let mut partial = Ok(());
        if !opts.exemplars.is_empty()
            && head.opts.enable_exemplar_storage
            && head.opts.max_exemplars.load(Ordering::Relaxed) > 0
        {
            // Currently only exemplars can return partial errors.
            partial = self.append_exemplars_to(&s, opts.exemplars);
        }
        if head.opts.enable_metadata_wal_records
            && let Some(m) = opts.metadata
            && !m.is_empty()
        {
            let meta_changed = {
                let g = s.lock();
                match g.meta() {
                    Some(h) if !metadata::is_zero_handle(h) => !h.value().equals(m),
                    _ => true,
                }
            };
            if meta_changed {
                let b = self.base.get_current_batch(SampleType::None, s.r#ref);
                b.metadata.push(RefMetadata {
                    r#ref: s.r#ref,
                    r#type: record::get_metric_type(m.typ),
                    unit: m.unit.clone(),
                    help: m.help.clone(),
                });
                b.metadata_series.push(s.clone());
            }
        }
        partial.map(|_| s.r#ref)
    }

    fn as_exemplar_appender_v2(&mut self) -> Option<&mut dyn ExemplarAppenderV2> {
        Some(self)
    }

    fn type_name(&self) -> &'static str {
        "*tsdb.headAppenderV2"
    }
}

impl ExemplarAppenderV2 for HeadAppenderV2 {
    /// Go `AppendExemplars`: the series MUST already exist.
    fn append_exemplars(&mut self, r: SeriesRef, ls: &Labels, exemplars: &[Exemplar]) -> Result<SeriesRef, Error> {
        let head = self.base.head.clone();
        let mut s = head.series.get_by_id(r);
        if s.is_none() {
            let ls = ls.without_empty();
            s = head.series.get_by_hash(ls.hash(), &ls);
        }
        let Some(s) = s else {
            return Err(Error::wrap(
                format!("unknown HeadSeriesRef when trying to add exemplars: {}", r),
                ERR_NOT_FOUND,
            ));
        };
        if exemplars.is_empty()
            || !head.opts.enable_exemplar_storage
            || head.opts.max_exemplars.load(Ordering::Relaxed) <= 0
        {
            return Ok(s.r#ref);
        }
        self.append_exemplars_to(&s, exemplars).map(|_| s.r#ref)
    }
}

/// Go `initAppenderV2`: initializes the time bounds of the head upon the first sample it receives.
pub struct InitAppenderV2 {
    app: Option<HeadAppenderV2>,
    head: Arc<Head>,
}

impl AppenderTransaction for InitAppenderV2 {
    fn commit(&mut self) -> Option<Error> {
        match self.app.as_mut() {
            None => {
                self.head.metrics.active_appenders.dec();
                None
            }
            Some(a) => a.commit(),
        }
    }
    fn rollback(&mut self) -> Option<Error> {
        match self.app.as_mut() {
            None => {
                self.head.metrics.active_appenders.dec();
                None
            }
            Some(a) => a.rollback(),
        }
    }
}

impl GetRef for InitAppenderV2 {
    fn get_ref(&mut self, lset: &Labels, hash: u64) -> (SeriesRef, Labels) {
        match self.app.as_ref() {
            Some(a) => a.get_ref(lset, hash),
            None => (0, Labels::empty()),
        }
    }
}

impl AppenderV2 for InitAppenderV2 {
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
        if self.app.is_none() {
            self.head.init_time(t);
            self.app = Some(HeadAppenderV2::new(self.head.clone()));
        }
        self.app.as_mut().expect("appender").append(r, ls, st, t, v, h, fh, opts)
    }

    fn as_exemplar_appender_v2(&mut self) -> Option<&mut dyn ExemplarAppenderV2> {
        Some(self)
    }

    fn type_name(&self) -> &'static str {
        "*tsdb.initAppenderV2"
    }
}

impl ExemplarAppenderV2 for InitAppenderV2 {
    fn append_exemplars(&mut self, r: SeriesRef, ls: &Labels, exemplars: &[Exemplar]) -> Result<SeriesRef, Error> {
        if self.app.is_none() {
            if !self.head.initialized() {
                // AppendExemplars MUST be called after an Append for the same series, which would have already
                // initialized the head.
                return Err(Error::wrap(
                    "AppendExemplars called before any Append; no series exists yet",
                    ERR_NOT_FOUND,
                ));
            }
            self.app = Some(HeadAppenderV2::new(self.head.clone()));
        }
        self.app.as_mut().expect("appender").append_exemplars(r, ls, exemplars)
    }
}

impl Head {
    /// Go `AppenderV2`: a new V2 appender on the head.
    pub fn appender_v2(&self, _ctx: &Context) -> Box<dyn AppenderV2> {
        self.metrics.active_appenders.inc();
        self.metrics.appenders_created.inc();
        // The head cache might not have a starting point yet. The init appender picks up the first appended
        // timestamp as the base.
        if !self.initialized() {
            return Box::new(InitAppenderV2 {
                app: None,
                head: self.arc(),
            });
        }
        Box::new(self.head_appender_v2())
    }

    /// Go `appenderV2`: the head V2 appender itself (the head must be initialized).
    pub fn head_appender_v2(&self) -> HeadAppenderV2 {
        HeadAppenderV2::new(self.arc())
    }
}

impl storage::AppendableV2 for Head {
    fn appender_v2(&self, ctx: &Context) -> Box<dyn AppenderV2> {
        Head::appender_v2(self, ctx)
    }
}
