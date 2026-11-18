//! Port of `storage/fanout.go`: a storage that sends reads and writes to a primary and secondary storages.

use std::sync::Arc;

use model::exemplar::Exemplar;
use model::histogram::{FloatHistogram, Histogram};
use model::labels::Labels;
use model::metadata::Metadata;

use crate::context::Context;
use crate::error::{AppendPartialError, Error};
use crate::interface::*;
use crate::merge::{
    chained_series_merge, new_compacting_chunk_series_merger, new_merge_chunk_querier,
    new_merge_querier,
};

/// Logs an error (Go `*slog.Logger` `Error(msg, "err", err)`).
pub type Logger = Arc<dyn Fn(&str, &Error) + Send + Sync>;

/// Go `fanout`.
pub struct Fanout {
    logger: Option<Logger>,
    primary: Arc<dyn Storage>,
    secondaries: Vec<Arc<dyn Storage>>,
}

/// Go `NewFanout`: a storage that proxies reads and writes to several storages.
///
/// Reads: if the primary querier fails, every querier operation fails; if a secondary fails, its result is
/// dropped and its error becomes a warning. Prometheus treats all remote storages as secondary.
pub fn new_fanout(
    logger: Option<Logger>,
    primary: Arc<dyn Storage>,
    secondaries: Vec<Arc<dyn Storage>>,
) -> Fanout {
    Fanout {
        logger,
        primary,
        secondaries,
    }
}

fn log_error(logger: &Option<Logger>, msg: &str, err: &Error) {
    if let Some(l) = logger {
        l(msg, err);
    }
}

impl Queryable for Fanout {
    fn querier(&self, mint: i64, maxt: i64) -> Result<BoxQuerier, Error> {
        let mut primary = self.primary.querier(mint, maxt)?;
        let mut secondaries: Vec<BoxQuerier> = Vec::with_capacity(self.secondaries.len());
        for storage in &self.secondaries {
            match storage.querier(mint, maxt) {
                Ok(q) => {
                    if !q.is_noop() {
                        secondaries.push(q);
                    }
                }
                Err(err) => {
                    // Close the open queriers and add their errors.
                    let mut errs = vec![Some(err), primary.close()];
                    for q in &mut secondaries {
                        errs.push(q.close());
                    }
                    return Err(Error::join(errs).expect("an error"));
                }
            }
        }
        Ok(new_merge_querier(
            vec![primary],
            secondaries,
            Arc::new(chained_series_merge),
        ))
    }
}

impl ChunkQueryable for Fanout {
    fn chunk_querier(&self, mint: i64, maxt: i64) -> Result<BoxChunkQuerier, Error> {
        let mut primary = self.primary.chunk_querier(mint, maxt)?;
        let mut secondaries: Vec<BoxChunkQuerier> = Vec::with_capacity(self.secondaries.len());
        for storage in &self.secondaries {
            match storage.chunk_querier(mint, maxt) {
                Ok(q) => secondaries.push(q),
                Err(err) => {
                    let mut errs = vec![Some(err), primary.close()];
                    for q in &mut secondaries {
                        errs.push(q.close());
                    }
                    return Err(Error::join(errs).expect("an error"));
                }
            }
        }
        Ok(new_merge_chunk_querier(
            vec![primary],
            secondaries,
            new_compacting_chunk_series_merger(Arc::new(chained_series_merge)),
        ))
    }
}

impl CostSamplingQueryable for Fanout {
    /// Forwards a safe primary when every secondary is empty.
    fn cost_sampling_queryable(&self) -> (Option<&dyn SampleAndChunkQueryable>, bool) {
        let Some(provider) = self.primary.as_cost_sampling_queryable() else {
            return (None, false);
        };
        let (source, supported) = provider.cost_sampling_queryable();
        if !supported {
            return (None, false);
        }
        for secondary in &self.secondaries {
            let Some(provider) = secondary.as_cost_sampling_queryable() else {
                return (None, false);
            };
            let (secondary_source, supported) = provider.cost_sampling_queryable();
            if !supported || secondary_source.is_some() {
                // Merging even native sources can eagerly compact overlapping chunks.
                return (None, false);
            }
        }
        (source, true)
    }
}

impl Appendable for Fanout {
    fn appender(&self, ctx: &Context) -> Box<dyn Appender> {
        Box::new(FanoutAppender {
            logger: self.logger.clone(),
            primary: self.primary.appender(ctx),
            secondaries: self.secondaries.iter().map(|s| s.appender(ctx)).collect(),
        })
    }
}

impl AppendableV2 for Fanout {
    fn appender_v2(&self, ctx: &Context) -> Box<dyn AppenderV2> {
        Box::new(FanoutAppenderV2 {
            logger: self.logger.clone(),
            primary: self.primary.appender_v2(ctx),
            secondaries: self
                .secondaries
                .iter()
                .map(|s| s.appender_v2(ctx))
                .collect(),
        })
    }
}

impl Storage for Fanout {
    /// The earliest start time of all storages.
    fn start_time(&self) -> Result<i64, Error> {
        let mut first_time = self.primary.start_time()?;
        for s in &self.secondaries {
            let t = s.start_time()?;
            if t < first_time {
                first_time = t;
            }
        }
        Ok(first_time)
    }

    fn close(&self) -> Option<Error> {
        let mut errs = vec![self.primary.close()];
        for s in &self.secondaries {
            errs.push(s.close());
        }
        Error::join(errs)
    }

    fn as_cost_sampling_queryable(&self) -> Option<&dyn CostSamplingQueryable> {
        Some(self)
    }
}

/// Go `fanoutAppender`.
pub struct FanoutAppender {
    logger: Option<Logger>,
    primary: Box<dyn Appender>,
    secondaries: Vec<Box<dyn Appender>>,
}

impl FanoutAppender {
    /// Calls `f` on the primary with `r`, then on the secondaries with the primary's reference.
    fn each<F>(&mut self, r: SeriesRef, mut f: F) -> Result<SeriesRef, Error>
    where
        F: FnMut(&mut dyn Appender, SeriesRef) -> Result<SeriesRef, Error>,
    {
        let r = f(&mut *self.primary, r)?;
        for app in &mut self.secondaries {
            f(&mut **app, r)?;
        }
        Ok(r)
    }
}

impl AppenderTransaction for FanoutAppender {
    fn commit(&mut self) -> Option<Error> {
        let mut err = self.primary.commit();
        for app in &mut self.secondaries {
            if err.is_none() {
                err = app.commit();
            } else if let Some(rerr) = app.rollback() {
                log_error(&self.logger, "Squashed rollback error on commit", &rerr);
            }
        }
        err
    }

    fn rollback(&mut self) -> Option<Error> {
        let mut err = self.primary.rollback();
        for app in &mut self.secondaries {
            let rerr = app.rollback();
            match (&err, rerr) {
                (None, r) => err = r,
                (Some(_), Some(r)) => {
                    log_error(&self.logger, "Squashed rollback error on rollback", &r)
                }
                (Some(_), None) => {}
            }
        }
        err
    }
}

impl Appender for FanoutAppender {
    fn append(&mut self, r: SeriesRef, l: &Labels, t: i64, v: f64) -> Result<SeriesRef, Error> {
        self.each(r, |app, r| app.append(r, l, t, v))
    }

    /// Propagates the options to the primary and the secondaries.
    fn set_options(&mut self, opts: Option<&AppendOptions>) {
        self.primary.set_options(opts);
        for app in &mut self.secondaries {
            app.set_options(opts);
        }
    }

    fn append_exemplar(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        e: &Exemplar,
    ) -> Result<SeriesRef, Error> {
        self.each(r, |app, r| app.append_exemplar(r, l, e))
    }

    fn append_histogram(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.each(r, |app, r| app.append_histogram(r, l, t, h, fh))
    }

    fn append_histogram_st_zero_sample(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        st: i64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
    ) -> Result<SeriesRef, Error> {
        self.each(r, |app, r| {
            app.append_histogram_st_zero_sample(r, l, t, st, h, fh)
        })
    }

    fn update_metadata(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        m: &Metadata,
    ) -> Result<SeriesRef, Error> {
        self.each(r, |app, r| app.update_metadata(r, l, m))
    }

    fn append_st_zero_sample(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        t: i64,
        st: i64,
    ) -> Result<SeriesRef, Error> {
        self.each(r, |app, r| app.append_st_zero_sample(r, l, t, st))
    }
}

/// Go `fanoutAppenderV2`.
pub struct FanoutAppenderV2 {
    logger: Option<Logger>,
    primary: Box<dyn AppenderV2>,
    secondaries: Vec<Box<dyn AppenderV2>>,
}

/// The series reference of a successful or partially failed append.
fn ref_of(res: &Result<SeriesRef, Error>) -> SeriesRef {
    match res {
        Ok(r) => *r,
        Err(e) => e.as_append_partial().map_or(0, |p| p.series_ref),
    }
}

impl AppenderTransaction for FanoutAppenderV2 {
    fn commit(&mut self) -> Option<Error> {
        let mut err = self.primary.commit();
        for app in &mut self.secondaries {
            if err.is_none() {
                err = app.commit();
            } else if let Some(rerr) = app.rollback() {
                log_error(&self.logger, "Squashed rollback error on commit", &rerr);
            }
        }
        err
    }

    fn rollback(&mut self) -> Option<Error> {
        let mut err = self.primary.rollback();
        for app in &mut self.secondaries {
            let rerr = app.rollback();
            match (&err, rerr) {
                (None, r) => err = r,
                (Some(_), Some(r)) => {
                    log_error(&self.logger, "Squashed rollback error on rollback", &r)
                }
                (Some(_), None) => {}
            }
        }
        err
    }
}

impl AppenderV2 for FanoutAppenderV2 {
    fn append(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        st: i64,
        t: i64,
        v: f64,
        h: Option<&Histogram>,
        fh: Option<&FloatHistogram>,
        opts: &AppendV2Options<'_>,
    ) -> Result<SeriesRef, Error> {
        let mut partial: Option<AppendPartialError> = None;
        let res = self.primary.append(r, l, st, t, v, h, fh, opts);
        let r = ref_of(&res);
        if let Some(err) = AppendPartialError::handle(&mut partial, res.err()) {
            return Err(err);
        }
        for app in &mut self.secondaries {
            let sres = app.append(r, l, st, t, v, h, fh, opts);
            if let Some(serr) = AppendPartialError::handle(&mut partial, sres.err()) {
                return Err(serr);
            }
        }
        finish_partial(r, partial)
    }

    fn as_exemplar_appender_v2(&mut self) -> Option<&mut dyn ExemplarAppenderV2> {
        Some(self)
    }
}

fn finish_partial(r: SeriesRef, partial: Option<AppendPartialError>) -> Result<SeriesRef, Error> {
    match partial {
        Some(mut p) if !p.exemplar_errors.is_empty() => {
            p.series_ref = r;
            Err(AppendPartialError::to_error(Some(&p)).expect("exemplar errors"))
        }
        _ => Ok(r),
    }
}

impl ExemplarAppenderV2 for FanoutAppenderV2 {
    /// Sends the exemplars to the primary, which must support them, and to the secondaries that do.
    fn append_exemplars(
        &mut self,
        r: SeriesRef,
        l: &Labels,
        exemplars: &[Exemplar],
    ) -> Result<SeriesRef, Error> {
        let type_name = self.primary.type_name();
        let Some(pa) = self.primary.as_exemplar_appender_v2() else {
            return Err(Error::msg(format!(
                "primary appender {} does not implement ExemplarAppenderV2",
                type_name
            )));
        };
        let mut partial: Option<AppendPartialError> = None;
        let res = pa.append_exemplars(r, l, exemplars);
        let r = ref_of(&res);
        if let Some(err) = AppendPartialError::handle(&mut partial, res.err()) {
            return Err(err);
        }
        for app in &mut self.secondaries {
            let Some(sa) = app.as_exemplar_appender_v2() else {
                continue;
            };
            let sres = sa.append_exemplars(r, l, exemplars);
            if let Some(serr) = AppendPartialError::handle(&mut partial, sres.err()) {
                return Err(serr);
            }
        }
        finish_partial(r, partial)
    }
}
