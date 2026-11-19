//! Port of `discovery/refresh`: a discoverer that calls a refresh function at start and then at every interval,
//! and sends the groups it returns.
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::cancel::CancelToken;
use crate::chan::Sender;
use crate::log::Logger;
use crate::targetgroup::Group;
use crate::{Discoverer, RefreshMetrics, RefreshMetricsInstantiator};

/// The refresh function of a mechanism (Go `RefreshF`). It should give up early when `ctx` is cancelled.
pub trait Refresher: Send {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String>;
}

impl<F: FnMut(&CancelToken) -> Result<Vec<Arc<Group>>, String> + Send> Refresher for F {
    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        self(ctx)
    }
}

/// Go `refresh.Options`.
pub struct Options<'a, R: Refresher> {
    pub logger: Logger,
    pub mech: &'a str,
    pub set_name: &'a str,
    pub interval: Duration,
    pub refresh_f: R,
    pub metrics_instantiator: &'a dyn RefreshMetricsInstantiator,
}

/// Go `refresh.Discovery`.
pub struct Discovery<R: Refresher> {
    logger: Logger,
    interval: Duration,
    refresh_f: R,
    metrics: RefreshMetrics,
}

impl<R: Refresher> Discovery<R> {
    /// Go `NewDiscovery`.
    pub fn new(opts: Options<'_, R>) -> Discovery<R> {
        let metrics = opts.metrics_instantiator.instantiate(opts.mech, opts.set_name);
        Discovery { logger: opts.logger, interval: opts.interval, refresh_f: opts.refresh_f, metrics }
    }

    /// The refresh function (for mechanisms that expose their `Refresh`).
    pub fn refresher(&mut self) -> &mut R {
        &mut self.refresh_f
    }

    fn refresh(&mut self, ctx: &CancelToken) -> Result<Vec<Arc<Group>>, String> {
        let now = Instant::now();
        let r = self.refresh_f.refresh(ctx);
        if r.is_err() {
            self.metrics.failures.inc();
        }
        let secs = now.elapsed().as_secs_f64();
        self.metrics.duration.observe(secs);
        self.metrics.duration_histogram.observe(secs);
        r
    }

    fn refresh_and_send(&mut self, ctx: &CancelToken, ch: &Sender) -> bool {
        match self.refresh(ctx) {
            Err(e) => {
                if !ctx.is_cancelled() {
                    self.logger.error("Unable to refresh target groups", &[("err", &e)]);
                }
                true
            }
            Ok(tgs) => ch.send(tgs),
        }
    }
}

impl<R: Refresher> Discoverer for Discovery<R> {
    fn run(&mut self, ctx: &CancelToken, ch: Sender) {
        // Get an initial set right away.
        if !self.refresh_and_send(ctx, &ch) {
            return;
        }
        // A Go ticker: fires every interval from now; ticks missed while refreshing are dropped.
        let interval = self.interval.max(Duration::from_nanos(1));
        let mut next = Instant::now() + interval;
        loop {
            let now = Instant::now();
            if next > now && ctx.wait_timeout(next - now) {
                return;
            }
            if ctx.is_cancelled() {
                return;
            }
            let now = Instant::now();
            while next <= now {
                next += interval;
            }
            if !self.refresh_and_send(ctx, &ch) {
                return;
            }
        }
    }
}
