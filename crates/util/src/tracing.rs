//! Port of the `tracing` package: the manager that (re)installs the tracer provider when the `tracing` section of
//! the config changes.
//!
//! OTLP export is out of scope (PORTING.md), so an installed provider records spans nowhere. What is kept is the
//! manager's behavior: when it reinstalls, when it uninstalls (no endpoint), the TLS check on install, the log
//! lines, and stop. The manager is generic over the config type so that this crate does not depend on `config`;
//! the server passes `config::TracingConfig`.
use crate::logging::Logger;
use std::sync::{Condvar, Mutex};

/// Go `serviceName`.
pub const SERVICE_NAME: &str = "prometheus";

/// The tracer provider in use (Go `otel.GetTracerProvider()`), compared by identity as the Go tests do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TracerProvider {
    /// The provider before any config was applied (Go's global delegating provider).
    #[default]
    Global,
    /// Go `noop.NewTracerProvider()`: tracing disabled. All no-op providers are equal.
    Noop,
    /// A provider built from a config; each install gets a new id.
    Sdk(u64),
}

/// Go `Manager`.
pub struct Manager<C> {
    logger: Logger,
    config: C,
    provider: TracerProvider,
    installed: bool,
    next_id: u64,
    done: Mutex<bool>,
    cv: Condvar,
}

impl<C: Clone + PartialEq + Default> Manager<C> {
    /// Go `NewManager`.
    pub fn new(logger: Logger) -> Manager<C> {
        Manager {
            logger,
            config: C::default(),
            provider: TracerProvider::Global,
            installed: false,
            next_id: 1,
            done: Mutex::new(false),
            cv: Condvar::new(),
        }
    }

    /// Go `Run`: blocks until `stop`.
    pub fn run(&self) {
        let mut d = self.done.lock().unwrap_or_else(|p| p.into_inner());
        while !*d {
            d = self.cv.wait(d).unwrap_or_else(|p| p.into_inner());
        }
    }

    /// Go `ApplyConfig`. `endpoint` is the config's OTLP endpoint (empty disables tracing); `tls_set` is true when
    /// its TLS config is not the zero value (then every apply reinstalls, to pick up new certificates);
    /// `check_client` is Go `getClient` (for example it loads the TLS files when not insecure).
    pub fn apply_config(&mut self, cfg: &C, endpoint: &str, tls_set: bool, check_client: impl FnOnce() -> Result<(), String>) -> Result<(), String> {
        // Update only if a config change is detected.
        if self.config == *cfg && !tls_set {
            return Ok(());
        }
        // The shutdown of an installed provider has nothing to flush here, so it cannot fail.
        self.installed = false;
        if endpoint.is_empty() {
            self.config = cfg.clone();
            self.provider = TracerProvider::Noop;
            self.logger.info("Tracing provider uninstalled.", &[]);
            return Ok(());
        }
        check_client().map_err(|e| format!("failed to install a new tracer provider: {e}"))?;
        self.installed = true;
        self.config = cfg.clone();
        self.provider = TracerProvider::Sdk(self.next_id);
        self.next_id += 1;
        self.logger.info("Successfully installed a new tracer provider.", &[]);
        Ok(())
    }

    /// The provider now in use.
    pub fn tracer_provider(&self) -> TracerProvider {
        self.provider
    }

    /// Go `Stop`: shuts the provider down and ends `run`.
    pub fn stop(&mut self) {
        {
            let mut d = self.done.lock().unwrap_or_else(|p| p.into_inner());
            *d = true;
            self.cv.notify_all();
        }
        if !self.installed {
            return;
        }
        self.installed = false;
        self.logger.info("Tracing manager stopped", &[]);
    }

    /// True after `stop` (Go: the `done` channel is closed).
    pub fn is_done(&self) -> bool {
        *self.done.lock().unwrap_or_else(|p| p.into_inner())
    }
}
