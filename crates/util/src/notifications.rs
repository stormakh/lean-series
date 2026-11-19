//! Port of `util/notifications`: the server notifications that the UI shows (config reload failed, starting up,
//! shutting down), with live subscribers.
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

pub const CONFIGURATION_UNSUCCESSFUL: &str = "Configuration reload has failed.";
pub const STARTING_UP: &str = "Prometheus is starting and replaying the write-ahead log (WAL).";
pub const SHUTTING_DOWN: &str = "Prometheus is shutting down and gracefully stopping all operations.";

/// Metric names and help texts of the three self-metrics (gauge, counter, counter).
pub const METRIC_ACTIVE_SUBSCRIBERS: (&str, &str) =
    ("prometheus_api_notification_active_subscribers", "The current number of active notification subscribers.");
pub const METRIC_UPDATES_SENT: (&str, &str) =
    ("prometheus_api_notification_updates_sent_total", "Total number of notification updates sent.");
pub const METRIC_UPDATES_DROPPED: (&str, &str) =
    ("prometheus_api_notification_updates_dropped_total", "Total number of notification updates dropped.");

/// One notification (JSON `{"text":..,"date":..,"active":..}`).
#[derive(Debug, Clone, PartialEq)]
pub struct Notification {
    pub text: String,
    pub date: SystemTime,
    pub active: bool,
}

impl Notification {
    /// Go's `json.Marshal`: the date is RFC 3339 with nanoseconds in the local zone.
    pub fn write_json(&self, out: &mut String) {
        out.push_str("{\"text\":");
        json::write_str(&self.text, out);
        out.push_str(",\"date\":\"");
        crate::logging::tz::append_rfc3339(out, self.date, crate::logging::tz::local(), crate::logging::tz::Frac::Nanos);
        out.push_str("\",\"active\":");
        out.push_str(if self.active { "true" } else { "false" });
        out.push('}');
    }
}

struct Inner {
    notifications: Vec<Notification>,
    subscribers: Vec<(u64, SyncSender<Notification>)>,
    next_id: u64,
}

struct Shared {
    mu: Mutex<Inner>,
    max_subscribers: usize,
    subscriber_gauge: AtomicU64,
    sent: AtomicU64,
    dropped: AtomicU64,
}

/// Go `Notifications`. Cloning gives another handle to the same list.
#[derive(Clone)]
pub struct Notifications(Arc<Shared>);

impl Notifications {
    /// Go `NewNotifications`. The metrics are read with [`Notifications::active_subscribers`],
    /// [`Notifications::updates_sent`] and [`Notifications::updates_dropped`].
    pub fn new(max_subscribers: usize) -> Notifications {
        Notifications(Arc::new(Shared {
            mu: Mutex::new(Inner { notifications: Vec::new(), subscribers: Vec::new(), next_id: 0 }),
            max_subscribers,
            subscriber_gauge: AtomicU64::new(0),
            sent: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
        }))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.0.mu.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Adds a notification, or updates the date of the one with the same text.
    pub fn add_notification(&self, text: &str) {
        let mut g = self.lock();
        if let Some(i) = g.notifications.iter().position(|n| n.text == text) {
            g.notifications[i].date = SystemTime::now();
            let n = g.notifications[i].clone();
            self.notify_subscribers(&g, n);
            return;
        }
        let n = Notification { text: text.to_string(), date: SystemTime::now(), active: true };
        g.notifications.push(n.clone());
        self.notify_subscribers(&g, n);
    }

    /// Sends without blocking; a full subscriber channel drops the update.
    fn notify_subscribers(&self, g: &Inner, n: Notification) {
        for (_, sub) in g.subscribers.iter() {
            self.0.sent.fetch_add(1, Ordering::Relaxed);
            match sub.try_send(n.clone()) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                    self.0.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Removes the first notification with this text; subscribers get it with `active: false` first.
    pub fn delete_notification(&self, text: &str) {
        let mut g = self.lock();
        if let Some(i) = g.notifications.iter().position(|n| n.text == text) {
            let mut n = g.notifications[i].clone();
            n.active = false;
            self.notify_subscribers(&g, n);
            g.notifications.remove(i);
        }
    }

    /// A copy of the current notifications.
    pub fn get(&self) -> Vec<Notification> {
        self.lock().notifications.clone()
    }

    /// Go `Sub`: a channel with live notifications (current ones first) and the unsubscribe handle, or `None` when
    /// the subscriber limit is reached.
    pub fn sub(&self) -> Option<(Receiver<Notification>, Unsubscribe)> {
        let mut g = self.lock();
        if g.subscribers.len() >= self.0.max_subscribers {
            return None;
        }
        let (tx, rx) = sync_channel(10);
        let id = g.next_id;
        g.next_id += 1;
        // Go sends the current notifications with a blocking send while it holds the lock.
        for n in g.notifications.iter() {
            let _ = tx.send(n.clone());
        }
        g.subscribers.push((id, tx));
        self.0.subscriber_gauge.store(g.subscribers.len() as u64, Ordering::Relaxed);
        Some((rx, Unsubscribe { n: self.clone(), id }))
    }

    pub fn active_subscribers(&self) -> u64 {
        self.0.subscriber_gauge.load(Ordering::Relaxed)
    }
    pub fn updates_sent(&self) -> u64 {
        self.0.sent.load(Ordering::Relaxed)
    }
    pub fn updates_dropped(&self) -> u64 {
        self.0.dropped.load(Ordering::Relaxed)
    }
}

/// Removes a subscriber and closes its channel.
pub struct Unsubscribe {
    n: Notifications,
    id: u64,
}

impl Unsubscribe {
    pub fn unsubscribe(self) {
        let mut g = self.n.lock();
        g.subscribers.retain(|s| s.0 != self.id);
        self.n.0.subscriber_gauge.store(g.subscribers.len() as u64, Ordering::Relaxed);
    }
}
