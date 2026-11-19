//! Port of `util/notifications`: server notifications for the UI, with live subscribers.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Mutex};

use crate::gotime::Time;

pub const CONFIGURATION_UNSUCCESSFUL: &str = "Configuration reload has failed.";
pub const STARTING_UP: &str = "Prometheus is starting and replaying the write-ahead log (WAL).";
pub const SHUTTING_DOWN: &str = "Prometheus is shutting down and gracefully stopping all operations.";

/// One notification message (Go `Notification`).
#[derive(Clone, Debug, PartialEq)]
pub struct Notification {
    pub text: String,
    pub date: Time,
    pub active: bool,
}

impl Notification {
    /// Go `json.Marshal(Notification)`.
    pub fn write_json(&self, out: &mut String) {
        out.push_str("{\"text\":");
        json::write_str(&self.text, out);
        out.push_str(",\"date\":");
        self.date.write_json(out);
        out.push_str(",\"active\":");
        out.push_str(if self.active { "true" } else { "false" });
        out.push('}');
    }
}

struct State {
    notifications: Vec<Notification>,
    subscribers: Vec<(u64, SyncSender<Notification>)>,
    next_id: u64,
}

/// The notifications and their live subscribers (Go `Notifications`).
pub struct Notifications {
    state: Mutex<State>,
    max_subscribers: usize,
    /// `prometheus_api_notification_active_subscribers`.
    pub active_subscribers: AtomicU64,
    /// `prometheus_api_notification_updates_sent_total`.
    pub updates_sent: AtomicU64,
    /// `prometheus_api_notification_updates_dropped_total`.
    pub updates_dropped: AtomicU64,
}

/// A live subscription; dropping it unsubscribes (Go's `unsubscribe` func).
pub struct Subscription {
    pub rx: Receiver<Notification>,
    id: u64,
    owner: Arc<Notifications>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let mut st = self.owner.state.lock().unwrap_or_else(|e| e.into_inner());
        st.subscribers.retain(|(id, _)| *id != self.id);
        self.owner.active_subscribers.store(st.subscribers.len() as u64, Ordering::Relaxed);
    }
}

impl Notifications {
    /// Go `NewNotifications`.
    pub fn new(max_subscribers: usize) -> Arc<Notifications> {
        Arc::new(Notifications {
            state: Mutex::new(State { notifications: Vec::new(), subscribers: Vec::new(), next_id: 0 }),
            max_subscribers,
            active_subscribers: AtomicU64::new(0),
            updates_sent: AtomicU64::new(0),
            updates_dropped: AtomicU64::new(0),
        })
    }

    fn notify(&self, st: &State, n: &Notification) {
        for (_, sub) in &st.subscribers {
            self.updates_sent.fetch_add(1, Ordering::Relaxed);
            match sub.try_send(n.clone()) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                    self.updates_dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Go `AddNotification`: adds a notification or updates the date of an existing one.
    pub fn add_notification(&self, text: &str) {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(i) = st.notifications.iter().position(|n| n.text == text) {
            st.notifications[i].date = Time::now();
            let n = st.notifications[i].clone();
            self.notify(&st, &n);
            return;
        }
        let n = Notification { text: text.to_string(), date: Time::now(), active: true };
        st.notifications.push(n.clone());
        self.notify(&st, &n);
    }

    /// Go `DeleteNotification`: removes the first notification with this text, telling subscribers it is inactive.
    pub fn delete_notification(&self, text: &str) {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(i) = st.notifications.iter().position(|n| n.text == text) {
            let mut n = st.notifications[i].clone();
            n.active = false;
            self.notify(&st, &n);
            st.notifications.remove(i);
        }
    }

    /// Go `Get`.
    pub fn get(&self) -> Vec<Notification> {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).notifications.clone()
    }

    /// Go `Sub`: `None` when the maximum number of subscribers is reached.
    pub fn sub(self: &Arc<Self>) -> Option<Subscription> {
        let mut st = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if st.subscribers.len() >= self.max_subscribers {
            return None;
        }
        let (tx, rx) = sync_channel(10);
        let id = st.next_id;
        st.next_id += 1;
        for n in &st.notifications {
            // Go blocks here when more than 10 notifications exist; a full buffer drops the rest instead.
            let _ = tx.try_send(n.clone());
        }
        st.subscribers.push((id, tx));
        self.active_subscribers.store(st.subscribers.len() as u64, Ordering::Relaxed);
        Some(Subscription { rx, id, owner: self.clone() })
    }
}
