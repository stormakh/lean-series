//! The channel that a discoverer sends target group updates on: Go's unbuffered `chan []*targetgroup.Group`
//! with the `select { case ch <- tgs: case <-ctx.Done(): }` pattern built in.
//!
//! `send` hands the update over and blocks until the receiver took it (rendezvous, as an unbuffered Go channel),
//! and returns false instead when the channel's cancel token is cancelled or the receiver is gone. Dropping the
//! sender closes the channel (Go `close(ch)`).
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::cancel::CancelToken;
use crate::targetgroup::Group;

/// One update: the changed target groups (Go `[]*targetgroup.Group`).
pub type Update = Vec<Arc<Group>>;

struct State {
    slot: Option<Update>,
    /// A counter of updates taken, so a sender knows its own update was received.
    taken: u64,
    put: u64,
    senders: usize,
    closed: bool,
    rx_gone: bool,
    cancelled: bool,
}

struct Shared {
    m: Mutex<State>,
    cv: Condvar,
}

pub struct Sender(Arc<Shared>);
pub struct Receiver(Arc<Shared>);

/// What `Receiver::recv` got.
#[derive(Debug)]
pub enum Recv {
    Update(Update),
    /// The sender was dropped (Go: channel closed).
    Closed,
    /// The cancel token of the channel was cancelled.
    Cancelled,
    /// `recv_timeout` only.
    Timeout,
}

/// A channel without cancellation (the receiver side decides when to stop).
pub fn channel() -> (Sender, Receiver) {
    let s = Arc::new(Shared { m: Mutex::new(State { slot: None, taken: 0, put: 0, senders: 1, closed: false, rx_gone: false, cancelled: false }), cv: Condvar::new() });
    (Sender(s.clone()), Receiver(s))
}

/// A channel that stops blocking both sides when `ctx` is cancelled.
pub fn channel_with_cancel(ctx: &CancelToken) -> (Sender, Receiver) {
    let (tx, rx) = channel();
    let weak = Arc::downgrade(&tx.0);
    ctx.on_cancel(Box::new(move || {
        if let Some(s) = weak.upgrade() {
            s.m.lock().unwrap_or_else(|e| e.into_inner()).cancelled = true;
            s.cv.notify_all();
        }
    }));
    (tx, rx)
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.m.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Sender {
    /// Sends one update and waits until the receiver took it. Returns false (the update is dropped) when the
    /// channel was cancelled or the receiver was dropped first.
    pub fn send(&self, u: Update) -> bool {
        let sh = &self.0;
        let mut s = sh.lock();
        // wait for a free slot (other clones may be sending)
        while s.slot.is_some() {
            if s.cancelled || s.rx_gone {
                return false;
            }
            s = sh.cv.wait(s).unwrap_or_else(|e| e.into_inner());
        }
        if s.cancelled || s.rx_gone {
            return false;
        }
        s.slot = Some(u);
        s.put += 1;
        let my = s.put;
        sh.cv.notify_all();
        loop {
            if s.taken >= my {
                return true;
            }
            if s.cancelled || s.rx_gone {
                if s.taken < my {
                    s.slot = None;
                }
                sh.cv.notify_all();
                return false;
            }
            s = sh.cv.wait(s).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Whether the receiver is gone or the channel cancelled (a send would fail).
    pub fn is_closed(&self) -> bool {
        let s = self.0.lock();
        s.cancelled || s.rx_gone
    }
}

/// Clones send on the same channel; it closes when the last one is dropped.
impl Clone for Sender {
    fn clone(&self) -> Self {
        self.0.lock().senders += 1;
        Sender(self.0.clone())
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        let mut s = self.0.lock();
        s.senders -= 1;
        if s.senders == 0 {
            s.closed = true;
            drop(s);
            self.0.cv.notify_all();
        }
    }
}

impl Receiver {
    fn take(&self, deadline: Option<Instant>) -> Recv {
        let sh = &self.0;
        let mut s = sh.lock();
        loop {
            if s.cancelled {
                return Recv::Cancelled;
            }
            if let Some(u) = s.slot.take() {
                s.taken += 1;
                sh.cv.notify_all();
                return Recv::Update(u);
            }
            if s.closed {
                return Recv::Closed;
            }
            match deadline {
                None => s = sh.cv.wait(s).unwrap_or_else(|e| e.into_inner()),
                Some(d) => {
                    let now = Instant::now();
                    if now >= d {
                        return Recv::Timeout;
                    }
                    s = sh.cv.wait_timeout(s, d - now).unwrap_or_else(|e| e.into_inner()).0;
                }
            }
        }
    }

    /// Blocks for the next update (Go `tgs, ok := <-ch` with `ctx.Done()`).
    pub fn recv(&self) -> Recv {
        self.take(None)
    }

    /// Like `recv`, giving up after `d`.
    pub fn recv_timeout(&self, d: Duration) -> Recv {
        self.take(Some(Instant::now() + d))
    }

    /// The next update or None if there is none now.
    pub fn try_recv(&self) -> Option<Update> {
        let mut s = self.0.lock();
        let u = s.slot.take();
        if u.is_some() {
            s.taken += 1;
            self.0.cv.notify_all();
        }
        u
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.0.lock().rx_gone = true;
        self.0.cv.notify_all();
    }
}
