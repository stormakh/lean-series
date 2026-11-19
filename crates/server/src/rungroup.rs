//! Go `github.com/oklog/run`: a group of actors. Each actor is an execute function and an interrupt function.
//! `run` starts every execute function on its own thread, waits for the first one to return, then calls every
//! interrupt function (in the order the actors were added) with that error, waits for all execute functions to
//! return, and returns the first error.

use std::sync::mpsc::channel;

type Execute = Box<dyn FnOnce() -> Result<(), String> + Send>;
type Interrupt = Box<dyn FnOnce(Option<&str>) + Send>;

#[derive(Default)]
pub struct Group {
    actors: Vec<(String, Execute, Interrupt)>,
}

impl Group {
    pub fn new() -> Group {
        Group::default()
    }

    /// Go `Add`. `name` names the actor's thread.
    pub fn add(
        &mut self,
        name: &str,
        execute: impl FnOnce() -> Result<(), String> + Send + 'static,
        interrupt: impl FnOnce(Option<&str>) + Send + 'static,
    ) {
        self.actors.push((name.to_string(), Box::new(execute), Box::new(interrupt)));
    }

    /// Go `Run`.
    pub fn run(self) -> Result<(), String> {
        if self.actors.is_empty() {
            return Ok(());
        }
        let (tx, rx) = channel::<Result<(), String>>();
        let n = self.actors.len();
        let mut interrupts = Vec::with_capacity(n);
        for (name, execute, interrupt) in self.actors {
            let tx = tx.clone();
            // Actors are mostly blocked waits; a small stack is enough. The ones that do real work (the
            // TSDB open, rule evaluation, scraping) run on their own threads with their own stacks.
            std::thread::Builder::new()
                .name(name)
                .stack_size(crate::run::QUERY_STACK_SIZE)
                .spawn(move || {
                    let _ = tx.send(execute());
                })
                .expect("spawn run group actor");
            interrupts.push(interrupt);
        }
        drop(tx);
        // Wait for the first actor to stop.
        let err = rx.recv().unwrap_or(Ok(()));
        // Signal all actors to stop.
        let e = err.as_ref().err().map(|s| s.as_str());
        for i in interrupts {
            i(e);
        }
        // Wait for all actors to stop.
        for _ in 1..n {
            let _ = rx.recv();
        }
        err
    }
}

/// A one-shot event (Go: a channel that is closed once).
#[derive(Default)]
pub struct Event {
    set: std::sync::Mutex<bool>,
    cv: std::sync::Condvar,
}

impl Event {
    pub fn new() -> std::sync::Arc<Event> {
        std::sync::Arc::new(Event::default())
    }
    /// Closes the channel (idempotent, as Go's `sync.Once` close).
    pub fn set(&self) {
        let mut s = self.set.lock().unwrap_or_else(|p| p.into_inner());
        *s = true;
        self.cv.notify_all();
    }
    pub fn is_set(&self) -> bool {
        *self.set.lock().unwrap_or_else(|p| p.into_inner())
    }
    /// Waits until the event is set.
    pub fn wait(&self) {
        let mut s = self.set.lock().unwrap_or_else(|p| p.into_inner());
        while !*s {
            s = self.cv.wait(s).unwrap_or_else(|p| p.into_inner());
        }
    }
}
