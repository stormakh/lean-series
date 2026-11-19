//! Port of `util/netconnlimit`: limits simultaneous connections across several listeners with one shared
//! semaphore.
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

/// Go `NewSharedSemaphore`: a counting semaphore with `n` slots.
#[derive(Debug)]
pub struct SharedSemaphore {
    cap: usize,
    used: Mutex<usize>,
    cv: Condvar,
}

impl SharedSemaphore {
    pub fn new(n: usize) -> Arc<SharedSemaphore> {
        Arc::new(SharedSemaphore { cap: n, used: Mutex::new(0), cv: Condvar::new() })
    }
    /// Slots in use (Go `len(sem)`).
    pub fn len(&self) -> usize {
        *self.used.lock().unwrap_or_else(|p| p.into_inner())
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn release(&self) {
        let mut g = self.used.lock().unwrap_or_else(|p| p.into_inner());
        *g -= 1;
        self.cv.notify_all();
    }
}

/// Go `SharedLimitListener`: accepts at most as many simultaneous connections as the shared semaphore allows,
/// across every listener that shares it.
pub struct SharedLimitListener {
    listener: TcpListener,
    addr: Option<SocketAddr>,
    sem: Arc<SharedSemaphore>,
    done: AtomicBool,
    in_accept: AtomicUsize,
}

fn closed_err(addr: Option<SocketAddr>) -> io::Error {
    let a = addr.map(|a| a.to_string()).unwrap_or_default();
    io::Error::new(io::ErrorKind::NotConnected, format!("accept tcp {a}: use of closed network connection"))
}

impl SharedLimitListener {
    pub fn new(listener: TcpListener, sem: Arc<SharedSemaphore>) -> SharedLimitListener {
        let addr = listener.local_addr().ok();
        SharedLimitListener { listener, addr, sem, done: AtomicBool::new(false), in_accept: AtomicUsize::new(0) }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Takes a slot; false when the listener was closed while waiting.
    fn acquire(&self) -> bool {
        let mut g = self.sem.used.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            if self.done.load(Ordering::SeqCst) {
                return false;
            }
            if *g < self.sem.cap {
                *g += 1;
                return true;
            }
            g = self.sem.cv.wait(g).unwrap_or_else(|p| p.into_inner());
        }
    }

    /// Waits for a free slot, then accepts one connection. The slot is freed when the connection is dropped.
    /// After `close` it fails with "use of closed network connection".
    pub fn accept(&self) -> io::Result<LimitedConn> {
        if !self.acquire() {
            return Err(closed_err(self.addr));
        }
        self.in_accept.fetch_add(1, Ordering::SeqCst);
        let r = self.listener.accept();
        self.in_accept.fetch_sub(1, Ordering::SeqCst);
        match r {
            Ok((c, _)) if !self.done.load(Ordering::SeqCst) => Ok(LimitedConn { conn: c, sem: Some(self.sem.clone()) }),
            Ok(_) => {
                self.sem.release();
                Err(closed_err(self.addr))
            }
            Err(e) => {
                self.sem.release();
                Err(e)
            }
        }
    }

    /// Go `Close`: later and waiting `accept` calls fail. A thread blocked in the OS accept is woken with a local
    /// connection, because std's `TcpListener` cannot be closed while it is shared.
    pub fn close(&self) -> io::Result<()> {
        if self.done.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        {
            let _g = self.sem.used.lock().unwrap_or_else(|p| p.into_inner());
            self.sem.cv.notify_all();
        }
        if let Some(a) = self.addr {
            for _ in 0..self.in_accept.load(Ordering::SeqCst) {
                let _ = TcpStream::connect(a);
            }
        }
        Ok(())
    }
}

/// A connection that holds one semaphore slot until it is closed or dropped.
#[derive(Debug)]
pub struct LimitedConn {
    conn: TcpStream,
    sem: Option<Arc<SharedSemaphore>>,
}

impl LimitedConn {
    /// Closes the connection and frees the slot (once).
    pub fn close(&mut self) {
        let _ = self.conn.shutdown(std::net::Shutdown::Both);
        if let Some(s) = self.sem.take() {
            s.release();
        }
    }
}

impl Drop for LimitedConn {
    fn drop(&mut self) {
        if let Some(s) = self.sem.take() {
            s.release();
        }
    }
}

impl Deref for LimitedConn {
    type Target = TcpStream;
    fn deref(&self) -> &TcpStream {
        &self.conn
    }
}

impl DerefMut for LimitedConn {
    fn deref_mut(&mut self) -> &mut TcpStream {
        &mut self.conn
    }
}
