//! Operating system helpers: signals, memory allocator tuning, and Go's `util/runtime` (uname, limits, file
//! system type). Everything that needs `unsafe` in the server is here.

use std::sync::{Condvar, Mutex};
use std::time::Duration;

// ---------------------------------------------------------------------------------------------------------------
// Signals.

/// A signal the server handles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Signal {
    Interrupt,
    Terminate,
    Hangup,
}

impl Signal {
    /// Go `os.Signal.String()`.
    pub fn go_name(self) -> &'static str {
        match self {
            Signal::Interrupt => "interrupt",
            Signal::Terminate => "terminated",
            Signal::Hangup => "hangup",
        }
    }
}

fn handled_set() -> libc::sigset_t {
    // SAFETY: sigemptyset/sigaddset initialize and fill a sigset_t that we own.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGINT);
        libc::sigaddset(&mut set, libc::SIGTERM);
        libc::sigaddset(&mut set, libc::SIGHUP);
        set
    }
}

/// Blocks SIGINT, SIGTERM and SIGHUP in the calling thread. Call it in `main` before any thread starts, so every
/// thread inherits the mask and the signals wait for [`wait_signal`] (Go `signal.Notify`).
pub fn block_signals() {
    let set = handled_set();
    // SAFETY: changes the signal mask of the calling thread only.
    unsafe {
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
    }
}

/// Waits for the next blocked signal (the thread sleeps in `sigwait`; nothing wakes it while idle).
pub fn wait_signal() -> Signal {
    let set = handled_set();
    loop {
        let mut sig: libc::c_int = 0;
        // SAFETY: `set` is initialized and `sig` is a valid out pointer.
        let r = unsafe { libc::sigwait(&set, &mut sig) };
        if r != 0 {
            continue;
        }
        match sig {
            libc::SIGINT => return Signal::Interrupt,
            libc::SIGTERM => return Signal::Terminate,
            libc::SIGHUP => return Signal::Hangup,
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Memory.

/// glibc tuning for a long-running thread-per-connection server (as in Lean Sight): at most two malloc arenas
/// (each thread would otherwise get its own heap) and a fixed mmap threshold, so that large buffers go back to the
/// OS when freed.
pub fn tune_malloc() {
    #[cfg(target_env = "gnu")]
    // SAFETY: mallopt only changes allocator settings of this process.
    unsafe {
        libc::mallopt(libc::M_ARENA_MAX, 2);
        libc::mallopt(libc::M_MMAP_THRESHOLD, 128 * 1024);
    }
}

static IDLE: Mutex<u64> = Mutex::new(0);
static WAKE: Condvar = Condvar::new();

/// Called by the HTTP server when its last connection closes: the trim thread gives free heap pages back to the
/// OS one second later (once per burst of idle signals).
pub fn on_idle() {
    let mut n = IDLE.lock().unwrap_or_else(|p| p.into_inner());
    *n += 1;
    WAKE.notify_one();
}

/// Starts the trim thread. It sleeps until [`on_idle`] wakes it; it never wakes on a timer while idle.
pub fn start_trim_thread() {
    let _ = std::thread::Builder::new().name("malloc-trim".into()).stack_size(64 * 1024).spawn(|| {
        loop {
            {
                let mut n = IDLE.lock().unwrap_or_else(|p| p.into_inner());
                while *n == 0 {
                    n = WAKE.wait(n).unwrap_or_else(|p| p.into_inner());
                }
                loop {
                    let seen = *n;
                    n = WAKE.wait_timeout(n, Duration::from_secs(1)).unwrap_or_else(|p| p.into_inner()).0;
                    if *n == seen {
                        break;
                    }
                }
                *n = 0;
            }
            trim();
        }
    });
}

/// Gives free heap memory back to the OS (glibc `malloc_trim`).
pub fn trim() {
    #[cfg(target_env = "gnu")]
    // SAFETY: malloc_trim only releases free pages.
    unsafe {
        libc::malloc_trim(0);
    }
}

// ---------------------------------------------------------------------------------------------------------------
// util/runtime.

fn cstr(b: &[libc::c_char]) -> String {
    let bytes: Vec<u8> = b.iter().take_while(|c| **c != 0).map(|c| *c as u8).collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Go `prom_runtime.Uname()`: "(sysname release version machine nodename domainname)".
pub fn uname() -> String {
    // SAFETY: uname fills the zeroed struct.
    let u = unsafe {
        let mut u: libc::utsname = std::mem::zeroed();
        if libc::uname(&mut u) != 0 {
            return "(unknown)".to_string();
        }
        u
    };
    format!(
        "({} {} {} {} {} {})",
        cstr(&u.sysname),
        cstr(&u.release),
        cstr(&u.version),
        cstr(&u.machine),
        cstr(&u.nodename),
        cstr(&u.domainname)
    )
}

fn rlimit(res: libc::__rlimit_resource_t) -> Option<(u64, u64)> {
    // SAFETY: getrlimit fills the zeroed struct.
    unsafe {
        let mut r: libc::rlimit = std::mem::zeroed();
        if libc::getrlimit(res, &mut r) != 0 {
            return None;
        }
        Some((r.rlim_cur, r.rlim_max))
    }
}

fn limit_str(v: u64) -> String {
    if v == libc::RLIM_INFINITY { "unlimited".to_string() } else { v.to_string() }
}

/// Go `prom_runtime.FdLimits()`: "(soft=N, hard=N)".
pub fn fd_limits() -> String {
    match rlimit(libc::RLIMIT_NOFILE) {
        Some((s, h)) => format!("(soft={}, hard={})", limit_str(s), limit_str(h)),
        None => "(error)".to_string(),
    }
}

/// Go `prom_runtime.VMLimits()`: "(soft=N, hard=N)" of RLIMIT_AS.
pub fn vm_limits() -> String {
    match rlimit(libc::RLIMIT_AS) {
        Some((s, h)) => format!("(soft={}, hard={})", limit_str(s), limit_str(h)),
        None => "(error)".to_string(),
    }
}

/// Go `prom_runtime.FsType(path)`: the magic name of the file system ("EXT4_SUPER_MAGIC", ...), or the hex
/// magic when unknown, or "0" on error.
pub fn fs_type(path: &str) -> String {
    use std::os::unix::ffi::OsStrExt;
    let Ok(c) = std::ffi::CString::new(std::path::Path::new(path).as_os_str().as_bytes()) else {
        return "0".to_string();
    };
    // SAFETY: `c` is a valid NUL-terminated path and `st` is a zeroed statfs the call fills in.
    let magic = unsafe {
        let mut st: libc::statfs = std::mem::zeroed();
        if libc::statfs(c.as_ptr(), &mut st) != 0 {
            return "0".to_string();
        }
        st.f_type as i64
    };
    // The names of Go's util/runtime statfs_linux_386.go table.
    let name = match magic {
        0xadf5 => "ADFS_SUPER_MAGIC",
        0xADFF => "AFFS_SUPER_MAGIC",
        0x42465331 => "BEFS_SUPER_MAGIC",
        0x1BADFACE => "BFS_MAGIC",
        0xFF534D42 => "CIFS_MAGIC_NUMBER",
        0x73757245 => "CODA_SUPER_MAGIC",
        0x012FF7B7 => "COH_SUPER_MAGIC",
        0x28cd3d45 => "CRAMFS_MAGIC",
        0x1373 => "DEVFS_SUPER_MAGIC",
        0x00414A53 => "EFS_SUPER_MAGIC",
        0x137D => "EXT_SUPER_MAGIC",
        0xEF53 => "EXT4_SUPER_MAGIC",
        0x9123683E => "BTRFS_SUPER_MAGIC",
        0x4244 => "HFS_SUPER_MAGIC",
        0xF995E849 => "HPFS_SUPER_MAGIC",
        0x958458f6 => "HUGETLBFS_MAGIC",
        0x9660 => "ISOFS_SUPER_MAGIC",
        0x72b6 => "JFFS2_SUPER_MAGIC",
        0x3153464a => "JFS_SUPER_MAGIC",
        0x137F => "MINIX_SUPER_MAGIC",
        0x138F => "MINIX_SUPER_MAGIC2",
        0x2468 => "MINIX2_SUPER_MAGIC",
        0x2478 => "MINIX2_SUPER_MAGIC2",
        0x4d44 => "MSDOS_SUPER_MAGIC",
        0x564c => "NCP_SUPER_MAGIC",
        0x6969 => "NFS_SUPER_MAGIC",
        0x5346544e => "NTFS_SB_MAGIC",
        0x9fa1 => "OPENPROM_SUPER_MAGIC",
        0x9fa0 => "PROC_SUPER_MAGIC",
        0x002f => "QNX4_SUPER_MAGIC",
        0x52654973 => "REISERFS_SUPER_MAGIC",
        0x7275 => "ROMFS_MAGIC",
        0x517B => "SMB_SUPER_MAGIC",
        0x012FF7B6 => "SYSV2_SUPER_MAGIC",
        0x012FF7B5 => "SYSV4_SUPER_MAGIC",
        0x01021994 => "TMPFS_MAGIC",
        0x15013346 => "UDF_SUPER_MAGIC",
        0x00011954 => "UFS_MAGIC",
        0x9fa2 => "USBDEVICE_SUPER_MAGIC",
        0xa501FCF5 => "VXFS_SUPER_MAGIC",
        0x012FF7B4 => "XENIX_SUPER_MAGIC",
        0x58465342 => "XFS_SUPER_MAGIC",
        0x012FD16D => "_XIAFS_SUPER_MAGIC",
        0x794c7630 => "OVERLAYFS_SUPER_MAGIC",
        0x2fc12fc1 => "ZFS_SUPER_MAGIC",
        _ => return format!("{magic:x}"),
    };
    name.to_string()
}

/// Go `prom_runtime.FsSize(path)`.
pub fn fs_size(path: &str) -> u64 {
    tsdb::db::fs_size(std::path::Path::new(path))
}

/// The host name (Go `os.Hostname`).
pub fn hostname() -> Result<String, String> {
    std::fs::read_to_string("/proc/sys/kernel/hostname").map(|s| s.trim().to_string()).map_err(|e| e.to_string())
}
