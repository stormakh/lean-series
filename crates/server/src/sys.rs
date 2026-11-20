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

/// glibc settings that only the environment can give (read once at process start): each thread caches at most
/// 2 freed blocks per size (default 7). With one thread per connection, the default per-thread caches pin
/// scattered heap pages that malloc_trim cannot free: 120 open connections held ~12 MB more, at the same CPU.
const MALLOC_TUNABLES: &str = "glibc.malloc.tcache_count=2";

/// Runs the program again with [`MALLOC_TUNABLES`] in `GLIBC_TUNABLES` when they are not set yet (same process id,
/// same arguments). Call it first in `main`, before any thread starts. Does nothing when the exec fails.
pub fn set_malloc_tunables() {
    #[cfg(target_env = "gnu")]
    {
        use std::os::unix::ffi::OsStrExt;
        let cur = std::env::var_os("GLIBC_TUNABLES").unwrap_or_default();
        if cur.as_bytes().windows(23).any(|w| w == b"glibc.malloc.tcache_cou") {
            return;
        }
        let mut val = cur.as_bytes().to_vec();
        if !val.is_empty() {
            val.push(b':');
        }
        val.extend_from_slice(MALLOC_TUNABLES.as_bytes());
        // SAFETY: no other thread exists yet, so changing the environment races with nothing.
        unsafe { std::env::set_var("GLIBC_TUNABLES", std::ffi::OsStr::from_bytes(&val)) };
        use std::os::unix::process::CommandExt;
        let mut args = std::env::args_os();
        let arg0 = args.next().unwrap_or_default();
        // exec only returns on failure; then run on with the default settings.
        let _ = std::process::Command::new("/proc/self/exe").arg0(arg0).args(args).exec();
    }
}

static IDLE: Mutex<u64> = Mutex::new(0);
static WAKE: Condvar = Condvar::new();

/// Shortest time between two trims: each trim makes the next requests fault their code and heap pages in again.
const TRIM_INTERVAL: Duration = Duration::from_secs(10);

/// Called by the HTTP server when no request is in flight: the trim thread gives free memory back to the OS once
/// no request came for one second (at most every [`TRIM_INTERVAL`]).
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
            std::thread::sleep(TRIM_INTERVAL);
        }
    });
}

/// Gives free heap memory back to the OS (glibc `malloc_trim`) and unmaps the pages of the binary's code.
pub fn trim() {
    #[cfg(target_env = "gnu")]
    // SAFETY: malloc_trim only releases free pages.
    unsafe {
        libc::malloc_trim(0);
    }
    release_binary_pages();
}

/// Unmaps the pages of the binary's read-only file mappings (code, constants) from this process. They stay in the
/// page cache and come back on the next use without I/O. The kernel maps code in 64 KB blocks (fault-around), so
/// after startup most of the 13 MB of code is mapped although little of it runs again; this keeps only the code
/// that the server runs from now on. Mappings with private (relocated) pages are left alone.
pub fn release_binary_pages() {
    static RANGES: std::sync::OnceLock<Vec<(usize, usize)>> = std::sync::OnceLock::new();
    for &(a, len) in RANGES.get_or_init(binary_ranges) {
        // SAFETY: a read-only private file mapping of the executable with no private pages (see
        // `binary_ranges`): dropping its pages loses nothing, the next access maps the same file contents again.
        unsafe {
            libc::madvise(a as *mut libc::c_void, len, libc::MADV_DONTNEED);
        }
    }
}

/// The read-only mappings of the executable that have no private pages: (address, length).
fn binary_ranges() -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let Ok(exe) = std::fs::read_link("/proc/self/exe") else { return out };
    let Ok(smaps) = std::fs::read_to_string("/proc/self/smaps") else { return out };
    let exe = exe.to_string_lossy();
    let mut cur: Option<(usize, usize)> = None;
    for line in smaps.lines() {
        let mut f = line.split_ascii_whitespace();
        let first = f.next().unwrap_or("");
        if let Some((a, b)) = first.split_once('-')
            && let (Ok(a), Ok(b)) = (usize::from_str_radix(a, 16), usize::from_str_radix(b, 16))
        {
            let perms = f.next().unwrap_or("");
            let path = f.nth(3).unwrap_or("");
            cur = (path == exe && perms.starts_with('r') && perms.as_bytes().get(1) == Some(&b'-')).then_some((a, b - a));
        } else if first == "Anonymous:"
            && let Some(r) = cur.take()
            && f.next() == Some("0")
        {
            out.push(r);
        }
    }
    out
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
