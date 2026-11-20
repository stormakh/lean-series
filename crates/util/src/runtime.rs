//! Port of `util/runtime`: file descriptor and virtual memory limits, `uname`, and the file system type and size
//! of a path (Linux and other Unix systems).
//!
//! These are system calls with no std API, so this module calls `libc` (already a dependency of tsdb-format).

/// Go `syscall.RLIM_INFINITY`.
const UNLIMITED: u64 = libc::RLIM_INFINITY as u64;

fn limit_to_string(v: u64, unit: &str) -> String {
    if v == UNLIMITED { "unlimited".to_string() } else { format!("{v}{unit}") }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
type Resource = libc::__rlimit_resource_t;
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
type Resource = libc::c_int;

fn get_limits(resource: Resource, unit: &str) -> String {
    let mut r = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    // SAFETY: `r` is a valid, writable rlimit for the call.
    let rc = unsafe { libc::getrlimit(resource, &mut r) };
    if rc != 0 {
        panic!("syscall.Getrlimit failed: {}", std::io::Error::last_os_error());
    }
    format!("(soft={}, hard={})", limit_to_string(r.rlim_cur as u64, unit), limit_to_string(r.rlim_max as u64, unit))
}

/// Go `FdLimits`: "(soft=1024, hard=524288)".
pub fn fd_limits() -> String {
    get_limits(libc::RLIMIT_NOFILE, "")
}

/// Go `VMLimits`: "(soft=unlimited, hard=unlimited)" (bytes, with a "b" unit).
pub fn vm_limits() -> String {
    get_limits(libc::RLIMIT_AS, "b")
}

fn cstr(b: &[libc::c_char]) -> String {
    let bytes: Vec<u8> = b.iter().take_while(|&&c| c != 0).map(|&c| c as u8).collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

/// Go `Uname`: "(sysname release version machine nodename domainname)".
pub fn uname() -> String {
    // SAFETY: utsname is plain bytes; all zero is a valid value.
    let mut u: libc::utsname = unsafe { std::mem::zeroed() };
    // SAFETY: `u` is a valid, writable utsname for the call.
    if unsafe { libc::uname(&mut u) } != 0 {
        panic!("unix.Uname failed: {}", std::io::Error::last_os_error());
    }
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

/// statfs(2); `None` on error (including a path with a NUL byte).
fn statfs(path: &str) -> Option<libc::statfs> {
    let c = std::ffi::CString::new(path).ok()?;
    // SAFETY: statfs is plain data; all zero is a valid value.
    let mut s: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is a NUL-terminated path and `s` a valid, writable statfs for the call.
    if unsafe { libc::statfs(c.as_ptr(), &mut s) } != 0 {
        return None;
    }
    Some(s)
}

/// Types of file systems that `statfs` may return (linux/magic.h, then some that are not in it).
const FS_TYPES: &[(i64, &str)] = &[
    (0xadf5, "ADFS_SUPER_MAGIC"),
    (0xadff, "AFFS_SUPER_MAGIC"),
    (0x5346414F, "AFS_SUPER_MAGIC"),
    (0x0187, "AUTOFS_SUPER_MAGIC"),
    (0x00c36400, "CEPH_SUPER_MAGIC"),
    (0x73757245, "CODA_SUPER_MAGIC"),
    (0x28cd3d45, "CRAMFS_MAGIC"),
    (0x453dcd28, "CRAMFS_MAGIC_WEND"),
    (0x64626720, "DEBUGFS_MAGIC"),
    (0x73636673, "SECURITYFS_MAGIC"),
    (0xf97cff8c, "SELINUX_MAGIC"),
    (0x43415d53, "SMACK_MAGIC"),
    (0x858458f6, "RAMFS_MAGIC"),
    (0x01021994, "TMPFS_MAGIC"),
    (0x958458f6, "HUGETLBFS_MAGIC"),
    (0x73717368, "SQUASHFS_MAGIC"),
    (0xf15f, "ECRYPTFS_SUPER_MAGIC"),
    (0x414A53, "EFS_SUPER_MAGIC"),
    (0xE0F5E1E2, "EROFS_SUPER_MAGIC_V1"),
    (0xabba1974, "XENFS_SUPER_MAGIC"),
    (0xEF53, "EXT4_SUPER_MAGIC"),
    (0x9123683E, "BTRFS_SUPER_MAGIC"),
    (0x3434, "NILFS_SUPER_MAGIC"),
    (0xF2F52010, "F2FS_SUPER_MAGIC"),
    (0xf995e849, "HPFS_SUPER_MAGIC"),
    (0x9660, "ISOFS_SUPER_MAGIC"),
    (0x72b6, "JFFS2_SUPER_MAGIC"),
    (0x58465342, "XFS_SUPER_MAGIC"),
    (0x6165676C, "PSTOREFS_MAGIC"),
    (0xde5e81e4, "EFIVARFS_MAGIC"),
    (0x00c0ffee, "HOSTFS_SUPER_MAGIC"),
    (0x794c7630, "OVERLAYFS_SUPER_MAGIC"),
    (0x65735546, "FUSE_SUPER_MAGIC"),
    (0xca451a4e, "BCACHEFS_SUPER_MAGIC"),
    (0x137F, "MINIX_SUPER_MAGIC"),
    (0x138F, "MINIX_SUPER_MAGIC2"),
    (0x2468, "MINIX2_SUPER_MAGIC"),
    (0x2478, "MINIX2_SUPER_MAGIC2"),
    (0x4d5a, "MINIX3_SUPER_MAGIC"),
    (0x4d44, "MSDOS_SUPER_MAGIC"),
    (0x2011BAB0, "EXFAT_SUPER_MAGIC"),
    (0x564c, "NCP_SUPER_MAGIC"),
    (0x6969, "NFS_SUPER_MAGIC"),
    (0x7461636f, "OCFS2_SUPER_MAGIC"),
    (0x9fa1, "OPENPROM_SUPER_MAGIC"),
    (0x002f, "QNX4_SUPER_MAGIC"),
    (0x68191122, "QNX6_SUPER_MAGIC"),
    (0x6B414653, "AFS_FS_MAGIC"),
    (0x52654973, "REISERFS_SUPER_MAGIC"),
    (0x517B, "SMB_SUPER_MAGIC"),
    (0xFF534D42, "CIFS_SUPER_MAGIC"),
    (0xFE534D42, "SMB2_SUPER_MAGIC"),
    (0x27e0eb, "CGROUP_SUPER_MAGIC"),
    (0x63677270, "CGROUP2_SUPER_MAGIC"),
    (0x7655821, "RDTGROUP_SUPER_MAGIC"),
    (0x57AC6E9D, "STACK_END_MAGIC"),
    (0x74726163, "TRACEFS_MAGIC"),
    (0x01021997, "V9FS_MAGIC"),
    (0x62646576, "BDEVFS_MAGIC"),
    (0x64646178, "DAXFS_MAGIC"),
    (0x42494e4d, "BINFMTFS_MAGIC"),
    (0x1cd1, "DEVPTS_SUPER_MAGIC"),
    (0x6c6f6f70, "BINDERFS_SUPER_MAGIC"),
    (0xBAD1DEA, "FUTEXFS_SUPER_MAGIC"),
    (0x50495045, "PIPEFS_MAGIC"),
    (0x9fa0, "PROC_SUPER_MAGIC"),
    (0x534F434B, "SOCKFS_MAGIC"),
    (0x62656572, "SYSFS_MAGIC"),
    (0x9fa2, "USBDEVICE_SUPER_MAGIC"),
    (0x11307854, "MTD_INODE_FS_MAGIC"),
    (0x09041934, "ANON_INODE_FS_MAGIC"),
    (0x73727279, "BTRFS_TEST_MAGIC"),
    (0x6e736673, "NSFS_MAGIC"),
    (0xcafe4a11, "BPF_FS_MAGIC"),
    (0x5a3c69f0, "AAFS_MAGIC"),
    (0x5a4f4653, "ZONEFS_MAGIC"),
    (0x15013346, "UDF_SUPER_MAGIC"),
    (0x444d4142, "DMA_BUF_MAGIC"),
    (0x454d444d, "DEVMEM_MAGIC"),
    (0x5345434d, "SECRETMEM_MAGIC"),
    (0x50494446, "PID_FS_MAGIC"),
    (0x474d454d, "GUEST_MEMFD_MAGIC"),
    (0x4E554C4C, "NULL_FS_MAGIC"),
    // not in magic.h
    (0x42465331, "BEFS_SUPER_MAGIC"),
    (0x1BADFACE, "BFS_MAGIC"),
    (0x012FF7B7, "COH_SUPER_MAGIC"),
    (0x1373, "DEVFS_SUPER_MAGIC"),
    (0x137D, "EXT_SUPER_MAGIC"),
    (0xEF51, "EXT2_OLD_SUPER_MAGIC"),
    (0x4244, "HFS_SUPER_MAGIC"),
    (0x3153464a, "JFS_SUPER_MAGIC"),
    (0x5346544e, "NTFS_SB_MAGIC"),
    (0x7275, "ROMFS_MAGIC"),
    (0x012FF7B6, "SYSV2_SUPER_MAGIC"),
    (0x012FF7B5, "SYSV4_SUPER_MAGIC"),
    (0x00011954, "UFS_MAGIC"),
    (0xa501FCF5, "VXFS_SUPER_MAGIC"),
    (0x012FF7B4, "XENIX_SUPER_MAGIC"),
    (0x012FD16D, "_XIAFS_SUPER_MAGIC"),
];

/// Go `FsType`: the magic name of the file system of `path` (for example "EXT4_SUPER_MAGIC"), or its type number
/// in hex when the name is unknown. A failed `statfs` gives "0".
pub fn fs_type(path: &str) -> String {
    let t = match statfs(path) {
        Some(s) => s.f_type as i64,
        None => return "0".to_string(),
    };
    for (m, name) in FS_TYPES {
        if *m == t {
            return name.to_string();
        }
    }
    if t < 0 { format!("-{:x}", t.unsigned_abs()) } else { format!("{t:x}") }
}

/// Go `FsSize`: block size times block count, or 0 when `statfs` fails.
pub fn fs_size(path: &str) -> u64 {
    match statfs(path) {
        Some(s) => (s.f_bsize as u64).wrapping_mul(s.f_blocks as u64),
        None => 0,
    }
}

/// A file mapped read-write and shared (`mmap(MAP_SHARED)`), as Go's `fileutil.OpenMmapFile` with write access:
/// writes reach the page cache without a system call, and the kernel writes them to the file. Used by the active
/// query tracker (`queries.active`), which Go also maps.
pub struct SharedMap {
    ptr: *mut u8,
    len: usize,
}

// SAFETY: the mapping is plain memory owned by this value; `&self` only reads it, writes need `&mut self`.
unsafe impl Send for SharedMap {}
// SAFETY: see `Send`.
unsafe impl Sync for SharedMap {}

impl SharedMap {
    /// Maps the first `len` bytes of `file` (opened read-write, at least `len` bytes long).
    pub fn new(file: &std::fs::File, len: usize) -> std::io::Result<SharedMap> {
        use std::os::unix::io::AsRawFd;
        if len == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "mmap of zero bytes"));
        }
        // SAFETY: a new shared mapping of a file descriptor that is valid for the call; the result is checked.
        let p = unsafe {
            libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ | libc::PROT_WRITE, libc::MAP_SHARED, file.as_raw_fd(), 0)
        };
        if p == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        Ok(SharedMap { ptr: p as *mut u8, len })
    }

    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: `ptr` maps `len` readable bytes for the life of `self`.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: `ptr` maps `len` writable bytes for the life of `self`, and `&mut self` makes this the only access.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    /// `msync(MS_SYNC)`: writes the mapped pages to the file.
    pub fn sync(&self) -> std::io::Result<()> {
        // SAFETY: `ptr` and `len` are the mapping made in `new`.
        if unsafe { libc::msync(self.ptr as *mut libc::c_void, self.len, libc::MS_SYNC) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Drop for SharedMap {
    fn drop(&mut self) {
        // SAFETY: `ptr` and `len` are the mapping made in `new`, unmapped only here.
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.len);
        }
    }
}
