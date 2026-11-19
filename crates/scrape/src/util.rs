//! Small Go compatibility helpers and a fast hasher for the scrape cache.

use std::hash::{BuildHasherDefault, Hasher};
use std::time::Duration;

/// Go `%q` of a string.
pub fn go_quote(s: &str) -> String {
    yaml::go_quote(s)
}

/// FNV-1a 64 (Go `hash/fnv.New64a`).
pub fn fnv64a(b: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &c in b {
        h ^= c as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Go `strconv.ParseBool` with its error text.
pub fn parse_bool(s: &str) -> Result<bool, String> {
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(format!("strconv.ParseBool: parsing {}: invalid syntax", go_quote(s))),
    }
}

/// Go `strconv.FormatFloat(f, 'f', -1, 64)`.
pub fn format_float_f(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "+Inf".to_string() } else { "-Inf".to_string() };
    }
    let s = format!("{f}");
    s
}

/// Go `time.Duration.String`: "1h2m3.5s", "10ms", "0s".
pub fn go_duration_string(d: Duration) -> String {
    go_duration_string_nanos(d.as_nanos() as i64)
}

/// Go `time.Duration.String` of nanoseconds.
pub fn go_duration_string_nanos(n: i64) -> String {
    if n == 0 {
        return "0s".to_string();
    }
    let neg = n < 0;
    let u = n.unsigned_abs();
    let mut out = String::new();
    if u < 1_000_000_000 {
        // Sub-second: use the largest unit that keeps a non-zero integer part.
        let (unit, div) = if u < 1_000 {
            ("ns", 1u64)
        } else if u < 1_000_000 {
            ("µs", 1_000)
        } else {
            ("ms", 1_000_000)
        };
        out.push_str(&frac(u, div));
        out.push_str(unit);
    } else {
        let mut secs = u / 1_000_000_000;
        let nanos = u % 1_000_000_000;
        let h = secs / 3600;
        secs %= 3600;
        let m = secs / 60;
        secs %= 60;
        if h > 0 {
            out.push_str(&format!("{h}h"));
        }
        if h > 0 || m > 0 {
            out.push_str(&format!("{m}m"));
        }
        out.push_str(&frac(secs * 1_000_000_000 + nanos, 1_000_000_000));
        out.push('s');
    }
    if neg {
        out.insert(0, '-');
    }
    out
}

fn frac(v: u64, div: u64) -> String {
    let i = v / div;
    let mut f = v % div;
    if f == 0 {
        return i.to_string();
    }
    let mut digits = div.to_string().len() - 1;
    while f % 10 == 0 {
        f /= 10;
        digits -= 1;
    }
    format!("{i}.{f:0digits$}")
}

/// A fast non-cryptographic hasher (the FxHash mixing of rustc) for byte-string and integer keys.
#[derive(Default, Clone, Copy)]
pub struct FxHasher {
    hash: u64,
}

const SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

impl FxHasher {
    #[inline]
    fn add(&mut self, w: u64) {
        self.hash = (self.hash.rotate_left(5) ^ w).wrapping_mul(SEED);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut b = bytes;
        while b.len() >= 8 {
            self.add(u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]));
            b = &b[8..];
        }
        if b.len() >= 4 {
            self.add(u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as u64);
            b = &b[4..];
        }
        for &x in b {
            self.add(x as u64);
        }
    }
    #[inline]
    fn write_u8(&mut self, i: u8) {
        self.add(i as u64);
    }
    #[inline]
    fn write_u32(&mut self, i: u32) {
        self.add(i as u64);
    }
    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.add(i);
    }
    #[inline]
    fn write_usize(&mut self, i: usize) {
        self.add(i as u64);
    }
    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
}

pub type FxBuild = BuildHasherDefault<FxHasher>;
pub type FxHashMap<K, V> = std::collections::HashMap<K, V, FxBuild>;

/// A pool of byte buffers in size classes (Go `util/pool.Pool` with `pool.New(1e3, maxSize, 3, ...)`): buffers
/// are shared between the scrape loops, so memory follows the number of concurrent scrapes, not of targets.
pub struct BufPool {
    min: usize,
    max: usize,
    factor: usize,
    buckets: std::sync::Mutex<Vec<Vec<Vec<u8>>>>,
    sizes: Vec<usize>,
}

/// Buffers kept per size class.
const POOL_KEEP: usize = 8;
/// Bytes kept per size class: large classes keep fewer buffers. Go's sync.Pool frees idle buffers at each GC;
/// a pool that never frees would keep the peak of concurrent scrapes (for example all targets at startup)
/// forever.
const POOL_KEEP_BYTES: usize = 16 << 20;

impl BufPool {
    /// Size classes from `min` to `max`, each `factor` times the previous.
    pub fn new(min: usize, max: usize, factor: usize) -> BufPool {
        let mut sizes = Vec::new();
        let mut s = min.max(1);
        while s <= max {
            sizes.push(s);
            s *= factor.max(2);
        }
        let n = sizes.len();
        BufPool { min, max, factor, buckets: std::sync::Mutex::new(vec![Vec::new(); n]), sizes }
    }

    /// A cleared buffer with at least `sz` capacity.
    pub fn get(&self, sz: usize) -> Vec<u8> {
        for (i, &s) in self.sizes.iter().enumerate() {
            if sz <= s {
                if let Some(mut b) = self.buckets.lock().unwrap()[i].pop() {
                    b.clear();
                    return b;
                }
                return Vec::with_capacity(s);
            }
        }
        Vec::with_capacity(sz)
    }

    /// Returns a buffer to the pool.
    pub fn put(&self, mut b: Vec<u8>) {
        let cap = b.capacity();
        // The largest class that the buffer can serve.
        let Some(i) = self.sizes.iter().rposition(|&s| s <= cap) else { return };
        let mut g = self.buckets.lock().unwrap();
        if g[i].len() < POOL_KEEP.min((POOL_KEEP_BYTES / self.sizes[i]).max(1)) {
            b.clear();
            g[i].push(b);
        }
    }

    /// The pool parameters.
    pub fn params(&self) -> (usize, usize, usize) {
        (self.min, self.max, self.factor)
    }
}
