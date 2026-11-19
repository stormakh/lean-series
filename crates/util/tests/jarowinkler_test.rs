//! Port of `util/strutil/jarowinkler_test.go`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use util::strutil::{JaroWinklerMatcher, JaroWinklerWorkspace};

// Counts allocations per thread, for Go's testing.AllocsPerRun.
struct CountingAlloc;

thread_local! {
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCS.try_with(|c| c.set(c.get() + 1));
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let _ = ALLOCS.try_with(|c| c.set(c.get() + 1));
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

/// Go `testing.AllocsPerRun`: one unmeasured warm-up call, then the average allocations per call.
fn allocs_per_run(runs: u64, mut f: impl FnMut()) -> f64 {
    f();
    let before = ALLOCS.with(|c| c.get());
    for _ in 0..runs {
        f();
    }
    let after = ALLOCS.with(|c| c.get());
    // Go truncates the average to an integer.
    ((after - before) / runs) as f64
}

#[test]
fn test_jaro_winkler_matcher() {
    let tests: &[(&str, &str, f64, f64)] = &[
        // Identical strings.
        ("prometheus", "prometheus", 1.0, 1.0),
        ("", "", 1.0, 1.0),
        // Empty vs non-empty.
        ("", "abc", 0.0, 0.0),
        ("abc", "", 0.0, 0.0),
        // Completely different strings.
        ("abc", "xyz", 0.0, 0.01),
        // Similar strings.
        ("mimir", "mimer", 0.90, 0.92),
        ("martha", "marhta", 0.96, 0.97),
        ("dwayne", "duane", 0.83, 0.85),
        ("dixon", "dicksonx", 0.81, 0.83),
        // Single character strings.
        ("a", "a", 1.0, 1.0),
        ("a", "b", 0.0, 0.0),
        // Common prefix boost.
        ("prefix_abc", "prefix_xyz", 0.80, 0.90),
        // Unicode strings (exercises the rune path).
        ("café", "cafe", 0.88, 0.89),
        ("naïve", "naive", 0.89, 0.90),
        ("résumé", "resume", 0.79, 0.81),
        // Identical Unicode strings.
        ("café", "café", 1.0, 1.0),
        // Empty vs Unicode.
        ("", "café", 0.0, 0.0),
        ("café", "", 0.0, 0.0),
        // Two Unicode strings compared to each other.
        ("café", "cafè", 0.88, 0.89),
        // Common Unicode prefix (exercises Winkler boost on runes).
        ("préfixe_abc", "préfixe_xyz", 0.80, 0.90),
        // Unicode strings with unequal rune lengths (exercises swap in rune path).
        ("naïve_long", "naïve", 0.89, 0.91),
        // Completely different Unicode strings (exercises zero-matches in rune path).
        ("äöü", "éèê", 0.0, 0.01),
        // Unicode transpositions (mirrors martha/marhta in rune path).
        ("màrthà", "màrhtà", 0.96, 0.97),
    ];
    for &(s1, s2, min, max) in tests {
        let score = JaroWinklerMatcher::new(s1).score(s2);
        assert!(score >= min && score <= max, "NewJaroWinklerMatcher({s1:?}).Score({s2:?}) = {score}, want in [{min}, {max}]");
        // Verify symmetry.
        let reverse = JaroWinklerMatcher::new(s2).score(s1);
        assert!((score - reverse).abs() <= 1e-10, "{s1:?}/{s2:?}: {score} vs {reverse} (not symmetric)");
    }
}

#[test]
fn test_jaro_winkler_matcher_across_candidate_sizes() {
    let matcher = JaroWinklerMatcher::new("prometheus");
    let tests: Vec<(&str, String, f64, f64)> = vec![
        ("large similar", format!("promethus{}", "x".repeat(128)), 0.79, 0.80),
        ("small similar", "promethus".to_string(), 0.979, 0.981),
        ("large dissimilar", "z".repeat(256), 0.0, 0.0),
        ("exact", "prometheus".to_string(), 1.0, 1.0),
    ];
    for (name, candidate, min, max) in tests {
        let score = matcher.score(&candidate);
        assert!(score >= min && score <= max, "{name}: Score({candidate:?}) = {score}, want in [{min}, {max}]");
    }
}

#[test]
fn test_jaro_winkler_workspace_match_slices() {
    // A workspace whose buffer was shrunk by an earlier, smaller call, with stale flags left in the backing
    // array.
    let mut backing = vec![true; 200];
    backing.truncate(50);
    let mut w = JaroWinklerWorkspace { matches: backing };

    let steps: &[(&str, usize, usize, bool, usize)] = &[
        ("grow past len within cap", 60, 90, true, 200),
        ("shrink", 3, 40, true, 200),
        ("grow to exactly cap", 100, 100, true, 200),
        ("grow past cap", 150, 151, false, 301),
    ];
    for &(name, l1, l2, want_reused, want_cap_from) in steps {
        let prev = w.matches.as_ptr();
        let (s1, s2) = w.match_slices(l1, l2);
        assert!(s1.len() == l1 && s2.len() == l2, "{name}: lengths = {}, {}, want {l1}, {l2}", s1.len(), s2.len());
        // Dirty the buffers, as a score would, after the checks below.
        let reused = w.matches.as_ptr() == prev;
        assert_eq!(reused, want_reused, "{name}: reused backing array");
        assert!(w.matches.capacity() >= want_cap_from, "{name}: cap = {}, want at least {want_cap_from}", w.matches.capacity());
        for (i, &v) in w.matches.iter().enumerate() {
            assert!(!v, "{name}: flag {i} is stale");
        }
        let (s1, s2) = w.matches.split_at_mut(l1);
        s1.fill(true);
        s2.fill(true);
    }
}

#[test]
fn test_jaro_winkler_matcher_allocations() {
    let tests: Vec<(&str, String, String, f64)> = vec![
        ("long ASCII", "a".repeat(40), format!("{}b", "a".repeat(39)), 0.0),
        ("short term and medium ASCII candidate", "prometheus".to_string(), format!("{}b", "a".repeat(49)), 0.0),
        ("ASCII term and Unicode candidate", "a".repeat(40), format!("{}é", "a".repeat(39)), 1.0),
        ("Unicode term and candidate", format!("{}é", "a".repeat(39)), format!("{}è", "a".repeat(39)), 1.0),
    ];
    for (name, term, candidate, max_allocs) in tests {
        let matcher = JaroWinklerMatcher::new(&term);
        let got = allocs_per_run(1000, || {
            std::hint::black_box(matcher.score(&candidate));
        });
        assert!(got <= max_allocs, "{name}: Score() allocations = {got}, want at most {max_allocs}");
    }
}

#[test]
fn test_jaro_winkler_matcher_concurrent_unicode() {
    const ITERATIONS: usize = 10;
    const WORKERS: usize = 32;
    const TERM: &str = "prometheus";
    let candidate = format!("prométheus{}", "x".repeat(128));
    let want = JaroWinklerMatcher::new(TERM).score(&candidate);

    for _ in 0..ITERATIONS {
        let matcher = Arc::new(JaroWinklerMatcher::new(TERM));
        let start = Arc::new(std::sync::Barrier::new(WORKERS));
        let handles: Vec<_> = (0..WORKERS)
            .map(|_| {
                let (matcher, start, candidate) = (matcher.clone(), start.clone(), candidate.clone());
                std::thread::spawn(move || {
                    start.wait();
                    matcher.score(&candidate)
                })
            })
            .collect();
        for h in handles {
            let score = h.join().expect("worker");
            assert_eq!(score, want, "Score() = {score}, want {want}");
        }
    }
}

/// Go `referenceJaroWinkler`: the pre-pooling implementation, an oracle for the pooled buffers and the ASCII fast
/// path.
fn reference_jaro_winkler(a: &str, b: &str) -> f64 {
    if a == b {
        return 1.0;
    }
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }
    let (mut r1, mut r2): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
    if r1.len() > r2.len() {
        std::mem::swap(&mut r1, &mut r2);
    }
    let (l1, l2) = (r1.len(), r2.len());
    let match_distance = (l2 / 2).saturating_sub(1);
    let mut r1_matches = vec![false; l1];
    let mut r2_matches = vec![false; l2];
    let (mut matches, mut transpositions) = (0.0f64, 0.0f64);
    for i in 0..l1 {
        for j in i.saturating_sub(match_distance)..(i + match_distance + 1).min(l2) {
            if r2_matches[j] || r1[i] != r2[j] {
                continue;
            }
            r1_matches[i] = true;
            r2_matches[j] = true;
            matches += 1.0;
            break;
        }
    }
    if matches == 0.0 {
        return 0.0;
    }
    let mut k = 0;
    for i in 0..l1 {
        if !r1_matches[i] {
            continue;
        }
        while !r2_matches[k] {
            k += 1;
        }
        if r1[i] != r2[k] {
            transpositions += 1.0;
        }
        k += 1;
    }
    let jaro = (matches / l1 as f64 + matches / l2 as f64 + (matches - transpositions / 2.0) / matches) / 3.0;
    let mut prefix_len = 0;
    while prefix_len < 4.min(l1) && r1[prefix_len] == r2[prefix_len] {
        prefix_len += 1;
    }
    jaro + prefix_len as f64 * 0.1 * (1.0 - jaro)
}

/// Go `math/rand/v2.PCG` (128-bit LCG with the DXSM output function) and the `Rand.IntN` it drives.
struct Pcg {
    hi: u64,
    lo: u64,
}

impl Pcg {
    fn new(seed1: u64, seed2: u64) -> Pcg {
        Pcg { hi: seed1, lo: seed2 }
    }

    fn next(&mut self) -> (u64, u64) {
        const MUL_HI: u64 = 2549297995355413924;
        const MUL_LO: u64 = 4865540595714422341;
        const INC_HI: u64 = 6364136223846793005;
        const INC_LO: u64 = 1442695040888963407;
        // state = state * mul + inc
        let state = ((self.hi as u128) << 64) | self.lo as u128;
        let mul = ((MUL_HI as u128) << 64) | MUL_LO as u128;
        let inc = ((INC_HI as u128) << 64) | INC_LO as u128;
        let s = state.wrapping_mul(mul).wrapping_add(inc);
        self.hi = (s >> 64) as u64;
        self.lo = s as u64;
        (self.hi, self.lo)
    }

    fn uint64(&mut self) -> u64 {
        let (mut hi, lo) = self.next();
        const CHEAP_MUL: u64 = 0xda942042e4dd58b5;
        hi ^= hi >> 32;
        hi = hi.wrapping_mul(CHEAP_MUL);
        hi ^= hi >> (3 * 64 / 4);
        hi.wrapping_mul(lo | 1)
    }

    /// Go `Rand.IntN` (`uint64n` on 64-bit platforms).
    fn int_n(&mut self, n: usize) -> usize {
        let n = n as u64;
        if n & (n - 1) == 0 {
            return (self.uint64() & (n - 1)) as usize;
        }
        let mul = |x: u64| -> (u64, u64) {
            let p = x as u128 * n as u128;
            ((p >> 64) as u64, p as u64)
        };
        let (mut hi, mut lo) = mul(self.uint64());
        if lo < n {
            let thresh = n.wrapping_neg() % n;
            while lo < thresh {
                (hi, lo) = mul(self.uint64());
            }
        }
        hi as usize
    }
}

#[test]
fn test_jaro_winkler_matcher_matches_reference() {
    let mut rng = Pcg::new(1, 2);
    // A small alphabet produces plenty of matches and transpositions. The accented runes route some pairs through
    // the rune path.
    let alphabets = ["abcdef_", "abcdef_é"];
    let random_string = |rng: &mut Pcg, alphabet: &[char], n: usize| -> String {
        (0..n).map(|_| alphabet[rng.int_n(alphabet.len())]).collect()
    };
    // One matcher per term scores many candidates, so pooled buffers are reused across calls of different sizes,
    // and lengths straddle the pool threshold.
    for _ in 0..200 {
        let alphabet: Vec<char> = alphabets[rng.int_n(alphabets.len())].chars().collect();
        let n = rng.int_n(100);
        let term = random_string(&mut rng, &alphabet, n);
        let matcher = JaroWinklerMatcher::new(&term);
        for _ in 0..100 {
            let n = rng.int_n(100);
            let candidate = random_string(&mut rng, &alphabet, n);
            let got = matcher.score(&candidate);
            let want = reference_jaro_winkler(&term, &candidate);
            assert!((got - want).abs() <= 1e-12, "NewJaroWinklerMatcher({term:?}).Score({candidate:?}) = {got}, want {want}");
        }
    }
}
