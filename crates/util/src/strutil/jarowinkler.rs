//! Port of `util/strutil/jarowinkler.go`: Jaro-Winkler similarity of a fixed term against many candidates.

use std::cell::RefCell;
use std::sync::OnceLock;

/// Go `jaroWinklerPoolThreshold`: the largest match buffer, in flags, that is kept on the stack; longer inputs
/// use the reusable workspace.
const JARO_WINKLER_POOL_THRESHOLD: usize = 32;

/// Go `jaroWinklerMaxPooledCapacity`: unusually large inputs do not keep a correspondingly large buffer.
const JARO_WINKLER_MAX_POOLED_CAPACITY: usize = 64 << 10;

/// Go `jaroWinklerWorkspace`: reusable match flags.
#[derive(Debug, Default)]
pub struct JaroWinklerWorkspace {
    pub matches: Vec<bool>,
}

impl JaroWinklerWorkspace {
    /// Go `matchSlices`: cleared match buffers of lengths `l1` and `l2`, reusing the workspace's storage when it
    /// is large enough.
    pub fn match_slices(&mut self, l1: usize, l2: usize) -> (&mut [bool], &mut [bool]) {
        let total = l1 + l2;
        if self.matches.capacity() < total {
            self.matches = vec![false; total];
        } else {
            self.matches.clear();
            self.matches.resize(total, false);
        }
        self.matches.split_at_mut(l1)
    }
}

// Go keeps workspaces in a sync.Pool; a per-thread workspace gives the same reuse without locking.
thread_local! {
    static WORKSPACE: RefCell<JaroWinklerWorkspace> = RefCell::new(JaroWinklerWorkspace::default());
}

/// Go `JaroWinklerMatcher`: records a fixed search term so that it can be scored against many candidates
/// without repeating work on the term. Safe for concurrent use.
#[derive(Debug)]
pub struct JaroWinklerMatcher {
    /// The original term; used directly on the ASCII path.
    term: String,
    /// Whether the term is pure ASCII.
    term_ascii: bool,
    /// The term's characters for the Unicode path (set lazily for an ASCII term).
    term_runes: OnceLock<Vec<char>>,
}

impl JaroWinklerMatcher {
    /// Go `NewJaroWinklerMatcher`.
    pub fn new(term: &str) -> JaroWinklerMatcher {
        if term.is_ascii() {
            return JaroWinklerMatcher { term: term.to_string(), term_ascii: true, term_runes: OnceLock::new() };
        }
        JaroWinklerMatcher { term: term.to_string(), term_ascii: false, term_runes: OnceLock::from(term.chars().collect::<Vec<_>>()) }
    }

    /// Go `Score`: the Jaro-Winkler similarity between the term and `s`, in [0.0, 1.0] where 1.0 means
    /// identical strings.
    pub fn score(&self, s: &str) -> f64 {
        if self.term == s {
            return 1.0;
        }
        if self.term.is_empty() || s.is_empty() {
            return 0.0;
        }
        if self.term_ascii && s.is_ascii() {
            return jaro_winkler(self.term.as_bytes(), s.as_bytes());
        }
        // Either the term or s is Unicode; use the rune path.
        let term_runes = self.term_runes.get_or_init(|| self.term.chars().collect());
        // Go `[]rune(s)`: one allocation of the exact size.
        let mut s_runes: Vec<char> = Vec::with_capacity(s.chars().count());
        s_runes.extend(s.chars());
        jaro_winkler(term_runes, &s_runes)
    }
}

/// Go `jaroWinklerString` and `jaroWinklerRunes` (the same algorithm on bytes of ASCII strings or on runes).
fn jaro_winkler<'a, T: PartialEq + Copy>(mut s1: &'a [T], mut s2: &'a [T]) -> f64 {
    let (mut l1, mut l2) = (s1.len(), s2.len());
    // Swap so s1 is always the shorter input.
    if l1 > l2 {
        std::mem::swap(&mut s1, &mut s2);
        std::mem::swap(&mut l1, &mut l2);
    }
    if l2 > JARO_WINKLER_POOL_THRESHOLD {
        // l2 is the longer input.
        WORKSPACE.with(|w| {
            let mut w = w.borrow_mut();
            let (m1, m2) = w.match_slices(l1, l2);
            let r = jaro_winkler_with(s1, s2, m1, m2);
            if w.matches.capacity() > JARO_WINKLER_MAX_POOLED_CAPACITY {
                *w = JaroWinklerWorkspace::default();
            }
            r
        })
    } else {
        let mut buf = [false; 2 * JARO_WINKLER_POOL_THRESHOLD];
        let (m1, rest) = buf.split_at_mut(l1);
        jaro_winkler_with(s1, s2, m1, &mut rest[..l2])
    }
}

fn jaro_winkler_with<T: PartialEq + Copy>(s1: &[T], s2: &[T], s1_matches: &mut [bool], s2_matches: &mut [bool]) -> f64 {
    let (l1, l2) = (s1.len(), s2.len());
    // Jaro match distance: characters must be within this many positions to match.
    let match_distance = (l2 / 2).saturating_sub(1);

    let mut matches = 0.0f64;
    let mut transpositions = 0.0f64;

    for i in 0..l1 {
        let start = i.saturating_sub(match_distance);
        let end = (i + match_distance + 1).min(l2);
        for j in start..end {
            if s2_matches[j] || s1[i] != s2[j] {
                continue;
            }
            s1_matches[i] = true;
            s2_matches[j] = true;
            matches += 1.0;
            break;
        }
    }

    if matches == 0.0 {
        return 0.0;
    }

    let mut k = 0;
    for i in 0..l1 {
        if !s1_matches[i] {
            continue;
        }
        while !s2_matches[k] {
            k += 1;
        }
        if s1[i] != s2[k] {
            transpositions += 1.0;
        }
        k += 1;
    }

    // Use precomputed reciprocals to replace repeated divisions.
    let inv_l1 = 1.0 / l1 as f64;
    let inv_l2 = 1.0 / l2 as f64;
    let jaro = (matches * inv_l1 + matches * inv_l2 + (matches - transpositions * 0.5) / matches) / 3.0;

    // Winkler modification: boost for common prefix up to 4 characters.
    let mut prefix_len = 0;
    while prefix_len < 4.min(l1).min(l2) && s1[prefix_len] == s2[prefix_len] {
        prefix_len += 1;
    }

    // Standard Winkler prefix scaling factor; not intended to be user-configurable.
    const P: f64 = 0.1;
    jaro + prefix_len as f64 * P * (1.0 - jaro)
}
