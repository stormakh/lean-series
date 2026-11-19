//! Port of `web/api/v1/search_filters.go`: the filters that decide which values a search returns and their scores.
//!
//! Filters expect the query passed to their constructor and the value passed to `accept` to share the same case.
//! To search case-insensitively, lowercase the query at construction time and wrap the resulting filter with
//! [`CaseFoldingFilter`] so incoming values are lowercased once for the whole chain.
//!
//! The Jaro-Winkler and subsequence matchers of Go's `util/strutil` are ported at the end of this file, because the
//! `util` crate does not have `strutil` yet.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use storage::Filter;

/// Case-sensitive substring matching with a position-based score (Go `SubstringFilter`). A prefix match scores
/// 1.0; substring matches score in [0.1, 1.0), earlier match positions score higher.
pub struct SubstringFilter {
    query: String,
}

impl SubstringFilter {
    /// Go `NewSubstringFilter`.
    pub fn new(query: &str) -> SubstringFilter {
        SubstringFilter { query: query.to_string() }
    }
}

impl Filter for SubstringFilter {
    /// The position is counted in characters (runes), not bytes, with an ASCII fast path.
    fn accept(&self, value: &str) -> (bool, f64) {
        if self.query.is_empty() {
            return (true, 1.0);
        }
        let Some(idx) = value.find(self.query.as_str()) else {
            return (false, 0.0);
        };
        if idx == 0 {
            return (true, 1.0);
        }
        let (pos, max_pos) = if self.query.is_ascii() && value.is_ascii() {
            (idx as i64, value.len() as i64 - self.query.len() as i64)
        } else {
            (
                value[..idx].chars().count() as i64,
                value.chars().count() as i64 - self.query.chars().count() as i64,
            )
        };
        if max_pos <= 0 {
            return (true, 1.0);
        }
        (true, 1.0 - 0.9 * pos as f64 / max_pos as f64)
    }
}

/// Jaro-Winkler fuzzy matching against a query (Go `FuzzyFilter`).
pub struct FuzzyFilter {
    matcher: JaroWinklerMatcher,
    threshold: f64,
}

impl FuzzyFilter {
    /// Go `NewFuzzyFilter`. `threshold` is in [0.0, 1.0]; 1.0 requires a perfect match.
    pub fn new(query: &str, threshold: f64) -> FuzzyFilter {
        FuzzyFilter { matcher: JaroWinklerMatcher::new(query), threshold }
    }
}

impl Filter for FuzzyFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        let score = self.matcher.score(value);
        (score >= self.threshold, score)
    }
}

/// Fuzzy matching that requires all query characters in order (Go `SubsequenceFilter`); consecutive runs score
/// higher, gaps lower. Prefix matches always score 1.0.
pub struct SubsequenceFilter {
    query: String,
    matcher: SubsequenceMatcher,
    threshold: f64,
}

impl SubsequenceFilter {
    /// Go `NewSubsequenceFilter`. `threshold` is in [0.0, 1.0]; 0.0 accepts any subsequence match.
    pub fn new(query: &str, threshold: f64) -> SubsequenceFilter {
        SubsequenceFilter { query: query.to_string(), matcher: SubsequenceMatcher::new(query), threshold }
    }
}

impl Filter for SubsequenceFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        if value.starts_with(self.query.as_str()) {
            return (true, 1.0);
        }
        let score = self.matcher.score(value);
        // A score of 0 means no subsequence match; always reject regardless of threshold.
        (score > 0.0 && score >= self.threshold, score)
    }
}

/// Lowercases the value once before delegating (Go `caseFoldingFilter`). The inner filter must have been built
/// with a lowercased query.
pub struct CaseFoldingFilter {
    inner: Arc<dyn Filter>,
}

impl CaseFoldingFilter {
    /// Go `newCaseFoldingFilter`.
    pub fn new(inner: Arc<dyn Filter>) -> CaseFoldingFilter {
        CaseFoldingFilter { inner }
    }
}

impl Filter for CaseFoldingFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        self.inner.accept(&go_to_lower(value))
    }
}

/// Go `strings.ToLower`: maps every rune with `unicode.ToLower` (one rune each, no context rules).
pub fn go_to_lower(s: &str) -> String {
    if s.is_ascii() {
        return s.to_ascii_lowercase();
    }
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c == '\u{130}' {
            // Go maps U+0130 (I with dot above) to 'i'; Rust's full mapping adds a combining dot.
            out.push('i');
            continue;
        }
        let mut l = c.to_lowercase();
        match (l.next(), l.next()) {
            (Some(a), None) => out.push(a),
            _ => out.push(c),
        }
    }
    out
}

/// A cached filter result (Go `memoEntry`).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct MemoEntry {
    pub accepted: bool,
    pub score: f64,
}

/// Go `memoizingFilterMaxEntries`.
pub const MEMOIZING_FILTER_MAX_ENTRIES: usize = 100_000;
/// Go `memoizingFilterMaxBytes` (10 MiB).
pub const MEMOIZING_FILTER_MAX_BYTES: usize = 10 << 20;

struct MemoState {
    cache: HashMap<String, MemoEntry>,
    bytes: usize,
}

/// Caches the result of the inner filter per distinct value (Go `memoizingFilter`), up to
/// [`MEMOIZING_FILTER_MAX_ENTRIES`] entries and [`MEMOIZING_FILTER_MAX_BYTES`] key bytes. Past either cap the inner
/// filter is called without caching. (Go's is not safe for concurrent use; this one takes an uncontended lock.)
pub struct MemoizingFilter {
    inner: Arc<dyn Filter>,
    state: Mutex<MemoState>,
}

impl MemoizingFilter {
    /// Go `newMemoizingFilter`.
    pub fn new(inner: Arc<dyn Filter>) -> MemoizingFilter {
        MemoizingFilter { inner, state: Mutex::new(MemoState { cache: HashMap::new(), bytes: 0 }) }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MemoState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The number of cached values (Go `len(f.cache)`).
    pub fn cache_len(&self) -> usize {
        self.lock().cache.len()
    }

    /// Whether `value` is cached.
    pub fn cache_contains(&self, value: &str) -> bool {
        self.lock().cache.contains_key(value)
    }

    /// The sum of the cached key lengths (Go `f.bytes`).
    pub fn bytes(&self) -> usize {
        self.lock().bytes
    }
}

impl Filter for MemoizingFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        if let Some(e) = self.lock().cache.get(value) {
            return (e.accepted, e.score);
        }
        let (accepted, score) = self.inner.accept(value);
        let mut st = self.lock();
        if st.cache.len() < MEMOIZING_FILTER_MAX_ENTRIES && st.bytes + value.len() <= MEMOIZING_FILTER_MAX_BYTES {
            if st.cache.insert(value.to_string(), MemoEntry { accepted, score }).is_none() {
                st.bytes += value.len();
            }
        }
        (accepted, score)
    }
}

/// AND of filters; the score is the best score of the filters (Go `ChainFilter`).
pub struct ChainFilter {
    filters: Vec<Arc<dyn Filter>>,
}

impl ChainFilter {
    /// Go `NewChainFilter`.
    pub fn new(filters: Vec<Arc<dyn Filter>>) -> ChainFilter {
        ChainFilter { filters }
    }
}

impl Filter for ChainFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        if self.filters.is_empty() {
            return (true, 1.0);
        }
        let mut max_score = 0.0;
        for f in &self.filters {
            let (accepted, score) = f.accept(value);
            if !accepted {
                return (false, 0.0);
            }
            if score > max_score {
                max_score = score;
            }
        }
        (true, max_score)
    }
}

/// OR of per-term filters; the score is the best accepting score (Go `orSearchesFilter`). Stops at a perfect 1.0.
pub struct OrSearchesFilter {
    filters: Vec<Arc<dyn Filter>>,
}

impl OrSearchesFilter {
    /// Go `newOrSearchesFilter`.
    pub fn new(filters: Vec<Arc<dyn Filter>>) -> OrSearchesFilter {
        OrSearchesFilter { filters }
    }
}

impl Filter for OrSearchesFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        let mut max_score = 0.0;
        let mut accepted = false;
        for f in &self.filters {
            let (ok, score) = f.accept(value);
            if !ok {
                continue;
            }
            accepted = true;
            if score > max_score {
                max_score = score;
            }
            if max_score >= 1.0 {
                return (true, max_score);
            }
        }
        (accepted, max_score)
    }
}

/// Substring, then fuzzy when the substring does not match (Go `orFilter`).
pub struct OrFilter {
    pub substring_filter: Option<SubstringFilter>,
    pub fuzzy_filter: Option<FuzzyFilter>,
}

impl Filter for OrFilter {
    fn accept(&self, value: &str) -> (bool, f64) {
        if self.substring_filter.is_none() && self.fuzzy_filter.is_none() {
            return (true, 1.0);
        }
        if let Some(s) = &self.substring_filter {
            let (accepted, score) = s.accept(value);
            if accepted {
                return (true, score);
            }
        }
        if let Some(f) = &self.fuzzy_filter {
            return f.accept(value);
        }
        (false, 0.0)
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Port of util/strutil/jarowinkler.go and subsequence.go.

/// Scores candidates against a fixed term with Jaro-Winkler similarity (Go `strutil.JaroWinklerMatcher`). Safe
/// for concurrent use.
pub struct JaroWinklerMatcher {
    term: String,
    term_ascii: bool,
    term_runes: Vec<char>,
}

/// Inputs up to this many match flags use a stack buffer.
const JW_STACK_FLAGS: usize = 64;

impl JaroWinklerMatcher {
    /// Go `NewJaroWinklerMatcher`.
    pub fn new(term: &str) -> JaroWinklerMatcher {
        let term_ascii = term.is_ascii();
        JaroWinklerMatcher {
            term: term.to_string(),
            term_ascii,
            term_runes: if term_ascii { Vec::new() } else { term.chars().collect() },
        }
    }

    /// The similarity of the term and `s` in [0.0, 1.0]; 1.0 for identical strings.
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
        let s_runes: Vec<char> = s.chars().collect();
        if self.term_ascii {
            let t: Vec<char> = self.term.chars().collect();
            return jaro_winkler(&t, &s_runes);
        }
        jaro_winkler(&self.term_runes, &s_runes)
    }
}

fn jaro_winkler<T: PartialEq>(a: &[T], b: &[T]) -> f64 {
    let (s1, s2) = if a.len() > b.len() { (b, a) } else { (a, b) };
    let (l1, l2) = (s1.len(), s2.len());
    let match_distance = (l2 / 2).saturating_sub(1);

    let mut stack = [false; JW_STACK_FLAGS];
    let mut heap = Vec::new();
    let flags: &mut [bool] = if l1 + l2 <= JW_STACK_FLAGS {
        &mut stack[..l1 + l2]
    } else {
        heap.resize(l1 + l2, false);
        &mut heap
    };
    let (s1m, s2m) = flags.split_at_mut(l1);

    let mut matches = 0.0f64;
    let mut transpositions = 0.0f64;
    for i in 0..l1 {
        let start = i.saturating_sub(match_distance);
        let end = (i + match_distance + 1).min(l2);
        for j in start..end {
            if s2m[j] || s1[i] != s2[j] {
                continue;
            }
            s1m[i] = true;
            s2m[j] = true;
            matches += 1.0;
            break;
        }
    }
    if matches == 0.0 {
        return 0.0;
    }
    let mut k = 0;
    for i in 0..l1 {
        if !s1m[i] {
            continue;
        }
        while !s2m[k] {
            k += 1;
        }
        if s1[i] != s2[k] {
            transpositions += 1.0;
        }
        k += 1;
    }
    let inv_l1 = 1.0 / l1 as f64;
    let inv_l2 = 1.0 / l2 as f64;
    let jaro = (matches * inv_l1 + matches * inv_l2 + (matches - transpositions * 0.5) / matches) / 3.0;

    let mut prefix_len = 0;
    while prefix_len < 4.min(l1).min(l2) && s1[prefix_len] == s2[prefix_len] {
        prefix_len += 1;
    }
    const P: f64 = 0.1;
    jaro + prefix_len as f64 * P * (1.0 - jaro)
}

/// Go `subsequenceNonExactScoreScale`.
const SUBSEQUENCE_NON_EXACT_SCORE_SCALE: f64 = 0.999;

/// Scores candidates against a fixed pattern by subsequence matching (Go `strutil.SubsequenceMatcher`).
pub struct SubsequenceMatcher {
    pattern: String,
    pattern_ascii: bool,
    pattern_runes: Vec<char>,
}

impl SubsequenceMatcher {
    /// Go `NewSubsequenceMatcher`.
    pub fn new(pattern: &str) -> SubsequenceMatcher {
        SubsequenceMatcher {
            pattern: pattern.to_string(),
            pattern_ascii: pattern.is_ascii(),
            pattern_runes: pattern.chars().collect(),
        }
    }

    /// The score in [0.0, 1.0]: 1.0 only for an exact match, 0.0 when the pattern is not a subsequence of `text`.
    /// Raw score: Σ(interval_size²) − Σ(gap_size / text_length) − trailing_gap / (2 * text_length), normalized by
    /// pattern_length² and scaled below 1.0.
    pub fn score(&self, text: &str) -> f64 {
        if self.pattern.is_empty() {
            return 1.0;
        }
        if text.is_empty() {
            return 0.0;
        }
        if self.pattern == text {
            return 1.0;
        }
        if self.pattern.len() > text.len() {
            return 0.0;
        }
        let text_ascii = text.is_ascii();
        match (self.pattern_ascii, text_ascii) {
            (true, true) => return match_subsequence_string(self.pattern.as_bytes(), text.as_bytes()),
            (false, true) => return 0.0,
            _ => {}
        }
        let t: Vec<char> = text.chars().collect();
        match_subsequence_runes(&self.pattern_runes, &t)
    }
}

fn index_byte(s: &[u8], c: u8) -> Option<usize> {
    s.iter().position(|&b| b == c)
}

fn match_subsequence_string(pattern: &[u8], text: &[u8]) -> f64 {
    let pattern_len = pattern.len();
    let text_len = text.len();
    let inv_text_len = 1.0 / text_len as f64;
    let max_start = text_len - pattern_len;

    let score_from = |start_pos: usize| -> Option<f64> {
        let mut i = start_pos;
        let mut from = i;
        let mut to = i;
        let mut pattern_idx = 1;
        i += 1;
        while pattern_idx < pattern_len && i < text_len && text[i] == pattern[pattern_idx] {
            to = i;
            pattern_idx += 1;
            i += 1;
        }
        let mut score = 0.0f64;
        if from > 0 {
            score -= from as f64 * inv_text_len;
        }
        let mut size = to - from + 1;
        score += (size * size) as f64;
        let mut prev_to = to;

        while pattern_idx < pattern_len {
            let j = index_byte(&text[i..], pattern[pattern_idx])?;
            i += j;
            from = i;
            to = i;
            pattern_idx += 1;
            i += 1;
            while pattern_idx < pattern_len && i < text_len && text[i] == pattern[pattern_idx] {
                to = i;
                pattern_idx += 1;
                i += 1;
            }
            let gap = from as i64 - prev_to as i64 - 1;
            if gap > 0 {
                score -= gap as f64 * inv_text_len;
            }
            size = to - from + 1;
            score += (size * size) as f64;
            prev_to = to;
        }
        let trailing = text_len as i64 - 1 - prev_to as i64;
        if trailing > 0 {
            score -= trailing as f64 * inv_text_len * 0.5;
        }
        Some(score)
    };

    let mut best = -1.0f64;
    let mut i = 0;
    while i <= max_start {
        let Some(j) = index_byte(&text[i..max_start + 1], pattern[0]) else { break };
        i += j;
        let Some(s) = score_from(i) else {
            // No later start can complete the pattern either.
            break;
        };
        if s > best {
            best = s;
        }
        i += 1;
    }
    if best < 0.0 {
        return 0.0;
    }
    normalize_subsequence_score(best, pattern_len)
}

fn match_subsequence_runes(pattern: &[char], text: &[char]) -> f64 {
    let pattern_len = pattern.len();
    let text_len = text.len();
    let inv_text_len = 1.0 / text_len as f64;

    let match_from_pos = |start_pos: usize| -> Option<f64> {
        let mut pattern_idx = 0;
        let mut i = start_pos;
        let mut score = 0.0f64;
        let mut prev_to: i64 = -1;
        while i < text_len && pattern_idx < pattern_len {
            if text[i] == pattern[pattern_idx] {
                let from = i;
                let mut to = i;
                pattern_idx += 1;
                i += 1;
                while i < text_len && pattern_idx < pattern_len && text[i] == pattern[pattern_idx] {
                    to = i;
                    pattern_idx += 1;
                    i += 1;
                }
                let gap = if prev_to < 0 { from as i64 } else { from as i64 - prev_to - 1 };
                if gap > 0 {
                    score -= gap as f64 * inv_text_len;
                }
                let size = to - from + 1;
                score += (size * size) as f64;
                prev_to = to as i64;
            } else {
                i += 1;
            }
        }
        if pattern_idx < pattern_len {
            return None;
        }
        let trailing = text_len as i64 - 1 - prev_to;
        if trailing > 0 {
            score -= trailing as f64 * inv_text_len * 0.5;
        }
        Some(score)
    };

    let mut best = -1.0f64;
    if text_len >= pattern_len {
        for i in 0..=text_len - pattern_len {
            if text[i] != pattern[0] {
                continue;
            }
            let Some(s) = match_from_pos(i) else { break };
            if s > best {
                best = s;
            }
        }
    }
    if best < 0.0 {
        return 0.0;
    }
    normalize_subsequence_score(best, pattern_len)
}

fn normalize_subsequence_score(raw: f64, pattern_len: usize) -> f64 {
    raw / (pattern_len * pattern_len) as f64 * SUBSEQUENCE_NON_EXACT_SCORE_SCALE
}
