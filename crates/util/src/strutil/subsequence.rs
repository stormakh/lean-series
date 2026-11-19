//! Port of `util/strutil/subsequence.go`: a fuzzy subsequence scorer for autocomplete ranking.
//!
//! The scoring algorithm is inspired by two JavaScript libraries: https://github.com/Nexucis/fuzzy (MIT
//! License), used by the Prometheus UI, which itself was inspired by https://github.com/mattyork/fuzzy (MIT
//! License).

/// Non-exact matches are scaled below 1.0 so rounded scores stay distinguishable from exact matches.
const SUBSEQUENCE_NON_EXACT_SCORE_SCALE: f64 = 0.999;

/// Go `SubsequenceMatcher`: pre-computes the encoding of a fixed search pattern so that it can be scored against
/// many candidates. The first `score` call with a Unicode candidate caches the pattern's characters.
#[derive(Debug)]
pub struct SubsequenceMatcher {
    pattern: String,
    /// Whether the pattern is pure ASCII.
    pattern_ascii: bool,
    /// The pattern's characters; set when the pattern is not ASCII or on the first Unicode text.
    pattern_runes: Option<Vec<char>>,
}

impl SubsequenceMatcher {
    /// Go `NewSubsequenceMatcher`.
    pub fn new(pattern: &str) -> SubsequenceMatcher {
        if pattern.is_ascii() {
            return SubsequenceMatcher { pattern: pattern.to_string(), pattern_ascii: true, pattern_runes: None };
        }
        SubsequenceMatcher { pattern: pattern.to_string(), pattern_ascii: false, pattern_runes: Some(pattern.chars().collect()) }
    }

    /// Go `Score`: a fuzzy match score between the pattern and `text` with a greedy character matching
    /// algorithm. The characters of the pattern must appear in the text in order (subsequence matching). The
    /// score is in [0.0, 1.0]: 1.0 only for an exact match, 0.0 when the pattern is not a subsequence of the
    /// text; intermediate values reward consecutive matches and penalize gaps.
    ///
    /// The raw scoring formula is: Σ(interval_size²) − Σ(gap_size / text_length) − trailing_gap / (2 *
    /// text_length). The result is normalized by pattern_length² and scaled below 1.0 for non-exact matches.
    pub fn score(&mut self, text: &str) -> f64 {
        if self.pattern.is_empty() {
            return 1.0;
        }
        if text.is_empty() {
            return 0.0;
        }
        // Exact match: perfect score, checked before any allocation.
        if self.pattern == text {
            return 1.0;
        }
        // Byte length >= rune count, so this is a safe early exit before any allocation.
        if self.pattern.len() > text.len() {
            return 0.0;
        }
        // If the pattern has non-ASCII characters but the text is pure ASCII, no non-ASCII pattern character
        // can ever match, so the pattern cannot be a subsequence.
        let text_ascii = text.is_ascii();
        match (self.pattern_ascii, text_ascii) {
            (true, true) => return match_subsequence_string(self.pattern.as_bytes(), text.as_bytes()),
            (false, true) => return 0.0,
            _ => {}
        }
        let pattern = self.pattern_runes.get_or_insert_with(|| self.pattern.chars().collect());
        let text: Vec<char> = text.chars().collect();
        match_subsequence_runes(pattern, &text)
    }
}

/// Go `matchSubsequenceString`: the scoring algorithm on ASCII bytes, with divisions by the text length
/// replaced by a precomputed reciprocal multiply.
fn match_subsequence_string(pattern: &[u8], text: &[u8]) -> f64 {
    let pattern_len = pattern.len();
    let text_len = text.len();
    let inv_text_len = 1.0 / text_len as f64;
    let max_start = text_len - pattern_len;

    // Scores a match starting at start_pos, where text[start_pos] == pattern[0].
    let score_from = |start_pos: usize| -> Option<f64> {
        let mut i = start_pos;
        let mut from = i;
        let mut to = i;
        let mut pattern_idx = 1;
        i += 1;
        // Extend the initial consecutive run.
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
            // Jump to the next occurrence of pattern[pattern_idx].
            let j = text[i..].iter().position(|&c| c == pattern[pattern_idx])?;
            i += j;
            from = i;
            to = i;
            pattern_idx += 1;
            i += 1;
            // Extend the consecutive run.
            while pattern_idx < pattern_len && i < text_len && text[i] == pattern[pattern_idx] {
                to = i;
                pattern_idx += 1;
                i += 1;
            }
            let gap = from - prev_to - 1;
            if gap > 0 {
                score -= gap as f64 * inv_text_len;
            }
            size = to - from + 1;
            score += (size * size) as f64;
            prev_to = to;
        }

        // Penalise unmatched trailing characters at half the leading/inner rate.
        let trailing = text_len - 1 - prev_to;
        if trailing > 0 {
            score -= trailing as f64 * inv_text_len * 0.5;
        }
        Some(score)
    };

    let mut best_score = -1.0f64;
    let mut i = 0;
    while i <= max_start {
        // Scan for the first pattern character.
        let Some(j) = text[i..max_start + 1].iter().position(|&c| c == pattern[0]) else { break };
        i += j;
        let Some(s) = score_from(i) else {
            // If the pattern cannot be completed from i, no later start can succeed: text[i+1:] is a strict
            // subset of text[i:].
            break;
        };
        if s > best_score {
            best_score = s;
        }
        i += 1;
    }

    if best_score < 0.0 {
        return 0.0;
    }
    normalize_subsequence_score(best_score, pattern_len)
}

/// Go `matchSubsequenceRunes`: the scoring algorithm on characters for the Unicode path.
fn match_subsequence_runes(pattern: &[char], text: &[char]) -> f64 {
    let pattern_len = pattern.len();
    let text_len = text.len();
    let inv_text_len = 1.0 / text_len as f64;

    // Tries to match all pattern characters as a subsequence of text starting at start_pos; the raw score, or
    // None if the pattern cannot be fully matched.
    let match_from_pos = |start_pos: usize| -> Option<f64> {
        let mut pattern_idx = 0;
        let mut i = start_pos;
        let mut score = 0.0f64;
        let mut prev_to: isize = -1;

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
                let gap_size = if prev_to < 0 { from as isize } else { from as isize - prev_to - 1 };
                if gap_size > 0 {
                    score -= gap_size as f64 * inv_text_len;
                }
                let size = to - from + 1;
                score += (size * size) as f64;
                prev_to = to as isize;
            } else {
                i += 1;
            }
        }

        if pattern_idx < pattern_len {
            return None;
        }

        // Penalize unmatched trailing characters at half the leading/inner gap rate.
        let trailing_gap = text_len as isize - 1 - prev_to;
        if trailing_gap > 0 {
            score -= trailing_gap as f64 * inv_text_len * 0.5;
        }
        Some(score)
    };

    let mut best_score = -1.0f64;
    // Only iterate while there are enough characters left for the pattern to fit.
    if text_len >= pattern_len {
        let max_start = text_len - pattern_len;
        for i in 0..=max_start {
            if text[i] != pattern[0] {
                continue;
            }
            let Some(s) = match_from_pos(i) else {
                // If matching fails from this position, no later position can succeed since the remaining text is
                // a strict subset.
                break;
            };
            if s > best_score {
                best_score = s;
            }
        }
    }

    if best_score < 0.0 {
        return 0.0;
    }
    normalize_subsequence_score(best_score, pattern_len)
}

fn normalize_subsequence_score(raw_score: f64, pattern_len: usize) -> f64 {
    let score = raw_score / (pattern_len * pattern_len) as f64;
    score * SUBSEQUENCE_NON_EXACT_SCORE_SCALE
}
