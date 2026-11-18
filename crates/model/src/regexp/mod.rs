//! Port of `model/labels/regexp.go`: `FastRegexMatcher`, which avoids running the regex engine for common label
//! matcher patterns (literals, alternations of literals, prefixes, suffixes, `.*foo.*`, ...), plus the Go-compatible
//! regex engine it falls back to (`Regexp`) and the Go syntax parser (`syntax`).
//!
//! The optimizations inspect the Go syntax tree, so they choose exactly what Go chooses.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

mod compile;
pub mod syntax;

pub use compile::{quote_meta, Error, Regexp};
use syntax::Op;

use crate::unicode;

/// Maximum number of set matches extracted from a regexp.
pub const MAX_SET_MATCHES: usize = 256;

/// Minimum number of alternate values for which a map is used instead of a list.
pub const MIN_EQUAL_MULTI_STRING_MATCHER_MAP_THRESHOLD: usize = 16;

/// A regex matcher (anchored at both ends, `.` matches newlines) with fast paths for common patterns.
#[derive(Clone, Debug)]
pub struct FastRegexMatcher {
    re_string: String,
    re: Option<Regexp>,
    set_matches: Vec<String>,
    string_matcher: Option<StringMatcher>,
    prefix: String,
    suffix: String,
    contains: Vec<String>,
    case_insensitive_prefix: bool,
    kind: MatchKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MatchKind {
    /// Only the string matcher.
    StringMatcher,
    /// Equality with the single set match.
    Equal,
    /// Case-insensitive prefix check, then the regex.
    CaseInsensitivePrefix,
    /// Prefix, suffix and contains checks, then the string matcher or the regex.
    General,
}

impl FastRegexMatcher {
    /// Go `NewFastRegexMatcher`.
    pub fn new(v: &str) -> Result<FastRegexMatcher, Error> {
        let mut m = FastRegexMatcher {
            re_string: v.to_string(),
            re: None,
            set_matches: Vec::new(),
            string_matcher: None,
            prefix: String::new(),
            suffix: String::new(),
            contains: Vec::new(),
            case_insensitive_prefix: false,
            kind: MatchKind::StringMatcher,
        };
        let (sm, set_matches) = optimize_alternating_literals(v);
        m.set_matches = set_matches;
        if sm.is_some() {
            m.string_matcher = sm;
            m.kind = MatchKind::StringMatcher;
            return Ok(m);
        }
        let parsed = syntax::parse(v, syntax::PERL | syntax::DOT_NL)?;
        let mut parsed = optimize_alternating_simple_contains(parsed);

        // Go compiles "^(?s:" + parsed.String() + ")$".
        let mut wrapped = syntax::Regexp::new(Op::Concat);
        let mut begin = syntax::Regexp::new(Op::BeginText);
        begin.flags = syntax::PERL;
        let mut end = syntax::Regexp::new(Op::EndText);
        end.flags = syntax::PERL | syntax::WAS_DOLLAR;
        wrapped.sub = vec![begin, parsed.clone(), end];
        m.re = Some(Regexp::from_tree(format!("^(?s:{})$", v), &wrapped)?);

        clear_capture(&mut parsed);
        if parsed.op == Op::Concat {
            let (cip, prefix, suffix, contains) = optimize_concat_regex(&mut parsed);
            m.case_insensitive_prefix = cip;
            m.prefix = prefix;
            m.suffix = suffix;
            m.contains = contains;
        }
        let (matches, case_sensitive) = find_set_matches(&mut parsed);
        if !matches.is_empty() {
            if case_sensitive {
                m.set_matches = matches.clone();
            }
            if matches.len() > 1 {
                let mut emsm = new_equal_multi_string_matcher(case_sensitive, matches.len(), 0, 0);
                for s in &matches {
                    emsm.multi_add(s);
                }
                m.string_matcher = Some(emsm);
            }
        }
        if m.string_matcher.is_none() {
            m.string_matcher = if is_simple_concatenation_pattern(&parsed) {
                Some(StringMatcher::True)
            } else {
                string_matcher_from_regexp(&mut parsed)
            };
        }
        m.kind = m.compile_match_string_function();
        Ok(m)
    }

    fn compile_match_string_function(&self) -> MatchKind {
        if self.set_matches.len() == 1 {
            return MatchKind::Equal;
        }
        if self.prefix.is_empty() && self.suffix.is_empty() && self.contains.is_empty() && self.string_matcher.is_some()
        {
            return MatchKind::StringMatcher;
        }
        if self.case_insensitive_prefix && !self.prefix.is_empty() {
            return MatchKind::CaseInsensitivePrefix;
        }
        MatchKind::General
    }

    /// Whether `s` matches the whole regexp.
    #[inline]
    pub fn match_string(&self, s: &str) -> bool {
        match self.kind {
            MatchKind::StringMatcher => self.string_matcher.as_ref().is_some_and(|m| m.matches(s)),
            MatchKind::Equal => s == self.set_matches[0],
            MatchKind::CaseInsensitivePrefix => {
                has_prefix_case_insensitive(s.as_bytes(), self.prefix.as_bytes())
                    && self.re.as_ref().is_some_and(|re| re.is_match(s))
            }
            MatchKind::General => {
                if !self.prefix.is_empty() && !s.starts_with(self.prefix.as_str()) {
                    return false;
                }
                if !self.suffix.is_empty() && !s.ends_with(self.suffix.as_str()) {
                    return false;
                }
                if !self.contains.is_empty() && !contains_in_order(s, &self.contains) {
                    return false;
                }
                if let Some(sm) = &self.string_matcher {
                    return sm.matches(s);
                }
                self.re.as_ref().is_some_and(|re| re.is_match(s))
            }
        }
    }

    /// Whether any fast-path optimization applies.
    pub fn is_optimized(&self) -> bool {
        !self.set_matches.is_empty()
            || self.string_matcher.is_some()
            || !self.prefix.is_empty()
            || !self.suffix.is_empty()
            || !self.contains.is_empty()
    }

    /// The values this regexp matches exactly, if it only matches a known set (a copy).
    pub fn set_matches(&self) -> Vec<String> {
        self.set_matches.clone()
    }

    /// The original regexp string.
    pub fn get_regex_string(&self) -> &str {
        &self.re_string
    }

    /// The required literal prefix ("" when none).
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Whether the prefix is matched case-insensitively.
    pub fn case_insensitive_prefix(&self) -> bool {
        self.case_insensitive_prefix
    }

    /// The required literal suffix ("" when none).
    pub fn suffix(&self) -> &str {
        &self.suffix
    }

    /// Literals that must appear in order.
    pub fn contains(&self) -> &[String] {
        &self.contains
    }

    /// The string matcher in use, if any.
    pub fn string_matcher(&self) -> Option<&StringMatcher> {
        self.string_matcher.as_ref()
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Set matches.

fn rune_char(r: i32) -> char {
    char::from_u32(r as u32).unwrap_or('\u{FFFD}')
}

fn runes_string(rs: &[i32]) -> String {
    rs.iter().map(|&r| rune_char(r)).collect()
}

/// Extracts the equality matches of a regexp: empty when the regexp cannot be replaced by equality matchers only,
/// or mixes case sensitive and insensitive parts. The bool tells whether the matches are case sensitive.
pub fn find_set_matches(re: &mut syntax::Regexp) -> (Vec<String>, bool) {
    clear_begin_end_text(re);
    find_set_matches_internal(re, "")
}

fn find_set_matches_internal(re: &mut syntax::Regexp, base: &str) -> (Vec<String>, bool) {
    match re.op {
        Op::BeginText | Op::EndText => (Vec::new(), false),
        Op::Literal => (vec![format!("{}{}", base, runes_string(&re.rune))], is_case_sensitive(re)),
        Op::EmptyMatch => {
            if !base.is_empty() {
                (vec![base.to_string()], is_case_sensitive(re))
            } else {
                (Vec::new(), false)
            }
        }
        Op::Alternate => find_set_matches_from_alternate(re, base),
        Op::Capture => {
            clear_capture(re);
            find_set_matches_internal(re, base)
        }
        Op::Concat => find_set_matches_from_concat(re, base),
        Op::CharClass => {
            if re.rune.len() % 2 != 0 {
                return (Vec::new(), false);
            }
            let mut total: i64 = 0;
            for p in re.rune.chunks(2) {
                total += (p[1] - p[0]) as i64 + 1;
            }
            if total > MAX_SET_MATCHES as i64 {
                return (Vec::new(), false);
            }
            let mut matches = Vec::new();
            for p in re.rune.chunks(2) {
                for c in p[0]..=p[1] {
                    let mut s = String::with_capacity(base.len() + 4);
                    s.push_str(base);
                    s.push(rune_char(c));
                    matches.push(s);
                }
            }
            let cs = is_case_sensitive(re);
            if matches.is_empty() { (Vec::new(), false) } else { (matches, cs) }
        }
        _ => (Vec::new(), false),
    }
}

fn find_set_matches_from_concat(re: &mut syntax::Regexp, base: &str) -> (Vec<String>, bool) {
    if re.sub.is_empty() {
        return (Vec::new(), false);
    }
    for s in re.sub.iter_mut() {
        clear_capture(s);
    }
    let mut matches = vec![base.to_string()];
    let mut matches_cs = false;
    for i in 0..re.sub.len() {
        let mut new_matches: Vec<String> = Vec::new();
        for (j, b) in matches.iter().enumerate() {
            let (m, cs) = find_set_matches_internal(&mut re.sub[i], b);
            if m.is_empty() {
                return (Vec::new(), false);
            }
            if too_many_matches(&new_matches, &m) {
                return (Vec::new(), false);
            }
            if i == 0 && j == 0 {
                matches_cs = cs;
            }
            if matches_cs != cs {
                return (Vec::new(), false);
            }
            new_matches.extend(m);
        }
        matches = new_matches;
    }
    (matches, matches_cs)
}

fn find_set_matches_from_alternate(re: &mut syntax::Regexp, base: &str) -> (Vec<String>, bool) {
    let mut matches: Vec<String> = Vec::new();
    let mut matches_cs = false;
    for (i, sub) in re.sub.iter_mut().enumerate() {
        let (found, cs) = find_set_matches_internal(sub, base);
        if found.is_empty() {
            return (Vec::new(), false);
        }
        if too_many_matches(&matches, &found) {
            return (Vec::new(), false);
        }
        if i == 0 {
            matches_cs = cs;
        }
        if matches_cs != cs {
            return (Vec::new(), false);
        }
        matches.extend(found);
    }
    (matches, matches_cs)
}

/// Removes capture operations (possibly nested) at the top of `r`.
pub fn clear_capture(r: &mut syntax::Regexp) {
    while r.op == Op::Capture {
        let s = r.sub.swap_remove(0);
        *r = s;
    }
}

/// Removes the begin and end text operators at the start and end of `re` (Prometheus regexps are anchored anyway).
pub fn clear_begin_end_text(re: &mut syntax::Regexp) {
    if re.op == Op::Alternate {
        return;
    }
    if re.sub.is_empty() {
        return;
    }
    if re.sub.len() == 1 && (re.sub[0].op == Op::BeginText || re.sub[0].op == Op::EndText) {
        re.op = Op::EmptyMatch;
        re.sub.clear();
        return;
    }
    if re.sub[0].op == Op::BeginText {
        re.sub.remove(0);
    }
    if re.sub.last().is_some_and(|s| s.op == Op::EndText) {
        re.sub.pop();
    }
}

fn is_case_insensitive(re: &syntax::Regexp) -> bool {
    re.flags & syntax::FOLD_CASE != 0
}

fn is_case_sensitive(re: &syntax::Regexp) -> bool {
    !is_case_insensitive(re)
}

fn too_many_matches(matches: &[String], added: &[String]) -> bool {
    matches.len() + added.len() > MAX_SET_MATCHES
}

/// Optimizes `literal1|literal2|...`: returns a string matcher (or None) and up to `MAX_SET_MATCHES` set matches.
pub fn optimize_alternating_literals(s: &str) -> (Option<StringMatcher>, Vec<String>) {
    if s.is_empty() {
        return (Some(StringMatcher::Empty), Vec::new());
    }
    let estimated = s.bytes().filter(|&c| c == b'|').count() + 1;
    if estimated == 1 {
        if quote_meta(s) == s {
            return (Some(StringMatcher::Equal { s: s.to_string(), case_sensitive: true }), vec![s.to_string()]);
        }
        return (None, Vec::new());
    }
    let mut multi = new_equal_multi_string_matcher(true, estimated, 0, 0);
    for sub in s.split('|') {
        if quote_meta(sub) != sub {
            return (None, Vec::new());
        }
        multi.multi_add(sub);
    }
    let sm = multi.multi_set_matches();
    (Some(multi), sm)
}

/// Rewrites an alternation of `.*literal.*` into `.*(literal1|literal2).*`.
pub fn optimize_alternating_simple_contains(r: syntax::Regexp) -> syntax::Regexp {
    if r.op != Op::Alternate {
        return r;
    }
    for sub in &r.sub {
        if sub.op != Op::Concat || sub.sub.len() != 3 {
            return r;
        }
        if !is_case_sensitive_literal(&sub.sub[1]) || !is_match_any(&sub.sub[0]) || !is_match_any(&sub.sub[2]) {
            return r;
        }
    }
    if r.sub.len() > 1 {
        let literals: Vec<syntax::Regexp> = r.sub.into_iter().map(|mut s| s.sub.swap_remove(1)).collect();
        let any = || {
            let mut star = syntax::Regexp::new(Op::Star);
            star.flags = syntax::PERL | syntax::DOT_NL;
            star.sub = vec![syntax::Regexp::new(Op::AnyChar)];
            star
        };
        let mut alts = syntax::Regexp::new(Op::Alternate);
        alts.sub = literals;
        let mut ret = syntax::Regexp::new(Op::Concat);
        ret.sub = vec![any(), alts, any()];
        return ret;
    }
    r
}

/// Returns the literal prefix (and whether it is case-insensitive), suffix and contained literals of a
/// concatenation, which can be checked before running the regexp.
pub fn optimize_concat_regex(r: &mut syntax::Regexp) -> (bool, String, String, Vec<String>) {
    let mut cip = false;
    let mut prefix = String::new();
    let mut suffix = String::new();
    let mut contains = Vec::new();
    for s in r.sub.iter_mut() {
        clear_capture(s);
    }
    let mut sub: &[syntax::Regexp] = &r.sub;
    if sub.first().is_some_and(|s| s.op == Op::BeginText) {
        sub = &sub[1..];
    }
    if sub.last().is_some_and(|s| s.op == Op::EndText) {
        sub = &sub[..sub.len() - 1];
    }
    if sub.is_empty() {
        return (cip, prefix, suffix, contains);
    }
    if sub[0].op == Op::Literal {
        prefix = runes_string(&sub[0].rune);
        cip = sub[0].flags & syntax::FOLD_CASE != 0;
    }
    let last = sub.len() - 1;
    if sub[last].op == Op::Literal && sub[last].flags & syntax::FOLD_CASE == 0 {
        suffix = runes_string(&sub[last].rune);
    }
    for s in sub.iter().take(sub.len().saturating_sub(1)).skip(1) {
        if s.op == Op::Literal && s.flags & syntax::FOLD_CASE == 0 {
            contains.push(runes_string(&s.rune));
        }
    }
    (cip, prefix, suffix, contains)
}

// ---------------------------------------------------------------------------------------------------------------
// String matchers.

/// Matches a string in place of a regular expression (Go `StringMatcher` and its implementations).
#[derive(Clone, Debug, PartialEq)]
pub enum StringMatcher {
    /// Matches if the string contains one of `substrings` with `left` matching before and `right` after it. With
    /// no `left`, the substring must be a prefix; with no `right`, a suffix.
    Contains { left: Option<Box<StringMatcher>>, substrings: Vec<String>, right: Option<Box<StringMatcher>> },
    /// A case-sensitive literal prefix, then `right`.
    LiteralPrefixSensitive { prefix: String, right: Option<Box<StringMatcher>> },
    /// A case-insensitive literal prefix, then `right`.
    LiteralPrefixInsensitive { prefix: String, right: Option<Box<StringMatcher>> },
    /// `left`, then a literal suffix.
    LiteralSuffix { left: Option<Box<StringMatcher>>, suffix: String, suffix_case_sensitive: bool },
    /// Matches the empty string.
    Empty,
    /// Matches if any sub-matcher matches.
    Or(Vec<StringMatcher>),
    /// Equality, optionally case-insensitive.
    Equal { s: String, case_sensitive: bool },
    /// Equality with one of a short list of values.
    EqualMultiSlice { values: Vec<String>, lengths_mask: u64, case_sensitive: bool },
    /// Equality with one of many values, or a match of one of the prefix matchers.
    EqualMultiMap(EqualMultiStringMapMatcher),
    /// Any string without a newline.
    AnyStringWithoutNewline,
    /// Any non-empty string (without newlines unless `match_nl`).
    AnyNonEmpty { match_nl: bool },
    /// Zero or one character (a newline only if `match_nl`).
    ZeroOrOneCharacter { match_nl: bool },
    /// Any string.
    True,
}

/// The map form of the multi-value equality matcher.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct EqualMultiStringMapMatcher {
    /// Values to match. Lower case (normalized) when the matcher is case-insensitive.
    pub values: HashSet<String>,
    /// Bit `min(len, 63)` is set for each value length (case-sensitive only).
    pub lengths_mask: u64,
    /// Matchers by the first `min_prefix_len` bytes of their prefix (lower case when case-insensitive).
    pub prefixes: HashMap<Vec<u8>, Vec<StringMatcher>>,
    /// Zero when there are no prefix matchers.
    pub min_prefix_len: usize,
    pub case_sensitive: bool,
}

#[inline]
fn sm_matches(m: &Option<Box<StringMatcher>>, s: &str) -> bool {
    match m {
        Some(m) => m.matches(s),
        None => panic!("nil StringMatcher"),
    }
}

/// Smallest char boundary of `s` at or after `i`.
#[inline]
fn ceil_char_boundary(s: &str, mut i: usize) -> usize {
    while i < s.len() && !s.is_char_boundary(i) {
        i += 1;
    }
    i
}

impl StringMatcher {
    /// Whether `s` matches.
    pub fn matches(&self, s: &str) -> bool {
        match self {
            StringMatcher::Contains { left, substrings, right } => {
                for substr in substrings {
                    match (left, right) {
                        (Some(l), Some(r)) => {
                            let mut start = 0;
                            while start <= s.len() {
                                let Some(pos) = s[start..].find(substr.as_str()) else { break };
                                let pos = pos + start;
                                if l.matches(&s[..pos]) && r.matches(&s[pos + substr.len()..]) {
                                    return true;
                                }
                                start = ceil_char_boundary(s, pos + 1);
                            }
                        }
                        (Some(l), None) => {
                            if s.ends_with(substr.as_str()) && l.matches(&s[..s.len() - substr.len()]) {
                                return true;
                            }
                        }
                        (None, Some(r)) => {
                            if s.starts_with(substr.as_str()) && r.matches(&s[substr.len()..]) {
                                return true;
                            }
                        }
                        (None, None) => {}
                    }
                }
                false
            }
            StringMatcher::LiteralPrefixSensitive { prefix, right } => {
                if !s.starts_with(prefix.as_str()) {
                    return false;
                }
                sm_matches(right, &s[prefix.len()..])
            }
            StringMatcher::LiteralPrefixInsensitive { prefix, right } => {
                match prefix_case_insensitive_match_len(s.as_bytes(), prefix.as_bytes()) {
                    Some(n) => sm_matches(right, &s[n..]),
                    None => false,
                }
            }
            StringMatcher::LiteralSuffix { left, suffix, suffix_case_sensitive } => {
                if *suffix_case_sensitive {
                    if !s.ends_with(suffix.as_str()) {
                        return false;
                    }
                    return sm_matches(left, &s[..s.len() - suffix.len()]);
                }
                match suffix_case_insensitive_match_len(s.as_bytes(), suffix.as_bytes()) {
                    Some(n) => sm_matches(left, &s[..s.len() - n]),
                    None => false,
                }
            }
            StringMatcher::Empty => s.is_empty(),
            StringMatcher::Or(ms) => ms.iter().any(|m| m.matches(s)),
            StringMatcher::Equal { s: v, case_sensitive } => {
                if *case_sensitive { v == s } else { unicode::equal_fold(v, s) }
            }
            StringMatcher::EqualMultiSlice { values, lengths_mask, case_sensitive } => {
                if *case_sensitive {
                    return lengths_mask & length_mask(s) > 0 && values.iter().any(|v| v == s);
                }
                values.iter().any(|v| unicode::equal_fold(s, v))
            }
            StringMatcher::EqualMultiMap(m) => m.matches(s),
            StringMatcher::AnyStringWithoutNewline => !s.as_bytes().contains(&b'\n'),
            StringMatcher::AnyNonEmpty { match_nl } => {
                if *match_nl {
                    return !s.is_empty();
                }
                !s.is_empty() && !s.as_bytes().contains(&b'\n')
            }
            StringMatcher::ZeroOrOneCharacter { match_nl } => {
                let mut it = s.chars();
                match it.next() {
                    None => true,
                    Some(c) => {
                        if c == '\u{FFFD}' {
                            return c.len_utf8() == s.len();
                        }
                        if c.len_utf8() < s.len() {
                            return false;
                        }
                        *match_nl || s.as_bytes()[0] != b'\n'
                    }
                }
            }
            StringMatcher::True => true,
        }
    }

    /// Adds a value to a multi-value equality matcher (Go `multiStringMatcherBuilder.add`).
    pub fn multi_add(&mut self, s: &str) {
        match self {
            StringMatcher::EqualMultiSlice { values, lengths_mask, .. } => {
                values.push(s.to_string());
                *lengths_mask |= length_mask(s);
            }
            StringMatcher::EqualMultiMap(m) => m.add(s),
            _ => panic!("not a multi string matcher"),
        }
    }

    /// Adds a prefix matcher to a multi-value equality matcher (Go `multiStringMatcherBuilder.addPrefix`).
    pub fn multi_add_prefix(&mut self, prefix: &str, prefix_case_sensitive: bool, matcher: StringMatcher) {
        match self {
            StringMatcher::EqualMultiSlice { .. } => panic!("not implemented"),
            StringMatcher::EqualMultiMap(m) => m.add_prefix(prefix, prefix_case_sensitive, matcher),
            _ => panic!("not a multi string matcher"),
        }
    }

    /// The set matches of a multi-value equality matcher (Go `multiStringMatcherBuilder.setMatches`).
    pub fn multi_set_matches(&self) -> Vec<String> {
        match self {
            StringMatcher::EqualMultiSlice { values, .. } => values.clone(),
            StringMatcher::EqualMultiMap(m) => {
                if m.values.len() >= MAX_SET_MATCHES || !m.prefixes.is_empty() {
                    return Vec::new();
                }
                m.values.iter().cloned().collect()
            }
            _ => panic!("not a multi string matcher"),
        }
    }
}

impl EqualMultiStringMapMatcher {
    fn add(&mut self, s: &str) {
        if !self.case_sensitive {
            let l = to_normalised_lower(s).into_owned();
            self.values.insert(l);
        } else {
            self.lengths_mask |= length_mask(s);
            self.values.insert(s.to_string());
        }
    }

    fn add_prefix(&mut self, prefix: &str, prefix_case_sensitive: bool, matcher: StringMatcher) {
        if self.min_prefix_len == 0 {
            panic!("addPrefix called when no prefix length defined");
        }
        if prefix.len() < self.min_prefix_len {
            panic!("addPrefix called with a too short prefix");
        }
        if self.case_sensitive != prefix_case_sensitive {
            panic!("addPrefix called with a prefix whose case sensitivity is different than the expected one");
        }
        let mut s = prefix.as_bytes()[..self.min_prefix_len].to_vec();
        if !self.case_sensitive {
            s = go_bytes_to_lower(&s);
        }
        self.prefixes.entry(s).or_default().push(matcher);
    }

    fn matches(&self, s: &str) -> bool {
        if !self.values.is_empty() {
            if self.min_prefix_len == 0 && self.case_sensitive && self.lengths_mask & length_mask(s) == 0 {
                return false;
            }
            let hit = if self.case_sensitive {
                self.values.contains(s)
            } else {
                let mut buf = [0u8; 32];
                self.values.contains(to_normalised_lower_buf(s, &mut buf).as_ref())
            };
            if hit {
                return true;
            }
        }
        if self.min_prefix_len > 0 && s.len() >= self.min_prefix_len {
            let p = &s.as_bytes()[..self.min_prefix_len];
            let key: Cow<'_, [u8]> = if self.case_sensitive { Cow::Borrowed(p) } else { to_normalised_lower_bytes(p) };
            if let Some(ms) = self.prefixes.get(key.as_ref()) {
                for m in ms {
                    if m.matches(s) {
                        return true;
                    }
                }
            }
        }
        false
    }
}

/// Go `newEqualMultiStringMatcher`: a list matcher for few values, a map matcher otherwise.
pub fn new_equal_multi_string_matcher(
    case_sensitive: bool,
    estimated_size: usize,
    estimated_prefixes: usize,
    min_prefix_length: usize,
) -> StringMatcher {
    if estimated_size < MIN_EQUAL_MULTI_STRING_MATCHER_MAP_THRESHOLD && estimated_prefixes == 0 {
        return StringMatcher::EqualMultiSlice {
            values: Vec::with_capacity(estimated_size),
            lengths_mask: 0,
            case_sensitive,
        };
    }
    StringMatcher::EqualMultiMap(EqualMultiStringMapMatcher {
        values: HashSet::with_capacity(estimated_size),
        lengths_mask: 0,
        prefixes: HashMap::with_capacity(estimated_prefixes),
        min_prefix_len: min_prefix_length,
        case_sensitive,
    })
}

/// Bitmask with bit `min(len(s), 63)` set.
#[inline]
pub fn length_mask(s: &str) -> u64 {
    1u64 << s.len().min(63)
}

/// Go `toNormalisedLower`: NFKD then lower case when `s` has non-ASCII characters, ASCII lower case otherwise.
pub fn to_normalised_lower(s: &str) -> Cow<'_, str> {
    if !s.is_ascii() {
        return Cow::Owned(unicode::nfkd(s).chars().map(unicode::to_lower).collect());
    }
    if s.bytes().any(|c| c.is_ascii_uppercase()) {
        return Cow::Owned(s.to_ascii_lowercase());
    }
    Cow::Borrowed(s)
}

/// `to_normalised_lower` that uses `buf` instead of allocating for short ASCII strings.
fn to_normalised_lower_buf<'a>(s: &'a str, buf: &'a mut [u8; 32]) -> Cow<'a, str> {
    if s.is_ascii() && s.len() < buf.len() && s.bytes().any(|c| c.is_ascii_uppercase()) {
        let b = &mut buf[..s.len()];
        b.copy_from_slice(s.as_bytes());
        b.make_ascii_lowercase();
        return Cow::Borrowed(std::str::from_utf8(b).unwrap_or(""));
    }
    to_normalised_lower(s)
}

/// Go-style UTF-8 decoding of the first rune of `b`: invalid bytes decode as U+FFFD with width 1.
fn decode_rune(b: &[u8]) -> (char, usize) {
    let n = match b.first() {
        None => return ('\u{FFFD}', 0),
        Some(&c) if c < 0x80 => return (c as char, 1),
        Some(&c) if c >= 0xF0 => 4,
        Some(&c) if c >= 0xE0 => 3,
        Some(_) => 2,
    };
    if b.len() >= n {
        if let Ok(s) = std::str::from_utf8(&b[..n]) {
            if let Some(c) = s.chars().next() {
                return (c, n);
            }
        }
    }
    ('\u{FFFD}', 1)
}

/// Go-style decoding of the last rune of `b`.
fn decode_last_rune(b: &[u8]) -> (char, usize) {
    if b.is_empty() {
        return ('\u{FFFD}', 0);
    }
    let end = b.len();
    let lim = end.saturating_sub(4);
    let mut start = end - 1;
    while start > lim && (b[start] & 0xC0) == 0x80 {
        start -= 1;
    }
    let (c, size) = decode_rune(&b[start..end]);
    if start + size != end {
        return ('\u{FFFD}', 1);
    }
    (c, size)
}

/// Go `strings.ToLower` on bytes that may not be valid UTF-8 (invalid bytes become U+FFFD).
fn go_bytes_to_lower(b: &[u8]) -> Vec<u8> {
    if b.is_ascii() {
        return b.to_ascii_lowercase();
    }
    let mut out = String::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        let (c, n) = decode_rune(&b[i..]);
        out.push(unicode::to_lower(c));
        i += n;
    }
    out.into_bytes()
}

/// `to_normalised_lower` on bytes that may not be valid UTF-8 (a prefix cut at a byte length).
fn to_normalised_lower_bytes(b: &[u8]) -> Cow<'_, [u8]> {
    match std::str::from_utf8(b) {
        Ok(s) => match to_normalised_lower(s) {
            Cow::Borrowed(_) => Cow::Borrowed(b),
            Cow::Owned(o) => Cow::Owned(o.into_bytes()),
        },
        Err(_) => {
            // Invalid bytes pass through NFKD and become U+FFFD in the lower-casing map.
            let mut s = String::with_capacity(b.len() + 4);
            let mut i = 0;
            while i < b.len() {
                let (c, n) = decode_rune(&b[i..]);
                s.push(c);
                i += n;
            }
            Cow::Owned(unicode::nfkd(&s).chars().map(unicode::to_lower).collect::<String>().into_bytes())
        }
    }
}

/// Whether `s` begins with `prefix` under Unicode simple case folding.
pub fn has_prefix_case_insensitive(s: &[u8], prefix: &[u8]) -> bool {
    prefix_case_insensitive_match_len(s, prefix).is_some()
}

/// If `s` begins with a prefix equal to `prefix` under simple case folding, the byte length of that prefix in `s`.
pub fn prefix_case_insensitive_match_len(s: &[u8], prefix: &[u8]) -> Option<usize> {
    let mut i = 0;
    while i < prefix.len() && i < s.len() {
        let (pc, sc) = (prefix[i], s[i]);
        if pc >= 0x80 || sc >= 0x80 {
            break;
        }
        if pc != sc && pc.to_ascii_lowercase() != sc.to_ascii_lowercase() {
            return None;
        }
        i += 1;
    }
    if i == prefix.len() {
        return Some(i);
    }
    let mut n = i;
    let mut p = i;
    while p < prefix.len() {
        let (pr, psize) = decode_rune(&prefix[p..]);
        p += psize;
        if n >= s.len() {
            return None;
        }
        let (sr, size) = decode_rune(&s[n..]);
        if sr != pr && !rune_fold_equal(sr, pr) {
            return None;
        }
        n += size;
    }
    Some(n)
}

/// Like `prefix_case_insensitive_match_len`, for suffixes.
pub fn suffix_case_insensitive_match_len(s: &[u8], suffix: &[u8]) -> Option<usize> {
    let (mut i, mut j) = (suffix.len(), s.len());
    while i > 0 && j > 0 {
        let (pc, sc) = (suffix[i - 1], s[j - 1]);
        if pc >= 0x80 || sc >= 0x80 {
            break;
        }
        if pc != sc && pc.to_ascii_lowercase() != sc.to_ascii_lowercase() {
            return None;
        }
        i -= 1;
        j -= 1;
    }
    if i == 0 {
        return Some(s.len() - j);
    }
    while i > 0 {
        if j == 0 {
            return None;
        }
        let (pr, psize) = decode_last_rune(&suffix[..i]);
        let (sr, ssize) = decode_last_rune(&s[..j]);
        if sr != pr && !rune_fold_equal(sr, pr) {
            return None;
        }
        i -= psize;
        j -= ssize;
    }
    Some(s.len() - j)
}

/// Whether two distinct runes are equal under simple case folding.
fn rune_fold_equal(a: char, b: char) -> bool {
    let (a, b) = (a as i32, b as i32);
    let mut r = unicode::simple_fold(a);
    while r != a {
        if r == b {
            return true;
        }
        r = unicode::simple_fold(r);
    }
    false
}

/// Whether `s` contains all of `contains`, in order, without overlaps.
pub fn contains_in_order(s: &str, contains: &[String]) -> bool {
    if contains.len() == 1 {
        return s.contains(contains[0].as_str());
    }
    let mut offset = 0;
    for substr in contains {
        match s[offset..].find(substr.as_str()) {
            Some(at) => offset += at + substr.len(),
            None => return false,
        }
    }
    true
}

/// Replaces a common regexp with a string matcher; None when the regexp is not supported.
pub fn string_matcher_from_regexp(re: &mut syntax::Regexp) -> Option<StringMatcher> {
    clear_begin_end_text(re);
    let m = string_matcher_from_regexp_internal(re);
    m.map(|m| optimize_equal_or_prefix_string_matchers(m, MIN_EQUAL_MULTI_STRING_MATCHER_MAP_THRESHOLD))
}

fn is_any_op(op: Op) -> bool {
    op == Op::Plus || op == Op::Star || op == Op::Quest
}

/// Go `stringMatcherFromRegexpInternal`.
pub fn string_matcher_from_regexp_internal(re: &mut syntax::Regexp) -> Option<StringMatcher> {
    clear_capture(re);
    match re.op {
        Op::BeginText | Op::EndText => None,
        Op::Plus => {
            let o = re.sub[0].op;
            if o != Op::AnyChar && o != Op::AnyCharNotNL {
                return None;
            }
            Some(StringMatcher::AnyNonEmpty { match_nl: o == Op::AnyChar })
        }
        Op::Star => {
            let o = re.sub[0].op;
            if o != Op::AnyChar && o != Op::AnyCharNotNL {
                return None;
            }
            if o == Op::AnyChar {
                return Some(StringMatcher::True);
            }
            Some(StringMatcher::AnyStringWithoutNewline)
        }
        Op::Quest => {
            if re.sub.len() != 1 || (re.sub[0].op != Op::AnyChar && re.sub[0].op != Op::AnyCharNotNL) {
                return None;
            }
            Some(StringMatcher::ZeroOrOneCharacter { match_nl: re.sub[0].op == Op::AnyChar })
        }
        Op::EmptyMatch => Some(StringMatcher::Empty),
        Op::Literal => Some(StringMatcher::Equal { s: runes_string(&re.rune), case_sensitive: !is_case_insensitive(re) }),
        Op::Alternate => {
            let mut or = Vec::with_capacity(re.sub.len());
            for sub in re.sub.iter_mut() {
                or.push(string_matcher_from_regexp_internal(sub)?);
            }
            Some(StringMatcher::Or(or))
        }
        Op::Concat => {
            for s in re.sub.iter_mut() {
                clear_capture(s);
            }
            if re.sub.is_empty() {
                return Some(StringMatcher::Empty);
            }
            if re.sub.len() == 1 {
                return string_matcher_from_regexp_internal(&mut re.sub[0]);
            }
            let mut left: Option<StringMatcher> = None;
            let mut right: Option<StringMatcher> = None;
            if is_any_op(re.sub[0].op) {
                left = Some(string_matcher_from_regexp_internal(&mut re.sub[0])?);
                re.sub.remove(0);
            }
            if re.sub.last().is_some_and(|s| is_any_op(s.op)) {
                let last = re.sub.len() - 1;
                right = Some(string_matcher_from_regexp_internal(&mut re.sub[last])?);
                re.sub.pop();
            }
            let (mut matches, mut matches_cs) = find_set_matches_internal(re, "");
            if matches.is_empty() && re.sub.len() == 2 {
                if right.is_none() && re.sub[0].op == Op::Literal {
                    right = string_matcher_from_regexp_internal(&mut re.sub[1]);
                    if right.is_some() {
                        matches = vec![runes_string(&re.sub[0].rune)];
                        matches_cs = !is_case_insensitive(&re.sub[0]);
                    }
                } else if left.is_none() && re.sub[1].op == Op::Literal {
                    left = string_matcher_from_regexp_internal(&mut re.sub[0]);
                    if left.is_some() {
                        matches = vec![runes_string(&re.sub[1].rune)];
                        matches_cs = !is_case_insensitive(&re.sub[1]);
                    }
                }
            }
            if matches.is_empty() {
                return None;
            }
            match (left, right) {
                (None, None) => Some(StringMatcher::Or(
                    matches.into_iter().map(|s| StringMatcher::Equal { s, case_sensitive: matches_cs }).collect(),
                )),
                (None, Some(r)) if matches.len() == 1 => {
                    Some(new_literal_prefix_string_matcher(matches.pop().unwrap(), matches_cs, r))
                }
                (Some(l), None) if matches.len() == 1 => Some(StringMatcher::LiteralSuffix {
                    left: Some(Box::new(l)),
                    suffix: matches.pop().unwrap(),
                    suffix_case_sensitive: matches_cs,
                }),
                (l, r) if matches_cs => Some(StringMatcher::Contains {
                    substrings: matches,
                    left: l.map(Box::new),
                    right: r.map(Box::new),
                }),
                _ => None,
            }
        }
        _ => None,
    }
}

fn new_literal_prefix_string_matcher(prefix: String, case_sensitive: bool, right: StringMatcher) -> StringMatcher {
    if case_sensitive {
        StringMatcher::LiteralPrefixSensitive { prefix, right: Some(Box::new(right)) }
    } else {
        StringMatcher::LiteralPrefixInsensitive { prefix, right: Some(Box::new(right)) }
    }
}

/// Whether `re` is a concatenation like `.*-.*-.*`: wildcards and case-sensitive literals alternating, starting and
/// ending with a wildcard.
pub fn is_simple_concatenation_pattern(re: &syntax::Regexp) -> bool {
    if re.op != Op::Concat {
        return false;
    }
    if re.sub.len() < 3 || re.sub.len() % 2 == 0 {
        return false;
    }
    re.sub.iter().enumerate().all(|(i, s)| if i % 2 == 0 { is_match_any(s) } else { is_case_sensitive_literal(s) })
}

fn is_match_any(re: &syntax::Regexp) -> bool {
    re.op == Op::Star && re.sub[0].op == Op::AnyChar
}

fn is_case_sensitive_literal(re: &syntax::Regexp) -> bool {
    re.op == Op::Literal && is_case_sensitive(re)
}

/// Go `optimizeEqualOrPrefixStringMatchers`: replaces a large alternation of equality and literal-prefix matchers
/// by a multi-value matcher.
pub fn optimize_equal_or_prefix_string_matchers(input: StringMatcher, threshold: usize) -> StringMatcher {
    enum Item {
        Equal(String, bool),
        Prefix(String, bool, StringMatcher),
    }
    let items = std::cell::RefCell::new(Vec::new());
    let found = find_equal_or_prefix_string_matchers(
        &input,
        &mut |s: &str, cs: bool| {
            items.borrow_mut().push(Item::Equal(s.to_string(), cs));
            true
        },
        Some(&mut |prefix: &str, cs: bool, m: &StringMatcher| {
            items.borrow_mut().push(Item::Prefix(prefix.to_string(), cs, m.clone()));
            true
        }),
    );
    if !found {
        return input;
    }
    let items = items.into_inner();
    // Analyse the matchers in visit order, as Go's callbacks do: all must have the same case sensitivity.
    let mut case_sensitive = false;
    let mut case_sensitive_set = false;
    let mut num_values = 0;
    let mut num_prefixes = 0;
    let mut min_prefix_length = 0;
    for it in &items {
        let cs = match it {
            Item::Equal(_, cs) | Item::Prefix(_, cs, _) => *cs,
        };
        if case_sensitive_set && case_sensitive != cs {
            return input;
        } else if !case_sensitive_set {
            case_sensitive = cs;
            case_sensitive_set = true;
        }
        match it {
            Item::Equal(..) => num_values += 1,
            Item::Prefix(p, _, _) => {
                if num_prefixes == 0 || p.len() < min_prefix_length {
                    min_prefix_length = p.len();
                }
                num_prefixes += 1;
            }
        }
    }
    if num_values + num_prefixes < threshold {
        return input;
    }
    let mut multi = new_equal_multi_string_matcher(case_sensitive, num_values, num_prefixes, min_prefix_length);
    for it in items {
        match it {
            Item::Equal(s, _) => multi.multi_add(&s),
            Item::Prefix(p, _, m) => multi.multi_add_prefix(&p, case_sensitive, m),
        }
    }
    multi
}

/// Go `findEqualOrPrefixStringMatchers`: calls `equal` for each equality matcher and `prefix` for each literal
/// prefix matcher of an alternation. Returns true only if the input is an alternation of only those.
pub fn find_equal_or_prefix_string_matchers(
    input: &StringMatcher,
    equal: &mut dyn FnMut(&str, bool) -> bool,
    mut prefix: Option<&mut dyn FnMut(&str, bool, &StringMatcher) -> bool>,
) -> bool {
    let StringMatcher::Or(ms) = input else { return false };
    for m in ms {
        match m {
            StringMatcher::Or(_) => {
                let p = prefix.as_mut().map(|p| &mut **p as &mut dyn FnMut(&str, bool, &StringMatcher) -> bool);
                if !find_equal_or_prefix_string_matchers(m, equal, p) {
                    return false;
                }
            }
            StringMatcher::Equal { s, case_sensitive } => {
                if !equal(s, *case_sensitive) {
                    return false;
                }
            }
            StringMatcher::LiteralPrefixSensitive { prefix: p, .. } => {
                let ok = match prefix.as_mut() {
                    Some(cb) => cb(p, true, m),
                    None => false,
                };
                if !ok {
                    return false;
                }
            }
            StringMatcher::LiteralPrefixInsensitive { prefix: p, .. } => {
                let ok = match prefix.as_mut() {
                    Some(cb) => cb(p, false, m),
                    None => false,
                };
                if !ok {
                    return false;
                }
            }
            _ => return false,
        }
    }
    true
}
