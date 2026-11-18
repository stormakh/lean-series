//! A Go-compatible compiled regular expression (Go `regexp.Regexp`, grafana fork): the pattern is parsed with the Go
//! syntax parser (`syntax.rs`), so the accepted syntax, the error messages and the meaning of every construct are
//! Go's. The tree is then written out as an equivalent `regex` crate pattern that only uses explicit code point
//! classes and ASCII-only assertions, so no Unicode table or flag of the `regex` crate changes the meaning.
//!
//! Matching semantics are the same in both engines: leftmost-first (Perl-like) matches and submatches.

use std::borrow::Cow;
use std::fmt;
use std::fmt::Write;
use std::sync::Arc;

use super::syntax::{self, Op};
use crate::unicode;

pub use super::syntax::Error;

/// A compiled Go regular expression. Cloning is cheap (shared).
#[derive(Clone)]
pub struct Regexp {
    inner: Arc<Inner>,
}

struct Inner {
    expr: String,
    re: regex::Regex,
    subexp_names: Vec<String>,
    /// Go capture index -> `regex` group index (None when the `regex` crate dropped the group, as in `(a){0}`).
    groups: Vec<Option<usize>>,
}

impl fmt::Debug for Regexp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Regexp({:?})", self.inner.expr)
    }
}

impl fmt::Display for Regexp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.inner.expr)
    }
}

impl Regexp {
    /// Go `regexp.Compile`: parses `expr` with Perl flags.
    pub fn compile(expr: &str) -> Result<Regexp, Error> {
        let tree = syntax::parse(expr, syntax::PERL)?;
        Regexp::from_tree(expr.to_string(), &tree)
    }

    /// Go `regexp.MustCompile`.
    pub fn must_compile(expr: &str) -> Regexp {
        match Regexp::compile(expr) {
            Ok(r) => r,
            Err(e) => panic!("regexp: Compile({}): {}", unicode::quote(expr), e),
        }
    }

    /// Compiles a parsed tree; `expr` is what `as_str` returns.
    pub(crate) fn from_tree(expr: String, tree: &syntax::Regexp) -> Result<Regexp, Error> {
        let mut pat = String::with_capacity(expr.len() * 2 + 16);
        write_rust(tree, &mut pat);
        let re = regex::RegexBuilder::new(&pat)
            .unicode(true)
            .size_limit(256 << 20)
            .dfa_size_limit(16 << 20)
            .nest_limit(10_000)
            .build()
            .map_err(|_| Error { code: syntax::ErrorCode::Large, expr: expr.clone() })?;
        let subexp_names = tree.cap_names();
        let mut groups = vec![None; subexp_names.len()];
        groups[0] = Some(0);
        for (i, name) in re.capture_names().enumerate() {
            if let Some(n) = name.and_then(|n| n.strip_prefix('c')).and_then(|n| n.parse::<usize>().ok()) {
                if n < groups.len() {
                    groups[n] = Some(i);
                }
            }
        }
        Ok(Regexp { inner: Arc::new(Inner { expr, re, subexp_names, groups }) })
    }

    /// The source text (Go `String()`).
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.inner.expr
    }

    /// The `regex` crate pattern that implements this expression.
    pub fn translated(&self) -> &str {
        self.inner.re.as_str()
    }

    /// Whether `a` and `b` are the same compiled object (Go pointer equality).
    #[inline]
    pub fn ptr_eq(a: &Regexp, b: &Regexp) -> bool {
        Arc::ptr_eq(&a.inner, &b.inner)
    }

    /// Go `MatchString`.
    #[inline]
    pub fn is_match(&self, s: &str) -> bool {
        self.inner.re.is_match(s)
    }

    /// Number of parenthesized subexpressions (Go `NumSubexp`).
    pub fn num_subexp(&self) -> usize {
        self.inner.subexp_names.len() - 1
    }

    /// Names of the capturing groups (Go `SubexpNames`); index 0 is "".
    pub fn subexp_names(&self) -> &[String] {
        &self.inner.subexp_names
    }

    /// Go `FindStringSubmatchIndex`: pairs of byte offsets, -1 for groups that did not participate.
    pub fn find_string_submatch_index(&self, s: &str) -> Option<Vec<isize>> {
        self.find_at(s, 0)
    }

    fn find_at(&self, s: &str, start: usize) -> Option<Vec<isize>> {
        let mut locs = self.inner.re.capture_locations();
        self.inner.re.captures_read_at(&mut locs, s, start)?;
        let mut out = Vec::with_capacity(self.inner.groups.len() * 2);
        for g in &self.inner.groups {
            match g.and_then(|i| locs.get(i)) {
                Some((a, b)) => {
                    out.push(a as isize);
                    out.push(b as isize);
                }
                None => {
                    out.push(-1);
                    out.push(-1);
                }
            }
        }
        Some(out)
    }

    /// Go `ExpandString`: appends `template` to `dst` with `$name`/`${name}` replaced by submatches of `src`.
    pub fn expand_string(&self, dst: &mut String, mut template: &str, src: &str, m: &[isize]) {
        while let Some(i) = template.find('$') {
            dst.push_str(&template[..i]);
            template = &template[i + 1..];
            if template.as_bytes().first() == Some(&b'$') {
                dst.push('$');
                template = &template[1..];
                continue;
            }
            let Some((name, num, rest)) = extract(template) else {
                dst.push('$');
                continue;
            };
            template = rest;
            if num >= 0 {
                let num = num as usize;
                if 2 * num + 1 < m.len() && m[2 * num] >= 0 {
                    dst.push_str(&src[m[2 * num] as usize..m[2 * num + 1] as usize]);
                }
            } else {
                for (i, namei) in self.inner.subexp_names.iter().enumerate() {
                    if name == namei && 2 * i + 1 < m.len() && m[2 * i] >= 0 {
                        dst.push_str(&src[m[2 * i] as usize..m[2 * i + 1] as usize]);
                        break;
                    }
                }
            }
        }
        dst.push_str(template);
    }

    /// Go `ReplaceAllString`: `$` in `repl` expands as in `expand_string`.
    pub fn replace_all_string(&self, src: &str, repl: &str) -> String {
        let mut out = String::with_capacity(src.len());
        let mut last_match_end = 0;
        let mut search_pos = 0;
        while search_pos <= src.len() {
            let Some(a) = self.find_at(src, search_pos) else { break };
            let (a0, a1) = (a[0] as usize, a[1] as usize);
            out.push_str(&src[last_match_end..a0]);
            if a1 > last_match_end || a0 == 0 {
                self.expand_string(&mut out, repl, src, &a);
            }
            last_match_end = a1;
            let width = src[search_pos..].chars().next().map_or(0, |c| c.len_utf8());
            if search_pos + width > a1 {
                search_pos += width;
            } else if search_pos + 1 > a1 {
                search_pos += 1;
            } else {
                search_pos = a1;
            }
        }
        out.push_str(&src[last_match_end..]);
        out
    }
}

/// Go `extract`: the name from a leading "name" or "{name}", and its number (or -1).
fn extract(s: &str) -> Option<(&str, i64, &str)> {
    if s.is_empty() {
        return None;
    }
    let mut str = s;
    let brace = str.as_bytes()[0] == b'{';
    if brace {
        str = &str[1..];
    }
    let mut i = 0;
    for c in str.chars() {
        if !unicode::is_letter(c) && !unicode::is_digit(c) && c != '_' {
            break;
        }
        i += c.len_utf8();
    }
    if i == 0 {
        return None;
    }
    let name = &str[..i];
    if brace {
        if i >= str.len() || str.as_bytes()[i] != b'}' {
            return None;
        }
        i += 1;
    }
    let mut num: i64 = 0;
    for &b in name.as_bytes() {
        if !b.is_ascii_digit() || num >= 100_000_000 {
            num = -1;
            break;
        }
        num = num * 10 + (b - b'0') as i64;
    }
    if name.as_bytes()[0] == b'0' && name.len() > 1 {
        num = -1;
    }
    Some((name, num, &str[i..]))
}

/// Go `regexp.QuoteMeta`.
pub fn quote_meta(s: &str) -> Cow<'_, str> {
    fn special(b: u8) -> bool {
        matches!(b, b'\\' | b'.' | b'+' | b'*' | b'?' | b'(' | b')' | b'|' | b'[' | b']' | b'{' | b'}' | b'^' | b'$')
    }
    if !s.bytes().any(special) {
        return Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len() * 2);
    for c in s.chars() {
        if c.is_ascii() && special(c as u8) {
            out.push('\\');
        }
        out.push(c);
    }
    Cow::Owned(out)
}

const NO_MATCH: &str = r"[^\x{0}-\x{10FFFF}]";

fn push_char(out: &mut String, r: i32) {
    if let Some(c) = char::from_u32(r as u32) {
        if c.is_ascii_alphanumeric() {
            out.push(c);
            return;
        }
    }
    let _ = write!(out, "\\x{{{:X}}}", r);
}

/// Writes a class of `(lo, hi)` code point ranges, without surrogates (no Rust string contains them, and Go never
/// decodes them from input). An empty class never matches.
fn write_class(out: &mut String, ranges: impl Iterator<Item = (i32, i32)>) {
    let start = out.len();
    out.push('[');
    let mut any = false;
    for (lo, hi) in ranges {
        let mut parts = [(lo, hi.min(0xD7FF)), (lo.max(0xE000), hi)];
        if hi < 0xD800 {
            parts[1] = (1, 0);
        } else if lo > 0xDFFF {
            parts[0] = (1, 0);
        }
        for (a, b) in parts {
            if a > b {
                continue;
            }
            any = true;
            push_char(out, a);
            if b > a {
                out.push('-');
                push_char(out, b);
            }
        }
    }
    if !any {
        out.truncate(start);
        out.push_str(NO_MATCH);
        return;
    }
    out.push(']');
}

fn write_literal_rune(out: &mut String, r: i32, fold: bool) {
    if (0xD800..=0xDFFF).contains(&r) {
        out.push_str(NO_MATCH);
        return;
    }
    if fold {
        let mut orbit = vec![r];
        let mut f = unicode::simple_fold(r);
        while f != r {
            orbit.push(f);
            f = unicode::simple_fold(f);
        }
        if orbit.len() > 1 {
            orbit.sort_unstable();
            write_class(out, orbit.into_iter().map(|c| (c, c)));
            return;
        }
    }
    push_char(out, r);
}

fn write_group(re: &syntax::Regexp, out: &mut String) {
    out.push_str("(?:");
    write_rust(re, out);
    out.push(')');
}

/// Writes `re` as a `regex` crate pattern with the same meaning as Go's.
pub(crate) fn write_rust(re: &syntax::Regexp, out: &mut String) {
    match re.op {
        Op::NoMatch => out.push_str(NO_MATCH),
        Op::EmptyMatch => out.push_str("(?:)"),
        Op::Literal => {
            let fold = re.flags & syntax::FOLD_CASE != 0;
            for &r in &re.rune {
                write_literal_rune(out, r, fold);
            }
        }
        Op::CharClass => write_class(out, re.rune.chunks(2).map(|p| (p[0], p[1]))),
        Op::AnyCharNotNL => out.push_str(r"[^\n]"),
        Op::AnyChar => out.push_str("(?s:.)"),
        Op::BeginLine => out.push_str("(?m:^)"),
        Op::EndLine => out.push_str("(?m:$)"),
        Op::BeginText => out.push_str(r"\A"),
        Op::EndText => out.push_str(r"\z"),
        Op::WordBoundary => out.push_str(r"(?-u:\b)"),
        Op::NoWordBoundary => out.push_str(r"(?-u:\B)"),
        Op::Capture => {
            let _ = write!(out, "(?P<c{}>", re.cap);
            write_rust(&re.sub[0], out);
            out.push(')');
        }
        Op::Star | Op::Plus | Op::Quest | Op::Repeat => {
            write_group(&re.sub[0], out);
            match re.op {
                Op::Star => out.push('*'),
                Op::Plus => out.push('+'),
                Op::Quest => out.push('?'),
                _ => {
                    if re.max == -1 {
                        let _ = write!(out, "{{{},}}", re.min);
                    } else if re.min == re.max {
                        let _ = write!(out, "{{{}}}", re.min);
                    } else {
                        let _ = write!(out, "{{{},{}}}", re.min, re.max);
                    }
                }
            }
            if re.flags & syntax::NON_GREEDY != 0 {
                out.push('?');
            }
        }
        Op::Concat => {
            for s in &re.sub {
                write_group(s, out);
            }
        }
        Op::Alternate => {
            out.push_str("(?:");
            for (i, s) in re.sub.iter().enumerate() {
                if i > 0 {
                    out.push('|');
                }
                write_rust(s, out);
            }
            out.push(')');
        }
        Op::LeftParen | Op::VerticalBar => out.push_str(NO_MATCH),
    }
}
