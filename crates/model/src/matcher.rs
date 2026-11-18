//! Port of `model/labels/matcher.go`.

use std::fmt;
use std::sync::Arc;

use crate::regexp::{Error, FastRegexMatcher};

/// The type of a label matcher.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum MatchType {
    Equal,
    NotEqual,
    Regexp,
    NotRegexp,
}

impl MatchType {
    /// The PromQL operator: `=`, `!=`, `=~`, `!~`.
    pub fn as_str(self) -> &'static str {
        match self {
            MatchType::Equal => "=",
            MatchType::NotEqual => "!=",
            MatchType::Regexp => "=~",
            MatchType::NotRegexp => "!~",
        }
    }
}

impl fmt::Display for MatchType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Matches a label value. Cloning is cheap (the compiled regexp is shared).
#[derive(Clone)]
pub struct Matcher {
    pub typ: MatchType,
    pub name: String,
    pub value: String,
    re: Option<Arc<FastRegexMatcher>>,
}

impl Matcher {
    /// Go `NewMatcher`: compiles the regexp for the regexp match types.
    pub fn new(typ: MatchType, name: &str, value: &str) -> Result<Matcher, Error> {
        let re = match typ {
            MatchType::Regexp | MatchType::NotRegexp => Some(Arc::new(FastRegexMatcher::new(value)?)),
            _ => None,
        };
        Ok(Matcher { typ, name: name.to_string(), value: value.to_string(), re })
    }

    /// Go `MustNewMatcher`: panics on error (for tests).
    pub fn must_new(typ: MatchType, name: &str, value: &str) -> Matcher {
        match Matcher::new(typ, name, value) {
            Ok(m) => m,
            Err(e) => panic!("{}", e),
        }
    }

    fn should_quote_name(&self) -> bool {
        for (i, c) in self.name.chars().enumerate() {
            if c == '_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit()) {
                continue;
            }
            return true;
        }
        self.name.is_empty()
    }

    /// Whether the matcher matches the value `s`.
    #[inline]
    pub fn matches(&self, s: &str) -> bool {
        match self.typ {
            MatchType::Equal => s == self.value,
            MatchType::NotEqual => s != self.value,
            MatchType::Regexp => self.re.as_ref().is_some_and(|re| re.match_string(s)),
            MatchType::NotRegexp => !self.re.as_ref().is_some_and(|re| re.match_string(s)),
        }
    }

    /// A matcher that matches the opposite.
    pub fn inverse(&self) -> Result<Matcher, Error> {
        let typ = match self.typ {
            MatchType::Equal => MatchType::NotEqual,
            MatchType::NotEqual => MatchType::Equal,
            MatchType::Regexp => MatchType::NotRegexp,
            MatchType::NotRegexp => MatchType::Regexp,
        };
        match &self.re {
            // Same pattern: share the compiled regexp instead of compiling it again.
            Some(re) => Ok(Matcher { typ, name: self.name.clone(), value: self.value.clone(), re: Some(re.clone()) }),
            None => Matcher::new(typ, &self.name, &self.value),
        }
    }

    /// The regexp string ("" for equality matchers).
    pub fn get_regex_string(&self) -> &str {
        self.re.as_ref().map_or("", |re| re.get_regex_string())
    }

    /// Equality values that the regexp is limited to, if any (`a(b|f)` gives `ab`, `af`).
    pub fn set_matches(&self) -> Vec<String> {
        self.re.as_ref().map_or_else(Vec::new, |re| re.set_matches())
    }

    /// The literal prefix the value must start with, byte for byte ("" when unknown).
    pub fn prefix(&self) -> &str {
        match &self.re {
            Some(re) if !re.case_insensitive_prefix() => re.prefix(),
            _ => "",
        }
    }

    /// Whether the regexp prefix is case-insensitive.
    pub fn has_case_insensitive_prefix(&self) -> bool {
        self.re.as_ref().is_some_and(|re| re.case_insensitive_prefix())
    }

    /// Whether a fast path applies to the regexp.
    pub fn is_regex_optimized(&self) -> bool {
        self.re.as_ref().is_some_and(|re| re.is_optimized())
    }

    /// The compiled regexp matcher, if any.
    pub fn regex(&self) -> Option<&FastRegexMatcher> {
        self.re.as_deref()
    }
}

impl fmt::Display for Matcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::with_capacity(self.name.len() + self.value.len() + 6);
        if self.should_quote_name() {
            crate::unicode::append_quote(&mut s, &self.name);
        } else {
            s.push_str(&self.name);
        }
        s.push_str(self.typ.as_str());
        crate::unicode::append_quote(&mut s, &self.value);
        f.write_str(&s)
    }
}

impl fmt::Debug for Matcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl PartialEq for Matcher {
    fn eq(&self, o: &Matcher) -> bool {
        self.typ == o.typ && self.name == o.name && self.value == o.value
    }
}

impl Eq for Matcher {}
