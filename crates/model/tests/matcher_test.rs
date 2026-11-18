// Port of model/labels/matcher_test.go.

use model::labels::{MatchType, Matcher};

fn must_new_matcher(t: MatchType, value: &str) -> Matcher {
    Matcher::new(t, "test_label_name", value).unwrap()
}

#[test]
fn test_matcher() {
    let tests: Vec<(Matcher, &str, bool)> = vec![
        (must_new_matcher(MatchType::Equal, "bar"), "bar", true),
        (must_new_matcher(MatchType::Equal, "bar"), "foo-bar", false),
        (must_new_matcher(MatchType::NotEqual, "bar"), "bar", false),
        (must_new_matcher(MatchType::NotEqual, "bar"), "foo-bar", true),
        (must_new_matcher(MatchType::Regexp, "bar"), "bar", true),
        (must_new_matcher(MatchType::Regexp, "bar"), "foo-bar", false),
        (must_new_matcher(MatchType::Regexp, ".*bar"), "foo-bar", true),
        (must_new_matcher(MatchType::NotRegexp, "bar"), "bar", false),
        (must_new_matcher(MatchType::NotRegexp, "bar"), "foo-bar", true),
        (must_new_matcher(MatchType::NotRegexp, ".*bar"), "foo-bar", false),
        (must_new_matcher(MatchType::Regexp, "$*bar"), "foo-bar", false),
        (must_new_matcher(MatchType::Regexp, "bar^+"), "foo-bar", false),
        (must_new_matcher(MatchType::Regexp, "$+bar"), "foo-bar", false),
    ];
    for (m, value, want) in tests {
        assert_eq!(m.matches(value), want, "{} on {:?}", m, value);
    }
}

#[test]
fn test_inverse() {
    // adapted: Go builds the input matchers as struct literals without a compiled regexp; Rust builds them with
    // Matcher::new (the private regexp field cannot be left out). The assertion is the same.
    let tests = vec![
        (Matcher::new(MatchType::Equal, "name1", "value1").unwrap(), MatchType::NotEqual),
        (Matcher::new(MatchType::NotEqual, "name2", "value2").unwrap(), MatchType::Equal),
        (Matcher::new(MatchType::Regexp, "name3", "value3.*").unwrap(), MatchType::NotRegexp),
        (Matcher::new(MatchType::NotRegexp, "name4", "value4.*").unwrap(), MatchType::Regexp),
    ];
    for (m, expected) in tests {
        let result = m.inverse().unwrap();
        assert_eq!(expected, result.typ);
    }
}

#[test]
fn test_prefix() {
    let cases: Vec<(Matcher, &str, bool)> = vec![
        (must_new_matcher(MatchType::Equal, "abc"), "", false),
        (must_new_matcher(MatchType::NotEqual, "abc"), "", false),
        (must_new_matcher(MatchType::Regexp, "abc.+"), "abc", false),
        (must_new_matcher(MatchType::Regexp, "abcd|abc.+"), "abc", false),
        (must_new_matcher(MatchType::NotRegexp, "abcd|abc.+"), "abc", false),
        (must_new_matcher(MatchType::Regexp, "abc(def|ghj)|ab|a."), "a", false),
        (must_new_matcher(MatchType::Regexp, "foo.+bar|foo.*baz"), "foo", false),
        (must_new_matcher(MatchType::Regexp, "abc|.*"), "", false),
        (must_new_matcher(MatchType::Regexp, "abc|def"), "", false),
        (must_new_matcher(MatchType::Regexp, ".+def"), "", false),
        (must_new_matcher(MatchType::NotRegexp, "(?i)abc.+"), "", true),
    ];
    for (i, (m, prefix, ci)) in cases.iter().enumerate() {
        assert_eq!(*prefix, m.prefix(), "{}: {}", i, m);
        assert_eq!(*ci, m.has_case_insensitive_prefix(), "{}: {}", i, m);
    }
}

#[test]
fn test_is_regex_optimized() {
    let cases = vec![
        (must_new_matcher(MatchType::Equal, "abc"), false),
        (must_new_matcher(MatchType::Regexp, "."), false),
        (must_new_matcher(MatchType::Regexp, "abc.+"), true),
    ];
    for (i, (m, want)) in cases.iter().enumerate() {
        assert_eq!(*want, m.is_regex_optimized(), "{}: {}", i, m);
    }
}
