//! Port of `util/strutil/quote_test.go`. Go strings hold bytes (`"\xff"` is one byte), so expected values are
//! byte strings and the check uses `unquote_bytes`, the exact form of Go `Unquote`.

use util::strutil::{ERR_SYNTAX, unquote_bytes};

/// Go `quotetests`: (in, out, ascii).
const QUOTE_TESTS: &[(&[u8], &str, &str)] = &[
    (b"\x07\x08\x0c\r\n\t\x0b", r#""\a\b\f\r\n\t\v""#, r#""\a\b\f\r\n\t\v""#),
    (b"\\", r#""\\""#, r#""\\""#),
    (b"abc\xffdef", r#""abc\xffdef""#, r#""abc\xffdef""#),
    ("\u{263a}".as_bytes(), "\"☺\"", r#""\u263a""#),
    ("\u{10ffff}".as_bytes(), r#""\U0010ffff""#, r#""\U0010ffff""#),
    (b"\x04", r#""\x04""#, r#""\x04""#),
];

/// Go `unquotetests`: (in, out).
const UNQUOTE_TESTS: &[(&str, &[u8])] = &[
    (r#""""#, b""),
    (r#""a""#, b"a"),
    (r#""abc""#, b"abc"),
    (r#""☺""#, "☺".as_bytes()),
    (r#""hello world""#, b"hello world"),
    (r#""\xFF""#, b"\xFF"),
    (r#""\377""#, b"\xff"),
    (r#""\u1234""#, "\u{1234}".as_bytes()),
    (r#""\U00010111""#, "\u{10111}".as_bytes()),
    (r#""\U0001011111""#, "\u{10111}11".as_bytes()),
    (r#""\a\b\f\n\r\t\v\\\"""#, b"\x07\x08\x0c\n\r\t\x0b\\\""),
    (r#""'""#, b"'"),
    (r#"''"#, b""),
    (r#"'a'"#, b"a"),
    (r#"'abc'"#, b"abc"),
    (r#"'☺'"#, "☺".as_bytes()),
    (r#"'hello world'"#, b"hello world"),
    (r#"'\ahéllo world'"#, "\x07héllo world".as_bytes()),
    (r#"'\xFF'"#, b"\xFF"),
    (r#"'\377'"#, b"\xff"),
    (r#"'\u1234'"#, "\u{1234}".as_bytes()),
    (r#"'\U00010111'"#, "\u{10111}".as_bytes()),
    (r#"'\U0001011111'"#, "\u{10111}11".as_bytes()),
    (r#"'\a\b\f\n\r\t\v\\\''"#, b"\x07\x08\x0c\n\r\t\x0b\\'"),
    (r#"'"'"#, b"\""),
    ("``", b""),
    ("`a`", b"a"),
    ("`abc`", b"abc"),
    ("`☺`", "☺".as_bytes()),
    ("`hello world`", b"hello world"),
    ("`\\xFF`", br"\xFF"),
    ("`\\377`", br"\377"),
    ("`\\`", br"\"),
    ("`\n`", b"\n"),
    ("`\t`", b"\t"),
];

/// Go `misquoted`.
const MISQUOTED: &[&str] = &[
    "",
    r#"""#,
    r#""a"#,
    r#""'"#,
    r#"b""#,
    r#""\""#,
    r#""\9""#,
    r#""\19""#,
    r#""\129""#,
    r#"'\'"#,
    r#"'\9'"#,
    r#"'\19'"#,
    r#"'\129'"#,
    r#"'\400'"#,
    r#""\x1!""#,
    r#""\U12345678""#,
    r#""\z""#,
    "`",
    "`xxx",
    "`\"",
    r#""\'""#,
    r#"'\"'"#,
    "\"\n\"",
    "\"\\n\n\"",
    "'\n'",
    "`1`9`",
    "1231",
    r#"'\xF'"#,
    r#"""12345""#,
];

#[test]
fn test_unquote() {
    for &(input, out) in UNQUOTE_TESTS {
        // As in Go, the output is compared only when Unquote fails (then it is empty).
        let res = unquote_bytes(input);
        if let Err(_) = res {
            assert_eq!(out, b"", "Unquote({input:?})");
        }
    }

    // Run the quote tests too, backward.
    for &(input, out, _) in QUOTE_TESTS {
        let got = unquote_bytes(out);
        assert_eq!(got.as_deref(), Ok(input), "Unquote({out:?})");
    }

    for &s in MISQUOTED {
        let res = unquote_bytes(s);
        assert_eq!(res, Err(ERR_SYNTAX), "Unquote({s:?})");
    }
}
