//! Port of Go `html/template/transition.go`: context transitions over template text.

use super::content::{ContentType, attr_type, decode_css, ends_with_css_keyword, is_js_ident_part, next_js_ctx};
use super::context::*;
use crate::gofmt::quote;

/// Go `%q` of a byte slice.
pub(crate) fn qb(b: &[u8]) -> String {
    quote(&String::from_utf8_lossy(b))
}

/// Go `%.32q` of a byte slice: at most 32 runes, quoted.
pub(crate) fn qb32(b: &[u8]) -> String {
    let s = String::from_utf8_lossy(b);
    let t: String = s.chars().take(32).collect();
    quote(&t)
}

fn index_any(s: &[u8], chars: &[u8]) -> Option<usize> {
    s.iter().position(|c| chars.contains(c))
}

fn index_of(s: &[u8], sub: &[u8]) -> Option<usize> {
    if sub.is_empty() {
        return Some(0);
    }
    s.windows(sub.len()).position(|w| w == sub)
}

/// Go `transitionFunc[state]`.
pub fn transition(c: Context, s: &[u8]) -> (Context, usize) {
    match c.state {
        State::Text => t_text(c, s),
        State::Tag => t_tag(c, s),
        State::AttrName => t_attr_name(c, s),
        State::AfterName => t_after_name(c, s),
        State::BeforeValue => t_before_value(c, s),
        State::HtmlCmt => t_html_cmt(c, s),
        State::Rcdata => t_special_tag_end(c, s),
        State::Attr => (c, s.len()),
        State::Url | State::Srcset => t_url(c, s),
        State::MetaContent => t_meta_content(c, s),
        State::MetaContentUrl => t_meta_content_url(c, s),
        State::Js => t_js(c, s),
        State::JsDqStr | State::JsSqStr | State::JsRegexp => t_js_delimited(c, s),
        State::JsTmplLit => t_js_tmpl(c, s),
        State::JsBlockCmt | State::CssBlockCmt => t_block_cmt(c, s),
        State::JsLineCmt | State::JsHtmlOpenCmt | State::JsHtmlCloseCmt | State::CssLineCmt => t_line_cmt(c, s),
        State::Css => t_css(c, s),
        State::CssDqStr | State::CssSqStr | State::CssDqUrl | State::CssSqUrl | State::CssUrl => t_css_str(c, s),
        State::Error | State::Dead => (c, s.len()),
    }
}

const COMMENT_START: &[u8] = b"<!--";
const COMMENT_END: &[u8] = b"-->";

fn t_text(c: Context, s: &[u8]) -> (Context, usize) {
    let mut k = 0;
    loop {
        let Some(off) = s[k..].iter().position(|&b| b == b'<') else { return (c, s.len()) };
        let mut i = k + off;
        if i + 1 == s.len() {
            return (c, s.len());
        } else if i + 4 <= s.len() && &s[i..i + 4] == COMMENT_START {
            return (Context::with_state(State::HtmlCmt), i + 4);
        }
        i += 1;
        let mut end = false;
        if s[i] == b'/' {
            if i + 1 == s.len() {
                return (c, s.len());
            }
            end = true;
            i += 1;
        }
        let (j, mut e) = eat_tag_name(s, i);
        if j != i {
            if end {
                e = Element::None;
            }
            return (Context { state: State::Tag, element: e, ..Default::default() }, j);
        }
        k = j;
    }
}

fn element_content_type(e: Element) -> State {
    match e {
        Element::None => State::Text,
        Element::Script => State::Js,
        Element::Style => State::Css,
        Element::Textarea => State::Rcdata,
        Element::Title => State::Rcdata,
        Element::Meta => State::Text,
    }
}

fn t_tag(c: Context, s: &[u8]) -> (Context, usize) {
    let i = eat_white_space(s, 0);
    if i == s.len() {
        return (c, s.len());
    }
    if s[i] == b'>' {
        if c.element == Element::Meta {
            return (Context::with_state(State::Text), i + 1);
        }
        return (Context { state: element_content_type(c.element), element: c.element, ..Default::default() }, i + 1);
    }
    let j = match eat_attr_name(s, i) {
        Ok(j) => j,
        Err(e) => return (Context::error(e), s.len()),
    };
    if i == j {
        return (
            Context::error(errorf(
                ErrorCode::BadHtml,
                None,
                0,
                format!("expected space, attr name, or end of tag, but got {}", qb(&s[i..])),
            )),
            s.len(),
        );
    }
    let attr_name = String::from_utf8_lossy(&s[i..j]).to_lowercase();
    let attr = if c.element == Element::Script && attr_name == "type" {
        Attr::ScriptType
    } else if c.element == Element::Meta && attr_name == "content" {
        Attr::MetaContent
    } else {
        match attr_type(&attr_name) {
            ContentType::Url => Attr::Url,
            ContentType::Css => Attr::Style,
            ContentType::Js => Attr::Script,
            ContentType::Srcset => Attr::Srcset,
            _ => Attr::None,
        }
    };
    let state = if j == s.len() { State::AttrName } else { State::AfterName };
    (Context { state, element: c.element, attr, ..Default::default() }, j)
}

fn t_attr_name(mut c: Context, s: &[u8]) -> (Context, usize) {
    match eat_attr_name(s, 0) {
        Err(e) => (Context::error(e), s.len()),
        Ok(i) => {
            if i != s.len() {
                c.state = State::AfterName;
            }
            (c, i)
        }
    }
}

fn t_after_name(mut c: Context, s: &[u8]) -> (Context, usize) {
    let i = eat_white_space(s, 0);
    if i == s.len() {
        return (c, s.len());
    } else if s[i] != b'=' {
        c.state = State::Tag;
        return (c, i);
    }
    c.state = State::BeforeValue;
    (c, i + 1)
}

/// Go `attrStartStates`.
pub fn attr_start_state(a: Attr) -> State {
    match a {
        Attr::None => State::Attr,
        Attr::Script => State::Js,
        Attr::ScriptType => State::Attr,
        Attr::Style => State::Css,
        Attr::Url => State::Url,
        Attr::Srcset => State::Srcset,
        Attr::MetaContent => State::MetaContent,
    }
}

fn t_before_value(mut c: Context, s: &[u8]) -> (Context, usize) {
    let mut i = eat_white_space(s, 0);
    if i == s.len() {
        return (c, s.len());
    }
    let mut delim = Delim::SpaceOrTagEnd;
    match s[i] {
        b'\'' => {
            delim = Delim::SingleQuote;
            i += 1;
        }
        b'"' => {
            delim = Delim::DoubleQuote;
            i += 1;
        }
        _ => {}
    }
    c.state = attr_start_state(c.attr);
    c.delim = delim;
    (c, i)
}

fn t_html_cmt(c: Context, s: &[u8]) -> (Context, usize) {
    if let Some(i) = index_of(s, COMMENT_END) {
        return (Context::default(), i + 3);
    }
    (c, s.len())
}

fn special_tag_end_marker(e: Element) -> &'static [u8] {
    match e {
        Element::Script => b"script",
        Element::Style => b"style",
        Element::Textarea => b"textarea",
        Element::Title => b"title",
        Element::Meta | Element::None => b"",
    }
}

/// Go `tSpecialTagEnd`.
pub fn t_special_tag_end(c: Context, s: &[u8]) -> (Context, usize) {
    if c.element != Element::None {
        if c.element == Element::Script && (is_in_script_literal(c.state) || is_comment(c.state)) {
            return (c, s.len());
        }
        if let Some(i) = index_tag_end(s, special_tag_end_marker(c.element)) {
            return (Context::default(), i);
        }
    }
    (c, s.len())
}

fn index_tag_end(mut s: &[u8], tag: &[u8]) -> Option<usize> {
    let mut res = 0;
    let plen = 2;
    while !s.is_empty() {
        let i = index_of(s, b"</")?;
        s = &s[i + plen..];
        if tag.len() <= s.len() && s[..tag.len()].eq_ignore_ascii_case(tag) {
            s = &s[tag.len()..];
            if !s.is_empty() && b"> \t\n\x0c/".contains(&s[0]) {
                return Some(res + i);
            }
            res += tag.len();
        }
        res += i + plen;
    }
    None
}

/// Go `tURL`.
pub fn t_url(mut c: Context, s: &[u8]) -> (Context, usize) {
    if s.iter().any(|&b| b == b'#' || b == b'?') {
        c.url_part = UrlPart::QueryOrFrag;
    } else if s.len() != eat_white_space(s, 0) && c.url_part == UrlPart::None {
        c.url_part = UrlPart::PreQuery;
    }
    (c, s.len())
}

fn t_js(mut c: Context, s: &[u8]) -> (Context, usize) {
    let Some(mut i) = index_any(s, b"\"`'/{}<-#") else {
        c.js_ctx = next_js_ctx(s, c.js_ctx);
        return (c, s.len());
    };
    c.js_ctx = next_js_ctx(&s[..i], c.js_ctx);
    match s[i] {
        b'"' => {
            c.state = State::JsDqStr;
            c.js_ctx = JsCtx::Regexp;
        }
        b'\'' => {
            c.state = State::JsSqStr;
            c.js_ctx = JsCtx::Regexp;
        }
        b'`' => {
            c.state = State::JsTmplLit;
            c.js_ctx = JsCtx::Regexp;
        }
        b'/' => {
            if i + 1 < s.len() && s[i + 1] == b'/' {
                c.state = State::JsLineCmt;
                i += 1;
            } else if i + 1 < s.len() && s[i + 1] == b'*' {
                c.state = State::JsBlockCmt;
                i += 1;
            } else if c.js_ctx == JsCtx::Regexp {
                c.state = State::JsRegexp;
            } else if c.js_ctx == JsCtx::DivOp {
                c.js_ctx = JsCtx::Regexp;
            } else {
                return (
                    Context::error(errorf(
                        ErrorCode::SlashAmbig,
                        None,
                        0,
                        format!("'/' could start a division or regexp: {}", qb32(&s[i..])),
                    )),
                    s.len(),
                );
            }
        }
        b'<' => {
            if i + 3 < s.len() && &s[i..i + 4] == COMMENT_START {
                c.state = State::JsHtmlOpenCmt;
                i += 3;
            }
        }
        b'-' => {
            if i + 2 < s.len() && &s[i..i + 3] == COMMENT_END {
                c.state = State::JsHtmlCloseCmt;
                i += 2;
            }
        }
        b'#' => {
            if i + 1 < s.len() && s[i + 1] == b'!' {
                c.state = State::JsLineCmt;
                i += 1;
            }
        }
        b'{' => {
            let Some(bd) = c.js_brace_depth.as_mut().filter(|bd| !bd.is_empty()) else {
                c.js_ctx = next_js_ctx(&s[i..i + 1], c.js_ctx);
                return (c, i + 1);
            };
            let l = bd.len();
            bd[l - 1] += 1;
            c.js_ctx = next_js_ctx(&s[i..i + 1], c.js_ctx);
        }
        b'}' => {
            let Some(bd) = c.js_brace_depth.as_mut().filter(|bd| !bd.is_empty()) else {
                c.js_ctx = next_js_ctx(&s[i..i + 1], c.js_ctx);
                return (c, i + 1);
            };
            let l = bd.len();
            bd[l - 1] -= 1;
            if bd[l - 1] >= 0 {
                c.js_ctx = next_js_ctx(&s[i..i + 1], c.js_ctx);
                return (c, i + 1);
            }
            bd.pop();
            c.state = State::JsTmplLit;
        }
        _ => unreachable!(),
    }
    (c, i + 1)
}

fn t_js_tmpl(mut c: Context, s: &[u8]) -> (Context, usize) {
    let mut k = 0;
    while let Some(off) = index_any(&s[k..], b"`\\$") {
        let mut i = k + off;
        match s[i] {
            b'\\' => {
                i += 1;
                if i == s.len() {
                    return (
                        Context::error(errorf(
                            ErrorCode::PartialEscape,
                            None,
                            0,
                            format!("unfinished escape sequence in JS string: {}", qb(s)),
                        )),
                        s.len(),
                    );
                }
            }
            b'$' => {
                if s.len() >= i + 2 && s[i + 1] == b'{' {
                    c.js_brace_depth.get_or_insert_with(Vec::new).push(0);
                    c.state = State::Js;
                    return (c, i + 2);
                }
            }
            b'`' => {
                c.state = State::Js;
                return (c, i + 1);
            }
            _ => {}
        }
        k = i + 1;
    }
    (c, s.len())
}

fn t_js_delimited(mut c: Context, s: &[u8]) -> (Context, usize) {
    let specials: &[u8] = match c.state {
        State::JsSqStr => b"\\'",
        State::JsRegexp => b"\\/[]",
        _ => b"\\\"",
    };
    let mut k = 0;
    let mut in_charset = false;
    while let Some(off) = index_any(&s[k..], specials) {
        let mut i = k + off;
        match s[i] {
            b'\\' => {
                i += 1;
                if i == s.len() {
                    return (
                        Context::error(errorf(
                            ErrorCode::PartialEscape,
                            None,
                            0,
                            format!("unfinished escape sequence in JS string: {}", qb(s)),
                        )),
                        s.len(),
                    );
                }
            }
            b'[' => in_charset = true,
            b']' => in_charset = false,
            b'/' => {
                if i > 0 && i + 7 <= s.len() && s[i - 1..i + 7].eq_ignore_ascii_case(b"</script") {
                    i += 1;
                } else if !in_charset {
                    c.state = State::Js;
                    c.js_ctx = JsCtx::DivOp;
                    return (c, i + 1);
                }
            }
            _ => {
                if !in_charset {
                    c.state = State::Js;
                    c.js_ctx = JsCtx::DivOp;
                    return (c, i + 1);
                }
            }
        }
        k = i + 1;
    }
    if in_charset {
        return (
            Context::error(errorf(
                ErrorCode::PartialCharset,
                None,
                0,
                format!("unfinished JS regexp charset: {}", qb(s)),
            )),
            s.len(),
        );
    }
    (c, s.len())
}

fn t_block_cmt(mut c: Context, s: &[u8]) -> (Context, usize) {
    let Some(i) = index_of(s, b"*/") else { return (c, s.len()) };
    c.state = if c.state == State::JsBlockCmt { State::Js } else { State::Css };
    (c, i + 2)
}

fn t_line_cmt(mut c: Context, s: &[u8]) -> (Context, usize) {
    let (end_state, i) = match c.state {
        State::CssLineCmt => (State::Css, index_any(s, b"\n\x0c\r")),
        _ => {
            // "\n\r  " (bytes.IndexAny matches runes).
            let i = (0..s.len()).find(|&i| {
                matches!(s[i], b'\n' | b'\r')
                    || (s[i] == 0xE2 && s.len() >= i + 3 && s[i + 1] == 0x80 && matches!(s[i + 2], 0xA8 | 0xA9))
            });
            (State::Js, i)
        }
    };
    let Some(i) = i else { return (c, s.len()) };
    c.state = end_state;
    (c, i)
}

fn t_css(mut c: Context, s: &[u8]) -> (Context, usize) {
    let mut k = 0;
    loop {
        let Some(off) = index_any(&s[k..], b"(\"'/") else { return (c, s.len()) };
        let i = k + off;
        match s[i] {
            b'(' => {
                let p = trim_right(&s[..i], b"\t\n\x0c\r ");
                if ends_with_css_keyword(p, "url") {
                    let rest = &s[i + 1..];
                    let trimmed = trim_left(rest, b"\t\n\x0c\r ");
                    let mut j = s.len() - trimmed.len();
                    if j != s.len() && s[j] == b'"' {
                        c.state = State::CssDqUrl;
                        j += 1;
                    } else if j != s.len() && s[j] == b'\'' {
                        c.state = State::CssSqUrl;
                        j += 1;
                    } else {
                        c.state = State::CssUrl;
                    }
                    return (c, j);
                }
            }
            b'/' => {
                if i + 1 < s.len() {
                    match s[i + 1] {
                        b'/' => {
                            c.state = State::CssLineCmt;
                            return (c, i + 2);
                        }
                        b'*' => {
                            c.state = State::CssBlockCmt;
                            return (c, i + 2);
                        }
                        _ => {}
                    }
                }
            }
            b'"' => {
                c.state = State::CssDqStr;
                return (c, i + 1);
            }
            b'\'' => {
                c.state = State::CssSqStr;
                return (c, i + 1);
            }
            _ => {}
        }
        k = i + 1;
    }
}

fn t_css_str(mut c: Context, s: &[u8]) -> (Context, usize) {
    let end_and_esc: &[u8] = match c.state {
        State::CssDqStr | State::CssDqUrl => b"\\\"",
        State::CssSqStr | State::CssSqUrl => b"\\'",
        _ => b"\\\t\n\x0c\r )",
    };
    let mut k = 0;
    loop {
        let Some(off) = index_any(&s[k..], end_and_esc) else {
            let decoded = decode_css(&s[k..]);
            let (c2, nread) = t_url(c, &decoded);
            return (c2, k + nread);
        };
        let mut i = k + off;
        if s[i] == b'\\' {
            i += 1;
            if i == s.len() {
                return (
                    Context::error(errorf(
                        ErrorCode::PartialEscape,
                        None,
                        0,
                        format!("unfinished escape sequence in CSS string: {}", qb(s)),
                    )),
                    s.len(),
                );
            }
        } else {
            c.state = State::Css;
            return (c, i + 1);
        }
        let decoded = decode_css(&s[..i + 1]);
        c = t_url(c, &decoded).0;
        k = i + 1;
    }
}

fn t_meta_content(mut c: Context, s: &[u8]) -> (Context, usize) {
    for i in 0..s.len() {
        if i + 3 <= s.len().saturating_sub(1) && s[i..i + 3].eq_ignore_ascii_case(b"url") {
            let j = eat_white_space(s, i + 3);
            if j < s.len() && s[j] == b'=' {
                c.state = State::MetaContentUrl;
                return (c, j + 1);
            }
        }
    }
    (c, s.len())
}

fn t_meta_content_url(mut c: Context, s: &[u8]) -> (Context, usize) {
    for (i, &b) in s.iter().enumerate() {
        if b == b';' {
            c.state = State::MetaContent;
            return (c, i + 1);
        }
    }
    (c, s.len())
}

fn eat_attr_name(s: &[u8], i: usize) -> Result<usize, std::sync::Arc<Error>> {
    for j in i..s.len() {
        match s[j] {
            b' ' | b'\t' | b'\n' | b'\x0c' | b'\r' | b'=' | b'>' => return Ok(j),
            b'\'' | b'"' | b'<' => {
                return Err(errorf(
                    ErrorCode::BadHtml,
                    None,
                    0,
                    format!("{} in attribute name: {}", qb(&s[j..j + 1]), qb32(s)),
                ));
            }
            _ => {}
        }
    }
    Ok(s.len())
}

fn element_name(name: &str) -> Element {
    match name {
        "script" => Element::Script,
        "style" => Element::Style,
        "textarea" => Element::Textarea,
        "title" => Element::Title,
        "meta" => Element::Meta,
        _ => Element::None,
    }
}

fn eat_tag_name(s: &[u8], i: usize) -> (usize, Element) {
    if i == s.len() || !s[i].is_ascii_alphabetic() {
        return (i, Element::None);
    }
    let mut j = i + 1;
    while j < s.len() {
        let x = s[j];
        if x.is_ascii_alphanumeric() {
            j += 1;
            continue;
        }
        if (x == b':' || x == b'-') && j + 1 < s.len() && s[j + 1].is_ascii_alphanumeric() {
            j += 2;
            continue;
        }
        break;
    }
    (j, element_name(&String::from_utf8_lossy(&s[i..j]).to_lowercase()))
}

/// Go `eatWhiteSpace`.
pub fn eat_white_space(s: &[u8], i: usize) -> usize {
    for (j, &b) in s.iter().enumerate().skip(i) {
        if !matches!(b, b' ' | b'\t' | b'\n' | b'\x0c' | b'\r') {
            return j;
        }
    }
    s.len()
}

pub(crate) fn trim_right<'a>(s: &'a [u8], set: &[u8]) -> &'a [u8] {
    let mut e = s.len();
    while e > 0 && set.contains(&s[e - 1]) {
        e -= 1;
    }
    &s[..e]
}

pub(crate) fn trim_left<'a>(s: &'a [u8], set: &[u8]) -> &'a [u8] {
    let mut b = 0;
    while b < s.len() && set.contains(&s[b]) {
        b += 1;
    }
    &s[b..]
}

#[allow(dead_code)]
fn unused(r: char) -> bool {
    is_js_ident_part(r)
}
