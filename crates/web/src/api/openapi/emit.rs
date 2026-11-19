//! The YAML tree of the OpenAPI document and a writer that prints it as libopenapi's `Render` does
//! (`github.com/pb33f/go-yaml`: 4-space indent, no line folding, sequences indented under mapping keys).

use std::borrow::Cow;
use std::cmp::Ordering;

/// A string that is usually static.
pub type Str = Cow<'static, str>;

/// A YAML node.
#[derive(Clone, Debug, PartialEq)]
pub enum Node {
    /// A Go string (`!!str`): quoted when a plain scalar would read back as something else.
    Str(Str),
    /// A scalar node without a tag (Go `&yaml.Node{Kind: yaml.ScalarNode, Value: v}`): plain when the
    /// emitter allows it.
    Plain(Str),
    Int(i64),
    Float(f64),
    Bool(bool),
    Seq(Vec<Node>),
    /// A mapping with its keys in output order.
    Map(Vec<(Str, Node)>),
    /// JSON text decoded by go-yaml (Go `marshalToYAMLNode`): written in flow style.
    Flow(String),
}

impl From<&'static str> for Node {
    fn from(s: &'static str) -> Node {
        Node::Str(Cow::Borrowed(s))
    }
}
impl From<String> for Node {
    fn from(s: String) -> Node {
        Node::Str(Cow::Owned(s))
    }
}
impl From<i64> for Node {
    fn from(i: i64) -> Node {
        Node::Int(i)
    }
}
impl From<i32> for Node {
    fn from(i: i32) -> Node {
        Node::Int(i as i64)
    }
}
impl From<f64> for Node {
    fn from(f: f64) -> Node {
        Node::Float(f)
    }
}
impl From<bool> for Node {
    fn from(b: bool) -> Node {
        Node::Bool(b)
    }
}
impl From<Vec<Node>> for Node {
    fn from(v: Vec<Node>) -> Node {
        Node::Seq(v)
    }
}

/// A Go map marshaled by go-yaml: the keys sorted in go-yaml's key order.
pub fn go_map(mut pairs: Vec<(&'static str, Node)>) -> Node {
    pairs.sort_by(|a, b| key_cmp(a.0, b.0));
    Node::Map(
        pairs
            .into_iter()
            .map(|(k, v)| (Cow::Borrowed(k), v))
            .collect(),
    )
}

/// A Go `[]string` marshaled by go-yaml.
pub fn str_seq(v: &[&'static str]) -> Node {
    Node::Seq(v.iter().map(|s| Node::from(*s)).collect())
}

/// go-yaml `keyList.Less` for string keys.
fn key_cmp(a: &str, b: &str) -> Ordering {
    if key_less(a, b) {
        Ordering::Less
    } else if key_less(b, a) {
        Ordering::Greater
    } else {
        Ordering::Equal
    }
}

fn key_less(a: &str, b: &str) -> bool {
    let ar: Vec<char> = a.chars().collect();
    let br: Vec<char> = b.chars().collect();
    let mut digits = false;
    let mut i = 0;
    while i < ar.len() && i < br.len() {
        if ar[i] == br[i] {
            digits = ar[i].is_numeric();
            i += 1;
            continue;
        }
        let al = ar[i].is_alphabetic();
        let bl = br[i].is_alphabetic();
        if al && bl {
            return ar[i] < br[i];
        }
        if al || bl {
            return if digits { al } else { bl };
        }
        let (mut an, mut bn) = (0i64, 0i64);
        if ar[i] == '0' || br[i] == '0' {
            let mut j = i;
            while j > 0 && ar[j - 1].is_numeric() {
                j -= 1;
                if ar[j] != '0' {
                    an = 1;
                    bn = 1;
                    break;
                }
            }
        }
        let mut ai = i;
        while ai < ar.len() && ar[ai].is_ascii_digit() {
            an = an * 10 + (ar[ai] as i64 - '0' as i64);
            ai += 1;
        }
        let mut bi = i;
        while bi < br.len() && br[bi].is_ascii_digit() {
            bn = bn * 10 + (br[bi] as i64 - '0' as i64);
            bi += 1;
        }
        if an != bn {
            return an < bn;
        }
        if ai != bi {
            return ai < bi;
        }
        return ar[i] < br[i];
    }
    ar.len() < br.len()
}

// ---------------------------------------------------------------------------------------------------------
// Writer.

const INDENT: usize = 4;

/// Writes a document whose root is a mapping.
pub fn render(root: &Node) -> String {
    let mut out = String::with_capacity(256 * 1024);
    match root {
        Node::Map(pairs) if !pairs.is_empty() => write_map(&mut out, pairs, 0, 0, false),
        other => {
            write_inline(&mut out, other, false);
            out.push('\n');
        }
    }
    out
}

fn pad(out: &mut String, n: usize) {
    for _ in 0..n {
        out.push(' ');
    }
}

/// Writes mapping pairs with the keys at column `key_ind`; their values are indented from `base_ind`
/// (a mapping that is a sequence item has its keys after "- " but its values indented from the "-").
fn write_map(
    out: &mut String,
    pairs: &[(Str, Node)],
    key_ind: usize,
    base_ind: usize,
    mut inline_first: bool,
) {
    for (k, v) in pairs {
        if !inline_first {
            pad(out, key_ind);
        }
        inline_first = false;
        write_scalar(out, &Node::Str(k.clone()), true, false);
        out.push(':');
        write_value(out, v, base_ind);
    }
}

fn write_seq(out: &mut String, items: &[Node], ind: usize, mut inline_first: bool) {
    for v in items {
        if !inline_first {
            pad(out, ind);
        }
        inline_first = false;
        out.push('-');
        match v {
            Node::Map(p) if !p.is_empty() => {
                out.push(' ');
                write_map(out, p, ind + 2, ind, true);
            }
            Node::Seq(s) if !s.is_empty() => {
                out.push(' ');
                write_seq(out, s, ind + 2, true);
            }
            _ => write_value(out, v, ind),
        }
    }
}

/// Writes a value after `key:` or `-` (at indentation `ind`), ending with a line break.
fn write_value(out: &mut String, v: &Node, ind: usize) {
    match v {
        Node::Map(p) if !p.is_empty() => {
            out.push('\n');
            write_map(out, p, ind + INDENT, ind + INDENT, false);
        }
        Node::Seq(s) if !s.is_empty() => {
            out.push('\n');
            write_seq(out, s, ind + INDENT, false);
        }
        Node::Str(s) if s.contains('\n') && literal_allowed(s) => {
            out.push(' ');
            write_literal(out, s, ind + INDENT);
        }
        _ => {
            out.push(' ');
            write_inline(out, v, false);
            out.push('\n');
        }
    }
}

/// A scalar or an empty collection or a flow node on the current line.
fn write_inline(out: &mut String, v: &Node, flow: bool) {
    match v {
        Node::Seq(s) => {
            out.push('[');
            for (i, x) in s.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_inline(out, x, true);
            }
            out.push(']');
        }
        Node::Map(p) => {
            out.push('{');
            for (i, (k, x)) in p.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_scalar(out, &Node::Str(k.clone()), true, true);
                out.push_str(": ");
                write_inline(out, x, true);
            }
            out.push('}');
        }
        Node::Flow(json) => write_flow_json(out, json),
        _ => write_scalar(out, v, false, flow),
    }
}

/// go-yaml writes JSON it decoded as flow collections, keeping the double quotes of strings: the same text
/// with a space after each `:` and `,` outside strings.
fn write_flow_json(out: &mut String, json: &str) {
    let mut in_str = false;
    let mut esc = false;
    for c in json.chars() {
        out.push(c);
        if in_str {
            if esc {
                esc = false;
            } else if c == '\\' {
                esc = true;
            } else if c == '"' {
                in_str = false;
            }
        } else if c == '"' {
            in_str = true;
        } else if c == ':' || c == ',' {
            out.push(' ');
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Style {
    Plain,
    Single,
    Double,
}

struct Analysis {
    multiline: bool,
    flow_plain_allowed: bool,
    block_plain_allowed: bool,
    single_quoted_allowed: bool,
    block_allowed: bool,
}

fn is_printable(c: char) -> bool {
    matches!(c, '\u{0A}' | '\u{20}'..='\u{7E}' | '\u{85}' | '\u{A0}'..='\u{D7FF}' | '\u{E000}'..='\u{FFFD}' | '\u{10000}'..='\u{10FFFF}')
        && c != '\u{FEFF}'
}

/// libyaml `yaml_emitter_analyze_scalar`.
fn analyze(value: &str) -> Analysis {
    if value.is_empty() {
        return Analysis {
            multiline: false,
            flow_plain_allowed: false,
            block_plain_allowed: true,
            single_quoted_allowed: true,
            block_allowed: false,
        };
    }
    let (mut block_ind, mut flow_ind) = (false, false);
    if value.starts_with("---") || value.starts_with("...") {
        block_ind = true;
        flow_ind = true;
    }
    let (mut line_breaks, mut special, mut tabs) = (false, false, false);
    let (mut leading_space, mut leading_break, mut trailing_space, mut trailing_break) =
        (false, false, false, false);
    let (mut break_space, mut space_break) = (false, false);
    let (mut prev_space, mut prev_break) = (false, false);
    let mut preceded_by_ws = true;
    let chars: Vec<char> = value.chars().collect();
    let n = chars.len();
    for i in 0..n {
        let c = chars[i];
        let followed_by_ws = i + 1 >= n
            || matches!(
                chars[i + 1],
                ' ' | '\t' | '\r' | '\n' | '\u{85}' | '\u{2028}' | '\u{2029}'
            );
        if i == 0 {
            match c {
                '#' | ',' | '[' | ']' | '{' | '}' | '&' | '*' | '!' | '|' | '>' | '\'' | '"'
                | '%' | '@' | '`' => {
                    flow_ind = true;
                    block_ind = true;
                }
                '?' | ':' => {
                    flow_ind = true;
                    if followed_by_ws {
                        block_ind = true;
                    }
                }
                '-' => {
                    if followed_by_ws {
                        flow_ind = true;
                        block_ind = true;
                    }
                }
                _ => {}
            }
        } else {
            match c {
                ',' | '?' | '[' | ']' | '{' | '}' => flow_ind = true,
                ':' => {
                    flow_ind = true;
                    if followed_by_ws {
                        block_ind = true;
                    }
                }
                '#' => {
                    if preceded_by_ws {
                        flow_ind = true;
                        block_ind = true;
                    }
                }
                _ => {}
            }
        }
        if c == '\t' {
            tabs = true;
        } else if !is_printable(c) {
            special = true;
        }
        let is_break = matches!(c, '\r' | '\n' | '\u{85}' | '\u{2028}' | '\u{2029}');
        if c == ' ' {
            if i == 0 {
                leading_space = true;
            }
            if i + 1 == n {
                trailing_space = true;
            }
            if prev_break {
                break_space = true;
            }
            prev_space = true;
            prev_break = false;
        } else if is_break {
            line_breaks = true;
            if i == 0 {
                leading_break = true;
            }
            if i + 1 == n {
                trailing_break = true;
            }
            if prev_space {
                space_break = true;
            }
            prev_space = false;
            prev_break = true;
        } else {
            prev_space = false;
            prev_break = false;
        }
        preceded_by_ws = matches!(
            c,
            ' ' | '\t' | '\r' | '\n' | '\u{85}' | '\u{2028}' | '\u{2029}' | '\0'
        );
    }
    let mut a = Analysis {
        multiline: line_breaks,
        flow_plain_allowed: true,
        block_plain_allowed: true,
        single_quoted_allowed: true,
        block_allowed: true,
    };
    if leading_space || leading_break || trailing_space || trailing_break {
        a.flow_plain_allowed = false;
        a.block_plain_allowed = false;
    }
    if trailing_space {
        a.block_allowed = false;
    }
    if break_space {
        a.flow_plain_allowed = false;
        a.block_plain_allowed = false;
        a.single_quoted_allowed = false;
    }
    if space_break || tabs || special {
        a.flow_plain_allowed = false;
        a.block_plain_allowed = false;
        a.single_quoted_allowed = false;
    }
    if space_break || special {
        a.block_allowed = false;
    }
    if line_breaks {
        a.flow_plain_allowed = false;
        a.block_plain_allowed = false;
    }
    if flow_ind {
        a.flow_plain_allowed = false;
    }
    if block_ind {
        a.block_plain_allowed = false;
    }
    a
}

/// Whether a multi-line string is written as a literal block (go-yaml `shouldUseLiteralStyle` and the
/// emitter's `block_allowed`).
fn literal_allowed(s: &str) -> bool {
    s.chars().count() >= 2 && s.chars().any(|c| !c.is_whitespace()) && analyze(s).block_allowed
}

/// The style go-yaml's representer and desolver request for a Go string.
fn requested_style(s: &str) -> Style {
    if yaml::resolve::is_base60_float(s) || is_old_bool(s) || s == "<<" {
        return Style::Single;
    }
    match yaml::resolve::resolve("", s) {
        Ok((tag, _)) if tag != yaml::node::STR_TAG => Style::Double,
        _ => Style::Plain,
    }
}

fn is_old_bool(s: &str) -> bool {
    matches!(
        s,
        "y" | "Y"
            | "yes"
            | "Yes"
            | "YES"
            | "on"
            | "On"
            | "ON"
            | "n"
            | "N"
            | "no"
            | "No"
            | "NO"
            | "off"
            | "Off"
            | "OFF"
    )
}

/// Writes a scalar; `key` is the simple key context, `flow` the flow context.
fn write_scalar(out: &mut String, v: &Node, key: bool, flow: bool) {
    let (s, requested): (&str, Style) = match v {
        Node::Str(s) => (s, requested_style(s)),
        Node::Plain(s) => (s, Style::Plain),
        Node::Int(i) => {
            out.push_str(&i.to_string());
            return;
        }
        Node::Float(f) => {
            let s = yaml::format_float_g(*f);
            out.push_str(match s.as_str() {
                "+Inf" => ".inf",
                "-Inf" => "-.inf",
                "NaN" => ".nan",
                _ => &s,
            });
            return;
        }
        Node::Bool(b) => {
            out.push_str(if *b { "true" } else { "false" });
            return;
        }
        _ => unreachable!("not a scalar"),
    };
    let a = analyze(s);
    let mut style = if s.contains('\n') {
        Style::Double
    } else {
        requested
    };
    if key && a.multiline {
        style = Style::Double;
    }
    if style == Style::Plain
        && ((flow && !a.flow_plain_allowed)
            || (!flow && !a.block_plain_allowed)
            || (s.is_empty() && (flow || key)))
    {
        style = Style::Single;
    }
    if style == Style::Single && !a.single_quoted_allowed {
        style = Style::Double;
    }
    match style {
        Style::Plain => out.push_str(s),
        Style::Single => {
            out.push('\'');
            for c in s.chars() {
                if c == '\'' {
                    out.push('\'');
                }
                out.push(c);
            }
            out.push('\'');
        }
        Style::Double => write_double(out, s),
    }
}

/// libyaml `yaml_emitter_write_double_quoted_scalar` (without folding).
fn write_double(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        if !is_printable(c)
            || c == '\u{FEFF}'
            || matches!(
                c,
                '\r' | '\n' | '\u{85}' | '\u{2028}' | '\u{2029}' | '"' | '\\'
            )
        {
            out.push('\\');
            match c {
                '\0' => out.push('0'),
                '\u{07}' => out.push('a'),
                '\u{08}' => out.push('b'),
                '\t' => out.push('t'),
                '\n' => out.push('n'),
                '\u{0B}' => out.push('v'),
                '\u{0C}' => out.push('f'),
                '\r' => out.push('r'),
                '\u{1B}' => out.push('e'),
                '"' => out.push('"'),
                '\\' => out.push('\\'),
                '\u{85}' => out.push('N'),
                '\u{A0}' => out.push('_'),
                '\u{2028}' => out.push('L'),
                '\u{2029}' => out.push('P'),
                c => {
                    let v = c as u32;
                    if v <= 0xFF {
                        out.push_str(&format!("x{v:02X}"));
                    } else if v <= 0xFFFF {
                        out.push_str(&format!("u{v:04X}"));
                    } else {
                        out.push_str(&format!("U{v:08X}"));
                    }
                }
            }
        } else {
            out.push(c);
        }
    }
    out.push('"');
}

/// A literal block scalar (`|`) with its lines at indentation `ind`.
fn write_literal(out: &mut String, s: &str, ind: usize) {
    out.push('|');
    if s.starts_with(' ') || s.starts_with('\n') {
        out.push_str(&INDENT.to_string());
    }
    if !s.ends_with('\n') {
        out.push('-');
    } else if s.len() == 1 || s[..s.len() - 1].ends_with('\n') {
        out.push('+');
    }
    out.push('\n');
    let body = s.strip_suffix('\n').unwrap_or(s);
    for line in body.split('\n') {
        if !line.is_empty() {
            pad(out, ind);
            out.push_str(line);
        }
        out.push('\n');
    }
}
