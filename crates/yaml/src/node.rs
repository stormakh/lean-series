//! YAML text to a node tree, as go-yaml v2 builds it (decode.go `parser`): scalars keep their text, tag and
//! "implicit" flag (plain style without a tag); mappings keep their pairs in order (duplicates kept, so the
//! decoder can report them); aliases point to the anchored node. Lines are 1-based (Go prints `line+1`).
use std::collections::HashMap;
use std::rc::Rc;

use yaml_rust2::parser::{Event, MarkedEventReceiver, Parser};
use yaml_rust2::scanner::{Marker, TScalarStyle};

pub const STR_TAG: &str = "tag:yaml.org,2002:str";
pub const BOOL_TAG: &str = "tag:yaml.org,2002:bool";
pub const INT_TAG: &str = "tag:yaml.org,2002:int";
pub const FLOAT_TAG: &str = "tag:yaml.org,2002:float";
pub const NULL_TAG: &str = "tag:yaml.org,2002:null";
pub const TIMESTAMP_TAG: &str = "tag:yaml.org,2002:timestamp";
pub const SEQ_TAG: &str = "tag:yaml.org,2002:seq";
pub const MAP_TAG: &str = "tag:yaml.org,2002:map";
pub const BINARY_TAG: &str = "tag:yaml.org,2002:binary";
pub const MERGE_TAG: &str = "tag:yaml.org,2002:merge";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Scalar,
    Mapping,
    Sequence,
    Alias,
}

#[derive(Debug)]
pub struct Node {
    pub kind: Kind,
    /// 1-based line of the node start.
    pub line: usize,
    pub column: usize,
    /// Long form ("tag:yaml.org,2002:str") or "" when the node has no tag. Mappings and sequences keep "".
    pub tag: String,
    /// Scalar text, or the anchor name of an alias.
    pub value: String,
    /// Plain style and no tag.
    pub implicit: bool,
    /// Sequence items, or mapping keys and values interleaved (k0, v0, k1, v1, ...).
    pub children: Vec<Rc<Node>>,
    pub alias: Option<Rc<Node>>,
}

impl Node {
    /// go-yaml's null test in `prepare`.
    pub fn is_null(&self) -> bool {
        self.tag == NULL_TAG || self.kind == Kind::Scalar && self.tag.is_empty() && (self.value == "null" || self.value == "~" || self.value.is_empty() && self.implicit)
    }
    pub fn is_merge(&self) -> bool {
        self.kind == Kind::Scalar && self.value == "<<" && (self.implicit || self.tag == MERGE_TAG)
    }
    /// Mapping pairs.
    pub fn pairs(&self) -> impl Iterator<Item = (&Rc<Node>, &Rc<Node>)> {
        self.children.chunks(2).map(|c| (&c[0], &c[1]))
    }
}

struct Frame {
    node: Node,
    anchor: usize,
}

struct Builder {
    stack: Vec<Frame>,
    anchors: HashMap<usize, Rc<Node>>,
    root: Option<Rc<Node>>,
    docs: usize,
    err: Option<String>,
}

fn long_tag(t: &yaml_rust2::parser::Tag) -> String {
    if t.handle == "!!" || t.handle == "tag:yaml.org,2002:" {
        format!("tag:yaml.org,2002:{}", t.suffix)
    } else {
        format!("{}{}", t.handle, t.suffix)
    }
}

impl Builder {
    fn add(&mut self, n: Rc<Node>, anchor: usize) {
        if anchor != 0 {
            self.anchors.insert(anchor, n.clone());
        }
        match self.stack.last_mut() {
            Some(f) => f.node.children.push(n),
            None => {
                if self.root.is_none() {
                    self.root = Some(n);
                }
            }
        }
    }
}

impl MarkedEventReceiver for Builder {
    fn on_event(&mut self, ev: Event, mark: Marker) {
        if self.err.is_some() || self.docs > 1 {
            return;
        }
        let line = mark.line();
        let column = mark.col();
        let mk = |kind: Kind| Node { kind, line, column, tag: String::new(), value: String::new(), implicit: false, children: Vec::new(), alias: None };
        match ev {
            Event::DocumentStart => self.docs += 1,
            Event::Scalar(v, style, anchor, tag) => {
                let mut n = mk(Kind::Scalar);
                n.implicit = style == TScalarStyle::Plain && tag.is_none();
                if let Some(t) = tag.as_ref() {
                    n.tag = long_tag(t);
                    if n.tag == "!" {
                        // non-specific tag: a quoted-like string
                        n.tag.clear();
                    }
                }
                n.value = v;
                self.add(Rc::new(n), anchor);
            }
            Event::SequenceStart(anchor, _) => self.stack.push(Frame { node: mk(Kind::Sequence), anchor }),
            Event::MappingStart(anchor, _) => self.stack.push(Frame { node: mk(Kind::Mapping), anchor }),
            Event::SequenceEnd | Event::MappingEnd => {
                if let Some(f) = self.stack.pop() {
                    self.add(Rc::new(f.node), f.anchor);
                }
            }
            Event::Alias(id) => match self.anchors.get(&id) {
                Some(target) => {
                    let mut n = mk(Kind::Alias);
                    n.alias = Some(target.clone());
                    self.add(Rc::new(n), 0);
                }
                None => self.err = Some(format!("unknown anchor '{id}' referenced")),
            },
            _ => {}
        }
    }
}

/// Parses the first document of `text`. `Ok(None)` for an empty stream (no document), as go-yaml.
pub fn parse(text: &str) -> Result<Option<Rc<Node>>, String> {
    let mut b = Builder { stack: Vec::new(), anchors: HashMap::new(), root: None, docs: 0, err: None };
    let mut p = Parser::new_from_str(text);
    if let Err(e) = p.load(&mut b, false) {
        return Err(scan_error(&e, text));
    }
    if let Some(e) = b.err {
        return Err(format!("yaml: {e}"));
    }
    Ok(b.root)
}

/// Formats a parser error as go-yaml does ("yaml: line N: problem"). libyaml keeps the context ("while parsing a
/// block mapping") apart from the problem and Go prints only the problem; yaml-rust2 joins them with ", ".
/// go-yaml prints no line for errors on the first line, the 1-based line for scanner errors and the 0-based
/// line for parser errors.
fn scan_error(e: &yaml_rust2::ScanError, text: &str) -> String {
    let mut info = e.info().to_string();
    if info.starts_with("while ")
        && let Some(i) = info.find(", ") {
            info = info[i + 2..].to_string();
        }
    let mut l0 = e.marker().line().saturating_sub(1);
    let mut parser_error = false;
    match info.as_str() {
        "did not find expected key" | "did not find expected node content" | "did not find expected '-' indicator" => parser_error = true,
        "expected ',' or ']'" => {
            info = "did not find expected ',' or ']'".to_string();
            parser_error = true;
        }
        "expected ',' or '}'" => {
            info = "did not find expected ',' or '}'".to_string();
            parser_error = true;
        }
        "found unexpected end of stream" => l0 = text.matches('\n').count(),
        "found unknown anchor" => {
            // go-yaml reports this while building nodes, without a line and with the name
            let idx = e.marker().index().min(text.len());
            let start = if text[idx..].starts_with('*') { idx + 1 } else { text[..idx].rfind('*').map(|i| i + 1).unwrap_or(idx) };
            let name: String = text[start..].chars().take_while(|c| !c.is_whitespace() && !matches!(c, ',' | ']' | '}')).collect();
            return format!("yaml: unknown anchor '{name}' referenced");
        }
        i if i.starts_with("tabs disallowed") => info = "found character that cannot start any token".to_string(),
        _ => {}
    }
    if l0 == 0 {
        return format!("yaml: {info}");
    }
    format!("yaml: line {}: {info}", if parser_error { l0 } else { l0 + 1 })
}
