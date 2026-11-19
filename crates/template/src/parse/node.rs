//! Port of Go `text/template/parse/node.go`: the parse tree.
//!
//! Text, action and template nodes carry a unique `id` (Go identifies them by pointer): `html/template` records
//! its edits by node and applies them later. [`Node::copy`] gives the copy new ids, as a Go copy has new pointers.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::gofmt::quote;

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// A new node id.
pub fn new_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

/// Node types (Go `NodeType`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeType {
    Text,
    Action,
    Bool,
    Chain,
    Command,
    Dot,
    Else,
    End,
    Field,
    Identifier,
    If,
    List,
    Nil,
    Number,
    Pipe,
    Range,
    String,
    Template,
    Variable,
    With,
    Comment,
    Break,
    Continue,
}

/// A list of nodes (Go `ListNode`).
#[derive(Clone, Debug, Default)]
pub struct ListNode {
    pub pos: usize,
    pub nodes: Vec<Node>,
}

/// Plain text (Go `TextNode`).
#[derive(Clone, Debug)]
pub struct TextNode {
    pub id: u64,
    pub pos: usize,
    pub text: Vec<u8>,
}

/// A pipeline with optional declarations (Go `PipeNode`).
#[derive(Clone, Debug, Default)]
pub struct PipeNode {
    pub pos: usize,
    pub line: usize,
    pub is_assign: bool,
    pub decl: Vec<VariableNode>,
    pub cmds: Vec<CommandNode>,
}

/// A non-control action such as a field evaluation (Go `ActionNode`).
#[derive(Clone, Debug)]
pub struct ActionNode {
    pub id: u64,
    pub pos: usize,
    pub line: usize,
    pub pipe: PipeNode,
}

/// A command: an element of a pipeline (Go `CommandNode`).
#[derive(Clone, Debug, Default)]
pub struct CommandNode {
    pub pos: usize,
    pub args: Vec<Node>,
}

/// A variable with optional fields (Go `VariableNode`).
#[derive(Clone, Debug)]
pub struct VariableNode {
    pub pos: usize,
    pub ident: Vec<String>,
}

/// A field chain on dot (Go `FieldNode`).
#[derive(Clone, Debug)]
pub struct FieldNode {
    pub pos: usize,
    pub ident: Vec<String>,
}

/// Fields of a term (Go `ChainNode`).
#[derive(Clone, Debug)]
pub struct ChainNode {
    pub pos: usize,
    pub node: Box<Node>,
    pub field: Vec<String>,
}

/// A numeric constant (Go `NumberNode`).
#[derive(Clone, Debug, Default)]
pub struct NumberNode {
    pub pos: usize,
    pub is_int: bool,
    pub is_uint: bool,
    pub is_float: bool,
    pub is_complex: bool,
    pub int64: i64,
    pub uint64: u64,
    pub float64: f64,
    pub complex128: (f64, f64),
    pub text: String,
}

/// A string constant (Go `StringNode`).
#[derive(Clone, Debug)]
pub struct StringNode {
    pub pos: usize,
    pub quoted: String,
    pub text: String,
}

/// An if, range or with action (Go `BranchNode`).
#[derive(Clone, Debug)]
pub struct BranchNode {
    pub pos: usize,
    pub line: usize,
    pub pipe: PipeNode,
    pub list: ListNode,
    pub else_list: Option<ListNode>,
}

/// A `{{template}}` action (Go `TemplateNode`).
#[derive(Clone, Debug)]
pub struct TemplateNode {
    pub id: u64,
    pub pos: usize,
    pub line: usize,
    pub name: String,
    pub pipe: Option<PipeNode>,
}

/// A parse tree node (Go `parse.Node`).
#[derive(Clone, Debug)]
pub enum Node {
    Text(TextNode),
    Action(ActionNode),
    Bool { pos: usize, val: bool },
    Chain(ChainNode),
    Command(CommandNode),
    Dot(usize),
    Else { pos: usize, line: usize },
    End(usize),
    Field(FieldNode),
    Identifier { pos: usize, ident: String },
    If(Box<BranchNode>),
    List(ListNode),
    Nil(usize),
    Number(Box<NumberNode>),
    Pipe(Box<PipeNode>),
    Range(Box<BranchNode>),
    String(StringNode),
    Template(TemplateNode),
    Variable(VariableNode),
    With(Box<BranchNode>),
    Comment { pos: usize, text: String },
    Break { pos: usize, line: usize },
    Continue { pos: usize, line: usize },
}

impl Node {
    /// Go `Type()`.
    pub fn node_type(&self) -> NodeType {
        match self {
            Node::Text(_) => NodeType::Text,
            Node::Action(_) => NodeType::Action,
            Node::Bool { .. } => NodeType::Bool,
            Node::Chain(_) => NodeType::Chain,
            Node::Command(_) => NodeType::Command,
            Node::Dot(_) => NodeType::Dot,
            Node::Else { .. } => NodeType::Else,
            Node::End(_) => NodeType::End,
            Node::Field(_) => NodeType::Field,
            Node::Identifier { .. } => NodeType::Identifier,
            Node::If(_) => NodeType::If,
            Node::List(_) => NodeType::List,
            Node::Nil(_) => NodeType::Nil,
            Node::Number(_) => NodeType::Number,
            Node::Pipe(_) => NodeType::Pipe,
            Node::Range(_) => NodeType::Range,
            Node::String(_) => NodeType::String,
            Node::Template(_) => NodeType::Template,
            Node::Variable(_) => NodeType::Variable,
            Node::With(_) => NodeType::With,
            Node::Comment { .. } => NodeType::Comment,
            Node::Break { .. } => NodeType::Break,
            Node::Continue { .. } => NodeType::Continue,
        }
    }

    /// Go `Position()`.
    pub fn pos(&self) -> usize {
        match self {
            Node::Text(n) => n.pos,
            Node::Action(n) => n.pos,
            Node::Bool { pos, .. } => *pos,
            Node::Chain(n) => n.pos,
            Node::Command(n) => n.pos,
            Node::Dot(p) | Node::End(p) | Node::Nil(p) => *p,
            Node::Else { pos, .. } => *pos,
            Node::Field(n) => n.pos,
            Node::Identifier { pos, .. } => *pos,
            Node::If(b) | Node::Range(b) | Node::With(b) => b.pos,
            Node::List(n) => n.pos,
            Node::Number(n) => n.pos,
            Node::Pipe(n) => n.pos,
            Node::String(n) => n.pos,
            Node::Template(n) => n.pos,
            Node::Variable(n) => n.pos,
            Node::Comment { pos, .. } => *pos,
            Node::Break { pos, .. } | Node::Continue { pos, .. } => *pos,
        }
    }

    /// Go `writeTo`.
    pub fn write_to(&self, sb: &mut String) {
        match self {
            Node::Text(n) => sb.push_str(&String::from_utf8_lossy(&n.text)),
            Node::Action(n) => {
                sb.push_str("{{");
                n.pipe.write_to(sb);
                sb.push_str("}}");
            }
            Node::Bool { val, .. } => sb.push_str(if *val { "true" } else { "false" }),
            Node::Chain(n) => n.write_to(sb),
            Node::Command(n) => n.write_to(sb),
            Node::Dot(_) => sb.push('.'),
            Node::Else { .. } => sb.push_str("{{else}}"),
            Node::End(_) => sb.push_str("{{end}}"),
            Node::Field(n) => {
                for id in &n.ident {
                    sb.push('.');
                    sb.push_str(id);
                }
            }
            Node::Identifier { ident, .. } => sb.push_str(ident),
            Node::If(b) => b.write_to(sb, "if"),
            Node::Range(b) => b.write_to(sb, "range"),
            Node::With(b) => b.write_to(sb, "with"),
            Node::List(l) => l.write_to(sb),
            Node::Nil(_) => sb.push_str("nil"),
            Node::Number(n) => sb.push_str(&n.text),
            Node::Pipe(p) => p.write_to(sb),
            Node::String(s) => sb.push_str(&s.quoted),
            Node::Template(t) => {
                sb.push_str("{{template ");
                sb.push_str(&quote(&t.name));
                if let Some(p) = &t.pipe {
                    sb.push(' ');
                    p.write_to(sb);
                }
                sb.push_str("}}");
            }
            Node::Variable(v) => v.write_to(sb),
            Node::Comment { text, .. } => {
                sb.push_str("{{");
                sb.push_str(text);
                sb.push_str("}}");
            }
            Node::Break { .. } => sb.push_str("{{break}}"),
            Node::Continue { .. } => sb.push_str("{{continue}}"),
        }
    }

    /// Go `Copy()`: a deep copy with new node ids.
    pub fn copy(&self) -> Node {
        match self {
            Node::Text(n) => Node::Text(TextNode { id: new_id(), pos: n.pos, text: n.text.clone() }),
            Node::Action(n) => Node::Action(ActionNode { id: new_id(), pos: n.pos, line: n.line, pipe: n.pipe.copy() }),
            Node::Chain(c) => Node::Chain(ChainNode { pos: c.pos, node: Box::new(c.node.copy()), field: c.field.clone() }),
            Node::Command(c) => Node::Command(c.copy()),
            Node::If(b) => Node::If(Box::new(b.copy())),
            Node::Range(b) => Node::Range(Box::new(b.copy())),
            Node::With(b) => Node::With(Box::new(b.copy())),
            Node::List(l) => Node::List(l.copy()),
            Node::Pipe(p) => Node::Pipe(Box::new(p.copy())),
            Node::Template(t) => Node::Template(TemplateNode {
                id: new_id(),
                pos: t.pos,
                line: t.line,
                name: t.name.clone(),
                pipe: t.pipe.as_ref().map(|p| p.copy()),
            }),
            other => other.clone(),
        }
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        self.write_to(&mut s);
        f.write_str(&s)
    }
}

impl ListNode {
    pub fn write_to(&self, sb: &mut String) {
        for n in &self.nodes {
            n.write_to(sb);
        }
    }

    /// Go `CopyList`.
    pub fn copy(&self) -> ListNode {
        ListNode { pos: self.pos, nodes: self.nodes.iter().map(Node::copy).collect() }
    }
}

impl std::fmt::Display for ListNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        self.write_to(&mut s);
        f.write_str(&s)
    }
}

impl PipeNode {
    pub fn write_to(&self, sb: &mut String) {
        if !self.decl.is_empty() {
            for (i, v) in self.decl.iter().enumerate() {
                if i > 0 {
                    sb.push_str(", ");
                }
                v.write_to(sb);
            }
            sb.push_str(if self.is_assign { " = " } else { " := " });
        }
        for (i, c) in self.cmds.iter().enumerate() {
            if i > 0 {
                sb.push_str(" | ");
            }
            c.write_to(sb);
        }
    }

    /// Go `CopyPipe`.
    pub fn copy(&self) -> PipeNode {
        PipeNode {
            pos: self.pos,
            line: self.line,
            is_assign: self.is_assign,
            decl: self.decl.clone(),
            cmds: self.cmds.iter().map(CommandNode::copy).collect(),
        }
    }
}

/// Moves the pipelines nested in `cmds` (arguments and chain bases) to `stack`.
fn take_nested_pipes(cmds: &mut [CommandNode], stack: &mut Vec<Box<PipeNode>>) {
    for c in cmds.iter_mut() {
        for a in c.args.iter_mut() {
            let slot: &mut Node = match a {
                Node::Chain(ch) => &mut ch.node,
                other => other,
            };
            if matches!(slot, Node::Pipe(_)) {
                if let Node::Pipe(p) = std::mem::replace(slot, Node::Nil(0)) {
                    stack.push(p);
                }
            }
        }
    }
}

impl Drop for PipeNode {
    /// Drops nested parenthesized pipelines iteratively: they may nest 10000 deep.
    fn drop(&mut self) {
        let mut stack: Vec<Box<PipeNode>> = Vec::new();
        take_nested_pipes(&mut self.cmds, &mut stack);
        while let Some(mut p) = stack.pop() {
            take_nested_pipes(&mut p.cmds, &mut stack);
        }
    }
}

impl std::fmt::Display for PipeNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        self.write_to(&mut s);
        f.write_str(&s)
    }
}

impl CommandNode {
    pub fn write_to(&self, sb: &mut String) {
        for (i, arg) in self.args.iter().enumerate() {
            if i > 0 {
                sb.push(' ');
            }
            if let Node::Pipe(p) = arg {
                sb.push('(');
                p.write_to(sb);
                sb.push(')');
                continue;
            }
            arg.write_to(sb);
        }
    }

    pub fn copy(&self) -> CommandNode {
        CommandNode { pos: self.pos, args: self.args.iter().map(Node::copy).collect() }
    }
}

impl std::fmt::Display for CommandNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        self.write_to(&mut s);
        f.write_str(&s)
    }
}

impl VariableNode {
    pub fn write_to(&self, sb: &mut String) {
        for (i, id) in self.ident.iter().enumerate() {
            if i > 0 {
                sb.push('.');
            }
            sb.push_str(id);
        }
    }
}

impl ChainNode {
    pub fn write_to(&self, sb: &mut String) {
        if let Node::Pipe(p) = &*self.node {
            sb.push('(');
            p.write_to(sb);
            sb.push(')');
        } else {
            self.node.write_to(sb);
        }
        for field in &self.field {
            sb.push('.');
            sb.push_str(field);
        }
    }
}

impl std::fmt::Display for ChainNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();
        self.write_to(&mut s);
        f.write_str(&s)
    }
}

impl BranchNode {
    pub fn write_to(&self, sb: &mut String, name: &str) {
        sb.push_str("{{");
        sb.push_str(name);
        sb.push(' ');
        self.pipe.write_to(sb);
        sb.push_str("}}");
        self.list.write_to(sb);
        if let Some(e) = &self.else_list {
            sb.push_str("{{else}}");
            e.write_to(sb);
        }
        sb.push_str("{{end}}");
    }

    pub fn copy(&self) -> BranchNode {
        BranchNode {
            pos: self.pos,
            line: self.line,
            pipe: self.pipe.copy(),
            list: self.list.copy(),
            else_list: self.else_list.as_ref().map(ListNode::copy),
        }
    }
}

/// Go `IsEmptyTree`.
pub fn is_empty_tree(n: Option<&ListNode>) -> bool {
    match n {
        None => true,
        Some(l) => l.nodes.iter().all(is_empty_node),
    }
}

fn is_empty_node(n: &Node) -> bool {
    match n {
        Node::Comment { .. } => true,
        Node::List(l) => l.nodes.iter().all(is_empty_node),
        Node::Text(t) => text_is_space(&t.text),
        _ => false,
    }
}

/// Go `bytes.TrimSpace(b)` is empty: only Unicode white space.
fn text_is_space(b: &[u8]) -> bool {
    String::from_utf8_lossy(b).chars().all(char::is_whitespace)
}
