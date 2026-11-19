//! Port of Go `text/template/parse`: the lexer, the parse tree and the parser.

pub mod lex;
pub mod node;
mod number;

use std::collections::HashMap;
use std::sync::Arc;

pub use node::*;

use lex::{Item, ItemType, LexOptions, Lexer};

use crate::gofmt::quote;

/// Go `strconv.ParseFloat(s, 64)` for finite decimal and hexadecimal numbers (`None` on error).
pub fn number_parse_float(s: &str) -> Option<f64> {
    number::parse_float(s)
}

/// Maximum nesting of parenthesized pipelines (Go `maxStackDepth`).
const MAX_STACK_DEPTH: usize = 10000;

/// Parsing modes (Go `parse.Mode`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Mode {
    /// Keep comments in the tree (Go `ParseComments`).
    pub parse_comments: bool,
    /// Do not check that functions are defined (Go `SkipFuncCheck`).
    pub skip_func_check: bool,
}

/// A parsed template (Go `parse.Tree`).
#[derive(Clone, Debug)]
pub struct Tree {
    /// The name of the template.
    pub name: String,
    /// The name of the top-level template during parsing, for error messages.
    pub parse_name: String,
    /// The root list; `None` for an incomplete template.
    pub root: Option<ListNode>,
    /// The text parsed to create the template (or its parent).
    pub text: Arc<str>,
    pub mode: Mode,
}

impl Tree {
    /// Go `Tree.Copy`: a deep copy with new node ids.
    pub fn copy(&self) -> Tree {
        Tree {
            name: self.name.clone(),
            parse_name: self.parse_name.clone(),
            root: self.root.as_ref().map(ListNode::copy),
            text: self.text.clone(),
            mode: self.mode,
        }
    }

    /// Go `ErrorContext`: the location `name:line:col` of the byte position `pos` (nodes of a tree hold
    /// positions in this tree's text).
    pub fn error_location(&self, pos: usize) -> String {
        let pos = pos.min(self.text.len());
        let text = &self.text.as_bytes()[..pos];
        let byte_num = match text.iter().rposition(|&c| c == b'\n') {
            None => pos,
            Some(i) => pos - (i + 1),
        };
        let line_num = 1 + text.iter().filter(|&&c| c == b'\n').count();
        format!("{}:{}:{}", self.parse_name, line_num, byte_num)
    }
}

/// The per-tree parser state (Go keeps it in each `Tree`).
struct TreeState {
    name: String,
    token: [Item; 3],
    peek_count: usize,
    vars: Vec<String>,
    action_line: usize,
    range_depth: usize,
    stack_depth: usize,
}

impl TreeState {
    fn new(name: &str) -> TreeState {
        let empty = Item { typ: ItemType::Eof, pos: 0, val: String::new(), line: 0 };
        TreeState {
            name: name.to_string(),
            token: [empty.clone(), empty.clone(), empty],
            peek_count: 0,
            vars: vec!["$".to_string()],
            action_line: 0,
            range_depth: 0,
            stack_depth: 0,
        }
    }
}

type PResult<T> = Result<T, String>;

struct Parser<'a, 'f> {
    lex: Lexer<'a>,
    st: TreeState,
    parse_name: String,
    text: Arc<str>,
    mode: Mode,
    has_func: &'f (dyn Fn(&str) -> bool + Sync),
    tree_set: HashMap<String, Tree>,
}

/// Go `parse.Parse`: parses `text` as the template `name`, returning the trees of the template and of its
/// `{{define}}` and `{{block}}` templates. `has_func` tells whether a function name is defined.
pub fn parse(
    name: &str,
    text: &str,
    left_delim: &str,
    right_delim: &str,
    mode: Mode,
    has_func: &(dyn Fn(&str) -> bool + Sync),
) -> Result<HashMap<String, Tree>, String> {
    let mut lex = Lexer::new(text, left_delim, right_delim);
    lex.options = LexOptions {
        emit_comment: mode.parse_comments,
        break_ok: !has_func("break"),
        continue_ok: !has_func("continue"),
    };
    let mut p = Parser {
        lex,
        st: TreeState::new(name),
        parse_name: name.to_string(),
        text: Arc::from(text),
        mode,
        has_func,
        tree_set: HashMap::new(),
    };
    let root = p.parse()?;
    p.add(name, Some(root))?;
    Ok(p.tree_set)
}

impl<'a, 'f> Parser<'a, 'f> {
    fn next(&mut self) -> Item {
        if self.st.peek_count > 0 {
            self.st.peek_count -= 1;
        } else {
            self.st.token[0] = self.lex.next_item();
        }
        self.st.token[self.st.peek_count].clone()
    }

    fn backup(&mut self) {
        self.st.peek_count += 1;
    }

    fn backup2(&mut self, t1: Item) {
        self.st.token[1] = t1;
        self.st.peek_count = 2;
    }

    fn backup3(&mut self, t2: Item, t1: Item) {
        self.st.token[1] = t1;
        self.st.token[2] = t2;
        self.st.peek_count = 3;
    }

    fn peek(&mut self) -> Item {
        if self.st.peek_count > 0 {
            return self.st.token[self.st.peek_count - 1].clone();
        }
        self.st.peek_count = 1;
        self.st.token[0] = self.lex.next_item();
        self.st.token[0].clone()
    }

    fn next_non_space(&mut self) -> Item {
        loop {
            let token = self.next();
            if token.typ != ItemType::Space {
                return token;
            }
        }
    }

    fn peek_non_space(&mut self) -> Item {
        let token = self.next_non_space();
        self.backup();
        token
    }

    /// Go `Tree.errorf`.
    fn errorf<T>(&self, msg: String) -> PResult<T> {
        Err(format!("template: {}:{}: {}", self.parse_name, self.st.token[0].line, msg))
    }

    fn expect(&mut self, expected: ItemType, context: &str) -> PResult<Item> {
        let token = self.next_non_space();
        if token.typ != expected {
            return self.unexpected(&token, context);
        }
        Ok(token)
    }

    fn expect_one_of(&mut self, e1: ItemType, e2: ItemType, context: &str) -> PResult<Item> {
        let token = self.next_non_space();
        if token.typ != e1 && token.typ != e2 {
            return self.unexpected(&token, context);
        }
        Ok(token)
    }

    fn unexpected<T>(&self, token: &Item, context: &str) -> PResult<T> {
        if token.typ == ItemType::Error {
            let mut extra = String::new();
            if self.st.action_line != 0 && self.st.action_line != token.line {
                extra = format!(" in action started at {}:{}", self.parse_name, self.st.action_line);
                if token.val.ends_with(" action") {
                    extra = extra[" in action".len()..].to_string();
                }
            }
            return self.errorf(format!("{}{}", token, extra));
        }
        self.errorf(format!("unexpected {} in {}", token, context))
    }

    /// Go `Tree.add`.
    fn add(&mut self, name: &str, root: Option<ListNode>) -> PResult<()> {
        let existing_empty = match self.tree_set.get(name) {
            None => true,
            Some(t) => is_empty_tree(t.root.as_ref()),
        };
        if existing_empty {
            self.tree_set.insert(
                name.to_string(),
                Tree {
                    name: name.to_string(),
                    parse_name: self.parse_name.clone(),
                    root,
                    text: self.text.clone(),
                    mode: self.mode,
                },
            );
            return Ok(());
        }
        if !is_empty_tree(root.as_ref()) {
            return self.errorf(format!("template: multiple definition of template {}", quote(name)));
        }
        Ok(())
    }

    /// Go `Tree.parse`: the top-level template.
    fn parse(&mut self) -> PResult<ListNode> {
        let mut root = ListNode { pos: self.peek().pos, nodes: Vec::new() };
        while self.peek().typ != ItemType::Eof {
            if self.peek().typ == ItemType::LeftDelim {
                let delim = self.next();
                if self.next_non_space().typ == ItemType::Define {
                    let saved = std::mem::replace(&mut self.st, TreeState::new("definition"));
                    let r = self.parse_definition();
                    self.st = saved;
                    r?;
                    continue;
                }
                self.backup2(delim);
            }
            let n = self.text_or_action()?;
            match n.node_type() {
                NodeType::End | NodeType::Else => return self.errorf(format!("unexpected {}", n)),
                _ => root.nodes.push(n),
            }
        }
        Ok(root)
    }

    fn parse_definition(&mut self) -> PResult<()> {
        let context = "define clause";
        let name = self.expect_one_of(ItemType::String, ItemType::RawString, context)?;
        match crate::parse::number::unquote(&name.val) {
            Ok(n) => self.st.name = n,
            Err(e) => return self.errorf(e.to_string()),
        }
        self.expect(ItemType::RightDelim, context)?;
        let (root, end) = self.item_list()?;
        if end.node_type() != NodeType::End {
            return self.errorf(format!("unexpected {} in {}", end, context));
        }
        let name = self.st.name.clone();
        self.add(&name, Some(root))
    }

    fn item_list(&mut self) -> PResult<(ListNode, Node)> {
        let mut list = ListNode { pos: self.peek_non_space().pos, nodes: Vec::new() };
        while self.peek_non_space().typ != ItemType::Eof {
            let n = self.text_or_action()?;
            match n.node_type() {
                NodeType::End | NodeType::Else => return Ok((list, n)),
                _ => list.nodes.push(n),
            }
        }
        self.errorf("unexpected EOF".to_string())
    }

    fn text_or_action(&mut self) -> PResult<Node> {
        let token = self.next_non_space();
        match token.typ {
            ItemType::Text => Ok(Node::Text(TextNode { id: new_id(), pos: token.pos, text: token.val.into_bytes() })),
            ItemType::LeftDelim => {
                self.st.action_line = token.line;
                let r = self.action();
                self.st.action_line = 0;
                r
            }
            ItemType::Comment => Ok(Node::Comment { pos: token.pos, text: token.val }),
            _ => self.unexpected(&token, "input"),
        }
    }

    fn action(&mut self) -> PResult<Node> {
        let token = self.next_non_space();
        match token.typ {
            ItemType::Block => return self.block_control(),
            ItemType::Break => return self.break_control(token.pos, token.line),
            ItemType::Continue => return self.continue_control(token.pos, token.line),
            ItemType::Else => return self.else_control(),
            ItemType::End => return self.end_control(),
            ItemType::If => return self.if_control(),
            ItemType::Range => return self.range_control(),
            ItemType::Template => return self.template_control(),
            ItemType::With => return self.with_control(),
            _ => {}
        }
        self.backup();
        let token = self.peek();
        let pipe = self.pipeline("command", ItemType::RightDelim)?;
        Ok(Node::Action(ActionNode { id: new_id(), pos: token.pos, line: token.line, pipe }))
    }

    fn break_control(&mut self, pos: usize, line: usize) -> PResult<Node> {
        let token = self.next_non_space();
        if token.typ != ItemType::RightDelim {
            return self.unexpected(&token, "{{break}}");
        }
        if self.st.range_depth == 0 {
            return self.errorf("{{break}} outside {{range}}".to_string());
        }
        Ok(Node::Break { pos, line })
    }

    fn continue_control(&mut self, pos: usize, line: usize) -> PResult<Node> {
        let token = self.next_non_space();
        if token.typ != ItemType::RightDelim {
            return self.unexpected(&token, "{{continue}}");
        }
        if self.st.range_depth == 0 {
            return self.errorf("{{continue}} outside {{range}}".to_string());
        }
        Ok(Node::Continue { pos, line })
    }

    fn new_variable(pos: usize, ident: &str) -> VariableNode {
        VariableNode { pos, ident: ident.split('.').map(str::to_string).collect() }
    }

    fn pipeline(&mut self, context: &str, end: ItemType) -> PResult<PipeNode> {
        let token = self.peek_non_space();
        let mut pipe = PipeNode { pos: token.pos, line: token.line, is_assign: false, decl: Vec::new(), cmds: Vec::new() };
        'decls: loop {
            let v = self.peek_non_space();
            if v.typ == ItemType::Variable {
                self.next();
                let token_after_variable = self.peek();
                let next = self.peek_non_space();
                if next.typ == ItemType::Assign || next.typ == ItemType::Declare {
                    pipe.is_assign = next.typ == ItemType::Assign;
                    self.next_non_space();
                    pipe.decl.push(Self::new_variable(v.pos, &v.val));
                    self.st.vars.push(v.val.clone());
                } else if next.typ == ItemType::Char && next.val == "," {
                    self.next_non_space();
                    pipe.decl.push(Self::new_variable(v.pos, &v.val));
                    self.st.vars.push(v.val.clone());
                    if context == "range" && pipe.decl.len() < 2 {
                        match self.peek_non_space().typ {
                            ItemType::Variable | ItemType::RightDelim | ItemType::RightParen => continue 'decls,
                            _ => return self.errorf("range can only initialize variables".to_string()),
                        }
                    }
                    return self.errorf(format!("too many declarations in {}", context));
                } else if token_after_variable.typ == ItemType::Space {
                    self.backup3(v, token_after_variable);
                } else {
                    self.backup2(v);
                }
            }
            break;
        }
        loop {
            let token = self.next_non_space();
            if token.typ == end {
                self.check_pipeline(&pipe, context)?;
                return Ok(pipe);
            }
            match token.typ {
                ItemType::Bool
                | ItemType::CharConstant
                | ItemType::Complex
                | ItemType::Dot
                | ItemType::Field
                | ItemType::Identifier
                | ItemType::Number
                | ItemType::Nil
                | ItemType::RawString
                | ItemType::String
                | ItemType::Variable
                | ItemType::LeftParen => {
                    self.backup();
                    let cmd = self.command()?;
                    pipe.cmds.push(cmd);
                }
                _ => return self.unexpected(&token, context),
            }
        }
    }

    fn check_pipeline(&self, pipe: &PipeNode, context: &str) -> PResult<()> {
        if pipe.cmds.is_empty() {
            return self.errorf(format!("missing value for {}", context));
        }
        for (i, c) in pipe.cmds[1..].iter().enumerate() {
            match c.args[0].node_type() {
                NodeType::Bool | NodeType::Dot | NodeType::Nil | NodeType::Number | NodeType::String => {
                    return self.errorf(format!("non executable command in pipeline stage {}", i + 2));
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn parse_control(&mut self, context: &str) -> PResult<BranchNode> {
        let nvars = self.st.vars.len();
        let r = self.parse_control_inner(context);
        self.st.vars.truncate(nvars);
        r
    }

    fn parse_control_inner(&mut self, context: &str) -> PResult<BranchNode> {
        let pipe = self.pipeline(context, ItemType::RightDelim)?;
        if context == "range" {
            self.st.range_depth += 1;
        }
        let r = self.item_list();
        if context == "range" {
            self.st.range_depth -= 1;
        }
        let (list, next) = r?;
        let mut else_list = None;
        if next.node_type() == NodeType::Else {
            if context == "if" && self.peek().typ == ItemType::If {
                self.next();
                let mut l = ListNode { pos: next.pos(), nodes: Vec::new() };
                l.nodes.push(self.if_control()?);
                else_list = Some(l);
            } else if context == "with" && self.peek().typ == ItemType::With {
                self.next();
                let mut l = ListNode { pos: next.pos(), nodes: Vec::new() };
                l.nodes.push(self.with_control()?);
                else_list = Some(l);
            } else {
                let (l, next) = self.item_list()?;
                if next.node_type() != NodeType::End {
                    return self.errorf(format!("expected end; found {}", next));
                }
                else_list = Some(l);
            }
        }
        Ok(BranchNode { pos: pipe.pos, line: pipe.line, pipe, list, else_list })
    }

    fn if_control(&mut self) -> PResult<Node> {
        Ok(Node::If(Box::new(self.parse_control("if")?)))
    }

    fn range_control(&mut self) -> PResult<Node> {
        Ok(Node::Range(Box::new(self.parse_control("range")?)))
    }

    fn with_control(&mut self) -> PResult<Node> {
        Ok(Node::With(Box::new(self.parse_control("with")?)))
    }

    fn end_control(&mut self) -> PResult<Node> {
        Ok(Node::End(self.expect(ItemType::RightDelim, "end")?.pos))
    }

    fn else_control(&mut self) -> PResult<Node> {
        let peek = self.peek_non_space();
        if peek.typ == ItemType::If || peek.typ == ItemType::With {
            return Ok(Node::Else { pos: peek.pos, line: peek.line });
        }
        let token = self.expect(ItemType::RightDelim, "else")?;
        Ok(Node::Else { pos: token.pos, line: token.line })
    }

    fn block_control(&mut self) -> PResult<Node> {
        let context = "block clause";
        let token = self.next_non_space();
        let name = self.parse_template_name(&token, context)?;
        let pipe = self.pipeline(context, ItemType::RightDelim)?;

        let saved = std::mem::replace(&mut self.st, TreeState::new(&name));
        let res = match self.item_list() {
            Err(e) => Err(e),
            Ok((_, end)) if end.node_type() != NodeType::End => Ok(Some(end)),
            Ok((root, _)) => self.add(&name, Some(root)).map(|_| None),
        };
        self.st = saved;
        if let Some(end) = res? {
            return self.errorf(format!("unexpected {} in {}", end, context));
        }
        Ok(Node::Template(TemplateNode { id: new_id(), pos: token.pos, line: token.line, name, pipe: Some(pipe) }))
    }

    fn template_control(&mut self) -> PResult<Node> {
        let context = "template clause";
        let token = self.next_non_space();
        let name = self.parse_template_name(&token, context)?;
        let mut pipe = None;
        if self.next_non_space().typ != ItemType::RightDelim {
            self.backup();
            pipe = Some(self.pipeline(context, ItemType::RightDelim)?);
        }
        Ok(Node::Template(TemplateNode { id: new_id(), pos: token.pos, line: token.line, name, pipe }))
    }

    fn parse_template_name(&mut self, token: &Item, context: &str) -> PResult<String> {
        match token.typ {
            ItemType::String | ItemType::RawString => match crate::parse::number::unquote(&token.val) {
                Ok(s) => Ok(s),
                Err(e) => self.errorf(e.to_string()),
            },
            _ => self.unexpected(token, context),
        }
    }

    fn command(&mut self) -> PResult<CommandNode> {
        let mut cmd = CommandNode { pos: self.peek_non_space().pos, args: Vec::new() };
        loop {
            self.peek_non_space();
            if let Some(operand) = self.operand()? {
                cmd.args.push(operand);
            }
            let token = self.next();
            match token.typ {
                ItemType::Space => continue,
                ItemType::RightDelim | ItemType::RightParen => self.backup(),
                ItemType::Pipe => {}
                _ => return self.unexpected(&token, "operand"),
            }
            break;
        }
        if cmd.args.is_empty() {
            return self.errorf("empty command".to_string());
        }
        Ok(cmd)
    }

    fn operand(&mut self) -> PResult<Option<Node>> {
        let Some(node) = self.term()? else { return Ok(None) };
        if self.peek().typ == ItemType::Field {
            let pos = self.peek().pos;
            let mut fields = Vec::new();
            while self.peek().typ == ItemType::Field {
                let f = self.next().val;
                // Go ChainNode.Add: drop the leading dot.
                fields.push(f[1..].to_string());
            }
            let chain_string = || {
                let mut s = String::new();
                node.write_to(&mut s);
                for f in &fields {
                    s.push('.');
                    s.push_str(f);
                }
                s
            };
            return Ok(Some(match node.node_type() {
                NodeType::Field => Node::Field(FieldNode {
                    pos,
                    ident: chain_string()[1..].split('.').map(str::to_string).collect(),
                }),
                NodeType::Variable => Node::Variable(Self::new_variable(pos, &chain_string())),
                NodeType::Bool | NodeType::String | NodeType::Number | NodeType::Nil | NodeType::Dot => {
                    return self.errorf(format!("unexpected . after term {}", quote(&node.to_string())));
                }
                _ => Node::Chain(ChainNode { pos, node: Box::new(node), field: fields }),
            }));
        }
        Ok(Some(node))
    }

    fn term(&mut self) -> PResult<Option<Node>> {
        let token = self.next_non_space();
        match token.typ {
            ItemType::Identifier => {
                if !self.mode.skip_func_check && !(self.has_func)(&token.val) {
                    return self.errorf(format!("function {} not defined", quote(&token.val)));
                }
                Ok(Some(Node::Identifier { pos: token.pos, ident: token.val }))
            }
            ItemType::Dot => Ok(Some(Node::Dot(token.pos))),
            ItemType::Nil => Ok(Some(Node::Nil(token.pos))),
            ItemType::Variable => self.use_var(token.pos, &token.val).map(Some),
            ItemType::Field => Ok(Some(Node::Field(FieldNode {
                pos: token.pos,
                ident: token.val[1..].split('.').map(str::to_string).collect(),
            }))),
            ItemType::Bool => Ok(Some(Node::Bool { pos: token.pos, val: token.val == "true" })),
            ItemType::CharConstant | ItemType::Complex | ItemType::Number => {
                match number::new_number(token.pos, &token.val, token.typ) {
                    Ok(n) => Ok(Some(Node::Number(Box::new(n)))),
                    Err(e) => self.errorf(e),
                }
            }
            ItemType::LeftParen => {
                if self.st.stack_depth >= MAX_STACK_DEPTH {
                    return self.errorf("max expression depth exceeded".to_string());
                }
                self.st.stack_depth += 1;
                let depth = self.st.stack_depth;
                let r = crate::stack::maybe_grow(depth, || self.pipeline("parenthesized pipeline", ItemType::RightParen));
                self.st.stack_depth -= 1;
                Ok(Some(Node::Pipe(Box::new(r?))))
            }
            ItemType::String | ItemType::RawString => match number::unquote(&token.val) {
                Ok(s) => Ok(Some(Node::String(StringNode { pos: token.pos, quoted: token.val, text: s }))),
                Err(e) => self.errorf(e.to_string()),
            },
            _ => {
                self.backup();
                Ok(None)
            }
        }
    }

    fn use_var(&mut self, pos: usize, name: &str) -> PResult<Node> {
        let v = Self::new_variable(pos, name);
        if self.st.vars.iter().any(|n| *n == v.ident[0]) {
            return Ok(Node::Variable(v));
        }
        self.errorf(format!("undefined variable {}", quote(&v.ident[0])))
    }
}
