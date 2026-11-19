//! Port of Go `html/template/escape.go`: the contextual escaper. It walks templates, computes the context at
//! each action and records which escaping functions each pipeline needs; `commit` applies the edits.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use super::content::{self, is_js_type};
use super::context::*;
use super::transition::{attr_start_state, qb, t_special_tag_end, transition};
use crate::gofmt::quote;
use crate::parse::{ActionNode, BranchNode, CommandNode, ListNode, Node, PipeNode, TemplateNode, TextNode, Tree};
use crate::text::{self, FuncMap};
use crate::value::{Func, StrKind, Type, Value};

/// Go `funcMap` of html/template: the escaping functions the escaper inserts.
pub(crate) fn escaper_funcs() -> FuncMap {
    fn f(g: fn(&[Value]) -> String) -> Func {
        Func::variadic(vec![Type::Interface], Type::String(StrKind::String), false, move |a| Ok(Value::string(g(a))))
    }
    let mut m = FuncMap::new();
    m.insert("_html_template_attrescaper".into(), f(content::attr_escaper));
    m.insert("_html_template_commentescaper".into(), f(content::comment_escaper));
    m.insert("_html_template_cssescaper".into(), f(content::css_escaper));
    m.insert("_html_template_cssvaluefilter".into(), f(content::css_value_filter));
    m.insert("_html_template_htmlnamefilter".into(), f(content::html_name_filter));
    m.insert("_html_template_htmlescaper".into(), f(content::html_escaper));
    m.insert("_html_template_jsregexpescaper".into(), f(content::js_regexp_escaper));
    m.insert("_html_template_jsstrescaper".into(), f(content::js_str_escaper));
    m.insert("_html_template_jstmpllitescaper".into(), f(content::js_tmpl_lit_escaper));
    m.insert("_html_template_jsvalescaper".into(), f(content::js_val_escaper));
    m.insert("_html_template_nospaceescaper".into(), f(content::html_nospace_escaper));
    m.insert("_html_template_rcdataescaper".into(), f(content::rcdata_escaper));
    m.insert("_html_template_srcsetescaper".into(), f(content::srcset_filter_and_escaper));
    m.insert("_html_template_urlescaper".into(), f(content::url_escaper));
    m.insert("_html_template_urlfilter".into(), f(content::url_filter));
    m.insert("_html_template_urlnormalizer".into(), f(content::url_normalizer));
    m.insert("_eval_args_".into(), f(content::eval_args));
    m
}

#[derive(Default)]
struct RangeContext {
    outer: Option<Arc<Mutex<RangeContext>>>,
    breaks: Vec<Context>,
    continues: Vec<Context>,
}

fn lock(rc: &Arc<Mutex<RangeContext>>) -> std::sync::MutexGuard<'_, RangeContext> {
    match rc.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

/// What the escaper needs from the template set: the text templates and the html template names.
pub(crate) struct Env<'a> {
    pub text: &'a text::Template,
    pub html_names: &'a HashSet<String>,
}

impl Env<'_> {
    /// Go `escaper.template`: the text template `name` (`Some(None)` when it has no tree), or a derived one.
    fn template(&self, e: &Escaper, name: &str) -> Option<Option<Arc<Tree>>> {
        let c = self.text.common.lock();
        if let Some(t) = c.tmpl.get(name) {
            return Some(t.clone());
        }
        drop(c);
        e.derived.get(name).map(|t| Some(t.clone()))
    }
}

/// Go `escaper`.
#[derive(Default)]
pub(crate) struct Escaper {
    output: HashMap<String, Context>,
    derived: HashMap<String, Arc<Tree>>,
    called: HashSet<String>,
    action_node_edits: HashMap<u64, Vec<String>>,
    template_node_edits: HashMap<u64, String>,
    text_node_edits: HashMap<u64, Vec<u8>>,
    range_context: Option<Arc<Mutex<RangeContext>>>,
}

fn is_predefined_escaper(s: &str) -> bool {
    s == "html" || s == "urlquery"
}

fn normalize_esc_fn(e: &str) -> &str {
    match e {
        "_html_template_attrescaper" | "_html_template_htmlescaper" | "_html_template_rcdataescaper" => "html",
        "_html_template_urlescaper" | "_html_template_urlnormalizer" => "urlquery",
        _ => e,
    }
}

fn redundant(last: &str, next: &str) -> bool {
    match last {
        "_html_template_commentescaper" => {
            next == "_html_template_attrescaper" || next == "_html_template_htmlescaper"
        }
        "_html_template_cssescaper" | "_html_template_jsregexpescaper" | "_html_template_jsstrescaper"
        | "_html_template_jstmpllitescaper" => next == "_html_template_attrescaper",
        "_html_template_urlescaper" => next == "_html_template_urlnormalizer",
        _ => false,
    }
}

fn first_ident(cmd: &CommandNode) -> Option<&str> {
    match cmd.args.first() {
        Some(Node::Identifier { ident, .. }) => Some(ident),
        _ => None,
    }
}

fn append_cmd(cmds: &mut Vec<CommandNode>, cmd: CommandNode) {
    if let (Some(last), Some(next)) = (cmds.last().and_then(first_ident), first_ident(&cmd))
        && redundant(last, next) {
            return;
        }
    cmds.push(cmd);
}

fn new_ident_cmd(identifier: &str, pos: usize) -> CommandNode {
    CommandNode { pos: 0, args: vec![Node::Identifier { pos, ident: identifier.to_string() }] }
}

/// Go `ensurePipelineContains`.
fn ensure_pipeline_contains(p: &mut PipeNode, s: &[String]) {
    if s.is_empty() {
        return;
    }
    let mut s: Vec<String> = s.to_vec();
    let mut pipeline_len = p.cmds.len();
    if pipeline_len > 0 {
        let last_ident = first_ident(&p.cmds[pipeline_len - 1]).map(str::to_string);
        if let Some(esc) = last_ident
            && is_predefined_escaper(&esc) {
                let last_args = p.cmds[pipeline_len - 1].args.len();
                if p.cmds.len() == 1 && last_args > 1 {
                    let pos0 = p.cmds[pipeline_len - 1].args[0].pos();
                    p.cmds[pipeline_len - 1].args[0] = Node::Identifier { pos: pos0, ident: "_eval_args_".to_string() };
                    let cmd = new_ident_cmd(&esc, p.pos);
                    append_cmd(&mut p.cmds, cmd);
                    pipeline_len += 1;
                }
                let mut dup = false;
                for x in s.iter_mut() {
                    if normalize_esc_fn(&esc) == normalize_esc_fn(x) {
                        *x = esc.clone();
                        dup = true;
                    }
                }
                if dup {
                    pipeline_len -= 1;
                }
            }
    }
    let mut new_cmds: Vec<CommandNode> = p.cmds[..pipeline_len.min(p.cmds.len())].to_vec();
    let mut inserted: HashSet<String> = HashSet::new();
    for cmd in &new_cmds {
        if let Some(id) = first_ident(cmd) {
            inserted.insert(normalize_esc_fn(id).to_string());
        }
    }
    for name in &s {
        if !inserted.contains(normalize_esc_fn(name)) {
            append_cmd(&mut new_cmds, new_ident_cmd(name, p.pos));
        }
    }
    p.cmds = new_cmds;
}

/// Go `nudge`.
fn nudge(mut c: Context) -> Context {
    match c.state {
        State::Tag => c.state = State::AttrName,
        State::BeforeValue => {
            c.state = attr_start_state(c.attr);
            c.delim = Delim::SpaceOrTagEnd;
            c.attr = Attr::None;
        }
        State::AfterName => {
            c.state = State::AttrName;
            c.attr = Attr::None;
        }
        _ => {}
    }
    c
}

/// Go `join`.
fn join(a: Context, b: Context, loc: &Option<String>, node_name: &str) -> Context {
    if a.state == State::Error {
        return a;
    }
    if b.state == State::Error {
        return b;
    }
    if a.state == State::Dead {
        return b;
    }
    if b.state == State::Dead {
        return a;
    }
    if a.eq(&b) {
        return a;
    }
    let mut c = a.clone();
    c.url_part = b.url_part;
    if c.eq(&b) {
        c.url_part = UrlPart::Unknown;
        return c;
    }
    let mut c = a.clone();
    c.js_ctx = b.js_ctx;
    if c.eq(&b) {
        c.js_ctx = JsCtx::Unknown;
        return c;
    }
    let (c2, d) = (nudge(a.clone()), nudge(b.clone()));
    if !(c2.eq(&a) && d.eq(&b)) {
        let e = join(c2, d, loc, node_name);
        if e.state != State::Error {
            return e;
        }
    }
    Context::error(errorf(
        ErrorCode::BranchEnd,
        loc.clone(),
        0,
        format!("{{{{{}}}}} branches end in different contexts: {}, {}", node_name, a, b),
    ))
}

/// Replaces the error of `c` by a copy with a new line and a prefixed description.
fn amend_error(mut c: Context, line: usize, prefix: &str) -> Context {
    if let Some(e) = &c.err {
        let mut e2 = (**e).clone();
        e2.line = line;
        e2.description = format!("{}{}", prefix, e2.description);
        c.err = Some(Arc::new(e2));
    }
    c
}

fn join_range(mut c0: Context, rc: &Arc<Mutex<RangeContext>>) -> Context {
    let (breaks, continues) = {
        let g = lock(rc);
        (g.breaks.clone(), g.continues.clone())
    };
    for c in breaks {
        let n = c.n.clone().unwrap_or(CtxNode { loc: String::new(), line: 0 });
        c0 = join(c0, c, &Some(n.loc.clone()), "range");
        if c0.state == State::Error {
            return amend_error(c0, n.line, "at range loop break: ");
        }
    }
    for c in continues {
        let n = c.n.clone().unwrap_or(CtxNode { loc: String::new(), line: 0 });
        c0 = join(c0, c, &Some(n.loc.clone()), "range");
        if c0.state == State::Error {
            return amend_error(c0, n.line, "at range loop continue: ");
        }
    }
    c0
}

fn delim_ends(d: Delim) -> &'static [u8] {
    match d {
        Delim::DoubleQuote => b"\"",
        Delim::SingleQuote => b"'",
        Delim::SpaceOrTagEnd => b" \t\n\x0c\r>",
        Delim::None => b"",
    }
}

/// Go `contextAfterText`.
fn context_after_text(c: Context, s: &[u8]) -> (Context, usize) {
    if c.delim == Delim::None {
        let (c1, i) = t_special_tag_end(c.clone(), s);
        if i == 0 {
            return (c1, 0);
        }
        return transition(c, &s[..i]);
    }
    let ends = delim_ends(c.delim);
    let mut i = s.iter().position(|b| ends.contains(b)).unwrap_or(s.len());
    if c.delim == Delim::SpaceOrTagEnd
        && let Some(j) = s[..i].iter().position(|b| b"\"'<=`".contains(b)) {
            return (
                Context::error(errorf(
                    ErrorCode::BadHtml,
                    None,
                    0,
                    format!("{} in unquoted attr: {}", qb(&s[j..j + 1]), qb(&s[..i])),
                )),
                s.len(),
            );
        }
    if i == s.len() {
        let unescaped = html_unescape(&String::from_utf8_lossy(s));
        let mut u: &[u8] = unescaped.as_bytes();
        let mut c = c;
        while !u.is_empty() {
            let (c1, i1) = transition(c, u);
            c = c1;
            u = &u[i1..];
        }
        return (c, s.len());
    }
    let mut element = c.element;
    if c.state == State::Attr
        && c.element == Element::Script
        && c.attr == Attr::ScriptType
        && !is_js_type(&String::from_utf8_lossy(&s[..i]))
    {
        element = Element::None;
    }
    if c.delim != Delim::SpaceOrTagEnd {
        i += 1;
    }
    (Context { state: State::Tag, element, ..Default::default() }, i)
}

/// Go `html.UnescapeString` for the common entities (named entities of HTML 4 that matter in attributes,
/// and numeric character references).
fn html_unescape(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'&' {
            let ch = s[i..].chars().next().unwrap_or('\u{FFFD}');
            out.push(ch);
            i += ch.len_utf8();
            continue;
        }
        let rest = &s[i + 1..];
        if let Some(num) = rest.strip_prefix('#') {
            let (hex, digits) = match num.as_bytes().first() {
                Some(b'x') | Some(b'X') => (true, &num[1..]),
                _ => (false, num),
            };
            let n = digits
                .bytes()
                .take_while(|c| if hex { c.is_ascii_hexdigit() } else { c.is_ascii_digit() })
                .count();
            if n > 0 {
                let v = u32::from_str_radix(&digits[..n], if hex { 16 } else { 10 }).unwrap_or(0xFFFD);
                let v = match v {
                    0x80..=0x9F => replacement_table(v),
                    0 => 0xFFFD,
                    v if v > 0x10FFFF || (0xD800..0xE000).contains(&v) => 0xFFFD,
                    v => v,
                };
                out.push(char::from_u32(v).unwrap_or('\u{FFFD}'));
                let mut consumed = 1 + 1 + usize::from(hex) + n;
                if s.as_bytes().get(i + consumed) == Some(&b';') {
                    consumed += 1;
                }
                i += consumed;
                continue;
            }
        } else {
            let name_len = rest.bytes().take_while(|c| c.is_ascii_alphanumeric()).count();
            let name = &rest[..name_len];
            // Longest known entity prefix.
            let mut matched = None;
            for l in (1..=name_len).rev() {
                if let Some(v) = named_entity(&name[..l]) {
                    let has_semi = rest.as_bytes().get(l) == Some(&b';');
                    if has_semi || l == name_len {
                        matched = Some((l, v, has_semi));
                        break;
                    }
                }
            }
            if let Some((l, v, semi)) = matched {
                out.push_str(v);
                i += 1 + l + usize::from(semi);
                continue;
            }
        }
        out.push('&');
        i += 1;
    }
    out
}

fn replacement_table(v: u32) -> u32 {
    const T: [u32; 32] = [
        0x20AC, 0x81, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021, 0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0x8D,
        0x017D, 0x8F, 0x90, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014, 0x02DC, 0x2122, 0x0161, 0x203A,
        0x0153, 0x9D, 0x017E, 0x0178,
    ];
    T[(v - 0x80) as usize]
}

fn named_entity(name: &str) -> Option<&'static str> {
    Some(match name {
        "amp" | "AMP" => "&",
        "lt" | "LT" => "<",
        "gt" | "GT" => ">",
        "quot" | "QUOT" => "\"",
        "apos" => "'",
        "nbsp" => "\u{a0}",
        "colon" => ":",
        "sol" => "/",
        "lpar" => "(",
        "rpar" => ")",
        "Tab" => "\t",
        "NewLine" => "\n",
        "num" => "#",
        "quest" => "?",
        "equals" => "=",
        "period" => ".",
        "comma" => ",",
        "semi" => ";",
        "excl" => "!",
        "percnt" => "%",
        "plus" => "+",
        "bsol" => "\\",
        "grave" => "`",
        "lsqb" | "lbrack" => "[",
        "rsqb" | "rbrack" => "]",
        "lcub" | "lbrace" => "{",
        "rcub" | "rbrace" => "}",
        "ast" | "midast" => "*",
        "dollar" => "$",
        "commat" => "@",
        "lowbar" | "UnderBar" => "_",
        "hyphen" | "dash" => "\u{2010}",
        "copy" | "COPY" => "\u{a9}",
        "reg" | "REG" => "\u{ae}",
        _ => return None,
    })
}

impl Escaper {
    fn loc(tree: &Tree, pos: usize) -> Option<String> {
        Some(tree.error_location(pos))
    }

    fn escape(&mut self, env: &Env<'_>, tree: &Tree, c: Context, n: &Node) -> Context {
        match n {
            Node::Action(a) => self.escape_action(tree, c, a),
            Node::Break { pos, line } => {
                let mut c = c;
                c.n = Some(CtxNode { loc: tree.error_location(*pos), line: *line });
                if let Some(rc) = &self.range_context {
                    lock(rc).breaks.push(c);
                }
                Context::with_state(State::Dead)
            }
            Node::Comment { .. } => c,
            Node::Continue { pos, line } => {
                let mut c = c;
                c.n = Some(CtxNode { loc: tree.error_location(*pos), line: *line });
                if let Some(rc) = &self.range_context {
                    lock(rc).continues.push(c);
                }
                Context::with_state(State::Dead)
            }
            Node::If(b) => self.escape_branch(env, tree, c, b, "if"),
            Node::List(l) => self.escape_list(env, tree, c, Some(l)),
            Node::Range(b) => self.escape_branch(env, tree, c, b, "range"),
            Node::Template(t) => self.escape_template(env, tree, c, t),
            Node::Text(t) => self.escape_text(c, t),
            Node::With(b) => self.escape_branch(env, tree, c, b, "with"),
            _ => c,
        }
    }

    fn escape_action(&mut self, tree: &Tree, c: Context, n: &ActionNode) -> Context {
        if !n.pipe.decl.is_empty() {
            return c;
        }
        let mut c = nudge(c);
        let ncmds = n.pipe.cmds.len();
        for (pos, cmd) in n.pipe.cmds.iter().enumerate() {
            let Some(ident) = first_ident(cmd) else { continue };
            if is_predefined_escaper(ident)
                && (pos < ncmds - 1 || (c.state == State::Attr && c.delim == Delim::SpaceOrTagEnd && ident == "html"))
            {
                return Context::error(errorf(
                    ErrorCode::PredefinedEscaper,
                    Self::loc(tree, n.pos),
                    n.line,
                    format!("predefined escaper {} disallowed in template", quote(ident)),
                ));
            }
        }
        let mut s: Vec<String> = Vec::with_capacity(3);
        match c.state {
            State::Error => return c,
            State::Url | State::CssDqStr | State::CssSqStr | State::CssDqUrl | State::CssSqUrl | State::CssUrl => {
                match c.url_part {
                    UrlPart::None | UrlPart::PreQuery => {
                        if c.url_part == UrlPart::None {
                            s.push("_html_template_urlfilter".into());
                        }
                        match c.state {
                            State::CssDqStr | State::CssSqStr => s.push("_html_template_cssescaper".into()),
                            _ => s.push("_html_template_urlnormalizer".into()),
                        }
                    }
                    UrlPart::QueryOrFrag => s.push("_html_template_urlescaper".into()),
                    UrlPart::Unknown => {
                        let mut ns = String::new();
                        Node::Action(n.clone()).write_to(&mut ns);
                        return Context::error(errorf(
                            ErrorCode::AmbigContext,
                            Self::loc(tree, n.pos),
                            n.line,
                            format!("{} appears in an ambiguous context within a URL", ns),
                        ));
                    }
                }
            }
            State::MetaContent => {}
            State::MetaContentUrl => s.push("_html_template_urlfilter".into()),
            State::Js => {
                s.push("_html_template_jsvalescaper".into());
                c.js_ctx = JsCtx::DivOp;
            }
            State::JsDqStr | State::JsSqStr => s.push("_html_template_jsstrescaper".into()),
            State::JsTmplLit => s.push("_html_template_jstmpllitescaper".into()),
            State::JsRegexp => s.push("_html_template_jsregexpescaper".into()),
            State::Css => s.push("_html_template_cssvaluefilter".into()),
            State::Text => s.push("_html_template_htmlescaper".into()),
            State::Rcdata => s.push("_html_template_rcdataescaper".into()),
            State::Attr => {}
            State::AttrName | State::Tag => {
                c.state = State::AttrName;
                s.push("_html_template_htmlnamefilter".into());
            }
            State::Srcset => s.push("_html_template_srcsetescaper".into()),
            st => {
                if is_comment(st) {
                    s.push("_html_template_commentescaper".into());
                } else {
                    // Go panics: "unexpected state".
                    return Context::error(errorf(
                        ErrorCode::OutputContext,
                        Self::loc(tree, n.pos),
                        n.line,
                        format!("unexpected state {}", st),
                    ));
                }
            }
        }
        match c.delim {
            Delim::None => {}
            Delim::SpaceOrTagEnd => s.push("_html_template_nospaceescaper".into()),
            _ => s.push("_html_template_attrescaper".into()),
        }
        self.action_node_edits.insert(n.id, s);
        c
    }

    fn escape_branch(&mut self, env: &Env<'_>, tree: &Tree, c: Context, n: &BranchNode, node_name: &str) -> Context {
        let loc = Self::loc(tree, n.pos);
        if node_name == "range" {
            self.range_context = Some(Arc::new(Mutex::new(RangeContext { outer: self.range_context.take(), ..Default::default() })));
        }
        let mut c0 = self.escape_list(env, tree, c.clone(), Some(&n.list));
        if node_name == "range" {
            let rc = self.range_context.clone().expect("set above");
            if c0.state != State::Error {
                c0 = join_range(c0, &rc);
            }
            self.range_context = lock(&rc).outer.clone();
            if c0.state == State::Error {
                return c0;
            }
            self.range_context = Some(Arc::new(Mutex::new(RangeContext { outer: self.range_context.take(), ..Default::default() })));
            let (c1, _) = self.escape_list_conditionally(env, tree, c0.clone(), Some(&n.list), None);
            c0 = join(c0, c1, &loc, node_name);
            let rc = self.range_context.clone().expect("set above");
            if c0.state == State::Error {
                self.range_context = lock(&rc).outer.clone();
                return amend_error(c0, n.line, "on range loop re-entry: ");
            }
            c0 = join_range(c0, &rc);
            self.range_context = lock(&rc).outer.clone();
            if c0.state == State::Error {
                return c0;
            }
        }
        let c1 = self.escape_list(env, tree, c, n.else_list.as_ref());
        join(c0, c1, &loc, node_name)
    }

    fn escape_list(&mut self, env: &Env<'_>, tree: &Tree, mut c: Context, n: Option<&ListNode>) -> Context {
        let Some(n) = n else { return c };
        for m in &n.nodes {
            c = self.escape(env, tree, c, m);
            if c.state == State::Dead {
                break;
            }
        }
        c
    }

    fn escape_list_conditionally(
        &mut self,
        env: &Env<'_>,
        tree: &Tree,
        c: Context,
        n: Option<&ListNode>,
        filter: Option<&dyn Fn(&Escaper, &Context) -> bool>,
    ) -> (Context, bool) {
        let mut e1 = Escaper { range_context: self.range_context.clone(), output: self.output.clone(), ..Default::default() };
        let c = e1.escape_list(env, tree, c, n);
        let ok = filter.is_some_and(|f| f(&e1, &c));
        if ok {
            self.output.extend(e1.output);
            self.derived.extend(e1.derived);
            self.called.extend(e1.called);
            self.action_node_edits.extend(e1.action_node_edits);
            self.template_node_edits.extend(e1.template_node_edits);
            self.text_node_edits.extend(e1.text_node_edits);
        }
        (c, ok)
    }

    fn escape_template(&mut self, env: &Env<'_>, tree: &Tree, c: Context, n: &TemplateNode) -> Context {
        let (c, name) = self.escape_tree(env, c, Self::loc(tree, n.pos), &n.name, n.line);
        if name != n.name {
            self.template_node_edits.insert(n.id, name);
        }
        c
    }

    /// Go `escapeTree`. `loc` is the location of the node for errors.
    pub(crate) fn escape_tree(&mut self, env: &Env<'_>, c: Context, loc: Option<String>, name: &str, line: usize) -> (Context, String) {
        let dname = c.mangle(name);
        self.called.insert(dname.clone());
        if let Some(out) = self.output.get(&dname) {
            return (out.clone(), dname);
        }
        let t = match env.template(self, name) {
            Some(Some(t)) if t.root.is_some() => t,
            Some(_) => {
                return (
                    Context::error(errorf(
                        ErrorCode::NoSuchTemplate,
                        loc,
                        line,
                        format!("{} is an incomplete or empty template", quote(name)),
                    )),
                    dname,
                );
            }
            None => {
                let msg = if env.html_names.contains(name) {
                    format!("{} is an incomplete or empty template", quote(name))
                } else {
                    format!("no such template {}", quote(name))
                };
                return (Context::error(errorf(ErrorCode::NoSuchTemplate, loc, line, msg)), dname);
            }
        };
        let t = if dname != name {
            match env.template(self, &dname) {
                Some(Some(dt)) => dt,
                _ => {
                    let dt = Arc::new(Tree {
                        name: dname.clone(),
                        parse_name: t.parse_name.clone(),
                        root: t.root.as_ref().map(ListNode::copy),
                        text: t.text.clone(),
                        mode: t.mode,
                    });
                    self.derived.insert(dname.clone(), dt.clone());
                    dt
                }
            }
        } else {
            t
        };
        (self.compute_out_ctx(env, c, &dname, &t), dname)
    }

    fn compute_out_ctx(&mut self, env: &Env<'_>, c: Context, name: &str, t: &Arc<Tree>) -> Context {
        let (mut c1, mut ok) = self.escape_template_body(env, c, name, t);
        if !ok {
            let (c2, ok2) = self.escape_template_body(env, c1.clone(), name, t);
            if ok2 {
                c1 = c2;
                ok = true;
            }
        }
        if !ok && c1.state != State::Error {
            let pos = t.root.as_ref().map_or(0, |r| r.pos);
            return Context::error(errorf(
                ErrorCode::OutputContext,
                Self::loc(t, pos),
                0,
                format!("cannot compute output context for template {}", name),
            ));
        }
        c1
    }

    fn escape_template_body(&mut self, env: &Env<'_>, c: Context, name: &str, t: &Arc<Tree>) -> (Context, bool) {
        let c_in = c.clone();
        let tname = name.to_string();
        let filter = move |e1: &Escaper, c1: &Context| -> bool {
            if c1.state == State::Error {
                return false;
            }
            if !e1.called.contains(&tname) {
                return true;
            }
            c_in.eq(c1)
        };
        self.output.insert(name.to_string(), c.clone());
        self.escape_list_conditionally(env, t, c, t.root.as_ref(), Some(&filter))
    }

    fn escape_text(&mut self, c: Context, n: &TextNode) -> Context {
        let s = &n.text;
        let mut c = c;
        let mut written = 0;
        let mut i = 0;
        let mut b: Vec<u8> = Vec::new();
        while i != s.len() {
            let (c1, nread) = context_after_text(c.clone(), &s[i..]);
            let i1 = i + nread;
            if c.state == State::Text || c.state == State::Rcdata {
                let mut end = i1;
                if c1.state != c.state {
                    for j in (i..end).rev() {
                        if s[j] == b'<' {
                            end = j;
                            break;
                        }
                    }
                }
                for j in i..end {
                    if s[j] == b'<' && !s[j..].to_ascii_uppercase().starts_with(b"<!DOCTYPE") {
                        b.extend_from_slice(&s[written..j]);
                        b.extend_from_slice(b"&lt;");
                        written = j + 1;
                    }
                }
            } else if is_comment(c.state) && c.delim == Delim::None {
                match c.state {
                    State::JsBlockCmt => {
                        let seg = &s[written..i1];
                        let has_nl = seg.iter().any(|&x| x == b'\n' || x == b'\r')
                            || seg.windows(3).any(|w| w == [0xE2, 0x80, 0xA8] || w == [0xE2, 0x80, 0xA9]);
                        b.push(if has_nl { b'\n' } else { b' ' });
                    }
                    State::CssBlockCmt => b.push(b' '),
                    _ => {}
                }
                written = i1;
            }
            if c.state != c1.state && is_comment(c1.state) && c1.delim == Delim::None {
                let mut cs = i1 as isize - 2;
                if c1.state == State::HtmlCmt || c1.state == State::JsHtmlOpenCmt {
                    cs -= 2;
                } else if c1.state == State::JsHtmlCloseCmt {
                    cs -= 1;
                }
                let cs = cs.max(written as isize) as usize;
                b.extend_from_slice(&s[written..cs]);
                written = i1;
            }
            if is_in_script_literal(c.state) && contains_special_script_tag(&s[i..i1]) {
                b.extend_from_slice(&s[written..i]);
                b.extend_from_slice(&escape_special_script_tags(&s[i..i1]));
                written = i1;
            }
            if i == i1 && c.state == c1.state {
                // Go panics ("infinite loop"); stop instead.
                return Context::error(errorf(
                    ErrorCode::OutputContext,
                    None,
                    0,
                    format!("infinite loop from {} to {} on {}..{}", c, c1, qb(&s[..i]), qb(&s[i..])),
                ));
            }
            c = c1;
            i = i1;
        }
        if written != 0 && c.state != State::Error {
            if !is_comment(c.state) || c.delim != Delim::None {
                b.extend_from_slice(&n.text[written..]);
            }
            self.text_node_edits.insert(n.id, b);
        }
        c
    }

    /// Go `escaper.commit`: applies the recorded edits to the trees of the set.
    pub(crate) fn commit(&mut self, env: &Env<'_>) {
        if !self.output.is_empty() {
            env.text.funcs(&escaper_funcs());
        }
        for (name, t) in &self.derived {
            env.text.add_parse_tree(name, (**t).clone());
        }
        let edits = Edits {
            actions: std::mem::take(&mut self.action_node_edits),
            templates: std::mem::take(&mut self.template_node_edits),
            texts: std::mem::take(&mut self.text_node_edits),
        };
        let mut c = env.text.common.lock();
        for slot in c.tmpl.values_mut() {
            let Some(tree) = slot else { continue };
            let Some(root) = &tree.root else { continue };
            if !edits.touches_list(root) {
                continue;
            }
            let mut t2 = (**tree).clone();
            if let Some(r) = t2.root.as_mut() {
                edits.apply_list(r);
            }
            *slot = Some(Arc::new(t2));
        }
        drop(c);
        // Derived trees not yet in the set keep their recorded copies; Go edits them in place as well.
        for t in self.derived.values_mut() {
            if t.root.as_ref().is_some_and(|r| edits.touches_list(r)) {
                let mut t2 = (**t).clone();
                if let Some(r) = t2.root.as_mut() {
                    edits.apply_list(r);
                }
                *t = Arc::new(t2);
            }
        }
        self.called.clear();
    }
}

fn contains_special_script_tag(s: &[u8]) -> bool {
    let l = s.to_ascii_lowercase();
    l.windows(7).any(|w| w == b"<script")
        || l.windows(8).any(|w| w == b"</script")
        || s.windows(4).any(|w| w == b"<!--")
}

/// `(?i)<(script|/script|!--)` replaced by `\x3C$1`.
fn escape_special_script_tags(s: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(s.len() + 8);
    let mut i = 0;
    'outer: while i < s.len() {
        if s[i] == b'<' {
            let rest = &s[i + 1..];
            for m in [&b"script"[..], &b"/script"[..], &b"!--"[..]] {
                if rest.len() >= m.len() && rest[..m.len()].eq_ignore_ascii_case(m) {
                    out.extend_from_slice(b"\\x3C");
                    out.extend_from_slice(&rest[..m.len()]);
                    i += 1 + m.len();
                    continue 'outer;
                }
            }
        }
        out.push(s[i]);
        i += 1;
    }
    out
}

struct Edits {
    actions: HashMap<u64, Vec<String>>,
    templates: HashMap<u64, String>,
    texts: HashMap<u64, Vec<u8>>,
}

impl Edits {
    fn touches_list(&self, l: &ListNode) -> bool {
        l.nodes.iter().any(|n| self.touches(n))
    }

    fn touches(&self, n: &Node) -> bool {
        match n {
            Node::Action(a) => self.actions.contains_key(&a.id),
            Node::Template(t) => self.templates.contains_key(&t.id),
            Node::Text(t) => self.texts.contains_key(&t.id),
            Node::List(l) => self.touches_list(l),
            Node::If(b) | Node::Range(b) | Node::With(b) => {
                self.touches_list(&b.list) || b.else_list.as_ref().is_some_and(|e| self.touches_list(e))
            }
            _ => false,
        }
    }

    fn apply_list(&self, l: &mut ListNode) {
        for n in l.nodes.iter_mut() {
            self.apply(n);
        }
    }

    fn apply(&self, n: &mut Node) {
        match n {
            Node::Action(a) => {
                if let Some(s) = self.actions.get(&a.id) {
                    ensure_pipeline_contains(&mut a.pipe, s);
                }
            }
            Node::Template(t) => {
                if let Some(name) = self.templates.get(&t.id) {
                    t.name = name.clone();
                }
            }
            Node::Text(t) => {
                if let Some(text) = self.texts.get(&t.id) {
                    t.text = text.clone();
                }
            }
            Node::List(l) => self.apply_list(l),
            Node::If(b) | Node::Range(b) | Node::With(b) => {
                self.apply_list(&mut b.list);
                if let Some(e) = b.else_list.as_mut() {
                    self.apply_list(e);
                }
            }
            _ => {}
        }
    }
}

/// Go `escapeTemplate`: escapes the template `name` of the set and commits the edits. On failure the
/// template's tree is removed and the error returned.
pub(crate) fn escape_template(esc: &mut Escaper, env: &Env<'_>, root_loc: Option<String>, name: &str) -> Result<(), Arc<Error>> {
    let (c, _) = esc.escape_tree(env, Context::default(), root_loc, name, 0);
    let err = if let Some(e) = &c.err {
        let mut e2 = (**e).clone();
        e2.name = name.to_string();
        Some(Arc::new(e2))
    } else if c.state != State::Text {
        Some(Arc::new(Error {
            code: ErrorCode::EndContext,
            loc: None,
            name: name.to_string(),
            line: 0,
            description: format!("ends in a non-text context: {}", c),
        }))
    } else {
        None
    };
    if let Some(err) = err {
        env.text.set_tree(name, None);
        return Err(err);
    }
    esc.commit(env);
    Ok(())
}
