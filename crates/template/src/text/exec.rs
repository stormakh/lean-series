//! Port of Go `text/template/exec.go`: the executor.

use std::sync::Arc;

use super::funcs::{builtins, is_true, prepare_arg, printable_value, truth};
use super::{MissingKey, Template};
use crate::gofmt::{quote, sprint_one};
use crate::methods::{histogram_field, method};
use crate::parse::{BranchNode, ChainNode, CommandNode, ListNode, Node, NodeType, NumberNode, PipeNode, Tree};
use crate::stack::maybe_grow;
use crate::value::{Func, Kind, Type, Value, from_slot, indirect, indirect_interface};

/// Go `maxExecDepth`.
const MAX_EXEC_DEPTH: usize = 100000;

/// A reference to the node being executed, for error messages.
#[derive(Clone, Copy)]
enum NodeRef<'t> {
    Node(&'t Node),
    Pipe(&'t PipeNode),
    Cmd(&'t CommandNode),
    List(&'t ListNode),
    Chain(&'t ChainNode),
}

impl NodeRef<'_> {
    fn pos(&self) -> usize {
        match self {
            NodeRef::Node(n) => n.pos(),
            NodeRef::Pipe(p) => p.pos,
            NodeRef::Cmd(c) => c.pos,
            NodeRef::List(l) => l.pos,
            NodeRef::Chain(c) => c.pos,
        }
    }

    fn string(&self) -> String {
        match self {
            NodeRef::Node(n) => n.to_string(),
            NodeRef::Pipe(p) => p.to_string(),
            NodeRef::Cmd(c) => c.to_string(),
            NodeRef::List(l) => l.to_string(),
            NodeRef::Chain(c) => c.to_string(),
        }
    }
}

/// Control flow out of a walk: an error, or `{{break}}` / `{{continue}}`.
pub(crate) enum Flow {
    Err(String),
    Break,
    Continue,
}

type EResult<T> = Result<T, Flow>;

struct State<'t, 'o> {
    tmpl: Template,
    tree: &'t Tree,
    out: &'o mut Vec<u8>,
    node: Option<NodeRef<'t>>,
    vars: Vec<(String, Value)>,
    depth: usize,
    /// Nesting of pipelines being evaluated (parenthesized pipelines recurse).
    pipe_depth: usize,
    missing_key: MissingKey,
}

/// Executes `t` with `data`, writing to `out`.
pub(crate) fn execute(t: &Template, out: &mut Vec<u8>, data: &Value) -> Result<(), String> {
    let missing_key = t.missing_key();
    let tree = t.tree();
    let Some(tree) = tree.filter(|tr| tr.root.is_some()) else {
        return Err(format!("template: {}: {} is an incomplete or empty template", t.name(), quote(t.name())));
    };
    let mut s = State {
        tmpl: t.clone(),
        tree: &tree,
        out,
        node: None,
        vars: vec![("$".to_string(), data.clone())],
        depth: 0,
        pipe_depth: 0,
        missing_key,
    };
    let root = tree.root.as_ref().expect("checked above");
    match s.walk_list(data, root) {
        Ok(()) => Ok(()),
        Err(Flow::Err(e)) => Err(e),
        // The parser rejects break and continue outside range.
        Err(Flow::Break) | Err(Flow::Continue) => Ok(()),
    }
}

fn is_hex_int(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() > 2 && b[0] == b'0' && (b[1] == b'x' || b[1] == b'X') && !s.contains(['p', 'P'])
}

fn is_rune_int(s: &str) -> bool {
    s.starts_with('\'')
}

/// Go `reflect.Value.String` of a non-string value: `<T Value>`.
fn reflect_string(v: &Value) -> String {
    match v {
        Value::Invalid => "<invalid Value>".to_string(),
        Value::String(s, _) => s.to_string(),
        _ => format!("<{} Value>", v.type_name()),
    }
}

/// Go `call`: calls a function value with prepared arguments.
fn call_func(name: &str, fnv: &Value, args: &[Value]) -> Result<Value, String> {
    let fnv = indirect_interface(fnv);
    if !fnv.is_valid() {
        return Err("call of nil".to_string());
    }
    let Value::Func(f) = &fnv else {
        return Err(format!("non-function {} of type {}", name, fnv.type_name()));
    };
    let num_in = f.sig.args.len();
    if f.sig.variadic {
        if args.len() < num_in - 1 {
            return Err(format!("wrong number of args for {}: got {} want at least {}", name, args.len(), num_in - 1));
        }
    } else if args.len() != num_in {
        return Err(format!("wrong number of args for {}: got {} want {}", name, args.len(), num_in));
    }
    let mut argv = Vec::with_capacity(args.len());
    for (i, arg) in args.iter().enumerate() {
        let arg = indirect_interface(arg);
        let arg_type = if !f.sig.variadic || i < num_in - 1 { &f.sig.args[i] } else { &f.sig.args[num_in - 1] };
        match prepare_arg(&arg, arg_type) {
            Ok(v) => argv.push(indirect_interface(&v)),
            Err(e) => return Err(format!("arg {}: {}", i, e)),
        }
    }
    f.call(&argv)
}

impl<'t, 'o> State<'t, 'o> {
    fn at(&mut self, n: NodeRef<'t>) {
        self.node = Some(n);
    }

    /// Go `state.errorf`.
    fn error(&self, msg: String) -> Flow {
        let name = self.tmpl.name();
        match &self.node {
            None => Flow::Err(format!("template: {}: {}", name, msg)),
            Some(n) => {
                let location = self.tree.error_location(n.pos());
                Flow::Err(format!("template: {}: executing {} at <{}>: {}", location, quote(name), n.string(), msg))
            }
        }
    }

    fn errorf<T>(&self, msg: String) -> EResult<T> {
        Err(self.error(msg))
    }

    fn mark(&self) -> usize {
        self.vars.len()
    }

    fn pop(&mut self, mark: usize) {
        self.vars.truncate(mark);
    }

    fn set_var(&mut self, name: &str, value: Value) -> EResult<()> {
        for v in self.vars.iter_mut().rev() {
            if v.0 == name {
                v.1 = value;
                return Ok(());
            }
        }
        self.errorf(format!("undefined variable: {}", name))
    }

    fn set_top_var(&mut self, n: usize, value: Value) {
        let l = self.vars.len();
        self.vars[l - n].1 = value;
    }

    fn var_value(&self, name: &str) -> EResult<Value> {
        for v in self.vars.iter().rev() {
            if v.0 == name {
                return Ok(v.1.clone());
            }
        }
        self.errorf(format!("undefined variable: {}", name))
    }

    fn walk_list(&mut self, dot: &Value, list: &'t ListNode) -> EResult<()> {
        self.at(NodeRef::List(list));
        for n in &list.nodes {
            self.walk(dot, n)?;
        }
        Ok(())
    }

    fn walk(&mut self, dot: &Value, node: &'t Node) -> EResult<()> {
        self.at(NodeRef::Node(node));
        match node {
            Node::Action(a) => {
                let val = self.eval_pipeline(dot, Some(&a.pipe))?;
                if a.pipe.decl.is_empty() {
                    self.print_value(node, &val)?;
                }
                Ok(())
            }
            Node::Break { .. } => Err(Flow::Break),
            Node::Comment { .. } => Ok(()),
            Node::Continue { .. } => Err(Flow::Continue),
            Node::If(b) => self.walk_if_or_with(false, dot, b),
            Node::List(l) => {
                for n in &l.nodes {
                    self.walk(dot, n)?;
                }
                Ok(())
            }
            Node::Range(r) => self.walk_range(dot, node, r),
            Node::Template(t) => self.walk_template(dot, node, &t.name, t.pipe.as_ref()),
            Node::Text(t) => {
                self.out.extend_from_slice(&t.text);
                Ok(())
            }
            Node::With(b) => self.walk_if_or_with(true, dot, b),
            _ => self.errorf(format!("unknown node: {}", node)),
        }
    }

    fn walk_if_or_with(&mut self, is_with: bool, dot: &Value, b: &'t BranchNode) -> EResult<()> {
        let mark = self.mark();
        let val = self.eval_pipeline(dot, Some(&b.pipe))?;
        let (truth, ok) = is_true(&indirect_interface(&val));
        if !ok {
            return self.errorf(format!("if/with can't use {}", sprint_one(&val)));
        }
        let r = if truth {
            if is_with { self.walk_list_inner(&val, &b.list) } else { self.walk_list_inner(dot, &b.list) }
        } else if let Some(e) = &b.else_list {
            self.walk_list_inner(dot, e)
        } else {
            Ok(())
        };
        self.pop(mark);
        r
    }

    /// Walks a list that is a child of a control node (Go `s.walk(dot, list)`).
    fn walk_list_inner(&mut self, dot: &Value, list: &'t ListNode) -> EResult<()> {
        self.walk_list(dot, list)
    }

    fn one_iteration(&mut self, r: &'t BranchNode, mark: usize, index: Value, elem: Value) -> EResult<()> {
        let decl = &r.pipe.decl;
        if !decl.is_empty() {
            if r.pipe.is_assign {
                if decl.len() > 1 {
                    self.set_var(&decl[0].ident[0], index.clone())?;
                } else {
                    self.set_var(&decl[0].ident[0], elem.clone())?;
                }
            } else {
                self.set_top_var(1, elem.clone());
            }
        }
        if decl.len() > 1 {
            if r.pipe.is_assign {
                self.set_var(&decl[1].ident[0], elem.clone())?;
            } else {
                self.set_top_var(2, index);
            }
        }
        let res = self.walk_list(&elem, &r.list);
        self.pop(mark);
        match res {
            Err(Flow::Continue) => Ok(()),
            other => other,
        }
    }

    fn walk_range(&mut self, dot: &Value, node: &'t Node, r: &'t BranchNode) -> EResult<()> {
        self.at(NodeRef::Node(node));
        let mark0 = self.mark();
        let res = self.walk_range_inner(dot, node, r);
        self.pop(mark0);
        match res {
            Err(Flow::Break) => Ok(()),
            other => other,
        }
    }

    fn walk_range_inner(&mut self, dot: &Value, node: &'t Node, r: &'t BranchNode) -> EResult<()> {
        let val = indirect(&self.eval_pipeline(dot, Some(&r.pipe))?).0;
        let mark = self.mark();
        let _ = node;
        match &val {
            Value::Int(n, k) => {
                if r.pipe.decl.len() > 1 {
                    return self.errorf(format!("can't use {} to iterate over more than one variable", sprint_one(&val)));
                }
                if *n > 0 {
                    for i in 0..*n {
                        self.one_iteration(r, mark, Value::Invalid, Value::Int(i, *k))?;
                    }
                    return Ok(());
                }
            }
            Value::Uint(n, k) => {
                if r.pipe.decl.len() > 1 {
                    return self.errorf(format!("can't use {} to iterate over more than one variable", sprint_one(&val)));
                }
                if *n > 0 {
                    for i in 0..*n {
                        self.one_iteration(r, mark, Value::Invalid, Value::Uint(i, *k))?;
                    }
                    return Ok(());
                }
            }
            Value::Slice(s) => {
                if !s.items.is_empty() {
                    let elem_t = s.ty.elem().cloned().unwrap_or(Type::Interface);
                    for (i, item) in s.items.iter().enumerate() {
                        self.one_iteration(r, mark, Value::int(i as i64), from_slot(item, &elem_t))?;
                    }
                    return Ok(());
                }
            }
            Value::Map(m) => {
                if !m.entries.is_empty() {
                    let elem_t = m.ty.elem().cloned().unwrap_or(Type::Interface);
                    for (k, v) in m.entries.iter() {
                        self.one_iteration(r, mark, Value::string(k.as_str()), from_slot(v, &elem_t))?;
                    }
                    return Ok(());
                }
            }
            Value::Invalid => {}
            Value::Nil(t) if matches!(t.kind(), Kind::Slice | Kind::Map) => {}
            _ => return self.errorf(format!("range can't iterate over {}", sprint_one(&val))),
        }
        if let Some(e) = &r.else_list {
            self.walk_list(dot, e)?;
        }
        Ok(())
    }

    fn walk_template(&mut self, dot: &Value, node: &'t Node, name: &str, pipe: Option<&'t PipeNode>) -> EResult<()> {
        self.at(NodeRef::Node(node));
        let Some(tmpl) = self.tmpl.lookup(name) else {
            return self.errorf(format!("template {} not defined", quote(name)));
        };
        if self.depth == MAX_EXEC_DEPTH {
            return self.errorf(format!("exceeded maximum template depth ({})", MAX_EXEC_DEPTH));
        }
        let dot = self.eval_pipeline(dot, pipe)?;
        let Some(tree) = tmpl.tree().filter(|t| t.root.is_some()) else {
            return self.errorf(format!("{} is an incomplete or empty template", quote(name)));
        };
        let depth = self.depth + 1;
        let missing_key = self.missing_key;
        let out: &mut Vec<u8> = self.out;
        maybe_grow(depth, move || {
            let mut ns = State {
                tmpl,
                tree: &tree,
                out,
                node: None,
                vars: vec![("$".to_string(), dot.clone())],
                depth,
                pipe_depth: 0,
                missing_key,
            };
            let root = tree.root.as_ref().expect("checked above");
            ns.walk_list(&dot, root)
        })
    }

    fn eval_pipeline(&mut self, dot: &Value, pipe: Option<&'t PipeNode>) -> EResult<Value> {
        let Some(pipe) = pipe else { return Ok(Value::Invalid) };
        self.pipe_depth += 1;
        let depth = self.pipe_depth;
        let r = maybe_grow(depth, || self.eval_pipeline_inner(dot, pipe));
        self.pipe_depth -= 1;
        r
    }

    fn eval_pipeline_inner(&mut self, dot: &Value, pipe: &'t PipeNode) -> EResult<Value> {
        self.at(NodeRef::Pipe(pipe));
        let mut value: Option<Value> = None;
        for cmd in &pipe.cmds {
            let v = self.eval_command(dot, cmd, value.take())?;
            value = Some(indirect_interface(&v));
        }
        let value = value.unwrap_or(Value::Invalid);
        for variable in &pipe.decl {
            if pipe.is_assign {
                self.set_var(&variable.ident[0], value.clone())?;
            } else {
                self.vars.push((variable.ident[0].clone(), value.clone()));
            }
        }
        Ok(value)
    }

    fn not_a_function(&self, args: &[Node], final_: &Option<Value>) -> EResult<()> {
        if args.len() > 1 || final_.is_some() {
            return self.errorf(format!("can't give argument to non-function {}", args[0]));
        }
        Ok(())
    }

    fn eval_command(&mut self, dot: &Value, cmd: &'t CommandNode, final_: Option<Value>) -> EResult<Value> {
        let first = &cmd.args[0];
        match first {
            Node::Field(_) => return self.eval_field_node(dot, first, &cmd.args, final_),
            Node::Chain(c) => return self.eval_chain_node(dot, c, &cmd.args, final_),
            Node::Identifier { .. } => return self.eval_function(dot, first, NodeRef::Cmd(cmd), &cmd.args, final_),
            Node::Pipe(p) => {
                self.not_a_function(&cmd.args, &final_)?;
                return self.eval_pipeline(dot, Some(p));
            }
            Node::Variable(_) => return self.eval_variable_node(dot, first, &cmd.args, final_),
            _ => {}
        }
        self.at(NodeRef::Node(first));
        self.not_a_function(&cmd.args, &final_)?;
        match first {
            Node::Bool { val, .. } => Ok(Value::Bool(*val)),
            Node::Dot(_) => Ok(dot.clone()),
            Node::Nil(_) => self.errorf("nil is not a command".to_string()),
            Node::Number(n) => self.ideal_constant(first, n),
            Node::String(s) => Ok(Value::string(s.text.as_str())),
            _ => self.errorf(format!("can't evaluate command {}", quote(&first.to_string()))),
        }
    }

    fn ideal_constant(&mut self, node: &'t Node, c: &NumberNode) -> EResult<Value> {
        self.at(NodeRef::Node(node));
        if c.is_complex {
            return Ok(Value::Complex(c.complex128.0, c.complex128.1));
        }
        if c.is_float && !is_hex_int(&c.text) && !is_rune_int(&c.text) && c.text.contains(['.', 'e', 'E', 'p', 'P']) {
            return Ok(Value::Float(c.float64));
        }
        if c.is_int {
            return Ok(Value::int(c.int64));
        }
        if c.is_uint {
            return self.errorf(format!("{} overflows int", c.text));
        }
        Ok(Value::Invalid)
    }

    fn eval_field_node(&mut self, dot: &Value, field: &'t Node, args: &'t [Node], final_: Option<Value>) -> EResult<Value> {
        self.at(NodeRef::Node(field));
        let Node::Field(f) = field else { return Ok(Value::Invalid) };
        self.eval_field_chain(dot, dot.clone(), NodeRef::Node(field), &f.ident, args, final_)
    }

    fn eval_chain_node(&mut self, dot: &Value, chain: &'t ChainNode, args: &'t [Node], final_: Option<Value>) -> EResult<Value> {
        self.at(NodeRef::Chain(chain));
        if chain.field.is_empty() {
            return self.errorf("internal error: no fields in evalChainNode".to_string());
        }
        if chain.node.node_type() == NodeType::Nil {
            return self.errorf(format!("indirection through explicit nil in {}", chain));
        }
        let pipe = self.eval_arg(dot, None, &chain.node)?;
        self.eval_field_chain(dot, pipe, NodeRef::Chain(chain), &chain.field, args, final_)
    }

    fn eval_variable_node(&mut self, dot: &Value, variable: &'t Node, args: &'t [Node], final_: Option<Value>) -> EResult<Value> {
        self.at(NodeRef::Node(variable));
        let Node::Variable(v) = variable else { return Ok(Value::Invalid) };
        let value = self.var_value(&v.ident[0])?;
        if v.ident.len() == 1 {
            self.not_a_function(args, &final_)?;
            return Ok(value);
        }
        self.eval_field_chain(dot, value, NodeRef::Node(variable), &v.ident[1..], args, final_)
    }

    fn eval_field_chain(
        &mut self,
        dot: &Value,
        mut receiver: Value,
        node: NodeRef<'t>,
        ident: &'t [String],
        args: &'t [Node],
        final_: Option<Value>,
    ) -> EResult<Value> {
        let n = ident.len();
        for id in &ident[..n - 1] {
            receiver = self.eval_field(dot, id, node, &[], None, receiver)?;
        }
        self.eval_field(dot, &ident[n - 1], node, args, final_, receiver)
    }

    fn find_function(&self, name: &str) -> Option<(Func, bool)> {
        if let Some(f) = self.tmpl.common.lock().funcs.get(name) {
            return Some((f.clone(), false));
        }
        builtins().get(name).map(|f| (f.clone(), true))
    }

    fn eval_function(&mut self, dot: &Value, node: &'t Node, cmd: NodeRef<'t>, args: &'t [Node], final_: Option<Value>) -> EResult<Value> {
        self.at(NodeRef::Node(node));
        let Node::Identifier { ident: name, .. } = node else { return Ok(Value::Invalid) };
        let Some((function, is_builtin)) = self.find_function(name) else {
            return self.errorf(format!("{} is not a defined function", quote(name)));
        };
        self.eval_call(dot, &function, is_builtin, cmd, name, args, final_)
    }

    fn eval_field(
        &mut self,
        dot: &Value,
        field_name: &str,
        node: NodeRef<'t>,
        args: &'t [Node],
        final_: Option<Value>,
        receiver: Value,
    ) -> EResult<Value> {
        if !receiver.is_valid() {
            if self.missing_key == MissingKey::Error {
                return self.errorf(format!("nil data; no entry for key {}", quote(field_name)));
            }
            return Ok(Value::Invalid);
        }
        let typ = receiver.type_of().unwrap_or(Type::Interface);
        let (receiver, is_nil) = indirect(&receiver);
        if receiver.kind() == Kind::Interface && is_nil {
            return self.errorf(format!("nil pointer evaluating {}.{}", typ, field_name));
        }
        if let Some(m) = method(&receiver, field_name) {
            return self.eval_call(dot, &m, false, node, field_name, args, final_);
        }
        let has_args = args.len() > 1 || final_.is_some();
        match &receiver {
            Value::Struct(s) => {
                if let Some(i) = s.ty.field_index(field_name) {
                    if !field_name.starts_with(|c: char| c.is_uppercase()) {
                        return self.errorf(format!("{} is an unexported field of struct type {}", field_name, typ));
                    }
                    if has_args {
                        return self.errorf(format!("{} has arguments but cannot be invoked as function", field_name));
                    }
                    return Ok(from_slot(&s.fields[i], &s.ty.fields[i].1));
                }
            }
            Value::Histogram(h) => {
                if let Some(v) = histogram_field(h, field_name) {
                    if has_args {
                        return self.errorf(format!("{} has arguments but cannot be invoked as function", field_name));
                    }
                    return Ok(v);
                }
            }
            Value::Map(_) | Value::Nil(Type::Map(..)) => {
                if has_args {
                    return self.errorf(format!("{} is not a method but has arguments", field_name));
                }
                let (found, elem) = match &receiver {
                    Value::Map(m) => {
                        let elem = m.ty.elem().cloned().unwrap_or(Type::Interface);
                        (m.entries.get(field_name).map(|v| from_slot(v, &elem)), elem)
                    }
                    Value::Nil(t) => (None, t.elem().cloned().unwrap_or(Type::Interface)),
                    _ => (None, Type::Interface),
                };
                return match found {
                    Some(v) => Ok(v),
                    None => match self.missing_key {
                        MissingKey::Invalid => Ok(Value::Invalid),
                        MissingKey::ZeroValue => Ok(elem.zero()),
                        MissingKey::Error => self.errorf(format!("map has no entry for key {}", quote(field_name))),
                    },
                };
            }
            Value::Nil(Type::Ptr(elem)) => {
                let has_field = match &**elem {
                    Type::Struct(st) => st.field_index(field_name).is_some(),
                    _ => true,
                };
                if has_field && is_nil {
                    return self.errorf(format!("nil pointer evaluating {}.{}", typ, field_name));
                }
            }
            _ => {}
        }
        self.errorf(format!("can't evaluate field {} in type {}", field_name, typ))
    }

    #[allow(clippy::too_many_arguments)]
    fn eval_call(
        &mut self,
        dot: &Value,
        fun: &Func,
        is_builtin: bool,
        node: NodeRef<'t>,
        name: &str,
        args: &'t [Node],
        final_: Option<Value>,
    ) -> EResult<Value> {
        let args: &'t [Node] = if args.is_empty() { args } else { &args[1..] };
        let sig = fun.sig.clone();
        let decl_in = sig.args.len();
        let num_in = args.len() + usize::from(final_.is_some());
        let num_fixed = if sig.variadic { decl_in - 1 } else { args.len() };
        if sig.variadic {
            if num_in < num_fixed {
                return self.errorf(format!(
                    "wrong number of args for {}: want at least {} got {}",
                    name,
                    decl_in - 1,
                    args.len()
                ));
            }
        } else if num_in != decl_in {
            return self.errorf(format!("wrong number of args for {}: want {} got {}", name, decl_in, num_in));
        }

        if is_builtin && (name == "and" || name == "or") {
            let arg_type = &sig.args[0];
            let mut v = Value::Invalid;
            for arg in args {
                v = self.eval_arg(dot, Some(arg_type), arg)?;
                if truth(&v) == (name == "or") {
                    return Ok(v);
                }
            }
            if let Some(f) = final_ {
                v = self.validate_type(f, Some(arg_type))?;
            }
            return Ok(v);
        }

        let mut argv: Vec<Value> = Vec::with_capacity(num_in);
        let mut i = 0;
        while i < num_fixed && i < args.len() {
            let v = self.eval_arg(dot, Some(&sig.args[i]), &args[i])?;
            argv.push(v);
            i += 1;
        }
        if sig.variadic {
            let arg_type = &sig.args[decl_in - 1];
            while i < args.len() {
                let v = self.eval_arg(dot, Some(arg_type), &args[i])?;
                argv.push(v);
                i += 1;
            }
        }
        let mut final_string = None;
        if let Some(f) = final_ {
            let t = if sig.variadic && num_in - 1 < num_fixed { &sig.args[num_in - 1] } else { &sig.args[decl_in - 1] };
            final_string = Some(reflect_string(&f));
            let v = self.validate_type(f, Some(t))?;
            argv.push(v);
        }
        let argv: Vec<Value> = argv.iter().map(indirect_interface).collect();

        let result = if is_builtin && name == "call" {
            let callee_name = if args.is_empty() { final_string.unwrap_or_default() } else { args[0].to_string() };
            call_func(&callee_name, &argv[0], &argv[1..])
        } else {
            fun.call(&argv)
        };
        match result {
            Ok(v) => Ok(v),
            Err(e) => {
                self.at(node);
                self.errorf(format!("error calling {}: {}", name, e))
            }
        }
    }

    /// Go `validateType`.
    fn validate_type(&mut self, value: Value, typ: Option<&Type>) -> EResult<Value> {
        if !value.is_valid() {
            return match typ {
                None => Ok(Value::Invalid),
                Some(t) if t.can_be_nil() => Ok(t.zero()),
                Some(t) => self.errorf(format!("invalid value; expected {}", t)),
            };
        }
        let Some(typ) = typ else { return Ok(value) };
        if *typ == Type::ReflectValue {
            return Ok(value);
        }
        let mut value = value;
        let mut vt = value.type_of().unwrap_or(Type::Interface);
        if !vt.assignable_to(typ) {
            if let Value::Iface(inner) = &value {
                let inner = (**inner).clone();
                vt = inner.type_of().unwrap_or(Type::Interface);
                if vt.assignable_to(typ) {
                    return Ok(inner);
                }
                value = inner;
            }
            if let Value::Ptr(inner) = &value
                && inner.type_of().unwrap_or(Type::Interface).assignable_to(typ) {
                    return Ok((**inner).clone());
                }
            if let Value::Nil(Type::Ptr(elem)) = &value
                && elem.assignable_to(typ) {
                    return self.errorf(format!("dereference of nil pointer of type {}", typ));
                }
            return self.errorf(format!("wrong type for value; expected {}; got {}", typ, vt));
        }
        Ok(value)
    }

    fn eval_arg(&mut self, dot: &Value, typ: Option<&Type>, n: &'t Node) -> EResult<Value> {
        self.at(NodeRef::Node(n));
        match n {
            Node::Dot(_) => return self.validate_type(dot.clone(), typ),
            Node::Nil(_) => {
                return match typ {
                    Some(t) if t.can_be_nil() => Ok(t.zero()),
                    Some(t) => self.errorf(format!("cannot assign nil to {}", t)),
                    None => self.errorf("cannot assign nil to <nil>".to_string()),
                };
            }
            Node::Field(_) => {
                let v = self.eval_field_node(dot, n, std::slice::from_ref(n), None)?;
                return self.validate_type(v, typ);
            }
            Node::Variable(_) => {
                let v = self.eval_variable_node(dot, n, &[], None)?;
                return self.validate_type(v, typ);
            }
            Node::Pipe(p) => {
                let v = self.eval_pipeline(dot, Some(p))?;
                return self.validate_type(v, typ);
            }
            Node::Identifier { .. } => {
                let v = self.eval_function(dot, n, NodeRef::Node(n), &[], None)?;
                return self.validate_type(v, typ);
            }
            Node::Chain(c) => {
                let v = self.eval_chain_node(dot, c, &[], None)?;
                return self.validate_type(v, typ);
            }
            _ => {}
        }
        let Some(typ) = typ else {
            return self.errorf(format!("can't handle {} for arg of type <nil>", n));
        };
        match typ {
            Type::Bool => {
                if let Node::Bool { val, .. } = n {
                    return Ok(Value::Bool(*val));
                }
                self.errorf(format!("expected bool; found {}", n))
            }
            Type::Complex128 => {
                if let Node::Number(num) = n
                    && num.is_complex {
                        return Ok(Value::Complex(num.complex128.0, num.complex128.1));
                    }
                self.errorf(format!("expected complex; found {}", n))
            }
            Type::Float64 => {
                if let Node::Number(num) = n
                    && num.is_float {
                        return Ok(Value::Float(num.float64));
                    }
                self.errorf(format!("expected float; found {}", n))
            }
            Type::Int(k) => {
                if let Node::Number(num) = n
                    && num.is_int {
                        return Ok(Value::Int(num.int64, *k));
                    }
                self.errorf(format!("expected integer; found {}", n))
            }
            Type::Uint(k) => {
                if let Node::Number(num) = n
                    && num.is_uint {
                        return Ok(Value::Uint(num.uint64, *k));
                    }
                self.errorf(format!("expected unsigned integer; found {}", n))
            }
            Type::String(k) => {
                if let Node::String(s) = n {
                    return Ok(Value::String(Arc::from(s.text.as_str()), *k));
                }
                self.errorf(format!("expected string; found {}", n))
            }
            Type::Interface | Type::ReflectValue => self.eval_empty_interface(dot, n),
            _ => self.errorf(format!("can't handle {} for arg of type {}", n, typ)),
        }
    }

    fn eval_empty_interface(&mut self, dot: &Value, n: &'t Node) -> EResult<Value> {
        self.at(NodeRef::Node(n));
        match n {
            Node::Bool { val, .. } => Ok(Value::Bool(*val)),
            Node::Dot(_) => Ok(dot.clone()),
            Node::Field(_) => self.eval_field_node(dot, n, &[], None),
            Node::Identifier { .. } => self.eval_function(dot, n, NodeRef::Node(n), &[], None),
            Node::Nil(_) => self.errorf("evalEmptyInterface: nil (can't happen)".to_string()),
            Node::Number(num) => self.ideal_constant(n, num),
            Node::String(s) => Ok(Value::string(s.text.as_str())),
            Node::Variable(_) => self.eval_variable_node(dot, n, &[], None),
            Node::Pipe(p) => self.eval_pipeline(dot, Some(p)),
            _ => self.errorf(format!("can't handle assignment of {} to empty interface argument", n)),
        }
    }

    fn print_value(&mut self, n: &'t Node, v: &Value) -> EResult<()> {
        self.at(NodeRef::Node(n));
        let Some(iface) = printable_value(v) else {
            return self.errorf(format!("can't print {} of type {}", n, v.type_name()));
        };
        let s = sprint_one(&iface);
        self.out.extend_from_slice(s.as_bytes());
        Ok(())
    }
}
