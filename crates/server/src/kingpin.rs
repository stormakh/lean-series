//! A port of the parts of `github.com/alecthomas/kingpin/v2` (v2.4.0) that promtool and prometheus use
//! (copied from the promtool crate; the server adds the `Bytes`, `Float`, `ModelDuration` and `Custom` kinds,
//! the record of user-set flags and the error stages of `Application.Parse`): the application
//! model, the tokenizer and parser, value validation with kingpin's error texts, the default usage template
//! and `util/documentcli` (`write-documentation`).
//!
//! Not ported: the hidden `--help-long`, `--help-man` and `--completion-*` flags (they print an error), envars,
//! and terminal width detection (kingpin uses `$COLUMNS`, then the terminal width, then 80; this port uses
//! `$COLUMNS`, then 80).

use std::collections::{BTreeMap, HashMap};
use std::io::Write;

use crate::doctext::to_text;

/// The value type of a flag or argument (the kingpin parser chosen with `.String()`, `.Bool()`, ...).
#[derive(Clone, Debug)]
pub enum Kind {
    Bool,
    String,
    Strings,
    StringMap,
    Duration,
    Int,
    Int64,
    Enum(Vec<&'static str>),
    ExistingFile,
    ExistingFiles,
    Url,
    /// `OpenFile(os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)`.
    OpenFile,
    /// `BytesVar` (`units.Base2Bytes`).
    Bytes,
    /// `FloatVar`.
    Float,
    /// `SetValue(&model.Duration)`.
    ModelDuration,
    /// A custom `kingpin.Value` (`SetValue`): validates a new value given the values set before; `true` when the
    /// value is cumulative (`IsCumulative`).
    Custom(Validator, bool),
}

impl PartialEq for Kind {
    /// Same kind (custom values compare by cumulativeness only: function pointers have no stable identity).
    fn eq(&self, o: &Kind) -> bool {
        match (self, o) {
            (Kind::Enum(a), Kind::Enum(b)) => a == b,
            (Kind::Custom(_, a), Kind::Custom(_, b)) => a == b,
            _ => std::mem::discriminant(self) == std::mem::discriminant(o),
        }
    }
}

/// The `Set` of a custom value: the values set before and the new one.
pub type Validator = fn(&[String], &str) -> Result<(), String>;

impl Kind {
    /// kingpin `repeatableFlag.IsCumulative()` / `remainderArg.IsCumulative()`.
    fn cumulative(&self) -> bool {
        matches!(self, Kind::Strings | Kind::StringMap | Kind::ExistingFiles | Kind::Custom(_, true))
    }
    /// documentcli checks for an accumulator (a struct with a `slice` field): `Strings()`, `ExistingFiles()`.
    fn is_accumulator(&self) -> bool {
        matches!(self, Kind::Strings | Kind::ExistingFiles)
    }
}

#[derive(Clone, Debug)]
pub struct Flag {
    pub name: &'static str,
    pub help: String,
    pub short: Option<char>,
    pub default: Vec<String>,
    pub placeholder: String,
    pub required: bool,
    pub hidden: bool,
    pub kind: Kind,
}

impl Flag {
    pub fn new(name: &'static str, help: impl Into<String>, kind: Kind) -> Flag {
        Flag {
            name,
            help: help.into(),
            short: None,
            default: Vec::new(),
            placeholder: String::new(),
            required: false,
            hidden: false,
            kind,
        }
    }
    pub fn default(mut self, v: &str) -> Flag {
        self.default = vec![v.to_string()];
        self
    }
    pub fn short(mut self, c: char) -> Flag {
        self.short = Some(c);
        self
    }
    pub fn required(mut self) -> Flag {
        self.required = true;
        self
    }
    pub fn hidden(mut self) -> Flag {
        self.hidden = true;
        self
    }
    pub fn placeholder(mut self, p: &str) -> Flag {
        self.placeholder = p.to_string();
        self
    }
    fn is_bool(&self) -> bool {
        self.kind == Kind::Bool
    }
    /// kingpin `FlagModel.FormatPlaceHolder`.
    fn format_placeholder(&self) -> String {
        if !self.placeholder.is_empty() {
            return self.placeholder.clone();
        }
        if !self.default.is_empty() {
            let ellipsis = if self.default.len() > 1 { "..." } else { "" };
            if self.kind == Kind::String {
                return format!("{}{}", promql::parser::quote::go_quote(&self.default[0]), ellipsis);
            }
            return format!("{}{}", self.default[0], ellipsis);
        }
        self.name.to_uppercase()
    }
}

#[derive(Clone, Debug)]
pub struct Arg {
    pub name: &'static str,
    pub help: String,
    pub default: Vec<String>,
    pub required: bool,
    pub kind: Kind,
}

impl Arg {
    pub fn new(name: &'static str, help: impl Into<String>, kind: Kind) -> Arg {
        Arg { name, help: help.into(), default: Vec::new(), required: false, kind }
    }
    pub fn default(mut self, v: &str) -> Arg {
        self.default = vec![v.to_string()];
        self
    }
    pub fn required(mut self) -> Arg {
        self.required = true;
        self
    }
    fn consumes_remainder(&self) -> bool {
        self.kind.cumulative()
    }
}

#[derive(Clone, Debug)]
pub struct Cmd {
    pub name: &'static str,
    pub help: String,
    pub hidden: bool,
    pub flags: Vec<Flag>,
    pub args: Vec<Arg>,
    pub cmds: Vec<Cmd>,
    /// Space separated names from the top (Go `FullCommand()`).
    pub full: String,
}

impl Cmd {
    pub fn new(name: &'static str, help: impl Into<String>) -> Cmd {
        Cmd { name, help: help.into(), hidden: false, flags: Vec::new(), args: Vec::new(), cmds: Vec::new(), full: String::new() }
    }
    pub fn hidden(mut self) -> Cmd {
        self.hidden = true;
        self
    }
    pub fn flag(mut self, f: Flag) -> Cmd {
        self.flags.push(f);
        self
    }
    pub fn arg(mut self, a: Arg) -> Cmd {
        self.args.push(a);
        self
    }
    pub fn cmd(mut self, c: Cmd) -> Cmd {
        self.cmds.push(c);
        self
    }
    fn set_full(&mut self, parent: &str) {
        self.full = if parent.is_empty() { self.name.to_string() } else { format!("{parent} {}", self.name) };
        let full = self.full.clone();
        for c in self.cmds.iter_mut() {
            c.set_full(&full);
        }
    }
}

fn flatten<'a>(cmds: &'a [Cmd], out: &mut Vec<&'a Cmd>) {
    for c in cmds {
        if c.cmds.is_empty() {
            out.push(c);
        }
        flatten(&c.cmds, out);
    }
}

/// kingpin `FlagGroupModel.FlagSummary`.
fn flag_summary(flags: &[Flag]) -> String {
    let ignore = ["help", "help-long", "help-man", "completion-bash", "completion-script-bash", "completion-script-zsh"];
    let mut out = Vec::new();
    let mut count = 0;
    for f in flags {
        if !ignore.contains(&f.name) {
            count += 1;
        }
        if f.required {
            if f.is_bool() {
                out.push(format!("--[no-]{}", f.name));
            } else {
                out.push(format!("--{}={}", f.name, f.format_placeholder()));
            }
        }
    }
    if count != out.len() {
        out.push("[<flags>]".to_string());
    }
    out.join(" ")
}

/// The application (Go `kingpin.Application`).
pub struct App {
    pub name: String,
    pub help: String,
    pub version: String,
    pub flags: Vec<Flag>,
    pub cmds: Vec<Cmd>,
}

/// The parsed values of the selected command: flag and argument values by name (after validation and
/// defaults), and the selected command (Go `FullCommand()`).
#[derive(Debug, Default)]
pub struct Parsed {
    pub command: String,
    /// The flags the user gave, in order, each once (what kingpin `PreAction`s see).
    pub user_flags: Vec<String>,
    values: HashMap<String, Vec<String>>,
    maps: HashMap<String, BTreeMap<String, String>>,
}

impl Parsed {
    fn get(&self, name: &str) -> Option<&Vec<String>> {
        self.values.get(name)
    }
    /// The last value of a flag or argument ("" when unset).
    pub fn str(&self, name: &str) -> String {
        self.get(name).and_then(|v| v.last().cloned()).unwrap_or_default()
    }
    pub fn is_set(&self, name: &str) -> bool {
        self.get(name).is_some_and(|v| !v.is_empty())
    }
    pub fn strings(&self, name: &str) -> Vec<String> {
        self.get(name).cloned().unwrap_or_default()
    }
    pub fn bool(&self, name: &str) -> bool {
        parse_bool(&self.str(name)).unwrap_or(false)
    }
    /// Nanoseconds (Go `time.Duration`).
    pub fn duration(&self, name: &str) -> i64 {
        let s = self.str(name);
        if s.is_empty() { 0 } else { parse_duration(&s).unwrap_or(0) }
    }
    pub fn int(&self, name: &str) -> i64 {
        let s = self.str(name);
        if s.is_empty() { 0 } else { parse_go_float(&s).map(|f| f as i64).unwrap_or(0) }
    }
    pub fn int64(&self, name: &str) -> i64 {
        let s = self.str(name);
        if s.is_empty() { 0 } else { parse_go_int(&s).unwrap_or(0) }
    }
    pub fn map(&self, name: &str) -> BTreeMap<String, String> {
        self.maps.get(name).cloned().unwrap_or_default()
    }
    /// Go `float64` flag value.
    pub fn float(&self, name: &str) -> f64 {
        parse_go_float(&self.str(name)).unwrap_or(0.0)
    }
    /// Bytes of a `BytesVar` flag (0 when unset).
    pub fn bytes(&self, name: &str) -> i64 {
        let s = self.str(name);
        if s.is_empty() { 0 } else { yaml::types::parse_base2_bytes(&s).map(|b| b.0).unwrap_or(0) }
    }
    /// Nanoseconds of a `model.Duration` flag (0 when unset).
    pub fn model_duration(&self, name: &str) -> i64 {
        let s = self.str(name);
        if s.is_empty() { 0 } else { yaml::types::parse_duration(&s).map(|d| d.0).unwrap_or(0) }
    }
}

/// What `App::try_parse` decided (Go `Application.Parse` and its error stages).
pub enum GoParse {
    Run(Parsed),
    /// Help or version was printed; exit with this code.
    Exit(i32),
    /// The command line does not parse (unknown flag, missing argument): Go's `parseContext` error.
    ContextErr(String),
    /// A value does not parse or a flag is repeated or missing: Go's `setValues`/`validateRequired` error.
    ValueErr(String),
}

/// What `App::parse` decided.
pub enum Outcome {
    /// Run this command.
    Run(Parsed),
    /// Help, version or a parse error was printed; exit with this code.
    Exit(i32),
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum TokType {
    Short,
    Long,
    Arg,
    Error,
    Eol,
}

#[derive(Clone, Debug)]
struct Token {
    typ: TokType,
    value: String,
}

impl Token {
    fn string(&self) -> String {
        match self.typ {
            TokType::Short => format!("-{}", self.value),
            TokType::Long => format!("--{}", self.value),
            TokType::Arg => self.value.clone(),
            TokType::Error => format!("error: {}", self.value),
            TokType::Eol => "<EOL>".to_string(),
        }
    }
}

enum Elem<'a> {
    Flag(&'a Flag, String),
    Arg(&'a Arg, String),
    Cmd(&'a Cmd),
}

struct Ctx<'a> {
    selected: Option<&'a Cmd>,
    args_only: bool,
    peek: Vec<Token>,
    argi: usize,
    args: Vec<String>,
    raw: Vec<String>,
    flags: Vec<&'a Flag>,
    arguments: Vec<&'a Arg>,
    argumenti: usize,
    elements: Vec<Elem<'a>>,
}

impl<'a> Ctx<'a> {
    fn new(args: &[String]) -> Ctx<'a> {
        Ctx {
            selected: None,
            args_only: false,
            peek: Vec::new(),
            argi: 0,
            args: args.to_vec(),
            raw: args.to_vec(),
            flags: Vec::new(),
            arguments: Vec::new(),
            argumenti: 0,
            elements: Vec::new(),
        }
    }
    fn long(&self, name: &str) -> Option<&'a Flag> {
        self.flags.iter().rev().find(|f| f.name == name).copied()
    }
    fn short(&self, c: char) -> Option<&'a Flag> {
        self.flags.iter().rev().find(|f| f.short == Some(c)).copied()
    }
    fn next_arg(&mut self) -> Option<&'a Arg> {
        if self.argumenti >= self.arguments.len() {
            return None;
        }
        let a = self.arguments[self.argumenti];
        if !a.consumes_remainder() {
            self.argumenti += 1;
        }
        Some(a)
    }
    fn advance(&mut self) {
        self.argi += 1;
        self.args.remove(0);
    }
    fn next(&mut self) -> Token {
        if let Some(t) = self.peek.pop() {
            return t;
        }
        if self.args.is_empty() {
            return Token { typ: TokType::Eol, value: String::new() };
        }
        if self.argi > 0 && self.argi <= self.raw.len() && self.raw[self.argi - 1] == "--" {
            self.args_only = true;
        }
        let arg = self.args[0].clone();
        self.advance();
        if self.args_only {
            return Token { typ: TokType::Arg, value: arg };
        }
        if arg == "--" {
            return self.next();
        }
        if let Some(rest) = arg.strip_prefix("--") {
            let (name, val) = match rest.find('=') {
                Some(i) => (&rest[..i], Some(&rest[i + 1..])),
                None => (rest, None),
            };
            let t = Token { typ: TokType::Long, value: name.to_string() };
            if let Some(v) = val {
                self.peek.push(Token { typ: TokType::Arg, value: v.to_string() });
            }
            return t;
        }
        if arg.starts_with('-') {
            if arg.len() == 1 {
                return Token { typ: TokType::Arg, value: String::new() };
            }
            let c = arg[1..].chars().next().unwrap();
            let size = c.len_utf8();
            let short = c.to_string();
            match self.short(c) {
                Some(f) if !f.is_bool() => {
                    let t = Token { typ: TokType::Short, value: short };
                    if arg.len() > size + 1 {
                        self.peek.push(Token { typ: TokType::Arg, value: arg[size + 1..].to_string() });
                    }
                    return t;
                }
                _ => {}
            }
            if arg.len() > size + 1 {
                self.args.insert(0, format!("-{}", &arg[size + 1..]));
            }
            return Token { typ: TokType::Short, value: short };
        }
        Token { typ: TokType::Arg, value: arg }
    }
    fn peek(&mut self) -> Token {
        if self.peek.is_empty() {
            let t = self.next();
            self.peek.push(t);
        }
        self.peek.last().unwrap().clone()
    }
    fn eol(&mut self) -> bool {
        self.peek().typ == TokType::Eol
    }
    fn error(&mut self) -> bool {
        self.peek().typ == TokType::Error
    }
    fn matched_cmd(&mut self, c: &'a Cmd) {
        self.elements.push(Elem::Cmd(c));
        for f in c.flags.iter() {
            self.flags.push(f);
        }
        for a in c.args.iter() {
            self.arguments.push(a);
        }
        self.selected = Some(c);
    }

    /// kingpin `flagGroup.parse`.
    fn parse_flag(&mut self) -> Result<Option<&'a Flag>, String> {
        let token = self.peek();
        match token.typ {
            TokType::Long | TokType::Short => {
                let flag_token = token.clone();
                let mut invert = false;
                let flag = if token.typ == TokType::Long {
                    let mut f = self.long(&token.value);
                    if f.is_none() {
                        if let Some(n) = token.value.strip_prefix("no-") {
                            invert = true;
                            f = self.long(n);
                        }
                    }
                    f.ok_or_else(|| format!("unknown long flag '{}'", flag_token.string()))?
                } else {
                    let c = token.value.chars().next().unwrap();
                    self.short(c).ok_or_else(|| format!("unknown short flag '{}'", flag_token.string()))?
                };
                self.next();
                let value = if flag.is_bool() {
                    if invert { "false" } else { "true" }.to_string()
                } else {
                    if invert {
                        self.peek.push(token);
                        return Err(format!("unknown long flag '{}'", flag_token.string()));
                    }
                    let t = self.peek();
                    if t.typ != TokType::Arg {
                        self.peek.push(t);
                        return Err(format!("expected argument for flag '{}'", flag_token.string()));
                    }
                    self.next();
                    t.value
                };
                self.elements.push(Elem::Flag(flag, value));
                Ok(Some(flag))
            }
            _ => Ok(None),
        }
    }
}

impl App {
    /// Creates the application with kingpin's built-in flags (help, the hidden ones, version) and the `help`
    /// command. `flags` and `cmds` are the application's own.
    pub fn new(name: &str, help: &str, version: &str, flags: Vec<Flag>, cmds: Vec<Cmd>) -> App {
        let mut all = vec![
            Flag::new("help", "Show context-sensitive help (also try --help-long and --help-man).", Kind::Bool).short('h'),
            Flag::new("help-long", "Generate long help.", Kind::Bool).hidden(),
            Flag::new("help-man", "Generate a man page.", Kind::Bool).hidden(),
            Flag::new("completion-bash", "Output possible completions for the given args.", Kind::Bool).hidden(),
            Flag::new("completion-script-bash", "Generate completion script for bash.", Kind::Bool).hidden(),
            Flag::new("completion-script-zsh", "Generate completion script for ZSH.", Kind::Bool).hidden(),
            Flag::new("version", "Show application version.", Kind::Bool),
        ];
        all.extend(flags);
        // kingpin adds the `help` command only to applications with commands.
        let mut c = if cmds.is_empty() {
            Vec::new()
        } else {
            vec![Cmd::new("help", "Show help.").arg(Arg::new("command", "Show help on command.", Kind::Strings))]
        };
        c.extend(cmds);
        for cmd in c.iter_mut() {
            cmd.set_full("");
        }
        App { name: name.to_string(), help: help.to_string(), version: version.to_string(), flags: all, cmds: c }
    }

    fn parse_context<'a>(&'a self, ignore_default: bool, args: &[String]) -> (Ctx<'a>, Result<(), String>) {
        let _ = ignore_default; // promtool has no default commands.
        let mut ctx = Ctx::new(args);
        for f in self.flags.iter() {
            ctx.flags.push(f);
        }
        let mut cmds: &'a [Cmd] = &self.cmds;
        let r = (|| {
            while !ctx.eol() && !ctx.error() {
                let token = ctx.peek();
                match token.typ {
                    TokType::Long | TokType::Short => {
                        ctx.parse_flag()?;
                    }
                    TokType::Arg => {
                        if !cmds.is_empty() {
                            let name = token.string();
                            let cmd = match cmds.iter().find(|c| c.name == name) {
                                Some(c) => c,
                                None => {
                                    return Err(format!(
                                        "expected command but got {}",
                                        promql::parser::quote::go_quote(&name)
                                    ));
                                }
                            };
                            ctx.matched_cmd(cmd);
                            cmds = &cmd.cmds;
                            ctx.next();
                        } else if !ctx.arguments.is_empty() {
                            let Some(arg) = ctx.next_arg() else { break };
                            ctx.elements.push(Elem::Arg(arg, token.string()));
                            ctx.next();
                        } else {
                            break;
                        }
                    }
                    _ => break,
                }
            }
            if ctx.error() {
                return Err(ctx.peek().value);
            }
            if !ctx.eol() {
                return Err(format!("unexpected {}", ctx.peek().string()));
            }
            Ok(())
        })();
        (ctx, r)
    }

    fn errorf(&self, err: &mut dyn Write, msg: &str) {
        let _ = writeln!(err, "{}: error: {}", self.name, msg);
    }

    /// Go `app.Parse(os.Args[1:])` followed by `kingpin.MustParse`.
    pub fn parse(&self, args: &[String], out: &mut dyn Write, err: &mut dyn Write) -> Outcome {
        match self.try_parse(args, out) {
            GoParse::Run(p) => Outcome::Run(p),
            GoParse::Exit(c) => Outcome::Exit(c),
            GoParse::ContextErr(e) | GoParse::ValueErr(e) => {
                self.errorf(err, &format!("{e}, try --help"));
                Outcome::Exit(1)
            }
        }
    }

    /// Go `app.Parse(args)`: help and version are printed to `out` (and give `Exit`); errors are returned with
    /// the stage that produced them.
    pub fn try_parse(&self, args: &[String], out: &mut dyn Write) -> GoParse {
        let (ctx, parse_err) = self.parse_context(false, args);
        let mut parsed = Parsed::default();

        // setDefaults.
        let skip_defaults =
            ctx.elements.iter().any(|e| matches!(e, Elem::Flag(f, _) if f.name == "help" || f.name == "version"));
        if !skip_defaults {
            for f in ctx.flags.iter() {
                if !ctx.elements.iter().any(|e| matches!(e, Elem::Flag(g, _) if g.name == f.name)) {
                    for d in f.default.iter() {
                        if let Err(e) = set_value(&mut parsed, f.name, &f.kind, d) {
                            return GoParse::ValueErr(e);
                        }
                    }
                }
            }
            for a in ctx.arguments.iter() {
                if !ctx.elements.iter().any(|e| matches!(e, Elem::Arg(g, _) if g.name == a.name)) {
                    for d in a.default.iter() {
                        if let Err(e) = set_value(&mut parsed, a.name, &a.kind, d) {
                            return GoParse::ValueErr(e);
                        }
                    }
                }
            }
        }

        // setValues.
        let mut selected = Vec::new();
        let set_values_err = (|| {
            let mut flag_set: Vec<&str> = Vec::new();
            let mut last_cmd: Option<&Cmd> = None;
            for e in ctx.elements.iter() {
                match e {
                    Elem::Flag(f, v) => {
                        if flag_set.contains(&f.name) && !f.kind.cumulative() {
                            return Err(format!("flag '{}' cannot be repeated", f.name));
                        }
                        if !flag_set.contains(&f.name) {
                            // A user value replaces a default (defaults are only set for flags not given).
                            parsed.values.remove(f.name);
                            parsed.maps.remove(f.name);
                        }
                        set_value(&mut parsed, f.name, &f.kind, v)?;
                        flag_set.push(f.name);
                    }
                    Elem::Arg(a, v) => set_value(&mut parsed, a.name, &a.kind, v)?,
                    Elem::Cmd(c) => {
                        selected.push(c.name);
                        last_cmd = Some(c);
                    }
                }
            }
            if let Some(c) = last_cmd {
                if !c.cmds.is_empty() {
                    return Err(format!("must select a subcommand of '{}'", c.full));
                }
            }
            Ok(())
        })();

        // applyPreActions.
        for e in ctx.elements.iter() {
            match e {
                Elem::Flag(f, _) if f.name == "version" => {
                    let _ = writeln!(out, "{}", self.version);
                    return GoParse::Exit(0);
                }
                Elem::Flag(f, _) if f.name.starts_with("help-") || f.name.starts_with("completion-") => {
                    return GoParse::ContextErr(format!("--{} is not supported by this port", f.name));
                }
                Elem::Cmd(c) if c.name == "help" && std::ptr::eq(*c, &self.cmds[0]) => {
                    let command = parsed.strings("command");
                    let (hctx, herr) = self.parse_context(true, &command);
                    if let Err(e) = herr {
                        return GoParse::ContextErr(e);
                    }
                    let _ = out.write_all(self.usage(&hctx).as_bytes());
                    return GoParse::Exit(0);
                }
                _ => {}
            }
        }

        if let Err(e) = parse_err {
            return GoParse::ContextErr(e);
        }

        // maybeHelp.
        if ctx.elements.iter().any(|e| matches!(e, Elem::Flag(f, _) if f.name == "help")) {
            let (hctx, _) = self.parse_context(true, &ctx.raw);
            let _ = out.write_all(self.usage(&hctx).as_bytes());
            return GoParse::Exit(0);
        }

        if let Err(e) = set_values_err {
            return GoParse::ValueErr(e);
        }

        // validateRequired.
        let mut missing = Vec::new();
        for f in ctx.flags.iter() {
            if f.required
                && f.default.is_empty()
                && !ctx.elements.iter().any(|e| matches!(e, Elem::Flag(g, _) if g.name == f.name))
            {
                missing.push(format!("'--{}'", f.name));
            }
        }
        if !missing.is_empty() {
            return GoParse::ValueErr(format!("required flag(s) {} not provided", missing.join(", ")));
        }
        for a in ctx.arguments.iter() {
            if a.required
                && a.default.is_empty()
                && !ctx.elements.iter().any(|e| matches!(e, Elem::Arg(g, _) if g.name == a.name))
            {
                return GoParse::ValueErr(format!("required argument '{}' not provided", a.name));
            }
        }

        let command = selected.join(" ");
        if command.is_empty() && !self.cmds.is_empty() {
            // ErrCommandNotSpecified: usage, exit 0.
            let _ = out.write_all(self.usage(&ctx).as_bytes());
            return GoParse::Exit(0);
        }
        parsed.command = command;
        for e in ctx.elements.iter() {
            if let Elem::Flag(f, _) = e {
                if !parsed.user_flags.iter().any(|n| n == f.name) {
                    parsed.user_flags.push(f.name.to_string());
                }
            }
        }
        GoParse::Run(parsed)
    }

    /// Go `app.Usage(args)`: the usage for the context of `args` (Err: the context error, which Go reports with
    /// `FatalIfError`).
    pub fn usage_for(&self, args: &[String]) -> Result<String, String> {
        let (ctx, e) = self.parse_context(true, args);
        e?;
        Ok(self.usage(&ctx))
    }

    fn width() -> i64 {
        std::env::var("COLUMNS").ok().and_then(|c| c.parse::<i64>().ok()).unwrap_or(80)
    }

    /// The kingpin `DefaultUsageTemplate` for a parse context.
    fn usage(&self, ctx: &Ctx) -> String {
        let width = Self::width();
        let indent = 2i64;
        let wrap = |ind: i64, s: &str| -> String {
            let t = " ".repeat(ind as usize);
            to_text(s, &t, &format!("  {t}"), width - ind)
        };
        let format_command = |flags: &[Flag], args: &[Arg]| -> String {
            let mut s = String::new();
            let fs = if flags.is_empty() { String::new() } else { flag_summary(flags) };
            if !fs.is_empty() {
                s.push(' ');
                s.push_str(&fs);
            }
            for a in args {
                s.push(' ');
                if !a.required {
                    s.push('[');
                }
                s.push('<');
                s.push_str(a.name);
                s.push('>');
                if a.kind.cumulative() {
                    s.push_str("...");
                }
                if !a.required {
                    s.push(']');
                }
            }
            s
        };
        let format_usage = |flags: &[Flag], args: &[Arg], has_cmds: bool, help: &str| -> String {
            let mut s = format_command(flags, args);
            if has_cmds {
                s.push_str(" <command> [<args> ...]");
            }
            s.push('\n');
            if !help.is_empty() {
                s.push('\n');
                s.push_str(&wrap(0, help));
            }
            s
        };
        let format_commands = |cmds: &[Cmd]| -> String {
            let mut fl = Vec::new();
            flatten(cmds, &mut fl);
            let mut s = String::new();
            for c in fl {
                if c.hidden {
                    continue;
                }
                s.push_str(&c.full);
                s.push_str(&format_command(&c.flags, &c.args));
                s.push('\n');
                s.push_str(&wrap(4, &c.help));
                s.push('\n');
            }
            s
        };
        let mut out = String::new();
        match ctx.selected {
            Some(c) => {
                out.push_str(&format!("usage: {} {}", self.name, c.full));
                out.push_str(&format_usage(&c.flags, &c.args, !c.cmds.is_empty(), &c.help));
            }
            None => {
                out.push_str(&format!("usage: {}", self.name));
                out.push_str(&format_usage(&self.flags, &[], !self.cmds.is_empty(), &self.help));
            }
        }
        out.push_str("\n\n");
        if !ctx.flags.is_empty() {
            out.push_str("Flags:\n");
            let have_short = ctx.flags.iter().any(|f| f.short.is_some());
            let rows: Vec<(String, String)> = ctx
                .flags
                .iter()
                .filter(|f| !f.hidden)
                .map(|f| (format_flag(have_short, f), f.help.clone()))
                .collect();
            out.push_str(&format_two_columns(indent, indent, width, &rows));
            out.push('\n');
        }
        if !ctx.arguments.is_empty() {
            out.push_str("Args:\n");
            let rows: Vec<(String, String)> = ctx
                .arguments
                .iter()
                .map(|a| {
                    let mut s = format!("<{}>", a.name);
                    if !a.required {
                        s = format!("[{s}]");
                    }
                    (s, a.help.clone())
                })
                .collect();
            out.push_str(&format_two_columns(indent, indent, width, &rows));
            out.push('\n');
        }
        match ctx.selected {
            Some(c) => {
                if !c.cmds.is_empty() {
                    out.push_str("Subcommands:\n");
                    out.push_str(&format_commands(&c.cmds));
                    out.push('\n');
                }
            }
            None => {
                if !self.cmds.is_empty() {
                    out.push_str("Commands:\n");
                    out.push_str(&format_commands(&self.cmds));
                    out.push('\n');
                }
            }
        }
        out
    }

    /// Go `documentcli.GenerateMarkdown(app.Model(), w)`.
    pub fn generate_markdown(&self) -> String {
        let mut w = format!("---\ntitle: {}\n---\n\n{}\n", self.name, self.help);
        write_flag_table(&mut w, 0, &self.flags);
        // The application has no arguments.
        let mut rows = vec![vec!["Command".to_string(), "Description".to_string()]];
        for c in self.cmds.iter() {
            if !c.hidden {
                rows.push(vec![c.full.clone(), c.help.clone()]);
            }
        }
        write_table(&mut w, &rows, "## Commands");
        write_subcommands(&mut w, 1, &self.name, &self.cmds);
        w
    }
}

/// kingpin `formatFlag`.
fn format_flag(have_short: bool, f: &Flag) -> String {
    let mut name = f.name.to_string();
    if f.is_bool() {
        name = format!("[no-]{name}");
    }
    let mut s = match f.short {
        Some(c) => format!("-{c}, --{name}"),
        None if have_short => format!("    --{name}"),
        None => format!("--{name}"),
    };
    if !f.is_bool() {
        s.push('=');
        s.push_str(&f.format_placeholder());
    }
    if f.kind.cumulative() {
        s.push_str(" ...");
    }
    s
}

/// kingpin `formatTwoColumns`.
fn format_two_columns(indent: i64, padding: i64, width: i64, rows: &[(String, String)]) -> String {
    let mut s = 0usize;
    for r in rows {
        let c = r.0.len();
        if c > s && c < 30 {
            s = c;
        }
    }
    let indent_str = " ".repeat(indent as usize);
    let offset_str = " ".repeat(s + padding as usize);
    let mut w = String::new();
    for r in rows {
        let buf = to_text(&r.1, "", "  ", width - s as i64 - padding - indent);
        let trimmed = buf.trim_end_matches('\n');
        let lines: Vec<&str> = trimmed.split('\n').collect();
        w.push_str(&format!("{indent_str}{:<width$}{}", r.0, " ".repeat(padding as usize), width = s));
        if r.0.len() >= 30 {
            w.push_str(&format!("\n{indent_str}{offset_str}"));
        }
        w.push_str(lines[0]);
        w.push('\n');
        for l in &lines[1..] {
            w.push_str(&format!("{indent_str}{offset_str}{l}\n"));
        }
    }
    w
}

fn write_flag_table(w: &mut String, level: usize, flags: &[Flag]) {
    if flags.is_empty() {
        return;
    }
    let mut rows = vec![vec!["Flag".to_string(), "Description".to_string(), "Default".to_string()]];
    for f in flags.iter().filter(|f| !f.hidden) {
        let default = match f.default.first() {
            Some(d) if !d.is_empty() => format!("`{d}`"),
            _ => String::new(),
        };
        let mut name = format!("<code class=\"text-nowrap\">--{}</code>", f.name);
        if let Some(c) = f.short {
            name = format!("<code class=\"text-nowrap\">-{c}</code>, <code class=\"text-nowrap\">--{}</code>", f.name);
        }
        if f.kind.is_accumulator() {
            name = format!("{name} <code class=\"text-nowrap\">...</code>");
        }
        rows.push(vec![name, f.help.replace('|', "\\|"), default]);
    }
    write_table(w, &rows, &format!("{} Flags", "#".repeat(level + 2)));
}

fn write_arg_table(w: &mut String, level: usize, args: &[Arg]) {
    if args.is_empty() {
        return;
    }
    let mut rows =
        vec![vec!["Argument".to_string(), "Description".to_string(), "Default".to_string(), "Required".to_string()]];
    for a in args {
        let default = a.default.first().map(|d| format!("`{d}`")).unwrap_or_default();
        let required = if a.required { "Yes" } else { "" };
        rows.push(vec![a.name.to_string(), a.help.clone(), default, required.to_string()]);
    }
    write_table(w, &rows, &format!("{} Arguments", "#".repeat(level + 2)));
}

fn write_table(w: &mut String, data: &[Vec<String>], header: &str) {
    if data.len() < 2 {
        return;
    }
    w.push_str(&format!("\n\n{header}\n\n"));
    let cols: Vec<usize> = (0..data[0].len()).filter(|&j| data[1..].iter().any(|r| !r[j].is_empty())).collect();
    w.push('|');
    for &j in &cols {
        w.push_str(&format!(" {} |", data[0][j]));
    }
    w.push('\n');
    w.push('|');
    for _ in &cols {
        w.push_str(" --- |");
    }
    w.push('\n');
    for row in &data[1..] {
        w.push('|');
        for &j in &cols {
            w.push_str(&format!(" {} |", row[j]));
        }
        w.push('\n');
    }
    w.push_str("\n\n");
}

fn write_subcommands(w: &mut String, level: usize, model_name: &str, cmds: &[Cmd]) {
    let level = (level + 1).min(4);
    for c in cmds {
        if c.hidden {
            continue;
        }
        let help = format_hyphenated_words(&c.help);
        w.push_str(&format!("\n\n{} `{} {}`\n\n{}\n\n", "#".repeat(level + 1), model_name, c.full, help));
        write_flag_table(w, level, &c.flags);
        write_arg_table(w, level, &c.args);
        if !c.cmds.is_empty() {
            write_subcommands(w, level + 1, model_name, &c.cmds);
        }
    }
}

/// documentcli `formatHyphenatedWords`: wraps `\B--\w+\b` in backticks.
fn format_hyphenated_words(input: &str) -> String {
    let b = input.as_bytes();
    let is_word = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
    let mut out = String::new();
    let mut last = 0;
    let mut i = 0;
    while i + 2 < b.len() {
        // \B before '-': the previous byte is not a word byte either.
        if b[i] == b'-' && b[i + 1] == b'-' && is_word(b[i + 2]) && !(i > 0 && is_word(b[i - 1])) {
            let mut j = i + 2;
            while j < b.len() && is_word(b[j]) {
                j += 1;
            }
            out.push_str(&input[last..i]);
            out.push('`');
            out.push_str(&input[i..j]);
            out.push('`');
            last = j;
            i = j;
        } else {
            i += 1;
        }
    }
    out.push_str(&input[last..]);
    out
}

fn set_value(p: &mut Parsed, name: &str, kind: &Kind, v: &str) -> Result<(), String> {
    match kind {
        Kind::Bool => {
            parse_bool(v)?;
        }
        Kind::String | Kind::Strings => {}
        Kind::StringMap => {
            // regexp "[:=]" split in two.
            let Some(i) = v.find([':', '=']) else {
                return Err(format!("expected KEY=VALUE got '{v}'"));
            };
            p.maps.entry(name.to_string()).or_default().insert(v[..i].to_string(), v[i + 1..].to_string());
        }
        Kind::Duration => {
            parse_duration(v)?;
        }
        Kind::Int => {
            parse_go_float(v).map_err(|_| format!("strconv.ParseFloat: parsing {}: invalid syntax", go_q(v)))?;
        }
        Kind::Int64 => {
            parse_go_int(v)?;
        }
        Kind::Enum(opts) => {
            if !opts.contains(&v) {
                return Err(format!("enum value must be one of {}, got '{v}'", opts.join(",")));
            }
        }
        Kind::ExistingFile | Kind::ExistingFiles => match std::fs::metadata(v) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(format!("path '{v}' does not exist")),
            Err(e) => return Err(format!("stat {v}: {}", crate::io_err_text(&e))),
            Ok(m) if m.is_dir() => {
                let base = std::path::Path::new(v).file_name().map(|s| s.to_string_lossy().to_string());
                return Err(format!("'{}' is a directory", base.unwrap_or_else(|| v.to_string())));
            }
            Ok(_) => {}
        },
        Kind::Url => {
            yaml::url::parse(v).map_err(|e| format!("invalid URL: {e}"))?;
        }
        Kind::Bytes => {
            yaml::types::parse_base2_bytes(v)?;
        }
        Kind::Float => {
            parse_go_float(v).map_err(|_| format!("strconv.ParseFloat: parsing {}: invalid syntax", go_q(v)))?;
        }
        Kind::ModelDuration => {
            yaml::types::parse_duration(v)?;
        }
        Kind::Custom(f, _) => {
            let prev = p.values.get(name).cloned().unwrap_or_default();
            f(&prev, v)?;
        }
        Kind::OpenFile => {
            use std::os::unix::fs::OpenOptionsExt;
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o644)
                .open(v)
                .map_err(|e| format!("open {v}: {}", crate::io_err_text(&e)))?;
        }
    }
    p.values.entry(name.to_string()).or_default().push(v.to_string());
    Ok(())
}

fn go_q(s: &str) -> String {
    promql::parser::quote::go_quote(s)
}

/// Go `strconv.ParseBool`.
pub fn parse_bool(s: &str) -> Result<bool, String> {
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(format!("strconv.ParseBool: parsing {}: invalid syntax", go_q(s))),
    }
}

/// Go `strconv.ParseFloat(s, 64)` (decimal and the special values; enough for flag values).
pub fn parse_go_float(s: &str) -> Result<f64, ()> {
    let t = s.replace('_', "");
    match t.to_ascii_lowercase().trim_start_matches(['+', '-']) {
        "inf" | "infinity" | "nan" => {}
        x if x.starts_with("0x") => return Err(()),
        _ => {}
    }
    t.parse::<f64>().map_err(|_| ())
}

/// Go `strconv.ParseInt(s, 0, 64)` with its error texts.
pub fn parse_go_int(s: &str) -> Result<i64, String> {
    let syntax = || format!("strconv.ParseInt: parsing {}: invalid syntax", go_q(s));
    let range = || format!("strconv.ParseInt: parsing {}: value out of range", go_q(s));
    let (neg, body) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    if body.is_empty() {
        return Err(syntax());
    }
    let lower = body.to_ascii_lowercase();
    let (base, digits, prefixed) = if let Some(r) = lower.strip_prefix("0x") {
        (16, r.to_string(), true)
    } else if let Some(r) = lower.strip_prefix("0b") {
        (2, r.to_string(), true)
    } else if let Some(r) = lower.strip_prefix("0o") {
        (8, r.to_string(), true)
    } else if lower.len() > 1 && lower.starts_with('0') {
        (8, lower[1..].to_string(), true)
    } else {
        (10, lower.clone(), false)
    };
    // Underscores are allowed only with a base prefix, between digits.
    if digits.contains('_') {
        if !prefixed || digits.starts_with('_') || digits.ends_with('_') || digits.contains("__") {
            return Err(syntax());
        }
    }
    let digits = digits.replace('_', "");
    if digits.is_empty() {
        return Err(syntax());
    }
    let mut v: u128 = 0;
    for c in digits.chars() {
        let d = c.to_digit(base).ok_or_else(syntax)? as u128;
        v = v * base as u128 + d;
        if v > (1u128 << 64) {
            v = 1u128 << 64;
        }
    }
    if neg {
        if v > 1u128 << 63 {
            return Err(range());
        }
        Ok((v as i128).wrapping_neg() as i64)
    } else {
        if v > i64::MAX as u128 {
            return Err(range());
        }
        Ok(v as i64)
    }
}

/// `github.com/xhit/go-str2duration/v2.ParseDuration` (Go `time.ParseDuration` plus `d` and `w`), in
/// nanoseconds.
pub fn parse_duration(orig: &str) -> Result<i64, String> {
    let invalid = || format!("time: invalid duration \"{orig}\"");
    let mut s = orig.as_bytes();
    let mut d: i64 = 0;
    let mut neg = false;
    if !s.is_empty() && (s[0] == b'-' || s[0] == b'+') {
        neg = s[0] == b'-';
        s = &s[1..];
    }
    if s == b"0" {
        return Ok(0);
    }
    if s.is_empty() {
        return Err(invalid());
    }
    while !s.is_empty() {
        let mut f: i64 = 0;
        let mut scale: f64 = 1.0;
        if !(s[0] == b'.' || s[0].is_ascii_digit()) {
            return Err(invalid());
        }
        let pl = s.len();
        let mut v: i64 = 0;
        let mut i = 0;
        while i < s.len() && s[i].is_ascii_digit() {
            if v > (i64::MAX) / 10 {
                return Err(invalid());
            }
            v = v * 10 + (s[i] - b'0') as i64;
            if v < 0 {
                return Err(invalid());
            }
            i += 1;
        }
        s = &s[i..];
        let pre = pl != s.len();
        let mut post = false;
        if !s.is_empty() && s[0] == b'.' {
            s = &s[1..];
            let pl = s.len();
            let mut i = 0;
            let mut overflow = false;
            while i < s.len() && s[i].is_ascii_digit() {
                if !overflow {
                    if f > i64::MAX / 10 {
                        overflow = true;
                    } else {
                        let y = f * 10 + (s[i] - b'0') as i64;
                        if y < 0 {
                            overflow = true;
                        } else {
                            f = y;
                            scale *= 10.0;
                        }
                    }
                }
                i += 1;
            }
            s = &s[i..];
            post = pl != s.len();
        }
        if !pre && !post {
            return Err(invalid());
        }
        let mut i = 0;
        while i < s.len() && !(s[i] == b'.' || s[i].is_ascii_digit()) {
            i += 1;
        }
        if i == 0 {
            return Err(format!("time: missing unit in duration \"{orig}\""));
        }
        let u = std::str::from_utf8(&s[..i]).unwrap_or("");
        s = &s[i..];
        let unit: i64 = match u {
            "ns" => 1,
            "us" | "\u{b5}s" | "\u{3bc}s" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            "d" => 86_400_000_000_000,
            "w" => 604_800_000_000_000,
            _ => return Err(format!("time: unknown unit \"{u}\" in duration \"{orig}\"")),
        };
        if v > i64::MAX / unit {
            return Err(invalid());
        }
        v *= unit;
        if f > 0 {
            v += (f as f64 * (unit as f64 / scale)) as i64;
            if v < 0 {
                return Err(invalid());
            }
        }
        d += v;
        if d < 0 {
            return Err(invalid());
        }
    }
    Ok(if neg { -d } else { d })
}
