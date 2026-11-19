//! Port of Go `html/template`: contextual autoescaping on top of [`crate::text`].

pub mod content;
pub mod context;
mod escape;
mod transition;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use crate::gofmt::quote;
use crate::text::{self, FuncMap};
use crate::value::Value;
use escape::{Env, Escaper, escape_template};

pub use context::Error;

/// The escaping state of one template (Go `Template.escapeErr`).
#[derive(Clone)]
enum EscapeState {
    NotEscaped,
    Ok,
    Err(String),
}

struct NsInner {
    set: HashMap<String, EscapeState>,
    escaped: bool,
    esc: Escaper,
}

/// The state shared by associated html templates (Go `nameSpace`).
struct NameSpace {
    inner: Mutex<NsInner>,
}

impl NameSpace {
    fn lock(&self) -> MutexGuard<'_, NsInner> {
        match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }
}

/// An html template (Go `html/template.Template`). Cloning gives another handle to the same template.
#[derive(Clone)]
pub struct Template {
    text: text::Template,
    ns: Arc<NameSpace>,
}

/// A weak handle to an html template (for functions that call back into their own template set).
#[derive(Clone)]
pub struct WeakTemplate {
    name: String,
    text: Weak<text::Common>,
    ns: Weak<NameSpace>,
}

impl WeakTemplate {
    /// The template, if it still exists.
    pub fn upgrade(&self) -> Option<Template> {
        let common = self.text.upgrade()?;
        let ns = self.ns.upgrade()?;
        let mut t = text::Template::new(&self.name);
        t.common = common;
        Some(Template { text: t, ns })
    }
}

impl Template {
    /// Go `template.New`.
    pub fn new(name: &str) -> Template {
        let mut set = HashMap::new();
        set.insert(name.to_string(), EscapeState::NotEscaped);
        Template {
            text: text::Template::new(name),
            ns: Arc::new(NameSpace { inner: Mutex::new(NsInner { set, escaped: false, esc: Escaper::default() }) }),
        }
    }

    /// A weak handle.
    pub fn downgrade(&self) -> WeakTemplate {
        WeakTemplate { name: self.name().to_string(), text: Arc::downgrade(&self.text.common), ns: Arc::downgrade(&self.ns) }
    }

    /// Go `Name`.
    pub fn name(&self) -> &str {
        self.text.name()
    }

    /// The underlying text template.
    pub fn text(&self) -> &text::Template {
        &self.text
    }

    /// Go `Option`.
    pub fn option(&self, opts: &[impl AsRef<str>]) -> Result<(), String> {
        self.text.option(opts)
    }

    /// Go `Funcs`.
    pub fn funcs(&self, m: &FuncMap) -> &Template {
        self.text.funcs(m);
        self
    }

    /// Adds one function.
    pub fn func(&self, name: &str, f: crate::value::Func) -> &Template {
        self.text.func(name, f);
        self
    }

    fn check_can_parse(&self) -> Result<(), String> {
        if self.ns.lock().escaped {
            return Err("html/template: cannot Parse after Execute".to_string());
        }
        Ok(())
    }

    /// Go `t.New(name)`.
    pub fn new_associated(&self, name: &str) -> Template {
        let mut ns = self.ns.lock();
        ns.set.insert(name.to_string(), EscapeState::NotEscaped);
        Template { text: self.text.new_associated(name), ns: self.ns.clone() }
    }

    /// Go `Parse`.
    pub fn parse(&self, text: &str) -> Result<&Template, String> {
        self.check_can_parse()?;
        self.text.parse(text)?;
        let names: Vec<String> = self.text.templates().iter().map(|t| t.name().to_string()).collect();
        let mut ns = self.ns.lock();
        for name in names {
            ns.set.entry(name).or_insert(EscapeState::NotEscaped);
        }
        Ok(self)
    }

    /// Go `ParseFiles`.
    pub fn parse_files(&self, filenames: &[impl AsRef<str>]) -> Result<&Template, String> {
        self.check_can_parse()?;
        if filenames.is_empty() {
            return Err("html/template: no files named in call to ParseFiles".to_string());
        }
        for f in filenames {
            let (name, s) = text::read_file(f.as_ref())?;
            let tmpl = if name == self.name() { self.clone() } else { self.new_associated(&name) };
            tmpl.parse(&s)?;
        }
        Ok(self)
    }

    /// Go `Lookup`.
    pub fn lookup(&self, name: &str) -> Option<Template> {
        if self.ns.lock().set.contains_key(name) {
            Some(Template { text: self.text.new_associated(name), ns: self.ns.clone() })
        } else {
            None
        }
    }

    /// Go `DefinedTemplates`.
    pub fn defined_templates(&self) -> String {
        self.text.defined_templates()
    }

    fn env_names(ns: &NsInner) -> HashSet<String> {
        ns.set.keys().cloned().collect()
    }

    /// Go `escape`.
    fn escape(&self) -> Result<(), String> {
        let mut ns = self.ns.lock();
        ns.escaped = true;
        let name = self.name().to_string();
        match ns.set.get(&name).cloned() {
            Some(EscapeState::Ok) => Ok(()),
            Some(EscapeState::Err(e)) => Err(e),
            _ => {
                let Some(tree) = self.text.tree() else {
                    return Err(format!("template: {} is an incomplete or empty template", quote(&name)));
                };
                let loc = tree.root.as_ref().map(|r| tree.error_location(r.pos));
                self.run_escape(&mut ns, loc, &name)
            }
        }
    }

    fn run_escape(&self, ns: &mut NsInner, loc: Option<String>, name: &str) -> Result<(), String> {
        let names = Self::env_names(ns);
        let env = Env { text: &self.text, html_names: &names };
        let r = escape_template(&mut ns.esc, &env, loc, name);
        match r {
            Ok(()) => {
                if let Some(s) = ns.set.get_mut(name) {
                    *s = EscapeState::Ok;
                }
                Ok(())
            }
            Err(e) => {
                let msg = e.to_string();
                if let Some(s) = ns.set.get_mut(name) {
                    *s = EscapeState::Err(msg.clone());
                }
                Err(msg)
            }
        }
    }

    /// Go `Execute`: escapes the template (once) and applies it to `data`.
    pub fn execute(&self, data: &Value) -> Result<String, String> {
        self.escape()?;
        self.text.execute(data)
    }

    /// Go `ExecuteTemplate`.
    pub fn execute_template(&self, name: &str, data: &Value) -> Result<String, String> {
        let tmpl = self.lookup_and_escape_template(name)?;
        tmpl.execute(data)
    }

    fn lookup_and_escape_template(&self, name: &str) -> Result<text::Template, String> {
        let mut ns = self.ns.lock();
        ns.escaped = true;
        let Some(state) = ns.set.get(name).cloned() else {
            return Err(format!("html/template: {} is undefined", quote(name)));
        };
        if let EscapeState::Err(e) = &state {
            return Err(e.clone());
        }
        let t = self.text.new_associated(name);
        let Some(tree) = t.tree().filter(|tr| tr.root.is_some()) else {
            return Err(format!("html/template: {} is an incomplete template", quote(name)));
        };
        if let EscapeState::NotEscaped = state {
            let loc = tree.root.as_ref().map(|r| tree.error_location(r.pos));
            self.run_escape(&mut ns, loc, name)?;
        }
        Ok(t)
    }
}
