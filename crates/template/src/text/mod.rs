//! Port of Go `text/template`: template sets, options, builtins and the executor.

mod exec;
pub mod funcs;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::gofmt::quote;
use crate::parse::{self, Mode, Tree, is_empty_tree};
use crate::value::{Func, Value};

/// A map of named functions (Go `template.FuncMap`).
pub type FuncMap = HashMap<String, Func>;

/// The `missingkey` option.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum MissingKey {
    /// `missingkey=default` or `missingkey=invalid`: an invalid value (prints `<no value>`).
    #[default]
    Invalid,
    /// `missingkey=zero`: the zero value of the map element type.
    ZeroValue,
    /// `missingkey=error`: an execution error.
    Error,
}

#[derive(Default)]
pub(crate) struct CommonInner {
    /// Templates by name (`None`: a template without a tree).
    pub tmpl: HashMap<String, Option<Arc<Tree>>>,
    pub funcs: FuncMap,
    pub option: MissingKey,
}

/// The state shared by associated templates (Go `common`).
#[derive(Default)]
pub(crate) struct Common {
    inner: Mutex<CommonInner>,
}

impl Common {
    pub(crate) fn lock(&self) -> MutexGuard<'_, CommonInner> {
        match self.inner.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }
}

/// A template (Go `text/template.Template`). Cloning gives another handle to the same template.
#[derive(Clone)]
pub struct Template {
    name: String,
    pub(crate) common: Arc<Common>,
    left_delim: String,
    right_delim: String,
}

impl Template {
    /// Go `template.New`.
    pub fn new(name: &str) -> Template {
        Template {
            name: name.to_string(),
            common: Arc::new(Common::default()),
            left_delim: String::new(),
            right_delim: String::new(),
        }
    }

    /// Go `Name`.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Go `t.New(name)`: a new template associated with this one.
    pub fn new_associated(&self, name: &str) -> Template {
        Template {
            name: name.to_string(),
            common: self.common.clone(),
            left_delim: self.left_delim.clone(),
            right_delim: self.right_delim.clone(),
        }
    }

    /// Go `Delims`.
    pub fn delims(&mut self, left: &str, right: &str) -> &mut Template {
        self.left_delim = left.to_string();
        self.right_delim = right.to_string();
        self
    }

    /// Go `Option`. An unrecognized option is an error with Go's panic text.
    pub fn option(&self, opts: &[impl AsRef<str>]) -> Result<(), String> {
        for opt in opts {
            let opt = opt.as_ref();
            if opt.is_empty() {
                return Err("empty option string".to_string());
            }
            let v = match opt.split_once('=') {
                Some(("missingkey", "invalid" | "default")) => MissingKey::Invalid,
                Some(("missingkey", "zero")) => MissingKey::ZeroValue,
                Some(("missingkey", "error")) => MissingKey::Error,
                _ => return Err(format!("unrecognized option: {}", opt)),
            };
            self.common.lock().option = v;
        }
        Ok(())
    }

    /// The `missingkey` option in effect.
    pub fn missing_key(&self) -> MissingKey {
        self.common.lock().option
    }

    /// Go `Funcs`: adds the functions to the template's function map.
    pub fn funcs(&self, m: &FuncMap) -> &Template {
        let mut c = self.common.lock();
        for (k, v) in m {
            c.funcs.insert(k.clone(), v.clone());
        }
        self
    }

    /// Adds one function.
    pub fn func(&self, name: &str, f: Func) -> &Template {
        self.common.lock().funcs.insert(name.to_string(), f);
        self
    }

    /// Go `Lookup`.
    pub fn lookup(&self, name: &str) -> Option<Template> {
        if self.common.lock().tmpl.contains_key(name) { Some(self.new_associated(name)) } else { None }
    }

    /// The parse tree (Go `t.Tree`), `None` when the template is not defined or incomplete.
    pub fn tree(&self) -> Option<Arc<Tree>> {
        self.common.lock().tmpl.get(&self.name).cloned().flatten()
    }

    /// Sets the tree of `name` in the set (used by `html/template`).
    pub(crate) fn set_tree(&self, name: &str, tree: Option<Arc<Tree>>) {
        self.common.lock().tmpl.insert(name.to_string(), tree);
    }

    /// Go `Templates`: the templates of the set.
    pub fn templates(&self) -> Vec<Template> {
        let names: Vec<String> = self.common.lock().tmpl.keys().cloned().collect();
        names.iter().map(|n| self.new_associated(n)).collect()
    }

    /// Go `DefinedTemplates`.
    pub fn defined_templates(&self) -> String {
        let c = self.common.lock();
        let mut b = String::new();
        let mut names: Vec<&String> =
            c.tmpl.iter().filter(|(_, t)| t.as_ref().is_some_and(|t| t.root.is_some())).map(|(n, _)| n).collect();
        names.sort();
        for name in names {
            if b.is_empty() {
                b.push_str("; defined templates are: ");
            } else {
                b.push_str(", ");
            }
            b.push_str(&quote(name));
        }
        b
    }

    /// Go `Parse`: parses `text` as the body of this template; `{{define}}`d templates join the set.
    pub fn parse(&self, text: &str) -> Result<&Template, String> {
        let funcs: Vec<String> = self.common.lock().funcs.keys().cloned().collect();
        let has = |n: &str| funcs.iter().any(|f| f == n) || funcs::is_builtin(n);
        let trees = parse::parse(&self.name, text, &self.left_delim, &self.right_delim, Mode::default(), &has)?;
        // Go ranges over the map in random order; add the definitions first and the main tree last.
        let mut trees: Vec<(String, Tree)> = trees.into_iter().collect();
        trees.sort_by(|a, b| (a.0 == self.name).cmp(&(b.0 == self.name)).then(a.0.cmp(&b.0)));
        for (name, tree) in trees {
            self.add_parse_tree(&name, tree);
        }
        Ok(self)
    }

    /// Go `AddParseTree`.
    pub fn add_parse_tree(&self, name: &str, tree: Tree) -> Template {
        let mut c = self.common.lock();
        let keep_old = matches!(c.tmpl.get(name), Some(Some(_))) && is_empty_tree(tree.root.as_ref());
        if !keep_old {
            c.tmpl.insert(name.to_string(), Some(Arc::new(tree)));
        }
        drop(c);
        self.new_associated(name)
    }

    /// Go `Clone`: a copy of the set (trees are shared, as in Go).
    pub fn clone_set(&self) -> Template {
        let c = self.common.lock();
        let inner = CommonInner { tmpl: c.tmpl.clone(), funcs: c.funcs.clone(), option: c.option };
        drop(c);
        Template {
            name: self.name.clone(),
            common: Arc::new(Common { inner: Mutex::new(inner) }),
            left_delim: self.left_delim.clone(),
            right_delim: self.right_delim.clone(),
        }
    }

    /// Go `ParseFiles`: each file's base name names its template.
    pub fn parse_files(&self, filenames: &[impl AsRef<str>]) -> Result<&Template, String> {
        if filenames.is_empty() {
            return Err("template: no files named in call to ParseFiles".to_string());
        }
        for f in filenames {
            let f = f.as_ref();
            let (name, s) = read_file(f)?;
            let tmpl = if name == self.name { self.clone() } else { self.new_associated(&name) };
            tmpl.parse(&s)?;
        }
        Ok(self)
    }

    /// Go `Execute`: applies the template to `data`.
    pub fn execute(&self, data: &Value) -> Result<String, String> {
        let mut out = Vec::new();
        exec::execute(self, &mut out, data)?;
        Ok(match String::from_utf8(out) {
            Ok(s) => s,
            Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
        })
    }

    /// Go `Execute` into a byte buffer: on error, the output written before the error stays in `out`.
    pub fn execute_to(&self, out: &mut Vec<u8>, data: &Value) -> Result<(), String> {
        exec::execute(self, out, data)
    }

    /// Go `ExecuteTemplate`.
    pub fn execute_template(&self, name: &str, data: &Value) -> Result<String, String> {
        match self.lookup(name) {
            Some(t) => t.execute(data),
            None => Err(format!("template: no template {} associated with template {}", quote(name), quote(&self.name))),
        }
    }
}

/// Go `readFileOS`: the base name and the content of a file.
pub(crate) fn read_file(file: &str) -> Result<(String, String), String> {
    let name = std::path::Path::new(file).file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    match std::fs::read(file) {
        Ok(b) => Ok((name, String::from_utf8_lossy(&b).into_owned())),
        Err(e) => {
            let msg = match e.kind() {
                std::io::ErrorKind::NotFound => "no such file or directory".to_string(),
                std::io::ErrorKind::PermissionDenied => "permission denied".to_string(),
                _ => e.to_string(),
            };
            Err(format!("open {}: {}", file, msg))
        }
    }
}
