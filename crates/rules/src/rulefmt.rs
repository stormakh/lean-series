//! Port of `model/rulefmt`: rule files (YAML, decoded with go-yaml v3 semantics: strict known fields, merge keys,
//! anchors, line and column positions) and their validation with upstream's error texts.
//!
//! It lives in the `rules` crate (not `model`) because validation parses PromQL and templates, and `promql`
//! depends on `model`.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::rc::Rc;

use model::labels::ValidationScheme;
use promql::parser::Parser;
use yaml::{Duration, Kind, Node};

use crate::log::Logger;

/// A boxed error (Go `error`).
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A plain error message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Msg(pub String);

impl fmt::Display for Msg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for Msg {}

fn msg(s: impl Into<String>) -> BoxError {
    Box::new(Msg(s.into()))
}

/// Go `fmt.Errorf("<prefix>: %w", err)`: the text is `prefix: err` and the source is `err`.
#[derive(Debug)]
pub struct Wrapped {
    pub prefix: String,
    pub err: BoxError,
}

impl fmt::Display for Wrapped {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.prefix, self.err)
    }
}
impl std::error::Error for Wrapped {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.err.as_ref())
    }
}

/// Wraps `err` as Go `fmt.Errorf("<prefix>: %w", err)`.
pub fn wrap(prefix: impl Into<String>, err: BoxError) -> BoxError {
    Box::new(Wrapped { prefix: prefix.into(), err })
}

/// The position of a YAML node (Go `yaml.Node` Line and Column, 1-based; 0:0 for a missing node).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Pos {
    pub line: usize,
    pub column: usize,
}

/// Go `rulefmt.Error`: a semantic error in a rule.
#[derive(Debug)]
pub struct Error {
    pub group: String,
    pub rule: usize,
    pub rule_name: String,
    pub err: WrappedError,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(e) = &self.err.err else { return Ok(()) };
        if let (Some(n), Some(a)) = (self.err.node, self.err.node_alt) {
            return write!(f, "{}:{}: {}:{}: group {}, rule {}, {}: {}", n.line, n.column, a.line, a.column, go_quote(&self.group), self.rule, go_quote(&self.rule_name), e);
        }
        if let Some(n) = self.err.node {
            return write!(f, "{}:{}: group {}, rule {}, {}: {}", n.line, n.column, go_quote(&self.group), self.rule, go_quote(&self.rule_name), e);
        }
        write!(f, "group {}, rule {}, {}: {}", go_quote(&self.group), self.rule, go_quote(&self.rule_name), e)
    }
}

impl std::error::Error for Error {
    /// Go `Unwrap` returns the WrappedError.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.err)
    }
}

/// Go `rulefmt.WrappedError`: an error with the YAML node(s) it is about.
#[derive(Debug, Default)]
pub struct WrappedError {
    pub err: Option<BoxError>,
    pub node: Option<Pos>,
    pub node_alt: Option<Pos>,
}

impl WrappedError {
    pub fn new(err: BoxError, node: Option<Pos>, node_alt: Option<Pos>) -> WrappedError {
        WrappedError { err: Some(err), node, node_alt }
    }
}

impl fmt::Display for WrappedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(e) = &self.err else { return Ok(()) };
        if let (Some(n), Some(a)) = (self.node, self.node_alt) {
            return write!(f, "{}:{}: {}:{}: {}", n.line, n.column, a.line, a.column, e);
        }
        if let Some(n) = self.node {
            return write!(f, "{}:{}: {}", n.line, n.column, e);
        }
        write!(f, "{e}")
    }
}

impl std::error::Error for WrappedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.err.as_ref().map(|e| e.as_ref() as &(dyn std::error::Error + 'static))
    }
}

/// Go `errors.Is`-like walk: whether `pred` holds for `err` or any error in its source chain.
pub fn chain_any(err: &(dyn std::error::Error + 'static), mut pred: impl FnMut(&(dyn std::error::Error + 'static)) -> bool) -> bool {
    let mut cur = Some(err);
    while let Some(e) = cur {
        if pred(e) {
            return true;
        }
        cur = e.source();
    }
    false
}

/// Go `strconv.Quote` (`%q`).
pub fn go_quote(s: &str) -> String {
    model::unicode::quote(s)
}

/// Go `RuleGroups`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RuleGroups {
    pub groups: Vec<RuleGroup>,
}

/// Go `RuleGroup`: a list of sequentially evaluated recording and alerting rules.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RuleGroup {
    pub name: String,
    pub interval: Duration,
    pub query_offset: Option<Duration>,
    pub limit: i64,
    pub rules: Vec<Rule>,
    pub labels: BTreeMap<String, String>,
}

/// Go `Rule`: an alerting or recording rule.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Rule {
    pub record: String,
    pub alert: String,
    pub expr: String,
    pub r#for: Duration,
    pub keep_firing_for: Duration,
    pub labels: BTreeMap<String, String>,
    pub annotations: BTreeMap<String, String>,
}

/// The positions that Go keeps in `ruleGroups` (`RuleGroupNode`, `RuleNode`).
#[derive(Debug, Clone, Default)]
pub struct RuleGroupsNode {
    pub groups: Vec<RuleGroupNode>,
}

#[derive(Debug, Clone, Default)]
pub struct RuleGroupNode {
    pub pos: Pos,
    pub rules: Vec<RuleNode>,
}

#[derive(Debug, Clone, Default)]
pub struct RuleNode {
    pub record: Pos,
    pub alert: Pos,
    pub expr: Pos,
}

/// Go `namevalidationutil.CheckNameValidationScheme`.
pub fn check_name_validation_scheme(s: ValidationScheme) -> Result<(), String> {
    match s {
        ValidationScheme::Utf8 | ValidationScheme::Legacy => Ok(()),
        ValidationScheme::Unset => Err("unset nameValidationScheme".to_string()),
    }
}

impl RuleGroups {
    /// Go `RuleGroups.Validate`.
    pub fn validate(&self, node: &RuleGroupsNode, scheme: ValidationScheme, p: &Parser) -> Vec<BoxError> {
        let mut errs: Vec<BoxError> = Vec::new();
        if let Err(e) = check_name_validation_scheme(scheme) {
            errs.push(msg(e));
            return errs;
        }
        let mut set = std::collections::HashSet::new();
        for (j, g) in self.groups.iter().enumerate() {
            let gpos = node.groups.get(j).map(|n| n.pos).unwrap_or_default();
            if g.name.is_empty() {
                errs.push(msg(format!("{}:{}: Groupname must not be empty", gpos.line, gpos.column)));
            }
            if set.contains(&g.name) {
                errs.push(msg(format!("{}:{}: groupname: \"{}\" is repeated in the same file", gpos.line, gpos.column, g.name)));
            }
            for (k, v) in &g.labels {
                if !scheme.is_valid_label_name(k) || k == "__name__" {
                    errs.push(msg(format!("invalid label name: {k}")));
                }
                let _ = v; // label values are valid UTF-8 (Rust strings)
            }
            set.insert(g.name.clone());
            for (i, r) in g.rules.iter().enumerate() {
                let rn = node.groups.get(j).and_then(|n| n.rules.get(i)).cloned().unwrap_or_default();
                for we in r.validate(&rn, scheme, p) {
                    let rule_name = if !r.alert.is_empty() { r.alert.clone() } else { r.record.clone() };
                    errs.push(Box::new(Error { group: g.name.clone(), rule: i + 1, rule_name, err: we }));
                }
            }
        }
        errs
    }
}

impl Rule {
    /// Go `Rule.Validate`.
    pub fn validate(&self, node: &RuleNode, scheme: ValidationScheme, p: &Parser) -> Vec<WrappedError> {
        let mut nodes = Vec::new();
        if !self.record.is_empty() && !self.alert.is_empty() {
            nodes.push(WrappedError::new(msg("only one of 'record' and 'alert' must be set"), Some(node.record), Some(node.alert)));
        }
        if self.record.is_empty() && self.alert.is_empty() {
            nodes.push(WrappedError::new(msg("one of 'record' or 'alert' must be set"), Some(node.record), Some(node.alert)));
        }
        if self.expr.is_empty() {
            nodes.push(WrappedError::new(msg("field 'expr' must be set in rule"), Some(node.expr), None));
        } else if let Err(e) = p.parse_expr(&self.expr) {
            nodes.push(WrappedError::new(wrap("could not parse expression", Box::new(e)), Some(node.expr), None));
        }
        if !self.record.is_empty() {
            if !self.annotations.is_empty() {
                nodes.push(WrappedError::new(msg("invalid field 'annotations' in recording rule"), Some(node.record), None));
            }
            if self.r#for.0 != 0 {
                nodes.push(WrappedError::new(msg("invalid field 'for' in recording rule"), Some(node.record), None));
            }
            if self.keep_firing_for.0 != 0 {
                nodes.push(WrappedError::new(msg("invalid field 'keep_firing_for' in recording rule"), Some(node.record), None));
            }
            if !scheme.is_valid_metric_name(&self.record) {
                nodes.push(WrappedError::new(msg(format!("invalid recording rule name: {}", self.record)), Some(node.record), None));
            }
            if self.record.contains('{') || self.record.contains('}') {
                nodes.push(WrappedError::new(
                    msg(format!("braces present in the recording rule name; should it be in expr?: {}", self.record)),
                    Some(node.record),
                    None,
                ));
            }
        }
        for k in self.labels.keys() {
            if !scheme.is_valid_label_name(k) || k == "__name__" {
                nodes.push(WrappedError::new(msg(format!("invalid label name: {k}")), None, None));
            }
        }
        for k in self.annotations.keys() {
            if !scheme.is_valid_label_name(k) {
                nodes.push(WrappedError::new(msg(format!("invalid annotation name: {k}")), None, None));
            }
        }
        for err in test_template_parsing(self) {
            nodes.push(WrappedError::new(err, None, None));
        }
        nodes
    }
}

/// Go `testTemplateParsing`: the templates in labels and annotations of alerting rules must parse.
fn test_template_parsing(rl: &Rule) -> Vec<BoxError> {
    let mut errs = Vec::new();
    if rl.alert.is_empty() {
        return errs;
    }
    let defs = "{{$labels := .Labels}}{{$externalLabels := .ExternalLabels}}{{$externalURL := .ExternalURL}}{{$value := .Value}}";
    let parse_test = |text: &str| -> Result<(), String> { crate::tmpl::parse_test(&format!("{defs}{text}"), &format!("__alert_{}", rl.alert)) };
    for (k, val) in &rl.labels {
        if let Err(e) = parse_test(val) {
            errs.push(wrap(format!("label {}", go_quote(k)), msg(e)));
        }
    }
    for (k, val) in &rl.annotations {
        if let Err(e) = parse_test(val) {
            errs.push(wrap(format!("annotation {}", go_quote(k)), msg(e)));
        }
    }
    errs
}

/// Go `rulefmt.Parse`: parses and validates a rule file. On YAML errors the groups are `None`.
pub fn parse(content: &[u8], ignore_unknown_fields: bool, scheme: ValidationScheme, p: &Parser, logger: &Logger) -> (Option<RuleGroups>, Vec<BoxError>) {
    let mut errs: Vec<BoxError> = Vec::new();
    let text = match std::str::from_utf8(content) {
        Ok(t) => t,
        Err(_) => return (None, vec![msg("yaml: invalid leading UTF-8 octet")]),
    };
    let root = match yaml::node::parse(text) {
        Ok(r) => r,
        Err(e) => {
            // Go reports the error of the decoder and the one of yaml.Unmarshal.
            let e = v3_syntax_error(text).unwrap_or(e);
            return (None, vec![msg(e.clone()), msg(e)]);
        }
    };
    let mut groups = RuleGroups::default();
    let mut node = RuleGroupsNode::default();
    if let Some(root) = &root {
        let mut d = V3Decoder { strict: !ignore_unknown_fields, terrors: Vec::new(), node_pass: false };
        if let Err(e) = d.rule_groups(root, &mut groups, &mut RuleGroupsNode::default()) {
            errs.push(msg(e));
        } else if !d.terrors.is_empty() {
            errs.push(msg(format!("yaml: unmarshal errors:\n  {}", d.terrors.join("\n  "))));
        }
    }
    if count_documents(text) > 1 {
        logger.warn("Multiple document yaml rules files are not supported, only the first document is processed", &[]);
    }
    if let Some(root) = &root {
        let mut d = V3Decoder { strict: false, terrors: Vec::new(), node_pass: true };
        let mut scratch = RuleGroups::default();
        if let Err(e) = d.rule_groups(root, &mut scratch, &mut node) {
            errs.push(msg(e));
        } else if !d.terrors.is_empty() {
            errs.push(msg(format!("yaml: unmarshal errors:\n  {}", d.terrors.join("\n  "))));
        }
    }
    if !errs.is_empty() {
        return (None, errs);
    }
    let verrs = groups.validate(&node, scheme, p);
    (Some(groups), verrs)
}

/// Go `rulefmt.ParseFile`.
pub fn parse_file(file: &str, ignore_unknown_fields: bool, scheme: ValidationScheme, p: &Parser, logger: &Logger) -> (Option<RuleGroups>, Vec<BoxError>) {
    let b = match std::fs::read(file) {
        Ok(b) => b,
        Err(e) => return (None, vec![wrap(file, msg(go_io_error(file, &e)))]),
    };
    let (rgs, errs) = parse(&b, ignore_unknown_fields, scheme, p, logger);
    (rgs, errs.into_iter().map(|e| wrap(file, e)).collect())
}

/// Go's `os.ReadFile` error text (`open <file>: no such file or directory`).
pub fn go_io_error(file: &str, e: &std::io::Error) -> String {
    let m = match e.kind() {
        std::io::ErrorKind::NotFound => "no such file or directory".to_string(),
        std::io::ErrorKind::PermissionDenied => "permission denied".to_string(),
        std::io::ErrorKind::IsADirectory => "is a directory".to_string(),
        _ => {
            let s = e.to_string();
            match s.find(" (os error") {
                Some(i) => s[..i].to_lowercase(),
                None => s,
            }
        }
    };
    if e.kind() == std::io::ErrorKind::IsADirectory {
        return format!("read {file}: {m}");
    }
    format!("open {file}: {m}")
}

fn count_documents(text: &str) -> usize {
    use yaml_rust2::parser::{Event, MarkedEventReceiver, Parser as YParser};
    use yaml_rust2::scanner::Marker;
    struct C(usize);
    impl MarkedEventReceiver for C {
        fn on_event(&mut self, ev: Event, _m: Marker) {
            if let Event::DocumentStart = ev {
                self.0 += 1;
            }
        }
    }
    let mut c = C(0);
    let mut p = YParser::new_from_str(text);
    let _ = p.load(&mut c, true);
    c.0
}

/// The parts of go-yaml v3 decoding that rule files need.
struct V3Decoder {
    strict: bool,
    terrors: Vec<String>,
    /// Decoding into Go's `ruleGroups` (the node types, whose record/alert/expr fields are `yaml.Node`).
    node_pass: bool,
}

/// go-yaml v3 reports parser errors at the start of the enclosing collection (the libyaml context mark, 0-based)
/// where v2 reports the problem line. `None` when the error has no context.
fn v3_syntax_error(text: &str) -> Option<String> {
    use yaml_rust2::parser::{Event, MarkedEventReceiver, Parser as YParser};
    use yaml_rust2::scanner::Marker;
    struct S(Vec<usize>);
    impl MarkedEventReceiver for S {
        fn on_event(&mut self, ev: Event, m: Marker) {
            match ev {
                Event::SequenceStart(..) | Event::MappingStart(..) => self.0.push(m.line()),
                Event::SequenceEnd | Event::MappingEnd => {
                    self.0.pop();
                }
                _ => {}
            }
        }
    }
    let mut st = S(Vec::new());
    let mut p = YParser::new_from_str(text);
    let err = p.load(&mut st, true).err()?;
    let info = err.info().to_string();
    let rest = info.strip_prefix("while parsing ")?;
    let problem = &rest[rest.find(", ")? + 2..];
    let ctx_line = st.0.last().copied()?.saturating_sub(1);
    if ctx_line == 0 {
        return None;
    }
    Some(format!("yaml: line {ctx_line}: {problem}"))
}

fn resolve(n: &Rc<Node>) -> &Rc<Node> {
    let mut n = n;
    while n.kind == Kind::Alias {
        match &n.alias {
            Some(a) => n = a,
            None => break,
        }
    }
    n
}

fn pos(n: &Node) -> Pos {
    Pos { line: n.line, column: n.column + 1 }
}

fn short_tag(n: &Node) -> String {
    match n.kind {
        Kind::Mapping => "!!map".to_string(),
        Kind::Sequence => "!!seq".to_string(),
        _ => {
            let t = if n.tag.is_empty() {
                if n.implicit { yaml::resolve::resolve("", &n.value).map(|r| r.0).unwrap_or_default() } else { "tag:yaml.org,2002:str".to_string() }
            } else {
                n.tag.clone()
            };
            match t.strip_prefix("tag:yaml.org,2002:") {
                Some(s) => format!("!!{s}"),
                None => t,
            }
        }
    }
}

impl V3Decoder {
    fn terror(&mut self, n: &Node, go_type: &str) {
        let mut value = String::new();
        if n.kind == Kind::Scalar {
            value = format!(" `{}`", n.value);
        }
        self.terrors.push(format!("line {}: cannot unmarshal {}{} into {}", n.line, short_tag(n), value, go_type));
    }

    /// The key/value pairs of a mapping with merge keys (`<<`) applied, as v3 `d.mapping` with `d.merge`:
    /// explicit keys win over merged ones. Duplicate explicit keys are an error.
    fn pairs(&mut self, n: &Rc<Node>) -> Result<Vec<(Rc<Node>, Rc<Node>)>, String> {
        let mut out: Vec<(Rc<Node>, Rc<Node>)> = Vec::new();
        let mut merged: Vec<(Rc<Node>, Rc<Node>)> = Vec::new();
        let mut seen: HashMap<String, usize> = HashMap::new();
        for (k, v) in n.pairs() {
            let kr = resolve(k);
            if kr.is_merge() {
                let vr = resolve(v).clone();
                match vr.kind {
                    Kind::Mapping => merged.extend(self.pairs(&vr)?),
                    Kind::Sequence => {
                        for item in vr.children.iter() {
                            let ir = resolve(item).clone();
                            if ir.kind != Kind::Mapping {
                                return Err("yaml: map merge requires map or sequence of maps as the value".to_string());
                            }
                            merged.extend(self.pairs(&ir)?);
                        }
                    }
                    _ => return Err("yaml: map merge requires map or sequence of maps as the value".to_string()),
                }
                continue;
            }
            if kr.kind == Kind::Scalar {
                if let Some(line) = seen.get(&kr.value) {
                    return Err(format!("yaml: unmarshal errors:\n  line {}: mapping key {} already defined at line {}", kr.line, go_quote(&kr.value), line));
                }
                seen.insert(kr.value.clone(), kr.line);
            }
            out.push((k.clone(), v.clone()));
        }
        for (k, v) in merged {
            let kr = resolve(&k);
            if kr.kind == Kind::Scalar && seen.contains_key(&kr.value) {
                continue;
            }
            if kr.kind == Kind::Scalar {
                seen.insert(kr.value.clone(), kr.line);
            }
            out.push((k, v));
        }
        Ok(out)
    }

    fn string(&mut self, n: &Rc<Node>, out: &mut String) {
        let n = resolve(n);
        match n.kind {
            Kind::Scalar => {
                if n.is_null() {
                    return;
                }
                *out = n.value.clone();
            }
            _ => self.terror(n, "string"),
        }
    }

    fn duration(&mut self, n: &Rc<Node>, out: &mut Duration) -> Result<(), String> {
        let n = resolve(n);
        if n.kind == Kind::Scalar && n.is_null() {
            return Ok(());
        }
        let mut s = String::new();
        let before = self.terrors.len();
        self.string(n, &mut s);
        if self.terrors.len() != before {
            // v3 passes the inner type error up through the obsolete unmarshaler
            return Ok(());
        }
        *out = yaml::parse_duration(&s)?;
        Ok(())
    }

    fn int(&mut self, n: &Rc<Node>, out: &mut i64) {
        let n = resolve(n);
        if n.kind == Kind::Scalar {
            if n.is_null() {
                return;
            }
            if n.implicit || n.tag == "tag:yaml.org,2002:int" {
                if let Some(v) = parse_yaml_int(&n.value) {
                    *out = v;
                    return;
                }
                if let Ok(f) = n.value.parse::<f64>()
                    && f == f.trunc()
                    && f.abs() < 9.2e18
                    && yaml::resolve::resolve("", &n.value).map(|r| r.0).unwrap_or_default() == "tag:yaml.org,2002:float"
                {
                    *out = f as i64;
                    return;
                }
            }
        }
        self.terror(n, "int");
    }

    fn string_map(&mut self, n: &Rc<Node>, out: &mut BTreeMap<String, String>) -> Result<(), String> {
        let n = resolve(n).clone();
        match n.kind {
            Kind::Scalar if n.is_null() => {
                out.clear();
                Ok(())
            }
            Kind::Mapping => {
                for (k, v) in self.pairs(&n)? {
                    let mut ks = String::new();
                    self.string(&k, &mut ks);
                    let mut vs = String::new();
                    self.string(&v, &mut vs);
                    out.insert(ks, vs);
                }
                Ok(())
            }
            _ => {
                self.terror(&n, "map[string]string");
                Ok(())
            }
        }
    }

    fn unknown(&mut self, k: &Node, go_type: &str) {
        if self.strict {
            self.terrors.push(format!("line {}: field {} not found in type {}", k.line, k.value, go_type));
        }
    }

    fn rule_groups(&mut self, root: &Rc<Node>, out: &mut RuleGroups, node: &mut RuleGroupsNode) -> Result<(), String> {
        let root = resolve(root).clone();
        if root.kind == Kind::Scalar && root.is_null() {
            return Ok(());
        }
        if root.kind != Kind::Mapping {
            self.terror(&root, if self.node_pass { "rulefmt.ruleGroups" } else { "rulefmt.RuleGroups" });
            return Ok(());
        }
        for (k, v) in self.pairs(&root)? {
            let kr = resolve(&k);
            match kr.value.as_str() {
                "groups" => {
                    let vr = resolve(&v).clone();
                    match vr.kind {
                        Kind::Scalar if vr.is_null() => {}
                        Kind::Sequence => {
                            out.groups.clear();
                            node.groups.clear();
                            for item in vr.children.iter() {
                                let mut g = RuleGroup::default();
                                let mut gn = RuleGroupNode::default();
                                self.rule_group(item, &mut g, &mut gn)?;
                                out.groups.push(g);
                                node.groups.push(gn);
                            }
                        }
                        _ => self.terror(&vr, if self.node_pass { "[]rulefmt.RuleGroupNode" } else { "[]rulefmt.RuleGroup" }),
                    }
                }
                _ => self.unknown(kr, "rulefmt.RuleGroups"),
            }
        }
        Ok(())
    }

    fn rule_group(&mut self, n: &Rc<Node>, out: &mut RuleGroup, node: &mut RuleGroupNode) -> Result<(), String> {
        let n = resolve(n).clone();
        // Go embeds yaml.Node in RuleGroupNode without `,inline`, so the decoder never fills it: positions of
        // groups are always 0:0 upstream.
        node.pos = Pos::default();
        if n.kind == Kind::Scalar && n.is_null() {
            return Ok(());
        }
        if n.kind != Kind::Mapping {
            self.terror(&n, if self.node_pass { "rulefmt.RuleGroupNode" } else { "rulefmt.RuleGroup" });
            return Ok(());
        }
        for (k, v) in self.pairs(&n)? {
            let kr = resolve(&k);
            match kr.value.as_str() {
                "name" => self.string(&v, &mut out.name),
                "interval" => self.duration(&v, &mut out.interval)?,
                "query_offset" => {
                    let vr = resolve(&v);
                    if vr.kind == Kind::Scalar && vr.is_null() {
                        out.query_offset = None;
                    } else {
                        let mut d = Duration(0);
                        self.duration(&v, &mut d)?;
                        out.query_offset = Some(d);
                    }
                }
                "limit" => self.int(&v, &mut out.limit),
                "rules" => {
                    let vr = resolve(&v).clone();
                    match vr.kind {
                        Kind::Scalar if vr.is_null() => {}
                        Kind::Sequence => {
                            out.rules.clear();
                            node.rules.clear();
                            for item in vr.children.iter() {
                                let mut r = Rule::default();
                                let mut rn = RuleNode::default();
                                self.rule(item, &mut r, &mut rn)?;
                                out.rules.push(r);
                                node.rules.push(rn);
                            }
                        }
                        _ => self.terror(&vr, if self.node_pass { "[]rulefmt.RuleNode" } else { "[]rulefmt.Rule" }),
                    }
                }
                "labels" => self.string_map(&v, &mut out.labels)?,
                _ => self.unknown(kr, "rulefmt.RuleGroup"),
            }
        }
        Ok(())
    }

    fn rule(&mut self, n: &Rc<Node>, out: &mut Rule, node: &mut RuleNode) -> Result<(), String> {
        let n = resolve(n).clone();
        if n.kind == Kind::Scalar && n.is_null() {
            return Ok(());
        }
        if n.kind != Kind::Mapping {
            self.terror(&n, if self.node_pass { "rulefmt.RuleNode" } else { "rulefmt.Rule" });
            return Ok(());
        }
        for (k, v) in self.pairs(&n)? {
            let kr = resolve(&k);
            match kr.value.as_str() {
                "record" => {
                    node.record = pos(&v);
                    if !self.node_pass {
                        self.string(&v, &mut out.record);
                    }
                }
                "alert" => {
                    node.alert = pos(&v);
                    if !self.node_pass {
                        self.string(&v, &mut out.alert);
                    }
                }
                "expr" => {
                    node.expr = pos(&v);
                    if !self.node_pass {
                        self.string(&v, &mut out.expr);
                    }
                }
                "for" => self.duration(&v, &mut out.r#for)?,
                "keep_firing_for" => self.duration(&v, &mut out.keep_firing_for)?,
                "labels" => self.string_map(&v, &mut out.labels)?,
                "annotations" => self.string_map(&v, &mut out.annotations)?,
                _ => self.unknown(kr, "rulefmt.Rule"),
            }
        }
        Ok(())
    }
}

fn parse_yaml_int(s: &str) -> Option<i64> {
    let t = s.replace('_', "");
    let (neg, body) = match t.strip_prefix('-') {
        Some(b) => (true, b.to_string()),
        None => (false, t.strip_prefix('+').unwrap_or(&t).to_string()),
    };
    let v = if let Some(h) = body.strip_prefix("0x") {
        i64::from_str_radix(h, 16).ok()?
    } else if let Some(o) = body.strip_prefix("0o") {
        i64::from_str_radix(o, 8).ok()?
    } else if let Some(b) = body.strip_prefix("0b") {
        i64::from_str_radix(b, 2).ok()?
    } else {
        body.parse::<i64>().ok()?
    };
    Some(if neg { -v } else { v })
}

// ---------------- YAML marshaling of a rule ----------------

impl Rule {
    /// Go `yaml.Marshal(rule)` with go-yaml v2 (as `rules.RecordingRule.String` and `AlertingRule.String` do).
    pub fn to_yaml(&self) -> String {
        use yaml::Value as V;
        let mut m: Vec<(V, V)> = Vec::new();
        let s = |x: &str| V::Str(x.to_string());
        if !self.record.is_empty() {
            m.push((s("record"), s(&self.record)));
        }
        if !self.alert.is_empty() {
            m.push((s("alert"), s(&self.alert)));
        }
        m.push((s("expr"), s(&self.expr)));
        if self.r#for.0 != 0 {
            m.push((s("for"), s(&self.r#for.to_string())));
        }
        if self.keep_firing_for.0 != 0 {
            m.push((s("keep_firing_for"), s(&self.keep_firing_for.to_string())));
        }
        for (name, map) in [("labels", &self.labels), ("annotations", &self.annotations)] {
            if map.is_empty() {
                continue;
            }
            let mut pairs: Vec<(V, V)> = map.iter().map(|(k, v)| (s(k), s(v))).collect();
            yaml::value::sort_pairs(&mut pairs);
            m.push((s(name), V::Map(pairs)));
        }
        yaml::emit(&V::Map(m))
    }
}
