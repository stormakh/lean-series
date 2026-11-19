//! Port of `model/relabel`: relabeling of label sets. YAML decoding of `Config` belongs to the config crate; it must
//! start from `default_relabel_config()` (Go `*c = DefaultRelabelConfig`), which shares the default regexp object
//! so that the pointer comparisons of Go keep their meaning.

use std::fmt;
use std::sync::OnceLock;

use crate::labels::{Builder, ValidationScheme};
use crate::regexp::{self, Error};
use crate::unicode;

/// The action of a relabel rule.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum Action {
    /// The empty string.
    #[default]
    Unset,
    Replace,
    Keep,
    Drop,
    KeepEqual,
    DropEqual,
    HashMod,
    LabelMap,
    LabelDrop,
    LabelKeep,
    Lowercase,
    Uppercase,
    /// Any other string (possible through JSON; relabeling with it panics, as in Go).
    Other(String),
}

impl Action {
    pub fn as_str(&self) -> &str {
        match self {
            Action::Unset => "",
            Action::Replace => "replace",
            Action::Keep => "keep",
            Action::Drop => "drop",
            Action::KeepEqual => "keepequal",
            Action::DropEqual => "dropequal",
            Action::HashMod => "hashmod",
            Action::LabelMap => "labelmap",
            Action::LabelDrop => "labeldrop",
            Action::LabelKeep => "labelkeep",
            Action::Lowercase => "lowercase",
            Action::Uppercase => "uppercase",
            Action::Other(s) => s,
        }
    }

    /// The action for an exact string (JSON decoding: any string is kept).
    pub fn from_str_exact(s: &str) -> Action {
        match s {
            "" => Action::Unset,
            "replace" => Action::Replace,
            "keep" => Action::Keep,
            "drop" => Action::Drop,
            "keepequal" => Action::KeepEqual,
            "dropequal" => Action::DropEqual,
            "hashmod" => Action::HashMod,
            "labelmap" => Action::LabelMap,
            "labeldrop" => Action::LabelDrop,
            "labelkeep" => Action::LabelKeep,
            "lowercase" => Action::Lowercase,
            "uppercase" => Action::Uppercase,
            _ => Action::Other(s.to_string()),
        }
    }

    /// Go `Action.UnmarshalYAML`: case-insensitive, known actions only.
    pub fn parse(s: &str) -> Result<Action, String> {
        match Action::from_str_exact(&unicode::str_to_lower(s)) {
            Action::Unset | Action::Other(_) => Err(format!("unknown relabel action {}", unicode::quote(s))),
            a => Ok(a),
        }
    }
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An anchored regexp (`^(?s:...)$`); `None` is Go's nil regexp. `==` compares the patterns (Go
/// `reflect.DeepEqual`, used to compare configs); `same_object` is Go's `==` on the struct (pointer identity),
/// which relabeling and validation use against the default regexp.
#[derive(Clone, Debug, Default)]
pub struct Regexp(pub Option<regexp::Regexp>);

impl PartialEq for Regexp {
    fn eq(&self, o: &Regexp) -> bool {
        match (&self.0, &o.0) {
            (Some(a), Some(b)) => regexp::Regexp::ptr_eq(a, b) || a.as_str() == b.as_str(),
            (None, None) => true,
            _ => false,
        }
    }
}

impl Regexp {
    /// Go `==` on two Regexp values: the same compiled object (or both nil).
    pub fn same_object(&self, o: &Regexp) -> bool {
        match (&self.0, &o.0) {
            (Some(a), Some(b)) => regexp::Regexp::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        }
    }

    /// Go `NewRegexp`: compiles `^(?s:<s>)$`.
    pub fn new(s: &str) -> Result<Regexp, Error> {
        Ok(Regexp(Some(regexp::Regexp::compile(&format!("^(?s:{})$", s))?)))
    }

    /// Go `MustNewRegexp`.
    pub fn must_new(s: &str) -> Regexp {
        match Regexp::new(s) {
            Ok(r) => r,
            Err(e) => panic!("{}", e),
        }
    }

    fn re(&self) -> &regexp::Regexp {
        self.0.as_ref().expect("nil relabel regexp")
    }

    /// The original string used to compile the regexp, without the anchors ("" for a nil regexp).
    pub fn string(&self) -> &str {
        match &self.0 {
            None => "",
            Some(r) => {
                let s = r.as_str();
                &s[5..s.len() - 2]
            }
        }
    }

    pub fn is_match(&self, s: &str) -> bool {
        self.re().is_match(s)
    }

    /// Go `IsZero`: whether this is the default regexp object.
    pub fn is_zero(&self) -> bool {
        match (&self.0, &default_regex().0) {
            (Some(a), Some(b)) => regexp::Regexp::ptr_eq(a, b),
            (None, _) => false,
            _ => false,
        }
    }

    /// Go `MarshalYAML`: null for a nil regexp, else the original string.
    pub fn to_yaml(&self) -> yaml_rust2::Yaml {
        match &self.0 {
            None => yaml_rust2::Yaml::Null,
            Some(_) => yaml_rust2::Yaml::String(self.string().to_string()),
        }
    }

    /// The YAML text of `to_yaml`.
    pub fn marshal_yaml(&self) -> String {
        if self.0.is_none() {
            // go-yaml writes null as `null` (yaml-rust2 writes `~`).
            return "null\n".to_string();
        }
        let mut out = String::new();
        let _ = yaml_rust2::YamlEmitter::new(&mut out).dump(&self.to_yaml());
        let mut s = out.strip_prefix("---").unwrap_or(&out).trim_start().to_string();
        s.push('\n');
        s
    }

    /// Go `UnmarshalYAML`: null leaves the regexp nil; any scalar is compiled as a string.
    pub fn from_yaml(y: &yaml_rust2::Yaml) -> Result<Regexp, String> {
        use yaml_rust2::Yaml;
        let s = match y {
            Yaml::Null => return Ok(Regexp(None)),
            Yaml::String(s) | Yaml::Real(s) => s.clone(),
            Yaml::Integer(i) => i.to_string(),
            Yaml::Boolean(b) => b.to_string(),
            _ => return Err("yaml: cannot unmarshal into string".to_string()),
        };
        Regexp::new(&s).map_err(|e| e.to_string())
    }

    /// Parses YAML text with `from_yaml`.
    pub fn unmarshal_yaml(text: &str) -> Result<Regexp, String> {
        let docs = yaml_rust2::YamlLoader::load_from_str(text).map_err(|e| e.to_string())?;
        match docs.first() {
            Some(y) => Regexp::from_yaml(y),
            None => Ok(Regexp(None)),
        }
    }

    /// Go `MarshalJSON`.
    pub fn to_json(&self) -> json::Json {
        json::Json::Str(self.string().to_string())
    }

    /// Go `UnmarshalJSON`.
    pub fn from_json(v: &json::Json) -> Result<Regexp, String> {
        match v {
            json::Json::Str(s) => Regexp::new(s).map_err(|e| e.to_string()),
            json::Json::Null => Ok(Regexp(None)),
            _ => Err("json: cannot unmarshal into Go value of type string".to_string()),
        }
    }
}

fn default_regex() -> &'static Regexp {
    static R: OnceLock<Regexp> = OnceLock::new();
    R.get_or_init(|| Regexp::must_new("(.*)"))
}

/// The legacy target label pattern that also allows `$1` and `${name}` variables.
pub fn relabel_target_legacy() -> &'static regexp::Regexp {
    static R: OnceLock<regexp::Regexp> = OnceLock::new();
    R.get_or_init(|| regexp::Regexp::must_compile(r"^(?:(?:[a-zA-Z_]|\$(?:\{\w+\}|\w+))+\w*)+$"))
}

/// The configuration of one relabel rule.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Config {
    /// Labels whose values are concatenated with `separator`; None is Go's nil.
    pub source_labels: Option<Vec<String>>,
    pub separator: String,
    pub regex: Regexp,
    pub modulus: u64,
    pub target_label: String,
    pub replacement: String,
    pub action: Action,
    pub name_validation_scheme: ValidationScheme,
}

/// Go `DefaultRelabelConfig`: action replace, separator ";", regex "(.*)" (a shared object), replacement "$1".
pub fn default_relabel_config() -> Config {
    Config {
        source_labels: None,
        separator: ";".to_string(),
        regex: default_regex().clone(),
        modulus: 0,
        target_label: String::new(),
        replacement: "$1".to_string(),
        action: Action::Replace,
        name_validation_scheme: ValidationScheme::Unset,
    }
}

fn var_in_regex_template(template: &str) -> bool {
    template.contains('$')
}

impl Config {
    /// Validates the rule; an unset name validation scheme becomes `scheme`.
    pub fn validate(&mut self, scheme: ValidationScheme) -> Result<(), String> {
        let a = &self.action;
        if *a == Action::Unset {
            return Err("relabel action cannot be empty".to_string());
        }
        if self.modulus == 0 && *a == Action::HashMod {
            return Err("relabel configuration for hashmod requires non-zero modulus".to_string());
        }
        let needs_target = matches!(
            a,
            Action::Replace | Action::HashMod | Action::Lowercase | Action::Uppercase | Action::KeepEqual | Action::DropEqual
        );
        if needs_target && self.target_label.is_empty() {
            return Err(format!("relabel configuration for {} action requires 'target_label' value", a));
        }
        if self.name_validation_scheme == ValidationScheme::Unset {
            self.name_validation_scheme = scheme;
        }
        let ns = self.name_validation_scheme;
        let a = &self.action;
        let tl = &self.target_label;
        if *a == Action::Replace && !var_in_regex_template(tl) && !ns.is_valid_label_name(tl) {
            return Err(format!("{} is invalid 'target_label' for {} action", unicode::quote(tl), a));
        }
        let is_valid_with_var = |v: &str| -> bool {
            match ns {
                ValidationScheme::Utf8 => ns.is_valid_label_name(v),
                _ => relabel_target_legacy().is_match(v),
            }
        };
        if *a == Action::Replace && var_in_regex_template(tl) && !is_valid_with_var(tl) {
            return Err(format!("{} is invalid 'target_label' for {} action", unicode::quote(tl), a));
        }
        let lower_upper_eq = matches!(a, Action::Lowercase | Action::Uppercase | Action::KeepEqual | Action::DropEqual);
        if lower_upper_eq && !ns.is_valid_label_name(tl) {
            return Err(format!("{} is invalid 'target_label' for {} action", unicode::quote(tl), a));
        }
        let def = default_relabel_config();
        if lower_upper_eq && self.replacement != def.replacement {
            return Err(format!("'replacement' can not be set for {} action", a));
        }
        if *a == Action::LabelMap && !is_valid_with_var(&self.replacement) {
            return Err(format!("{} is invalid 'replacement' for {} action", unicode::quote(&self.replacement), a));
        }
        if *a == Action::HashMod && !ns.is_valid_label_name(tl) {
            return Err(format!("{} is invalid 'target_label' for {} action", unicode::quote(tl), a));
        }
        if (*a == Action::DropEqual || *a == Action::KeepEqual)
            && (!self.regex.same_object(&def.regex)
                || self.modulus != def.modulus
                || self.separator != def.separator
                || self.replacement != def.replacement)
        {
            return Err(format!("{} action requires only 'source_labels' and `target_label`, and no other fields", a));
        }
        if (*a == Action::LabelDrop || *a == Action::LabelKeep)
            && (self.source_labels.is_some()
                || *tl != def.target_label
                || self.modulus != def.modulus
                || self.separator != def.separator
                || self.replacement != def.replacement)
        {
            return Err(format!("{} action requires only 'regex', and no other fields", a));
        }
        Ok(())
    }

    /// Go `json.Marshal(Config)`.
    pub fn to_json(&self) -> json::Json {
        let mut o = json::Json::obj();
        if let Some(sl) = &self.source_labels {
            if !sl.is_empty() {
                o.set("source_labels", json::Json::Arr(sl.iter().map(|s| json::Json::Str(s.clone())).collect()));
            }
        }
        o.set("separator", self.separator.as_str());
        o.set("regex", self.regex.to_json());
        if self.modulus != 0 {
            o.set("modulus", json::Json::Int(self.modulus as i64));
        }
        if !self.target_label.is_empty() {
            o.set("target_label", self.target_label.as_str());
        }
        o.set("replacement", self.replacement.as_str());
        if self.action != Action::Unset {
            o.set("action", self.action.as_str());
        }
        o
    }

    /// Go `json.Unmarshal` into a zero `Config`.
    pub fn from_json(v: &json::Json) -> Result<Config, String> {
        let json::Json::Obj(fields) = v else {
            return Err("json: cannot unmarshal into Go value of type relabel.Config".to_string());
        };
        let mut c = Config::default();
        let str_of = |v: &json::Json| -> Result<String, String> {
            match v {
                json::Json::Str(s) => Ok(s.clone()),
                json::Json::Null => Ok(String::new()),
                _ => Err("json: cannot unmarshal into Go value of type string".to_string()),
            }
        };
        for (k, v) in fields {
            // encoding/json matches field names case-insensitively.
            match k.to_ascii_lowercase().as_str() {
                "source_labels" => match v {
                    json::Json::Null => c.source_labels = None,
                    json::Json::Arr(a) => {
                        let mut out = Vec::with_capacity(a.len());
                        for x in a {
                            let s = str_of(x)?;
                            if !ValidationScheme::Utf8.is_valid_label_name(&s) {
                                return Err(format!("{} is not a valid label name", unicode::quote(&s)));
                            }
                            out.push(s);
                        }
                        c.source_labels = Some(out);
                    }
                    _ => return Err("json: cannot unmarshal into Go value of type model.LabelNames".to_string()),
                },
                "separator" => c.separator = str_of(v)?,
                "regex" => c.regex = Regexp::from_json(v)?,
                "modulus" => {
                    c.modulus = match v {
                        json::Json::Int(i) if *i >= 0 => *i as u64,
                        json::Json::Null => 0,
                        _ => return Err("json: cannot unmarshal into Go value of type uint64".to_string()),
                    }
                }
                "target_label" => c.target_label = str_of(v)?,
                "replacement" => c.replacement = str_of(v)?,
                "action" => c.action = Action::from_str_exact(&str_of(v)?),
                _ => {}
            }
        }
        Ok(c)
    }
}

/// Applies the relabel rules in order to the labels in `lb`. Returns false if a rule says to drop.
pub fn process_builder<'a>(lb: &mut Builder, cfgs: impl IntoIterator<Item = &'a Config>) -> bool {
    for cfg in cfgs {
        if !relabel(cfg, lb) {
            return false;
        }
    }
    true
}

/// Applies one rule. Returns false if the rule says to drop.
pub fn relabel(cfg: &Config, lb: &mut Builder) -> bool {
    let mut val = String::new();
    if let Some(sl) = &cfg.source_labels {
        for (i, ln) in sl.iter().enumerate() {
            if i > 0 {
                val.push_str(&cfg.separator);
            }
            val.push_str(lb.get(ln));
        }
    }
    match &cfg.action {
        Action::Drop => {
            if cfg.regex.is_match(&val) {
                return false;
            }
        }
        Action::Keep => {
            if !cfg.regex.is_match(&val) {
                return false;
            }
        }
        Action::DropEqual => {
            if lb.get(&cfg.target_label) == val {
                return false;
            }
        }
        Action::KeepEqual => {
            if lb.get(&cfg.target_label) != val {
                return false;
            }
        }
        Action::Replace => {
            if val.is_empty()
                && cfg.regex.same_object(default_regex())
                && !var_in_regex_template(&cfg.target_label)
                && !var_in_regex_template(&cfg.replacement)
            {
                lb.set(&cfg.target_label, &cfg.replacement);
                return true;
            }
            let re = cfg.regex.re();
            let Some(indexes) = re.find_string_submatch_index(&val) else { return true };
            let mut target = String::new();
            re.expand_string(&mut target, &cfg.target_label, &val, &indexes);
            if !cfg.name_validation_scheme.is_valid_label_name(&target) {
                return true;
            }
            let mut res = String::new();
            re.expand_string(&mut res, &cfg.replacement, &val, &indexes);
            if res.is_empty() {
                lb.del(&target);
            } else {
                lb.set(&target, &res);
            }
        }
        Action::Lowercase => {
            lb.set(&cfg.target_label, &unicode::str_to_lower(&val));
        }
        Action::Uppercase => {
            lb.set(&cfg.target_label, &unicode::str_to_upper(&val));
        }
        Action::HashMod => {
            let hash = crypto::md5(val.as_bytes());
            let n = u64::from_be_bytes(hash[8..16].try_into().unwrap());
            let m = n % cfg.modulus;
            lb.set(&cfg.target_label, &m.to_string());
        }
        Action::LabelMap => {
            let re = cfg.regex.re();
            lb.range_mut(|lb, name, value| {
                if re.is_match(name) {
                    let res = re.replace_all_string(name, &cfg.replacement);
                    lb.set(&res, value);
                }
            });
        }
        Action::LabelDrop => {
            let re = cfg.regex.re();
            lb.range_mut(|lb, name, _| {
                if re.is_match(name) {
                    lb.del(name);
                }
            });
        }
        Action::LabelKeep => {
            let re = cfg.regex.re();
            lb.range_mut(|lb, name, _| {
                if !re.is_match(name) {
                    lb.del(name);
                }
            });
        }
        a => panic!("relabel: unknown relabel action type {}", unicode::quote(a.as_str())),
    }
    true
}
