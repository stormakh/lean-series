//! Port of Go `html/template/context.go` and `error.go`: the escaping context and escaping errors.

use std::fmt;
use std::sync::Arc;

/// Go `ErrorCode`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    Ok,
    AmbigContext,
    BadHtml,
    BranchEnd,
    EndContext,
    NoSuchTemplate,
    OutputContext,
    PartialCharset,
    PartialEscape,
    RangeLoopReentry,
    SlashAmbig,
    PredefinedEscaper,
    JsTemplate,
}

/// An escaping error (Go `html/template.Error`). `loc` is the `name:line:col` of the node, when there is one.
#[derive(Clone, Debug)]
pub struct Error {
    pub code: ErrorCode,
    pub loc: Option<String>,
    pub name: String,
    pub line: usize,
    pub description: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(loc) = &self.loc {
            write!(f, "html/template:{}: {}", loc, self.description)
        } else if self.line != 0 {
            write!(f, "html/template:{}:{}: {}", self.name, self.line, self.description)
        } else if !self.name.is_empty() {
            write!(f, "html/template:{}: {}", self.name, self.description)
        } else {
            write!(f, "html/template: {}", self.description)
        }
    }
}

/// Go `errorf` (html/template).
pub fn errorf(code: ErrorCode, loc: Option<String>, line: usize, description: String) -> Arc<Error> {
    Arc::new(Error { code, loc, name: String::new(), line, description })
}

macro_rules! named_enum {
    ($name:ident { $($v:ident = $s:literal),* $(,)? }) => {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Hash)]
        pub enum $name { #[default] $($v),* }
        impl $name {
            pub fn as_str(self) -> &'static str {
                match self { $($name::$v => $s),* }
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(self.as_str()) }
        }
    };
}

named_enum!(State {
    Text = "stateText",
    Tag = "stateTag",
    AttrName = "stateAttrName",
    AfterName = "stateAfterName",
    BeforeValue = "stateBeforeValue",
    HtmlCmt = "stateHTMLCmt",
    Rcdata = "stateRCDATA",
    Attr = "stateAttr",
    Url = "stateURL",
    Srcset = "stateSrcset",
    Js = "stateJS",
    JsDqStr = "stateJSDqStr",
    JsSqStr = "stateJSSqStr",
    JsTmplLit = "stateJSTmplLit",
    JsRegexp = "stateJSRegexp",
    JsBlockCmt = "stateJSBlockCmt",
    JsLineCmt = "stateJSLineCmt",
    JsHtmlOpenCmt = "stateJSHTMLOpenCmt",
    JsHtmlCloseCmt = "stateJSHTMLCloseCmt",
    Css = "stateCSS",
    CssDqStr = "stateCSSDqStr",
    CssSqStr = "stateCSSSqStr",
    CssDqUrl = "stateCSSDqURL",
    CssSqUrl = "stateCSSSqURL",
    CssUrl = "stateCSSURL",
    CssBlockCmt = "stateCSSBlockCmt",
    CssLineCmt = "stateCSSLineCmt",
    Error = "stateError",
    MetaContent = "stateMetaContent",
    MetaContentUrl = "stateMetaContentURL",
    Dead = "stateDead",
});

named_enum!(Delim {
    None = "delimNone",
    DoubleQuote = "delimDoubleQuote",
    SingleQuote = "delimSingleQuote",
    SpaceOrTagEnd = "delimSpaceOrTagEnd",
});

named_enum!(UrlPart {
    None = "urlPartNone",
    PreQuery = "urlPartPreQuery",
    QueryOrFrag = "urlPartQueryOrFrag",
    Unknown = "urlPartUnknown",
});

named_enum!(JsCtx {
    Regexp = "jsCtxRegexp",
    DivOp = "jsCtxDivOp",
    Unknown = "jsCtxUnknown",
});

named_enum!(Element {
    None = "elementNone",
    Script = "elementScript",
    Style = "elementStyle",
    Textarea = "elementTextarea",
    Title = "elementTitle",
    Meta = "elementMeta",
});

named_enum!(Attr {
    None = "attrNone",
    Script = "attrScript",
    ScriptType = "attrScriptType",
    Style = "attrStyle",
    Url = "attrURL",
    Srcset = "attrSrcset",
    MetaContent = "attrMetaContent",
});

/// Go `isComment`.
pub fn is_comment(s: State) -> bool {
    matches!(
        s,
        State::HtmlCmt
            | State::JsBlockCmt
            | State::JsLineCmt
            | State::JsHtmlOpenCmt
            | State::JsHtmlCloseCmt
            | State::CssBlockCmt
            | State::CssLineCmt
    )
}

/// Go `isInTag`.
pub fn is_in_tag(s: State) -> bool {
    matches!(s, State::Tag | State::AttrName | State::AfterName | State::BeforeValue | State::Attr)
}

/// Go `isInScriptLiteral`.
pub fn is_in_script_literal(s: State) -> bool {
    matches!(s, State::JsDqStr | State::JsSqStr | State::JsTmplLit | State::JsRegexp)
}

/// The node a context was recorded at (range break/continue): its location and line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CtxNode {
    pub loc: String,
    pub line: usize,
}

/// The escaping context (Go `context`).
#[derive(Clone, Debug, Default)]
pub struct Context {
    pub state: State,
    pub delim: Delim,
    pub url_part: UrlPart,
    pub js_ctx: JsCtx,
    /// `None` is Go's nil slice (it changes the mangled name).
    pub js_brace_depth: Option<Vec<i32>>,
    pub attr: Attr,
    pub element: Element,
    pub n: Option<CtxNode>,
    pub err: Option<Arc<Error>>,
}

impl Context {
    /// A context in `state` with defaults otherwise.
    pub fn with_state(state: State) -> Context {
        Context { state, ..Default::default() }
    }

    /// An error context.
    pub fn error(err: Arc<Error>) -> Context {
        Context { state: State::Error, err: Some(err), ..Default::default() }
    }

    /// Go `context.eq`.
    pub fn eq(&self, d: &Context) -> bool {
        let bd = |c: &Context| c.js_brace_depth.clone().unwrap_or_default();
        self.state == d.state
            && self.delim == d.delim
            && self.url_part == d.url_part
            && self.js_ctx == d.js_ctx
            && bd(self) == bd(d)
            && self.attr == d.attr
            && self.element == d.element
            && match (&self.err, &d.err) {
                (None, None) => true,
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                _ => false,
            }
    }

    /// Go `context.mangle`.
    pub fn mangle(&self, template_name: &str) -> String {
        if self.state == State::Text {
            return template_name.to_string();
        }
        let mut s = format!("{}$htmltemplate_{}", template_name, self.state);
        if self.delim != Delim::None {
            s.push('_');
            s.push_str(self.delim.as_str());
        }
        if self.url_part != UrlPart::None {
            s.push('_');
            s.push_str(self.url_part.as_str());
        }
        if self.js_ctx != JsCtx::Regexp {
            s.push('_');
            s.push_str(self.js_ctx.as_str());
        }
        if let Some(bd) = &self.js_brace_depth {
            s.push_str(&format!("_jsBraceDepth({})", int_slice(bd)));
        }
        if self.attr != Attr::None {
            s.push('_');
            s.push_str(self.attr.as_str());
        }
        if self.element != Element::None {
            s.push('_');
            s.push_str(self.element.as_str());
        }
        s
    }
}

/// Go `%v` of an `[]int`.
fn int_slice(v: &[i32]) -> String {
    let parts: Vec<String> = v.iter().map(|i| i.to_string()).collect();
    format!("[{}]", parts.join(" "))
}

impl fmt::Display for Context {
    /// Go `context.String`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let err = match &self.err {
            Some(e) => e.to_string(),
            None => "<nil>".to_string(),
        };
        write!(
            f,
            "{{{} {} {} {} {} {} {} {}}}",
            self.state,
            self.delim,
            self.url_part,
            self.js_ctx,
            int_slice(self.js_brace_depth.as_deref().unwrap_or(&[])),
            self.attr,
            self.element,
            err
        )
    }
}
