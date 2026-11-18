//! The libyaml emitter as go-yaml v2 runs it (emitterc.go), for block and flow collections and plain, single,
//! double-quoted and literal scalars: 2-space indent, lines folded after column 80, sequences inside mappings
//! not indented. Anchors, tags and directives are not needed (go-yaml's Marshal never emits them for strings).

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Style {
    Plain,
    SingleQuoted,
    DoubleQuoted,
    Literal,
}

/// The event tree: a scalar with its requested style, or a collection.
#[derive(Debug, Clone)]
pub enum Ev {
    Scalar(String, Style),
    Seq(Vec<Ev>),
    Map(Vec<(Ev, Ev)>),
}

struct ScalarData {
    multiline: bool,
    flow_plain_allowed: bool,
    block_plain_allowed: bool,
    single_quoted_allowed: bool,
    block_allowed: bool,
}

pub struct Emitter {
    out: Vec<u8>,
    best_indent: i64,
    best_width: i64,
    indent: i64,
    indents: Vec<i64>,
    flow_level: i64,
    column: i64,
    whitespace: bool,
    indention: bool,
    open_ended: bool,
    root_context: bool,
    sequence_context: bool,
    mapping_context: bool,
    simple_key_context: bool,
}

fn at(b: &[u8], i: usize) -> u8 {
    if i < b.len() { b[i] } else { 0 }
}
fn width(c: u8) -> usize {
    if c & 0x80 == 0x00 {
        1
    } else if c & 0xE0 == 0xC0 {
        2
    } else if c & 0xF0 == 0xE0 {
        3
    } else if c & 0xF8 == 0xF0 {
        4
    } else {
        0
    }
}
fn is_space(b: &[u8], i: usize) -> bool {
    at(b, i) == b' '
}
fn is_blank(b: &[u8], i: usize) -> bool {
    let c = at(b, i);
    c == b' ' || c == b'\t'
}
fn is_break(b: &[u8], i: usize) -> bool {
    let c = at(b, i);
    c == b'\r' || c == b'\n' || (c == 0xC2 && at(b, i + 1) == 0x85) || (c == 0xE2 && at(b, i + 1) == 0x80 && at(b, i + 2) == 0xA8) || (c == 0xE2 && at(b, i + 1) == 0x80 && at(b, i + 2) == 0xA9)
}
fn is_breakz(b: &[u8], i: usize) -> bool {
    is_break(b, i) || i >= b.len() || b[i] == 0
}
fn is_blankz(b: &[u8], i: usize) -> bool {
    is_blank(b, i) || is_breakz(b, i)
}
fn is_bom(b: &[u8], _i: usize) -> bool {
    // go-yaml checks the first three bytes whatever i is
    at(b, 0) == 0xEF && at(b, 1) == 0xBB && at(b, 2) == 0xBF
}
fn is_printable(b: &[u8], i: usize) -> bool {
    let c = at(b, i);
    let c1 = at(b, i + 1);
    let c2 = at(b, i + 2);
    c == 0x0A
        || (0x20..=0x7E).contains(&c)
        || (c == 0xC2 && c1 >= 0xA0)
        || (c > 0xC2 && c < 0xED)
        || (c == 0xED && c1 < 0xA0)
        || c == 0xEE
        || (c == 0xEF && !(c1 == 0xBB && c2 == 0xBF) && !(c1 == 0xBF && (c2 == 0xBE || c2 == 0xBF)))
}

fn analyze_scalar(value: &[u8]) -> ScalarData {
    if value.is_empty() {
        return ScalarData { multiline: false, flow_plain_allowed: false, block_plain_allowed: true, single_quoted_allowed: true, block_allowed: false };
    }
    let mut block_indicators = false;
    let mut flow_indicators = false;
    let mut line_breaks = false;
    let mut special_characters = false;
    let mut leading_space = false;
    let mut leading_break = false;
    let mut trailing_space = false;
    let mut trailing_break = false;
    let mut break_space = false;
    let mut space_break = false;
    let mut previous_space = false;
    let mut previous_break = false;

    if value.len() >= 3 && (value.starts_with(b"---") || value.starts_with(b"...")) {
        block_indicators = true;
        flow_indicators = true;
    }
    let mut preceded_by_whitespace = true;
    let mut i = 0;
    while i < value.len() {
        let w = width(value[i]).max(1);
        let followed_by_whitespace = i + w >= value.len() || is_blank(value, i + w);
        if i == 0 {
            match value[i] {
                b'#' | b',' | b'[' | b']' | b'{' | b'}' | b'&' | b'*' | b'!' | b'|' | b'>' | b'\'' | b'"' | b'%' | b'@' | b'`' => {
                    flow_indicators = true;
                    block_indicators = true;
                }
                b'?' | b':' => {
                    flow_indicators = true;
                    if followed_by_whitespace {
                        block_indicators = true;
                    }
                }
                b'-' => {
                    if followed_by_whitespace {
                        flow_indicators = true;
                        block_indicators = true;
                    }
                }
                _ => {}
            }
        } else {
            match value[i] {
                b',' | b'?' | b'[' | b']' | b'{' | b'}' => flow_indicators = true,
                b':' => {
                    flow_indicators = true;
                    if followed_by_whitespace {
                        block_indicators = true;
                    }
                }
                b'#' => {
                    if preceded_by_whitespace {
                        flow_indicators = true;
                        block_indicators = true;
                    }
                }
                _ => {}
            }
        }
        if !is_printable(value, i) {
            special_characters = true;
        }
        if is_space(value, i) {
            if i == 0 {
                leading_space = true;
            }
            if i + w == value.len() {
                trailing_space = true;
            }
            if previous_break {
                break_space = true;
            }
            previous_space = true;
            previous_break = false;
        } else if is_break(value, i) {
            line_breaks = true;
            if i == 0 {
                leading_break = true;
            }
            if i + w == value.len() {
                trailing_break = true;
            }
            if previous_space {
                space_break = true;
            }
            previous_space = false;
            previous_break = true;
        } else {
            previous_space = false;
            previous_break = false;
        }
        preceded_by_whitespace = is_blankz(value, i);
        i += w;
    }
    let mut d = ScalarData { multiline: line_breaks, flow_plain_allowed: true, block_plain_allowed: true, single_quoted_allowed: true, block_allowed: true };
    if leading_space || leading_break || trailing_space || trailing_break {
        d.flow_plain_allowed = false;
        d.block_plain_allowed = false;
    }
    if trailing_space {
        d.block_allowed = false;
    }
    if break_space {
        d.flow_plain_allowed = false;
        d.block_plain_allowed = false;
        d.single_quoted_allowed = false;
    }
    if space_break || special_characters {
        d.flow_plain_allowed = false;
        d.block_plain_allowed = false;
        d.single_quoted_allowed = false;
        d.block_allowed = false;
    }
    if line_breaks {
        d.flow_plain_allowed = false;
        d.block_plain_allowed = false;
    }
    if flow_indicators {
        d.flow_plain_allowed = false;
    }
    if block_indicators {
        d.block_plain_allowed = false;
    }
    d
}

impl Default for Emitter {
    fn default() -> Self {
        Self::new()
    }
}

impl Emitter {
    pub fn new() -> Emitter {
        Emitter {
            out: Vec::new(),
            best_indent: 2,
            best_width: 80,
            indent: -1,
            indents: Vec::new(),
            flow_level: 0,
            column: 0,
            whitespace: true,
            indention: true,
            open_ended: false,
            root_context: false,
            sequence_context: false,
            mapping_context: false,
            simple_key_context: false,
        }
    }

    /// Emits one implicit document (go-yaml Marshal) and returns the text.
    pub fn document(mut self, root: &Ev) -> String {
        self.emit_node(root, true, false, false, false);
        // DOCUMENT-END (implicit)
        self.write_indent();
        String::from_utf8(self.out).unwrap_or_default()
    }

    fn put(&mut self, c: u8) {
        self.out.push(c);
        self.column += 1;
    }
    fn put_break(&mut self) {
        self.out.push(b'\n');
        self.column = 0;
    }
    fn write(&mut self, s: &[u8], i: &mut usize) {
        let w = width(s[*i]).max(1);
        self.out.extend_from_slice(&s[*i..(*i + w).min(s.len())]);
        self.column += 1;
        *i += w;
    }
    fn write_all(&mut self, s: &[u8]) {
        let mut i = 0;
        while i < s.len() {
            self.write(s, &mut i);
        }
    }
    fn write_break(&mut self, s: &[u8], i: &mut usize) {
        if s[*i] == b'\n' {
            self.put_break();
            *i += 1;
        } else {
            self.write(s, i);
            self.column = 0;
        }
    }

    fn increase_indent(&mut self, flow: bool, indentless: bool) {
        self.indents.push(self.indent);
        if self.indent < 0 {
            self.indent = if flow { self.best_indent } else { 0 };
        } else if !indentless {
            self.indent += self.best_indent;
        }
    }
    fn pop_indent(&mut self) {
        self.indent = self.indents.pop().unwrap_or(-1);
    }

    fn write_indent(&mut self) {
        let indent = self.indent.max(0);
        if !self.indention || self.column > indent || (self.column == indent && !self.whitespace) {
            self.put_break();
        }
        while self.column < indent {
            self.put(b' ');
        }
        self.whitespace = true;
        self.indention = true;
    }

    fn write_indicator(&mut self, ind: &[u8], need_whitespace: bool, is_whitespace: bool, is_indention: bool) {
        if need_whitespace && !self.whitespace {
            self.put(b' ');
        }
        self.write_all(ind);
        self.whitespace = is_whitespace;
        self.indention = self.indention && is_indention;
        self.open_ended = false;
    }

    fn check_simple_key(&self, n: &Ev) -> bool {
        match n {
            Ev::Scalar(v, _) => {
                let d = analyze_scalar(v.as_bytes());
                !d.multiline && v.len() <= 128
            }
            Ev::Seq(items) => items.is_empty(),
            Ev::Map(pairs) => pairs.is_empty(),
        }
    }

    fn emit_node(&mut self, n: &Ev, root: bool, sequence: bool, mapping: bool, simple_key: bool) {
        self.root_context = root;
        self.sequence_context = sequence;
        self.mapping_context = mapping;
        self.simple_key_context = simple_key;
        match n {
            Ev::Scalar(v, style) => self.emit_scalar(v, *style),
            Ev::Seq(items) => {
                if self.flow_level > 0 || items.is_empty() {
                    self.flow_sequence(items);
                } else {
                    self.block_sequence(items);
                }
            }
            Ev::Map(pairs) => {
                if self.flow_level > 0 || pairs.is_empty() {
                    self.flow_mapping(pairs);
                } else {
                    self.block_mapping(pairs);
                }
            }
        }
    }

    fn flow_sequence(&mut self, items: &[Ev]) {
        self.write_indicator(b"[", true, true, false);
        self.increase_indent(true, false);
        self.flow_level += 1;
        for (i, it) in items.iter().enumerate() {
            if i > 0 {
                self.write_indicator(b",", false, false, false);
            }
            if self.column > self.best_width {
                self.write_indent();
            }
            self.emit_node(it, false, true, false, false);
        }
        self.flow_level -= 1;
        self.pop_indent();
        self.write_indicator(b"]", false, false, false);
    }

    fn flow_mapping(&mut self, pairs: &[(Ev, Ev)]) {
        self.write_indicator(b"{", true, true, false);
        self.increase_indent(true, false);
        self.flow_level += 1;
        for (i, (k, v)) in pairs.iter().enumerate() {
            if i > 0 {
                self.write_indicator(b",", false, false, false);
            }
            if self.column > self.best_width {
                self.write_indent();
            }
            if self.check_simple_key(k) {
                self.emit_node(k, false, false, true, true);
                self.write_indicator(b":", false, false, false);
            } else {
                self.write_indicator(b"?", true, false, false);
                self.emit_node(k, false, false, true, false);
                if self.column > self.best_width {
                    self.write_indent();
                }
                self.write_indicator(b":", true, false, false);
            }
            self.emit_node(v, false, false, true, false);
        }
        self.flow_level -= 1;
        self.pop_indent();
        self.write_indicator(b"}", false, false, false);
    }

    fn block_sequence(&mut self, items: &[Ev]) {
        let indentless = self.mapping_context && !self.indention;
        self.increase_indent(false, indentless);
        for it in items {
            self.write_indent();
            self.write_indicator(b"-", true, false, true);
            self.emit_node(it, false, true, false, false);
        }
        self.pop_indent();
    }

    fn block_mapping(&mut self, pairs: &[(Ev, Ev)]) {
        self.increase_indent(false, false);
        for (k, v) in pairs {
            self.write_indent();
            if self.check_simple_key(k) {
                self.emit_node(k, false, false, true, true);
                self.write_indicator(b":", false, false, false);
            } else {
                self.write_indicator(b"?", true, false, true);
                self.emit_node(k, false, false, true, false);
                self.write_indent();
                self.write_indicator(b":", true, false, true);
            }
            self.emit_node(v, false, false, true, false);
        }
        self.pop_indent();
    }

    fn emit_scalar(&mut self, value: &str, requested: Style) {
        let v = value.as_bytes();
        let d = analyze_scalar(v);
        let mut style = requested;
        if self.simple_key_context && d.multiline {
            style = Style::DoubleQuoted;
        }
        if style == Style::Plain {
            if self.flow_level > 0 && !d.flow_plain_allowed || self.flow_level == 0 && !d.block_plain_allowed {
                style = Style::SingleQuoted;
            }
            if v.is_empty() && (self.flow_level > 0 || self.simple_key_context) {
                style = Style::SingleQuoted;
            }
        }
        if style == Style::SingleQuoted && !d.single_quoted_allowed {
            style = Style::DoubleQuoted;
        }
        if style == Style::Literal && (!d.block_allowed || self.flow_level > 0 || self.simple_key_context) {
            style = Style::DoubleQuoted;
        }
        self.increase_indent(true, false);
        let allow_breaks = !self.simple_key_context;
        match style {
            Style::Plain => self.write_plain(v, allow_breaks),
            Style::SingleQuoted => self.write_single(v, allow_breaks),
            Style::DoubleQuoted => self.write_double(v, allow_breaks),
            Style::Literal => self.write_literal(v),
        }
        self.pop_indent();
    }

    fn write_plain(&mut self, value: &[u8], allow_breaks: bool) {
        if !self.whitespace {
            self.put(b' ');
        }
        let mut spaces = false;
        let mut breaks = false;
        let mut i = 0;
        while i < value.len() {
            if is_space(value, i) {
                if allow_breaks && !spaces && self.column > self.best_width && !is_space(value, i + 1) {
                    self.write_indent();
                    i += width(value[i]).max(1);
                } else {
                    self.write(value, &mut i);
                }
                spaces = true;
            } else if is_break(value, i) {
                if !breaks && value[i] == b'\n' {
                    self.put_break();
                }
                self.write_break(value, &mut i);
                self.indention = true;
                breaks = true;
            } else {
                if breaks {
                    self.write_indent();
                }
                self.write(value, &mut i);
                self.indention = false;
                spaces = false;
                breaks = false;
            }
        }
        self.whitespace = false;
        self.indention = false;
        if self.root_context {
            self.open_ended = true;
        }
    }

    fn write_single(&mut self, value: &[u8], allow_breaks: bool) {
        self.write_indicator(b"'", true, false, false);
        let mut spaces = false;
        let mut breaks = false;
        let mut i = 0;
        while i < value.len() {
            if is_space(value, i) {
                if allow_breaks && !spaces && self.column > self.best_width && i > 0 && i < value.len() - 1 && !is_space(value, i + 1) {
                    self.write_indent();
                    i += width(value[i]).max(1);
                } else {
                    self.write(value, &mut i);
                }
                spaces = true;
            } else if is_break(value, i) {
                if !breaks && value[i] == b'\n' {
                    self.put_break();
                }
                self.write_break(value, &mut i);
                self.indention = true;
                breaks = true;
            } else {
                if breaks {
                    self.write_indent();
                }
                if value[i] == b'\'' {
                    self.put(b'\'');
                }
                self.write(value, &mut i);
                self.indention = false;
                spaces = false;
                breaks = false;
            }
        }
        self.write_indicator(b"'", false, false, false);
        self.whitespace = false;
        self.indention = false;
    }

    fn write_double(&mut self, value: &[u8], allow_breaks: bool) {
        let mut spaces = false;
        self.write_indicator(b"\"", true, false, false);
        let mut i = 0;
        while i < value.len() {
            if !is_printable(value, i) || is_bom(value, i) || is_break(value, i) || value[i] == b'"' || value[i] == b'\\' {
                let octet = value[i];
                let (w, mut v): (usize, u32) = if octet & 0x80 == 0 {
                    (1, (octet & 0x7F) as u32)
                } else if octet & 0xE0 == 0xC0 {
                    (2, (octet & 0x1F) as u32)
                } else if octet & 0xF0 == 0xE0 {
                    (3, (octet & 0x0F) as u32)
                } else {
                    (4, (octet & 0x07) as u32)
                };
                for k in 1..w {
                    v = (v << 6) + (at(value, i + k) as u32 & 0x3F);
                }
                i += w;
                self.put(b'\\');
                match v {
                    0x00 => self.put(b'0'),
                    0x07 => self.put(b'a'),
                    0x08 => self.put(b'b'),
                    0x09 => self.put(b't'),
                    0x0A => self.put(b'n'),
                    0x0b => self.put(b'v'),
                    0x0c => self.put(b'f'),
                    0x0d => self.put(b'r'),
                    0x1b => self.put(b'e'),
                    0x22 => self.put(b'"'),
                    0x5c => self.put(b'\\'),
                    0x85 => self.put(b'N'),
                    0xA0 => self.put(b'_'),
                    0x2028 => self.put(b'L'),
                    0x2029 => self.put(b'P'),
                    _ => {
                        let digits = if v <= 0xFF {
                            self.put(b'x');
                            2
                        } else if v <= 0xFFFF {
                            self.put(b'u');
                            4
                        } else {
                            self.put(b'U');
                            8
                        };
                        let mut k: i32 = (digits - 1) * 4;
                        while k >= 0 {
                            let digit = ((v >> k) & 0x0F) as u8;
                            self.put(if digit < 10 { digit + b'0' } else { digit + b'A' - 10 });
                            k -= 4;
                        }
                    }
                }
                spaces = false;
            } else if is_space(value, i) {
                if allow_breaks && !spaces && self.column > self.best_width && i > 0 && i < value.len() - 1 {
                    self.write_indent();
                    if is_space(value, i + 1) {
                        self.put(b'\\');
                    }
                    i += width(value[i]).max(1);
                } else {
                    self.write(value, &mut i);
                }
                spaces = true;
            } else {
                self.write(value, &mut i);
                spaces = false;
            }
        }
        self.write_indicator(b"\"", false, false, false);
        self.whitespace = false;
        self.indention = false;
    }

    fn write_block_scalar_hints(&mut self, value: &[u8]) {
        if is_space(value, 0) || is_break(value, 0) {
            let hint = [b'0' + self.best_indent as u8];
            self.write_indicator(&hint, false, false, false);
        }
        self.open_ended = false;
        let mut chomp: u8 = 0;
        if value.is_empty() {
            chomp = b'-';
        } else {
            let mut i = value.len() - 1;
            while value[i] & 0xC0 == 0x80 {
                i -= 1;
            }
            if !is_break(value, i) {
                chomp = b'-';
            } else if i == 0 {
                chomp = b'+';
                self.open_ended = true;
            } else {
                i -= 1;
                while value[i] & 0xC0 == 0x80 {
                    i -= 1;
                }
                if is_break(value, i) {
                    chomp = b'+';
                    self.open_ended = true;
                }
            }
        }
        if chomp != 0 {
            self.write_indicator(&[chomp], false, false, false);
        }
    }

    fn write_literal(&mut self, value: &[u8]) {
        self.write_indicator(b"|", true, false, false);
        self.write_block_scalar_hints(value);
        self.put_break();
        self.indention = true;
        self.whitespace = true;
        let mut breaks = true;
        let mut i = 0;
        while i < value.len() {
            if is_break(value, i) {
                self.write_break(value, &mut i);
                self.indention = true;
                breaks = true;
            } else {
                if breaks {
                    self.write_indent();
                }
                self.write(value, &mut i);
                self.indention = false;
                breaks = false;
            }
        }
    }
}
