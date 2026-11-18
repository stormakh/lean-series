//! A minimal XML reader for the AWS Query protocol responses (EC2, RDS, ElastiCache, STS): elements with their
//! local names (namespace prefixes dropped), attributes, and text. Comments, processing instructions and the DOCTYPE
//! are skipped; CDATA and the predefined and numeric entities are decoded.

/// An XML element.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Element {
    /// The local name (without a namespace prefix).
    pub name: String,
    pub attrs: Vec<(String, String)>,
    pub children: Vec<Element>,
    /// The concatenated character data directly inside this element.
    pub text: String,
}

impl Element {
    /// The first child with this name.
    pub fn child(&self, name: &str) -> Option<&Element> {
        self.children.iter().find(|c| c.name == name)
    }

    /// All children with this name.
    pub fn children_named<'a>(&'a self, name: &'a str) -> impl Iterator<Item = &'a Element> + 'a {
        self.children.iter().filter(move |c| c.name == name)
    }

    /// The element at a path of child names ("a/b/c").
    pub fn path(&self, p: &str) -> Option<&Element> {
        let mut e = self;
        for n in p.split('/').filter(|s| !s.is_empty()) {
            e = e.child(n)?;
        }
        Some(e)
    }

    /// The text of the child at a path, if present (Go: a non-nil `*string`).
    pub fn text_at(&self, p: &str) -> Option<&str> {
        self.path(p).map(|e| e.text.as_str())
    }

    /// The items of a list wrapper: the children of the element at `p` (for example `reservationSet` -> its
    /// `item`s, or `DBInstances` -> its `DBInstance`s). Empty if the wrapper is missing.
    pub fn list<'a>(&'a self, p: &str) -> &'a [Element] {
        self.path(p).map(|e| e.children.as_slice()).unwrap_or(&[])
    }

    /// The value of an attribute.
    pub fn attr(&self, name: &str) -> Option<&str> {
        self.attrs.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }
}

fn local(name: &str) -> &str {
    name.rsplit_once(':').map(|(_, l)| l).unwrap_or(name)
}

/// Decodes the entities of character data.
pub fn unescape(s: &str) -> Result<String, String> {
    if !s.contains('&') {
        return Ok(s.to_string());
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('&') {
        out.push_str(&rest[..i]);
        let after = &rest[i + 1..];
        let Some(j) = after.find(';') else { return Err("XML syntax error: invalid character entity".to_string()) };
        let ent = &after[..j];
        match ent {
            "lt" => out.push('<'),
            "gt" => out.push('>'),
            "amp" => out.push('&'),
            "quot" => out.push('"'),
            "apos" => out.push('\''),
            _ => {
                let code = if let Some(h) = ent.strip_prefix("#x").or_else(|| ent.strip_prefix("#X")) {
                    u32::from_str_radix(h, 16).ok()
                } else if let Some(d) = ent.strip_prefix('#') {
                    d.parse::<u32>().ok()
                } else {
                    None
                };
                match code.and_then(char::from_u32) {
                    Some(c) => out.push(c),
                    None => return Err(format!("XML syntax error: invalid character entity &{ent};")),
                }
            }
        }
        rest = &after[j + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Parses a document and returns its root element.
pub fn parse(doc: &str) -> Result<Element, String> {
    let b = doc.as_bytes();
    let mut i = 0;
    let mut stack: Vec<Element> = Vec::new();
    let mut root: Option<Element> = None;
    while i < b.len() {
        if b[i] != b'<' {
            let end = doc[i..].find('<').map(|j| i + j).unwrap_or(b.len());
            let t = &doc[i..end];
            match stack.last_mut() {
                Some(e) => e.text.push_str(&unescape(t)?),
                None if t.trim().is_empty() => {}
                None => return Err("XML syntax error: character data outside the root element".to_string()),
            }
            i = end;
            continue;
        }
        let rest = &doc[i..];
        if rest.starts_with("<!--") {
            let j = rest.find("-->").ok_or("XML syntax error: unexpected EOF in comment")?;
            i += j + 3;
        } else if rest.starts_with("<![CDATA[") {
            let j = rest.find("]]>").ok_or("XML syntax error: unexpected EOF in CDATA section")?;
            if let Some(e) = stack.last_mut() {
                e.text.push_str(&rest[9..j]);
            }
            i += j + 3;
        } else if rest.starts_with("<?") {
            let j = rest.find("?>").ok_or("XML syntax error: unexpected EOF in processing instruction")?;
            i += j + 2;
        } else if rest.starts_with("<!") {
            let j = rest.find('>').ok_or("XML syntax error: unexpected EOF in directive")?;
            i += j + 1;
        } else if let Some(r) = rest.strip_prefix("</") {
            let j = r.find('>').ok_or("XML syntax error: unexpected EOF in end element")?;
            let name = local(r[..j].trim());
            let e = stack.pop().ok_or_else(|| format!("XML syntax error: unexpected end element </{name}>"))?;
            if e.name != name {
                return Err(format!("XML syntax error: element <{}> closed by </{name}>", e.name));
            }
            match stack.last_mut() {
                Some(p) => p.children.push(e),
                None => root = Some(e),
            }
            i += 2 + j + 1;
        } else {
            // start tag: find the closing '>' outside quoted attribute values
            let mut j = 1;
            let mut quote = 0u8;
            while j < rest.len() {
                let c = rest.as_bytes()[j];
                if quote != 0 {
                    if c == quote {
                        quote = 0;
                    }
                } else if c == b'"' || c == b'\'' {
                    quote = c;
                } else if c == b'>' {
                    break;
                }
                j += 1;
            }
            if j >= rest.len() {
                return Err("XML syntax error: unexpected EOF in start element".to_string());
            }
            let mut inner = &rest[1..j];
            let self_closing = inner.ends_with('/');
            if self_closing {
                inner = &inner[..inner.len() - 1];
            }
            let name_end = inner.find(|c: char| c.is_whitespace()).unwrap_or(inner.len());
            let name = local(&inner[..name_end]).to_string();
            if name.is_empty() {
                return Err("XML syntax error: expected element name after <".to_string());
            }
            let attrs = parse_attrs(&inner[name_end..])?;
            let e = Element { name, attrs, children: Vec::new(), text: String::new() };
            if self_closing {
                match stack.last_mut() {
                    Some(p) => p.children.push(e),
                    None => root = Some(e),
                }
            } else {
                stack.push(e);
            }
            i += j + 1;
        }
    }
    if let Some(e) = stack.last() {
        return Err(format!("XML syntax error: unexpected EOF, element <{}> not closed", e.name));
    }
    root.ok_or_else(|| "EOF".to_string())
}

fn parse_attrs(s: &str) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    let mut rest = s.trim_start();
    while !rest.is_empty() {
        let eq = rest.find('=').ok_or("XML syntax error: attribute without value")?;
        let name = rest[..eq].trim();
        let after = rest[eq + 1..].trim_start();
        let q = after.chars().next().ok_or("XML syntax error: unquoted or missing attribute value")?;
        if q != '"' && q != '\'' {
            return Err("XML syntax error: unquoted or missing attribute value in element".to_string());
        }
        let end = after[1..].find(q).ok_or("XML syntax error: unexpected EOF in attribute value")?;
        out.push((local(name).to_string(), unescape(&after[1..1 + end])?));
        rest = after[1 + end + 1..].trim_start();
    }
    Ok(out)
}

/// Escapes text for an XML document (for test fixtures and the mock servers).
pub fn escape(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '<' => o.push_str("&lt;"),
            '>' => o.push_str("&gt;"),
            '&' => o.push_str("&amp;"),
            '"' => o.push_str("&quot;"),
            '\'' => o.push_str("&apos;"),
            _ => o.push(c),
        }
    }
    o
}
