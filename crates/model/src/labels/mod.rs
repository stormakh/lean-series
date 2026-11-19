//! Port of `model/labels` in its default `stringlabels` form: a label set is one immutable byte buffer that holds
//! name/value pairs sorted by name, each string preceded by its length (one byte for 0..=253, else 0xFF and three
//! little-endian bytes). The buffer is shared (`Arc`), so cloning a `Labels` is cheap.
//!
//! Long-lived label sets (head series) may be interned ([`Labels::interned`]): their strings of at least
//! `MIN_INTERN_LEN` bytes become 3-byte references (0xFE and a little-endian id) to one copy in a process-wide
//! table. Equal label sets may therefore have different buffers; equality, ordering, hashes and the `bytes`
//! encodings are defined on the strings and do not depend on interning.
//!
//! All names and values are valid UTF-8 (they come from `&str`), so the string accessors never fail. Go strings may
//! hold invalid UTF-8; in Rust such bytes must be rejected (or converted) where they enter the program.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, LazyLock, Mutex, OnceLock};

mod float;
mod sharding;
mod test_utils;
pub mod xxhash;

pub use crate::matcher::{MatchType, Matcher};
pub use float::{append_float_g, append_open_metrics_float, format_float_g, format_open_metrics_float};
pub use sharding::stable_hash;
pub use test_utils::{read_labels, Selector};

/// The special label name that holds the metric name.
pub const METRIC_NAME: &str = "__name__";
/// The label name of an alert's name.
pub const ALERT_NAME: &str = "alertname";
/// The label name of a histogram bucket's upper bound.
pub const BUCKET_LABEL: &str = "le";

/// Separator used between labels in `hash_for_labels`/`hash_without_labels`.
pub(crate) const SEP: u8 = 0xff;

/// Name of this labels implementation (Go `ImplementationName`).
pub const IMPLEMENTATION_NAME: &str = "stringlabels";

/// A name/value pair of strings.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Label {
    pub name: String,
    pub value: String,
}

impl Label {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Label {
        Label { name: name.into(), value: value.into() }
    }
}

/// How metric and label names are validated (Go `model.ValidationScheme` from prometheus/common).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default)]
pub enum ValidationScheme {
    /// An undefined scheme. Validation with it panics, as in Go.
    #[default]
    Unset,
    /// Names must match `[a-zA-Z_:][a-zA-Z0-9_:]*` (metric names) or `[a-zA-Z_][a-zA-Z0-9_]*` (label names).
    Legacy,
    /// Names must be non-empty valid UTF-8.
    Utf8,
}

impl ValidationScheme {
    pub fn as_str(self) -> &'static str {
        match self {
            ValidationScheme::Unset => "unset",
            ValidationScheme::Legacy => "legacy",
            ValidationScheme::Utf8 => "utf8",
        }
    }

    /// Go `ValidationScheme.Set`: "" leaves the value unchanged.
    pub fn set(&mut self, text: &str) -> Result<(), String> {
        match text {
            "" => {}
            "legacy" => *self = ValidationScheme::Legacy,
            "utf8" => *self = ValidationScheme::Utf8,
            _ => return Err(format!("unrecognized ValidationScheme: {}", crate::unicode::quote(text))),
        }
        Ok(())
    }

    /// Whether `name` is a valid metric name under this scheme.
    pub fn is_valid_metric_name(self, name: &str) -> bool {
        match self {
            ValidationScheme::Legacy => {
                !name.is_empty() && name.bytes().enumerate().all(|(i, b)| is_valid_legacy_metric_byte(b, i))
            }
            ValidationScheme::Utf8 => !name.is_empty(),
            ValidationScheme::Unset => panic!("Invalid name validation scheme requested: unset"),
        }
    }

    /// Whether `name` is a valid label name under this scheme.
    pub fn is_valid_label_name(self, name: &str) -> bool {
        match self {
            ValidationScheme::Legacy => {
                !name.is_empty()
                    && name.bytes().enumerate().all(|(i, b)| {
                        b.is_ascii_alphabetic() || b == b'_' || (b.is_ascii_digit() && i > 0)
                    })
            }
            ValidationScheme::Utf8 => !name.is_empty(),
            ValidationScheme::Unset => panic!("Invalid name validation scheme requested: unset"),
        }
    }
}

impl fmt::Display for ValidationScheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[inline]
fn is_valid_legacy_metric_byte(b: u8, i: usize) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b == b':' || (b.is_ascii_digit() && i > 0)
}

// ---------------------------------------------------------------------------------------------------------------
// Encoding.

/// Marks a reference to an interned string (followed by a 2-byte little-endian id).
const REF_MARK: u8 = 254;
/// Marks a long string (followed by a 3-byte little-endian length).
const LONG_MARK: u8 = 255;
/// Strings shorter than this stay inline (a reference takes 3 bytes).
const MIN_INTERN_LEN: usize = 4;
/// The most strings and bytes the intern table holds; interned strings are never freed.
const MAX_SYMBOLS: usize = 1 << 16;
const MAX_SYMBOL_BYTES: usize = 16 << 20;

/// The interned strings by id.
static SYMBOLS: [OnceLock<&'static str>; MAX_SYMBOLS] = [const { OnceLock::new() }; MAX_SYMBOLS];

/// The ids of the interned strings, and their total size.
static SYMBOL_IDS: LazyLock<Mutex<(HashMap<&'static str, u16>, usize)>> =
    LazyLock::new(|| Mutex::new((HashMap::new(), 0)));

#[inline(always)]
fn symbol(id: usize) -> &'static [u8] {
    match SYMBOLS[id].get() {
        Some(s) => s.as_bytes(),
        None => unreachable!("label string reference to a missing symbol"),
    }
}

/// The id of `s` in the intern table, adding it when there is room.
fn intern(table: &mut (HashMap<&'static str, u16>, usize), s: &str) -> Option<u16> {
    if let Some(&id) = table.0.get(s) {
        return Some(id);
    }
    let id = table.0.len();
    if id >= MAX_SYMBOLS || table.1 + s.len() > MAX_SYMBOL_BYTES {
        return None;
    }
    let st: &'static str = Box::leak(s.to_owned().into_boxed_str());
    let _ = SYMBOLS[id].set(st);
    table.0.insert(st, id as u16);
    table.1 += s.len();
    Some(id as u16)
}

/// The string at `index` (inline or interned) and the index after it.
#[inline(always)]
fn decode_string(data: &[u8], index: usize) -> (&[u8], usize) {
    let b = data[index];
    if b < REF_MARK {
        let i = index + 1;
        return (&data[i..i + b as usize], i + b as usize);
    }
    if b == REF_MARK {
        return (symbol(data[index + 1] as usize | (data[index + 2] as usize) << 8), index + 3);
    }
    let i = index + 1;
    let size = data[i] as usize | (data[i + 1] as usize) << 8 | (data[i + 2] as usize) << 16;
    (&data[i + 3..i + 3 + size], i + 3 + size)
}

/// Whether the buffer has interned strings.
fn has_refs(data: &[u8]) -> bool {
    let mut i = 0;
    while i < data.len() {
        if data[i] == REF_MARK {
            return true;
        }
        (_, i) = decode_string(data, i);
    }
    false
}

/// Appends the plain (not interned) encoding of the strings of `data` to `buf`.
fn append_plain(buf: &mut Vec<u8>, data: &[u8]) {
    let mut i = 0;
    while i < data.len() {
        let (s, ni) = decode_string(data, i);
        if data[i] == REF_MARK {
            encode_size(buf, s.len());
            buf.extend_from_slice(s);
        } else {
            buf.extend_from_slice(&data[i..ni]);
        }
        i = ni;
    }
}

/// Views label bytes as a str. Every string in a label buffer was copied from a `&str`.
#[inline(always)]
fn as_str(b: &[u8]) -> &str {
    match std::str::from_utf8(b) {
        Ok(s) => s,
        Err(_) => unreachable!("label strings are valid UTF-8"),
    }
}

fn size_when_encoded(x: usize) -> usize {
    if x < REF_MARK as usize {
        1
    } else if x <= 1 << 24 {
        4
    } else {
        panic!("String too long to encode as label.")
    }
}

#[inline]
fn encode_size(buf: &mut Vec<u8>, v: usize) {
    if v < REF_MARK as usize {
        buf.push(v as u8);
    } else {
        buf.extend_from_slice(&[LONG_MARK, v as u8, (v >> 8) as u8, (v >> 16) as u8]);
    }
}

#[inline]
fn label_size(name: &str, value: &str) -> usize {
    name.len() + size_when_encoded(name.len()) + value.len() + size_when_encoded(value.len())
}

/// The size of a label in Go's encoding (a 1-byte length below 255 bytes).
fn go_label_size(name: usize, value: usize) -> usize {
    let size = |x: usize| if x < 255 { 1 } else { size_when_encoded(x) };
    name + size(name) + value + size(value)
}

#[inline]
fn append_label_to(buf: &mut Vec<u8>, name: &str, value: &str) {
    // Go panics in sizeWhenEncoded for strings over 16 MiB.
    size_when_encoded(name.len());
    size_when_encoded(value.len());
    encode_size(buf, name.len());
    buf.extend_from_slice(name.as_bytes());
    encode_size(buf, value.len());
    buf.extend_from_slice(value.as_bytes());
}

// ---------------------------------------------------------------------------------------------------------------
// Labels.

/// A sorted set of labels, stored as one shared byte buffer.
#[derive(Clone, Default)]
pub struct Labels {
    data: Option<Arc<[u8]>>,
}

/// Iterator over the `(name, value)` pairs of a `Labels`.
pub struct Iter<'a> {
    data: &'a [u8],
    i: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a str, &'a str);

    #[inline]
    fn next(&mut self) -> Option<(&'a str, &'a str)> {
        if self.i >= self.data.len() {
            return None;
        }
        let (n, i) = decode_string(self.data, self.i);
        let (v, i) = decode_string(self.data, i);
        self.i = i;
        Some((as_str(n), as_str(v)))
    }
}

impl<'a> IntoIterator for &'a Labels {
    type Item = (&'a str, &'a str);
    type IntoIter = Iter<'a>;
    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

/// Returns an empty label set.
#[inline]
pub fn empty_labels() -> Labels {
    Labels { data: None }
}

/// Returns whether the two label sets are equal.
#[inline]
pub fn equal(a: &Labels, b: &Labels) -> bool {
    a == b
}

impl Labels {
    #[inline]
    fn from_vec(v: Vec<u8>) -> Labels {
        if v.is_empty() { Labels { data: None } } else { Labels { data: Some(Arc::from(v)) } }
    }

    /// The raw encoded buffer.
    #[inline]
    pub fn data(&self) -> &[u8] {
        match &self.data {
            Some(d) => d,
            None => &[],
        }
    }

    /// An empty label set.
    #[inline]
    pub fn empty() -> Labels {
        empty_labels()
    }

    /// Writes an opaque, not-human-readable encoding of the labels into `buf` (cleared first), usable as a map key.
    pub fn bytes(&self, buf: &mut Vec<u8>) {
        buf.clear();
        append_plain(buf, self.data());
    }

    /// The same labels with their strings of at least `MIN_INTERN_LEN` bytes kept once in a process-wide table
    /// (strings that do not fit in the table stay inline). For label sets that live long, such as those of head
    /// series: an interned set takes about a fifth of the memory of a plain one.
    pub fn interned(&self) -> Labels {
        let data = self.data();
        let mut buf = Vec::with_capacity(data.len());
        let mut table = SYMBOL_IDS.lock().unwrap_or_else(|e| e.into_inner());
        let mut i = 0;
        while i < data.len() {
            let (s, ni) = decode_string(data, i);
            match (s.len() >= MIN_INTERN_LEN).then(|| intern(&mut table, as_str(s))).flatten() {
                Some(id) => buf.extend_from_slice(&[REF_MARK, id as u8, (id >> 8) as u8]),
                None => {
                    encode_size(&mut buf, s.len());
                    buf.extend_from_slice(s);
                }
            }
            i = ni;
        }
        drop(table);
        if buf[..] == data[..] {
            return self.clone();
        }
        Labels::from_vec(buf)
    }

    /// Whether some strings of the labels are interned.
    pub fn is_interned(&self) -> bool {
        has_refs(self.data())
    }

    /// Whether the labels are the zero value (empty).
    #[inline]
    pub fn is_zero(&self) -> bool {
        self.data().is_empty()
    }

    /// Whether the set has no labels.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data().is_empty()
    }

    /// Iterates `(name, value)` pairs in name order.
    #[inline]
    pub fn iter(&self) -> Iter<'_> {
        Iter { data: self.data(), i: 0 }
    }

    /// Iterates `(name, value)` pairs in name order as bytes, without the UTF-8 checks of [`Labels::iter`] (for
    /// hot loops that compare bytes).
    #[inline]
    pub fn iter_bytes(&self) -> impl Iterator<Item = (&[u8], &[u8])> + '_ {
        let data = self.data();
        let mut i = 0;
        std::iter::from_fn(move || {
            if i >= data.len() {
                return None;
            }
            let (n, j) = decode_string(data, i);
            let (v, j) = decode_string(data, j);
            i = j;
            Some((n, v))
        })
    }

    /// Calls `f` on each label (Go `Range`).
    #[inline]
    pub fn range(&self, mut f: impl FnMut(&str, &str)) {
        for (n, v) in self.iter() {
            f(n, v);
        }
    }

    /// Calls `f` on each label; stops at and returns the first error (Go `Validate`).
    pub fn validate<E>(&self, mut f: impl FnMut(&str, &str) -> Result<(), E>) -> Result<(), E> {
        for (n, v) in self.iter() {
            f(n, v)?;
        }
        Ok(())
    }

    /// Returns the subset of labels whose names are (`on`) or are not (`!on`, also dropping the metric name) in
    /// `names`.
    pub fn match_labels<S: AsRef<str>>(&self, on: bool, names: &[S]) -> Labels {
        let mut b = Builder::new(self.clone());
        if on {
            b.keep(names);
        } else {
            b.del(METRIC_NAME);
            b.del_many(names);
        }
        b.labels()
    }

    /// Hash of the label set (xxhash64 of the encoded buffer). Not stable across versions.
    #[inline]
    pub fn hash(&self) -> u64 {
        let data = self.data();
        if !has_refs(data) {
            return xxhash::sum64(data);
        }
        // The hash of the plain encoding.
        let mut d = xxhash::Digest::new();
        let mut size = Vec::with_capacity(4);
        let mut i = 0;
        while i < data.len() {
            let (s, ni) = decode_string(data, i);
            size.clear();
            encode_size(&mut size, s.len());
            d.write(&size);
            d.write(s);
            i = ni;
        }
        d.sum64()
    }

    /// Hash of the labels whose names are in `names` (sorted ascending). `b` is scratch space.
    pub fn hash_for_labels<S: AsRef<str>>(&self, b: &mut Vec<u8>, names: &[S]) -> u64 {
        b.clear();
        let data = self.data();
        let mut j = 0;
        let mut i = 0;
        while i < data.len() {
            let (name, ni) = decode_string(data, i);
            let (value, ni) = decode_string(data, ni);
            i = ni;
            while j < names.len() && names[j].as_ref().as_bytes() < name {
                j += 1;
            }
            if j == names.len() {
                break;
            }
            if name == names[j].as_ref().as_bytes() {
                b.extend_from_slice(name);
                b.push(SEP);
                b.extend_from_slice(value);
                b.push(SEP);
            }
        }
        xxhash::sum64(b)
    }

    /// Hash of all labels except the metric name and those in `names` (sorted ascending). `b` is scratch space.
    pub fn hash_without_labels<S: AsRef<str>>(&self, b: &mut Vec<u8>, names: &[S]) -> u64 {
        b.clear();
        let data = self.data();
        let mut j = 0;
        let mut i = 0;
        while i < data.len() {
            let (name, ni) = decode_string(data, i);
            let (value, ni) = decode_string(data, ni);
            i = ni;
            while j < names.len() && names[j].as_ref().as_bytes() < name {
                j += 1;
            }
            if name == METRIC_NAME.as_bytes() || (j < names.len() && name == names[j].as_ref().as_bytes()) {
                continue;
            }
            b.extend_from_slice(name);
            b.push(SEP);
            b.extend_from_slice(value);
            b.push(SEP);
        }
        xxhash::sum64(b)
    }

    /// Like `bytes`, but only for labels whose names are in `names` (sorted ascending).
    pub fn bytes_with_labels<S: AsRef<str>>(&self, buf: &mut Vec<u8>, names: &[S]) {
        buf.clear();
        let data = self.data();
        let mut j = 0;
        let mut pos = 0;
        while pos < data.len() {
            let (lname, np) = decode_string(data, pos);
            let (_, np) = decode_string(data, np);
            while j < names.len() && names[j].as_ref().as_bytes() < lname {
                j += 1;
            }
            if j == names.len() {
                break;
            }
            if lname == names[j].as_ref().as_bytes() {
                append_plain(buf, &data[pos..np]);
            }
            pos = np;
        }
    }

    /// Like `bytes`, but only for labels whose names are not in `names` (sorted ascending).
    pub fn bytes_without_labels<S: AsRef<str>>(&self, buf: &mut Vec<u8>, names: &[S]) {
        buf.clear();
        let data = self.data();
        let mut j = 0;
        let mut pos = 0;
        while pos < data.len() {
            let (lname, np) = decode_string(data, pos);
            let (_, np) = decode_string(data, np);
            while j < names.len() && names[j].as_ref().as_bytes() < lname {
                j += 1;
            }
            if j == names.len() || lname != names[j].as_ref().as_bytes() {
                append_plain(buf, &data[pos..np]);
            }
            pos = np;
        }
    }

    /// A deep copy of the labels (a new buffer).
    pub fn copy(&self) -> Labels {
        Labels::from_vec(self.data().to_vec())
    }

    /// Replaces `self` with `b` (shares the buffer).
    pub fn copy_from(&mut self, b: &Labels) {
        self.data = b.data.clone();
    }

    /// The value of the label `name`, or "" if it is not present.
    #[inline]
    pub fn get(&self, name: &str) -> &str {
        let nb = name.as_bytes();
        if nb.is_empty() {
            return "";
        }
        let data = self.data();
        let mut i = 0;
        while i < data.len() {
            let (lname, ni) = decode_string(data, i);
            let (v, ni) = decode_string(data, ni);
            i = ni;
            match lname.cmp(nb) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => return as_str(v),
                std::cmp::Ordering::Greater => break,
            }
        }
        ""
    }

    /// Whether the label `name` is present.
    #[inline]
    pub fn has(&self, name: &str) -> bool {
        let nb = name.as_bytes();
        if nb.is_empty() {
            return false;
        }
        let data = self.data();
        let mut i = 0;
        while i < data.len() {
            let (lname, ni) = decode_string(data, i);
            let (_, ni) = decode_string(data, ni);
            i = ni;
            match lname.cmp(nb) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => return true,
                std::cmp::Ordering::Greater => break,
            }
        }
        false
    }

    /// Returns the first duplicated label name, if any. Assumes the set is sorted.
    pub fn has_duplicate_label_names(&self) -> Option<&str> {
        let data = self.data();
        let mut prev: &[u8] = b"";
        let mut i = 0;
        while i < data.len() {
            let (lname, ni) = decode_string(data, i);
            let (_, ni) = decode_string(data, ni);
            i = ni;
            if lname == prev {
                return Some(as_str(lname));
            }
            prev = lname;
        }
        None
    }

    /// Returns the label set without labels that have empty values. May return the same set.
    pub fn without_empty(&self) -> Labels {
        let data = self.data();
        let mut pos = 0;
        while pos < data.len() {
            let (_, np) = decode_string(data, pos);
            let (lvalue, np) = decode_string(data, np);
            if !lvalue.is_empty() {
                pos = np;
                continue;
            }
            let mut buf = Vec::with_capacity(pos + (data.len() - np));
            buf.extend_from_slice(&data[..pos]);
            pos = np;
            while pos < data.len() {
                let (_, np) = decode_string(data, pos);
                let (lvalue, np) = decode_string(data, np);
                if !lvalue.is_empty() {
                    buf.extend_from_slice(&data[pos..np]);
                }
                pos = np;
            }
            return Labels::from_vec(buf);
        }
        self.clone()
    }

    /// Approximate size of the labels in bytes.
    #[inline]
    pub fn byte_size(&self) -> u64 {
        // Go reports the length of its buffer, which keeps strings of 254 bytes with a 1-byte length and has no
        // interned strings.
        let data = self.data();
        let mut n = 0;
        let mut i = 0;
        while i < data.len() {
            let (k, ni) = decode_string(data, i);
            let (v, ni) = decode_string(data, ni);
            n += go_label_size(k.len(), v.len());
            i = ni;
        }
        n as u64
    }

    /// Number of labels; relatively slow.
    pub fn len(&self) -> usize {
        let data = self.data();
        let mut count = 0;
        let mut i = 0;
        while i < data.len() {
            let (_, ni) = decode_string(data, i);
            (_, i) = decode_string(data, ni);
            count += 1;
        }
        count
    }

    /// Returns the labels without `__name__`.
    pub fn drop_metric_name(&self) -> Labels {
        self.drop_reserved(|n| n == METRIC_NAME)
    }

    /// Returns the labels without the reserved (starting with an underscore) labels chosen by `should_drop`.
    pub fn drop_reserved(&self, mut should_drop: impl FnMut(&str) -> bool) -> Labels {
        let mut data: std::borrow::Cow<'_, [u8]> = std::borrow::Cow::Borrowed(self.data());
        let mut i = 0;
        while i < data.len() {
            let (lname, i2) = decode_string(&data, i);
            let (_, i2) = decode_string(&data, i2);
            if lname[0] > b'_' {
                break;
            }
            if should_drop(as_str(lname)) {
                let mut v = data.into_owned();
                v.drain(i..i2);
                data = std::borrow::Cow::Owned(v);
                continue;
            }
            i = i2;
        }
        match data {
            std::borrow::Cow::Borrowed(_) => self.clone(),
            std::borrow::Cow::Owned(v) => Labels::from_vec(v),
        }
    }

    /// The labels as a map.
    pub fn map(&self) -> HashMap<String, String> {
        self.iter().map(|(n, v)| (n.to_string(), v.to_string())).collect()
    }

    /// Whether the metric name and label names are valid under `scheme` (label values are always valid UTF-8 in
    /// Rust).
    pub fn is_valid(&self, scheme: ValidationScheme) -> bool {
        self.iter().all(|(n, v)| {
            if n == METRIC_NAME && !scheme.is_valid_metric_name(v) {
                return false;
            }
            scheme.is_valid_label_name(n)
        })
    }

    /// Like `to_string` but without a space after the commas.
    pub fn string_no_space(&self) -> String {
        let mut s = String::with_capacity(self.data().len() + 16);
        self.write_string(&mut s, false);
        s
    }

    /// Appends the Go `String()` form `{a="b", c="d"}` to `out`.
    pub fn write_string(&self, out: &mut String, add_space: bool) {
        out.push('{');
        for (i, (n, v)) in self.iter().enumerate() {
            if i > 0 {
                out.push(',');
                if add_space {
                    out.push(' ');
                }
            }
            if !ValidationScheme::Legacy.is_valid_label_name(n) {
                crate::unicode::append_quote(out, n);
            } else {
                out.push_str(n);
            }
            out.push('=');
            crate::unicode::append_quote(out, v);
        }
        out.push('}');
    }

    /// The labels as a JSON object value (keys in name order, as Go's sorted map encoding).
    pub fn to_json(&self) -> json::Json {
        json::Json::Obj(self.iter().map(|(n, v)| (n.to_string(), json::Json::Str(v.to_string()))).collect())
    }

    /// Appends the JSON encoding (Go `MarshalJSON`) to `out`.
    pub fn write_json(&self, out: &mut String) {
        out.push('{');
        for (i, (n, v)) in self.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            json::write_str(n, out);
            out.push(':');
            json::write_str(v, out);
        }
        out.push('}');
    }

    /// Go `MarshalJSON`.
    pub fn marshal_json(&self) -> String {
        let mut s = String::with_capacity(self.data().len() + 16);
        self.write_json(&mut s);
        s
    }

    /// Go `UnmarshalJSON`: a JSON object of strings (or null).
    pub fn unmarshal_json(text: &str) -> Result<Labels, String> {
        let v = json::Json::parse(text).map_err(|e| e.to_string())?;
        Labels::from_json(&v)
    }

    /// Builds labels from a parsed JSON object of strings (null gives empty labels).
    pub fn from_json(v: &json::Json) -> Result<Labels, String> {
        match v {
            json::Json::Null => Ok(empty_labels()),
            json::Json::Obj(o) => {
                let mut m: HashMap<&str, &str> = HashMap::with_capacity(o.len());
                for (k, v) in o {
                    match v {
                        json::Json::Str(s) => {
                            m.insert(k, s);
                        }
                        json::Json::Null => {}
                        other => {
                            return Err(format!(
                                "json: cannot unmarshal {} into Go value of type string",
                                json_kind(other)
                            ));
                        }
                    }
                }
                Ok(from_map(m))
            }
            other => Err(format!(
                "json: cannot unmarshal {} into Go value of type map[string]string",
                json_kind(other)
            )),
        }
    }
}

impl Labels {
    /// Go `MarshalYAML`: a mapping of names to values.
    pub fn to_yaml(&self) -> yaml_rust2::Yaml {
        let mut h = yaml_rust2::yaml::Hash::new();
        for (n, v) in self.iter() {
            h.insert(yaml_rust2::Yaml::String(n.to_string()), yaml_rust2::Yaml::String(v.to_string()));
        }
        yaml_rust2::Yaml::Hash(h)
    }

    /// The YAML text of `to_yaml` (without a document start marker).
    pub fn marshal_yaml(&self) -> String {
        if self.is_empty() {
            return "{}\n".to_string();
        }
        let mut out = String::new();
        let _ = yaml_rust2::YamlEmitter::new(&mut out).dump(&self.to_yaml());
        let mut s = out.strip_prefix("---\n").unwrap_or(&out).to_string();
        s.push('\n');
        s
    }

    /// Go `UnmarshalYAML`: a mapping of scalars (null gives empty labels).
    pub fn from_yaml(y: &yaml_rust2::Yaml) -> Result<Labels, String> {
        use yaml_rust2::Yaml;
        fn scalar(y: &Yaml) -> Result<String, String> {
            Ok(match y {
                Yaml::String(s) | Yaml::Real(s) => s.clone(),
                Yaml::Integer(i) => i.to_string(),
                Yaml::Boolean(b) => b.to_string(),
                Yaml::Null => String::new(),
                _ => return Err("yaml: cannot unmarshal into map[string]string".to_string()),
            })
        }
        match y {
            Yaml::Null => Ok(empty_labels()),
            Yaml::Hash(h) => {
                let mut m: HashMap<String, String> = HashMap::with_capacity(h.len());
                for (k, v) in h {
                    m.insert(scalar(k)?, scalar(v)?);
                }
                Ok(from_map(m))
            }
            _ => Err("yaml: cannot unmarshal into map[string]string".to_string()),
        }
    }

    /// Parses YAML text with `from_yaml`.
    pub fn unmarshal_yaml(text: &str) -> Result<Labels, String> {
        let docs = yaml_rust2::YamlLoader::load_from_str(text).map_err(|e| e.to_string())?;
        match docs.first() {
            Some(y) => Labels::from_yaml(y),
            None => Ok(empty_labels()),
        }
    }
}

fn json_kind(v: &json::Json) -> &'static str {
    match v {
        json::Json::Null => "null",
        json::Json::Bool(_) => "bool",
        json::Json::Int(_) | json::Json::Float(_) => "number",
        json::Json::Str(_) => "string",
        json::Json::Arr(_) => "array",
        json::Json::Obj(_) => "object",
    }
}

impl PartialEq for Labels {
    #[inline]
    fn eq(&self, other: &Labels) -> bool {
        match (&self.data, &other.data) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b) || a[..] == b[..] || compare_data(a, b) == 0,
            (None, None) => true,
            _ => false,
        }
    }
}

impl Eq for Labels {}

impl std::hash::Hash for Labels {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Equal labels may be encoded differently (interned or not); hash the strings.
        state.write_u64(Labels::hash(self));
    }
}

impl PartialOrd for Labels {
    fn partial_cmp(&self, other: &Labels) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Labels {
    fn cmp(&self, other: &Labels) -> std::cmp::Ordering {
        compare(self, other).cmp(&0)
    }
}

impl fmt::Display for Labels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::with_capacity(self.data().len() + 16);
        self.write_string(&mut s, true);
        f.write_str(&s)
    }
}

impl fmt::Debug for Labels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// Compares two label sets: negative if a < b, 0 if equal, positive if a > b (Go `labels.Compare`).
pub fn compare(a: &Labels, b: &Labels) -> i32 {
    compare_data(a.data(), b.data())
}

/// [`compare`] on encoded buffers: the label strings in order, the shorter sequence first when one is a prefix.
fn compare_data(ad: &[u8], bd: &[u8]) -> i32 {
    if has_refs(ad) || has_refs(bd) {
        let (mut i, mut j) = (0, 0);
        loop {
            match (i < ad.len(), j < bd.len()) {
                (false, false) => return 0,
                (false, true) => return -1,
                (true, false) => return 1,
                (true, true) => {}
            }
            let (x, ni) = decode_string(ad, i);
            let (y, nj) = decode_string(bd, j);
            match x.cmp(y) {
                std::cmp::Ordering::Less => return -1,
                std::cmp::Ordering::Greater => return 1,
                std::cmp::Ordering::Equal => {}
            }
            (i, j) = (ni, nj);
        }
    }
    let (shorter, longer) = if bd.len() < ad.len() { (bd, ad) } else { (ad, bd) };
    let mut i = 0;
    while i + 8 < shorter.len() {
        let x = u64::from_ne_bytes(shorter[i..i + 8].try_into().unwrap());
        let y = u64::from_ne_bytes(longer[i..i + 8].try_into().unwrap());
        if x != y {
            break;
        }
        i += 8;
    }
    while i < shorter.len() {
        if shorter[i] != longer[i] {
            break;
        }
        i += 1;
    }
    if i == shorter.len() {
        return match ad.len().cmp(&bd.len()) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        };
    }
    let first_diff = i;
    let mut i = 0;
    let (_, mut next) = decode_string(ad, i);
    while next <= first_diff {
        i = next;
        (_, next) = decode_string(ad, i);
    }
    let (a_str, _) = decode_string(ad, i);
    let (b_str, _) = decode_string(bd, i);
    if a_str < b_str { -1 } else { 1 }
}

/// Returns sorted labels from the given labels (sorted in place). Names must be unique.
pub fn new(ls: &mut [Label]) -> Labels {
    ls.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    let size: usize = ls.iter().map(|l| label_size(&l.name, &l.value)).sum();
    let mut buf = Vec::with_capacity(size);
    for l in ls.iter() {
        append_label_to(&mut buf, &l.name, &l.value);
    }
    Labels::from_vec(buf)
}

/// Creates labels from pairs of strings. Panics on an odd number of strings.
pub fn from_strings<S: AsRef<str>>(ss: &[S]) -> Labels {
    if ss.len() % 2 != 0 {
        panic!("invalid number of strings");
    }
    let mut idx: Vec<usize> = (0..ss.len() / 2).collect();
    idx.sort_by(|&a, &b| ss[2 * a].as_ref().cmp(ss[2 * b].as_ref()));
    let size: usize = idx.iter().map(|&i| label_size(ss[2 * i].as_ref(), ss[2 * i + 1].as_ref())).sum();
    let mut buf = Vec::with_capacity(size);
    for &i in &idx {
        append_label_to(&mut buf, ss[2 * i].as_ref(), ss[2 * i + 1].as_ref());
    }
    Labels::from_vec(buf)
}

/// Creates sorted labels from a map (or any iterator of name/value pairs with unique names).
pub fn from_map<I, K, V>(m: I) -> Labels
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let mut v: Vec<(K, V)> = m.into_iter().collect();
    v.sort_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
    let size: usize = v.iter().map(|(n, x)| label_size(n.as_ref(), x.as_ref())).sum();
    let mut buf = Vec::with_capacity(size);
    for (n, x) in &v {
        append_label_to(&mut buf, n.as_ref(), x.as_ref());
    }
    Labels::from_vec(buf)
}

/// Approximate space required for `n` copies of a label.
pub fn size_of_labels(name: &str, value: &str, n: u64) -> u64 {
    go_label_size(name.len(), value.len()) as u64 * n
}

// ---------------------------------------------------------------------------------------------------------------
// Builder.

/// Modifies a base label set: deletions, additions and replacements, applied by `labels()`.
#[derive(Clone, Default, Debug)]
pub struct Builder {
    base: Labels,
    del: Vec<String>,
    add: Vec<Label>,
}

impl Builder {
    /// Go `NewBuilder`.
    pub fn new(base: Labels) -> Builder {
        let mut b = Builder { base: Labels::default(), del: Vec::with_capacity(5), add: Vec::with_capacity(5) };
        b.reset(base);
        b
    }

    /// Clears all current state and starts from `base`. Labels with empty values in `base` are deleted.
    pub fn reset(&mut self, base: Labels) {
        self.del.clear();
        self.add.clear();
        for (n, v) in base.iter() {
            if v.is_empty() {
                self.del.push(n.to_string());
            }
        }
        self.base = base;
    }

    /// Deletes the label `n`.
    pub fn del(&mut self, n: &str) -> &mut Builder {
        if let Some(i) = self.add.iter().position(|a| a.name == n) {
            self.add.remove(i);
        }
        self.del.push(n.to_string());
        self
    }

    /// Deletes each label in `ns` (Go variadic `Del`).
    pub fn del_many<S: AsRef<str>>(&mut self, ns: &[S]) -> &mut Builder {
        for n in ns {
            self.del(n.as_ref());
        }
        self
    }

    /// Removes all labels from the base except those with the given names.
    pub fn keep<S: AsRef<str>>(&mut self, ns: &[S]) -> &mut Builder {
        let base = self.base.clone();
        for (n, _) in base.iter() {
            if ns.iter().any(|x| x.as_ref() == n) {
                continue;
            }
            self.del.push(n.to_string());
        }
        self
    }

    /// Sets the label `n` to `v`. An empty value deletes the label.
    pub fn set(&mut self, n: &str, v: &str) -> &mut Builder {
        if v.is_empty() {
            return self.del(n);
        }
        for a in self.add.iter_mut() {
            if a.name == n {
                a.value.clear();
                a.value.push_str(v);
                return self;
            }
        }
        self.add.push(Label { name: n.to_string(), value: v.to_string() });
        self
    }

    /// The current value of label `n` ("" when absent or deleted).
    pub fn get(&self, n: &str) -> &str {
        for a in &self.add {
            if a.name == n {
                return &a.value;
            }
        }
        if self.del.iter().any(|d| d == n) {
            return "";
        }
        self.base.get(n)
    }

    /// Calls `f` on each label in the builder: base labels that are not deleted or replaced, then added labels.
    pub fn range(&self, mut f: impl FnMut(&str, &str)) {
        for (n, v) in self.base.iter() {
            if !self.del.iter().any(|d| d == n) && !self.add.iter().any(|a| a.name == n) {
                f(n, v);
            }
        }
        for a in &self.add {
            f(&a.name, &a.value);
        }
    }

    /// Like `range`, but `f` may modify the builder: it iterates a snapshot taken before the first call, as Go's
    /// `Range` does.
    pub fn range_mut(&mut self, mut f: impl FnMut(&mut Builder, &str, &str)) {
        let base = self.base.clone();
        let orig_add = self.add.clone();
        let orig_del = self.del.clone();
        for (n, v) in base.iter() {
            if !orig_del.iter().any(|d| d == n) && !orig_add.iter().any(|a| a.name == n) {
                f(self, n, v);
            }
        }
        for a in &orig_add {
            f(self, &a.name, &a.value);
        }
    }

    /// Returns the labels from the builder. If no modifications were made, the base labels are returned.
    pub fn labels(&mut self) -> Labels {
        if self.del.is_empty() && self.add.is_empty() {
            return self.base.clone();
        }
        self.add.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        self.del.sort_unstable();
        let (mut a, mut d) = (0, 0);
        let base = self.base.data();
        let add_size: usize = self.add.iter().map(|l| label_size(&l.name, &l.value)).sum();
        let mut buf = Vec::with_capacity(base.len() + add_size);
        let mut pos = 0;
        while pos < base.len() {
            let old = pos;
            let (lname, np) = decode_string(base, pos);
            let (_, np) = decode_string(base, np);
            pos = np;
            while d < self.del.len() && self.del[d].as_bytes() < lname {
                d += 1;
            }
            if d < self.del.len() && self.del[d].as_bytes() == lname {
                continue;
            }
            while a < self.add.len() && self.add[a].name.as_bytes() < lname {
                append_label_to(&mut buf, &self.add[a].name, &self.add[a].value);
                a += 1;
            }
            if a < self.add.len() && self.add[a].name.as_bytes() == lname {
                append_label_to(&mut buf, &self.add[a].name, &self.add[a].value);
                a += 1;
                continue;
            }
            buf.extend_from_slice(&base[old..pos]);
        }
        while a < self.add.len() {
            append_label_to(&mut buf, &self.add[a].name, &self.add[a].value);
            a += 1;
        }
        Labels::from_vec(buf)
    }
}

// ---------------------------------------------------------------------------------------------------------------
// ScratchBuilder.

/// Builds a `Labels` from scratch efficiently. Added strings are copied into an internal buffer, so the builder
/// allocates nothing once warmed up (except for the output buffer).
#[derive(Clone, Default, Debug)]
pub struct ScratchBuilder {
    strs: String,
    add: Vec<(u32, u32, u32, u32)>,
    output: Labels,
}

impl ScratchBuilder {
    /// Go `NewScratchBuilder`: a builder for about `n` labels.
    pub fn new(n: usize) -> ScratchBuilder {
        ScratchBuilder { strs: String::with_capacity(n * 16), add: Vec::with_capacity(n), output: Labels::default() }
    }

    pub fn reset(&mut self) {
        self.strs.clear();
        self.add.clear();
        self.output = empty_labels();
    }

    /// Adds a name/value pair. Adding the same name twice gives an invalid duplicate label.
    pub fn add(&mut self, name: &str, value: &str) {
        let ns = self.strs.len() as u32;
        self.strs.push_str(name);
        let vs = self.strs.len() as u32;
        self.strs.push_str(value);
        self.add.push((ns, vs, vs, self.strs.len() as u32));
    }

    #[inline]
    fn entry(&self, e: (u32, u32, u32, u32)) -> (&str, &str) {
        (&self.strs[e.0 as usize..e.1 as usize], &self.strs[e.2 as usize..e.3 as usize])
    }

    /// Number of labels added so far.
    pub fn len(&self) -> usize {
        self.add.len()
    }

    pub fn is_empty(&self) -> bool {
        self.add.is_empty()
    }

    /// Iterates the labels added so far, in insertion (or sorted, after `sort`) order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> + '_ {
        self.add.iter().map(|&e| self.entry(e))
    }

    /// Sorts the labels added so far by name.
    pub fn sort(&mut self) {
        let strs = &self.strs;
        self.add.sort_unstable_by(|a, b| strs[a.0 as usize..a.1 as usize].cmp(&strs[b.0 as usize..b.1 as usize]));
    }

    /// Makes `labels()` return `l`.
    pub fn assign(&mut self, l: Labels) {
        self.output = l;
    }

    fn encode(&self, buf: &mut Vec<u8>) {
        for &e in &self.add {
            let (n, v) = self.entry(e);
            append_label_to(buf, n, v);
        }
    }

    fn encoded_size(&self) -> usize {
        self.add.iter().map(|&e| {
            let (n, v) = self.entry(e);
            label_size(n, v)
        }).sum()
    }

    /// The labels added, in the order added (call `sort` first to sort them). Calling `add` after `labels` has no
    /// effect on the result until `reset`.
    pub fn labels(&mut self) -> Labels {
        if self.output.is_empty() {
            let mut buf = Vec::with_capacity(self.encoded_size());
            self.encode(&mut buf);
            self.output = Labels::from_vec(buf);
        }
        self.output.clone()
    }

    /// Writes the newly built labels to `ls`, reusing its buffer when nothing else shares it and the size matches.
    pub fn overwrite(&self, ls: &mut Labels) {
        let size = self.encoded_size();
        if let Some(d) = ls.data.as_mut() {
            if d.len() == size {
                if let Some(m) = Arc::get_mut(d) {
                    let mut pos = 0;
                    for &e in &self.add {
                        let (n, v) = self.entry(e);
                        pos = write_label_at(m, pos, n, v);
                    }
                    return;
                }
            }
        }
        let mut buf = Vec::with_capacity(size);
        self.encode(&mut buf);
        *ls = Labels::from_vec(buf);
    }
}

fn write_label_at(m: &mut [u8], mut pos: usize, name: &str, value: &str) -> usize {
    for s in [name, value] {
        let v = s.len();
        if v < REF_MARK as usize {
            m[pos] = v as u8;
            pos += 1;
        } else {
            m[pos..pos + 4].copy_from_slice(&[LONG_MARK, v as u8, (v >> 8) as u8, (v >> 16) as u8]);
            pos += 4;
        }
        m[pos..pos + v].copy_from_slice(s.as_bytes());
        pos += v;
    }
    pos
}
