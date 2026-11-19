//! A port of Go `fmt` (`Sprint`, `Sprintf`, `Sprintln`) and the `strconv` float and quote formatting it uses,
//! over [`Value`].

use std::fmt::Write;

use model::unicode::is_print;

use crate::methods::stringer;
use crate::value::{Kind, StrKind, Type, UintKind, Value};

const LDIGITS: &[u8] = b"0123456789abcdefx";
const UDIGITS: &[u8] = b"0123456789ABCDEFX";

// ---------------------------------------------------------------------------------------------------------------
// strconv

/// Go `strconv.FormatFloat(v, fmt, prec, 64)`.
pub fn format_float(v: f64, fmt: u8, prec: i32) -> String {
    let mut out = String::new();
    append_float(&mut out, v, fmt, prec);
    out
}

/// Go `strconv.AppendFloat(dst, v, fmt, prec, 64)`.
pub fn append_float(dst: &mut String, v: f64, fmt: u8, prec: i32) {
    if v.is_nan() {
        dst.push_str("NaN");
        return;
    }
    if v.is_infinite() {
        dst.push_str(if v > 0.0 { "+Inf" } else { "-Inf" });
        return;
    }
    let bits = v.to_bits();
    let neg = bits >> 63 != 0;
    let mut exp = ((bits >> 52) & 0x7ff) as i64;
    let mut mant = bits & ((1 << 52) - 1);
    if exp == 0 {
        exp += 1;
    } else {
        mant |= 1 << 52;
    }
    exp -= 1023;
    match fmt {
        b'b' => {
            if neg {
                dst.push('-');
            }
            let _ = write!(dst, "{}p", mant);
            let e = exp - 52;
            if e >= 0 {
                dst.push('+');
            }
            let _ = write!(dst, "{}", e);
            return;
        }
        b'x' | b'X' => {
            fmt_x(dst, prec, fmt, neg, mant, exp);
            return;
        }
        _ => {}
    }
    let a = v.abs();
    let shortest = prec < 0;
    match fmt {
        b'f' if !shortest => {
            if neg {
                dst.push('-');
            }
            let _ = write!(dst, "{:.*}", prec as usize, a);
        }
        b'e' | b'E' => {
            let (d, dp) = if shortest { digits(a, None) } else { digits(a, Some(prec as usize + 1)) };
            let p = if shortest { (d.len() as i32 - 1).max(0) } else { prec };
            fmt_e(dst, neg, &d, dp, p, fmt);
        }
        b'f' => {
            let (d, dp) = digits(a, None);
            let p = (d.len() as i32 - dp).max(0);
            fmt_f(dst, neg, &d, dp, p);
        }
        b'g' | b'G' => {
            let mut prec = prec;
            let (d, dp) = if shortest {
                let (d, dp) = digits(a, None);
                prec = d.len() as i32;
                (d, dp)
            } else {
                if prec == 0 {
                    prec = 1;
                }
                digits(a, Some(prec as usize))
            };
            let nd = d.len() as i32;
            let mut eprec = prec;
            if eprec > nd && nd >= dp {
                eprec = nd;
            }
            if shortest {
                eprec = 6;
            }
            let exp = dp - 1;
            if exp < -4 || exp >= eprec {
                if prec > nd {
                    prec = nd;
                }
                fmt_e(dst, neg, &d, dp, prec - 1, fmt + b'e' - b'g');
                return;
            }
            if prec > dp {
                prec = nd;
            }
            fmt_f(dst, neg, &d, dp, (prec - dp).max(0));
        }
        _ => {
            dst.push('%');
            dst.push(fmt as char);
        }
    }
}

/// The decimal digits (without trailing zeros) and decimal point position of `a` (>= 0): shortest when `sig` is
/// `None`, else rounded to `sig` significant digits. Zero has no digits.
fn digits(a: f64, sig: Option<usize>) -> (Vec<u8>, i32) {
    if a == 0.0 {
        return (Vec::new(), 0);
    }
    let s = match sig {
        Some(n) => format!("{:.*e}", n.saturating_sub(1), a),
        None => format!("{:e}", a),
    };
    let (m, e) = s.split_once('e').unwrap_or((&s, "0"));
    let exp: i32 = e.parse().unwrap_or(0);
    let mut d: Vec<u8> = m.bytes().filter(|c| c.is_ascii_digit()).collect();
    while d.last() == Some(&b'0') {
        d.pop();
    }
    (d, exp + 1)
}

fn fmt_e(dst: &mut String, neg: bool, d: &[u8], dp: i32, prec: i32, fmt: u8) {
    if neg {
        dst.push('-');
    }
    dst.push(if d.is_empty() { '0' } else { d[0] as char });
    if prec > 0 {
        dst.push('.');
        let mut i = 1usize;
        let m = d.len().min(prec as usize + 1);
        if i < m {
            for &c in &d[i..m] {
                dst.push(c as char);
            }
            i = m;
        }
        while i <= prec as usize {
            dst.push('0');
            i += 1;
        }
    }
    dst.push(fmt as char);
    let mut exp = dp - 1;
    if d.is_empty() {
        exp = 0;
    }
    if exp < 0 {
        dst.push('-');
        exp = -exp;
    } else {
        dst.push('+');
    }
    if exp < 10 {
        dst.push('0');
    }
    let _ = write!(dst, "{}", exp);
}

fn fmt_f(dst: &mut String, neg: bool, d: &[u8], dp: i32, prec: i32) {
    if neg {
        dst.push('-');
    }
    if dp > 0 {
        let m = d.len().min(dp as usize);
        for &c in &d[..m] {
            dst.push(c as char);
        }
        for _ in m..dp as usize {
            dst.push('0');
        }
    } else {
        dst.push('0');
    }
    if prec > 0 {
        dst.push('.');
        for i in 0..prec {
            let j = dp + i;
            let ch = if j >= 0 && (j as usize) < d.len() { d[j as usize] } else { b'0' };
            dst.push(ch as char);
        }
    }
}

fn fmt_x(dst: &mut String, prec: i32, fmt: u8, neg: bool, mut mant: u64, mut exp: i64) {
    if mant == 0 {
        exp = 0;
    }
    mant <<= 60 - 52;
    while mant != 0 && mant & (1 << 60) == 0 {
        mant <<= 1;
        exp -= 1;
    }
    if (0..15).contains(&prec) {
        let shift = (prec * 4) as u32;
        let extra = (mant << shift) & ((1 << 60) - 1);
        mant >>= 60 - shift;
        if extra | (mant & 1) > 1 << 59 {
            mant += 1;
        }
        mant <<= 60 - shift;
        if mant & (1 << 61) != 0 {
            mant >>= 1;
            exp += 1;
        }
    }
    let hex = if fmt == b'X' { UDIGITS } else { LDIGITS };
    if neg {
        dst.push('-');
    }
    dst.push('0');
    dst.push(fmt as char);
    dst.push((b'0' + ((mant >> 60) & 1) as u8) as char);
    mant <<= 4;
    if prec < 0 && mant != 0 {
        dst.push('.');
        while mant != 0 {
            dst.push(hex[((mant >> 60) & 15) as usize] as char);
            mant <<= 4;
        }
    } else if prec > 0 {
        dst.push('.');
        for _ in 0..prec {
            dst.push(hex[((mant >> 60) & 15) as usize] as char);
            mant <<= 4;
        }
    }
    dst.push(if fmt == b'X' { 'P' } else { 'p' });
    if exp < 0 {
        dst.push('-');
        exp = -exp;
    } else {
        dst.push('+');
    }
    if exp < 10 {
        dst.push('0');
    }
    let _ = write!(dst, "{}", exp);
}

fn append_escaped_rune(dst: &mut String, r: char, quote: char, ascii_only: bool) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    if r == quote || r == '\\' {
        dst.push('\\');
        dst.push(r);
        return;
    }
    if ascii_only {
        if (r as u32) < 0x80 && is_print(r) {
            dst.push(r);
            return;
        }
    } else if is_print(r) {
        dst.push(r);
        return;
    }
    match r {
        '\x07' => dst.push_str("\\a"),
        '\x08' => dst.push_str("\\b"),
        '\x0c' => dst.push_str("\\f"),
        '\n' => dst.push_str("\\n"),
        '\r' => dst.push_str("\\r"),
        '\t' => dst.push_str("\\t"),
        '\x0b' => dst.push_str("\\v"),
        _ => {
            let u = r as u32;
            if u < 0x20 || u == 0x7f {
                dst.push_str("\\x");
                dst.push(HEX[(u >> 4) as usize & 0xF] as char);
                dst.push(HEX[u as usize & 0xF] as char);
            } else if u < 0x10000 {
                dst.push_str("\\u");
                for s in (0..4).rev() {
                    dst.push(HEX[(u >> (s * 4)) as usize & 0xF] as char);
                }
            } else {
                dst.push_str("\\U");
                for s in (0..8).rev() {
                    dst.push(HEX[(u >> (s * 4)) as usize & 0xF] as char);
                }
            }
        }
    }
}

/// Go `strconv.Quote` (or `QuoteToASCII` when `ascii_only`).
pub fn quote_with(s: &str, ascii_only: bool) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for r in s.chars() {
        append_escaped_rune(&mut out, r, '"', ascii_only);
    }
    out.push('"');
    out
}

/// Go `strconv.Quote`.
pub fn quote(s: &str) -> String {
    quote_with(s, false)
}

/// Go `strconv.QuoteRune` (or `QuoteRuneToASCII`).
pub fn quote_rune(r: char, ascii_only: bool) -> String {
    let mut out = String::with_capacity(8);
    out.push('\'');
    append_escaped_rune(&mut out, r, '\'', ascii_only);
    out.push('\'');
    out
}

/// Go `strconv.CanBackquote`.
pub fn can_backquote(s: &str) -> bool {
    s.chars().all(|c| !((c < ' ' && c != '\t') || c == '`' || c == '\u{7f}' || c == '\u{FEFF}'))
}

// ---------------------------------------------------------------------------------------------------------------
// fmt

#[derive(Default, Clone, Copy)]
struct Flags {
    wid_present: bool,
    prec_present: bool,
    minus: bool,
    plus: bool,
    sharp: bool,
    space: bool,
    zero: bool,
    plus_v: bool,
    sharp_v: bool,
}

/// The printer (Go `pp` and `fmt`).
#[derive(Default)]
struct Printer {
    buf: String,
    f: Flags,
    wid: i64,
    prec: i64,
    erroring: bool,
    reordered: bool,
    good_arg_num: bool,
}

/// Go `fmt.Sprint`.
pub fn sprint(args: &[Value]) -> String {
    let mut p = Printer::default();
    p.do_print(args);
    p.buf
}

/// Go `fmt.Sprintln`.
pub fn sprintln(args: &[Value]) -> String {
    let mut p = Printer::default();
    p.do_println(args);
    p.buf
}

/// Go `fmt.Sprintf`.
pub fn sprintf(format: &str, args: &[Value]) -> String {
    let mut p = Printer::default();
    p.do_printf(format, args);
    p.buf
}

/// `fmt.Sprintf("%v", v)` for one value.
pub fn sprint_one(v: &Value) -> String {
    sprint(std::slice::from_ref(v))
}

fn rune_count(s: &str) -> i64 {
    s.chars().count() as i64
}

impl Printer {
    fn clearflags(&mut self) {
        self.f = Flags::default();
        self.wid = 0;
        self.prec = 0;
    }

    fn write_padding(&mut self, n: i64) {
        if n <= 0 {
            return;
        }
        let pad = if self.f.zero && !self.f.minus { '0' } else { ' ' };
        for _ in 0..n {
            self.buf.push(pad);
        }
    }

    fn pad(&mut self, s: &str) {
        if !self.f.wid_present || self.wid == 0 {
            self.buf.push_str(s);
            return;
        }
        let width = self.wid - rune_count(s);
        if !self.f.minus {
            self.write_padding(width);
            self.buf.push_str(s);
        } else {
            self.buf.push_str(s);
            self.write_padding(width);
        }
    }

    fn fmt_boolean(&mut self, v: bool) {
        self.pad(if v { "true" } else { "false" });
    }

    fn fmt_unicode(&mut self, u: u64) {
        let mut prec = 4i64;
        if self.f.prec_present && self.prec > 4 {
            prec = self.prec;
        }
        let mut s = String::new();
        let mut hex = String::new();
        let mut v = u;
        loop {
            hex.insert(0, UDIGITS[(v & 0xF) as usize] as char);
            prec -= 1;
            v >>= 4;
            if v == 0 {
                break;
            }
        }
        while prec > 0 {
            hex.insert(0, '0');
            prec -= 1;
        }
        s.push_str("U+");
        s.push_str(&hex);
        if self.f.sharp && u <= 0x10FFFF
            && let Some(c) = char::from_u32(u as u32)
                && is_print(c) {
                    s.push_str(" '");
                    s.push(c);
                    s.push('\'');
                }
        let old_zero = self.f.zero;
        self.f.zero = false;
        self.pad(&s);
        self.f.zero = old_zero;
    }

    fn fmt_integer(&mut self, mut u: u64, base: u32, is_signed: bool, verb: char, digits: &[u8]) {
        let negative = is_signed && (u as i64) < 0;
        if negative {
            u = (u as i64).wrapping_neg() as u64;
        }
        let mut prec = 0i64;
        if self.f.prec_present {
            prec = self.prec;
            if prec == 0 && u == 0 {
                let old_zero = self.f.zero;
                self.f.zero = false;
                self.write_padding(self.wid);
                self.f.zero = old_zero;
                return;
            }
        } else if self.f.zero && !self.f.minus && self.f.wid_present {
            prec = self.wid;
            if negative || self.f.plus || self.f.space {
                prec -= 1;
            }
        }
        let mut buf: Vec<u8> = Vec::with_capacity(24);
        let b = base as u64;
        loop {
            buf.push(digits[(u % b) as usize]);
            u /= b;
            if u == 0 {
                break;
            }
        }
        while (buf.len() as i64) < prec {
            buf.push(b'0');
        }
        if self.f.sharp {
            match base {
                2 => {
                    buf.push(b'b');
                    buf.push(b'0');
                }
                8 => {
                    if *buf.last().unwrap_or(&b'0') != b'0' {
                        buf.push(b'0');
                    }
                }
                16 => {
                    buf.push(digits[16]);
                    buf.push(b'0');
                }
                _ => {}
            }
        }
        if verb == 'O' {
            buf.push(b'o');
            buf.push(b'0');
        }
        if negative {
            buf.push(b'-');
        } else if self.f.plus {
            buf.push(b'+');
        } else if self.f.space {
            buf.push(b' ');
        }
        buf.reverse();
        let s = String::from_utf8(buf).unwrap_or_default();
        let old_zero = self.f.zero;
        self.f.zero = false;
        self.pad(&s);
        self.f.zero = old_zero;
    }

    fn truncate_string<'a>(&self, s: &'a str) -> &'a str {
        if self.f.prec_present {
            let mut n = self.prec;
            for (i, _) in s.char_indices() {
                n -= 1;
                if n < 0 {
                    return &s[..i];
                }
            }
        }
        s
    }

    fn fmt_s(&mut self, s: &str) {
        let s = self.truncate_string(s).to_string();
        self.pad(&s);
    }

    fn fmt_sbx(&mut self, s: &[u8], digits: &[u8]) {
        let mut length = s.len() as i64;
        if self.f.prec_present && self.prec < length {
            length = self.prec;
        }
        let mut width = 2 * length;
        if width > 0 {
            if self.f.space {
                if self.f.sharp {
                    width *= 2;
                }
                width += length - 1;
            } else if self.f.sharp {
                width += 2;
            }
        } else {
            if self.f.wid_present {
                self.write_padding(self.wid);
            }
            return;
        }
        if self.f.wid_present && self.wid > width && !self.f.minus {
            self.write_padding(self.wid - width);
        }
        if self.f.sharp {
            self.buf.push('0');
            self.buf.push(digits[16] as char);
        }
        for i in 0..length as usize {
            if self.f.space && i > 0 {
                self.buf.push(' ');
                if self.f.sharp {
                    self.buf.push('0');
                    self.buf.push(digits[16] as char);
                }
            }
            let c = s[i];
            self.buf.push(digits[(c >> 4) as usize] as char);
            self.buf.push(digits[(c & 0xF) as usize] as char);
        }
        if self.f.wid_present && self.wid > width && self.f.minus {
            self.write_padding(self.wid - width);
        }
    }

    fn fmt_q(&mut self, s: &str) {
        let s = self.truncate_string(s).to_string();
        if self.f.sharp && can_backquote(&s) {
            self.pad(&format!("`{}`", s));
            return;
        }
        let q = quote_with(&s, self.f.plus);
        self.pad(&q);
    }

    fn fmt_c(&mut self, c: u64) {
        let r = if c > 0x10FFFF { '\u{FFFD}' } else { char::from_u32(c as u32).unwrap_or('\u{FFFD}') };
        let mut s = String::new();
        s.push(r);
        self.pad(&s);
    }

    fn fmt_qc(&mut self, c: u64) {
        let r = if c > 0x10FFFF { '\u{FFFD}' } else { char::from_u32(c as u32).unwrap_or('\u{FFFD}') };
        let q = quote_rune(r, self.f.plus);
        self.pad(&q);
    }

    fn fmt_float_inner(&mut self, v: f64, verb: u8, mut prec: i32) {
        if self.f.prec_present {
            prec = self.prec as i32;
        }
        let mut num = String::from("+");
        append_float(&mut num, v, verb, prec);
        let mut num: Vec<u8> = num.into_bytes();
        if num[1] == b'-' || num[1] == b'+' {
            num.remove(0);
        } else {
            num[0] = b'+';
        }
        if self.f.space && num[0] == b'+' && !self.f.plus {
            num[0] = b' ';
        }
        if num[1] == b'I' || num[1] == b'N' {
            let old_zero = self.f.zero;
            self.f.zero = false;
            if num[1] == b'N' && !self.f.space && !self.f.plus {
                num.remove(0);
            }
            let s = String::from_utf8(num).unwrap_or_default();
            self.pad(&s);
            self.f.zero = old_zero;
            return;
        }
        if self.f.sharp && verb != b'b' {
            let mut digits: i64 = 0;
            if matches!(verb, b'v' | b'g' | b'G' | b'x') {
                digits = prec as i64;
                if digits == -1 {
                    digits = 6;
                }
            }
            let mut tail: Vec<u8> = Vec::new();
            let mut has_decimal_point = false;
            let mut saw_nonzero = false;
            let mut i = 1;
            while i < num.len() {
                match num[i] {
                    b'.' => has_decimal_point = true,
                    b'p' | b'P' => {
                        tail.extend_from_slice(&num[i..]);
                        num.truncate(i);
                        break;
                    }
                    b'e' | b'E' if verb != b'x' && verb != b'X' => {
                        tail.extend_from_slice(&num[i..]);
                        num.truncate(i);
                        break;
                    }
                    c => {
                        if c != b'0' {
                            saw_nonzero = true;
                        }
                        if saw_nonzero {
                            digits -= 1;
                        }
                    }
                }
                i += 1;
            }
            if !has_decimal_point {
                if num.len() == 2 && num[1] == b'0' {
                    digits -= 1;
                }
                num.push(b'.');
            }
            while digits > 0 {
                num.push(b'0');
                digits -= 1;
            }
            num.extend_from_slice(&tail);
        }
        let s = String::from_utf8(num).unwrap_or_default();
        if self.f.plus || !s.starts_with('+') {
            if self.f.zero && !self.f.minus && self.f.wid_present && self.wid > s.len() as i64 {
                self.buf.push_str(&s[..1]);
                self.write_padding(self.wid - s.len() as i64);
                self.buf.push_str(&s[1..]);
                return;
            }
            self.pad(&s);
            return;
        }
        self.pad(&s[1..]);
    }

    // pp methods

    fn bad_verb(&mut self, verb: char, arg: Option<&Value>) {
        self.erroring = true;
        self.buf.push_str("%!");
        self.buf.push(verb);
        self.buf.push('(');
        match arg {
            Some(v) if v.is_valid() => {
                self.buf.push_str(&v.type_name());
                self.buf.push('=');
                self.print_arg(v, 'v');
            }
            _ => self.buf.push_str("<nil>"),
        }
        self.buf.push(')');
        self.erroring = false;
    }

    fn fmt_bool(&mut self, v: bool, verb: char, arg: &Value) {
        match verb {
            't' | 'v' => self.fmt_boolean(v),
            _ => self.bad_verb(verb, Some(arg)),
        }
    }

    fn fmt0x64(&mut self, v: u64, leading0x: bool) {
        let sharp = self.f.sharp;
        self.f.sharp = leading0x;
        self.fmt_integer(v, 16, false, 'v', LDIGITS);
        self.f.sharp = sharp;
    }

    fn fmt_int(&mut self, v: u64, is_signed: bool, verb: char, arg: &Value) {
        match verb {
            'v' => {
                if self.f.sharp_v && !is_signed {
                    self.fmt0x64(v, true);
                } else {
                    self.fmt_integer(v, 10, is_signed, verb, LDIGITS);
                }
            }
            'd' => self.fmt_integer(v, 10, is_signed, verb, LDIGITS),
            'b' => self.fmt_integer(v, 2, is_signed, verb, LDIGITS),
            'o' | 'O' => self.fmt_integer(v, 8, is_signed, verb, LDIGITS),
            'x' => self.fmt_integer(v, 16, is_signed, verb, LDIGITS),
            'X' => self.fmt_integer(v, 16, is_signed, verb, UDIGITS),
            'c' => self.fmt_c(v),
            'q' => self.fmt_qc(v),
            'U' => self.fmt_unicode(v),
            _ => self.bad_verb(verb, Some(arg)),
        }
    }

    fn fmt_float(&mut self, v: f64, verb: char, arg: &Value) {
        match verb {
            'v' => self.fmt_float_inner(v, b'g', -1),
            'b' | 'g' | 'G' | 'x' | 'X' => self.fmt_float_inner(v, verb as u8, -1),
            'f' | 'e' | 'E' => self.fmt_float_inner(v, verb as u8, 6),
            'F' => self.fmt_float_inner(v, b'f', 6),
            _ => self.bad_verb(verb, Some(arg)),
        }
    }

    fn fmt_complex(&mut self, re: f64, im: f64, verb: char, arg: &Value) {
        match verb {
            'v' | 'b' | 'g' | 'G' | 'x' | 'X' | 'f' | 'F' | 'e' | 'E' => {
                let old_plus = self.f.plus;
                self.buf.push('(');
                self.fmt_float(re, verb, arg);
                self.f.plus = true;
                self.fmt_float(im, verb, arg);
                self.buf.push_str("i)");
                self.f.plus = old_plus;
            }
            _ => self.bad_verb(verb, Some(arg)),
        }
    }

    fn fmt_string(&mut self, v: &str, verb: char, arg: &Value) {
        match verb {
            'v' => {
                if self.f.sharp_v {
                    self.fmt_q(v);
                } else {
                    self.fmt_s(v);
                }
            }
            's' => self.fmt_s(v),
            'x' => self.fmt_sbx(v.as_bytes(), LDIGITS),
            'X' => self.fmt_sbx(v.as_bytes(), UDIGITS),
            'q' => self.fmt_q(v),
            _ => self.bad_verb(verb, Some(arg)),
        }
    }

    fn fmt_bytes(&mut self, v: &[u8], verb: char, type_string: &str, arg: &Value) {
        match verb {
            'v' | 'd' => {
                if self.f.sharp_v {
                    self.buf.push_str(type_string);
                    self.buf.push('{');
                    for (i, &c) in v.iter().enumerate() {
                        if i > 0 {
                            self.buf.push_str(", ");
                        }
                        self.fmt0x64(c as u64, true);
                    }
                    self.buf.push('}');
                } else {
                    self.buf.push('[');
                    for (i, &c) in v.iter().enumerate() {
                        if i > 0 {
                            self.buf.push(' ');
                        }
                        self.fmt_integer(c as u64, 10, false, verb, LDIGITS);
                    }
                    self.buf.push(']');
                }
            }
            's' => {
                let s = String::from_utf8_lossy(v).into_owned();
                self.fmt_s(&s);
            }
            'x' => self.fmt_sbx(v, LDIGITS),
            'X' => self.fmt_sbx(v, UDIGITS),
            'q' => {
                let s = String::from_utf8_lossy(v).into_owned();
                self.fmt_q(&s);
            }
            _ => self.bad_verb(verb, Some(arg)),
        }
    }

    /// A pseudo address for pointer-like values (Go prints the real address, which is not stable either).
    fn fmt_pointer(&mut self, value: &Value, verb: char) {
        let u: u64 = match value {
            Value::Nil(t) if matches!(t.kind(), Kind::Ptr | Kind::Map | Kind::Slice | Kind::Func) => 0,
            Value::Ptr(p) => std::sync::Arc::as_ptr(p) as usize as u64,
            Value::Map(m) => std::sync::Arc::as_ptr(m) as usize as u64,
            Value::Slice(s) => std::sync::Arc::as_ptr(s) as usize as u64,
            Value::Func(f) => std::sync::Arc::as_ptr(&f.f) as *const () as usize as u64,
            _ => {
                self.bad_verb(verb, Some(value));
                return;
            }
        };
        match verb {
            'v' => {
                if self.f.sharp_v {
                    self.buf.push('(');
                    self.buf.push_str(&value.type_name());
                    self.buf.push_str(")(");
                    if u == 0 {
                        self.buf.push_str("nil");
                    } else {
                        self.fmt0x64(u, true);
                    }
                    self.buf.push(')');
                } else if u == 0 {
                    self.pad("<nil>");
                } else {
                    let sharp = !self.f.sharp;
                    self.fmt0x64(u, sharp);
                }
            }
            'p' => {
                let sharp = !self.f.sharp;
                self.fmt0x64(u, sharp);
            }
            'b' | 'o' | 'd' | 'x' | 'X' => self.fmt_int(u, false, verb, value),
            _ => self.bad_verb(verb, Some(value)),
        }
    }

    /// Go `handleMethods`: `String()` of Stringers for the string verbs.
    fn handle_methods(&mut self, arg: &Value, verb: char) -> bool {
        if self.erroring {
            return false;
        }
        if verb == 'w' {
            self.bad_verb(verb, Some(arg));
            return true;
        }
        if self.f.sharp_v {
            return false;
        }
        if matches!(verb, 'v' | 's' | 'x' | 'X' | 'q')
            && let Some(s) = stringer(arg) {
                self.fmt_string(&s, verb, arg);
                return true;
            }
        false
    }

    fn print_arg(&mut self, arg: &Value, verb: char) {
        if let Value::Iface(inner) = arg {
            return self.print_arg(inner, verb);
        }
        if !arg.is_valid() {
            match verb {
                'T' | 'v' => self.pad("<nil>"),
                _ => self.bad_verb(verb, None),
            }
            return;
        }
        match verb {
            'T' => {
                let t = arg.type_name();
                self.fmt_s(&t);
                return;
            }
            'p' => {
                self.fmt_pointer(arg, 'p');
                return;
            }
            _ => {}
        }
        match arg {
            Value::Bool(b) => self.fmt_bool(*b, verb, arg),
            Value::Float(f) => self.fmt_float(*f, verb, arg),
            Value::Complex(re, im) => self.fmt_complex(*re, *im, verb, arg),
            Value::Int(i, k) if !matches!(k, crate::value::IntKind::Duration | crate::value::IntKind::Month | crate::value::IntKind::Weekday) => {
                self.fmt_int(*i as u64, true, verb, arg)
            }
            Value::Uint(u, _) => self.fmt_int(*u, false, verb, arg),
            Value::String(s, StrKind::String) => self.fmt_string(s, verb, arg),
            Value::Slice(s) if matches!(s.ty, Type::Slice(ref e, None) if **e == Type::Uint(UintKind::Uint8)) => {
                let bytes: Vec<u8> = s.items.iter().map(|v| if let Value::Uint(u, _) = v { *u as u8 } else { 0 }).collect();
                self.fmt_bytes(&bytes, verb, "[]byte", arg);
            }
            _ => {
                if !self.handle_methods(arg, verb) {
                    self.print_value(arg, verb, 0);
                }
            }
        }
    }

    fn print_value(&mut self, value: &Value, verb: char, depth: usize) {
        if depth > 0 && value.is_valid() && self.handle_methods(value, verb) {
            return;
        }
        match value {
            Value::Invalid => {
                if depth == 0 {
                    self.buf.push_str("<invalid reflect.Value>");
                } else if verb == 'v' {
                    self.buf.push_str("<nil>");
                } else {
                    self.bad_verb(verb, None);
                }
            }
            Value::Bool(b) => self.fmt_bool(*b, verb, value),
            Value::Int(i, _) => self.fmt_int(*i as u64, true, verb, value),
            Value::Uint(u, _) => self.fmt_int(*u, false, verb, value),
            Value::Float(f) => self.fmt_float(*f, verb, value),
            Value::Complex(re, im) => self.fmt_complex(*re, *im, verb, value),
            Value::String(s, _) => self.fmt_string(s, verb, value),
            Value::Nil(t) => match t.kind() {
                Kind::Map => {
                    if self.f.sharp_v {
                        self.buf.push_str(&t.to_string());
                        self.buf.push_str("(nil)");
                    } else {
                        self.buf.push_str("map[]");
                    }
                }
                Kind::Slice => {
                    if self.f.sharp_v {
                        self.buf.push_str(&t.to_string());
                        self.buf.push_str("(nil)");
                    } else {
                        self.buf.push_str("[]");
                    }
                }
                Kind::Interface => {
                    if self.f.sharp_v {
                        self.buf.push_str(&t.to_string());
                        self.buf.push_str("(nil)");
                    } else {
                        self.buf.push_str("<nil>");
                    }
                }
                _ => self.fmt_pointer(value, verb),
            },
            Value::Map(m) => {
                if self.f.sharp_v {
                    self.buf.push_str(&m.ty.to_string());
                    self.buf.push('{');
                } else {
                    self.buf.push_str("map[");
                }
                let elem = m.ty.elem().cloned().unwrap_or(Type::Interface);
                for (i, (k, v)) in m.entries.iter().enumerate() {
                    if i > 0 {
                        if self.f.sharp_v {
                            self.buf.push_str(", ");
                        } else {
                            self.buf.push(' ');
                        }
                    }
                    self.print_value(&Value::string(k.as_str()), verb, depth + 1);
                    self.buf.push(':');
                    let v = crate::value::from_slot(v, &elem);
                    self.print_iface_value(&v, verb, depth + 1);
                }
                self.buf.push(if self.f.sharp_v { '}' } else { ']' });
            }
            Value::Struct(s) => {
                if self.f.sharp_v {
                    self.buf.push_str(&s.ty.name);
                }
                self.buf.push('{');
                for (i, ((name, ty), v)) in s.ty.fields.iter().zip(s.fields.iter()).enumerate() {
                    if i > 0 {
                        if self.f.sharp_v {
                            self.buf.push_str(", ");
                        } else {
                            self.buf.push(' ');
                        }
                    }
                    if self.f.plus_v || self.f.sharp_v {
                        self.buf.push_str(name);
                        self.buf.push(':');
                    }
                    let v = crate::value::from_slot(v, ty);
                    self.print_iface_value(&v, verb, depth + 1);
                }
                self.buf.push('}');
            }
            Value::Slice(s) => {
                if matches!(verb, 's' | 'q' | 'x' | 'X')
                    && let Some(Type::Uint(UintKind::Uint8)) = s.ty.elem() {
                        let bytes: Vec<u8> =
                            s.items.iter().map(|v| if let Value::Uint(u, _) = v { *u as u8 } else { 0 }).collect();
                        self.fmt_bytes(&bytes, verb, &s.ty.to_string(), value);
                        return;
                    }
                let elem = s.ty.elem().cloned().unwrap_or(Type::Interface);
                if self.f.sharp_v {
                    self.buf.push_str(&s.ty.to_string());
                    self.buf.push('{');
                    for (i, v) in s.items.iter().enumerate() {
                        if i > 0 {
                            self.buf.push_str(", ");
                        }
                        let v = crate::value::from_slot(v, &elem);
                        self.print_iface_value(&v, verb, depth + 1);
                    }
                    self.buf.push('}');
                } else {
                    self.buf.push('[');
                    for (i, v) in s.items.iter().enumerate() {
                        if i > 0 {
                            self.buf.push(' ');
                        }
                        let v = crate::value::from_slot(v, &elem);
                        self.print_iface_value(&v, verb, depth + 1);
                    }
                    self.buf.push(']');
                }
            }
            Value::Ptr(inner) => {
                if depth == 0 && matches!(inner.kind(), Kind::Slice | Kind::Struct | Kind::Map) {
                    self.buf.push('&');
                    self.print_value(inner, verb, depth + 1);
                    return;
                }
                self.fmt_pointer(value, verb);
            }
            Value::Func(_) => self.fmt_pointer(value, verb),
            Value::Time(t) => {
                // A time.Time struct printed without its String method (only for non-string verbs).
                let s = t.string();
                self.fmt_s(&s);
            }
            Value::Histogram(h) => {
                let s = h.string();
                self.fmt_s(&s);
            }
            Value::Iface(inner) => self.print_value(inner, verb, depth + 1),
        }
    }

    /// Prints a value read from an interface slot: a nil interface prints `<nil>`.
    fn print_iface_value(&mut self, v: &Value, verb: char, depth: usize) {
        match v {
            Value::Nil(t) if t.kind() == Kind::Interface => {
                if self.f.sharp_v {
                    self.buf.push_str("interface {}(nil)");
                } else {
                    self.buf.push_str("<nil>");
                }
            }
            _ => self.print_value(v, verb, depth),
        }
    }

    fn do_print(&mut self, a: &[Value]) {
        let mut prev_string = false;
        for (arg_num, arg) in a.iter().enumerate() {
            let is_string = arg.is_valid() && arg.kind() == Kind::String;
            if arg_num > 0 && !is_string && !prev_string {
                self.buf.push(' ');
            }
            self.print_arg(arg, 'v');
            prev_string = is_string;
        }
    }

    fn do_println(&mut self, a: &[Value]) {
        for (arg_num, arg) in a.iter().enumerate() {
            if arg_num > 0 {
                self.buf.push(' ');
            }
            self.print_arg(arg, 'v');
        }
        self.buf.push('\n');
    }

    fn arg_number(&mut self, arg_num: usize, format: &[u8], i: usize, num_args: usize) -> (usize, usize, bool) {
        if format.len() <= i || format[i] != b'[' {
            return (arg_num, i, false);
        }
        self.reordered = true;
        let (index, wid, ok) = parse_arg_number(&format[i..]);
        if ok && 0 <= index && (index as usize) < num_args {
            return (index as usize, i + wid, true);
        }
        self.good_arg_num = false;
        (arg_num, i + wid, ok)
    }

    fn do_printf(&mut self, format: &str, a: &[Value]) {
        let fb = format.as_bytes();
        let end = fb.len();
        let mut arg_num = 0usize;
        let mut after_index;
        self.reordered = false;
        let mut i = 0usize;
        'format_loop: while i < end {
            self.good_arg_num = true;
            let lasti = i;
            while i < end && fb[i] != b'%' {
                i += 1;
            }
            if i > lasti {
                self.buf.push_str(&format[lasti..i]);
            }
            if i >= end {
                break;
            }
            i += 1;
            self.clearflags();
            'simple: while i < end {
                let c = fb[i];
                match c {
                    b'#' => self.f.sharp = true,
                    b'0' => self.f.zero = true,
                    b'+' => self.f.plus = true,
                    b'-' => self.f.minus = true,
                    b' ' => self.f.space = true,
                    _ => {
                        if c.is_ascii_lowercase() && arg_num < a.len() {
                            if c == b'v' || c == b'w' {
                                self.f.sharp_v = self.f.sharp;
                                self.f.sharp = false;
                                self.f.plus_v = self.f.plus;
                                self.f.plus = false;
                            }
                            self.print_arg(&a[arg_num], c as char);
                            arg_num += 1;
                            i += 1;
                            continue 'format_loop;
                        }
                        break 'simple;
                    }
                }
                i += 1;
            }
            (arg_num, i, after_index) = self.arg_number(arg_num, fb, i, a.len());
            if i < end && fb[i] == b'*' {
                i += 1;
                let (w, ok, n) = int_from_arg(a, arg_num);
                self.wid = w;
                self.f.wid_present = ok;
                arg_num = n;
                if !ok {
                    self.buf.push_str("%!(BADWIDTH)");
                }
                if self.wid < 0 {
                    self.wid = -self.wid;
                    self.f.minus = true;
                    self.f.zero = false;
                }
                after_index = false;
            } else {
                let (w, ok, ni) = parsenum(fb, i, end);
                self.wid = w;
                self.f.wid_present = ok;
                i = ni;
                if after_index && self.f.wid_present {
                    self.good_arg_num = false;
                }
            }
            if i + 1 < end && fb[i] == b'.' {
                i += 1;
                if after_index {
                    self.good_arg_num = false;
                }
                (arg_num, i, after_index) = self.arg_number(arg_num, fb, i, a.len());
                if i < end && fb[i] == b'*' {
                    i += 1;
                    let (p, ok, n) = int_from_arg(a, arg_num);
                    self.prec = p;
                    self.f.prec_present = ok;
                    arg_num = n;
                    if self.prec < 0 {
                        self.prec = 0;
                        self.f.prec_present = false;
                    }
                    if !self.f.prec_present {
                        self.buf.push_str("%!(BADPREC)");
                    }
                    after_index = false;
                } else {
                    let (p, ok, ni) = parsenum(fb, i, end);
                    self.prec = p;
                    self.f.prec_present = ok;
                    i = ni;
                    if !self.f.prec_present {
                        self.prec = 0;
                        self.f.prec_present = true;
                    }
                }
            }
            if !after_index {
                (arg_num, i, after_index) = self.arg_number(arg_num, fb, i, a.len());
            }
            let _ = after_index;
            if i >= end {
                self.buf.push_str("%!(NOVERB)");
                break;
            }
            let verb = format[i..].chars().next().unwrap_or('\u{FFFD}');
            i += verb.len_utf8();
            if verb == '%' {
                self.buf.push('%');
            } else if !self.good_arg_num {
                self.buf.push_str("%!");
                self.buf.push(verb);
                self.buf.push_str("(BADINDEX)");
            } else if arg_num >= a.len() {
                self.buf.push_str("%!");
                self.buf.push(verb);
                self.buf.push_str("(MISSING)");
            } else {
                if verb == 'v' || verb == 'w' {
                    self.f.sharp_v = self.f.sharp;
                    self.f.sharp = false;
                    self.f.plus_v = self.f.plus;
                    self.f.plus = false;
                }
                self.print_arg(&a[arg_num], verb);
                arg_num += 1;
            }
        }
        if !self.reordered && arg_num < a.len() {
            self.clearflags();
            self.buf.push_str("%!(EXTRA ");
            for (i, arg) in a[arg_num..].iter().enumerate() {
                if i > 0 {
                    self.buf.push_str(", ");
                }
                if !arg.is_valid() {
                    self.buf.push_str("<nil>");
                } else {
                    self.buf.push_str(&arg.type_name());
                    self.buf.push('=');
                    self.print_arg(arg, 'v');
                }
            }
            self.buf.push(')');
        }
    }
}

fn too_large(x: i64) -> bool {
    !(-1_000_000..=1_000_000).contains(&x)
}

fn parsenum(s: &[u8], start: usize, end: usize) -> (i64, bool, usize) {
    if start >= end {
        return (0, false, end);
    }
    let mut num = 0i64;
    let mut isnum = false;
    let mut newi = start;
    while newi < end && s[newi].is_ascii_digit() {
        if too_large(num) {
            return (0, false, end);
        }
        num = num * 10 + (s[newi] - b'0') as i64;
        isnum = true;
        newi += 1;
    }
    (num, isnum, newi)
}

fn parse_arg_number(format: &[u8]) -> (i64, usize, bool) {
    if format.len() < 3 {
        return (0, 1, false);
    }
    for i in 1..format.len() {
        if format[i] == b']' {
            let (width, ok, newi) = parsenum(format, 1, i);
            if !ok || newi != i {
                return (0, i + 1, false);
            }
            return (width - 1, i + 1, true);
        }
    }
    (0, 1, false)
}

fn int_from_arg(a: &[Value], arg_num: usize) -> (i64, bool, usize) {
    let mut new_arg_num = arg_num;
    let mut num = 0i64;
    let mut is_int = false;
    if arg_num < a.len() {
        match &a[arg_num] {
            Value::Int(n, _) => {
                num = *n;
                is_int = true;
            }
            Value::Uint(n, _)
                if (*n as i64) >= 0 => {
                    num = *n as i64;
                    is_int = true;
                }
            _ => {}
        }
        new_arg_num = arg_num + 1;
        if too_large(num) {
            num = 0;
            is_int = false;
        }
    }
    (num, is_int, new_arg_num)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floats() {
        assert_eq!(format_float(1353755652.0, b'g', -1), "1.353755652e+09");
        assert_eq!(format_float(1234567.0, b'g', 4), "1.235e+06");
        assert_eq!(format_float(0.12, b'g', 4), "0.12");
        assert_eq!(format_float(1e10, b'g', 4), "1e+10");
        assert_eq!(format_float(0.0, b'g', 4), "0");
        assert_eq!(format_float(100.0, b'g', -1), "100");
        assert_eq!(format_float(1e21, b'g', -1), "1e+21");
        assert_eq!(format_float(0.0001, b'g', -1), "0.0001");
        assert_eq!(format_float(0.00001, b'g', -1), "1e-05");
        assert_eq!(format_float(3720.01, b'f', 2), "3720.01");
        assert_eq!(format_float(1.0, b'e', 6), "1.000000e+00");
        assert_eq!(format_float(1.0, b'b', -1), "4503599627370496p-52");
        assert_eq!(format_float(1.0, b'x', -1), "0x1p+00");
    }

    #[test]
    fn printf() {
        assert_eq!(sprintf("%0.2f", &[Value::Float(3720.01)]), "3720.01");
        assert_eq!(sprintf("%5d|%-5d|%05d", &[Value::int(42), Value::int(42), Value::int(-42)]), "   42|42   |-0042");
        assert_eq!(sprintf("%q %x %X", &[Value::string("a\"b"), Value::string("hi"), Value::int(255)]), "\"a\\\"b\" 6869 FF");
        assert_eq!(sprintf("%d", &[Value::string("x")]), "%!d(string=x)");
        assert_eq!(sprintf("%d %d", &[Value::int(1)]), "1 %!d(MISSING)");
        assert_eq!(sprintf("%d", &[Value::int(1), Value::string("x")]), "1%!(EXTRA string=x)");
        assert_eq!(sprintf("%.4g%%", &[Value::Float(-22.222)]), "-22.22%");
        assert_eq!(sprintf("%v %v", &[Value::Float(f64::NAN), Value::Float(f64::INFINITY)]), "NaN +Inf");
        assert_eq!(sprintf("%T", &[Value::Float(1.0)]), "float64");
        assert_eq!(sprint(&[Value::int(1), Value::int(2), Value::string("a"), Value::int(3)]), "1 2a3");
    }
}
