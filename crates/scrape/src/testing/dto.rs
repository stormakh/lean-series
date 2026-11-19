//! The protobuf text format of `io.prometheus.client.MetricFamily` (client_model `metrics.proto`) to binary,
//! for tests (Go `proto.UnmarshalText` + `proto.Marshal`, and `textToProto` / `protoMarshalDelimited`).
//!
//! Every field present in the text is encoded (proto2 presence), in field number order as Go's marshaller does;
//! repeated scalars are not packed (proto2).

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Str,
    Enum,
    Double,
    Uint64,
    Int64,
    Int32,
    Uint32,
    Sint32,
    Sint64,
    Msg(&'static str),
}

fn field(msg: &str, name: &str) -> Option<(u32, Kind)> {
    use Kind::*;
    Some(match (msg, name) {
        ("MetricFamily", "name") => (1, Str),
        ("MetricFamily", "help") => (2, Str),
        ("MetricFamily", "type") => (3, Enum),
        ("MetricFamily", "metric") => (4, Msg("Metric")),
        ("MetricFamily", "unit") => (5, Str),
        ("Metric", "label") => (1, Msg("LabelPair")),
        ("Metric", "gauge") => (2, Msg("Gauge")),
        ("Metric", "counter") => (3, Msg("Counter")),
        ("Metric", "summary") => (4, Msg("Summary")),
        ("Metric", "untyped") => (5, Msg("Untyped")),
        ("Metric", "timestamp_ms") => (6, Int64),
        ("Metric", "histogram") => (7, Msg("Histogram")),
        ("LabelPair", "name") => (1, Str),
        ("LabelPair", "value") => (2, Str),
        ("Gauge", "value") | ("Untyped", "value") => (1, Double),
        ("Counter", "value") => (1, Double),
        ("Counter", "exemplar") => (2, Msg("Exemplar")),
        ("Counter", "created_timestamp") => (3, Msg("Timestamp")),
        ("Quantile", "quantile") => (1, Double),
        ("Quantile", "value") => (2, Double),
        ("Summary", "sample_count") => (1, Uint64),
        ("Summary", "sample_sum") => (2, Double),
        ("Summary", "quantile") => (3, Msg("Quantile")),
        ("Summary", "created_timestamp") => (4, Msg("Timestamp")),
        ("Histogram", "sample_count") => (1, Uint64),
        ("Histogram", "sample_sum") => (2, Double),
        ("Histogram", "bucket") => (3, Msg("Bucket")),
        ("Histogram", "sample_count_float") => (4, Double),
        ("Histogram", "schema") => (5, Sint32),
        ("Histogram", "zero_threshold") => (6, Double),
        ("Histogram", "zero_count") => (7, Uint64),
        ("Histogram", "zero_count_float") => (8, Double),
        ("Histogram", "negative_span") => (9, Msg("BucketSpan")),
        ("Histogram", "negative_delta") => (10, Sint64),
        ("Histogram", "negative_count") => (11, Double),
        ("Histogram", "positive_span") => (12, Msg("BucketSpan")),
        ("Histogram", "positive_delta") => (13, Sint64),
        ("Histogram", "positive_count") => (14, Double),
        ("Histogram", "created_timestamp") => (15, Msg("Timestamp")),
        ("Histogram", "exemplars") => (16, Msg("Exemplar")),
        ("Bucket", "cumulative_count") => (1, Uint64),
        ("Bucket", "upper_bound") => (2, Double),
        ("Bucket", "exemplar") => (3, Msg("Exemplar")),
        ("Bucket", "cumulative_count_float") => (4, Double),
        ("BucketSpan", "offset") => (1, Sint32),
        ("BucketSpan", "length") => (2, Uint32),
        ("Exemplar", "label") => (1, Msg("LabelPair")),
        ("Exemplar", "value") => (2, Double),
        ("Exemplar", "timestamp") => (3, Msg("Timestamp")),
        ("Timestamp", "seconds") => (1, Int64),
        ("Timestamp", "nanos") => (2, Int32),
        _ => return None,
    })
}

fn enum_value(s: &str) -> Result<u64, String> {
    Ok(match s {
        "COUNTER" => 0,
        "GAUGE" => 1,
        "SUMMARY" => 2,
        "UNTYPED" => 3,
        "HISTOGRAM" => 4,
        "GAUGE_HISTOGRAM" => 5,
        _ => s.parse::<u64>().map_err(|_| format!("unknown enum value {s}"))?,
    })
}

struct Lexer<'a> {
    s: &'a [u8],
    i: usize,
}

#[derive(Debug, PartialEq)]
enum Tok {
    Ident(String),
    Str(Vec<u8>),
    Num(String),
    Punct(u8),
    End,
}

impl Lexer<'_> {
    fn skip(&mut self) {
        while self.i < self.s.len() {
            let c = self.s[self.i];
            if c.is_ascii_whitespace() || c == b',' || c == b';' {
                self.i += 1;
            } else if c == b'#' {
                while self.i < self.s.len() && self.s[self.i] != b'\n' {
                    self.i += 1;
                }
            } else {
                break;
            }
        }
    }
    fn next(&mut self) -> Result<Tok, String> {
        self.skip();
        if self.i >= self.s.len() {
            return Ok(Tok::End);
        }
        let c = self.s[self.i];
        if c == b'"' || c == b'\'' {
            self.i += 1;
            let mut out = Vec::new();
            while self.i < self.s.len() && self.s[self.i] != c {
                let b = self.s[self.i];
                if b == b'\\' && self.i + 1 < self.s.len() {
                    self.i += 1;
                    let e = self.s[self.i];
                    match e {
                        b'n' => out.push(b'\n'),
                        b't' => out.push(b'\t'),
                        b'r' => out.push(b'\r'),
                        b'0'..=b'7' => {
                            let mut v = 0u32;
                            let mut n = 0;
                            while n < 3 && self.i < self.s.len() && (b'0'..=b'7').contains(&self.s[self.i]) {
                                v = v * 8 + (self.s[self.i] - b'0') as u32;
                                self.i += 1;
                                n += 1;
                            }
                            out.push(v as u8);
                            continue;
                        }
                        b'x' => {
                            let h = std::str::from_utf8(&self.s[self.i + 1..self.i + 3]).map_err(|e| e.to_string())?;
                            out.push(u8::from_str_radix(h, 16).map_err(|e| e.to_string())?);
                            self.i += 3;
                            continue;
                        }
                        other => out.push(other),
                    }
                    self.i += 1;
                } else {
                    out.push(b);
                    self.i += 1;
                }
            }
            self.i += 1;
            // Adjacent strings concatenate.
            self.skip();
            if self.i < self.s.len() && (self.s[self.i] == b'"' || self.s[self.i] == b'\'') {
                if let Tok::Str(rest) = self.next()? {
                    out.extend(rest);
                }
            }
            return Ok(Tok::Str(out));
        }
        if c.is_ascii_alphabetic() || c == b'_' {
            let st = self.i;
            while self.i < self.s.len() && (self.s[self.i].is_ascii_alphanumeric() || self.s[self.i] == b'_') {
                self.i += 1;
            }
            return Ok(Tok::Ident(String::from_utf8_lossy(&self.s[st..self.i]).into_owned()));
        }
        if c.is_ascii_digit() || c == b'-' || c == b'+' || c == b'.' {
            let st = self.i;
            self.i += 1;
            while self.i < self.s.len() {
                let d = self.s[self.i];
                if d.is_ascii_alphanumeric() || d == b'.' || d == b'+' || d == b'-' {
                    // a sign only after an exponent marker
                    if (d == b'+' || d == b'-') && !matches!(self.s[self.i - 1], b'e' | b'E') {
                        break;
                    }
                    self.i += 1;
                } else {
                    break;
                }
            }
            return Ok(Tok::Num(String::from_utf8_lossy(&self.s[st..self.i]).into_owned()));
        }
        self.i += 1;
        Ok(Tok::Punct(c))
    }
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn parse_f64(s: &str) -> Result<f64, String> {
    let l = s.to_ascii_lowercase();
    match l.trim_end_matches('f') {
        "inf" | "+inf" | "infinity" => return Ok(f64::INFINITY),
        "-inf" | "-infinity" => return Ok(f64::NEG_INFINITY),
        "nan" => return Ok(f64::NAN),
        _ => {}
    }
    s.parse::<f64>().map_err(|e| format!("bad float {s}: {e}"))
}

fn parse_i64(s: &str) -> Result<i64, String> {
    s.parse::<i64>().or_else(|_| s.parse::<f64>().map(|f| f as i64)).map_err(|e| format!("bad int {s}: {e}"))
}

/// Parses the fields of one message until `close` (or the end) and returns its binary encoding.
fn parse_msg(lx: &mut Lexer<'_>, msg: &'static str, close: Option<u8>) -> Result<Vec<u8>, String> {
    // (field number, order, encoded bytes)
    let mut fields: Vec<(u32, usize, Vec<u8>)> = Vec::new();
    loop {
        let t = lx.next()?;
        let name = match t {
            Tok::End if close.is_none() => break,
            Tok::Punct(c) if Some(c) == close => break,
            Tok::Ident(n) => n,
            other => return Err(format!("unexpected token {other:?} in {msg}")),
        };
        let (num, kind) = field(msg, &name).ok_or_else(|| format!("unknown field {name} in {msg}"))?;
        let mut t = lx.next()?;
        if t == Tok::Punct(b':') {
            t = lx.next()?;
        }
        let mut enc = Vec::new();
        match kind {
            Kind::Msg(sub) => {
                let close = match t {
                    Tok::Punct(b'<') => b'>',
                    Tok::Punct(b'{') => b'}',
                    other => return Err(format!("expected message for {name}, got {other:?}")),
                };
                let body = parse_msg(lx, sub, Some(close))?;
                put_varint(&mut enc, ((num as u64) << 3) | 2);
                put_varint(&mut enc, body.len() as u64);
                enc.extend(body);
            }
            Kind::Str => {
                let Tok::Str(b) = t else { return Err(format!("expected string for {name}")) };
                put_varint(&mut enc, ((num as u64) << 3) | 2);
                put_varint(&mut enc, b.len() as u64);
                enc.extend(b);
            }
            _ => {
                let s = match t {
                    Tok::Num(s) | Tok::Ident(s) => s,
                    other => return Err(format!("expected scalar for {name}, got {other:?}")),
                };
                match kind {
                    Kind::Double => {
                        put_varint(&mut enc, ((num as u64) << 3) | 1);
                        enc.extend_from_slice(&parse_f64(&s)?.to_le_bytes());
                    }
                    Kind::Enum => {
                        put_varint(&mut enc, (num as u64) << 3);
                        put_varint(&mut enc, enum_value(&s)?);
                    }
                    Kind::Uint64 | Kind::Uint32 => {
                        put_varint(&mut enc, (num as u64) << 3);
                        put_varint(&mut enc, s.parse::<u64>().map_err(|e| format!("bad uint {s}: {e}"))?);
                    }
                    Kind::Int64 | Kind::Int32 => {
                        put_varint(&mut enc, (num as u64) << 3);
                        put_varint(&mut enc, parse_i64(&s)? as u64);
                    }
                    Kind::Sint32 | Kind::Sint64 => {
                        let v = parse_i64(&s)?;
                        put_varint(&mut enc, (num as u64) << 3);
                        put_varint(&mut enc, ((v << 1) ^ (v >> 63)) as u64);
                    }
                    _ => unreachable!(),
                }
            }
        }
        let order = fields.len();
        fields.push((num, order, enc));
    }
    fields.sort_by_key(|f| (f.0, f.1));
    Ok(fields.into_iter().flat_map(|f| f.2).collect())
}

/// Go `proto.UnmarshalText` + `proto.Marshal` of a `dto.MetricFamily`.
pub fn metric_family_from_text(text: &str) -> Result<Vec<u8>, String> {
    let mut lx = Lexer { s: text.as_bytes(), i: 0 };
    parse_msg(&mut lx, "MetricFamily", None)
}

/// Go `textToProto`: the family as length-delimited protobuf (`encoding=delimited`), appended to `buf`.
pub fn text_to_proto(text: &str, buf: &mut Vec<u8>) -> Result<(), String> {
    let b = metric_family_from_text(text)?;
    put_varint(buf, b.len() as u64);
    buf.extend(b);
    Ok(())
}

/// Like [`text_to_proto`], panicking on errors (Go `protoMarshalDelimited` in tests).
pub fn proto_marshal_delimited(text: &str) -> Vec<u8> {
    let mut b = Vec::new();
    text_to_proto(text, &mut b).unwrap_or_else(|e| panic!("text_to_proto: {e}"));
    b
}
