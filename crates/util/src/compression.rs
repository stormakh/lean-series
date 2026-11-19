//! Port of `util/compression`: encode and decode with none, Snappy (block format) or zstd, with optional reusable
//! buffers.
use std::borrow::Cow;

/// Compression type name (Go `Type`).
pub type Type = &'static str;

pub const NONE: Type = "none";
pub const SNAPPY: Type = "snappy";
/// zstd "speed default" mode, roughly zstd level 3.
pub const ZSTD: Type = "zstd";

const ZSTD_LEVEL: i32 = 3;

/// Go `Types`.
pub fn types() -> [Type; 3] {
    [NONE, SNAPPY, ZSTD]
}

/// Buffer for [`encode`] (Go `EncodeBuffer`).
pub trait EncodeBuffer {
    /// Reusable output memory, or `None` for a fresh allocation each time.
    fn buf(&mut self) -> Option<&mut Vec<u8>>;
}

/// Buffer for [`decode`] (Go `DecodeBuffer`).
pub trait DecodeBuffer {
    fn buf(&mut self) -> Option<&mut Vec<u8>>;
}

/// Go `NewSyncEncodeBuffer`: keeps one output buffer; the result of `encode` is valid until the next call.
#[derive(Debug, Default)]
pub struct SyncEncodeBuffer(Vec<u8>);
/// Go `NewConcurrentEncodeBuffer`: no shared state, so every thread can use its own (it is free to create).
#[derive(Debug, Default, Clone, Copy)]
pub struct ConcurrentEncodeBuffer;
/// Go `NewSyncDecodeBuffer`.
#[derive(Debug, Default)]
pub struct SyncDecodeBuffer(Vec<u8>);
/// Go `NewConcurrentDecodeBuffer`.
#[derive(Debug, Default, Clone, Copy)]
pub struct ConcurrentDecodeBuffer;

impl EncodeBuffer for SyncEncodeBuffer {
    fn buf(&mut self) -> Option<&mut Vec<u8>> {
        Some(&mut self.0)
    }
}
impl EncodeBuffer for ConcurrentEncodeBuffer {
    fn buf(&mut self) -> Option<&mut Vec<u8>> {
        None
    }
}
impl DecodeBuffer for SyncDecodeBuffer {
    fn buf(&mut self) -> Option<&mut Vec<u8>> {
        Some(&mut self.0)
    }
}
impl DecodeBuffer for ConcurrentDecodeBuffer {
    fn buf(&mut self) -> Option<&mut Vec<u8>> {
        None
    }
}

/// Fills `buf` (or a new vector) with `f` and returns it.
fn into_out<'a>(buf: Option<&'a mut Vec<u8>>, n: usize, f: impl FnOnce(&mut Vec<u8>) -> Result<(), String>) -> Result<Cow<'a, [u8]>, String> {
    match buf {
        Some(b) => {
            b.clear();
            b.reserve(n);
            f(b)?;
            Ok(Cow::Borrowed(&b[..]))
        }
        None => {
            let mut v = Vec::with_capacity(n);
            f(&mut v)?;
            Ok(Cow::Owned(v))
        }
    }
}

/// Go `Encode`. Empty input and the none type return `src` as is. zstd needs a buffer, as in Go.
pub fn encode<'a>(t: &str, src: &'a [u8], buf: Option<&'a mut dyn EncodeBuffer>) -> Result<Cow<'a, [u8]>, String> {
    if src.is_empty() || t.is_empty() || t == NONE {
        return Ok(Cow::Borrowed(src));
    }
    if t == SNAPPY {
        let max = snap::raw::max_compress_len(src.len());
        if max == 0 {
            return Err(format!("compression: Snappy can't encode such a large message: {}", src.len()));
        }
        return into_out(buf.and_then(|b| b.buf()), max, |out| {
            out.resize(max, 0);
            let n = snap::raw::Encoder::new().compress(src, out).map_err(|e| e.to_string())?;
            out.truncate(n);
            Ok(())
        });
    }
    if t == ZSTD {
        let Some(b) = buf else { return Err("zstd requested but EncodeBuffer was not provided".to_string()) };
        return into_out(b.buf(), zstd::zstd_safe::compress_bound(src.len()), |out| {
            let mut e = zstd::stream::write::Encoder::new(out, ZSTD_LEVEL).map_err(|e| e.to_string())?;
            e.include_contentsize(true).map_err(|e| e.to_string())?;
            e.set_pledged_src_size(Some(src.len() as u64)).map_err(|e| e.to_string())?;
            std::io::Write::write_all(&mut e, src).map_err(|e| e.to_string())?;
            e.finish().map_err(|e| e.to_string())?;
            Ok(())
        });
    }
    Err(format!("unsupported compression type: {t}"))
}

/// Go `Decode`. Empty input and the none type return `src` as is. zstd needs a buffer, as in Go.
pub fn decode<'a>(t: &str, src: &'a [u8], buf: Option<&'a mut dyn DecodeBuffer>) -> Result<Cow<'a, [u8]>, String> {
    if src.is_empty() || t.is_empty() || t == NONE {
        return Ok(Cow::Borrowed(src));
    }
    if t == SNAPPY {
        let n = snap::raw::decompress_len(src).map_err(snappy_err)?;
        return into_out(buf.and_then(|b| b.buf()), n, |out| {
            out.resize(n, 0);
            let m = snap::raw::Decoder::new().decompress(src, out).map_err(snappy_err)?;
            out.truncate(m);
            Ok(())
        });
    }
    if t == ZSTD {
        let Some(b) = buf else { return Err("zstd requested but DecodeBuffer was not provided".to_string()) };
        return into_out(b.buf(), src.len() * 3, |out| {
            let mut d = zstd::stream::read::Decoder::with_buffer(src).map_err(|e| e.to_string())?;
            std::io::Read::read_to_end(&mut d, out).map_err(|e| e.to_string())?;
            Ok(())
        });
    }
    Err(format!("unsupported compression type: {t}"))
}

/// Go snappy error texts.
fn snappy_err(e: snap::Error) -> String {
    match e {
        snap::Error::TooBig { .. } => "snappy: decoded block is too large".to_string(),
        _ => "snappy: corrupt input".to_string(),
    }
}
