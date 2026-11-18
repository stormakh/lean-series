//! Port of `util/compression` as the WAL uses it: Snappy block format and zstd frames, with reusable buffers.
// TODO(util): move to a shared crate when remote write needs it.

use std::fmt;

use zstd::zstd_safe;

use super::{Error, Result};

/// Go `compression.Type`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Type {
    /// No compression. Go `""` means the same.
    #[default]
    None,
    /// Snappy block format.
    Snappy,
    /// Zstandard "speed" mode (about level 3).
    Zstd,
}

impl Type {
    /// Go `compression.Types()`.
    pub const ALL: [Type; 3] = [Type::None, Type::Snappy, Type::Zstd];

    pub fn as_str(self) -> &'static str {
        match self {
            Type::None => "none",
            Type::Snappy => "snappy",
            Type::Zstd => "zstd",
        }
    }

    /// Parses Go's string form; `""` is `None`.
    pub fn parse(s: &str) -> Option<Type> {
        match s {
            "" | "none" => Some(Type::None),
            "snappy" => Some(Type::Snappy),
            "zstd" => Some(Type::Zstd),
            _ => None,
        }
    }
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Zstandard level that matches klauspost/compress `SpeedDefault` ("roughly equivalent to the default
/// Zstandard mode (level 3)").
const ZSTD_LEVEL: i32 = 3;

/// Go `NewSyncEncodeBuffer`: encoder state and output buffer for one thread. The encoded bytes are valid until
/// the next `encode`.
#[derive(Default)]
pub struct EncodeBuffer {
    snappy: Option<snap::raw::Encoder>,
    zstd: Option<zstd::bulk::Compressor<'static>>,
    buf: Vec<u8>,
}

impl EncodeBuffer {
    pub fn new() -> EncodeBuffer {
        EncodeBuffer::default()
    }

    /// Go `compression.Encode`: returns `src` itself for `None` or empty input, otherwise the encoded bytes in
    /// this buffer.
    pub fn encode<'a>(&'a mut self, t: Type, src: &'a [u8]) -> Result<&'a [u8]> {
        if src.is_empty() || t == Type::None {
            return Ok(src);
        }
        match t {
            Type::Snappy => {
                let max = snap::raw::max_compress_len(src.len());
                if max == 0 {
                    return Err(Error::Msg(format!(
                        "compression: Snappy can't encode such a large message: {}",
                        src.len()
                    )));
                }
                if self.buf.len() < max {
                    self.buf.resize(max, 0);
                }
                let enc = self.snappy.get_or_insert_with(snap::raw::Encoder::new);
                let n = enc
                    .compress(src, &mut self.buf)
                    .map_err(|e| Error::Msg(format!("snappy: {e}")))?;
                Ok(&self.buf[..n])
            }
            Type::Zstd => {
                if self.zstd.is_none() {
                    self.zstd = Some(zstd::bulk::Compressor::new(ZSTD_LEVEL)?);
                }
                let c = self.zstd.as_mut().unwrap();
                self.buf.clear();
                self.buf.reserve(zstd_safe::compress_bound(src.len()));
                c.compress_to_buffer(src, &mut self.buf)?;
                Ok(&self.buf)
            }
            Type::None => unreachable!(),
        }
    }
}

/// Go `NewSyncDecodeBuffer`: decoder state and output buffer for one thread. The decoded bytes are valid until
/// the next `decode`.
#[derive(Default)]
pub struct DecodeBuffer {
    snappy: Option<snap::raw::Decoder>,
    zstd: Option<zstd_safe::DCtx<'static>>,
    buf: Vec<u8>,
}

impl DecodeBuffer {
    pub fn new() -> DecodeBuffer {
        DecodeBuffer::default()
    }

    /// The bytes of the last successful `decode_into_self`.
    pub fn get(&self) -> &[u8] {
        &self.buf
    }

    /// Go `compression.Decode`: returns `src` itself for `None` or empty input, otherwise the decoded bytes in
    /// this buffer.
    pub fn decode<'a>(&'a mut self, t: Type, src: &'a [u8]) -> Result<&'a [u8]> {
        if src.is_empty() || t == Type::None {
            return Ok(src);
        }
        self.decode_into_self(t, src)?;
        Ok(&self.buf)
    }

    /// Decodes `src` (compressed with `t`, not `None`, not empty) into this buffer.
    pub(crate) fn decode_into_self(&mut self, t: Type, src: &[u8]) -> Result<()> {
        match t {
            Type::Snappy => {
                let n = snap::raw::decompress_len(src).map_err(snappy_err)?;
                if self.buf.len() < n {
                    self.buf.resize(n, 0);
                }
                let dec = self.snappy.get_or_insert_with(snap::raw::Decoder::new);
                let n = dec
                    .decompress(src, &mut self.buf[..n])
                    .map_err(snappy_err)?;
                self.buf.truncate(n);
                Ok(())
            }
            Type::Zstd => {
                let dctx = self.zstd.get_or_insert_with(zstd_safe::DCtx::create);
                dctx.reset(zstd_safe::ResetDirective::SessionOnly)
                    .map_err(zstd_err)?;
                self.buf.clear();
                let hint = match zstd_safe::get_frame_content_size(src) {
                    Ok(Some(n)) => n as usize,
                    _ => src.len() * 4,
                };
                self.buf.reserve(hint.max(64));
                let mut input = zstd_safe::InBuffer::around(src);
                loop {
                    if self.buf.len() == self.buf.capacity() {
                        let more = self.buf.capacity().max(1024);
                        self.buf.reserve(more);
                    }
                    let pos = self.buf.len();
                    let (out_pos, hint) = {
                        let mut out = zstd_safe::OutBuffer::around_pos(&mut self.buf, pos);
                        let hint = dctx
                            .decompress_stream(&mut out, &mut input)
                            .map_err(zstd_err)?;
                        (out.pos(), hint)
                    };
                    let full = out_pos == self.buf.capacity();
                    if input.pos() == src.len() {
                        if hint == 0 {
                            return Ok(());
                        }
                        if !full {
                            return Err(Error::msg("zstd: unexpected EOF"));
                        }
                    }
                }
            }
            Type::None => unreachable!(),
        }
    }
}

fn snappy_err(e: snap::Error) -> Error {
    match e {
        snap::Error::TooBig { .. } => Error::msg("snappy: decoded block is too large"),
        _ => Error::msg("snappy: corrupt input"),
    }
}

fn zstd_err(code: usize) -> Error {
    Error::Msg(format!("zstd: {}", zstd_safe::get_error_name(code)))
}
