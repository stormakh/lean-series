//! Port of `storage/remote/chunked.go`: the framing of streamed remote read responses. Each frame is a uvarint
//! length, a big-endian CRC32 (Castagnoli) of the data, and the data.

use std::io::{self, BufRead, BufReader, Read, Write};

use storage::error::Sentinel;
use storage::Error;
use tsdb_format::encoding::crc32c;

use crate::prompb::Message;

static EOF: Sentinel = Sentinel::new("EOF");
static UNEXPECTED_EOF: Sentinel = Sentinel::new("unexpected EOF");
/// Go `io.EOF`: the stream ended cleanly before a frame.
pub const ERR_EOF: Error = Error::Sentinel(&EOF);
/// Go `io.ErrUnexpectedEOF`: the stream ended inside a frame.
pub const ERR_UNEXPECTED_EOF: Error = Error::Sentinel(&UNEXPECTED_EOF);

/// Maps an I/O error to the Go error (`io.EOF` and `io.ErrUnexpectedEOF` keep their identity).
pub(crate) fn io_err(e: io::Error) -> Error {
    match e.kind() {
        io::ErrorKind::UnexpectedEof => ERR_UNEXPECTED_EOF,
        _ => Error::msg(e.to_string()),
    }
}

/// Go `http.Flusher`.
pub trait Flusher {
    fn flush(&mut self);
}

/// A flusher that does nothing (the byte-body HTTP responses of this port have nothing to flush).
pub struct NopFlusher;

impl Flusher for NopFlusher {
    fn flush(&mut self) {}
}

/// Go `ChunkedWriter`: writes each buffer as one frame.
pub struct ChunkedWriter<W: Write, F: Flusher> {
    writer: W,
    flusher: F,
}

impl<W: Write, F: Flusher> ChunkedWriter<W, F> {
    /// Go `NewChunkedWriter`. Call [`ChunkedWriter::close`] when done.
    pub fn new(writer: W, flusher: F) -> ChunkedWriter<W, F> {
        ChunkedWriter { writer, flusher }
    }

    /// Go `Close`: flushes the underlying writer.
    pub fn close(&mut self) {
        self.flusher.flush();
    }

    /// The underlying writer.
    pub fn get_ref(&self) -> &W {
        &self.writer
    }

    pub fn get_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    pub fn into_inner(self) -> (W, F) {
        (self.writer, self.flusher)
    }

    /// Go `Write`: writes one frame (uvarint size, big-endian CRC32C, data). Returns the number of data bytes
    /// written (without size and checksum). An empty buffer writes nothing.
    pub fn write_frame(&mut self, b: &[u8]) -> io::Result<usize> {
        if b.is_empty() {
            return Ok(0);
        }
        let mut buf = [0u8; 10];
        let mut v = b.len() as u64;
        let mut n = 0;
        while v >= 0x80 {
            buf[n] = (v as u8) | 0x80;
            v >>= 7;
            n += 1;
        }
        buf[n] = v as u8;
        n += 1;
        self.writer.write_all(&buf[..n])?;
        self.writer.write_all(&crc32c(b).to_be_bytes())?;
        self.writer.write_all(b)?;
        Ok(b.len())
    }
}

impl<W: Write, F: Flusher> Write for ChunkedWriter<W, F> {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.write_frame(b)
    }
    /// Does not flush: the flusher runs only on `close`, as in Go.
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Go `ChunkedReader`: a buffered reader of frames, which checks the size limit and the checksum.
pub struct ChunkedReader<R: Read> {
    b: BufReader<R>,
    data: Vec<u8>,
    size_limit: u64,
}

impl<R: Read> ChunkedReader<R> {
    /// Go `NewChunkedReader`. `data` is a buffer to reuse.
    pub fn new(r: R, size_limit: u64, data: Vec<u8>) -> ChunkedReader<R> {
        ChunkedReader { b: BufReader::new(r), data, size_limit }
    }

    /// The underlying reader (Go keeps the response body apart; here the reader owns it).
    pub fn get_mut(&mut self) -> &mut R {
        self.b.get_mut()
    }

    /// Go `binary.ReadUvarint`: `io.EOF` when no byte was read, `io.ErrUnexpectedEOF` inside the varint.
    fn read_uvarint(&mut self) -> Result<u64, Error> {
        let mut x: u64 = 0;
        let mut s: u32 = 0;
        let mut i = 0;
        loop {
            let byte = match self.read_byte() {
                Ok(Some(b)) => b,
                Ok(None) => return Err(if i > 0 { ERR_UNEXPECTED_EOF } else { ERR_EOF }),
                Err(e) => return Err(io_err(e)),
            };
            if i == 9 && byte > 1 {
                return Err(Error::msg("binary: varint overflows a 64-bit integer"));
            }
            if byte < 0x80 {
                return Ok(x | (byte as u64) << s);
            }
            x |= ((byte & 0x7f) as u64) << s;
            s += 7;
            i += 1;
        }
    }

    fn read_byte(&mut self) -> io::Result<Option<u8>> {
        loop {
            match self.b.fill_buf() {
                Ok([]) => return Ok(None),
                Ok(buf) => {
                    let c = buf[0];
                    self.b.consume(1);
                    return Ok(Some(c));
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Go `io.ReadFull`: `io.EOF` when nothing was read, `io.ErrUnexpectedEOF` when only a part was read.
    fn read_full(b: &mut BufReader<R>, buf: &mut [u8]) -> Result<(), Error> {
        let mut n = 0;
        while n < buf.len() {
            match b.read(&mut buf[n..]) {
                Ok(0) => return Err(if n == 0 { ERR_EOF } else { ERR_UNEXPECTED_EOF }),
                Ok(k) => n += k,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(io_err(e)),
            }
        }
        Ok(())
    }

    /// Go `Next`: the next frame, or [`ERR_EOF`] at the end. The slice is valid until the next call.
    pub fn next(&mut self) -> Result<&[u8], Error> {
        let size = self.read_uvarint()?;
        if size > self.size_limit {
            return Err(Error::msg(format!(
                "chunkedReader: message size exceeded the limit {} bytes; got: {} bytes",
                self.size_limit, size
            )));
        }
        let size = size as usize;
        self.data.clear();
        self.data.resize(size, 0);

        let mut crc = [0u8; 4];
        Self::read_full(&mut self.b, &mut crc)?;
        let crc = u32::from_be_bytes(crc);

        Self::read_full(&mut self.b, &mut self.data)?;
        if crc32c(&self.data) != crc {
            return Err(Error::msg("chunkedReader: corrupted frame; checksum mismatch"));
        }
        Ok(&self.data)
    }

    /// Go `NextProto`: the next frame decoded as a protobuf message.
    pub fn next_proto<M: Message>(&mut self) -> Result<M, Error> {
        let rec = self.next()?;
        M::unmarshal(rec).map_err(|e| Error::msg(e.to_string()))
    }
}
