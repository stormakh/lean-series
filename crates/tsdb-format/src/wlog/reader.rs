//! Go `Reader`: reads WAL records from a byte stream.

use std::any::Any;
use std::io::Read;
use std::path::PathBuf;

use super::compression::{DecodeBuffer, Type};
use super::segment::SegmentBufReader;
use super::{
    CorruptionErr, Error, PAGE_SIZE, RECORD_HEADER_SIZE, RecType, Result, SNAPPY_MASK, ZSTD_MASK,
    read_full, validate_record,
};
use crate::encoding::crc32c;

/// Go `Reader`: reads WAL records from a byte stream. The record returned by `record` borrows the reader's
/// buffer and is valid until the next call to `next`.
///
/// When the stream is a `SegmentBufReader`, errors and positions name the segment and the offset in it.
pub struct Reader<R: Read + 'static> {
    rdr: R,
    err: Option<Error>,
    rec_in_dec: bool,

    precompr_buf: Vec<u8>,
    dec_buf: DecodeBuffer,
    buf: Box<[u8]>,
    total: i64,           // Total bytes processed.
    cur_rec_typ: RecType, // Used for checking that the last record is not torn.
}

impl<R: Read + 'static> Reader<R> {
    /// Go `NewReader`.
    pub fn new(r: R) -> Reader<R> {
        Reader {
            rdr: r,
            err: None,
            rec_in_dec: false,
            precompr_buf: Vec::new(),
            dec_buf: DecodeBuffer::new(),
            buf: vec![0u8; PAGE_SIZE].into_boxed_slice(),
            total: 0,
            cur_rec_typ: RecType::PAGE_TERM,
        }
    }

    /// The underlying stream.
    pub fn get_ref(&self) -> &R {
        &self.rdr
    }

    /// The underlying stream.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.rdr
    }

    /// Gives back the underlying stream.
    pub fn into_inner(self) -> R {
        self.rdr
    }

    fn seg_reader(&self) -> Option<&SegmentBufReader> {
        (&self.rdr as &dyn Any).downcast_ref::<SegmentBufReader>()
    }

    /// Go `Next`: advances the reader to the next record and returns true if it exists. It must not be called
    /// again after it returned false.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> bool {
        match self.next_new() {
            Err(e) if e.is_eof() => {
                // The last WAL segment record shouldn't be torn (should be full or last).
                // The last record would be torn after a crash just before
                // the last record part could be persisted to disk.
                if self.cur_rec_typ == RecType::FIRST || self.cur_rec_typ == RecType::MIDDLE {
                    self.err = Some(Error::msg("last record is torn"));
                }
                false
            }
            Err(e) => {
                self.err = Some(e);
                false
            }
            Ok(()) => {
                self.err = None;
                true
            }
        }
    }

    fn next_new(&mut self) -> Result<()> {
        self.precompr_buf.clear();

        let mut i = 0;
        loop {
            read_full(&mut self.rdr, &mut self.buf[..1])
                .map_err(|e| e.wrap("read first header byte"))?;
            self.total += 1;
            let hdr0 = self.buf[0];
            self.cur_rec_typ = RecType::from_header(hdr0);

            let compr = if hdr0 & SNAPPY_MASK == SNAPPY_MASK {
                Type::Snappy
            } else if hdr0 & ZSTD_MASK == ZSTD_MASK {
                Type::Zstd
            } else {
                Type::None
            };

            // Gobble up zero bytes.
            if self.cur_rec_typ == RecType::PAGE_TERM {
                // We are pedantic and check whether the zeros are actually up
                // to a page boundary.
                // It's not strictly necessary but may catch sketchy state early.
                let k = PAGE_SIZE - (self.total as usize % PAGE_SIZE);
                if k == PAGE_SIZE {
                    continue; // Initial 0 byte was last page byte.
                }
                let buf = &mut self.buf[1..1 + k];
                let n =
                    read_full(&mut self.rdr, buf).map_err(|e| e.wrap("read remaining zeros"))?;
                self.total += n as i64;

                if buf.iter().any(|&c| c != 0) {
                    return Err(Error::msg("unexpected non-zero byte in padded page"));
                }
                continue;
            }
            let n = read_full(&mut self.rdr, &mut self.buf[1..RECORD_HEADER_SIZE])
                .map_err(|e| e.wrap("read remaining header"))?;
            self.total += n as i64;

            let length = u16::from_be_bytes([self.buf[1], self.buf[2]]) as usize;
            let crc = u32::from_be_bytes([self.buf[3], self.buf[4], self.buf[5], self.buf[6]]);

            if length > PAGE_SIZE - RECORD_HEADER_SIZE {
                return Err(Error::Msg(format!("invalid record size {length}")));
            }
            let body = &mut self.buf[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + length];
            let n = if length == 0 {
                0
            } else {
                read_full(&mut self.rdr, body)?
            };
            self.total += n as i64;

            if n != length {
                return Err(Error::Msg(format!(
                    "invalid size: expected {length}, got {n}"
                )));
            }
            let body = &self.buf[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + length];
            let c = crc32c(body);
            if c != crc {
                return Err(Error::Msg(format!(
                    "unexpected checksum {c:x}, expected {crc:x}"
                )));
            }
            validate_record(self.cur_rec_typ, i)?;

            self.precompr_buf.extend_from_slice(body);
            if self.cur_rec_typ == RecType::LAST || self.cur_rec_typ == RecType::FULL {
                self.rec_in_dec = false;
                if !self.precompr_buf.is_empty() && compr != Type::None {
                    self.dec_buf.decode_into_self(compr, &self.precompr_buf)?;
                    self.rec_in_dec = true;
                }
                return Ok(());
            }

            // Only increment i for non-zero records since we use it
            // to determine valid content record sequences.
            i += 1;
        }
    }

    /// Go `Err`: the last encountered error wrapped in a corruption error.
    pub fn err(&self) -> Option<Error> {
        let err = self.err.as_ref()?;
        if let Some(b) = self.seg_reader()
            && let Some(seg) = b.current_segment()
        {
            return Some(Error::Corruption(Box::new(CorruptionErr {
                err: err.clone(),
                dir: seg.dir().to_path_buf(),
                segment: seg.index(),
                offset: b.offset() as i64,
            })));
        }
        Some(Error::Corruption(Box::new(CorruptionErr {
            err: err.clone(),
            dir: PathBuf::new(),
            segment: -1,
            offset: self.total,
        })))
    }

    /// Go `Record`: the current record. It is only valid until the next call of `next`.
    pub fn record(&self) -> &[u8] {
        if self.rec_in_dec {
            self.dec_buf.get()
        } else {
            &self.precompr_buf
        }
    }

    /// Go `Segment`: the current segment being read, or -1 when the stream is not a `SegmentBufReader`.
    pub fn segment(&self) -> i64 {
        match self
            .seg_reader()
            .and_then(SegmentBufReader::current_segment)
        {
            Some(s) => s.index(),
            None => -1,
        }
    }

    /// Go `Offset`: the current position of the reader. For a `SegmentBufReader` it is the offset in the current
    /// segment.
    pub fn offset(&self) -> i64 {
        if let Some(b) = self.seg_reader() {
            return b.offset() as i64;
        }
        self.total
    }
}
