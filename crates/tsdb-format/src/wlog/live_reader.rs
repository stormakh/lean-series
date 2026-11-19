//! Go `LiveReader`: reads WAL records from a segment that is still being written.

use std::io::{self, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::compression::{DecodeBuffer, Type};
use super::{
    Error, PAGE_SIZE, RECORD_HEADER_SIZE, RecType, Result, SNAPPY_MASK, ZSTD_MASK, validate_record,
};
use crate::encoding::crc32c;
use crate::record::Logger;

/// Go `LiveReaderMetrics`: `prometheus_tsdb_wal_reader_corruption_errors_total{error="record_span_page"}`.
/// The remote write storage registers it (see `remote::write`).
#[derive(Debug, Default)]
pub struct LiveReaderMetrics {
    /// `prometheus_tsdb_wal_reader_corruption_errors_total{error="record_span_page"}`.
    pub reader_corruption_errors_record_span_page: AtomicU64,
}

impl LiveReaderMetrics {
    /// Go `NewLiveReaderMetrics`.
    pub fn new() -> Arc<LiveReaderMetrics> {
        Arc::new(LiveReaderMetrics::default())
    }
}

/// Go `LiveReader`: reads WAL records from a stream that is still being written. `next` returns false at the
/// current end of the data with `err()` being `Eof`; the caller can call `next` again later to continue.
pub struct LiveReader<R: Read> {
    rdr: R,
    err: Option<Error>,
    rec_in_dec: bool,

    precompr_buf: Vec<u8>,
    dec_buf: DecodeBuffer,
    hdr: [u8; RECORD_HEADER_SIZE],
    buf: Box<[u8]>,
    read_index: usize,  // Index in buf to start at for next read.
    write_index: usize, // Index in buf to start at for next write.
    total: i64,         // Total bytes processed during reading in calls to next().
    index: usize, // Used to track partial records, should be 0 at the start of every new record.

    // For testing, we can treat EOF as a non-error.
    eof_non_err: bool,

    // We sometime see records span page boundaries. Should never happen, but it
    // does. Until we track down why, set permissive to true to tolerate it.
    // NB the non-live Reader implementation allows for this.
    permissive: bool,

    metrics: Option<Arc<LiveReaderMetrics>>,
    logger: Logger,
}

impl<R: Read> LiveReader<R> {
    /// Go `NewLiveReader`.
    pub fn new(logger: Logger, metrics: Option<Arc<LiveReaderMetrics>>, r: R) -> LiveReader<R> {
        LiveReader {
            rdr: r,
            err: None,
            rec_in_dec: false,
            precompr_buf: Vec::new(),
            dec_buf: DecodeBuffer::new(),
            hdr: [0; RECORD_HEADER_SIZE],
            buf: vec![0u8; PAGE_SIZE].into_boxed_slice(),
            read_index: 0,
            write_index: 0,
            total: 0,
            index: 0,
            eof_non_err: false,
            // Until we understand how they come about, make readers permissive
            // to records spanning pages.
            permissive: true,
            metrics,
            logger,
        }
    }

    /// Treat EOF as no error in `err` (Go test-only field `eofNonErr`).
    pub fn set_eof_non_err(&mut self, v: bool) {
        self.eof_non_err = v;
    }

    /// The underlying stream.
    pub fn get_ref(&self) -> &R {
        &self.rdr
    }

    /// The underlying stream.
    pub fn get_mut(&mut self) -> &mut R {
        &mut self.rdr
    }

    /// Go `Err`: the last encountered error. `Eof` is not an error if there are more records to come.
    pub fn err(&self) -> Option<&Error> {
        if self.eof_non_err && self.err.as_ref().is_some_and(Error::is_eof) {
            return None;
        }
        self.err.as_ref()
    }

    /// Go `Offset`: the number of bytes consumed from this segment.
    pub fn offset(&self) -> i64 {
        self.total
    }

    fn fill_buffer(&mut self) -> io::Result<usize> {
        loop {
            match self.rdr.read(&mut self.buf[self.write_index..PAGE_SIZE]) {
                Ok(n) => {
                    self.write_index += n;
                    return Ok(n);
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
    }

    /// Go `Next`: returns true if `record` will return a full record. False means there is an error or the end
    /// of the data; on `Eof` the caller can call `next` again when more data is written.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> bool {
        loop {
            // If build_record returns a non-EOF error, its game up - the segment is
            // corrupt. If build_record returns an EOF, we try and read more in
            // fill_buffer later on. If that fails to read anything (n=0 && err=EOF),
            // we return EOF and the user can try again later. If we have a full
            // page, build_record is guaranteed to return a record or a non-EOF; it
            // has checks the records fit in pages.
            match self.build_record() {
                Ok(true) => return true,
                Err(e) if !e.is_eof() => {
                    self.err = Some(e);
                    return false;
                }
                _ => {}
            }

            // If we've filled the page and not found a record, this
            // means records have started to span pages. Shouldn't happen
            // but does and until we found out why, we need to deal with this.
            if self.permissive && self.write_index == PAGE_SIZE && self.read_index > 0 {
                self.buf.copy_within(self.read_index.., 0);
                self.write_index -= self.read_index;
                self.read_index = 0;
                continue;
            }

            if self.read_index == PAGE_SIZE {
                self.write_index = 0;
                self.read_index = 0;
            }

            if self.write_index != PAGE_SIZE {
                match self.fill_buffer() {
                    Ok(0) => {
                        self.err = Some(Error::Eof);
                        return false;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        self.err = Some(e.into());
                        return false;
                    }
                }
            }
        }
    }

    /// Go `Record`: the current record. It is only valid until the next call of `next`.
    pub fn record(&self) -> &[u8] {
        if self.rec_in_dec {
            self.dec_buf.get()
        } else {
            &self.precompr_buf
        }
    }

    /// Go `buildRecord`: rebuilds a full record from potentially partial records. Returns false if there was an
    /// error or if more data is needed.
    fn build_record(&mut self) -> Result<bool> {
        loop {
            // Check that we have data in the internal buffer to read.
            if self.write_index <= self.read_index {
                return Ok(false);
            }

            // Attempt to read a record, partial or otherwise.
            let (temp, n) = self.read_record()?;

            self.read_index += n;
            self.total += n as i64;
            let Some((start, end)) = temp else {
                return Ok(false);
            };

            let rt = RecType::from_header(self.hdr[0]);
            if rt == RecType::FIRST || rt == RecType::FULL {
                self.precompr_buf.clear();
            }

            // Segment format has only 2 bits, so it's either of those 3 options.
            // https://github.com/prometheus/prometheus/blob/main/tsdb/docs/format/wal.md#records-encoding
            let compr = if self.hdr[0] & SNAPPY_MASK == SNAPPY_MASK {
                Type::Snappy
            } else if self.hdr[0] & ZSTD_MASK == ZSTD_MASK {
                Type::Zstd
            } else {
                Type::None
            };

            self.precompr_buf.extend_from_slice(&self.buf[start..end]);

            if let Err(e) = validate_record(rt, self.index) {
                self.index = 0;
                return Err(e);
            }
            if rt == RecType::LAST || rt == RecType::FULL {
                self.index = 0;
                self.rec_in_dec = false;
                if !self.precompr_buf.is_empty() && compr != Type::None {
                    self.dec_buf.decode_into_self(compr, &self.precompr_buf)?;
                    self.rec_in_dec = true;
                }
                return Ok(true);
            }
            // Only increment i for non-zero records since we use it
            // to determine valid content record sequences.
            self.index += 1;
        }
    }

    /// Go `readRecord`: returns the bounds in `buf` of the next fragment (None for a page term) and the number
    /// of bytes consumed. `Eof` means more data is needed.
    #[allow(clippy::type_complexity)]
    fn read_record(&mut self) -> Result<(Option<(usize, usize)>, usize)> {
        // Special case: for PAGE_TERM, check that are all zeros to end of page,
        // consume them but don't return them.
        if self.buf[self.read_index] == RecType::PAGE_TERM.0 {
            // End of page won't necessarily be end of buffer, as we may have
            // got misaligned by records spanning page boundaries.
            // total % PAGE_SIZE is the offset into the current page
            // that read_index points to in buf. Therefore
            // PAGE_SIZE - (total % PAGE_SIZE) is the amount left to read of
            // the current page.
            let remaining = PAGE_SIZE - (self.total as usize % PAGE_SIZE);
            if self.read_index + remaining > self.write_index {
                return Err(Error::Eof);
            }

            if self.buf[self.read_index..self.read_index + remaining]
                .iter()
                .any(|&c| c != 0)
            {
                return Err(Error::msg("unexpected non-zero byte in page term bytes"));
            }

            return Ok((None, remaining));
        }

        // Not a PAGE_TERM; read the record and check the checksum.
        if self.write_index - self.read_index < RECORD_HEADER_SIZE {
            return Err(Error::Eof);
        }

        self.hdr
            .copy_from_slice(&self.buf[self.read_index..self.read_index + RECORD_HEADER_SIZE]);
        let length = u16::from_be_bytes([self.hdr[1], self.hdr[2]]) as usize;
        let crc = u32::from_be_bytes([self.hdr[3], self.hdr[4], self.hdr[5], self.hdr[6]]);
        if self.read_index + RECORD_HEADER_SIZE + length > PAGE_SIZE {
            if !self.permissive {
                return Err(Error::Msg(format!(
                    "record would overflow current page: {} > {}",
                    self.read_index + RECORD_HEADER_SIZE + length,
                    PAGE_SIZE
                )));
            }
            if let Some(m) = &self.metrics {
                m.reader_corruption_errors_record_span_page
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.logger.warn(
                "Record spans page boundaries",
                &[
                    ("start", &self.read_index),
                    ("end", &(RECORD_HEADER_SIZE + length)),
                    ("pageSize", &PAGE_SIZE),
                ],
            );
        }
        if RECORD_HEADER_SIZE + length > PAGE_SIZE {
            return Err(Error::Msg(format!(
                "record length greater than a single page: {} > {}",
                RECORD_HEADER_SIZE + length,
                PAGE_SIZE
            )));
        }
        if self.read_index + RECORD_HEADER_SIZE + length > self.write_index {
            return Err(Error::Eof);
        }

        let start = self.read_index + RECORD_HEADER_SIZE;
        let end = start + length;
        let c = crc32c(&self.buf[start..end]);
        if c != crc {
            return Err(Error::Msg(format!(
                "unexpected checksum {c:x}, expected {crc:x}"
            )));
        }

        Ok((Some((start, end)), length + RECORD_HEADER_SIZE))
    }
}
