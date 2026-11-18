//! Start timestamp (ST) encoding for the histogram ST chunks (port of `tsdb/chunkenc/st.go`).

use crate::Error;
use crate::bstream::{Bstream, BstreamReader};
use crate::histogram::HISTOGRAM_HEADER_SIZE;
use crate::varbit::{put_varbit_int, read_varbit_int};
use crate::xor::write_varint;

/// The largest sample index that the first ST change can be recorded at in the header.
pub const MAX_FIRST_ST_CHANGE_ON: u16 = 0x7F;
/// Mask of the sample count in the first two header bytes of the histogram ST chunks.
pub const HISTOGRAM_ST_SAMPLE_COUNT_MASK: u16 = 0x3FFF;

/// ST encoder state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StEncoder {
    pub st: i64,
    pub st_diff: i64,
    pub first_st_change_on: u8,
    pub first_st_known: bool,
}

/// ST decoder state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StDecoder {
    pub st: i64,
    pub st_diff: i64,
    pub first_st_change_on: u8,
    pub first_st_known: bool,
}

/// Sets the "first ST known" bit (overwrites the ST header byte).
pub fn write_header_first_st_known(b: &mut [u8]) {
    b[0] = 0x80;
}

/// Records the sample index of the first ST change, if it fits.
pub fn write_header_first_st_change_on(b: &mut [u8], first_st_change_on: u16) {
    if first_st_change_on > MAX_FIRST_ST_CHANGE_ON {
        return;
    }
    b[0] |= first_st_change_on as u8;
}

/// Reads the ST header byte.
pub fn read_st_header(b: &[u8]) -> (bool, u8) {
    (b[0] & 0x80 != 0, b[0] & 0x7F)
}

impl StEncoder {
    /// Encodes the ST of sample number `num` (1-based, after the sample was appended).
    pub fn encode(&mut self, b: &mut Bstream, num: u16, cur_t: i64, prev_t: i64, st: i64) {
        let h = HISTOGRAM_HEADER_SIZE - 1;
        match num {
            1 => {
                if st != 0 {
                    write_varint(b, cur_t.wrapping_sub(st));
                    self.first_st_known = true;
                    write_header_first_st_known(&mut b.bytes_mut()[h..]);
                }
            }
            2 => {
                if st != self.st {
                    let st_diff = prev_t.wrapping_sub(st);
                    self.first_st_change_on = 1;
                    write_header_first_st_change_on(&mut b.bytes_mut()[h..], 1);
                    put_varbit_int(b, st_diff);
                    self.st_diff = st_diff;
                }
            }
            _ => {
                if self.first_st_change_on == 0 && st == self.st && num.wrapping_sub(1) != MAX_FIRST_ST_CHANGE_ON {
                    // Nothing to write.
                } else if self.first_st_change_on == 0 {
                    if st != self.st || num.wrapping_sub(1) == MAX_FIRST_ST_CHANGE_ON {
                        let st_diff = prev_t.wrapping_sub(st);
                        self.first_st_change_on = num.wrapping_sub(1) as u8;
                        write_header_first_st_change_on(&mut b.bytes_mut()[h..], num.wrapping_sub(1));
                        put_varbit_int(b, st_diff);
                        self.st_diff = st_diff;
                    }
                } else {
                    let st_diff = prev_t.wrapping_sub(st);
                    put_varbit_int(b, st_diff.wrapping_sub(self.st_diff));
                    self.st_diff = st_diff;
                }
            }
        }
        self.st = st;
    }
}

impl StDecoder {
    /// Decodes the ST of sample number `num_read` (1-based, after the sample was read).
    pub fn decode(&mut self, br: &mut BstreamReader<'_>, num_read: u16, cur_t: i64, prev_t: i64) -> Result<(), Error> {
        match num_read {
            1 => {
                if self.first_st_known {
                    let st_diff = br.read_varint()?;
                    self.st_diff = st_diff;
                    self.st = cur_t.wrapping_sub(st_diff);
                }
            }
            2 => {
                if self.first_st_change_on == 1 {
                    let sdod = read_varbit_int(br)?;
                    self.st_diff = sdod;
                    self.st = prev_t.wrapping_sub(sdod);
                }
            }
            _ => {
                if self.first_st_change_on > 0 && num_read.wrapping_sub(1) >= self.first_st_change_on as u16 {
                    let sdod = read_varbit_int(br)?;
                    if num_read.wrapping_sub(1) == self.first_st_change_on as u16 {
                        self.st_diff = sdod;
                    } else {
                        self.st_diff = self.st_diff.wrapping_add(sdod);
                    }
                    self.st = prev_t.wrapping_sub(self.st_diff);
                }
            }
        }
        Ok(())
    }
}
