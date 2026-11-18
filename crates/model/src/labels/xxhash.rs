//! XXH64 with seed 0, as `github.com/cespare/xxhash/v2`.

const P1: u64 = 11400714785074694791;
const P2: u64 = 14029467366897019727;
const P3: u64 = 1609587929392839161;
const P4: u64 = 9650029242287828579;
const P5: u64 = 2870177450012600261;

#[inline(always)]
fn read_u64(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
}

#[inline(always)]
fn read_u32(b: &[u8], i: usize) -> u64 {
    u32::from_le_bytes(b[i..i + 4].try_into().unwrap()) as u64
}

#[inline(always)]
fn round(acc: u64, input: u64) -> u64 {
    acc.wrapping_add(input.wrapping_mul(P2)).rotate_left(31).wrapping_mul(P1)
}

#[inline(always)]
fn merge_round(acc: u64, val: u64) -> u64 {
    let val = round(0, val);
    (acc ^ val).wrapping_mul(P1).wrapping_add(P4)
}

#[inline(always)]
fn finish(mut h: u64, b: &[u8]) -> u64 {
    let mut i = 0;
    while i + 8 <= b.len() {
        let k1 = round(0, read_u64(b, i));
        h ^= k1;
        h = h.rotate_left(27).wrapping_mul(P1).wrapping_add(P4);
        i += 8;
    }
    if i + 4 <= b.len() {
        h ^= read_u32(b, i).wrapping_mul(P1);
        h = h.rotate_left(23).wrapping_mul(P2).wrapping_add(P3);
        i += 4;
    }
    while i < b.len() {
        h ^= (b[i] as u64).wrapping_mul(P5);
        h = h.rotate_left(11).wrapping_mul(P1);
        i += 1;
    }
    h ^= h >> 33;
    h = h.wrapping_mul(P2);
    h ^= h >> 29;
    h = h.wrapping_mul(P3);
    h ^= h >> 32;
    h
}

/// XXH64 of `b` with seed 0.
pub fn sum64(b: &[u8]) -> u64 {
    let n = b.len();
    let mut h;
    let mut i = 0;
    if n >= 32 {
        let mut v1 = P1.wrapping_add(P2);
        let mut v2 = P2;
        let mut v3 = 0u64;
        let mut v4 = 0u64.wrapping_sub(P1);
        while i + 32 <= n {
            v1 = round(v1, read_u64(b, i));
            v2 = round(v2, read_u64(b, i + 8));
            v3 = round(v3, read_u64(b, i + 16));
            v4 = round(v4, read_u64(b, i + 24));
            i += 32;
        }
        h = v1.rotate_left(1).wrapping_add(v2.rotate_left(7)).wrapping_add(v3.rotate_left(12)).wrapping_add(v4.rotate_left(18));
        h = merge_round(h, v1);
        h = merge_round(h, v2);
        h = merge_round(h, v3);
        h = merge_round(h, v4);
    } else {
        h = P5;
    }
    h = h.wrapping_add(n as u64);
    finish(h, &b[i..])
}

/// Streaming XXH64 digest (Go `xxhash.Digest`).
#[derive(Clone)]
pub struct Digest {
    v: [u64; 4],
    total: u64,
    mem: [u8; 32],
    n: usize,
}

impl Default for Digest {
    fn default() -> Self {
        Self::new()
    }
}

impl Digest {
    pub fn new() -> Digest {
        Digest { v: [P1.wrapping_add(P2), P2, 0, 0u64.wrapping_sub(P1)], total: 0, mem: [0; 32], n: 0 }
    }

    pub fn write(&mut self, mut b: &[u8]) {
        self.total += b.len() as u64;
        if self.n + b.len() < 32 {
            self.mem[self.n..self.n + b.len()].copy_from_slice(b);
            self.n += b.len();
            return;
        }
        if self.n > 0 {
            let c = 32 - self.n;
            self.mem[self.n..].copy_from_slice(&b[..c]);
            let m = self.mem;
            self.v[0] = round(self.v[0], read_u64(&m, 0));
            self.v[1] = round(self.v[1], read_u64(&m, 8));
            self.v[2] = round(self.v[2], read_u64(&m, 16));
            self.v[3] = round(self.v[3], read_u64(&m, 24));
            b = &b[c..];
            self.n = 0;
        }
        while b.len() >= 32 {
            self.v[0] = round(self.v[0], read_u64(b, 0));
            self.v[1] = round(self.v[1], read_u64(b, 8));
            self.v[2] = round(self.v[2], read_u64(b, 16));
            self.v[3] = round(self.v[3], read_u64(b, 24));
            b = &b[32..];
        }
        self.mem[..b.len()].copy_from_slice(b);
        self.n = b.len();
    }

    pub fn sum64(&self) -> u64 {
        let mut h;
        if self.total >= 32 {
            let [v1, v2, v3, v4] = self.v;
            h = v1.rotate_left(1).wrapping_add(v2.rotate_left(7)).wrapping_add(v3.rotate_left(12)).wrapping_add(v4.rotate_left(18));
            h = merge_round(h, v1);
            h = merge_round(h, v2);
            h = merge_round(h, v3);
            h = merge_round(h, v4);
        } else {
            h = P5;
        }
        h = h.wrapping_add(self.total);
        finish(h, &self.mem[..self.n])
    }
}
