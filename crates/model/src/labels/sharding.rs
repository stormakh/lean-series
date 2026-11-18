//! Port of `model/labels/sharding_stringlabels.go`.

use super::xxhash::{self, Digest};
use super::{Labels, SEP};

/// A labels hash that is guaranteed not to change over time. Use it whenever hash compatibility matters.
pub fn stable_hash(ls: &Labels) -> u64 {
    // Same bytes as Go: name, 0xff, value, 0xff for each label. Go switches to the streaming API above 1 KiB,
    // which gives the same hash.
    let mut stack = [0u8; 1024];
    let mut n = 0;
    let mut h: Option<Digest> = None;
    for (name, value) in ls.iter() {
        if h.is_none() && n + name.len() + value.len() + 2 >= stack.len() {
            let mut d = Digest::new();
            d.write(&stack[..n]);
            h = Some(d);
        }
        if let Some(d) = h.as_mut() {
            d.write(name.as_bytes());
            d.write(&[SEP]);
            d.write(value.as_bytes());
            d.write(&[SEP]);
            continue;
        }
        stack[n..n + name.len()].copy_from_slice(name.as_bytes());
        n += name.len();
        stack[n] = SEP;
        n += 1;
        stack[n..n + value.len()].copy_from_slice(value.as_bytes());
        n += value.len();
        stack[n] = SEP;
        n += 1;
    }
    match h {
        Some(d) => d.sum64(),
        None => xxhash::sum64(&stack[..n]),
    }
}
