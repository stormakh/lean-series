//! Port of `model/labels/test_utils.go`.

use std::collections::HashSet;
use std::io::BufRead;

use super::{Labels, ScratchBuilder};
use crate::matcher::Matcher;

/// Constraints for matching against a label set.
#[derive(Clone, Debug, Default)]
pub struct Selector(pub Vec<Matcher>);

impl Selector {
    /// Whether the labels satisfy all matchers.
    pub fn matches(&self, labels: &Labels) -> bool {
        self.0.iter().all(|m| m.matches(labels.get(&m.name)))
    }
}

/// Reads up to `n` label sets from a file with one JSON-like label set per line (test data such as
/// `tsdb/testdata/20kseries.json`). Duplicate label sets (by hash) are skipped.
pub fn read_labels(path: &str, n: usize) -> Result<Vec<Labels>, String> {
    let f = std::fs::File::open(path).map_err(|e| format!("open {}: {}", path, e))?;
    let mut b = ScratchBuilder::new(0);
    let mut mets = Vec::new();
    let mut hashes = HashSet::new();
    let mut i = 0;
    for line in std::io::BufReader::new(f).lines() {
        if i >= n {
            break;
        }
        let line = line.map_err(|e| e.to_string())?;
        b.reset();
        let s: String = line.chars().filter(|&c| c != '"' && c != '{' && c != '}').collect();
        for chunk in s.split(',') {
            let mut split = chunk.split(':');
            let name = split.next().unwrap_or("");
            let value = split.next().ok_or_else(|| "index out of range".to_string())?;
            b.add(name, value);
        }
        b.sort();
        let m = b.labels();
        if !hashes.insert(m.hash()) {
            continue;
        }
        mets.push(m);
        i += 1;
    }
    if i != n {
        return Err(format!("requested {} metrics but found {}", n, i));
    }
    Ok(mets)
}
