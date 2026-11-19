//! Block metadata (`meta.json`, version 1) and ULIDs (port of the `BlockMeta` part of `tsdb/block.go` and of
//! `github.com/oklog/ulid/v2`, which upstream uses for block IDs).

use std::fmt;
use std::path::Path;
use std::sync::Mutex;

use json::Json;
use storage::Error;

/// Go `indexFilename`.
pub const INDEX_FILENAME: &str = "index";
/// Go `metaFilename`.
pub const META_FILENAME: &str = "meta.json";
/// Go `metaVersion1`.
pub const META_VERSION_1: i64 = 1;

/// Go `CompactionHintFromOutOfOrder`.
pub const COMPACTION_HINT_FROM_OUT_OF_ORDER: &str = "from-out-of-order";
/// Go `CompactionHintFromStaleSeries`.
pub const COMPACTION_HINT_FROM_STALE_SERIES: &str = "from-stale-series";
/// Go `CompactionHintFromSelectedSeries`.
pub const COMPACTION_HINT_FROM_SELECTED_SERIES: &str = "from-selected-series";

// ---------------------------------------------------------------------------------------------------------------
// ULID.

/// A ULID (Go `ulid.ULID`): 48 bits of milliseconds since the Unix epoch, then 80 random bits. Ordered by bytes,
/// so by time first. Printed as 26 Crockford base32 characters.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Ulid(pub [u8; 16]);

const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Go `ulid.ErrDataSize`.
pub const ULID_ERR_DATA_SIZE: &str = "ulid: bad data size when unmarshaling";
/// Go `ulid.ErrInvalidCharacters`.
pub const ULID_ERR_INVALID_CHARACTERS: &str = "ulid: bad data characters when unmarshaling";
/// Go `ulid.ErrOverflow`.
pub const ULID_ERR_OVERFLOW: &str = "ulid: overflow when unmarshaling";

fn crockford_value(c: u8) -> Option<u8> {
    let c = c.to_ascii_uppercase();
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'O' => Some(0),
        b'I' | b'L' => Some(1),
        _ => CROCKFORD.iter().position(|&x| x == c).map(|p| p as u8),
    }
}

impl Ulid {
    /// Go `ulid.MustNew(ulid.Timestamp(time.Now()), rand.Reader)`: a new ULID for the current time. ULIDs made
    /// in the same millisecond by this process are monotonic (the random part is incremented), as upstream's
    /// `ulid.Make` does.
    pub fn new() -> Ulid {
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Ulid::new_monotonic(ms)
    }

    /// A ULID with the given millisecond timestamp and a random part (monotonic within one millisecond).
    pub fn new_monotonic(ms: u64) -> Ulid {
        static LAST: Mutex<(u64, [u8; 10])> = Mutex::new((0, [0; 10]));
        let mut last = LAST.lock().unwrap_or_else(|p| p.into_inner());
        let mut entropy = [0u8; 10];
        if last.0 == ms {
            entropy = last.1;
            // Increment the 80-bit random part; on overflow take fresh entropy.
            let mut i = 10;
            loop {
                if i == 0 {
                    crypto::random_bytes(&mut entropy);
                    break;
                }
                i -= 1;
                entropy[i] = entropy[i].wrapping_add(1);
                if entropy[i] != 0 {
                    break;
                }
            }
        } else {
            crypto::random_bytes(&mut entropy);
        }
        *last = (ms, entropy);
        Ulid::from_parts(ms, entropy)
    }

    /// A ULID of a timestamp (milliseconds) and an entropy part.
    pub fn from_parts(ms: u64, entropy: [u8; 10]) -> Ulid {
        let mut b = [0u8; 16];
        b[..6].copy_from_slice(&ms.to_be_bytes()[2..]);
        b[6..].copy_from_slice(&entropy);
        Ulid(b)
    }

    /// Go `ULID.Time()`: the millisecond timestamp.
    pub fn time(&self) -> u64 {
        let mut t = [0u8; 8];
        t[2..].copy_from_slice(&self.0[..6]);
        u64::from_be_bytes(t)
    }

    /// Go `ulid.Parse` (also accepts lower case, as upstream does).
    pub fn parse(s: &str) -> Result<Ulid, Error> {
        let b = s.as_bytes();
        if b.len() != 26 {
            return Err(Error::msg(ULID_ERR_DATA_SIZE));
        }
        let mut v = [0u8; 26];
        for (i, &c) in b.iter().enumerate() {
            v[i] = crockford_value(c).ok_or_else(|| Error::msg(ULID_ERR_INVALID_CHARACTERS))?;
        }
        // The first character can only hold 3 bits (2^128 fits in 26 characters only with v[0] <= 7).
        if v[0] > 7 {
            return Err(Error::msg(ULID_ERR_OVERFLOW));
        }
        let mut n: u128 = 0;
        for x in v {
            n = (n << 5) | x as u128;
        }
        Ok(Ulid(n.to_be_bytes()))
    }

    /// Go `ULID.String()`.
    pub fn to_string_buf(&self, out: &mut String) {
        let n = u128::from_be_bytes(self.0);
        for i in (0..26).rev() {
            out.push(CROCKFORD[((n >> (i * 5)) & 31) as usize] as char);
        }
    }
}

impl fmt::Display for Ulid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = String::with_capacity(26);
        self.to_string_buf(&mut s);
        f.write_str(&s)
    }
}

impl fmt::Debug for Ulid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

// ---------------------------------------------------------------------------------------------------------------
// BlockMeta.

/// Go `BlockStats`: stats about the contents of a block.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlockStats {
    pub num_samples: u64,
    pub num_float_samples: u64,
    pub num_histogram_samples: u64,
    pub num_series: u64,
    pub num_chunks: u64,
    pub num_tombstones: u64,
}

/// Go `BlockDesc`: a block by ULID and time range.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlockDesc {
    pub ulid: Ulid,
    pub min_time: i64,
    pub max_time: i64,
}

/// Go `BlockMetaCompaction`: the compactions a block went through.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlockMetaCompaction {
    /// Maximum number of compaction cycles any source block has gone through.
    pub level: i64,
    /// ULIDs of all source head blocks that went into the block.
    pub sources: Vec<Ulid>,
    /// The compaction resulted in a block without samples; it is deleted on the next reload.
    pub deletable: bool,
    /// The direct blocks that were used to create this block.
    pub parents: Vec<BlockDesc>,
    pub failed: bool,
    /// Additional information, for example that the block was created from out-of-order chunks.
    pub hints: Vec<String>,
}

impl BlockMetaCompaction {
    fn set_hint(&mut self, hint: &str) {
        if self.hints.iter().any(|h| h == hint) {
            return;
        }
        self.hints.push(hint.to_string());
        self.hints.sort();
    }
    /// Go `SetOutOfOrder`.
    pub fn set_out_of_order(&mut self) {
        self.set_hint(COMPACTION_HINT_FROM_OUT_OF_ORDER)
    }
    /// Go `FromOutOfOrder`.
    pub fn from_out_of_order(&self) -> bool {
        self.hints.iter().any(|h| h == COMPACTION_HINT_FROM_OUT_OF_ORDER)
    }
    /// Go `SetStaleSeries`.
    pub fn set_stale_series(&mut self) {
        self.set_hint(COMPACTION_HINT_FROM_STALE_SERIES)
    }
    /// Go `FromStaleSeries`.
    pub fn from_stale_series(&self) -> bool {
        self.hints.iter().any(|h| h == COMPACTION_HINT_FROM_STALE_SERIES)
    }
    /// Go `SetSelectedSeries`.
    pub fn set_selected_series(&mut self) {
        self.set_hint(COMPACTION_HINT_FROM_SELECTED_SERIES)
    }
    /// Go `FromSelectedSeries`.
    pub fn from_selected_series(&self) -> bool {
        self.hints.iter().any(|h| h == COMPACTION_HINT_FROM_SELECTED_SERIES)
    }
}

/// Go `BlockMeta`: meta information about a block (the content of `meta.json`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlockMeta {
    /// Unique identifier for the block and its contents. Changes on compaction.
    pub ulid: Ulid,
    /// The time range all samples of the block are in: [min_time, max_time).
    pub min_time: i64,
    pub max_time: i64,
    pub stats: BlockStats,
    pub compaction: BlockMetaCompaction,
    /// Version of the meta file format (1).
    pub version: i64,
}

fn ulid_json(u: &Ulid) -> Json {
    Json::Str(u.to_string())
}

impl BlockMeta {
    /// The JSON value, with Go's field names, order and `omitempty` rules.
    pub fn to_json(&self) -> Json {
        let mut stats = Json::obj();
        let s = &self.stats;
        for (k, v) in [
            ("numSamples", s.num_samples),
            ("numFloatSamples", s.num_float_samples),
            ("numHistogramSamples", s.num_histogram_samples),
            ("numSeries", s.num_series),
            ("numChunks", s.num_chunks),
            ("numTombstones", s.num_tombstones),
        ] {
            if v != 0 {
                stats.set(k, Json::Int(v as i64));
            }
        }
        let c = &self.compaction;
        let mut comp = Json::obj().with("level", Json::Int(c.level));
        if !c.sources.is_empty() {
            comp.set("sources", Json::Arr(c.sources.iter().map(ulid_json).collect()));
        }
        if c.deletable {
            comp.set("deletable", Json::Bool(true));
        }
        if !c.parents.is_empty() {
            comp.set(
                "parents",
                Json::Arr(
                    c.parents
                        .iter()
                        .map(|p| {
                            Json::obj()
                                .with("ulid", ulid_json(&p.ulid))
                                .with("minTime", Json::Int(p.min_time))
                                .with("maxTime", Json::Int(p.max_time))
                        })
                        .collect(),
                ),
            );
        }
        if c.failed {
            comp.set("failed", Json::Bool(true));
        }
        if !c.hints.is_empty() {
            comp.set("hints", Json::Arr(c.hints.iter().map(|h| Json::Str(h.clone())).collect()));
        }
        Json::obj()
            .with("ulid", ulid_json(&self.ulid))
            .with("minTime", Json::Int(self.min_time))
            .with("maxTime", Json::Int(self.max_time))
            .with("stats", stats)
            .with("compaction", comp)
            .with("version", Json::Int(self.version))
    }

    /// Go `json.Unmarshal` into a `BlockMeta` (unknown fields are ignored).
    pub fn from_json(v: &Json) -> Result<BlockMeta, Error> {
        fn int(v: Option<&Json>) -> Result<i64, Error> {
            match v {
                None | Some(Json::Null) => Ok(0),
                Some(j) => j.as_i64().ok_or_else(|| Error::msg("json: cannot unmarshal number into Go value")),
            }
        }
        fn ulid(v: Option<&Json>) -> Result<Ulid, Error> {
            match v {
                None | Some(Json::Null) => Ok(Ulid::default()),
                Some(Json::Str(s)) => Ulid::parse(s),
                Some(_) => Err(Error::msg("json: cannot unmarshal into Go value of type ulid.ULID")),
            }
        }
        if v.as_obj().is_none() {
            return Err(Error::msg("json: cannot unmarshal into Go value of type tsdb.BlockMeta"));
        }
        let mut m = BlockMeta {
            ulid: ulid(v.get("ulid"))?,
            min_time: int(v.get("minTime"))?,
            max_time: int(v.get("maxTime"))?,
            version: int(v.get("version"))?,
            ..BlockMeta::default()
        };
        if let Some(s) = v.get("stats") {
            m.stats = BlockStats {
                num_samples: int(s.get("numSamples"))? as u64,
                num_float_samples: int(s.get("numFloatSamples"))? as u64,
                num_histogram_samples: int(s.get("numHistogramSamples"))? as u64,
                num_series: int(s.get("numSeries"))? as u64,
                num_chunks: int(s.get("numChunks"))? as u64,
                num_tombstones: int(s.get("numTombstones"))? as u64,
            };
        }
        if let Some(c) = v.get("compaction") {
            m.compaction.level = int(c.get("level"))?;
            if let Some(a) = c.get("sources").and_then(|a| a.as_arr()) {
                for s in a {
                    m.compaction.sources.push(ulid(Some(s))?);
                }
            }
            m.compaction.deletable = c.get("deletable").and_then(|b| b.as_bool()).unwrap_or(false);
            m.compaction.failed = c.get("failed").and_then(|b| b.as_bool()).unwrap_or(false);
            if let Some(a) = c.get("parents").and_then(|a| a.as_arr()) {
                for p in a {
                    m.compaction.parents.push(BlockDesc {
                        ulid: ulid(p.get("ulid"))?,
                        min_time: int(p.get("minTime"))?,
                        max_time: int(p.get("maxTime"))?,
                    });
                }
            }
            if let Some(a) = c.get("hints").and_then(|a| a.as_arr()) {
                for h in a {
                    m.compaction.hints.push(h.as_str().unwrap_or_default().to_string());
                }
            }
        }
        Ok(m)
    }
}

/// Go `readMetaFile`: the meta of the block in `dir` and the size of the meta file.
pub fn read_meta_file(dir: &Path) -> Result<(BlockMeta, i64), Error> {
    let p = dir.join(META_FILENAME);
    let b = std::fs::read(&p).map_err(|e| Error::msg(format!("open {}: {}", p.display(), crate::io_err_text(&e))))?;
    let v = Json::parse_bytes(&b).map_err(|e| Error::msg(e.to_string()))?;
    let m = BlockMeta::from_json(&v)?;
    if m.version != META_VERSION_1 {
        return Err(Error::msg(format!("unexpected meta file version {}", m.version)));
    }
    Ok((m, b.len() as i64))
}

/// Go `writeMetaFile`: writes `meta.json` (version 1, tab-indented as Go's `json.MarshalIndent`) through a
/// synced `.tmp` file that is then renamed. Returns the number of bytes written.
pub fn write_meta_file(dir: &Path, meta: &mut BlockMeta) -> Result<i64, Error> {
    use std::io::Write;
    meta.version = META_VERSION_1;
    let path = dir.join(META_FILENAME);
    let tmp = dir.join(format!("{META_FILENAME}.tmp"));
    let res = (|| -> Result<i64, Error> {
        let mut f = std::fs::File::create(&tmp).map_err(Error::other)?;
        let text = meta.to_json().stringify_pretty("\t");
        f.write_all(text.as_bytes()).map_err(Error::other)?;
        f.sync_all().map_err(Error::other)?;
        drop(f);
        tsdb_format::fileutil::replace(&tmp, &path).map_err(Error::other)?;
        Ok(text.len() as i64)
    })();
    let _ = std::fs::remove_file(&tmp);
    res
}
