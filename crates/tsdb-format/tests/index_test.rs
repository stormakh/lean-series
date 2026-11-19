//! Port of `tsdb/index/index_test.go`.

use std::collections::{HashMap, HashSet};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use model::labels::{self, Label, Labels, ScratchBuilder, read_labels, stable_hash};
use tsdb_format::SeriesRef;
use tsdb_format::chunks::Meta;
use storage::Context;
use tsdb_format::encoding::{Decbuf, Encbuf, crc32c};
use tsdb_format::index::{
    BoxPostings, Ctx, Decoder, Error, FORMAT_V2, INDEX_FILENAME, MemPostings, Postings, Reader,
    StringIter, Symbols, Writer, all_postings_key, background, decode_postings_raw, err_postings,
    expand_postings, merge, new_list_postings,
};

// ---------------------------------------------------------------------------------------------------------------
// Test helpers.

/// Go `t.TempDir()`: a unique directory under the cargo test temp dir, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        static N: AtomicUsize = AtomicUsize::new(0);
        let p = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "index_test-{name}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Go `testutil.MockContextErrAfter`.
struct MockContextErrAfter {
    count: AtomicU64,
    fail_after: u64,
}

impl MockContextErrAfter {
    fn new(fail_after: u64) -> MockContextErrAfter {
        MockContextErrAfter {
            count: AtomicU64::new(0),
            fail_after,
        }
    }
    fn count(&self) -> u64 {
        self.count.load(Ordering::SeqCst)
    }
}

impl Ctx for MockContextErrAfter {
    fn err(&self) -> Option<Error> {
        self.count.fetch_add(1, Ordering::SeqCst);
        if self.count.load(Ordering::SeqCst) >= self.fail_after {
            return Some(Error::canceled());
        }
        None
    }
}

/// Go `indexWriterSeries`.
struct IndexWriterSeries {
    labels: Labels,
    chunks: Vec<Meta>,
}

fn iws(labels: Labels) -> IndexWriterSeries {
    IndexWriterSeries {
        labels,
        chunks: Vec::new(),
    }
}

fn meta(r: u64, mint: i64, maxt: i64) -> Meta {
    Meta {
        chunk_ref: r,
        chunk: None,
        min_time: mint,
        max_time: maxt,
    }
}

fn metas_eq(a: &[Meta]) -> Vec<(u64, i64, i64)> {
    a.iter()
        .map(|m| (m.chunk_ref, m.min_time, m.max_time))
        .collect()
}

/// Go `createFileReader`: writes `input` to a temporary index file and returns a reader for it, the temp dir,
/// the file's name, and the symbol set.
fn create_file_reader(input: &[IndexWriterSeries]) -> (Reader, TempDir, PathBuf, HashSet<String>) {
    let dir = TempDir::new("reader");
    let fname = dir.path().join(INDEX_FILENAME);

    let mut iw = Writer::new(&Context::background(), &fname).unwrap();

    let mut symbols: HashSet<String> = HashSet::new();
    for s in input {
        s.labels.range(|n, v| {
            symbols.insert(n.to_string());
            symbols.insert(v.to_string());
        });
    }
    let mut syms: Vec<&String> = symbols.iter().collect();
    syms.sort();
    for s in syms {
        iw.add_symbol(s).unwrap();
    }
    for (i, s) in input.iter().enumerate() {
        iw.add_series(i as SeriesRef, &s.labels, &s.chunks).unwrap();
    }
    iw.close().unwrap();

    let ir = Reader::open(&fname, decode_postings_raw).unwrap();
    (ir, dir, fname, symbols)
}

/// Go `mockIndex`.
struct MockIndex {
    series: HashMap<SeriesRef, (Labels, Vec<(u64, i64, i64)>)>,
    postings: HashMap<Label, Vec<SeriesRef>>,
    symbols: HashSet<String>,
}

impl MockIndex {
    fn new() -> MockIndex {
        let mut postings = HashMap::new();
        let (n, v) = all_postings_key();
        postings.insert(Label::new(n, v), Vec::new());
        MockIndex {
            series: HashMap::new(),
            postings,
            symbols: HashSet::new(),
        }
    }

    fn add_series(&mut self, r: SeriesRef, l: &Labels, chunks: &[Meta]) -> Result<(), String> {
        if self.series.contains_key(&r) {
            return Err(format!("series with reference {r} already added"));
        }
        l.range(|n, v| {
            self.symbols.insert(n.to_string());
            self.symbols.insert(v.to_string());
            self.postings.entry(Label::new(n, v)).or_default().push(r);
        });
        let (n, v) = all_postings_key();
        self.postings.get_mut(&Label::new(n, v)).unwrap().push(r);
        // Actual chunk data is not stored in the index.
        self.series.insert(r, (l.clone(), metas_eq(chunks)));
        Ok(())
    }

    fn postings(&self, name: &str, values: &[&str]) -> BoxPostings<'static> {
        let mut p = Vec::new();
        for value in values {
            let l = Label::new(name, *value);
            let list = self.postings.get(&l).cloned().unwrap_or_default();
            p.push(self.sorted_postings(Box::new(new_list_postings(list))));
        }
        merge(p)
    }

    fn sorted_postings(&self, mut p: BoxPostings<'static>) -> BoxPostings<'static> {
        let mut ep = match expand_postings(&mut p) {
            Ok(ep) => ep,
            Err(e) => return err_postings(Error::wrap("expand postings", e)),
        };
        ep.sort_by(|a, b| labels::compare(&self.series[a].0, &self.series[b].0).cmp(&0));
        Box::new(new_list_postings(ep))
    }

    fn series(
        &self,
        r: SeriesRef,
        builder: &mut ScratchBuilder,
        chks: &mut Vec<(u64, i64, i64)>,
    ) -> Result<(), String> {
        let Some(s) = self.series.get(&r) else {
            return Err("not found".to_string());
        };
        builder.assign(s.0.clone());
        *chks = s.1.clone();
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Tests.

#[test]
fn test_index_rw_create_open() {
    let dir = TempDir::new("create_open");
    let fname = dir.path().join(INDEX_FILENAME);

    // An empty index must still result in a readable file.
    let iw = Writer::new(&Context::background(), &fname).unwrap();
    iw.close().unwrap();

    let ir = Reader::open(&fname, decode_postings_raw).unwrap();
    ir.close().unwrap();

    // Modify magic header must cause open to fail.
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&fname)
        .unwrap();
    f.write_all_at(&[0, 0], 0).unwrap();
    drop(f);

    assert!(Reader::open(dir.path(), decode_postings_raw).is_err());
}

#[test]
fn test_index_rw_postings() {
    let ctx = background();
    let input: Vec<IndexWriterSeries> = (1..5)
        .map(|i| iws(labels::from_strings(&["a", "1", "b", &i.to_string()])))
        .collect();
    let (ir, _dir, fname, _) = create_file_reader(&input);

    let mut p = ir.postings(ctx, "a", &["1"]).unwrap();

    let mut c: Vec<Meta> = Vec::new();
    let mut builder = ScratchBuilder::default();

    let mut i = 0;
    while p.next() {
        ir.series(p.at(), &mut builder, Some(&mut c)).unwrap();
        assert!(c.is_empty());
        assert_eq!(input[i].labels, builder.labels());
        i += 1;
    }
    assert!(p.err().is_none());

    // ShardedPostings().
    {
        let ir = Reader::open(&fname, decode_postings_raw).unwrap();

        // List all postings for a given label value. This is what we expect to get in output from all shards.
        let mut p = ir.postings(ctx, "a", &["1"]).unwrap();
        let mut expected: Vec<SeriesRef> = Vec::new();
        while p.next() {
            expected.push(p.at());
        }
        assert!(p.err().is_none());
        assert!(!expected.is_empty());

        // Query the same postings for each shard.
        const SHARD_COUNT: u64 = 4;
        let mut actual_shards: HashMap<u64, Vec<SeriesRef>> = HashMap::new();
        let mut actual_postings: Vec<SeriesRef> = Vec::with_capacity(expected.len());

        for shard_index in 0..SHARD_COUNT {
            let p = ir.postings(ctx, "a", &["1"]).unwrap();
            let mut p = ir.sharded_postings(p, shard_index, SHARD_COUNT);
            while p.next() {
                let r = p.at();
                actual_shards.entry(shard_index).or_default().push(r);
                actual_postings.push(r);
            }
            assert!(p.err().is_none());
        }

        // We expect the postings merged out of shards is the exact same of the non sharded ones.
        let mut e = expected.clone();
        e.sort_unstable();
        actual_postings.sort_unstable();
        assert_eq!(e, actual_postings);

        // We expect the series in each shard are the expected ones.
        for (shard_index, ids) in &actual_shards {
            for &id in ids {
                let mut lbls = ScratchBuilder::default();
                ir.series(id, &mut lbls, None).unwrap();
                assert_eq!(*shard_index, stable_hash(&lbls.labels()) % SHARD_COUNT);
            }
        }

        let err_expected = Error::new("postings error");
        for (name, refs) in [("empty", Vec::new()), ("non-empty", expected.clone())] {
            let p = merge(vec![
                Box::new(new_list_postings(refs)) as BoxPostings,
                err_postings(err_expected.clone()),
            ]);
            let mut p = ir.sharded_postings(p, 0, SHARD_COUNT);
            assert!(!p.next(), "{name}");
            assert!(p.err().is_some_and(|e| e.is(&err_expected)), "{name}");
        }
        drop(p);
        ir.close().unwrap();
    }
}

#[test]
fn test_postings_many() {
    let ctx = background();
    // Create a label in the index which has 999 values.
    let input: Vec<IndexWriterSeries> = (1..1000)
        .map(|i| {
            iws(labels::from_strings(&[
                "i",
                &format!("{i:03}"),
                "foo",
                "bar",
            ]))
        })
        .collect();
    let (ir, _dir, _, symbols) = create_file_reader(&input);

    let cases: Vec<Vec<&str>> = vec![
        // Simple cases, everything is present.
        vec!["002"],
        vec!["031", "032", "033"],
        vec!["032", "033"],
        vec!["127", "128"],
        vec!["127", "128", "129"],
        vec!["127", "129"],
        vec!["128", "129"],
        vec!["998", "999"],
        vec!["999"],
        // Before actual values.
        vec!["000"],
        vec!["000", "001"],
        vec!["000", "002"],
        // After actual values.
        vec!["999a"],
        vec!["999", "999a"],
        vec!["998", "999", "999a"],
        // In the middle of actual values.
        vec!["126a", "127", "128"],
        vec!["127", "127a", "128"],
        vec!["127", "127a", "128", "128a", "129"],
        vec!["127", "128a", "129"],
        vec!["128", "128a", "129"],
        vec!["128", "129", "129a"],
        vec![
            "126a", "126b", "127", "127a", "127b", "128", "128a", "128b", "129", "129a", "129b",
        ],
    ];

    let mut builder = ScratchBuilder::default();
    for c in cases {
        let mut it = ir.postings(ctx, "i", &c).unwrap();

        let mut got: Vec<String> = Vec::new();
        let mut metas: Vec<Meta> = Vec::new();
        while it.next() {
            ir.series(it.at(), &mut builder, Some(&mut metas)).unwrap();
            got.push(builder.labels().get("i").to_string());
        }
        assert!(it.err().is_none());
        let exp: Vec<String> = c
            .iter()
            .filter(|e| symbols.contains(**e) && **e != "l")
            .map(|e| e.to_string())
            .collect();
        assert_eq!(exp, got, "input: {c:?}");
    }
}

#[test]
fn test_persistence_index_e2e() {
    let ctx = background();
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../upstream/tsdb/testdata/20kseries.json"
    );
    let mut lbls = read_labels(path, 20000).unwrap();
    // Sort labels as the index writer expects series in sorted order.
    lbls.sort_by(|a, b| labels::compare(a, b).cmp(&0));

    let mut input: Vec<IndexWriterSeries> = Vec::new();
    let mut r: u64 = 0;
    // Generate chunk metas for every label set.
    for (i, lset) in lbls.into_iter().enumerate() {
        let mut metas = Vec::new();
        for j in 0..=(i % 20) as i64 {
            r += 1;
            metas.push(Meta {
                chunk_ref: r,
                chunk: Some(std::sync::Arc::new(chunkenc::xor::XorChunk::new())),
                min_time: j * 10000,
                max_time: (j + 1) * 10000 - 1,
            });
        }
        input.push(IndexWriterSeries {
            labels: lset,
            chunks: metas,
        });
    }

    let (ir, _dir, _, _) = create_file_reader(&input);

    // Population procedure as done by compaction.
    let postings = MemPostings::new();
    let mut values: HashMap<String, HashSet<String>> = HashMap::new();

    let mut mi = MockIndex::new();

    for (i, s) in input.iter().enumerate() {
        mi.add_series(i as SeriesRef, &s.labels, &s.chunks).unwrap();
        s.labels.range(|n, v| {
            values
                .entry(n.to_string())
                .or_default()
                .insert(v.to_string());
        });
        postings.add(i as SeriesRef, &s.labels);
    }

    for p in mi.postings.keys() {
        let mut gotp = ir.postings(ctx, &p.name, &[&p.value]).unwrap();
        let mut expp = mi.postings(&p.name, &[&p.value]);

        let mut chks: Vec<Meta> = Vec::new();
        let mut expchks: Vec<(u64, i64, i64)> = Vec::new();
        let mut builder = ScratchBuilder::default();
        let mut e_builder = ScratchBuilder::default();

        while gotp.next() {
            assert!(expp.next());

            let r = gotp.at();

            ir.series(r, &mut builder, Some(&mut chks)).unwrap();
            mi.series(expp.at(), &mut e_builder, &mut expchks).unwrap();
            assert_eq!(e_builder.labels(), builder.labels());
            assert_eq!(expchks, metas_eq(&chks));
        }
        assert!(
            !expp.next(),
            "Expected no more postings for {:?}={:?}",
            p.name,
            p.value
        );
        assert!(gotp.err().is_none());
    }

    let mut label_pairs: HashMap<String, Vec<String>> = HashMap::new();
    for l in mi.postings.keys() {
        label_pairs
            .entry(l.name.clone())
            .or_default()
            .push(l.value.clone());
    }
    for (k, mut v) in label_pairs {
        v.sort();

        let res = ir.sorted_label_values(ctx, &k, None, &[]).unwrap();

        assert_eq!(res.len(), v.len());
        for i in 0..v.len() {
            assert_eq!(v[i], res[i]);
        }
    }

    let mut got_symbols: Vec<String> = Vec::new();
    let mut it = ir.symbols();
    while it.next() {
        got_symbols.push(it.at().to_string());
    }
    assert!(it.err().is_none());
    let mut exp_symbols: Vec<String> = mi.symbols.iter().cloned().collect();
    exp_symbols.sort();
    assert_eq!(exp_symbols, got_symbols);
}

#[test]
fn test_writer_should_return_error_on_series_with_duplicated_label_names() {
    let dir = TempDir::new("dup_names");
    let mut w = Writer::new(&Context::background(), &dir.path().join("index")).unwrap();

    w.add_symbol("__name__").unwrap();
    w.add_symbol("metric_1").unwrap();
    w.add_symbol("metric_2").unwrap();

    w.add_series(
        0,
        &labels::from_strings(&["__name__", "metric_1", "__name__", "metric_2"]),
        &[],
    )
    .unwrap();

    let err = w.close().unwrap_err();
    assert!(
        err.to_string()
            .contains("corruption detected when writing postings to index"),
        "{err}"
    );
}

#[test]
fn test_decbuf_uvarint_with_invalid_buffer() {
    let b: Vec<u8> = vec![0x81, 0x81, 0x81, 0x81, 0x81, 0x81];

    let db = Decbuf::new_uvarint_at(&b[..], 0);
    assert!(db.err().is_some());
}

#[test]
fn test_reader_with_invalid_buffer() {
    let b: Vec<u8> = vec![0x81, 0x81, 0x81, 0x81, 0x81, 0x81];

    assert!(Reader::new(b, decode_postings_raw).is_err());
}

/// Number of open file descriptors (including mappings) of this process that refer to `p`.
fn open_handles_to(p: &Path) -> usize {
    let mut n = 0;
    if let Ok(rd) = std::fs::read_dir("/proc/self/fd") {
        for e in rd.flatten() {
            if std::fs::read_link(e.path()).is_ok_and(|t| t == p) {
                n += 1;
            }
        }
    }
    if let Ok(maps) = std::fs::read_to_string("/proc/self/maps") {
        let s = p.to_string_lossy();
        n += maps.lines().filter(|l| l.ends_with(s.as_ref())).count();
    }
    n
}

/// Ensures that in case of an error no file remains open.
#[test]
fn test_new_file_reader_error_no_open_files() {
    let dir = TempDir::new("block");

    let idx_name = dir.path().join("index");
    std::fs::write(&idx_name, b"corrupted contents").unwrap();

    assert!(Reader::open(&idx_name, decode_postings_raw).is_err());

    // adapted: Go checks that removing the directory works on Windows; here we check directly that neither a file
    // descriptor nor a mapping of the index remains.
    assert_eq!(0, open_handles_to(&idx_name.canonicalize().unwrap()));
}

#[test]
fn test_symbols() {
    let mut buf = Encbuf::new();

    // Add prefix to the buffer to simulate symbols as part of larger buffer.
    buf.put_uvarint_str("something");

    let symbols_start = buf.len();
    buf.put_be32int(204); // Length of symbols table.
    buf.put_be32int(100); // Number of symbols.
    for i in 0..100u32 {
        // i represents index in unicode characters table.
        buf.put_uvarint_str(&char::from_u32(i).unwrap().to_string()); // Symbol.
    }
    let checksum = crc32c(&buf.get()[symbols_start + 4..]);
    buf.put_be32(checksum); // Check sum at the end.

    let b = buf.get();
    let s = Symbols::new(b, FORMAT_V2, symbols_start).unwrap();

    // We store only 4 offsets to symbols.
    assert_eq!(32, s.size());

    for i in (0..100u32).rev() {
        let sym = s.lookup(b, i).unwrap();
        assert_eq!(char::from_u32(i).unwrap().to_string(), sym);
    }
    assert!(s.lookup(b, 100).is_err());

    for i in (0..100u32).rev() {
        let r = s
            .reverse_lookup(b, &char::from_u32(i).unwrap().to_string())
            .unwrap();
        assert_eq!(i, r);
    }
    assert!(
        s.reverse_lookup(b, &char::from_u32(100).unwrap().to_string())
            .is_err()
    );

    let mut iter = s.iter(b);
    let mut i = 0u32;
    while iter.next() {
        assert_eq!(char::from_u32(i).unwrap().to_string(), iter.at());
        i += 1;
    }
    assert!(iter.err().is_none());
}

#[test]
fn test_decoder_postings_wrong_input() {
    let d = Decbuf::new(b"the cake is a lie");
    let dec = Decoder {
        decode_postings: decode_postings_raw,
    };
    assert!((dec.decode_postings)(d).is_err());
}

#[test]
fn test_chunks_ref_ordering() {
    let dir = TempDir::new("chunks_ref");

    let mut idx = Writer::new(&Context::background(), &dir.path().join("index")).unwrap();

    idx.add_symbol("1").unwrap();
    idx.add_symbol("2").unwrap();
    idx.add_symbol("__name__").unwrap();

    let c50 = || meta(50, 0, 0);
    let c100 = || meta(100, 0, 0);
    let c200 = || meta(200, 0, 0);

    idx.add_series(1, &labels::from_strings(&["__name__", "1"]), &[c100()])
        .unwrap();
    assert_eq!(
        "unsorted chunk reference: 50, previous: 100",
        idx.add_series(2, &labels::from_strings(&["__name__", "2"]), &[c50()])
            .unwrap_err()
            .to_string()
    );
    idx.add_series(2, &labels::from_strings(&["__name__", "2"]), &[c200()])
        .unwrap();
    idx.close().unwrap();
}

#[test]
fn test_chunks_time_ordering() {
    let dir = TempDir::new("chunks_time");

    let mut idx = Writer::new(&Context::background(), &dir.path().join("index")).unwrap();

    idx.add_symbol("1").unwrap();
    idx.add_symbol("2").unwrap();
    idx.add_symbol("__name__").unwrap();

    idx.add_series(
        1,
        &labels::from_strings(&["__name__", "1"]),
        &[
            meta(1, 0, 10), // Also checks that first chunk can have MinTime: 0.
            meta(2, 11, 20),
            meta(3, 21, 30),
        ],
    )
    .unwrap();

    assert_eq!(
        "chunk minT 10 is not higher than previous chunk maxT 10",
        idx.add_series(
            1,
            &labels::from_strings(&["__name__", "2"]),
            &[meta(10, 0, 10), meta(20, 10, 20)]
        )
        .unwrap_err()
        .to_string()
    );

    assert_eq!(
        "chunk maxT 30 is less than minT 100",
        idx.add_series(
            1,
            &labels::from_strings(&["__name__", "2"]),
            &[meta(10, 100, 30)]
        )
        .unwrap_err()
        .to_string()
    );

    idx.close().unwrap();
}

fn name_series(n: usize, fmt4: bool) -> Vec<IndexWriterSeries> {
    (1..=n)
        .map(|i| {
            let v = if fmt4 {
                format!("{i:4}")
            } else {
                i.to_string()
            };
            IndexWriterSeries {
                labels: labels::from_strings(&["__name__", &v]),
                chunks: vec![meta(1, 0, 10)],
            }
        })
        .collect()
}

#[test]
fn test_reader_postings_for_label_matching() {
    let input = name_series(9, false);
    let (ir, _dir, _, _) = create_file_reader(&input);

    let mut p = ir.postings_for_label_matching(background(), "__name__", &|v: &str| {
        let iv: i64 = v.parse().unwrap();
        iv % 2 == 0
    });
    assert!(p.err().is_none());
    let refs = expand_postings(&mut p).unwrap();
    assert_eq!(vec![4, 6, 8, 10], refs);
}

#[test]
fn test_reader_postings_for_all_label_values() {
    let input = name_series(9, false);
    let (ir, _dir, _, _) = create_file_reader(&input);

    let mut p = ir.postings_for_all_label_values(background(), "__name__");
    assert!(p.err().is_none());
    let refs = expand_postings(&mut p).unwrap();
    assert_eq!(vec![3, 4, 5, 6, 7, 8, 9, 10, 11], refs);
}

#[test]
fn test_reader_postings_for_label_matching_honors_context_cancel() {
    const SERIES_COUNT: usize = 1000;
    let input = name_series(SERIES_COUNT, true);
    let (ir, _dir, _, _) = create_file_reader(&input);

    let fail_after = (SERIES_COUNT / 2) as u64; // Fail after processing half of the series.
    let ctx = MockContextErrAfter::new(fail_after);
    let p = ir.postings_for_label_matching(&ctx, "__name__", &|_| true);
    assert!(p.err().is_some());
    assert_eq!(fail_after, ctx.count());
}

#[test]
fn test_reader_label_names_for_honors_context_cancel() {
    const SERIES_COUNT: usize = 1000;
    let input = name_series(SERIES_COUNT, true);
    let (ir, _dir, _, _) = create_file_reader(&input);

    let (name, value) = all_postings_key();
    let mut p = ir.postings(background(), name, &[value]).unwrap();
    // We check context cancellation every 128 iterations so 3 will fail after iterating 3 * 128 series.
    let fail_after = 3u64;
    let ctx = MockContextErrAfter::new(fail_after);
    assert!(ir.label_names_for(&ctx, &mut *p).is_err());
    assert_eq!(fail_after, ctx.count());
}
