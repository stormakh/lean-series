//! Port of `tsdb/head_wal_replay_test.go`.

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use model::histogram::CounterResetHint;
use model::labels::{self, Labels};
use model::matcher::{MatchType, Matcher};
use storage::{Context, Sample, SeriesRef};
use tsdb::db::DB;
use tsdb::head::{mmapped_chunks_dir, MmappedChunk, RangeHead};
use tsdb::head_wal::merge_replay_mmapped_chunks;
use tsdb_format::chunks::ChunkDiskMapperRef;

/// Go `requireEqualSamples` (testutil.go).
fn require_equal_samples(name: &str, expected: &[Sample], actual: &[Sample], ignore_counter_resets: bool) {
    assert_eq!(expected.len(), actual.len(), "Length not equal to expected for {name}");
    for (i, (e, a)) in expected.iter().zip(actual).enumerate() {
        assert_eq!(e.t, a.t, "Different timestamps for {name}[{i}]");
        let typ = |s: &Sample| {
            if s.h.is_some() {
                "histogram"
            } else if s.fh.is_some() {
                "floathistogram"
            } else {
                "float"
            }
        };
        assert_eq!(typ(e), typ(a), "Different types for {name}[{i}] at ts {}", e.t);
        if let Some(eh) = &e.h {
            let mut eh = (**eh).clone();
            let mut ah = (**a.h.as_ref().unwrap()).clone();
            if ignore_counter_resets && eh.counter_reset_hint != CounterResetHint::GaugeType {
                eh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                ah.counter_reset_hint = CounterResetHint::UnknownCounterReset;
            } else {
                assert_eq!(eh.counter_reset_hint, ah.counter_reset_hint, "Sample header doesn't match for {name}[{i}]");
            }
            assert_eq!(eh, ah, "Sample doesn't match for {name}[{i}] at ts {}", e.t);
        } else if let Some(efh) = &e.fh {
            let mut efh = (**efh).clone();
            let mut afh = (**a.fh.as_ref().unwrap()).clone();
            if ignore_counter_resets {
                efh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
                afh.counter_reset_hint = CounterResetHint::UnknownCounterReset;
            } else {
                assert_eq!(efh.counter_reset_hint, afh.counter_reset_hint, "Sample header doesn't match for {name}[{i}]");
            }
            assert_eq!(efh, afh, "Sample doesn't match for {name}[{i}] at ts {}", e.t);
        } else {
            assert_eq!(e.f.to_bits(), a.f.to_bits(), "Sample doesn't match for {name}[{i}] at ts {}", e.t);
        }
    }
}

/// Go `requireEqualSeries` (testutil.go).
fn require_equal_series(
    expected: &BTreeMap<String, Vec<Sample>>,
    actual: &BTreeMap<String, Vec<Sample>>,
    ignore_counter_resets: bool,
) {
    for (name, e) in expected {
        let a = actual.get(name).unwrap_or_else(|| panic!("Expected series {name} not found"));
        require_equal_samples(name, e, a, ignore_counter_resets);
    }
    for name in actual.keys() {
        assert!(expected.contains_key(name), "Unexpected series {name}");
    }
}

/// Go `scenario.appendFunc` through `db.Appender` or `AppenderV2AsLimitedV1(db.AppenderV2)`, then `Commit`.
fn append_one(db: &DB, app_v2: bool, scenario: &common::SampleTypeScenario, ls: &Labels, ts: i64) -> SeriesRef {
    let s = (scenario.sample_func)(ts, ts);
    let ctx = Context::background();
    let is_hist = s.h.is_some() || s.fh.is_some();
    if app_v2 {
        let mut app = storage::appender_v2_as_limited_v1(db.appender_v2(&ctx));
        let r = if is_hist {
            app.append_histogram(0, ls, ts, s.h.as_deref(), s.fh.as_deref())
        } else {
            app.append(0, ls, ts, s.f)
        }
        .unwrap();
        assert!(app.commit().is_none());
        r
    } else {
        let mut app = db.appender(&ctx);
        let r = if is_hist {
            app.append_histogram(0, ls, ts, s.h.as_deref(), s.fh.as_deref())
        } else {
            app.append(0, ls, ts, s.f)
        }
        .unwrap();
        assert!(app.commit().is_none());
        r
    }
}

#[test]
fn test_wal_replay_preserves_mapped_chunks_across_series_refs() {
    const MINUTE: i64 = 60 * 1000;
    struct Tc {
        name: &'static str,
        refs: i64,
        initial_ooo: i64,
        total_ooo: i64,
        mapped_in_order: bool,
        initial_mapped_in_order: bool,
        corrupt_wal: bool,
    }
    let tc = |name, refs, initial_ooo, total_ooo, mapped_in_order, initial_mapped_in_order, corrupt_wal| Tc {
        name,
        refs,
        initial_ooo,
        total_ooo,
        mapped_in_order,
        initial_mapped_in_order,
        corrupt_wal,
    };
    let cases = [
        tc("single reference", 1, 1, 33, false, false, false),
        tc("unmapped OOO", 2, 1, 32, false, false, false),
        tc("mapped OOO under earlier reference", 2, 1, 33, false, false, false),
        tc("mapped OOO under later reference", 2, 33, 33, false, false, false),
        tc("mapped OOO under both references", 2, 33, 65, false, false, false),
        tc("three references", 3, 1, 33, false, false, false),
        tc("mapped in-order", 2, 0, 0, true, false, false),
        tc("mapped in-order under both references", 2, 0, 0, true, true, false),
        tc("mapped in-order and OOO", 2, 1, 33, true, false, false),
        tc("WAL repair with single reference", 1, 0, 0, true, false, true),
        tc("WAL repair with two references", 2, 0, 0, true, false, true),
    ];
    for app_v2 in [false, true] {
        for (name, scenario) in common::sample_type_scenarios() {
            for tc in &cases {
                let test_name = format!("{}/{}/appV2={}", tc.name, name, app_v2);
                let tmp = common::TempDir::new();
                let dir = tmp.path().to_path_buf();
                let mut opts = tsdb::db::default_options();
                opts.wal_segment_size = 32 * 1024;
                opts.max_block_chunk_segment_size = 1024 * 1024;
                opts.out_of_order_time_window = 300 * MINUTE;
                let open_db = || {
                    let db = common::new_test_db(vec![common::with_dir(&dir), common::with_opts(opts.clone())]);
                    db.disable_compactions();
                    db
                };
                let ls = labels::from_strings(&["foo", "bar"]);
                let key = ls.to_string();
                let mut expected: BTreeMap<String, Vec<Sample>> = BTreeMap::new();
                expected.insert(key.clone(), Vec::new());
                let expect_sample = |expected: &mut BTreeMap<String, Vec<Sample>>, ts: i64| {
                    expected.get_mut(&key).unwrap().push((scenario.sample_func)(ts, ts));
                };
                let check_samples = |db: &DB, expected: &BTreeMap<String, Vec<Sample>>| {
                    let q = db.querier(i64::MIN, i64::MAX).unwrap();
                    let actual = common::query(q, &[Matcher::new(MatchType::Equal, "foo", "bar").unwrap()]);
                    require_equal_series(expected, &actual, true);
                    assert_eq!(0.0, db.head.metrics.mmap_chunk_corruption_total.get(), "{test_name}");
                };

                let db = open_db();
                let mut first_ref: SeriesRef = 0;
                // Compact and recreate the series, leaving its earlier definitions in the WAL.
                for i in 1..tc.refs {
                    let ts = i * 100 * MINUTE;
                    let r = append_one(&db, app_v2, &scenario, &ls, ts);
                    if i == 1 {
                        first_ref = r;
                    }
                    expect_sample(&mut expected, ts);
                    db.compact_head(&RangeHead::new(db.head.clone(), db.head.min_time(), ts)).unwrap();
                    assert_eq!(0, db.head.num_series(), "{test_name}");
                }
                let r = append_one(&db, app_v2, &scenario, &ls, 300 * MINUTE);
                if tc.refs > 1 {
                    assert_ne!(first_ref, r, "{test_name}");
                } else {
                    first_ref = r;
                }
                for i in 0..tc.initial_ooo {
                    append_one(&db, app_v2, &scenario, &ls, 250 * MINUTE + i);
                }
                if tc.initial_mapped_in_order {
                    append_one(&db, app_v2, &scenario, &ls, 400 * MINUTE);
                    db.head.mmap_head_chunks();
                }
                db.close().unwrap();
                drop(db);

                // Replay merges the references. Subsequent chunks belong to the earlier reference.
                let db = open_db();
                assert_eq!(first_ref, db.head.series.get_by_hash(ls.hash(), &ls).unwrap().r#ref, "{test_name}");
                let (segment, offset) = db.head.wal().unwrap().last_segment_and_offset().unwrap();
                for i in tc.initial_ooo..tc.total_ooo {
                    append_one(&db, app_v2, &scenario, &ls, 250 * MINUTE + i);
                }
                for i in 0..tc.total_ooo {
                    expect_sample(&mut expected, 250 * MINUTE + i);
                }
                expect_sample(&mut expected, 300 * MINUTE);
                if tc.mapped_in_order {
                    for minute in [400i64, 500, 650] {
                        let ts = minute * MINUTE;
                        if !tc.initial_mapped_in_order || minute != 400 {
                            append_one(&db, app_v2, &scenario, &ls, ts);
                        }
                        expect_sample(&mut expected, ts);
                    }
                    db.head.mmap_head_chunks();
                    assert_eq!(
                        3,
                        db.head.series.get_by_hash(ls.hash(), &ls).unwrap().lock().mmapped_chunks.len(),
                        "{test_name}"
                    );
                }
                check_samples(&db, &expected);
                let wal_dir = db.head.wal().unwrap().dir().to_path_buf();
                db.close().unwrap();
                drop(db);

                if tc.corrupt_wal {
                    // The chunks at 300, 400 and 500 are safely on disk. Only 650 still depends on the WAL tail
                    // which repair will discard.
                    use std::os::unix::fs::FileExt;
                    let f = std::fs::OpenOptions::new()
                        .write(true)
                        .open(tsdb_format::wlog::segment_name(&wal_dir, segment))
                        .unwrap();
                    f.write_at(&[255], offset as u64).unwrap();
                    drop(f);
                    expected.get_mut(&key).unwrap().pop();
                }

                for _ in 0..3 {
                    let db = open_db();
                    check_samples(&db, &expected);
                    db.close().unwrap();
                    drop(db);
                    if tc.mapped_in_order {
                        // Replaying samples already covered by mapped chunks must not write duplicate chunks
                        // that the next restart would treat as corruption.
                        let mapper = tsdb_format::chunks::ChunkDiskMapper::new(
                            &mmapped_chunks_dir(&dir),
                            Some(Arc::new(chunkenc::new_pool())),
                            tsdb_format::chunks::DEFAULT_WRITE_BUFFER_SIZE,
                            0,
                        )
                        .unwrap();
                        let mut count = 0;
                        mapper
                            .iterate_all_chunks(|_, _, _, _, _, _, ooo| {
                                if !ooo {
                                    count += 1;
                                }
                                Ok(())
                            })
                            .unwrap();
                        mapper.close().unwrap();
                        assert_eq!(3, count, "{test_name}");
                    }
                }
            }
        }
    }
}

fn mc(r: u64, min_time: i64, max_time: i64) -> MmappedChunk {
    MmappedChunk {
        r#ref: ChunkDiskMapperRef(r),
        num_samples: 0,
        encoding: chunkenc::ENC_NONE,
        min_time,
        max_time,
    }
}

#[test]
fn test_merge_replay_mmapped_chunks() {
    let a = mc(1, 100, 150);
    let b = mc(2, 200, 250);
    // Disk order can differ from time order after series references are merged.
    let c = mc(3, 0, 50);
    let replayed = mc(4, 300, 350);
    let overlap = mc(5, 125, 225);
    struct Tc {
        name: &'static str,
        existing: Vec<MmappedChunk>,
        incoming: Vec<MmappedChunk>,
        want: Vec<MmappedChunk>,
        last_ref: u64,
        ooo: bool,
    }
    let tc = |name, existing: &[MmappedChunk], incoming: &[MmappedChunk], want: &[MmappedChunk], last_ref, ooo| Tc {
        name,
        existing: existing.to_vec(),
        incoming: incoming.to_vec(),
        want: want.to_vec(),
        last_ref,
        ooo,
    };
    let cases = [
        tc("first definition", &[], &[a, b], &[a, b], 3, false),
        tc("empty later definition", &[a, b], &[], &[a, b], 3, false),
        tc("earlier chunks arrive later", &[b], &[c, a], &[c, a, b], 3, false),
        tc("later chunks arrive later", &[c, a], &[b], &[c, a, b], 3, false),
        tc("repeated definition", &[a, b], &[a, b], &[a, b], 3, false),
        tc("discard replayed chunks", &[c, a, replayed], &[b], &[c, a, b], 3, false),
        tc("discard all replayed chunks", &[a, b], &[], &[], 0, false),
        tc("overlapping in-order chunks", &[c, a, b], &[overlap], &[c, overlap], 5, false),
        tc("OOO sorted by disk reference", &[b], &[a, c], &[a, b, c], 3, true),
        tc("repeated OOO definition", &[a, b], &[a, b], &[a, b], 3, true),
        tc("overlapping OOO chunks", &[a, b], &[overlap], &[a, b, overlap], 5, true),
    ];
    for tc in cases {
        let existing = tc.existing.clone();
        let incoming = tc.incoming.clone();
        assert_eq!(
            tc.want,
            merge_replay_mmapped_chunks(&tc.existing, &tc.incoming, ChunkDiskMapperRef(tc.last_ref), tc.ooo),
            "{}",
            tc.name
        );
        // adapted: Go checks that the input slices are not modified; here they are shared references, so the
        // check is that their contents are unchanged.
        assert_eq!(existing, tc.existing, "do not modify the attached chunk list: {}", tc.name);
        assert_eq!(incoming, tc.incoming, "do not modify the replay inventory: {}", tc.name);
    }
}
