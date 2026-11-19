//! Port of `tsdb/chunks/chunks_test.go`.

use chunkenc::{ChunkIterator, ValueType};
use model::histogram::{Histogram, Span};
use std::path::PathBuf;
use tsdb_format::chunks::{BasicSample, Meta, Reader, Sample, chunk_from_samples, new_writer};

/// A unique empty directory for one test (Go `t.TempDir()`).
fn temp_dir(name: &str) -> PathBuf {
    let d = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("chunks_test-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// Go `tsdbutil.GenerateTestHistogram`.
fn generate_test_histogram(i: i64) -> Histogram {
    Histogram {
        count: 12 + (i * 9) as u64,
        zero_count: 2 + i as u64,
        zero_threshold: 0.001,
        sum: 18.4 * (i + 1) as f64,
        schema: 1,
        positive_spans: vec![Span::new(0, 2), Span::new(1, 2)],
        positive_buckets: vec![i + 1, 1, -1, 0],
        negative_spans: vec![Span::new(0, 2), Span::new(1, 2)],
        negative_buckets: vec![i + 1, 1, -1, 0],
        ..Default::default()
    }
}

#[test]
fn test_reader_with_invalid_buffer() {
    let b = vec![0x81u8, 0x81, 0x81, 0x81, 0x81, 0x81];
    let r = Reader::from_raw_segments(vec![b]);

    let res = r.chunk_or_iterable(&Meta {
        chunk_ref: 0,
        ..Default::default()
    });
    assert!(res.is_err());
}

#[test]
fn test_writer_with_default_segment_size() {
    let chk1 = chunk_from_samples(&[
        BasicSample::float(10, 11.0),
        BasicSample::float(20, 12.0),
        BasicSample::float(30, 13.0),
    ])
    .unwrap();

    let chk2 = chunk_from_samples(&[
        BasicSample::histogram(40, generate_test_histogram(1)),
        BasicSample::histogram(50, generate_test_histogram(2)),
        BasicSample::histogram(60, generate_test_histogram(3)),
    ])
    .unwrap();

    let dir = temp_dir("default_segment_size");
    let mut w = new_writer(&dir, &[]).unwrap();

    let mut chks = [chk1, chk2];
    w.write_chunks(&mut chks).unwrap();

    w.close().unwrap();

    let d: Vec<_> = std::fs::read_dir(&dir).unwrap().collect();
    assert_eq!(
        1,
        d.len(),
        "expected only one segment to be created to hold both chunks"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_chunk_from_samples_with_st() {
    // Create samples with explicit ST (source timestamp) values.
    let samples = [
        BasicSample::float_st(5, 10, 11.0),
        BasicSample::float_st(15, 20, 12.0),
        BasicSample::float_st(25, 30, 13.0),
    ];

    let chk = chunk_from_samples(&samples).unwrap();
    assert!(chk.chunk.is_some());

    // Verify MinTime and MaxTime.
    assert_eq!(10, chk.min_time);
    assert_eq!(30, chk.max_time);

    // Iterate over the chunk and verify ST values are preserved.
    let c = chk.chunk.as_ref().unwrap();
    let mut it = c.iterator(None);
    let mut idx = 0;
    loop {
        let vt = it.next();
        if vt == ValueType::None {
            break;
        }
        assert_eq!(ValueType::Float, vt);
        let (ts, v) = it.at();
        let st = it.at_st();
        assert_eq!(samples[idx].st(), st, "ST mismatch at index {idx}");
        assert_eq!(samples[idx].t(), ts, "T mismatch at index {idx}");
        assert_eq!(samples[idx].f(), v, "F mismatch at index {idx}");
        idx += 1;
    }
    assert!(it.err().is_none());
    assert_eq!(samples.len(), idx, "expected all samples to be iterated");
}
