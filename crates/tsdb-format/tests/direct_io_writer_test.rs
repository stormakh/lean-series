//! Port of tsdb/fileutil/direct_io_writer_test.go (Linux).

use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use tsdb_format::fileutil::{
    DirectIoRqmts, DirectIoWriter, aligned_block, current_file_offset, enable_direct_io,
    file_direct_io_rqmts, is_aligned,
};

static N: AtomicUsize = AtomicUsize::new(0);

fn temp_dir() -> PathBuf {
    let d = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "direct_io_test-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&d);
    fs::create_dir_all(&d).unwrap();
    d
}

fn open_wo(p: &PathBuf) -> File {
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(p)
        .unwrap()
}

fn direct_io_rqmts_for_test() -> DirectIoRqmts {
    let f = open_wo(&temp_dir().join("foo"));
    file_direct_io_rqmts(&f).unwrap()
}

#[test]
fn test_direct_io_file() {
    let f = open_wo(&temp_dir().join("test"));
    enable_direct_io(f.as_raw_fd()).unwrap();
}

#[test]
fn test_aligned_block_early_panic() {
    let align_rqmts = direct_io_rqmts_for_test();
    let cases = [
        ("Zero size", 0usize),
        ("Size not multiple of offset alignment", 9973),
    ];
    for (desc, size) in cases {
        let r = std::panic::catch_unwind(|| aligned_block(size, &align_rqmts));
        assert!(r.is_err(), "{desc}: expected a panic");
    }
}

#[test]
fn test_aligned_bloc() {
    let align_rqmts = direct_io_rqmts_for_test();
    let (raw, off) = aligned_block(5 * align_rqmts.offset_align, &align_rqmts);
    let block = &raw[off..off + 5 * align_rqmts.offset_align];
    assert!(is_aligned(block, &align_rqmts));
    assert_eq!(block.len(), 5 * align_rqmts.offset_align);
    assert!(!is_aligned(&block[1..], &align_rqmts));
}

#[test]
fn test_direct_io_writer() {
    let a = direct_io_rqmts_for_test().offset_align;
    struct Case {
        name: &'static str,
        initial_offset: usize,
        buffer_size: usize,
        data_size: usize,
        // written_bytes should also consider needed zero padding.
        written_bytes: usize,
        should_invalidate: bool,
    }
    let c = |name, initial_offset, buffer_size, data_size, written_bytes, should_invalidate| Case {
        name,
        initial_offset,
        buffer_size,
        data_size,
        written_bytes,
        should_invalidate,
    };
    let cases = [
        c("data equal to buffer", 0, 8 * a, 8 * a, 8 * a, false),
        c("data exceeds buffer", 0, 4 * a, 64 * a, 64 * a, false),
        c(
            "data exceeds buffer + final offset unaligned",
            0,
            2 * a,
            4 * a + 33,
            4 * a + a,
            true,
        ),
        c("data smaller than buffer", 0, 8 * a, 3 * a, 3 * a, false),
        c(
            "data smaller than buffer + final offset unaligned",
            0,
            4 * a,
            a + 70,
            a + a,
            true,
        ),
        c("offset aligned", a, 8 * a, a, a, false),
        c(
            "initial offset unaligned + final offset unaligned",
            8,
            8 * a,
            64 * a,
            64 * a + (a - 8),
            true,
        ),
        c(
            "offset unaligned + final offset aligned",
            8,
            4 * a,
            4 * a + (a - 8),
            4 * a + (a - 8),
            false,
        ),
        c("empty data", 0, 4 * a, 0, 0, false),
    ];

    for tc in cases {
        let file_name = temp_dir().join("test");
        // Do not use 256 as it may be a divider of requiredAlignment. To avoid patterns.
        let data: Vec<u8> = (0..tc.data_size).map(|i| (i % 251) as u8).collect();

        let mut f = open_wo(&file_name);
        if tc.initial_offset != 0 {
            f.seek(SeekFrom::Start(tc.initial_offset as u64)).unwrap();
        }
        let mut w = DirectIoWriter::new(f, tc.buffer_size).unwrap();

        let n = w
            .write(&data)
            .unwrap_or_else(|e| panic!("{}: {e}", tc.name));
        assert_eq!(tc.data_size, n, "{}", tc.name);
        w.flush().unwrap_or_else(|e| panic!("{}: {e}", tc.name));

        // Check the file's final offset.
        let mut fc = w.file().unwrap().try_clone().unwrap();
        let curr_offset = current_file_offset(&mut fc).unwrap();
        assert_eq!(tc.data_size + tc.initial_offset, curr_offset, "{}", tc.name);

        // Check the written data.
        let file_bytes = fs::read(&file_name).unwrap();
        if tc.data_size > 0 {
            assert_eq!(
                file_bytes.len(),
                tc.written_bytes + tc.initial_offset,
                "{}",
                tc.name
            );
            assert_eq!(
                data,
                file_bytes[tc.initial_offset..tc.data_size + tc.initial_offset],
                "{}",
                tc.name
            );
        } else {
            assert!(file_bytes.is_empty(), "{}", tc.name);
        }

        // Check the writer state.
        if tc.should_invalidate {
            assert!(w.invalid, "{}", tc.name);
            assert!(w.flush().is_err(), "{}", tc.name);
            assert!(w.write(&[]).is_err(), "{}", tc.name);
        } else {
            assert!(!w.invalid, "{}", tc.name);
            w.flush().unwrap();
            w.write(&[]).unwrap();
        }
    }
}
