//! Port of `storage/remote/chunked_test.go`.

use std::cell::Cell;
use std::io::Cursor;
use std::rc::Rc;

use remote::chunked::{ChunkedReader, ChunkedWriter, Flusher, NopFlusher, ERR_EOF};

struct MockedFlusher(Rc<Cell<i32>>);

impl Flusher for MockedFlusher {
    fn flush(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

#[test]
fn test_chunked_reader_can_read_from_chunked_writer() {
    let flushed = Rc::new(Cell::new(0));
    let mut w = ChunkedWriter::new(Vec::new(), MockedFlusher(flushed.clone()));

    let msgs: Vec<&[u8]> = vec![
        b"test1",
        b"test2",
        b"test3",
        b"test4",
        b"", // This is ignored by writer.
        b"test5-after-empty",
    ];

    for msg in &msgs {
        let n = w.write_frame(msg).unwrap();
        assert_eq!(msg.len(), n);
    }
    w.close();

    let (b, _) = w.into_inner();
    let mut r = ChunkedReader::new(Cursor::new(b), 20, Vec::new());

    let mut i = 0;
    while i < 4 {
        let msg = r.next().unwrap();
        assert!(i < msgs.len(), "more messages than expected");
        assert_eq!(msgs[i], msg);
        i += 1;
    }

    // Empty byte slice is skipped.
    i += 1;

    let msg = r.next().unwrap();
    assert!(i < msgs.len(), "more messages than expected");
    assert_eq!(msgs[i], msg);

    let err = r.next().expect_err("expected io.EOF");
    assert!(err.is(&ERR_EOF));

    assert_eq!(1, flushed.get());
}

#[test]
fn test_chunked_reader_overflow() {
    let mut w = ChunkedWriter::new(Vec::new(), NopFlusher);
    w.write_frame(b"twelve bytes").unwrap();
    let (b, _) = w.into_inner();

    let b2 = b[..12].to_vec();

    let mut r = ChunkedReader::new(Cursor::new(b), 12, Vec::new());
    let ret = r.next().unwrap();
    assert_eq!("twelve bytes", std::str::from_utf8(ret).unwrap());

    let mut r2 = ChunkedReader::new(Cursor::new(b2), 11, Vec::new());
    let err = r2.next().expect_err("expect exceed limit error");
    assert_eq!("chunkedReader: message size exceeded the limit 11 bytes; got: 12 bytes", err.to_string());
}

#[test]
fn test_chunked_reader_corrupted_frame() {
    let mut w = ChunkedWriter::new(Vec::new(), NopFlusher);
    let n = w.write_frame(b"test1").unwrap();
    assert_eq!(5, n);

    let (mut bs, _) = w.into_inner();
    bs[9] = 1; // Malform the frame by changing one byte.

    let mut r = ChunkedReader::new(Cursor::new(bs), 20, Vec::new());
    let err = r.next().expect_err("expected malformed frame");
    assert_eq!("chunkedReader: corrupted frame; checksum mismatch", err.to_string());
}
