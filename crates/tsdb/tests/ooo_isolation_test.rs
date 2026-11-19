//! Port of `tsdb/ooo_isolation_test.go`.

use tsdb::ooo_isolation::OooIsolation;
use tsdb_format::chunks::ChunkDiskMapperRef as R;

#[test]
fn test_ooo_isolation() {
    let i = OooIsolation::new();

    // Empty state shouldn't have any open reads.
    assert!(!i.has_open_reads_at_or_before(R(0)));
    assert!(!i.has_open_reads_at_or_before(R(1)));
    assert!(!i.has_open_reads_at_or_before(R(2)));
    assert!(!i.has_open_reads_at_or_before(R(3)));

    // Add a read.
    let read1 = i.track_read_after(R(1));
    assert!(!i.has_open_reads_at_or_before(R(0)));
    assert!(!i.has_open_reads_at_or_before(R(1)));
    assert!(i.has_open_reads_at_or_before(R(2)));

    // Add another overlapping read.
    let read2 = i.track_read_after(R(0));
    assert!(!i.has_open_reads_at_or_before(R(0)));
    assert!(i.has_open_reads_at_or_before(R(1)));
    assert!(i.has_open_reads_at_or_before(R(2)));

    // Close the second read, should now only report open reads for the first read's ref.
    read2.close();
    assert!(!i.has_open_reads_at_or_before(R(0)));
    assert!(!i.has_open_reads_at_or_before(R(1)));
    assert!(i.has_open_reads_at_or_before(R(2)));

    // Close the second read again: this should do nothing and ensures we can safely call Close() multiple times.
    read2.close();
    assert!(!i.has_open_reads_at_or_before(R(0)));
    assert!(!i.has_open_reads_at_or_before(R(1)));
    assert!(i.has_open_reads_at_or_before(R(2)));

    // Closing the first read should indicate no further open reads.
    read1.close();
    assert!(!i.has_open_reads_at_or_before(R(0)));
    assert!(!i.has_open_reads_at_or_before(R(1)));
    assert!(!i.has_open_reads_at_or_before(R(2)));
}
