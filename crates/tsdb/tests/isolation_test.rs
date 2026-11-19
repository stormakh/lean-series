//! Port of `tsdb/isolation_test.go`.

use tsdb::isolation::Isolation;

fn count_open_reads(iso: &Isolation) -> usize {
    let mut count = 0;
    iso.traverse_open_reads(|_| {
        count += 1;
        true
    });
    count
}

#[test]
fn test_isolation() {
    let iso = Isolation::new(false);

    // Low watermark starts at 1.
    assert_eq!(0, iso.low_watermark());
    assert_eq!(i64::MAX, iso.lowest_append_time());

    // Pretend we are starting to append.
    let append_a = iso.new_append_id(10);
    assert_eq!((1, 1), append_a);
    assert_eq!(1, iso.low_watermark());

    assert_eq!(0, count_open_reads(&iso));
    assert_eq!(10, iso.lowest_append_time());

    // Now we start a read.
    let state_a = iso.state(10, 20);
    assert_eq!(1, count_open_reads(&iso));

    // Second appender.
    let append_b = iso.new_append_id(20);
    assert_eq!((2, 1), append_b);
    assert_eq!(1, iso.low_watermark());
    assert_eq!(10, iso.lowest_append_time());

    iso.close_append(append_a.0);
    // Low watermark remains at 1 because stateA is still open
    assert_eq!(1, iso.low_watermark());

    assert_eq!(1, count_open_reads(&iso));
    assert_eq!(20, iso.lowest_append_time());

    // Finish the read and low watermark should rise.
    state_a.close();
    assert_eq!(2, iso.low_watermark());

    assert_eq!(0, count_open_reads(&iso));

    iso.close_append(append_b.0);
    assert_eq!(2, iso.low_watermark());
    assert_eq!(i64::MAX, iso.lowest_append_time());
}

#[test]
fn test_committed_append_id() {
    // disabled isolation always returns 0
    {
        let iso = Isolation::new(true);
        assert_eq!(0, iso.committed_append_id());
        let _ = iso.new_append_id(0);
        assert_eq!(0, iso.committed_append_id());
    }
    // no appenders ever opened
    {
        let iso = Isolation::new(false);
        assert_eq!(0, iso.committed_append_id());
    }
    // all issued IDs closed returns lastAppendID
    {
        let iso = Isolation::new(false);
        let (id_a, _) = iso.new_append_id(0);
        let (id_b, _) = iso.new_append_id(0);
        iso.close_append(id_a);
        iso.close_append(id_b);
        assert_eq!(id_b, iso.committed_append_id());
        assert_eq!(iso.last_append_id(), iso.committed_append_id());
    }
    // single open appender caps committed at id-1
    {
        let iso = Isolation::new(false);
        let (id, _) = iso.new_append_id(0);
        assert_eq!(id - 1, iso.committed_append_id());
    }
    // multiple open appenders track the lowest
    {
        let iso = Isolation::new(false);
        let (id_a, _) = iso.new_append_id(0);
        let (id_b, _) = iso.new_append_id(0);
        let (id_c, _) = iso.new_append_id(0);
        assert_eq!(id_a - 1, iso.committed_append_id());
        iso.close_append(id_c);
        assert_eq!(id_a - 1, iso.committed_append_id());
        iso.close_append(id_a);
        assert_eq!(id_b - 1, iso.committed_append_id());
        iso.close_append(id_b);
        assert_eq!(id_c, iso.committed_append_id());
    }
    // open reads do not influence committed
    {
        let iso = Isolation::new(false);
        let (id_a, _) = iso.new_append_id(0);
        let state = iso.state(0, i64::MAX);
        iso.close_append(id_a);
        assert_eq!(id_a, iso.committed_append_id());
        state.close();
        assert_eq!(id_a, iso.committed_append_id());
    }
}
