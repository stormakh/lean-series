//! Port of `tsdb/index/postingsstats_test.go`.

use tsdb_format::index::{MaxHeap, Stat};

#[test]
fn test_postings_stats() {
    let mut stats = MaxHeap::default();
    const MAX_COUNT: u64 = 3000000;
    const HEAP_LENGTH: usize = 10;
    stats.init(HEAP_LENGTH);
    for i in 0..MAX_COUNT {
        let item = Stat {
            name: "Label-da".to_string(),
            count: i,
        };
        stats.push(item);
    }
    stats.push(Stat {
        name: "Stuff".to_string(),
        count: 3000000,
    });

    let data = stats.get();
    assert_eq!(data.len(), 10);
    for i in 0..HEAP_LENGTH {
        assert_eq!(MAX_COUNT - i as u64, data[i].count);
    }
}

#[test]
fn test_postings_stats2() {
    let mut stats = MaxHeap::default();
    const HEAP_LENGTH: usize = 10;

    stats.init(HEAP_LENGTH);
    stats.push(Stat {
        name: "Stuff".to_string(),
        count: 10,
    });
    stats.push(Stat {
        name: "Stuff".to_string(),
        count: 11,
    });
    stats.push(Stat {
        name: "Stuff".to_string(),
        count: 1,
    });
    stats.push(Stat {
        name: "Stuff".to_string(),
        count: 6,
    });

    let data = stats.get();

    assert_eq!(data.len(), 4);
    assert_eq!(11, data[0].count);
}
