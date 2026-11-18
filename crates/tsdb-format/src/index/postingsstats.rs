//! Port of `tsdb/index/postingsstats.go`: the top-N heap of cardinality statistics.

/// Go `Stat`: values for a single cardinality statistic.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Stat {
    pub name: String,
    pub count: u64,
}

/// Go `PostingsStats`: cardinality based statistics for postings.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PostingsStats {
    pub cardinality_metrics_stats: Vec<Stat>,
    pub cardinality_label_stats: Vec<Stat>,
    pub label_value_stats: Vec<Stat>,
    pub label_value_pairs_stats: Vec<Stat>,
    pub num_label_pairs: usize,
}

/// Go `maxHeap`: keeps the `max_length` items with the highest counts.
#[derive(Clone, Debug, Default)]
pub struct MaxHeap {
    max_length: usize,
    min_value: u64,
    min_index: usize,
    pub items: Vec<Stat>,
}

impl MaxHeap {
    /// Go `init`.
    pub fn init(&mut self, length: usize) {
        self.max_length = length;
        self.min_value = u64::MAX;
        self.items = Vec::with_capacity(length);
    }

    /// Go `push`.
    pub fn push(&mut self, item: Stat) {
        if self.items.len() < self.max_length {
            if item.count < self.min_value {
                self.min_value = item.count;
                self.min_index = self.items.len();
            }
            self.items.push(item);
            return;
        }
        if item.count < self.min_value {
            return;
        }

        self.min_value = item.count;
        self.items[self.min_index] = item;

        for (i, stat) in self.items.iter().enumerate() {
            if stat.count < self.min_value {
                self.min_value = stat.count;
                self.min_index = i;
            }
        }
    }

    /// Go `get`: the items sorted by count, highest first.
    pub fn get(&mut self) -> Vec<Stat> {
        self.items.sort_by(|a, b| b.count.cmp(&a.count));
        std::mem::take(&mut self.items)
    }
}
