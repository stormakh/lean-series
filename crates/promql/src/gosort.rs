//! Go's `sort.Sort` and `slices.SortFunc` (pattern-defeating quicksort, not stable) and `container/heap`, so that
//! results whose order depends on the algorithm (ties, NaN) come out in Go's order.
//!
//! Port of Go 1.26 `sort/zsortinterface.go` and `slices/zsortanyfunc.go`; both generate the same algorithm, only
//! the comparison differs (`Less(i, j)` versus `cmp(a, b) < 0`).

use std::cmp::Ordering;

/// Go `sort.Sort(data)` with `less(a, b)` as `data.Less(i, j)`.
pub fn sort<T>(data: &mut [T], mut less: impl FnMut(&T, &T) -> bool) {
    let n = data.len();
    if n <= 1 {
        return;
    }
    let limit = bits_len(n);
    pdqsort(data, 0, n, limit, &mut less);
}

/// Go `slices.SortFunc(x, cmp)`.
pub fn sort_func<T>(data: &mut [T], mut cmp: impl FnMut(&T, &T) -> Ordering) {
    let n = data.len();
    let limit = bits_len(n);
    let mut less = |a: &T, b: &T| cmp(a, b) == Ordering::Less;
    pdqsort(data, 0, n, limit, &mut less);
}

/// Go `sort.Search(n, f)`: the smallest index in `[0, n)` for which `f` is true, or `n`.
#[inline]
pub fn search(n: usize, mut f: impl FnMut(usize) -> bool) -> usize {
    let (mut i, mut j) = (0usize, n);
    while i < j {
        let h = (i + j) >> 1;
        if !f(h) {
            i = h + 1;
        } else {
            j = h;
        }
    }
    i
}

/// Go `bits.Len(uint(n))`.
#[inline]
fn bits_len(n: usize) -> usize {
    (usize::BITS - n.leading_zeros()) as usize
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SortedHint {
    Unknown,
    Increasing,
    Decreasing,
}

/// Go `xorshift`.
struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn next_power_of_two(length: usize) -> usize {
    1usize << bits_len(length)
}

type Less<'a, T> = dyn FnMut(&T, &T) -> bool + 'a;

#[inline]
fn lt<T>(data: &[T], i: usize, j: usize, less: &mut Less<'_, T>) -> bool {
    less(&data[i], &data[j])
}

fn insertion_sort<T>(data: &mut [T], a: usize, b: usize, less: &mut Less<'_, T>) {
    for i in a + 1..b {
        let mut j = i;
        while j > a && lt(data, j, j - 1, less) {
            data.swap(j, j - 1);
            j -= 1;
        }
    }
}

fn sift_down<T>(data: &mut [T], lo: usize, hi: usize, first: usize, less: &mut Less<'_, T>) {
    let mut root = lo;
    loop {
        let mut child = 2 * root + 1;
        if child >= hi {
            break;
        }
        if child + 1 < hi && lt(data, first + child, first + child + 1, less) {
            child += 1;
        }
        if !lt(data, first + root, first + child, less) {
            return;
        }
        data.swap(first + root, first + child);
        root = child;
    }
}

fn heap_sort<T>(data: &mut [T], a: usize, b: usize, less: &mut Less<'_, T>) {
    let first = a;
    let lo = 0;
    let hi = b - a;
    let mut i = (hi as isize - 1) / 2;
    while i >= 0 {
        sift_down(data, i as usize, hi, first, less);
        i -= 1;
    }
    let mut i = hi as isize - 1;
    while i >= 0 {
        data.swap(first, first + i as usize);
        sift_down(data, lo, i as usize, first, less);
        i -= 1;
    }
}

fn pdqsort<T>(data: &mut [T], mut a: usize, mut b: usize, mut limit: usize, less: &mut Less<'_, T>) {
    const MAX_INSERTION: usize = 12;
    let mut was_balanced = true;
    let mut was_partitioned = true;
    loop {
        let length = b - a;
        if length <= MAX_INSERTION {
            insertion_sort(data, a, b, less);
            return;
        }
        if limit == 0 {
            heap_sort(data, a, b, less);
            return;
        }
        if !was_balanced {
            break_patterns(data, a, b);
            limit -= 1;
        }
        let (mut pivot, mut hint) = choose_pivot(data, a, b, less);
        if hint == SortedHint::Decreasing {
            reverse_range(data, a, b);
            pivot = (b - 1) - (pivot - a);
            hint = SortedHint::Increasing;
        }
        if was_balanced && was_partitioned && hint == SortedHint::Increasing && partial_insertion_sort(data, a, b, less) {
            return;
        }
        if a > 0 && !lt(data, a - 1, pivot, less) {
            let mid = partition_equal(data, a, b, pivot, less);
            a = mid;
            continue;
        }
        let (mid, already_partitioned) = partition(data, a, b, pivot, less);
        was_partitioned = already_partitioned;
        let (left_len, right_len) = (mid - a, b - mid);
        let balance_threshold = length / 8;
        if left_len < right_len {
            was_balanced = left_len >= balance_threshold;
            pdqsort(data, a, mid, limit, less);
            a = mid + 1;
        } else {
            was_balanced = right_len >= balance_threshold;
            pdqsort(data, mid + 1, b, limit, less);
            b = mid;
        }
    }
}

fn partition<T>(data: &mut [T], a: usize, b: usize, pivot: usize, less: &mut Less<'_, T>) -> (usize, bool) {
    data.swap(a, pivot);
    // i and j are inclusive of the elements remaining to be partitioned; j may go to a (never below a).
    let (mut i, mut j) = (a as isize + 1, b as isize - 1);
    while i <= j && lt(data, i as usize, a, less) {
        i += 1;
    }
    while i <= j && !lt(data, j as usize, a, less) {
        j -= 1;
    }
    if i > j {
        data.swap(j as usize, a);
        return (j as usize, true);
    }
    data.swap(i as usize, j as usize);
    i += 1;
    j -= 1;
    loop {
        while i <= j && lt(data, i as usize, a, less) {
            i += 1;
        }
        while i <= j && !lt(data, j as usize, a, less) {
            j -= 1;
        }
        if i > j {
            break;
        }
        data.swap(i as usize, j as usize);
        i += 1;
        j -= 1;
    }
    data.swap(j as usize, a);
    (j as usize, false)
}

fn partition_equal<T>(data: &mut [T], a: usize, b: usize, pivot: usize, less: &mut Less<'_, T>) -> usize {
    data.swap(a, pivot);
    let (mut i, mut j) = (a as isize + 1, b as isize - 1);
    loop {
        while i <= j && !lt(data, a, i as usize, less) {
            i += 1;
        }
        while i <= j && lt(data, a, j as usize, less) {
            j -= 1;
        }
        if i > j {
            break;
        }
        data.swap(i as usize, j as usize);
        i += 1;
        j -= 1;
    }
    i as usize
}

fn partial_insertion_sort<T>(data: &mut [T], a: usize, b: usize, less: &mut Less<'_, T>) -> bool {
    const MAX_STEPS: usize = 5;
    const SHORTEST_SHIFTING: usize = 50;
    let mut i = a + 1;
    for _ in 0..MAX_STEPS {
        while i < b && !lt(data, i, i - 1, less) {
            i += 1;
        }
        if i == b {
            return true;
        }
        if b - a < SHORTEST_SHIFTING {
            return false;
        }
        data.swap(i, i - 1);
        // Shift the smaller one to the left (Go loops down to index 1, not to a).
        if i - a >= 2 {
            let mut j = i - 1;
            while j >= 1 {
                if !lt(data, j, j - 1, less) {
                    break;
                }
                data.swap(j, j - 1);
                j -= 1;
            }
        }
        // Shift the greater one to the right.
        if b - i >= 2 {
            let mut j = i + 1;
            while j < b {
                if !lt(data, j, j - 1, less) {
                    break;
                }
                data.swap(j, j - 1);
                j += 1;
            }
        }
    }
    false
}

fn break_patterns<T>(data: &mut [T], a: usize, b: usize) {
    let length = b - a;
    if length >= 8 {
        let mut random = XorShift(length as u64);
        let modulus = next_power_of_two(length);
        let mut idx = a + (length / 4) * 2 - 1;
        while idx <= a + (length / 4) * 2 + 1 {
            let mut other = (random.next() as usize) & (modulus - 1);
            if other >= length {
                other -= length;
            }
            data.swap(idx, a + other);
            idx += 1;
        }
    }
}

fn choose_pivot<T>(data: &mut [T], a: usize, b: usize, less: &mut Less<'_, T>) -> (usize, SortedHint) {
    const SHORTEST_NINTHER: usize = 50;
    const MAX_SWAPS: usize = 4 * 3;
    let l = b - a;
    let mut swaps = 0usize;
    let mut i = a + l / 4;
    let mut j = a + l / 4 * 2;
    let mut k = a + l / 4 * 3;
    if l >= 8 {
        if l >= SHORTEST_NINTHER {
            i = median_adjacent(data, i, &mut swaps, less);
            j = median_adjacent(data, j, &mut swaps, less);
            k = median_adjacent(data, k, &mut swaps, less);
        }
        j = median(data, i, j, k, &mut swaps, less);
    }
    match swaps {
        0 => (j, SortedHint::Increasing),
        MAX_SWAPS => (j, SortedHint::Decreasing),
        _ => (j, SortedHint::Unknown),
    }
}

fn order2<T>(data: &[T], a: usize, b: usize, swaps: &mut usize, less: &mut Less<'_, T>) -> (usize, usize) {
    if lt(data, b, a, less) {
        *swaps += 1;
        return (b, a);
    }
    (a, b)
}

fn median<T>(data: &[T], a: usize, b: usize, c: usize, swaps: &mut usize, less: &mut Less<'_, T>) -> usize {
    let (a, b) = order2(data, a, b, swaps, less);
    let (b, c) = order2(data, b, c, swaps, less);
    let _ = c;
    let (_a, b) = order2(data, a, b, swaps, less);
    b
}

fn median_adjacent<T>(data: &[T], a: usize, swaps: &mut usize, less: &mut Less<'_, T>) -> usize {
    median(data, a - 1, a, a + 1, swaps, less)
}

fn reverse_range<T>(data: &mut [T], a: usize, b: usize) {
    let (mut i, mut j) = (a, b - 1);
    while i < j {
        data.swap(i, j);
        i += 1;
        j -= 1;
    }
}

/// Go `heap.Push`: appends `x` and restores the heap invariant.
pub fn heap_push<T>(h: &mut Vec<T>, x: T, less: &mut impl FnMut(&T, &T) -> bool) {
    h.push(x);
    let n = h.len();
    heap_up(h, n - 1, less);
}

/// Go `heap.Fix`: restores the heap invariant after the element at `i` changed.
pub fn heap_fix<T>(h: &mut [T], i: usize, less: &mut impl FnMut(&T, &T) -> bool) {
    let n = h.len();
    if !heap_down(h, i, n, less) {
        heap_up(h, i, less);
    }
}

/// Go `heap.Init`.
pub fn heap_init<T>(h: &mut [T], less: &mut impl FnMut(&T, &T) -> bool) {
    let n = h.len();
    if n < 2 {
        return;
    }
    let mut i = n / 2;
    while i > 0 {
        i -= 1;
        heap_down(h, i, n, less);
    }
}

/// Go `heap.Pop`: removes and returns the minimum element.
pub fn heap_pop<T>(h: &mut Vec<T>, less: &mut impl FnMut(&T, &T) -> bool) -> Option<T> {
    let n = h.len();
    if n == 0 {
        return None;
    }
    h.swap(0, n - 1);
    heap_down(h, 0, n - 1, less);
    h.pop()
}

fn heap_up<T>(h: &mut [T], mut j: usize, less: &mut impl FnMut(&T, &T) -> bool) {
    loop {
        if j == 0 {
            break;
        }
        let i = (j - 1) / 2;
        if i == j || !less(&h[j], &h[i]) {
            break;
        }
        h.swap(i, j);
        j = i;
    }
}

fn heap_down<T>(h: &mut [T], i0: usize, n: usize, less: &mut impl FnMut(&T, &T) -> bool) -> bool {
    let mut i = i0;
    loop {
        let j1 = 2 * i + 1;
        if j1 >= n {
            break;
        }
        let mut j = j1;
        let j2 = j1 + 1;
        if j2 < n && less(&h[j2], &h[j1]) {
            j = j2;
        }
        if !less(&h[j], &h[i]) {
            break;
        }
        h.swap(i, j);
        i = j;
    }
    i > i0
}
