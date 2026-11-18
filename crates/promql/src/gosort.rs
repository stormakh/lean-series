//! Go's `sort.Sort` (pattern-defeating quicksort, not stable) and `container/heap`, so that results whose order
//! depends on the algorithm (ties, NaN) come out in Go's order.
//!
//! STUB for `sort`: being ported from Go's `sort/zsortinterface.go`. The heap functions are complete.

/// Go `sort.Sort(data)` with `less(a, b)` as `data.Less(i, j)`.
pub fn sort<T>(data: &mut [T], mut less: impl FnMut(&T, &T) -> bool) {
    // Placeholder until the pdqsort port lands.
    data.sort_by(|a, b| {
        if less(a, b) {
            std::cmp::Ordering::Less
        } else if less(b, a) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
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
