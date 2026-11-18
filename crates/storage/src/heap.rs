//! Go's `container/heap` algorithm over a `Vec`, with the order given by a closure. The same algorithm keeps the
//! same tie order as Go, which the merge semantics depend on.

/// Go `heap.Push`.
pub(crate) fn push<T>(h: &mut Vec<T>, x: T, mut less: impl FnMut(&T, &T) -> bool) {
    h.push(x);
    let n = h.len() - 1;
    up(h, n, &mut less);
}

/// Go `heap.Pop`: removes and returns the minimum.
pub(crate) fn pop<T>(h: &mut Vec<T>, mut less: impl FnMut(&T, &T) -> bool) -> T {
    let n = h.len() - 1;
    h.swap(0, n);
    down(h, 0, n, &mut less);
    h.pop().expect("pop from an empty heap")
}

fn up<T>(h: &mut [T], mut j: usize, less: &mut impl FnMut(&T, &T) -> bool) {
    while j > 0 {
        let i = (j - 1) / 2; // Parent.
        if !less(&h[j], &h[i]) {
            break;
        }
        h.swap(i, j);
        j = i;
    }
}

fn down<T>(h: &mut [T], i0: usize, n: usize, less: &mut impl FnMut(&T, &T) -> bool) -> bool {
    let mut i = i0;
    loop {
        let j1 = 2 * i + 1;
        if j1 >= n {
            break;
        }
        let mut j = j1; // Left child.
        let j2 = j1 + 1;
        if j2 < n && less(&h[j2], &h[j1]) {
            j = j2; // Right child.
        }
        if !less(&h[j], &h[i]) {
            break;
        }
        h.swap(i, j);
        i = j;
    }
    i > i0
}
