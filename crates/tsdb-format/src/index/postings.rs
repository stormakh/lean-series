//! Port of `tsdb/index/postings.go`: the `Postings` iterator, its helpers (intersect, merge, without, list and
//! big endian postings, `FindIntersectingPostings`) and `MemPostings`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use model::labels::{Label, Labels};

use super::{CHECK_CONTEXT_EVERY_N_ITERATIONS, MaxHeap, PostingsStats, Stat};
use super::{Ctx, Error, LabelHints, SeriesRef, StringListIter, smallest_n_values};

/// Go `allPostingsKey`: the label key that is used to store the postings list of all existing IDs.
const ALL_POSTINGS_NAME: &str = "";
const ALL_POSTINGS_VALUE: &str = "";

/// Go `AllPostingsKey`: the label key that is used to store the postings list of all existing IDs.
pub fn all_postings_key() -> (&'static str, &'static str) {
    (ALL_POSTINGS_NAME, ALL_POSTINGS_VALUE)
}

/// Go `ensureOrderBatchSize`: the max number of postings passed to a worker in a single batch in `ensure_order`.
const ENSURE_ORDER_BATCH_SIZE: usize = 1024;

// ---------------------------------------------------------------------------------------------------------------
// Postings.

/// Go `Postings`: iterative access over an ordered list of series references.
pub trait Postings {
    /// Advances the iterator and returns true if another value was found.
    fn next(&mut self) -> bool;

    /// Advances the iterator to value `v` or greater and returns true if a value was found.
    fn seek(&mut self, v: SeriesRef) -> bool;

    /// The value at the current iterator position. Only valid after a successful `next` or `seek`.
    fn at(&self) -> SeriesRef;

    /// The last error of the iterator.
    fn err(&self) -> Option<Error>;

    /// True only for the [`empty_postings`] sentinel (Go `p == EmptyPostings()`).
    #[doc(hidden)]
    fn is_empty_postings(&self) -> bool {
        false
    }
}

/// A boxed postings iterator (`Send`, so queries can move postings between threads).
pub type BoxPostings<'a> = Box<dyn Postings + Send + 'a>;

impl<P: Postings + ?Sized> Postings for Box<P> {
    #[inline]
    fn next(&mut self) -> bool {
        (**self).next()
    }
    #[inline]
    fn seek(&mut self, v: SeriesRef) -> bool {
        (**self).seek(v)
    }
    #[inline]
    fn at(&self) -> SeriesRef {
        (**self).at()
    }
    fn err(&self) -> Option<Error> {
        (**self).err()
    }
    fn is_empty_postings(&self) -> bool {
        (**self).is_empty_postings()
    }
}

impl<P: Postings + ?Sized> Postings for &mut P {
    #[inline]
    fn next(&mut self) -> bool {
        (**self).next()
    }
    #[inline]
    fn seek(&mut self, v: SeriesRef) -> bool {
        (**self).seek(v)
    }
    #[inline]
    fn at(&self) -> SeriesRef {
        (**self).at()
    }
    fn err(&self) -> Option<Error> {
        (**self).err()
    }
    fn is_empty_postings(&self) -> bool {
        (**self).is_empty_postings()
    }
}

/// Go `ExpandPostings`: the postings expanded as a vector.
pub fn expand_postings<P: Postings + ?Sized>(p: &mut P) -> Result<Vec<SeriesRef>, Error> {
    let mut res = Vec::new();
    while p.next() {
        res.push(p.at());
    }
    match p.err() {
        Some(e) => Err(e),
        None => Ok(res),
    }
}

// ---------------------------------------------------------------------------------------------------------------
// errPostings / EmptyPostings.

/// Go `errPostings`: an empty iterator that always errors (or the empty postings sentinel when `err` is `None`).
#[derive(Clone, Debug, Default)]
pub struct ErrPostings {
    err: Option<Error>,
}

impl Postings for ErrPostings {
    fn next(&mut self) -> bool {
        false
    }
    fn seek(&mut self, _: SeriesRef) -> bool {
        false
    }
    fn at(&self) -> SeriesRef {
        0
    }
    fn err(&self) -> Option<Error> {
        self.err.clone()
    }
    fn is_empty_postings(&self) -> bool {
        self.err.is_none()
    }
}

/// Go `EmptyPostings`: a postings list that's always empty. Returning it when there are no postings is
/// recommended: it triggers optimized flows in [`intersect`], [`without`] etc.
pub fn empty_postings<'a>() -> BoxPostings<'a> {
    Box::new(ErrPostings { err: None })
}

/// Go `IsEmptyPostingsType`: true if the postings are the empty postings sentinel. False does not mean that
/// the postings are not empty (it could be an empty intersection of two non-empty postings, for example).
pub fn is_empty_postings_type(p: &dyn Postings) -> bool {
    p.is_empty_postings()
}

/// Go `ErrPostings`: postings that immediately error.
pub fn err_postings<'a>(err: Error) -> BoxPostings<'a> {
    Box::new(ErrPostings { err: Some(err) })
}

// ---------------------------------------------------------------------------------------------------------------
// Intersect.

/// Go `Intersect`: a postings list over the intersection of the input postings.
pub fn intersect<'a>(mut its: Vec<BoxPostings<'a>>) -> BoxPostings<'a> {
    if its.is_empty() {
        return empty_postings();
    }
    if its.len() == 1 {
        return its.pop().unwrap();
    }
    if its.iter().any(|p| p.is_empty_postings()) {
        return empty_postings();
    }
    Box::new(IntersectPostings {
        postings: its,
        current: 0,
    })
}

/// Go `intersectPostings`.
pub struct IntersectPostings<'a> {
    postings: Vec<BoxPostings<'a>>,
    current: SeriesRef,
}

impl Postings for IntersectPostings<'_> {
    #[inline]
    fn at(&self) -> SeriesRef {
        self.current
    }

    fn seek(&mut self, mut target: SeriesRef) -> bool {
        loop {
            let mut all_equal = true;
            for p in self.postings.iter_mut() {
                if !p.seek(target) {
                    return false;
                }
                let at = p.at();
                if at > target {
                    target = at;
                    all_equal = false;
                }
            }
            // If all p.at() are all equal, we found an intersection.
            if all_equal {
                self.current = target;
                return true;
            }
        }
    }

    fn next(&mut self) -> bool {
        // Move forward the first postings and take its value as the target to match.
        let (first, rest) = self.postings.split_first_mut().unwrap();
        if !first.next() {
            return false;
        }
        let mut target = first.at();
        let mut all_equal = true;
        // Now move forward all the other ones and check if they match.
        for p in rest.iter_mut() {
            if !p.next() {
                return false;
            }
            let at = p.at();
            if at > target {
                // This one is past the target, so pick up a new target to seek at the end.
                target = at;
                all_equal = false;
            } else if at < target {
                // This one needs to seek to the target, but carry on with other postings in case they have an
                // even higher target.
                all_equal = false;
            }
        }
        if all_equal {
            self.current = target;
            return true;
        }
        self.seek(target)
    }

    fn err(&self) -> Option<Error> {
        self.postings.iter().find_map(|p| p.err())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Merge (loser tree, port of github.com/bboreham/go-loser).

/// Go `Merge`: an iterator over the union of the input iterators.
pub fn merge<'a>(mut its: Vec<BoxPostings<'a>>) -> BoxPostings<'a> {
    if its.is_empty() {
        return empty_postings();
    }
    if its.len() == 1 {
        return its.pop().unwrap();
    }
    Box::new(MergedPostings::new(its))
}

/// Go `Merge` over one concrete postings type, without a virtual call per element.
pub fn merge_typed<'a, P: Postings + Send + 'a>(mut its: Vec<P>) -> BoxPostings<'a> {
    if its.is_empty() {
        return empty_postings();
    }
    if its.len() == 1 {
        return Box::new(its.pop().unwrap());
    }
    Box::new(MergedPostings::new(its))
}

const LEAF_DONE: isize = -1;

#[derive(Clone, Copy, Debug)]
struct Node {
    /// The loser for all nodes except the 0th, where it is the winner. -1 on a leaf: the sequence is done.
    index: isize,
    /// Value copied from the loser node, or winner for node 0.
    value: SeriesRef,
}

/// Go `mergedPostings`: a loser tree over sorted postings. Duplicates are removed.
pub struct MergedPostings<P> {
    items: Vec<P>,
    nodes: Vec<Node>,
    cur: SeriesRef,
}

/// This value must be higher than all real values used in the tree.
const MAX_VAL: SeriesRef = SeriesRef::MAX;

impl<P: Postings> MergedPostings<P> {
    /// Go `newMergedPostings` (loser.New).
    pub fn new(items: Vec<P>) -> MergedPostings<P> {
        let n = items.len();
        let mut m = MergedPostings {
            items,
            nodes: vec![Node { index: 0, value: 0 }; n * 2],
            cur: 0,
        };
        for i in 0..n {
            m.move_next(i + n); // Must call next on each item so that at() has a value.
        }
        if n > 0 {
            m.nodes[0].index = -1; // Flag to be initialized on first call to next().
        }
        m
    }

    #[inline]
    fn move_next(&mut self, index: usize) -> bool {
        let n = self.items.len();
        let it = &mut self.items[index - n];
        if it.next() {
            self.nodes[index].value = it.at();
            return true;
        }
        self.nodes[index].value = MAX_VAL;
        self.nodes[index].index = LEAF_DONE;
        false
    }

    #[inline]
    fn winner(&self) -> usize {
        self.nodes[0].index as usize
    }

    #[inline]
    fn tree_at(&self) -> SeriesRef {
        self.nodes[0].value
    }

    fn tree_next(&mut self) -> bool {
        if self.nodes.is_empty() {
            return false;
        }
        if self.nodes[0].index == -1 {
            // If tree has not been initialized yet, do that.
            self.initialize();
            return self.nodes[self.winner()].index != LEAF_DONE;
        }
        let w = self.winner();
        if self.nodes[w].index == LEAF_DONE {
            // Already exhausted.
            return false;
        }
        self.move_next(w);
        self.replay_games(w);
        self.nodes[self.winner()].index != LEAF_DONE
    }

    /// Current winner has been advanced independently; fix up the loser tree.
    fn fix(&mut self, closed: bool) {
        let w = self.winner();
        if closed {
            self.nodes[w].value = MAX_VAL;
            self.nodes[w].index = LEAF_DONE;
        } else {
            self.nodes[w].value = self.items[w - self.items.len()].at();
        }
        self.replay_games(w);
    }

    fn tree_is_empty(&mut self) -> bool {
        if self.nodes[0].index == -1 {
            self.initialize();
        }
        self.nodes[self.winner()].index == LEAF_DONE
    }

    fn initialize(&mut self) {
        let winner = self.play_game(1);
        self.nodes[0].index = winner as isize;
        self.nodes[0].value = self.nodes[winner].value;
    }

    /// Finds the winner at position `pos`; if it is a non-leaf node, stores the loser.
    fn play_game(&mut self, pos: usize) -> usize {
        if pos >= self.nodes.len() / 2 {
            return pos;
        }
        let left = self.play_game(pos * 2);
        let right = self.play_game(pos * 2 + 1);
        let (loser, winner) = if self.nodes[left].value < self.nodes[right].value {
            (right, left)
        } else {
            (left, right)
        };
        self.nodes[pos].index = loser as isize;
        self.nodes[pos].value = self.nodes[loser].value;
        winner
    }

    /// Starting at `pos`, which is a winner, re-considers all values up to the root.
    #[inline]
    fn replay_games(&mut self, mut pos: usize) {
        let nodes = &mut self.nodes;
        let mut winning_value = nodes[pos].value;
        let mut n = pos >> 1;
        while n != 0 {
            let node = &mut nodes[n];
            if node.value < winning_value {
                // Record pos as the loser here, and the old loser is the new winner.
                let old = node.index as usize;
                node.index = pos as isize;
                pos = old;
                std::mem::swap(&mut node.value, &mut winning_value);
            }
            n >>= 1;
        }
        // pos is now the winner; store it in node 0.
        nodes[0].index = pos as isize;
        nodes[0].value = winning_value;
    }
}

impl<P: Postings> Postings for MergedPostings<P> {
    #[inline]
    fn next(&mut self) -> bool {
        loop {
            if !self.tree_next() {
                return false;
            }
            // Remove duplicate entries.
            let new_item = self.tree_at();
            if new_item != self.cur {
                self.cur = new_item;
                return true;
            }
        }
    }

    fn seek(&mut self, id: SeriesRef) -> bool {
        while !self.tree_is_empty() && self.tree_at() < id {
            let w = self.winner() - self.items.len();
            let finished = !self.items[w].seek(id);
            self.fix(finished);
        }
        if self.tree_is_empty() {
            return false;
        }
        self.cur = self.tree_at();
        true
    }

    #[inline]
    fn at(&self) -> SeriesRef {
        self.cur
    }

    fn err(&self) -> Option<Error> {
        self.items.iter().find_map(|p| p.err())
    }
}

// ---------------------------------------------------------------------------------------------------------------
// Without.

/// Go `Without`: a postings list that contains all elements from `full` that are not in `drop`.
pub fn without<'a>(full: BoxPostings<'a>, drop: BoxPostings<'a>) -> BoxPostings<'a> {
    if full.is_empty_postings() {
        return empty_postings();
    }
    if drop.is_empty_postings() {
        return full;
    }
    Box::new(RemovedPostings::new(full, drop))
}

/// Go `removedPostings`.
pub struct RemovedPostings<'a> {
    full: BoxPostings<'a>,
    remove: BoxPostings<'a>,
    cur: SeriesRef,
    initialized: bool,
    fok: bool,
    rok: bool,
}

impl<'a> RemovedPostings<'a> {
    /// Go `newRemovedPostings`.
    pub fn new(full: BoxPostings<'a>, remove: BoxPostings<'a>) -> RemovedPostings<'a> {
        RemovedPostings {
            full,
            remove,
            cur: 0,
            initialized: false,
            fok: false,
            rok: false,
        }
    }
}

impl Postings for RemovedPostings<'_> {
    #[inline]
    fn at(&self) -> SeriesRef {
        self.cur
    }

    fn next(&mut self) -> bool {
        if !self.initialized {
            self.fok = self.full.next();
            self.rok = self.remove.next();
            self.initialized = true;
        }
        loop {
            if !self.fok {
                return false;
            }
            if !self.rok {
                self.cur = self.full.at();
                self.fok = self.full.next();
                return true;
            }
            let (fcur, rcur) = (self.full.at(), self.remove.at());
            if fcur < rcur {
                self.cur = fcur;
                self.fok = self.full.next();
                return true;
            } else if rcur < fcur {
                // Forward the remove postings to the right position.
                self.rok = self.remove.seek(fcur);
            } else {
                // Skip the current posting.
                self.fok = self.full.next();
            }
        }
    }

    fn seek(&mut self, id: SeriesRef) -> bool {
        if self.cur >= id {
            return true;
        }
        self.fok = self.full.seek(id);
        self.rok = self.remove.seek(id);
        self.initialized = true;
        self.next()
    }

    fn err(&self) -> Option<Error> {
        if let Some(e) = self.full.err() {
            return Some(e);
        }
        self.remove.err()
    }
}

// ---------------------------------------------------------------------------------------------------------------
// ListPostings.

/// Go `listPostings`: postings over a plain sorted list. The list is shared (cheap to clone), so postings taken
/// from [`MemPostings`] stay valid after the lock is released.
#[derive(Clone, Debug)]
pub struct ListPostings {
    list: Arc<Vec<SeriesRef>>,
    i: usize,
    cur: SeriesRef,
}

/// Go `NewListPostings`: postings over `list`, which must be sorted.
pub fn new_list_postings(list: Vec<SeriesRef>) -> ListPostings {
    ListPostings {
        list: Arc::new(list),
        i: 0,
        cur: 0,
    }
}

impl ListPostings {
    /// Postings over a shared sorted list.
    pub fn from_shared(list: Arc<Vec<SeriesRef>>) -> ListPostings {
        ListPostings { list, i: 0, cur: 0 }
    }

    /// Go `Len`: the remaining number of postings in the list.
    pub fn len(&self) -> usize {
        self.list.len() - self.i
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Postings for ListPostings {
    #[inline]
    fn at(&self) -> SeriesRef {
        self.cur
    }

    #[inline]
    fn next(&mut self) -> bool {
        if let Some(&v) = self.list.get(self.i) {
            self.cur = v;
            self.i += 1;
            return true;
        }
        self.cur = 0;
        false
    }

    #[inline]
    fn seek(&mut self, x: SeriesRef) -> bool {
        // If the current value satisfies, then return.
        if self.cur >= x {
            return true;
        }
        let rest = &self.list[self.i..];
        if rest.is_empty() {
            return false;
        }
        // Check the next item in the list, otherwise binary search between current position and end.
        let mut i = 0;
        if rest[0] < x {
            i = rest.partition_point(|&v| v < x);
            if i >= rest.len() {
                // Off the end - terminate the iterator.
                self.i = self.list.len();
                return false;
            }
        }
        self.cur = rest[i];
        self.i += i + 1;
        true
    }

    fn err(&self) -> Option<Error> {
        None
    }
}

// ---------------------------------------------------------------------------------------------------------------
// BigEndianPostings.

/// Go `bigEndianPostings`: postings over a borrowed byte stream of big endian `u32` numbers (no copy).
#[derive(Clone, Debug)]
pub struct BigEndianPostings<'a> {
    list: &'a [u8],
    cur: u32,
}

impl<'a> BigEndianPostings<'a> {
    /// Go `newBigEndianPostings`.
    pub fn new(list: &'a [u8]) -> BigEndianPostings<'a> {
        BigEndianPostings { list, cur: 0 }
    }
}

#[inline]
fn be32(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

impl Postings for BigEndianPostings<'_> {
    #[inline]
    fn at(&self) -> SeriesRef {
        self.cur as SeriesRef
    }

    #[inline]
    fn next(&mut self) -> bool {
        if self.list.len() >= 4 {
            self.cur = be32(self.list);
            self.list = &self.list[4..];
            return true;
        }
        false
    }

    #[inline]
    fn seek(&mut self, x: SeriesRef) -> bool {
        if self.cur as SeriesRef >= x {
            return true;
        }
        let num = self.list.len() / 4;
        // Do binary search between current position and end.
        let x = x as u32;
        let list = self.list;
        let (mut lo, mut hi) = (0usize, num);
        while lo < hi {
            let h = (lo + hi) / 2;
            if be32(&list[h * 4..]) < x {
                lo = h + 1;
            } else {
                hi = h;
            }
        }
        if lo < num {
            let j = lo * 4;
            self.cur = be32(&list[j..]);
            self.list = &list[j + 4..];
            return true;
        }
        self.list = &[];
        false
    }

    fn err(&self) -> Option<Error> {
        None
    }
}

// ---------------------------------------------------------------------------------------------------------------
// FindIntersectingPostings.

/// Go `FindIntersectingPostings`: checks the intersection of `p` and `candidates[i]` for each `i`; if it is not
/// empty, `i` is added to the indexes returned. Returned indexes are not sorted.
pub fn find_intersecting_postings<'a>(
    p: &mut dyn Postings,
    candidates: Vec<BoxPostings<'a>>,
) -> Result<Vec<usize>, Error> {
    let mut h = PostingsWithIndexHeap(Vec::with_capacity(candidates.len()));
    for (idx, mut it) in candidates.into_iter().enumerate() {
        if it.next() {
            h.0.push(PostingsWithIndex {
                index: idx,
                p: it,
                popped: false,
            });
        } else if let Some(e) = it.err() {
            return Err(e);
        }
    }
    let mut indexes = Vec::new();
    if h.empty() {
        return Ok(indexes);
    }
    h.init();

    while !h.empty() {
        if !p.seek(h.at()) {
            return match p.err() {
                Some(e) => Err(e),
                None => Ok(indexes),
            };
        }
        if p.at() == h.at() {
            indexes.push(h.pop_index());
        } else {
            h.seek_head(p.at())?;
        }
    }
    Ok(indexes)
}

/// Go `postingsWithIndex`: a heap element of [`find_intersecting_postings`], keeping the original index of each
/// postings while they move inside the heap.
pub struct PostingsWithIndex<'a> {
    pub index: usize,
    pub p: BoxPostings<'a>,
    /// These postings shouldn't be considered anymore. See [`PostingsWithIndexHeap::pop_index`].
    pub popped: bool,
}

/// Go `postingsWithIndexHeap`: a min heap by `p.at()`, with popped elements at the bottom. Same algorithms as
/// Go's `container/heap`.
pub struct PostingsWithIndexHeap<'a>(pub Vec<PostingsWithIndex<'a>>);

impl<'a> PostingsWithIndexHeap<'a> {
    /// True if the heap has no elements, or if the smallest element is popped.
    pub fn empty(&self) -> bool {
        self.0.is_empty() || self.0[0].popped
    }

    /// Marks the smallest element as popped, fixes its position (after all non-popped elements) and returns its
    /// index.
    pub fn pop_index(&mut self) -> usize {
        let index = self.0[0].index;
        self.0[0].popped = true;
        self.fix(0);
        index
    }

    /// The series reference where the root postings is pointing at this moment.
    pub fn at(&self) -> SeriesRef {
        self.0[0].p.at()
    }

    /// Seeks the root of the heap; if it is exhausted or fails, it is removed from the heap.
    pub fn seek_head(&mut self, val: SeriesRef) -> Result<(), Error> {
        if self.0[0].p.seek(val) {
            self.fix(0);
            return Ok(());
        }
        if let Some(e) = self.0[0].p.err() {
            return Err(Error::wrap(
                &format!("seek postings {}", self.0[0].index),
                e,
            ));
        }
        self.pop_index();
        Ok(())
    }

    /// Go `Len`. `len() > 0` does not imply that the heap is not empty; use `empty`.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn less(&self, i: usize, j: usize) -> bool {
        let (a, b) = (&self.0[i], &self.0[j]);
        if a.popped != b.popped {
            return b.popped;
        }
        a.p.at() < b.p.at()
    }

    /// Go `heap.Init`.
    pub fn init(&mut self) {
        let n = self.0.len();
        for i in (0..n / 2).rev() {
            self.down(i, n);
        }
    }

    /// Go `heap.Fix`.
    pub fn fix(&mut self, i: usize) {
        if !self.down(i, self.0.len()) {
            self.up(i);
        }
    }

    /// Go `heap.Pop`: removes and returns the minimum element.
    pub fn pop(&mut self) -> PostingsWithIndex<'a> {
        let n = self.0.len() - 1;
        self.0.swap(0, n);
        self.down(0, n);
        self.0.pop().unwrap()
    }

    fn down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n {
                break;
            }
            let mut j = j1; // Left child.
            let j2 = j1 + 1;
            if j2 < n && self.less(j2, j1) {
                j = j2;
            }
            if !self.less(j, i) {
                break;
            }
            self.0.swap(i, j);
            i = j;
        }
        i > i0
    }

    fn up(&mut self, mut j: usize) {
        while j > 0 {
            let i = (j - 1) / 2; // Parent.
            if !self.less(j, i) {
                break;
            }
            self.0.swap(i, j);
            j = i;
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// MemPostings.

type List = Arc<Vec<SeriesRef>>;

#[derive(Default)]
struct MemInner {
    /// The postings lists for each label-value pair, indexed first by label name, and then by label value.
    /// Lists are copy-on-write: readers keep a clone of the `Arc`, writers copy only when a reader holds one.
    m: HashMap<Arc<str>, HashMap<Arc<str>, List>>,
    /// The label values for each label name, in insertion order (append-only, copy-on-write).
    lvs: HashMap<Arc<str>, Arc<Vec<Arc<str>>>>,
    ordered: bool,
}

/// Go `MemPostings`: postings lists of series references per label pair. They may be written to out of order;
/// `ensure_order` must be called once before any reads are done, which allows quick unordered batch fills on
/// startup.
pub struct MemPostings {
    mtx: RwLock<MemInner>,
}

const DEFAULT_LABEL_NAMES_MAP_SIZE: usize = 512;

impl Default for MemPostings {
    fn default() -> Self {
        MemPostings::new()
    }
}

impl MemPostings {
    /// Go `NewMemPostings`: ready for reads and writes.
    pub fn new() -> MemPostings {
        MemPostings::with_order(true)
    }

    /// Go `NewUnorderedMemPostings`: not safe to be read from until `ensure_order` was called once.
    pub fn new_unordered() -> MemPostings {
        MemPostings::with_order(false)
    }

    fn with_order(ordered: bool) -> MemPostings {
        MemPostings {
            mtx: RwLock::new(MemInner {
                m: HashMap::with_capacity(DEFAULT_LABEL_NAMES_MAP_SIZE),
                lvs: HashMap::with_capacity(DEFAULT_LABEL_NAMES_MAP_SIZE),
                ordered,
            }),
        }
    }

    fn read(&self) -> RwLockReadGuard<'_, MemInner> {
        self.mtx.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write(&self) -> RwLockWriteGuard<'_, MemInner> {
        self.mtx.write().unwrap_or_else(|e| e.into_inner())
    }

    /// Go `Symbols`: an iterator over all unique name and value strings, in order.
    pub fn symbols(&self) -> StringListIter {
        // Make a quick clone of the map to avoid holding the lock while iterating.
        let lvs: Vec<(Arc<str>, Arc<Vec<Arc<str>>>)> = self
            .read()
            .lvs
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        // Add all the strings to a set to de-duplicate.
        let mut symbols: HashSet<&str> = HashSet::with_capacity(DEFAULT_LABEL_NAMES_MAP_SIZE);
        for (n, values) in &lvs {
            symbols.insert(n);
            for v in values.iter() {
                symbols.insert(v);
            }
        }
        let mut res: Vec<String> = symbols.into_iter().map(str::to_string).collect();
        res.sort_unstable();
        StringListIter::new(res)
    }

    /// Go `SortedKeys`: the sorted label keys of the postings.
    pub fn sorted_keys(&self) -> Vec<Label> {
        let mut keys = Vec::new();
        {
            let g = self.read();
            for (n, e) in &g.m {
                for v in e.keys() {
                    keys.push(Label::new(&**n, &**v));
                }
            }
        }
        keys.sort_unstable_by(|a, b| a.name.cmp(&b.name).then_with(|| a.value.cmp(&b.value)));
        keys
    }

    /// Go `LabelNames`: all the unique label names (unsorted).
    pub fn label_names(&self) -> Vec<String> {
        let g = self.read();
        g.m.keys()
            .filter(|n| &***n != ALL_POSTINGS_NAME)
            .map(|n| n.to_string())
            .collect()
    }

    /// Go `LabelValues`: the label values for `name` (insertion order, or the smallest ones sorted with
    /// `limit_smallest`). The result is a copy.
    pub fn label_values(
        &self,
        _ctx: &dyn Ctx,
        name: &str,
        hints: Option<&LabelHints>,
    ) -> Vec<String> {
        let values = match self.read().lvs.get(name) {
            Some(v) => v.clone(),
            None => return Vec::new(),
        };
        // The values are not held in order, so keeping the leading limit of them returns an arbitrary subset.
        // apply_limit selects the smallest values when the caller needs them.
        if let Some(h) = hints {
            if h.limit > 0 && values.len() as i64 > h.limit {
                let limit = h.limit as usize;
                if h.limit_smallest {
                    let refs: Vec<&str> = values.iter().map(|v| &**v).collect();
                    return smallest_n_values(refs, limit)
                        .into_iter()
                        .map(str::to_string)
                        .collect();
                }
                return values[..limit].iter().map(|v| v.to_string()).collect();
            }
        }
        values.iter().map(|v| v.to_string()).collect()
    }

    /// Go `Stats`: cardinality statistics. `label_size_func` computes the space required for n series with a
    /// given label.
    pub fn stats(
        &self,
        label: &str,
        limit: usize,
        label_size_func: impl Fn(&str, &str, u64) -> u64,
    ) -> PostingsStats {
        let g = self.read();
        let mut metrics = MaxHeap::default();
        let mut labels = MaxHeap::default();
        let mut label_value_length = MaxHeap::default();
        let mut label_value_pairs = MaxHeap::default();
        let mut num_label_pairs = 0;
        metrics.init(limit);
        labels.init(limit);
        label_value_length.init(limit);
        label_value_pairs.init(limit);

        for (n, e) in &g.m {
            if &**n == ALL_POSTINGS_NAME {
                continue;
            }
            labels.push(Stat {
                name: n.to_string(),
                count: e.len() as u64,
            });
            num_label_pairs += e.len();
            let mut size = 0u64;
            for (name, values) in e {
                let series_cnt = values.len() as u64;
                if **n == *label {
                    metrics.push(Stat {
                        name: name.to_string(),
                        count: series_cnt,
                    });
                }
                label_value_pairs.push(Stat {
                    name: format!("{n}={name}"),
                    count: series_cnt,
                });
                size = size.wrapping_add(label_size_func(n, name, series_cnt));
            }
            label_value_length.push(Stat {
                name: n.to_string(),
                count: size,
            });
        }
        drop(g);

        PostingsStats {
            cardinality_metrics_stats: metrics.get(),
            cardinality_label_stats: labels.get(),
            label_value_stats: label_value_length.get(),
            label_value_pairs_stats: label_value_pairs.get(),
            num_label_pairs,
        }
    }

    /// Go `All`: a postings list over all documents ever added.
    pub fn all(&self) -> BoxPostings<'static> {
        self.postings(
            super::background(),
            ALL_POSTINGS_NAME,
            &[ALL_POSTINGS_VALUE],
        )
    }

    /// Go `EnsureOrder`: sorts all postings lists. After it returns, `add` and `add_for` insert new IDs in a
    /// sorted manner. `number_of_concurrent_processes` is the maximum number of threads used; `<= 0` uses the
    /// number of CPUs.
    pub fn ensure_order(&self, number_of_concurrent_processes: i32) {
        let mut g = self.write();
        if g.ordered {
            return;
        }
        let concurrency = if number_of_concurrent_processes <= 0 {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        } else {
            number_of_concurrent_processes as usize
        };

        let mut batches: Vec<Vec<&mut Vec<SeriesRef>>> = Vec::new();
        let mut next_job = Vec::with_capacity(ENSURE_ORDER_BATCH_SIZE);
        for e in g.m.values_mut() {
            for l in e.values_mut() {
                next_job.push(Arc::make_mut(l));
                if next_job.len() >= ENSURE_ORDER_BATCH_SIZE {
                    batches.push(std::mem::replace(
                        &mut next_job,
                        Vec::with_capacity(ENSURE_ORDER_BATCH_SIZE),
                    ));
                }
            }
        }
        // If the last job was partially filled, we need to push it to workers too.
        if !next_job.is_empty() {
            batches.push(next_job);
        }

        let workers = concurrency.min(batches.len());
        if workers <= 1 {
            for job in batches {
                for l in job {
                    l.sort_unstable();
                }
            }
        } else {
            let work = Mutex::new(batches);
            std::thread::scope(|s| {
                for _ in 0..workers {
                    s.spawn(|| {
                        loop {
                            let job = work.lock().unwrap_or_else(|e| e.into_inner()).pop();
                            let Some(job) = job else { break };
                            for l in job {
                                l.sort_unstable();
                            }
                        }
                    });
                }
            });
        }
        g.ordered = true;
    }

    /// Go `Delete`: removes all ids in `deleted` from the postings lists. `affected` contains all the labels
    /// that are affected by the deletion; there's no need to check other labels.
    pub fn delete(&self, deleted: &HashSet<SeriesRef>, affected: &HashSet<Label>) {
        let mut g = self.write();
        let mut affected_label_names: HashSet<Arc<str>> = HashSet::new();

        fn process(
            g: &mut MemInner,
            deleted: &HashSet<SeriesRef>,
            name: &str,
            value: &str,
            aff: &mut HashSet<Arc<str>>,
        ) {
            let Some((name_key, nm)) = g.m.get_key_value(name) else {
                return;
            };
            let orig: &[SeriesRef] = match nm.get(value) {
                Some(l) => l,
                None => &[],
            };
            let repl: Vec<SeriesRef> = orig
                .iter()
                .copied()
                .filter(|id| !deleted.contains(id))
                .collect();
            let name_key = name_key.clone();
            let nm = g.m.get_mut(name).unwrap();
            if !repl.is_empty() {
                if let Some(l) = nm.get_mut(value) {
                    *l = Arc::new(repl);
                }
            } else {
                nm.remove(value);
                aff.insert(name_key);
            }
        }

        let mut i = 0;
        for l in affected {
            i += 1;
            process(
                &mut g,
                deleted,
                &l.name,
                &l.value,
                &mut affected_label_names,
            );
            // From time to time we want some readers to go through and read their postings.
            if i % 512 == 0 {
                g = self.unlock_wait_and_lock_again(g);
            }
        }
        process(
            &mut g,
            deleted,
            ALL_POSTINGS_NAME,
            ALL_POSTINGS_VALUE,
            &mut affected_label_names,
        );

        // Now we need to update the label values slices.
        let mut i = 0;
        for name in affected_label_names {
            i += 1;
            // From time to time we want some readers to go through and read their postings.
            if i % 512 == 0 {
                g = self.unlock_wait_and_lock_again(g);
            }
            let Some(nm) = g.m.get(&name) else { continue };
            if nm.is_empty() {
                // Delete the label name key if we deleted all values.
                g.m.remove(&name);
                g.lvs.remove(&name);
                continue;
            }
            // Leave enough room to grow without reallocating: there's definitely some churn.
            let mut lvs = Vec::with_capacity(2 * nm.len());
            lvs.extend(nm.keys().cloned());
            g.lvs.insert(name, Arc::new(lvs));
        }
    }

    /// Unlocks the write lock, lets waiting readers get the lock, waits a little bit and locks it again.
    fn unlock_wait_and_lock_again<'g>(
        &'g self,
        g: RwLockWriteGuard<'g, MemInner>,
    ) -> RwLockWriteGuard<'g, MemInner> {
        drop(g);
        // Grab one read lock ourselves, so that waiting readers had their chance.
        drop(self.read());
        std::thread::sleep(std::time::Duration::from_millis(1));
        self.write()
    }

    /// Go `Iter`: calls `f` for each postings list. It aborts if `f` returns an error and returns it.
    pub fn iter<E>(
        &self,
        mut f: impl FnMut(&str, &str, ListPostings) -> Result<(), E>,
    ) -> Result<(), E> {
        let g = self.read();
        for (n, e) in &g.m {
            for (v, p) in e {
                f(n, v, ListPostings::from_shared(p.clone()))?;
            }
        }
        Ok(())
    }

    /// Go `Add`: adds a label set to the postings index.
    pub fn add(&self, id: SeriesRef, lset: &Labels) {
        let mut g = self.write();
        for (n, v) in lset.iter() {
            g.add_for(id, n, v);
        }
        g.add_for(id, ALL_POSTINGS_NAME, ALL_POSTINGS_VALUE);
    }

    /// Go `addFor`: adds `id` to the list of `name=value`.
    pub fn add_for(&self, id: SeriesRef, name: &str, value: &str) {
        self.write().add_for(id, name, value);
    }

    /// Go `PostingsForLabelMatching`: postings of all values of `name` for which `match_fn` is true.
    pub fn postings_for_label_matching(
        &self,
        ctx: &dyn Ctx,
        name: &str,
        match_fn: &dyn Fn(&str) -> bool,
    ) -> BoxPostings<'static> {
        // Take the label values and match over them without holding the lock, which can be slow (seconds) if
        // the match function is a huge regex.
        let read_only_label_values = self.read().lvs.get(name).cloned();
        let read_only_label_values: &[Arc<str>] =
            read_only_label_values.as_deref().map_or(&[], |v| v);

        let mut vals: Vec<&str> = Vec::with_capacity(read_only_label_values.len());
        for (i, v) in read_only_label_values.iter().enumerate() {
            if i % CHECK_CONTEXT_EVERY_N_ITERATIONS == 0 && ctx.err().is_some() {
                return err_postings(ctx.err().unwrap_or_else(Error::canceled));
            }
            if match_fn(v) {
                vals.push(v);
            }
        }
        // If none matched (or this label had no values), no need to grab the lock again.
        if vals.is_empty() {
            return super::empty_postings();
        }

        // Now `vals` only contains the values that matched, get their postings.
        let mut its = Vec::with_capacity(vals.len());
        {
            let g = self.read();
            if let Some(e) = g.m.get(name) {
                for v in vals {
                    // Some of the values may have been garbage-collected in the meantime; skip them.
                    if let Some(refs) = e.get(v) {
                        its.push(ListPostings::from_shared(refs.clone()));
                    }
                }
            }
        }
        merge_typed(its)
    }

    /// Go `Postings`: a postings iterator for the given label values.
    pub fn postings<S: AsRef<str>>(
        &self,
        _ctx: &dyn Ctx,
        name: &str,
        values: &[S],
    ) -> BoxPostings<'static> {
        let mut res = Vec::with_capacity(values.len());
        {
            let g = self.read();
            if let Some(m) = g.m.get(name) {
                for v in values {
                    if let Some(lp) = m.get(v.as_ref()) {
                        res.push(ListPostings::from_shared(lp.clone()));
                    }
                }
            }
        }
        merge_typed(res)
    }

    /// Go `PostingsForAllLabelValues`.
    pub fn postings_for_all_label_values(
        &self,
        _ctx: &dyn Ctx,
        name: &str,
    ) -> BoxPostings<'static> {
        let mut its = Vec::new();
        {
            let g = self.read();
            if let Some(e) = g.m.get(name) {
                its.reserve(e.len());
                for refs in e.values() {
                    if !refs.is_empty() {
                        its.push(ListPostings::from_shared(refs.clone()));
                    }
                }
            }
        }
        merge_typed(its)
    }

    // Test helpers: direct access to the maps (Go tests read and write `p.m` and `p.lvs`).

    /// Sets the list of `name=value` in the postings map only (not the label values), like Go tests do.
    #[doc(hidden)]
    pub fn test_set_list(&self, name: &str, value: &str, list: Vec<SeriesRef>) {
        let mut g = self.write();
        let nm = g.m.entry(Arc::from(name)).or_default();
        nm.insert(Arc::from(value), Arc::new(list));
    }

    /// The list of `name=value` (Go `p.m[name][value]`).
    #[doc(hidden)]
    pub fn test_list(&self, name: &str, value: &str) -> Option<Vec<SeriesRef>> {
        self.read()
            .m
            .get(name)
            .and_then(|e| e.get(value))
            .map(|l| l.to_vec())
    }

    /// All lists.
    #[doc(hidden)]
    pub fn test_all_lists(&self) -> Vec<Vec<SeriesRef>> {
        self.read()
            .m
            .values()
            .flat_map(|e| e.values().map(|l| l.to_vec()))
            .collect()
    }

    /// The label values list of `name` (Go `p.lvs[name]`).
    #[doc(hidden)]
    pub fn test_lvs(&self, name: &str) -> Option<Vec<String>> {
        self.read()
            .lvs
            .get(name)
            .map(|v| v.iter().map(|s| s.to_string()).collect())
    }
}

impl MemInner {
    fn add_for(&mut self, id: SeriesRef, name: &str, value: &str) {
        if !self.m.contains_key(name) {
            self.m.insert(Arc::from(name), HashMap::new());
        }
        let nm = self.m.get_mut(name).unwrap();
        if !nm.contains_key(value) {
            let v: Arc<str> = Arc::from(value);
            nm.insert(v.clone(), List::default());
            match self.lvs.get_mut(name) {
                Some(l) => Arc::make_mut(l).push(v),
                None => {
                    self.lvs.insert(Arc::from(name), Arc::new(vec![v]));
                }
            }
        }
        let list = Arc::make_mut(nm.get_mut(value).unwrap());
        list.push(id);

        if !self.ordered {
            return;
        }
        // There is no guarantee that no higher ID was inserted before as they may be generated independently
        // before adding them to postings. We repair order violations on insert. The invariant is that the first
        // n-1 items in the list are already sorted.
        let mut i = list.len() - 1;
        while i >= 1 {
            if list[i] >= list[i - 1] {
                break;
            }
            list.swap(i, i - 1);
            i -= 1;
        }
    }
}
