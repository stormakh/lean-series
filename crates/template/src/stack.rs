//! Deep recursion support. Go grows goroutine stacks, so Go templates may nest `{{template}}` calls 100000 deep
//! (and parenthesized pipelines 10000 deep) before an error. Rust threads have fixed stacks, so every
//! [`SEGMENT`] levels the work continues on a new scoped thread with a large stack.

/// Recursion levels per stack segment.
pub const SEGMENT: usize = 128;

/// Stack size of a segment thread.
const SEGMENT_STACK: usize = 16 << 20;

/// Runs `f` on a new thread with a large stack when `depth` starts a new segment, else on this thread.
pub fn maybe_grow<R: Send>(depth: usize, f: impl FnOnce() -> R + Send) -> R {
    if depth == 0 || depth % SEGMENT != 0 {
        return f();
    }
    std::thread::scope(|s| {
        let h = std::thread::Builder::new()
            .name("template-deep".to_string())
            .stack_size(SEGMENT_STACK)
            .spawn_scoped(s, f);
        match h {
            Ok(h) => match h.join() {
                Ok(r) => r,
                Err(p) => std::panic::resume_unwind(p),
            },
            Err(e) => panic!("template: cannot start a thread for deep recursion: {e}"),
        }
    })
}
