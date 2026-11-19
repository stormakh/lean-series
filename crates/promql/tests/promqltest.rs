//! Runs the upstream PromQL test scripts (`upstream/promql/promqltest/testdata/*.test`, read unchanged) as Go's
//! `TestEvaluations` does: `promqltest.RunBuiltinTests` with `NewTestEngine(t, false, 0, DefaultMaxSamplesPerQuery)`.
//!
//! Prints `test <file>_line_<n> ... ok|FAILED` per eval command (the ledger names: the file stem with `-` as `_`
//! and the line of the eval command), with a short reason under each failure. As in Go, a failed eval does not
//! stop the script; a failed `load` does (the evals after it are reported as FAILED, not run).
//!
//! Optional arguments: substrings of test names to run (like the libtest filter). The evaluation runs on threads
//! with a 256 MiB stack, because the parser and the evaluator recurse.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use promql::promqltest::{self, EvalOutcome, TestCommand};

const STACK_SIZE: usize = 256 << 20;
const MAX_REASON: usize = 600;

thread_local! {
    static LAST_PANIC_LOCATION: RefCell<Option<String>> = const { RefCell::new(None) };
}

struct FileResult {
    lines: Vec<(String, Result<(), String>)>,
}

fn test_name(file: &str, line: usize) -> String {
    let stem = file.strip_suffix(".test").unwrap_or(file).replace('-', "_");
    format!("{}_line_{}", stem, line)
}

fn short(reason: &str) -> String {
    let one: String = reason.replace('\n', " | ");
    if one.len() <= MAX_REASON {
        return one;
    }
    let mut cut = MAX_REASON;
    while !one.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}...", &one[..cut])
}

fn run_file(name: &str, content: &str) -> FileResult {
    // The eval commands of the script, to report the ones a failed load stops.
    let open = || -> Arc<dyn storage::Storage> { Arc::new(promqltest::new_builtin_test_storage()) };
    let eval_lines: Vec<usize> = match promqltest::Test::new(content, false, &open) {
        Ok(t) => t
            .commands()
            .iter()
            .filter_map(|c| match c {
                TestCommand::Eval(e) => Some(e.line),
                _ => None,
            })
            .collect(),
        Err(_) => Vec::new(),
    };
    let engine = promqltest::new_test_engine(
        false,
        Duration::ZERO,
        promqltest::DEFAULT_MAX_SAMPLES_PER_QUERY,
    );
    let mut results: HashMap<usize, Result<(), String>> = HashMap::new();
    let r =
        promqltest::run_test_with_storage(content, &engine, &open, &mut |o: EvalOutcome<'_>| {
            let mut res = o.result;
            if let Err(e) = &mut res {
                if e.starts_with("panic: ") {
                    if let Some(loc) = LAST_PANIC_LOCATION.with(|l| l.borrow_mut().take()) {
                        e.push_str(&format!(" (at {})", loc));
                    }
                }
            }
            results.insert(o.cmd.line, res);
        });
    let mut lines = Vec::new();
    for l in eval_lines {
        let res = match results.remove(&l) {
            Some(r) => r,
            None => Err(match &r {
                Err(e) => format!("not run: the script stopped: {}", e),
                Ok(()) => "not run".to_string(),
            }),
        };
        lines.push((test_name(name, l), res));
    }
    if let Err(e) = r {
        if lines.is_empty() {
            lines.push((
                format!(
                    "{}_script",
                    name.strip_suffix(".test").unwrap_or(name).replace('-', "_")
                ),
                Err(e),
            ));
        }
    }
    FileResult { lines }
}

fn main() {
    let filters: Vec<String> = std::env::args()
        .skip(1)
        .filter(|a| !a.starts_with('-'))
        .collect();
    std::panic::set_hook(Box::new(|info| {
        let loc = info
            .location()
            .map(|l| format!("{}:{}", l.file(), l.line()));
        LAST_PANIC_LOCATION.with(|l| *l.borrow_mut() = loc);
    }));

    let files = promqltest::builtin_test_files().expect("read the upstream test files");
    // Run the files in parallel (each on its own large-stack thread), print in file order.
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .min(files.len().max(1));
    let files = Arc::new(files);
    let next = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let results: Arc<std::sync::Mutex<Vec<Option<FileResult>>>> = Arc::new(std::sync::Mutex::new(
        (0..files.len()).map(|_| None).collect(),
    ));
    let mut handles = Vec::new();
    for _ in 0..workers {
        let (files, next, results) = (files.clone(), next.clone(), results.clone());
        handles.push(
            std::thread::Builder::new()
                .stack_size(STACK_SIZE)
                .spawn(move || {
                    loop {
                        let i = next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        if i >= files.len() {
                            break;
                        }
                        let (name, content) = &files[i];
                        let r = match std::panic::catch_unwind(|| run_file(name, content)) {
                            Ok(r) => r,
                            Err(p) => FileResult {
                                lines: vec![(
                                    format!("{}_script", name.replace('-', "_")),
                                    Err(format!("panic: {}", promqltest::panic_message(&*p))),
                                )],
                            },
                        };
                        results.lock().unwrap()[i] = Some(r);
                    }
                })
                .expect("spawn a test thread"),
        );
    }
    for h in handles {
        h.join().expect("test thread");
    }

    let results = std::mem::take(&mut *results.lock().unwrap());
    let all: Vec<(String, Result<(), String>)> = results
        .into_iter()
        .flatten()
        .flat_map(|r| r.lines)
        .collect();
    let selected: Vec<&(String, Result<(), String>)> = all
        .iter()
        .filter(|(n, _)| filters.is_empty() || filters.iter().any(|f| n.contains(f.as_str())))
        .collect();
    println!("\nrunning {} tests", selected.len());
    let (mut passed, mut failed) = (0, 0);
    for (name, res) in &selected {
        match res {
            Ok(()) => {
                passed += 1;
                println!("test {} ... ok", name);
            }
            Err(e) => {
                failed += 1;
                println!("test {} ... FAILED", name);
                println!("    {}", short(e));
            }
        }
    }
    println!(
        "\ntest result: {}. {} passed; {} failed; 0 ignored; 0 measured; {} filtered out\n",
        if failed == 0 { "ok" } else { "FAILED" },
        passed,
        failed,
        all.len() - selected.len()
    );
    if failed > 0 {
        std::process::exit(101);
    }
}
