//! Port of `promql/promqltest/test_migrate.go`: rewrites test files from the `eval_fail`/`eval_warn`/`eval_info`/
//! `eval_ordered` forms to `eval` with `expect` lines.

use std::collections::HashMap;
use std::path::Path;

use model::regexp::Regexp;

const DEFAULT_TEST_DATA_DIR: &str = "promql/promqltest/testdata";

/// How strict the added `expect` lines are (Go `MigrateMode`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MigrateMode {
    Strict,
    Basic,
    Tolerant,
}

/// Go `ParseMigrateMode`.
pub fn parse_migrate_mode(s: &str) -> Result<MigrateMode, String> {
    match s {
        "strict" => Ok(MigrateMode::Strict),
        "basic" => Ok(MigrateMode::Basic),
        "tolerant" => Ok(MigrateMode::Tolerant),
        _ => Err(format!("invalid mode: {}", s)),
    }
}

fn annotation_map(mode: MigrateMode) -> HashMap<&'static str, Vec<&'static str>> {
    let mut m = HashMap::new();
    match mode {
        MigrateMode::Strict => {
            m.insert(
                "eval_fail",
                vec!["expect fail", "expect no_warn", "expect no_info"],
            );
            m.insert("eval_warn", vec!["expect warn", "expect no_info"]);
            m.insert("eval_info", vec!["expect info", "expect no_warn"]);
            m.insert(
                "eval_ordered",
                vec!["expect ordered", "expect no_warn", "expect no_info"],
            );
            m.insert("eval", vec!["expect no_warn", "expect no_info"]);
        }
        MigrateMode::Basic => {
            m.insert("eval_fail", vec!["expect fail"]);
            m.insert("eval_warn", vec!["expect warn"]);
            m.insert("eval_info", vec!["expect info"]);
            m.insert("eval_ordered", vec!["expect ordered"]);
        }
        MigrateMode::Tolerant => {
            m.insert("eval_fail", vec!["expect fail"]);
            m.insert("eval_ordered", vec!["expect ordered"]);
        }
    }
    m
}

/// Go `MigrateTestData`: migrates all `.test` files in `dir` (default `promql/promqltest/testdata`) in place.
pub fn migrate_test_data(mode: &str, dir: &str) -> Result<(), String> {
    let dir = if dir.is_empty() {
        DEFAULT_TEST_DATA_DIR
    } else {
        dir
    };
    let mode = parse_migrate_mode(mode).map_err(|e| format!("failed to parse mode: {}", e))?;
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .map_err(|e| format!("failed to read testdata directory: {}", e))?
        .filter_map(|e| e.ok())
        .collect();
    entries.sort_by_key(|e| e.file_name());
    let amap = annotation_map(mode);
    let eval_regex =
        Regexp::must_compile(r"^(eval |eval_fail |eval_warn |eval_info |eval_ordered )(.*)$");
    for e in entries {
        let name = e.file_name().to_string_lossy().into_owned();
        if e.path().is_dir() || !name.ends_with(".test") {
            continue;
        }
        let path = Path::new(dir).join(&name);
        let content = std::fs::read_to_string(&path)
            .map_err(|e| format!("failed to read file {}: {}", path.display(), e))?;
        let lines: Vec<&str> = content.split('\n').collect();
        let processed = process_test_file_lines(&lines, &amap, &eval_regex);
        std::fs::write(&path, processed.join("\n"))
            .map_err(|e| format!("failed to write file {}: {}", path.display(), e))?;
    }
    Ok(())
}

/// Go `FindStringSubmatch`.
fn submatches<'a>(re: &Regexp, s: &'a str) -> Option<Vec<&'a str>> {
    let idx = re.find_string_submatch_index(s)?;
    Some(
        idx.chunks(2)
            .map(|p| {
                if p[0] < 0 {
                    ""
                } else {
                    &s[p[0] as usize..p[1] as usize]
                }
            })
            .collect(),
    )
}

/// Go `strings.TrimSpace`.
fn trim(s: &str) -> &str {
    s.trim_matches(|c: char| c.is_whitespace())
}

/// The leading spaces and tabs of a line that has a non-space character after them (Go `indentRegex`).
fn indent(line: &str) -> Option<&str> {
    let n = line.len() - line.trim_start_matches([' ', '\t']).len();
    if n == 0 {
        return None;
    }
    let rest = &line[n..];
    // Go `\S` matches any character that is not ASCII white space.
    match rest.chars().next() {
        Some(c) if !matches!(c, ' ' | '\t' | '\n' | '\r' | '\x0c') => Some(&line[..n]),
        _ => None,
    }
}

fn process_test_file_lines(
    lines: &[&str],
    amap: &HashMap<&'static str, Vec<&'static str>>,
    eval_regex: &Regexp,
) -> Vec<String> {
    let mut result: Vec<String> = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let start_line = lines[i];
        let Some(matches) = submatches(eval_regex, trim(start_line)) else {
            result.push(start_line.to_string());
            i += 1;
            continue;
        };
        let mut input_block: Vec<&str> = Vec::new();
        let mut output_block: Vec<&str> = Vec::new();
        let mut skip_block = false;
        i += 1;
        while i < lines.len() {
            input_block.push(lines[i]);
            if trim(lines[i]).starts_with("expect ") {
                skip_block = true;
            }
            if i + 1 < lines.len() && eval_regex.is_match(trim(lines[i + 1])) {
                break;
            }
            i += 1;
        }
        if skip_block {
            result.push(start_line.to_string());
            result.extend(input_block.iter().map(|s| s.to_string()));
            i += 1;
            continue;
        }
        let leading_ws = indent(start_line).unwrap_or("");
        let command = trim(matches[1]);
        let expression = matches[2];
        result.push(format!("{}eval {}", leading_ws, expression));
        let mut ind = "  ";
        for line in &input_block {
            if let Some(m) = indent(line) {
                ind = m;
                break;
            }
        }
        let mut annotations: Vec<String> = amap
            .get(command)
            .map(|v| v.iter().map(|a| format!("{}{}", ind, a)).collect())
            .unwrap_or_default();
        for line in &input_block {
            let trimmed = trim(line);
            if trimmed.starts_with("expected_fail_message") {
                let msg = trimmed
                    .strip_prefix("expected_fail_message ")
                    .unwrap_or(trimmed);
                for a in annotations.iter_mut() {
                    if a.contains("expect fail") {
                        *a = format!("{}expect fail msg:{}", ind, msg);
                    }
                }
            } else if trimmed.starts_with("expected_fail_regexp") {
                let regex = trimmed
                    .strip_prefix("expected_fail_regexp ")
                    .unwrap_or(trimmed);
                for a in annotations.iter_mut() {
                    if a.contains("expect fail") {
                        *a = format!("{}expect fail regex:{}", ind, regex);
                    }
                }
            } else {
                output_block.push(line);
            }
        }
        result.extend(annotations);
        result.extend(output_block.iter().map(|s| s.to_string()));
        i += 1;
    }
    result
}
