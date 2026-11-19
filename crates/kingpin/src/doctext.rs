//! Port of Go `go/doc.ToText` (`go/doc/comment` parse + text printer) for the texts kingpin wraps: paragraphs,
//! code blocks (indented spans) and the minimum-raggedness line wrapping of `go/doc/comment/text.go`.
//!
//! Not ported (no promtool help text uses them): lists, headings, doc links.

/// Go `doc.ToText(w, text, prefix, codePrefix, width)`.
pub fn to_text(text: &str, prefix: &str, code_prefix: &str, width: i64) -> String {
    let blocks = parse(text);
    let code_prefix = if code_prefix.is_empty() { format!("{prefix}\t") } else { code_prefix.to_string() };
    let width = if width == 0 { 80 - prefix.chars().count() as i64 } else { width };
    let mut out = String::new();
    for (i, b) in blocks.iter().enumerate() {
        if i > 0 {
            out.push_str(prefix);
            write_nl(&mut out);
        }
        match b {
            Block::Para(t) => {
                out.push_str(prefix);
                let words: Vec<&str> = t.split_whitespace().collect();
                let seq = if width < 0 || words.is_empty() { vec![0, words.len()] } else { wrap(&words, width) };
                for i in 0..seq.len().saturating_sub(1) {
                    if i > 0 {
                        out.push_str(prefix);
                    }
                    for (j, w) in words[seq[i]..seq[i + 1]].iter().enumerate() {
                        if j > 0 {
                            out.push(' ');
                        }
                        out.push_str(w);
                    }
                    write_nl(&mut out);
                }
                if words.is_empty() {
                    // Go prints nothing for an empty paragraph after the prefix (seq = [0, 0] prints no line).
                }
            }
            Block::Code(t) => {
                let mut text = t.as_str();
                while !text.is_empty() {
                    let (line, rest) = match text.find('\n') {
                        Some(i) => (&text[..i], &text[i + 1..]),
                        None => (text, ""),
                    };
                    if !line.is_empty() {
                        out.push_str(&code_prefix);
                        out.push_str(line);
                    }
                    write_nl(&mut out);
                    text = rest;
                }
            }
        }
    }
    out
}

fn write_nl(out: &mut String) {
    while out.ends_with(' ') || out.ends_with('\t') {
        out.pop();
    }
    out.push('\n');
}

enum Block {
    Para(String),
    Code(String),
}

fn is_blank(s: &str) -> bool {
    s.chars().all(|c| c == ' ' || c == '\t' || c == '\n')
}

fn is_indented(s: &str) -> bool {
    s.starts_with(' ') || s.starts_with('\t')
}

fn common_prefix<'a>(a: &'a str, b: &str) -> &'a str {
    let mut i = 0;
    let ab = a.as_bytes();
    let bb = b.as_bytes();
    while i < ab.len() && i < bb.len() && ab[i] == bb[i] && (ab[i] == b' ' || ab[i] == b'\t') {
        i += 1;
    }
    &a[..i]
}

/// Go `comment.Parser.Parse` for paragraphs and code blocks.
fn parse(text: &str) -> Vec<Block> {
    // Split into lines; drop a final empty line from a trailing newline.
    let mut lines: Vec<String> = text.split('\n').map(|s| s.to_string()).collect();
    if lines.last().is_some_and(|l| l.is_empty()) {
        lines.pop();
    }
    // Unindent: remove the longest common leading whitespace of non-blank lines.
    let mut prefix: Option<String> = None;
    for l in lines.iter() {
        if is_blank(l) {
            continue;
        }
        prefix = Some(match prefix {
            None => l[..l.len() - l.trim_start_matches([' ', '\t']).len()].to_string(),
            Some(p) => common_prefix(&p, l).to_string(),
        });
    }
    let p = prefix.unwrap_or_default();
    let lines: Vec<String> = lines
        .into_iter()
        .map(|l| if is_blank(&l) { String::new() } else { l[p.len()..].to_string() })
        .collect();

    let mut blocks = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        if lines[i].is_empty() {
            i += 1;
            continue;
        }
        if is_indented(&lines[i]) {
            // Code block: a span of indented or blank lines, ending before trailing blanks.
            let start = i;
            let mut end = i;
            while i < lines.len() && (lines[i].is_empty() || is_indented(&lines[i])) {
                if !lines[i].is_empty() {
                    end = i + 1;
                }
                i += 1;
            }
            i = end;
            let span = &lines[start..end];
            let mut pre: Option<String> = None;
            for l in span.iter().filter(|l| !l.is_empty()) {
                pre = Some(match pre {
                    None => l[..l.len() - l.trim_start_matches([' ', '\t']).len()].to_string(),
                    Some(p) => common_prefix(&p, l).to_string(),
                });
            }
            let pre = pre.unwrap_or_default();
            let mut code = String::new();
            for l in span {
                if !l.is_empty() {
                    code.push_str(&l[pre.len()..]);
                }
                code.push('\n');
            }
            blocks.push(Block::Code(code));
            continue;
        }
        let mut para = Vec::new();
        while i < lines.len() && !lines[i].is_empty() && !is_indented(&lines[i]) {
            para.push(lines[i].as_str());
            i += 1;
        }
        blocks.push(Block::Para(smart_quotes(&para.join("\n"))));
    }
    blocks
}

/// The doc comment parser turns `` into “ and '' into ”.
fn smart_quotes(s: &str) -> String {
    s.replace("``", "\u{201c}").replace("''", "\u{201d}")
}

fn wrap_penalty(s: &str) -> i64 {
    match s.as_bytes()[s.len() - 1] {
        b'.' | b',' | b':' | b';' => 0,
        _ => 64,
    }
}

/// Go `wrap` of go/doc/comment/text.go: minimum raggedness line breaking (Hirschberg-Larmore).
fn wrap(words: &[&str], max: i64) -> Vec<usize> {
    type Score = (i64, i64);
    fn add(s: Score, t: Score) -> Score {
        (s.0 + t.0, s.1 + t.1)
    }
    fn cmp(s: Score, t: Score) -> i32 {
        if s.0 < t.0 {
            -1
        } else if s.0 > t.0 {
            1
        } else if s.1 < t.1 {
            -1
        } else if s.1 > t.1 {
            1
        } else {
            0
        }
    }
    let n_words = words.len();
    let mut total = vec![0i64; n_words + 1];
    for (i, s) in words.iter().enumerate() {
        total[1 + i] = total[i] + s.chars().count() as i64 + 1;
    }
    let weight = |i: usize, j: usize| -> Score {
        let n = total[j] - 1 - total[i];
        if j == n_words && n <= max {
            return (0, 0);
        }
        let p = wrap_penalty(words[j - 1]);
        let v = (max - n) * (max - n);
        if n > max {
            return (v, p);
        }
        (0, v + p)
    };
    let mut f: Vec<Score> = vec![(0, 0)];
    let g = |f: &Vec<Score>, i: usize, j: usize| add(f[i], weight(i, j));
    let bridge = |f: &Vec<Score>, a: usize, b: usize, c: usize| -> bool {
        // sort.Search(len(words)+1-c, pred(k+c)).
        let (mut lo, mut hi) = (0usize, n_words + 1 - c);
        while lo < hi {
            let h = (lo + hi) / 2;
            let k = h + c;
            if !(cmp(g(f, a, k), g(f, b, k)) > 0) {
                lo = h + 1;
            } else {
                hi = h;
            }
        }
        let k = c + lo;
        if k > n_words {
            return true;
        }
        cmp(g(f, c, k), g(f, b, k)) <= 0
    };
    let mut d: Vec<usize> = vec![0];
    let mut bestleft: Vec<isize> = vec![-1];
    for m in 1..n_words {
        let v = g(&f, d[0], m);
        f.push(v);
        bestleft.push(d[0] as isize);
        while d.len() > 1 && cmp(g(&f, d[1], m + 1), g(&f, d[0], m + 1)) <= 0 {
            d.remove(0);
        }
        while d.len() > 1 && bridge(&f, d[d.len() - 2], d[d.len() - 1], m) {
            d.pop();
        }
        if cmp(g(&f, m, n_words), g(&f, d[d.len() - 1], n_words)) < 0 {
            d.push(m);
            if d.len() == 2 && cmp(g(&f, d[1], m + 1), g(&f, d[0], m + 1)) <= 0 {
                d.remove(0);
            }
        }
    }
    bestleft.push(d[0] as isize);
    let mut seq = Vec::new();
    let mut m = n_words as isize;
    while m > 0 {
        seq.push(m as usize);
        m = bestleft[m as usize];
    }
    seq.push(0);
    seq.reverse();
    seq
}
