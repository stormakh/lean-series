//! Go `path/filepath.Match` and `Glob` (Unix), used for `file_sd_configs` files, `scrape_config_files` and
//! rule files: `*`, `?`, `[...]` classes and `\` escapes; matches never cross `/`.

/// Go `filepath.ErrBadPattern`.
pub const ERR_BAD_PATTERN: &str = "syntax error in pattern";

fn decode_rune(s: &[u8]) -> (char, usize) {
    let n = match s.first() {
        None => return ('\u{FFFD}', 0),
        Some(b) if *b < 0x80 => 1,
        Some(b) if *b >> 5 == 0b110 => 2,
        Some(b) if *b >> 4 == 0b1110 => 3,
        Some(b) if *b >> 3 == 0b11110 => 4,
        _ => return ('\u{FFFD}', 1),
    };
    if s.len() < n {
        return ('\u{FFFD}', 1);
    }
    match std::str::from_utf8(&s[..n]) {
        Ok(t) => (t.chars().next().unwrap_or('\u{FFFD}'), n),
        Err(_) => ('\u{FFFD}', 1),
    }
}

fn scan_chunk(p: &[u8]) -> (bool, &[u8], &[u8]) {
    let mut p = p;
    let mut star = false;
    while !p.is_empty() && p[0] == b'*' {
        p = &p[1..];
        star = true;
    }
    let mut inrange = false;
    let mut i = 0;
    while i < p.len() {
        match p[i] {
            b'\\' => {
                if i + 1 < p.len() {
                    i += 1;
                }
            }
            b'[' => inrange = true,
            b']' => inrange = false,
            b'*' if !inrange => break,
            _ => {}
        }
        i += 1;
    }
    (star, &p[..i], &p[i..])
}

fn get_esc(chunk: &[u8]) -> Result<(char, &[u8]), ()> {
    if chunk.is_empty() || chunk[0] == b'-' || chunk[0] == b']' {
        return Err(());
    }
    let mut c = chunk;
    if c[0] == b'\\' {
        c = &c[1..];
        if c.is_empty() {
            return Err(());
        }
    }
    let (r, n) = decode_rune(c);
    if r == '\u{FFFD}' && n == 1 {
        return Err(());
    }
    let rest = &c[n..];
    if rest.is_empty() {
        return Err(());
    }
    Ok((r, rest))
}

fn match_chunk<'a>(chunk: &[u8], s: &'a [u8]) -> Result<Option<&'a [u8]>, ()> {
    let mut chunk = chunk;
    let mut s = s;
    let mut failed = false;
    while !chunk.is_empty() {
        if !failed && s.is_empty() {
            failed = true;
        }
        match chunk[0] {
            b'[' => {
                let mut r = '\0';
                if !failed {
                    let (c, n) = decode_rune(s);
                    r = c;
                    s = &s[n..];
                }
                chunk = &chunk[1..];
                let mut negated = false;
                if !chunk.is_empty() && chunk[0] == b'^' {
                    negated = true;
                    chunk = &chunk[1..];
                }
                let mut matched = false;
                let mut nrange = 0;
                loop {
                    if !chunk.is_empty() && chunk[0] == b']' && nrange > 0 {
                        chunk = &chunk[1..];
                        break;
                    }
                    let (lo, rest) = get_esc(chunk)?;
                    chunk = rest;
                    let mut hi = lo;
                    if chunk[0] == b'-' {
                        let (h, rest) = get_esc(&chunk[1..])?;
                        hi = h;
                        chunk = rest;
                    }
                    if lo <= r && r <= hi {
                        matched = true;
                    }
                    nrange += 1;
                }
                if matched == negated {
                    failed = true;
                }
            }
            b'?' => {
                if !failed {
                    if s[0] == b'/' {
                        failed = true;
                    }
                    let (_, n) = decode_rune(s);
                    s = &s[n..];
                }
                chunk = &chunk[1..];
            }
            c => {
                let mut c = c;
                if c == b'\\' {
                    chunk = &chunk[1..];
                    if chunk.is_empty() {
                        return Err(());
                    }
                    c = chunk[0];
                }
                if !failed {
                    if c != s[0] {
                        failed = true;
                    }
                    s = &s[1..];
                }
                chunk = &chunk[1..];
            }
        }
    }
    if failed { Ok(None) } else { Ok(Some(s)) }
}

/// Go `filepath.Match`.
pub fn matches(pattern: &str, name: &str) -> Result<bool, String> {
    let bad = |_| ERR_BAD_PATTERN.to_string();
    let mut pattern = pattern.as_bytes();
    let mut name = name.as_bytes();
    'outer: while !pattern.is_empty() {
        let (star, chunk, rest) = scan_chunk(pattern);
        pattern = rest;
        if star && chunk.is_empty() {
            return Ok(!name.contains(&b'/'));
        }
        let r = match_chunk(chunk, name);
        if let Ok(Some(t)) = r
            && (t.is_empty() || !pattern.is_empty())
        {
            name = t;
            continue;
        }
        r.map_err(bad)?;
        if star {
            let mut i = 0;
            while i < name.len() && name[i] != b'/' {
                match match_chunk(chunk, &name[i + 1..]) {
                    Ok(Some(t)) => {
                        if pattern.is_empty() && !t.is_empty() {
                            i += 1;
                            continue;
                        }
                        name = t;
                        continue 'outer;
                    }
                    Ok(None) => {}
                    Err(e) => return Err(bad(e)),
                }
                i += 1;
            }
        }
        while !pattern.is_empty() {
            let (_, chunk, rest) = scan_chunk(pattern);
            pattern = rest;
            match_chunk(chunk, b"").map_err(bad)?;
        }
        return Ok(false);
    }
    Ok(name.is_empty())
}

fn has_meta(p: &str) -> bool {
    p.contains(['*', '?', '[', '\\'])
}

fn clean_glob_path(p: &str) -> String {
    match p {
        "" => ".".to_string(),
        "/" => p.to_string(),
        _ => p[..p.len() - 1].to_string(),
    }
}

fn glob_dir(dir: &str, pattern: &str, m: &mut Vec<String>) -> Result<(), String> {
    let Ok(md) = std::fs::metadata(dir) else { return Ok(()) };
    if !md.is_dir() {
        return Ok(());
    }
    let Ok(rd) = std::fs::read_dir(dir) else { return Ok(()) };
    let mut names: Vec<String> = rd.filter_map(|e| e.ok()).map(|e| e.file_name().to_string_lossy().into_owned()).collect();
    names.sort();
    for n in names {
        if matches(pattern, &n)? {
            m.push(net::config::clean_path(&format!("{dir}/{n}")));
        }
    }
    Ok(())
}

/// Go `filepath.Glob`: the names matching `pattern`, sorted per directory; I/O errors are ignored and the only
/// error is a malformed pattern.
pub fn glob(pattern: &str) -> Result<Vec<String>, String> {
    glob_depth(pattern, 0)
}

fn glob_depth(pattern: &str, depth: usize) -> Result<Vec<String>, String> {
    if depth > 10000 {
        return Err(ERR_BAD_PATTERN.to_string());
    }
    matches(pattern, "")?;
    if !has_meta(pattern) {
        if std::fs::symlink_metadata(pattern).is_err() {
            return Ok(Vec::new());
        }
        return Ok(vec![pattern.to_string()]);
    }
    let (dir, file) = match pattern.rfind('/') {
        Some(i) => (&pattern[..=i], &pattern[i + 1..]),
        None => ("", pattern),
    };
    let dir = clean_glob_path(dir);
    let mut out = Vec::new();
    if !has_meta(&dir) {
        glob_dir(&dir, file, &mut out)?;
        return Ok(out);
    }
    if dir == pattern {
        return Err(ERR_BAD_PATTERN.to_string());
    }
    for d in glob_depth(&dir, depth + 1)? {
        glob_dir(&d, file, &mut out)?;
    }
    Ok(out)
}
