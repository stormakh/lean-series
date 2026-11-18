// Extra check (not a ledger test): compares the Go syntax parser port, the translated regex engine and
// FastRegexMatcher with real Go (grafana/regexp) on about 530 patterns (Go's own parser tests, the Prometheus regexp
// tests and edge cases) and 120 values: parse trees, parse errors, anchored matches, FindStringSubmatchIndex and
// ReplaceAllString. The golden file is generated with Go, so the test is ignored by default:
//
//   cd crates/model/gen/regexp_gold && go run . patterns.txt values.txt > /tmp/regexp_gold.jsonl
//   MODEL_REGEXP_GOLD=/tmp/regexp_gold.jsonl cargo test -p model --test go_regexp_gold -- --ignored

use model::regexp::{syntax, Regexp};

fn dump(re: &syntax::Regexp) -> String {
    let runes: Vec<String> = re.rune.iter().map(|r| r.to_string()).collect();
    let mut s = format!(
        "{}/{}/[{}]/{}/{}/{}/{}(",
        re.op as u8,
        re.flags,
        runes.join(" "),
        re.min,
        re.max,
        re.cap,
        model::unicode::quote(&re.name)
    );
    for (i, x) in re.sub.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&dump(x));
    }
    s.push(')');
    s
}

#[test]
#[ignore]
fn go_regexp_gold() {
    let gold = std::env::var("MODEL_REGEXP_GOLD").expect("set MODEL_REGEXP_GOLD to the golden file");
    let vals: Vec<String> = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/gen/regexp_gold/values.txt"))
        .unwrap()
        .lines()
        .map(|l| json::Json::parse(l).unwrap_or_else(|_| panic!("{l}")).as_str().unwrap().to_string())
        .collect();
    let mut fails = 0;
    for line in std::fs::read_to_string(&gold).unwrap().lines() {
        let r = json::Json::parse(line).unwrap();
        let p = r.str_of("p");
        let tree = match syntax::parse(p, syntax::PERL | syntax::DOT_NL) {
            Ok(t) => dump(&t),
            Err(e) => format!("ERR {}", e),
        };
        if tree != r.str_of("tree") {
            fails += 1;
            println!("TREE {:?}\n  go: {}\n  rs: {}", p, r.str_of("tree"), tree);
        }
        let treep = match syntax::parse(p, syntax::PERL) {
            Ok(t) => dump(&t),
            Err(e) => format!("ERR {}", e),
        };
        if treep != r.str_of("tree_perl") {
            fails += 1;
            println!("TREEP {:?}\n  go: {}\n  rs: {}", p, r.str_of("tree_perl"), treep);
        }
        let full = format!("^(?s:{})$", p);
        match Regexp::compile(&full) {
            Err(e) => {
                if e.to_string() != r.str_of("compile") {
                    fails += 1;
                    println!("COMPILE {:?}: go {:?} rs {:?}", p, r.str_of("compile"), e.to_string());
                }
            }
            Ok(re) => {
                if !r.str_of("compile").is_empty() {
                    fails += 1;
                    println!("COMPILE {:?}: go {:?} rs ok", p, r.str_of("compile"));
                    continue;
                }
                let m = r.get("match").unwrap().as_arr().unwrap();
                let fm = model::regexp::FastRegexMatcher::new(p).unwrap();
                for (i, v) in vals.iter().enumerate() {
                    let want = m[i].as_bool().unwrap();
                    if fm.match_string(v) != want {
                        fails += 1;
                        println!("FAST {:?} on {:?}: go {} ({:?})", p, v, want, fm.string_matcher());
                    }
                    if re.is_match(v) != want {
                        fails += 1;
                        println!("MATCH {:?} on {:?}: go {} ({})", p, v, want, re.translated());
                    }
                }
            }
        }
        if let Ok(re) = Regexp::compile(p) {
            let f = r.get("find").unwrap().as_arr().unwrap();
            let rp = r.get("repl").unwrap().as_arr().unwrap();
            for (i, v) in vals.iter().enumerate() {
                let got = re.find_string_submatch_index(v);
                let want: Option<Vec<isize>> =
                    f[i].as_arr().map(|a| a.iter().map(|x| x.as_i64().unwrap() as isize).collect());
                if got != want {
                    fails += 1;
                    println!("FIND {:?} on {:?}: go {:?} rs {:?}", p, v, want, got);
                }
                let got = re.replace_all_string(v, "<$1|${name}|$0>");
                if got != rp[i].as_str().unwrap() {
                    fails += 1;
                    println!("REPL {:?} on {:?}: go {:?} rs {:?}", p, v, rp[i].as_str().unwrap(), got);
                }
            }
        }
    }
    println!("fails: {fails}");
    assert_eq!(fails, 0);
}
