// Port of promql/parser/lex_test.go. The table is read from a fixture that scripts/gen/promql_parser_fixtures.sh
// dumps from the Go test table.

mod common;

use common::*;
use promql::parser::*;

// TestLexer tests basic functionality of the lexer. More elaborate tests are implemented for the parser to avoid
// duplicated effort.
#[test]
fn test_lexer() {
    let groups = fixture("lex_test.json");
    let mut n = 0;
    for group in groups.as_arr().unwrap() {
        let gname = group.get("name").unwrap().as_str().unwrap();
        for (i, test) in group.get("tests").unwrap().as_arr().unwrap().iter().enumerate() {
            let input = match fixture_str(test.get("input").unwrap()) {
                Ok(s) => s,
                // adapted: a Go input with bytes that are not valid UTF-8 cannot be a Rust &str. Go's lexer sees
                // such a byte as utf8.RuneError, which is U+FFFD, so the input with U+FFFD takes the same path.
                Err(b) => String::from_utf8_lossy(&b).into_owned(),
            };
            n += 1;
            let series_desc = test.get("seriesDesc").unwrap().as_bool().unwrap();
            let mut l = Lexer::new(&input).with_series_desc(series_desc);
            let mut out: Vec<Item> = Vec::new();
            while l.has_state() {
                let mut it = Item::default();
                l.next_item(&mut it);
                out.push(it);
            }

            let last = out.last().unwrap().clone();
            if test.get("fail").unwrap().as_bool().unwrap() {
                assert!(
                    out.iter().any(|it| it.typ == ERROR),
                    "{}/{}: input {:?}, expected lexing error but did not fail",
                    gname,
                    i,
                    input
                );
                continue;
            }
            assert_ne!(
                ERROR, last.typ,
                "{}/{}: input {:?}, unexpected lexing error at position {}: {}",
                gname, i, input, last.pos, last
            );

            let eof = Item { typ: EOF, pos: input.len() as Pos, val: "".into() };
            assert_eq!(eof, last, "{}/{}: input {:?}", gname, i, input);

            out.pop();
            let want: Vec<(u16, i64, String)> = test
                .get("expected")
                .unwrap()
                .as_arr()
                .unwrap()
                .iter()
                .map(|it| {
                    (
                        it.get("Typ").unwrap().as_i64().unwrap() as u16,
                        it.get("Pos").unwrap().as_i64().unwrap(),
                        fixture_str(it.get("Val").unwrap()).unwrap(),
                    )
                })
                .collect();
            let got: Vec<(u16, i64, String)> =
                out.iter().map(|it| (it.typ.0, it.pos as i64, it.val.to_string())).collect();
            assert_eq!(want, got, "{}/{}: input {:?}", gname, i, input);
        }
    }
    assert!(n > 100);
}
