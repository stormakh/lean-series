//! Port of the tests of `model/textparse/benchmark_test.go` (the Go benchmarks themselves are not tests).

use model::labels::Labels;
use textparse::strconv::format_g;
use textparse::{Entry, NHCBParser, OpenMetrics2Parser, OpenMetricsOptions, OpenMetricsParser, Parser, ParserOptions};

/// Go `readTestdataFile`.
fn read_testdata_file(file: &str) -> Vec<u8> {
    let path = format!("{}/../../upstream/model/textparse/testdata/{file}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("read {path}: {e}"))
}

/// Go `collectSeries`: parses `data` and returns a sorted multiset of "labels => value" strings.
fn collect_series(data: &[u8], parser: &str) -> Vec<String> {
    let om1 = || -> Box<dyn Parser + '_> {
        Box::new(OpenMetricsParser::new(data, OpenMetricsOptions { skip_st_series: true, ..Default::default() }))
    };
    let mut p: Box<dyn Parser + '_> = match parser {
        "omtext" => om1(),
        "omtext_with_nhcb" => Box::new(NHCBParser::new(om1(), false, false)),
        "om2text" => Box::new(OpenMetrics2Parser::new(data, &ParserOptions::default())),
        "om2text_with_nhcb" => Box::new(OpenMetrics2Parser::new(
            data,
            &ParserOptions { convert_classic_histograms_to_nhcb: true, ..Default::default() },
        )),
        _ => panic!("unknown parser {parser:?}"),
    };

    let mut out = Vec::new();
    let mut ls = Labels::default();
    loop {
        let entry = match p.next() {
            Ok(e) => e,
            Err(e) if e.is_eof() => break,
            Err(e) => panic!("{parser}: unexpected error: {e}"),
        };
        match entry {
            Entry::Series => {
                let (_, _, v) = p.series();
                p.labels(&mut ls);
                out.push(format!("{ls} => {}", format_g(v)));
            }
            Entry::Histogram => {
                let (_, _, h, fh) = p.histogram();
                let s = match (h, fh) {
                    (Some(h), _) => h.string(),
                    (None, Some(fh)) => fh.string(),
                    (None, None) => panic!("{parser}: histogram entry without a histogram"),
                };
                p.labels(&mut ls);
                out.push(format!("{ls} => {s}"));
            }
            _ => {}
        }
    }
    out.sort();
    out
}

#[test]
fn test_om1_om2_bench_pairs_equivalent() {
    for (name, om1, om2) in [
        ("alltypes", "alltypes.bench.om.txt", "alltypes.bench.om2.txt"),
        ("ct", "ct.bench.om.txt", "ct.bench.om2.txt"),
        ("histograms", "histograms.bench.om.txt", "histograms.bench.om2.txt"),
    ] {
        let om1_data = read_testdata_file(om1);
        let om2_data = read_testdata_file(om2);
        let om1_series = collect_series(&om1_data, "omtext");
        let om2_series = collect_series(&om2_data, "om2text");
        assert_eq!(om1_series, om2_series, "{name}: OM1 and OM2 benchmark files must produce the same series multiset");

        let om1_nhcb_series = collect_series(&om1_data, "omtext_with_nhcb");
        let om2_nhcb_series = collect_series(&om2_data, "om2text_with_nhcb");
        assert_eq!(
            om1_nhcb_series, om2_nhcb_series,
            "{name}: OM1 and OM2 benchmark files with NHCB conversion must produce the same series multiset"
        );
    }
}
