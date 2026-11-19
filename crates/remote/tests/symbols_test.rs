//! Port of `prompb/io/prometheus/write/v2/symbols_test.go`.

use model::labels::{self, ScratchBuilder};
use remote::prompb::writev2::{desymbolize_labels, SymbolsTable};

fn strs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[test]
fn test_symbols_table() {
    let mut s = SymbolsTable::new();
    assert_eq!(strs(&[""]), s.symbols(), "required empty reference does not exist");
    assert_eq!(0, s.symbolize(""));
    assert_eq!(strs(&[""]), s.symbols());

    assert_eq!(1, s.symbolize("abc"));
    assert_eq!(strs(&["", "abc"]), s.symbols());

    assert_eq!(2, s.symbolize("__name__"));
    assert_eq!(strs(&["", "abc", "__name__"]), s.symbols());

    assert_eq!(3, s.symbolize("foo"));
    assert_eq!(strs(&["", "abc", "__name__", "foo"]), s.symbols());

    s.reset();
    assert_eq!(strs(&[""]), s.symbols(), "required empty reference does not exist");
    assert_eq!(0, s.symbolize(""));

    assert_eq!(1, s.symbolize("__name__"));
    assert_eq!(strs(&["", "__name__"]), s.symbols());

    assert_eq!(2, s.symbolize("abc"));
    assert_eq!(strs(&["", "__name__", "abc"]), s.symbols());

    let ls = labels::from_strings(&["__name__", "qwer", "zxcv", "1234"]);
    let mut encoded = Vec::new();
    s.symbolize_labels(&ls, &mut encoded);
    assert_eq!(vec![1u32, 3, 4, 5], encoded);
    let mut b = ScratchBuilder::new(encoded.len());
    let decoded = desymbolize_labels(&mut b, &encoded, s.symbols()).expect("desymbolize");
    assert_eq!(ls, decoded);

    // Different buf.
    let ls = labels::from_strings(&["__name__", "qwer", "zxcv2222", "1234"]);
    let mut encoded = vec![1u32, 3, 4, 5];
    s.symbolize_labels(&ls, &mut encoded);
    assert_eq!(vec![1u32, 3, 6, 5], encoded);
}
