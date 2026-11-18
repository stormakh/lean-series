//! Ported from github.com/alecthomas/units bytes_test.go (Base2Bytes, which Prometheus configs use).
use yaml::{Base2Bytes, parse_base2_bytes};

#[test]
fn test_base2_bytes_string() {
    assert_eq!(Base2Bytes(0).go_string(), "0B");
    assert_eq!(Base2Bytes(1025).go_string(), "1KiB1B");
    assert_eq!(Base2Bytes(1048577).go_string(), "1MiB1B");
}

#[test]
fn test_parse_base2_bytes() {
    assert_eq!(parse_base2_bytes("0B").unwrap().0, 0);
    assert!(parse_base2_bytes("1kB").is_err());
    assert_eq!(parse_base2_bytes("1KB").unwrap().0, 1024);
    assert_eq!(parse_base2_bytes("1MB1KB25B").unwrap().0, 1049625);
    assert_eq!(parse_base2_bytes("1.5MB").unwrap().0, 1572864);
    assert!(parse_base2_bytes("1kiB").is_err());
    assert_eq!(parse_base2_bytes("1KiB").unwrap().0, 1024);
    assert_eq!(parse_base2_bytes("1MiB1KiB25B").unwrap().0, 1049625);
    assert_eq!(parse_base2_bytes("1.5MiB").unwrap().0, 1572864);
}

// adapted: UnmarshalText is the YAML scalar decode of Base2Bytes (go-yaml calls UnmarshalText for it).
#[test]
fn test_base2_bytes_unmarshal_text() {
    let u = |s: &str| yaml::unmarshal::<Base2Bytes>(s);
    assert_eq!(u("0B").unwrap().0, 0);
    assert!(u("1kB").is_err());
    assert_eq!(u("1KB").unwrap().0, 1024);
    assert_eq!(u("1MB1KB25B").unwrap().0, 1049625);
    assert_eq!(u("1.5MB").unwrap().0, 1572864);
    assert!(u("1kiB").is_err());
    assert_eq!(u("1KiB").unwrap().0, 1024);
    assert_eq!(u("1MiB1KiB25B").unwrap().0, 1049625);
    assert_eq!(u("1.5MiB").unwrap().0, 1572864);
    // go-yaml hands even an int scalar to UnmarshalText, which needs a unit
    assert_eq!(u("1024").unwrap_err().to_string(), "units: unknown unit  in 1024");
}
