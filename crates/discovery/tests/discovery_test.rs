//! Port of `discovery/discovery_test.go`.
use discovery::Configs;

#[test]
fn test_configs_custom_un_marshal_marshal() {
    let input = "static_configs:
- targets:
  - foo:1234
  - bar:4321
";
    let cfg: Configs = yaml::unmarshal_strict(input).expect("unmarshal");

    let output = yaml::marshal(&cfg).expect("marshal");
    assert_eq!(input, output);
}
