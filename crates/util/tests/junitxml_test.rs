//! Port of upstream/util/junitxml/junitxml_test.go.
use util::junitxml::JUnitXml;

/// The output of Go's `WriteXML` for `FakeTestSuites`, taken from the upstream code.
const GO_OUTPUT: &str = concat!(
    r#"<testsuites><testsuite name="all good" tests="3" failures="0" errors="0" skipped="0" timestamp="">"#,
    r#"<testcase name="alpha"></testcase><testcase name="beta"></testcase><testcase name="gamma"></testcase></testsuite>"#,
    r#"<testsuite name="mixed" tests="3" failures="2" errors="1" skipped="0" timestamp=""><testcase name="good"></testcase>"#,
    r#"<testcase name="bad"><failure>once</failure><failure>twice</failure></testcase><testcase name="ugly"><error>buggy</error></testcase></testsuite>"#,
    r#"<testsuite name="fast" tests="1" failures="1" errors="0" skipped="0" timestamp=""><testcase name="unknown"><failure>fail early</failure></testcase></testsuite>"#,
    r#"</testsuites>"#
);

#[test]
fn test_junit_output() {
    let mut buf: Vec<u8> = Vec::new();
    let x = fake_test_suites();
    if let Err(e) = x.write_xml(&mut buf) {
        panic!("Failed to encode XML: {e}");
    }

    let output = String::from_utf8(buf).expect("utf8");

    // adapted: Go decodes the output with encoding/xml and counts the elements. There is no XML decoder here, so
    // the output must equal Go's output byte for byte, and the elements are counted in the text.
    assert_eq!(GO_OUTPUT, output);
    let total = output.matches("<testsuite ").count();
    assert_eq!(total, 3, "JUnit output had {total} testsuite elements; expected 3");
    let cases = output.matches("<testcase ").count();
    assert_eq!(cases, 7, "JUnit output had {cases} testcase; expected 7");
}

#[derive(Debug)]
struct Buggy;
impl std::fmt::Display for Buggy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("buggy")
    }
}

fn fake_test_suites() -> JUnitXml {
    let mut ju = JUnitXml::default();
    let good = ju.suite("all good");
    good.case("alpha");
    good.case("beta");
    good.case("gamma");
    let mixed = ju.suite("mixed");
    mixed.case("good");
    let bad = mixed.case("bad");
    bad.fail("once");
    bad.fail("twice");
    mixed.case("ugly").abort(&Buggy);
    ju.suite("fast").fail("fail early");
    ju
}
