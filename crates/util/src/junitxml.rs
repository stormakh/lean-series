//! Port of `util/junitxml`: the JUnit XML report of `promtool test rules`, written as Go's `encoding/xml` writes it.

#[derive(Debug, Default, Clone, PartialEq)]
pub struct JUnitXml {
    pub suites: Vec<TestSuite>,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct TestSuite {
    pub name: String,
    pub test_count: usize,
    pub failure_count: usize,
    pub error_count: usize,
    pub skipped_count: usize,
    pub timestamp: String,
    pub cases: Vec<TestCase>,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct TestCase {
    pub name: String,
    pub failures: Vec<String>,
    pub error: String,
}

impl JUnitXml {
    /// Go `WriteXML`: one line, no XML header, no indentation.
    pub fn write_xml(&self, w: &mut dyn std::io::Write) -> std::io::Result<()> {
        w.write_all(self.to_xml().as_bytes())
    }

    pub fn to_xml(&self) -> String {
        let mut s = String::with_capacity(256);
        s.push_str("<testsuites>");
        for ts in &self.suites {
            s.push_str("<testsuite name=\"");
            escape(&mut s, &ts.name);
            s.push_str(&format!(
                "\" tests=\"{}\" failures=\"{}\" errors=\"{}\" skipped=\"{}\" timestamp=\"",
                ts.test_count, ts.failure_count, ts.error_count, ts.skipped_count
            ));
            escape(&mut s, &ts.timestamp);
            s.push_str("\">");
            for c in &ts.cases {
                s.push_str("<testcase name=\"");
                escape(&mut s, &c.name);
                s.push_str("\">");
                for f in &c.failures {
                    s.push_str("<failure>");
                    escape(&mut s, f);
                    s.push_str("</failure>");
                }
                if !c.error.is_empty() {
                    s.push_str("<error>");
                    escape(&mut s, &c.error);
                    s.push_str("</error>");
                }
                s.push_str("</testcase>");
            }
            s.push_str("</testsuite>");
        }
        s.push_str("</testsuites>");
        s
    }

    /// Go `Suite`: adds a suite and returns it.
    pub fn suite(&mut self, name: &str) -> &mut TestSuite {
        self.suites.push(TestSuite { name: name.to_string(), ..TestSuite::default() });
        let n = self.suites.len() - 1;
        &mut self.suites[n]
    }
}

impl TestSuite {
    /// Go `Fail`: adds a failure to the last case (a case "unknown" when there is none).
    pub fn fail(&mut self, f: &str) {
        self.failure_count += 1;
        self.last_case().failures.push(f.to_string());
    }

    fn last_case(&mut self) -> &mut TestCase {
        if self.cases.is_empty() {
            self.case("unknown");
        }
        let n = self.cases.len() - 1;
        &mut self.cases[n]
    }

    /// Go `Case`: adds a case and returns the suite.
    pub fn case(&mut self, name: &str) -> &mut TestSuite {
        self.cases.push(TestCase { name: name.to_string(), ..TestCase::default() });
        self.test_count += 1;
        self
    }

    /// Go `Settime`.
    pub fn settime(&mut self, name: &str) {
        self.timestamp = name.to_string();
    }

    /// Go `Abort`: sets the error of the last case.
    pub fn abort(&mut self, e: &dyn std::fmt::Display) {
        self.error_count += 1;
        self.last_case().error = e.to_string();
    }
}

/// Go `xml.EscapeText`: `& < > " '`, tab and line breaks as character references, and characters that XML does
/// not allow as U+FFFD.
fn escape(s: &mut String, v: &str) {
    for c in v.chars() {
        match c {
            '"' => s.push_str("&#34;"),
            '\'' => s.push_str("&#39;"),
            '&' => s.push_str("&amp;"),
            '<' => s.push_str("&lt;"),
            '>' => s.push_str("&gt;"),
            '\t' => s.push_str("&#x9;"),
            '\n' => s.push_str("&#xA;"),
            '\r' => s.push_str("&#xD;"),
            c if is_in_character_range(c) => s.push(c),
            _ => s.push('\u{FFFD}'),
        }
    }
}

fn is_in_character_range(c: char) -> bool {
    let r = c as u32;
    r == 0x09 || r == 0x0A || r == 0x0D || (0x20..=0xD7FF).contains(&r) || (0xE000..=0xFFFD).contains(&r) || (0x10000..=0x10FFFF).contains(&r)
}
