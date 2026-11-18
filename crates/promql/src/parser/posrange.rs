//! Port of `promql/parser/posrange`: positions in query strings for error and warning messages.

/// A byte position in a query string. Negative numbers mark undefined positions.
pub type Pos = i32;

/// A position range in the input string of the parser.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct PositionRange {
    /// Start of the range, zero-indexed.
    pub start: Pos,
    /// End of the range, zero-indexed.
    pub end: Pos,
}

impl PositionRange {
    /// Converts the start of the range into a `line:col` string, or says why that is not possible
    /// (Go `StartPosInput`). `line_offset` is added to the line number.
    pub fn start_pos_input(&self, query: &str, line_offset: i32) -> String {
        if query.is_empty() {
            return "unknown position".to_string();
        }
        let pos = self.start;
        if pos < 0 || pos as usize > query.len() {
            return "invalid position".to_string();
        }
        let pos = pos as usize;
        let mut last_line_break: isize = -1;
        let mut line = line_offset + 1;
        for (i, c) in query.as_bytes()[..pos].iter().enumerate() {
            if *c == b'\n' {
                last_line_break = i as isize;
                line += 1;
            }
        }
        let col = pos as isize - last_line_break;
        format!("{}:{}", line, col)
    }
}
