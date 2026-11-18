//! Port of `openmetrics2parse.go` and its lexer `openmetrics2lex.l`. (Stub: being written.)

use model::exemplar::Exemplar;
use model::histogram::{FloatHistogram, Histogram};
use model::labels::Labels;
use model::metadata::MetricType;

use crate::{Entry, Error, Parser, ParserKind, ParserOptions};

/// Parses the OpenMetrics 2.0 text format (Go `openMetrics2Parser`).
pub struct OpenMetrics2Parser<'a> {
    #[allow(dead_code)]
    b: &'a [u8],
}

impl<'a> OpenMetrics2Parser<'a> {
    /// Go `NewOpenMetrics2Parser`.
    pub fn new(b: &'a [u8], _opts: &ParserOptions) -> OpenMetrics2Parser<'a> {
        OpenMetrics2Parser { b }
    }
}

impl Parser for OpenMetrics2Parser<'_> {
    fn series(&self) -> (&[u8], Option<i64>, f64) {
        (&[], None, 0.0)
    }
    fn histogram(&mut self) -> (&[u8], Option<i64>, Option<&Histogram>, Option<&FloatHistogram>) {
        (&[], None, None, None)
    }
    fn help(&mut self) -> (&[u8], &[u8]) {
        (&[], &[])
    }
    fn typ(&self) -> (&[u8], MetricType) {
        (&[], MetricType::Empty)
    }
    fn unit(&self) -> (&[u8], &[u8]) {
        (&[], &[])
    }
    fn comment(&self) -> &[u8] {
        &[]
    }
    fn labels(&mut self, _l: &mut Labels) {}
    fn exemplar(&mut self, _e: &mut Exemplar) -> bool {
        false
    }
    fn start_timestamp(&mut self) -> i64 {
        0
    }
    fn next(&mut self) -> Result<Entry, Error> {
        Err(Error::msg("openmetrics2 parser not implemented"))
    }
    fn kind(&self) -> ParserKind {
        ParserKind::OpenMetrics2
    }
}
