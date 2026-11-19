//! Port of `storage/remote/stats.go` (and the same type in client_golang `exp/api/remote`): the remote write
//! 2.0 response statistics headers.

use net::types::Header;

pub const RW20_WRITTEN_SAMPLES_HEADER: &str = "X-Prometheus-Remote-Write-Samples-Written";
pub const RW20_WRITTEN_HISTOGRAMS_HEADER: &str = "X-Prometheus-Remote-Write-Histograms-Written";
pub const RW20_WRITTEN_EXEMPLARS_HEADER: &str = "X-Prometheus-Remote-Write-Exemplars-Written";

/// Go `WriteResponseStats`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteResponseStats {
    pub samples: i64,
    pub histograms: i64,
    pub exemplars: i64,
    /// True when at least one stats header was present (the receiver speaks 2.0).
    pub confirmed: bool,
}

impl WriteResponseStats {
    pub fn no_data_written(&self) -> bool {
        self.samples + self.histograms + self.exemplars == 0
    }

    pub fn all_samples(&self) -> i64 {
        self.samples + self.histograms
    }

    /// Go `Add` (returns the sum; `confirmed` comes from `rs`).
    pub fn add(mut self, rs: WriteResponseStats) -> WriteResponseStats {
        self.confirmed = rs.confirmed;
        self.samples += rs.samples;
        self.histograms += rs.histograms;
        self.exemplars += rs.exemplars;
        self
    }

    /// Go `SetHeaders`: the three headers as (name, value) pairs.
    pub fn headers(&self) -> [(&'static str, String); 3] {
        [
            (RW20_WRITTEN_SAMPLES_HEADER, self.samples.to_string()),
            (RW20_WRITTEN_HISTOGRAMS_HEADER, self.histograms.to_string()),
            (RW20_WRITTEN_EXEMPLARS_HEADER, self.exemplars.to_string()),
        ]
    }
}

/// Go `strconv.Atoi` error text.
fn atoi(v: &str) -> Result<i64, String> {
    v.parse::<i64>().map_err(|e| {
        let kind = if matches!(e.kind(), std::num::IntErrorKind::PosOverflow | std::num::IntErrorKind::NegOverflow) {
            "value out of range"
        } else {
            "invalid syntax"
        };
        format!("strconv.Atoi: parsing {:?}: {kind}", v)
    })
}

/// Go `ParseWriteResponseStats`: stats from the response headers. Missing headers mean 0; `confirmed` is set
/// when at least one header is present. The error joins every parse error (Go `errors.Join`, one per line).
pub fn parse_write_response_stats(h: &Header) -> (WriteResponseStats, Option<String>) {
    let mut s = WriteResponseStats::default();
    let mut errs: Vec<String> = Vec::new();
    for (name, slot) in [
        (RW20_WRITTEN_SAMPLES_HEADER, 0usize),
        (RW20_WRITTEN_HISTOGRAMS_HEADER, 1),
        (RW20_WRITTEN_EXEMPLARS_HEADER, 2),
    ] {
        let v = h.get_str(name);
        if v.is_empty() {
            continue;
        }
        s.confirmed = true;
        let n = match atoi(v) {
            Ok(n) => n,
            Err(e) => {
                errs.push(e);
                0
            }
        };
        match slot {
            0 => s.samples = n,
            1 => s.histograms = n,
            _ => s.exemplars = n,
        }
    }
    (s, if errs.is_empty() { None } else { Some(errs.join("\n")) })
}
