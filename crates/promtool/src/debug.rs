//! Port of cmd/promtool/debug.go, archive.go and the debug endpoint tables of main.go
//! (`promtool debug pprof|metrics|all`).
//!
//! Differs from Go: the endpoints are fetched in a fixed order (Go iterates a map); pprof profiles are only
//! gunzipped instead of parsed and re-serialized with `profile.WriteUncompressed` (same profile data, the
//! protobuf bytes may differ).

use std::fs::File;
use std::io::{Read, Write};

use crate::{FAILURE_EXIT_CODE, SUCCESS_EXIT_CODE};

/// Go `endpointsGroup`: URL path to file name, and whether the body is a pprof profile.
struct EndpointsGroup {
    url_to_filename: &'static [(&'static str, &'static str)],
    profile: bool,
}

const PPROF_ENDPOINTS: &[EndpointsGroup] = &[
    EndpointsGroup {
        url_to_filename: &[
            ("/debug/pprof/profile?seconds=30", "cpu.pb"),
            ("/debug/pprof/block", "block.pb"),
            ("/debug/pprof/goroutine", "goroutine.pb"),
            ("/debug/pprof/heap", "heap.pb"),
            ("/debug/pprof/mutex", "mutex.pb"),
            ("/debug/pprof/threadcreate", "threadcreate.pb"),
        ],
        profile: true,
    },
    EndpointsGroup { url_to_filename: &[("/debug/pprof/trace?seconds=30", "trace.pb")], profile: false },
];

const METRICS_ENDPOINTS: &[EndpointsGroup] =
    &[EndpointsGroup { url_to_filename: &[("/metrics", "metrics.txt")], profile: false }];

fn post_process_profile(b: Vec<u8>) -> Result<Vec<u8>, String> {
    if b.len() >= 2 && b[0] == 0x1f && b[1] == 0x8b {
        let mut d = flate2::read::GzDecoder::new(&b[..]);
        let mut out = Vec::new();
        d.read_to_end(&mut out).map_err(|e| format!("decompressing profile: {e}"))?;
        return Ok(out);
    }
    Ok(b)
}

/// Go `tarGzFileWriter`.
pub struct TarGzFileWriter {
    gz: flate2::write::GzEncoder<File>,
}

impl TarGzFileWriter {
    /// Go `newTarGzFileWriter`.
    pub fn new(archive_name: &str) -> Result<TarGzFileWriter, String> {
        let file = File::create(archive_name)
            .map_err(|e| format!("error creating archive {}: open {archive_name}: {}", go_q(archive_name), crate::io_err_text(&e)))?;
        Ok(TarGzFileWriter { gz: flate2::write::GzEncoder::new(file, flate2::Compression::default()) })
    }

    /// Go `write`: a regular file entry with mode 0666.
    pub fn write(&mut self, filename: &str, b: &[u8]) -> Result<(), String> {
        let mut h = [0u8; 512];
        let name = filename.as_bytes();
        if name.len() > 100 {
            return Err(format!("archive/tar: cannot encode header: Format specifies USTAR; and USTAR cannot encode Name={}", go_q(filename)));
        }
        h[..name.len()].copy_from_slice(name);
        let octal = |field: &mut [u8], v: u64| {
            let s = format!("{:0width$o}", v, width = field.len() - 1);
            field[..s.len()].copy_from_slice(s.as_bytes());
            field[s.len()] = 0;
        };
        octal(&mut h[100..108], 0o666);
        octal(&mut h[108..116], 0);
        octal(&mut h[116..124], 0);
        octal(&mut h[124..136], b.len() as u64);
        octal(&mut h[136..148], 0);
        h[156] = b'0';
        h[257..263].copy_from_slice(b"ustar\0");
        h[263..265].copy_from_slice(b"00");
        octal(&mut h[329..337], 0);
        octal(&mut h[337..345], 0);
        for c in h[148..156].iter_mut() {
            *c = b' ';
        }
        let sum: u64 = h.iter().map(|&c| c as u64).sum();
        let s = format!("{sum:06o}");
        h[148..154].copy_from_slice(s.as_bytes());
        h[154] = 0;
        h[155] = b' ';
        self.gz.write_all(&h).map_err(|e| e.to_string())?;
        self.gz.write_all(b).map_err(|e| e.to_string())?;
        let pad = (512 - b.len() % 512) % 512;
        self.gz.write_all(&vec![0u8; pad]).map_err(|e| e.to_string())?;
        Ok(())
    }

    /// Go `close`.
    pub fn close(mut self) -> Result<(), String> {
        self.gz.write_all(&[0u8; 1024]).map_err(|e| e.to_string())?;
        let f = self.gz.finish().map_err(|e| e.to_string())?;
        f.sync_all().ok();
        Ok(())
    }
}

fn go_q(s: &str) -> String {
    promql::parser::quote::go_quote(s)
}

/// Go `debugWrite`.
fn debug_write(server_url: &str, tarball_name: &str, groups: &[&[EndpointsGroup]], out: &mut dyn Write) -> Result<(), String> {
    let mut archiver = TarGzFileWriter::new(tarball_name).map_err(|e| format!("error creating a new archiver: {e}"))?;
    let client = net::Client::new(crate::api::default_round_tripper());
    for group in groups.iter().flat_map(|g| g.iter()) {
        for (path, filename) in group.url_to_filename {
            let url = format!("{server_url}{path}");
            let _ = writeln!(out, "collecting: {url}");
            let mut req = net::Request::get(&url).map_err(|e| format!("error executing HTTP request: {e}"))?;
            req.decompress = true;
            let res = client.do_request(req).map_err(|e| format!("error executing HTTP request: {e}"))?;
            let mut body = res.body;
            if group.profile {
                body = post_process_profile(body).map_err(|e| format!("error post-processing HTTP response body: {e}"))?;
            }
            archiver.write(filename, &body).map_err(|e| format!("error writing into the archive: {e}"))?;
        }
    }
    archiver.close().map_err(|e| format!("error closing archive writer: {e}"))?;
    let _ = writeln!(out, "Compiling debug information complete, all files written in {}.", go_q(tarball_name));
    Ok(())
}

fn run(url: &str, groups: &[&[EndpointsGroup]], out: &mut dyn Write, err: &mut dyn Write) -> i32 {
    match debug_write(url, "debug.tar.gz", groups, out) {
        Ok(()) => SUCCESS_EXIT_CODE,
        Err(e) => {
            let _ = writeln!(err, "error completing debug command: {e}");
            FAILURE_EXIT_CODE
        }
    }
}

/// Go `debugPprof`.
pub fn debug_pprof(url: &str, out: &mut dyn Write, err: &mut dyn Write) -> i32 {
    run(url, &[PPROF_ENDPOINTS], out, err)
}

/// Go `debugMetrics`.
pub fn debug_metrics(url: &str, out: &mut dyn Write, err: &mut dyn Write) -> i32 {
    run(url, &[METRICS_ENDPOINTS], out, err)
}

/// Go `debugAll`.
pub fn debug_all(url: &str, out: &mut dyn Write, err: &mut dyn Write) -> i32 {
    run(url, &[PPROF_ENDPOINTS, METRICS_ENDPOINTS], out, err)
}
