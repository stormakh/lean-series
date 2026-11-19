//! Port of upstream/util/httputil/compression_test.go and upstream/util/compression/compression_test.go (both Go
//! files are named compression_test.go, so both ports live here).
#[path = "common/rawhttp.rs"]
mod rawhttp;

use rawhttp::{Func, request, serve};
use std::io::Read;
use std::sync::Arc;
use util::compression::{
    self, ConcurrentDecodeBuffer, ConcurrentEncodeBuffer, DecodeBuffer, EncodeBuffer, NONE, SNAPPY, SyncDecodeBuffer,
    SyncEncodeBuffer, ZSTD,
};
use util::httputil::{ACCEPT_ENCODING_HEADER, CONTENT_ENCODING_HEADER, CompressionHandler, DEFLATE_ENCODING, GZIP_ENCODING};

// ---- util/httputil ----

fn get_compression_handler_func() -> CompressionHandler {
    let hf = |_: http::Request| {
        let mut r = http::Response::new(200);
        r.body = http::Body::Bytes(b"Hello World!".to_vec());
        r
    };
    CompressionHandler { handler: Arc::new(Func(hf)) }
}

#[test]
fn test_compression_handler_plain_text() {
    let port = serve(Arc::new(get_compression_handler_func()));

    let resp = request("GET", port, "/foo_endpoint", &[]).expect("client get failed with unexpected error");
    let contents = resp.body;

    let expected = "Hello World!";
    let actual = String::from_utf8(contents).expect("unexpected error while creating the response body reader");
    assert_eq!(expected, actual, "expected response with content");
}

#[test]
fn test_compression_handler_gzip() {
    let port = serve(Arc::new(get_compression_handler_func()));

    let resp = request("GET", port, "/foo_endpoint", &[(ACCEPT_ENCODING_HEADER, GZIP_ENCODING)])
        .expect("client get failed with unexpected error");

    let actual_header = resp.header(CONTENT_ENCODING_HEADER);
    assert_eq!(GZIP_ENCODING, actual_header, "unexpected encoding header in response");

    let mut buf = String::new();
    let mut zr = flate2::read::GzDecoder::new(&resp.body[..]);
    zr.read_to_string(&mut buf).expect("unexpected error while reading the response body");

    let expected = "Hello World!";
    assert_eq!(expected, buf, "unexpected response content");
}

#[test]
fn test_compression_handler_deflate() {
    let port = serve(Arc::new(get_compression_handler_func()));

    let resp = request("GET", port, "/foo_endpoint", &[(ACCEPT_ENCODING_HEADER, DEFLATE_ENCODING)])
        .expect("client get failed with unexpected error");

    let actual_header = resp.header(CONTENT_ENCODING_HEADER);
    assert_eq!(DEFLATE_ENCODING, actual_header, "expected response with encoding header");

    let mut buf = String::new();
    let mut dr = flate2::read::ZlibDecoder::new(&resp.body[..]);
    dr.read_to_string(&mut buf).expect("unexpected error while reading the response body");

    let expected = "Hello World!";
    assert_eq!(expected, buf, "expected response with content");
}

#[test]
fn test_compression_handler_content_length() {
    struct Tc {
        name: &'static str,
        body: &'static str,
        status: u16,
    }
    for encoding in ["", GZIP_ENCODING, DEFLATE_ENCODING] {
        for tc in [
            Tc { name: "implicit status", body: "Hello World!", status: 0 },
            Tc { name: "explicit status", body: "Hello World!", status: 200 },
            Tc { name: "empty body", body: "", status: 0 },
            Tc { name: "no content", body: "", status: 204 },
        ] {
            let name = format!("{encoding}/{}", tc.name);
            let (body, status) = (tc.body, tc.status);
            let handler = move |_: http::Request| {
                // Go: the handler sets Content-Length, then the status (200 when not set), then the body.
                let mut r = http::Response::new(if status != 0 { status } else { 200 });
                r.set_header("Content-Length", &body.len().to_string());
                if !body.is_empty() {
                    r.body = http::Body::Bytes(body.as_bytes().to_vec());
                }
                r
            };
            let port = serve(Arc::new(CompressionHandler { handler: Arc::new(Func(handler)) }));

            let resp = request("GET", port, "/", &[(ACCEPT_ENCODING_HEADER, encoding)]).unwrap_or_else(|e| panic!("{name}: {e}"));
            if tc.status == 204 {
                assert_eq!(204, resp.status, "{name}");
                assert!(resp.body.is_empty(), "{name}");
                continue;
            }

            let mut got = Vec::new();
            match encoding {
                GZIP_ENCODING => {
                    flate2::read::GzDecoder::new(&resp.body[..]).read_to_end(&mut got).unwrap_or_else(|e| panic!("{name}: {e}"));
                }
                DEFLATE_ENCODING => {
                    flate2::read::ZlibDecoder::new(&resp.body[..]).read_to_end(&mut got).unwrap_or_else(|e| panic!("{name}: {e}"));
                }
                _ => {
                    assert_eq!(tc.body.len() as i64, resp.content_length(), "{name}");
                    got = resp.body.clone();
                }
            }
            assert_eq!(tc.body, String::from_utf8_lossy(&got), "{name}");
        }
    }
}

// ---- util/compression ----

const COMPRESSIBLE: &str = "ddddddddddsfpgjsdoadjgfpajdspfgjasfjapddddddddddaaaaaaaa
fsfsdfsfddddddddddsfpgjsdoadjgfpajdspfgjasfjapddddddddddaaaaaaaa
ddddddddddsfpgjsdoadjgfpajdspfgjasfjapddddddddddaaaaaaaa
ddddddddddsfpgjsdoadjgfpajdspfgjasfjapddddddddddaaaaaaaa
ddddddddddsfpgjsdoadjgfpajdspfgjasfjapddddddddddaaaaaaaa
ddddddddddsfpgjsdoadjgfpajdspfgjasfjapddddddddddaaaaaaaa
ddddddddddsfpgjsdoadjgfpajdspfgjasfjapddddddddddaaaaaaaa2
ddddddddddsfpgjsdoadjgfpajdspfgjasfjapddddddddddaaaaaaaa12
ddddddddddsfpgjsdoadjgfpajdspfgjasfjapddddddddddaaaaaaaa1
ddddddddddsfpgjsdoadjgfpajdspfgjasfjapddddddddddaaaaaaaa121
ddddddddddsfpgjsdoadjgfpajdspfgjasfjapddddddddddaaaaaaaa
ddddddddddsfpgjsdoadjgfpajdspfgjasfjapddddddddddaaaaaaaa
ddddddddddsfpgjsdoadjgfpajdspfgjasfjapddddddddddaaaaaaaa
ddddddddddsfpgjsdoadjgfpajdspfgjasfjapddddddddddaaaaaaaa
ddddddddddsfpgjsdoadjgfpajdspfgjasfjapddddddddddaaaaaaaa
ddddddddddsfpgjsdoadjgfpajdspfgjasfjapddddddddddaaaaaaaa324
ddddddddddsfpgjsdoadjgfpajdspfgjasfjapddddddddddaaaaaaaa145
";

#[derive(Clone, Copy)]
enum Bufs {
    None,
    Sync,
    Concurrent,
}

#[test]
fn test_encode_decode() {
    struct Case {
        name: &'static str,
        src: &'static str,
        types: Vec<&'static str>,
        bufs: Bufs,
        expect_compression: bool,
        expect_enc_err: Option<&'static str>,
    }
    let all = compression::types().to_vec();
    let cases = vec![
        Case { name: "empty src; no buffers", types: all.clone(), src: "", bufs: Bufs::None, expect_compression: false, expect_enc_err: None },
        Case { name: "empty src; sync buffers", types: all.clone(), src: "", bufs: Bufs::Sync, expect_compression: false, expect_enc_err: None },
        Case {
            name: "empty src; concurrent buffers",
            types: all.clone(),
            src: "",
            bufs: Bufs::Concurrent,
            expect_compression: false,
            expect_enc_err: None,
        },
        Case { name: "no buffers", types: vec![NONE], src: COMPRESSIBLE, bufs: Bufs::None, expect_compression: false, expect_enc_err: None },
        Case { name: "no buffers", types: vec![SNAPPY], src: COMPRESSIBLE, bufs: Bufs::None, expect_compression: true, expect_enc_err: None },
        Case {
            name: "no buffers",
            types: vec![ZSTD],
            src: COMPRESSIBLE,
            bufs: Bufs::None,
            expect_compression: false,
            expect_enc_err: Some("zstd requested but EncodeBuffer was not provided"),
        },
        Case { name: "sync buffers", types: vec![NONE], src: COMPRESSIBLE, bufs: Bufs::Sync, expect_compression: false, expect_enc_err: None },
        Case {
            name: "sync buffers",
            types: all[1..].to_vec(), // All but none
            src: COMPRESSIBLE,
            bufs: Bufs::Sync,
            expect_compression: true,
            expect_enc_err: None,
        },
        Case { name: "concurrent buffers", types: vec![NONE], src: COMPRESSIBLE, bufs: Bufs::Concurrent, expect_compression: false, expect_enc_err: None },
        Case {
            name: "concurrent buffers",
            types: all[1..].to_vec(), // All but none
            src: COMPRESSIBLE,
            bufs: Bufs::Concurrent,
            expect_compression: true,
            expect_enc_err: None,
        },
    ];
    for tcase in cases {
        assert!(!tcase.types.is_empty(), "must specify at least one type");
        // The buffers live across the types of one case, as in Go.
        let (mut se, mut sd) = (SyncEncodeBuffer::default(), SyncDecodeBuffer::default());
        let (mut ce, mut cd) = (ConcurrentEncodeBuffer, ConcurrentDecodeBuffer);
        for typ in &tcase.types {
            let name = format!("case={}/type={}", tcase.name, typ);
            let enc_buf: Option<&mut dyn EncodeBuffer> = match tcase.bufs {
                Bufs::None => None,
                Bufs::Sync => Some(&mut se),
                Bufs::Concurrent => Some(&mut ce),
            };
            let res = compression::encode(typ, tcase.src.as_bytes(), enc_buf);
            if let Some(want) = tcase.expect_enc_err {
                let err = res.expect_err(&name);
                assert!(err.contains(want), "{name}: {err}");
                continue;
            }
            let res = res.unwrap_or_else(|e| panic!("{name}: {e}")).into_owned();
            if tcase.expect_compression {
                assert!(res.len() < tcase.src.len(), "{name}");
            }

            // Decode back.
            let dec_buf: Option<&mut dyn DecodeBuffer> = match tcase.bufs {
                Bufs::None => None,
                Bufs::Sync => Some(&mut sd),
                Bufs::Concurrent => Some(&mut cd),
            };
            let got = compression::decode(typ, &res, dec_buf).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(tcase.src.as_bytes(), &got[..], "{name}");
        }
    }
}
