//! Port of `model/textparse/interface_test.go`.

use textparse::{new, ParserKind, ParserOptions};

/// The scrape protocols of `config` used by the test.
#[derive(Clone, Copy, PartialEq)]
enum ScrapeProtocol {
    None,
    PrometheusProto,
    PrometheusText0_0_4,
    PrometheusText1_0_0,
    OpenMetricsText0_0_1,
    OpenMetricsText1_0_0,
}

// adapted: `config.ScrapeProtocol.HeaderMediaType` lives in the config package; this is the media type of each
// protocol's header in `config.ScrapeProtocolsHeaders`, parsed with mime.ParseMediaType as Go does.
fn header_media_type(p: ScrapeProtocol) -> String {
    let header = match p {
        ScrapeProtocol::None => return String::new(),
        ScrapeProtocol::PrometheusProto => {
            "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited"
        }
        ScrapeProtocol::PrometheusText0_0_4 => "text/plain;version=0.0.4",
        ScrapeProtocol::PrometheusText1_0_0 => "text/plain;version=1.0.0",
        ScrapeProtocol::OpenMetricsText0_0_1 => "application/openmetrics-text;version=0.0.1",
        ScrapeProtocol::OpenMetricsText1_0_0 => "application/openmetrics-text;version=1.0.0",
    };
    match textparse::mime::parse_media_type(header) {
        Ok((m, _)) => m,
        Err(_) => String::new(),
    }
}

struct Case {
    name: &'static str,
    content_type: &'static str,
    fallback_scrape_protocol: ScrapeProtocol,
    enable_open_metrics2: bool,
    convert_classic_histograms_to_nhcb: bool,
    /// None: the parser must be nil.
    want: Option<ParserKind>,
    err: &'static str,
}

const fn case(name: &'static str, content_type: &'static str, want: Option<ParserKind>, err: &'static str) -> Case {
    Case {
        name,
        content_type,
        fallback_scrape_protocol: ScrapeProtocol::None,
        enable_open_metrics2: false,
        convert_classic_histograms_to_nhcb: false,
        want,
        err,
    }
}

#[test]
fn test_new_parser() {
    use ParserKind::*;
    use ScrapeProtocol as SP;
    let with_fallback = |mut c: Case, p: ScrapeProtocol| {
        c.fallback_scrape_protocol = p;
        c
    };
    let with_om2 = |mut c: Case| {
        c.enable_open_metrics2 = true;
        c
    };
    let with_nhcb = |mut c: Case| {
        c.convert_classic_histograms_to_nhcb = true;
        c
    };
    let cases = vec![
        case(
            "empty-string",
            "",
            None,
            "non-compliant scrape target sending blank Content-Type and no fallback_scrape_protocol specified for target",
        ),
        with_fallback(
            case(
                "empty-string-fallback-text-plain",
                "",
                Some(Prom),
                "non-compliant scrape target sending blank Content-Type, using fallback_scrape_protocol \"text/plain\"",
            ),
            SP::PrometheusText0_0_4,
        ),
        case("invalid-content-type-1", "invalid/", None, "expected token after slash"),
        with_fallback(
            case("invalid-content-type-1-fallback-text-plain", "invalid/", Some(Prom), "expected token after slash"),
            SP::PrometheusText0_0_4,
        ),
        with_fallback(
            case(
                "invalid-content-type-1-fallback-openmetrics",
                "invalid/",
                Some(OpenMetrics),
                "expected token after slash",
            ),
            SP::OpenMetricsText0_0_1,
        ),
        with_fallback(
            case("invalid-content-type-1-fallback-protobuf", "invalid/", Some(Protobuf), "expected token after slash"),
            SP::PrometheusProto,
        ),
        case("invalid-content-type-2", "invalid/invalid/invalid", None, "unexpected content after media subtype"),
        with_fallback(
            case(
                "invalid-content-type-2-fallback-text-plain",
                "invalid/invalid/invalid",
                Some(Prom),
                "unexpected content after media subtype",
            ),
            SP::PrometheusText1_0_0,
        ),
        case("invalid-content-type-3", "/", None, "no media type"),
        with_fallback(
            case("invalid-content-type-3-fallback-text-plain", "/", Some(Prom), "no media type"),
            SP::PrometheusText1_0_0,
        ),
        case(
            "invalid-content-type-4",
            "application/openmetrics-text; charset=UTF-8; charset=utf-8",
            None,
            "duplicate parameter name",
        ),
        with_fallback(
            case(
                "invalid-content-type-4-fallback-open-metrics",
                "application/openmetrics-text; charset=UTF-8; charset=utf-8",
                Some(OpenMetrics),
                "duplicate parameter name",
            ),
            SP::OpenMetricsText1_0_0,
        ),
        case("openmetrics", "application/openmetrics-text", Some(OpenMetrics), ""),
        with_nhcb(case("openmetrics-with-nhcb", "application/openmetrics-text", Some(Nhcb), "")),
        case("openmetrics-with-charset", "application/openmetrics-text; charset=utf-8", Some(OpenMetrics), ""),
        case(
            "openmetrics-with-charset-and-version",
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
            Some(OpenMetrics),
            "",
        ),
        with_om2(case("openmetrics-v2", "application/openmetrics-text; version=2.0.0", Some(OpenMetrics2), "")),
        with_nhcb(with_om2(case(
            "openmetrics-v2-with-nhcb",
            "application/openmetrics-text; version=2.0.0",
            Some(OpenMetrics2),
            "",
        ))),
        with_om2(case(
            "openmetrics-v2-with-charset",
            "application/openmetrics-text; version=2.0.0; charset=utf-8",
            Some(OpenMetrics2),
            "",
        )),
        case(
            "openmetrics-v2-disabled",
            "application/openmetrics-text; version=2.0.0",
            None,
            "received OpenMetrics 2.0 Content-Type \"application/openmetrics-text; version=2.0.0\", but the openmetrics2 feature flag is not enabled and no fallback_scrape_protocol specified for target",
        ),
        with_fallback(
            case(
                "openmetrics-v2-disabled-fallback-openmetrics",
                "application/openmetrics-text; version=2.0.0",
                Some(OpenMetrics),
                "received OpenMetrics 2.0 Content-Type \"application/openmetrics-text; version=2.0.0\", but the openmetrics2 feature flag is not enabled, using fallback_scrape_protocol \"application/openmetrics-text\"",
            ),
            SP::OpenMetricsText1_0_0,
        ),
        case("plain-text", "text/plain", Some(Prom), ""),
        case("protobuf", "application/vnd.google.protobuf", Some(Protobuf), ""),
        case("plain-text-with-version", "text/plain; version=0.0.4", Some(Prom), ""),
        case(
            "some-other-valid-content-type",
            "text/html",
            None,
            "received unsupported Content-Type \"text/html\" and no fallback_scrape_protocol specified for target",
        ),
        with_fallback(
            case(
                "some-other-valid-content-type-fallback-text-plain",
                "text/html",
                Some(Prom),
                "received unsupported Content-Type \"text/html\", using fallback_scrape_protocol \"text/plain\"",
            ),
            SP::PrometheusText0_0_4,
        ),
    ];
    for tt in cases {
        let opts = ParserOptions {
            fallback_content_type: header_media_type(tt.fallback_scrape_protocol),
            enable_open_metrics2: tt.enable_open_metrics2,
            convert_classic_histograms_to_nhcb: tt.convert_classic_histograms_to_nhcb,
            ..Default::default()
        };
        let (p, err) = new(&[], tt.content_type, &opts);
        match tt.want {
            None => assert!(p.is_none(), "{}: parser must be nil", tt.name),
            Some(k) => {
                let p = p.unwrap_or_else(|| panic!("{}: parser must not be nil", tt.name));
                assert_eq!(p.kind(), k, "{}", tt.name);
            }
        }
        if tt.err.is_empty() {
            assert!(err.is_none(), "{}: unexpected error {:?}", tt.name, err);
        } else {
            let e = err.unwrap_or_else(|| panic!("{}: expected error", tt.name)).to_string();
            assert!(e.contains(tt.err), "{}: error {e:?} does not contain {:?}", tt.name, tt.err);
        }
    }
}
