//! Port of `web/api/v1/json_codec_test.go`.

use std::sync::Arc;

use model::exemplar::{Exemplar, QueryResult};
use model::histogram::{FloatHistogram, Span};
use model::labels::from_strings;
use promql::parser::ValueType;
use promql::{FPoint, HPoint, Sample, Series, Value};
use web::api::{Codec, Data, JsonCodec, QueryData, Response};

fn qd(t: ValueType, v: Value) -> Data {
    Data::Query(QueryData { result_type: t, result: v, stats: None, cost: None })
}

fn fp(f: f64, t: i64) -> Data {
    Data::FPoint(FPoint { t, f })
}

#[test]
fn test_json_codec_encode() {
    let h = FloatHistogram {
        schema: 2,
        zero_threshold: 0.001,
        zero_count: 12.0,
        count: 10.0,
        sum: 20.0,
        positive_spans: vec![Span { offset: 3, length: 2 }, Span { offset: 1, length: 3 }],
        negative_spans: vec![Span { offset: 2, length: 2 }],
        positive_buckets: vec![1.0, 2.0, 2.0, 1.0, 1.0],
        negative_buckets: vec![2.0, 1.0],
        ..Default::default()
    };
    let cases: Vec<(Data, &str)> = vec![
        (
            qd(
                ValueType::Vector,
                Value::Vector(vec![
                    Sample { metric: from_strings(&["__name__", "foo"]), t: 1000, f: 1.0, ..Default::default() },
                    Sample { metric: from_strings(&["__name__", "bar"]), t: 2000, f: 2.0, ..Default::default() },
                ]),
            ),
            r#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"foo"},"value":[1,"1"]},{"metric":{"__name__":"bar"},"value":[2,"2"]}]}}"#,
        ),
        (
            qd(
                ValueType::Matrix,
                Value::Matrix(vec![
                    Series { metric: from_strings(&["__name__", "foo"]), floats: vec![FPoint { f: 1.0, t: 1000 }], ..Default::default() },
                    Series { metric: from_strings(&["__name__", "bar"]), floats: vec![FPoint { f: 2.0, t: 2000 }], ..Default::default() },
                ]),
            ),
            r#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"foo"},"values":[[1,"1"]]},{"metric":{"__name__":"bar"},"values":[[2,"2"]]}]}}"#,
        ),
        (
            qd(
                ValueType::Matrix,
                Value::Matrix(vec![Series { floats: vec![FPoint { f: 1.0, t: 1000 }], metric: from_strings(&["__name__", "foo"]), ..Default::default() }]),
            ),
            r#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"foo"},"values":[[1,"1"]]}]}}"#,
        ),
        (
            qd(
                ValueType::Matrix,
                Value::Matrix(vec![Series {
                    histograms: vec![HPoint { h: Arc::new(h), t: 1000 }],
                    metric: from_strings(&["__name__", "foo"]),
                    ..Default::default()
                }]),
            ),
            r#"{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"foo"},"histograms":[[1,{"count":"10","sum":"20","buckets":[[1,"-1.6817928305074288","-1.414213562373095","1"],[1,"-1.414213562373095","-1.189207115002721","2"],[3,"-0.001","0.001","12"],[0,"1.414213562373095","1.6817928305074288","1"],[0,"1.6817928305074288","2","2"],[0,"2.378414230005442","2.82842712474619","2"],[0,"2.82842712474619","3.3635856610148576","1"],[0,"3.3635856610148576","4","1"]]}]]}]}}"#,
        ),
        (fp(0.0, 0), r#"{"status":"success","data":[0,"0"]}"#),
        (fp(20.0, 1), r#"{"status":"success","data":[0.001,"20"]}"#),
        (fp(20.0, 10), r#"{"status":"success","data":[0.010,"20"]}"#),
        (fp(20.0, 100), r#"{"status":"success","data":[0.100,"20"]}"#),
        (fp(20.0, 1001), r#"{"status":"success","data":[1.001,"20"]}"#),
        (fp(20.0, 1010), r#"{"status":"success","data":[1.010,"20"]}"#),
        (fp(20.0, 1100), r#"{"status":"success","data":[1.100,"20"]}"#),
        (fp(20.0, 12345678123456555), r#"{"status":"success","data":[12345678123456.555,"20"]}"#),
        (fp(20.0, -1), r#"{"status":"success","data":[-0.001,"20"]}"#),
        (fp(f64::NAN, 0), r#"{"status":"success","data":[0,"NaN"]}"#),
        (fp(f64::INFINITY, 0), r#"{"status":"success","data":[0,"+Inf"]}"#),
        (fp(f64::NEG_INFINITY, 0), r#"{"status":"success","data":[0,"-Inf"]}"#),
        (fp(1.2345678e6, 0), r#"{"status":"success","data":[0,"1234567.8"]}"#),
        (fp(1.2345678e-6, 0), r#"{"status":"success","data":[0,"0.0000012345678"]}"#),
        (fp(1.2345678e-67, 0), r#"{"status":"success","data":[0,"1.2345678e-67"]}"#),
        (
            Data::Exemplars(vec![QueryResult {
                series_labels: from_strings(&["foo", "bar"]),
                exemplars: vec![Exemplar { labels: from_strings(&["trace_id", "abc"]), value: 100.123, ts: 1234, has_ts: false }],
            }]),
            r#"{"status":"success","data":[{"seriesLabels":{"foo":"bar"},"exemplars":[{"labels":{"trace_id":"abc"},"value":"100.123","timestamp":1.234}]}]}"#,
        ),
        (
            Data::Exemplars(vec![QueryResult {
                series_labels: from_strings(&["foo", "bar"]),
                exemplars: vec![Exemplar { labels: from_strings(&["trace_id", "abc"]), value: f64::INFINITY, ts: 1234, has_ts: false }],
            }]),
            r#"{"status":"success","data":[{"seriesLabels":{"foo":"bar"},"exemplars":[{"labels":{"trace_id":"abc"},"value":"+Inf","timestamp":1.234}]}]}"#,
        ),
    ];
    let codec = JsonCodec;
    for (data, expected) in cases {
        let body = codec.encode(&Response { status: "success".to_string(), data: Some(data), ..Default::default() }).unwrap();
        assert_eq!(expected, String::from_utf8(body).unwrap());
    }
}
