// Command gen encodes fixed remote write / remote read messages with upstream Prometheus code (gogo protobuf
// and golang/snappy) and writes them to the directory given as the first argument. It also decodes the files
// the Rust side wrote there (rust_*.bin), re-encodes them and checks the bytes are the same as Go's own.
// crates/remote/tests/interop_test.rs builds the same messages in Rust (see the comments there).
package main

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/golang/snappy"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/prompb"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
)

func intHist() histogram.Histogram {
	return histogram.Histogram{
		CounterResetHint: histogram.NotCounterReset,
		Schema:           -2,
		Count:            1234567,
		Sum:              -98.25,
		ZeroThreshold:    0.001,
		ZeroCount:        3,
		PositiveSpans:    []histogram.Span{{Offset: -3, Length: 2}, {Offset: 5, Length: 1}},
		PositiveBuckets:  []int64{100, -50, 7},
		NegativeSpans:    []histogram.Span{{Offset: 0, Length: 1}},
		NegativeBuckets:  []int64{-2},
	}
}

func floatHist() histogram.FloatHistogram {
	return histogram.FloatHistogram{
		CounterResetHint: histogram.GaugeType,
		Schema:           histogram.CustomBucketsSchema,
		Count:            12.5,
		Sum:              1e300,
		PositiveSpans:    []histogram.Span{{Offset: 0, Length: 3}},
		PositiveBuckets:  []float64{1.5, 0, 11},
		CustomValues:     []float64{0.1, 1, 10},
	}
}

func v1Write() *prompb.WriteRequest {
	ih, fh := intHist(), floatHist()
	return &prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels: []prompb.Label{{Name: "__name__", Value: "http_requests_total"}, {Name: "code", Value: "200"}, {Name: "empty", Value: ""}, {Name: "utf8", Value: "Grüße ✓"}},
				Samples: []prompb.Sample{
					{Value: 1, Timestamp: 1700000000000},
					{Value: 0, Timestamp: 1700000015000},
					{Value: math.Copysign(0, -1), Timestamp: 1700000030000},
					{Value: math.Float64frombits(0x7ff0000000000002), Timestamp: 1700000045000}, // stale NaN
					{Value: math.Inf(-1), Timestamp: -5},
					{Value: 3.5e-310, Timestamp: 0},
				},
				Exemplars: []prompb.Exemplar{
					{Labels: []prompb.Label{{Name: "trace_id", Value: "abc123"}}, Value: 0.25, Timestamp: 1700000000001},
					{Value: -1},
				},
			},
			{
				Labels:     []prompb.Label{{Name: "__name__", Value: "latency"}},
				Histograms: []prompb.Histogram{prompb.FromIntHistogram(1700000000000, &ih), prompb.FromFloatHistogram(-1, &fh), {}},
			},
			{},
		},
		Metadata: []prompb.MetricMetadata{
			{Type: prompb.MetricMetadata_COUNTER, MetricFamilyName: "http_requests_total", Help: "Total requests.", Unit: "requests"},
			{Type: prompb.MetricMetadata_STATESET, MetricFamilyName: "x"},
			{},
		},
	}
}

func v2Write() *writev2.Request {
	ih, fh := intHist(), floatHist()
	st := writev2.NewSymbolTable()
	refs1 := []uint32{st.Symbolize("__name__"), st.Symbolize("http_requests_total"), st.Symbolize("code"), st.Symbolize("200")}
	help := st.Symbolize("Total requests.")
	unit := st.Symbolize("requests")
	refs2 := []uint32{st.Symbolize("__name__"), st.Symbolize("latency")}
	exRefs := []uint32{st.Symbolize("trace_id"), st.Symbolize("abc123")}
	big := make([]uint32, 0, 300)
	for i := uint32(0); i < 300; i++ {
		big = append(big, i*1000)
	}
	return &writev2.Request{
		Symbols: st.Symbols(),
		Timeseries: []writev2.TimeSeries{
			{
				LabelsRefs: refs1,
				Samples: []writev2.Sample{
					{Value: 1, Timestamp: 1700000000000, StartTimestamp: 1699999999000},
					{Value: math.Float64frombits(0x7ff0000000000002), Timestamp: 1700000015000},
					{Value: -7.25, Timestamp: -3, StartTimestamp: -9},
				},
				Exemplars: []writev2.Exemplar{{LabelsRefs: exRefs, Value: 0.25, Timestamp: 1700000000001}, {}},
				Metadata:  writev2.Metadata{Type: writev2.Metadata_METRIC_TYPE_COUNTER, HelpRef: help, UnitRef: unit},
			},
			{
				LabelsRefs: refs2,
				Histograms: []writev2.Histogram{writev2.FromIntHistogram(1699999999000, 1700000000000, &ih), writev2.FromFloatHistogram(0, -1, &fh)},
				Exemplars:  []writev2.Exemplar{{LabelsRefs: exRefs, Value: 3, Timestamp: 5}},
				Metadata:   writev2.Metadata{Type: writev2.Metadata_METRIC_TYPE_HISTOGRAM},
			},
			{LabelsRefs: big},
			{},
		},
	}
}

func readRequest() *prompb.ReadRequest {
	return &prompb.ReadRequest{
		Queries: []*prompb.Query{
			{
				StartTimestampMs: 1700000000000,
				EndTimestampMs:   1700000300000,
				Matchers: []*prompb.LabelMatcher{
					{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "up"},
					{Type: prompb.LabelMatcher_NRE, Name: "job", Value: "a|b"},
				},
				Hints: &prompb.ReadHints{StepMs: 15000, Func: "rate", StartMs: 1700000000000, EndMs: 1700000300000, Grouping: []string{"job", ""}, By: true, RangeMs: 300000},
			},
			{StartTimestampMs: -1, Hints: &prompb.ReadHints{}},
		},
		AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS, prompb.ReadRequest_SAMPLES},
	}
}

func readResponse() *prompb.ReadResponse {
	w := v1Write()
	return &prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			{Timeseries: []*prompb.TimeSeries{&w.Timeseries[0], &w.Timeseries[1]}},
			{},
			{Timeseries: []*prompb.TimeSeries{{Labels: []prompb.Label{{Name: "a", Value: "b"}}, Samples: []prompb.Sample{{Value: 42, Timestamp: 1}}}}},
		},
	}
}

func chunkedReadResponse() *prompb.ChunkedReadResponse {
	data := make([]byte, 200)
	for i := range data {
		data[i] = byte(i * 7)
	}
	return &prompb.ChunkedReadResponse{
		ChunkedSeries: []*prompb.ChunkedSeries{
			{
				Labels: []prompb.Label{{Name: "__name__", Value: "up"}, {Name: "job", Value: "node"}},
				Chunks: []prompb.Chunk{
					{MinTimeMs: 1700000000000, MaxTimeMs: 1700000100000, Type: prompb.Chunk_XOR, Data: data},
					{MinTimeMs: -10, MaxTimeMs: 0, Type: prompb.Chunk_FLOAT_HISTOGRAM_ST, Data: []byte{0}},
					{},
				},
			},
			{},
		},
		QueryIndex: 3,
	}
}

type marshaler interface{ Marshal() ([]byte, error) }

func must(b []byte, err error) []byte {
	if err != nil {
		panic(err)
	}
	return b
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: gen <out-dir>")
		os.Exit(2)
	}
	dir := os.Args[1]
	if err := os.MkdirAll(dir, 0o755); err != nil {
		panic(err)
	}
	v2 := v2Write()
	opt := must(v2.OptimizedMarshal(nil))
	files := map[string][]byte{
		"v1_write.bin":          must(v1Write().Marshal()),
		"v2_write.bin":          must(v2.Marshal()),
		"v2_write_opt.bin":      opt,
		"read_request.bin":      must(readRequest().Marshal()),
		"read_response.bin":     must(readResponse().Marshal()),
		"chunked_response.bin":  must(chunkedReadResponse().Marshal()),
		"v1_write.snappy":       snappy.Encode(nil, must(v1Write().Marshal())),
		"v2_write_opt.snappy":   snappy.Encode(nil, opt),
		"read_response.snappy":  snappy.Encode(nil, must(readResponse().Marshal())),
		"chunked_response.snappy": snappy.Encode(nil, must(chunkedReadResponse().Marshal())),
	}
	for name, b := range files {
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o644); err != nil {
			panic(err)
		}
	}

	// Decode what Rust wrote (if present), re-encode with Go and compare with Go's own bytes.
	type check struct {
		file string
		msg  interface {
			marshaler
			Unmarshal([]byte) error
		}
		want []byte
	}
	checks := []check{
		{"rust_v1_write.bin", &prompb.WriteRequest{}, files["v1_write.bin"]},
		{"rust_v2_write.bin", &writev2.Request{}, files["v2_write.bin"]},
		{"rust_read_request.bin", &prompb.ReadRequest{}, files["read_request.bin"]},
		{"rust_read_response.bin", &prompb.ReadResponse{}, files["read_response.bin"]},
		{"rust_chunked_response.bin", &prompb.ChunkedReadResponse{}, files["chunked_response.bin"]},
	}
	failed := false
	n := 0
	for _, c := range checks {
		b, err := os.ReadFile(filepath.Join(dir, c.file))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			panic(err)
		}
		if err := c.msg.Unmarshal(b); err != nil {
			fmt.Printf("FAIL go decode %s: %v\n", c.file, err)
			failed = true
			continue
		}
		again := must(c.msg.Marshal())
		if !bytes.Equal(again, c.want) {
			fmt.Printf("FAIL go re-encode of %s differs from Go's bytes\n", c.file)
			failed = true
			continue
		}
		n++
	}
	if sb, err := os.ReadFile(filepath.Join(dir, "rust_v1_write.snappy")); err == nil {
		d, err := snappy.Decode(nil, sb)
		if err != nil || !bytes.Equal(d, files["v1_write.bin"]) {
			fmt.Printf("FAIL go snappy decode of rust_v1_write.snappy: %v\n", err)
			failed = true
		} else {
			n++
		}
	}
	fmt.Printf("go: wrote %d files to %s, checked %d Rust files\n", len(files), dir, n)
	if failed {
		os.Exit(1)
	}
}
