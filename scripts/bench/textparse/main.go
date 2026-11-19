// Command textparse runs the loop of upstream's benchParse (model/textparse/benchmark_test.go) on the same
// testdata files, in two modes: "full" (as upstream: Labels, StartTimestamp and Exemplar for every sample) and
// "scrape" (only Next and Series/Histogram, like a scrape whose series are all cached). It is the Go side of
// crates/textparse/examples/bench_parse.rs.
//
// Run from this directory with the Go env of tools/goenv.sh (offline: GOFLAGS=-mod=mod GOPROXY=off): `go run .`
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/textparse"
)

func newParser(kind string, b []byte, st *labels.SymbolTable) textparse.Parser {
	switch kind {
	case "promtext":
		return textparse.NewPromParser(b, st, false)
	case "omtext":
		return textparse.NewOpenMetricsParser(b, st, textparse.WithOMParserSTSeriesSkipped())
	case "omtext_with_nhcb":
		return textparse.NewNHCBParser(textparse.NewOpenMetricsParser(b, st, textparse.WithOMParserSTSeriesSkipped()), st, false, false)
	case "omtext_with_nhcb_st":
		return textparse.NewNHCBParser(textparse.NewOpenMetricsParser(b, st, textparse.WithOMParserSTSeriesSkipped()), st, false, true)
	case "om2text":
		return textparse.NewOpenMetrics2Parser(b, st, textparse.ParserOptions{})
	case "om2text_with_nhcb":
		return textparse.NewOpenMetrics2Parser(b, st, textparse.ParserOptions{ConvertClassicHistogramsToNHCB: true})
	case "promproto":
		return textparse.NewProtobufParser(b, false, true, false, false, st)
	}
	panic(kind)
}

func run(kind string, data []byte, full bool) testing.BenchmarkResult {
	return testing.Benchmark(func(b *testing.B) {
		var (
			res labels.Labels
			e   exemplar.Exemplar
		)
		b.SetBytes(int64(len(data)))
		b.ReportAllocs()
		st := labels.NewSymbolTable()
		for b.Loop() {
			p := newParser(kind, data, st)
		Inner:
			for {
				t, err := p.Next()
				switch t {
				case textparse.EntryInvalid:
					if errors.Is(err, io.EOF) {
						break Inner
					}
					b.Fatal(err)
				case textparse.EntryType:
					_, _ = p.Type()
					continue
				case textparse.EntryHelp:
					_, _ = p.Help()
					continue
				case textparse.EntryUnit:
					_, _ = p.Unit()
					continue
				case textparse.EntryComment:
					continue
				case textparse.EntryHistogram:
					_, _, _, _ = p.Histogram()
				case textparse.EntrySeries:
					_, _, _ = p.Series()
				}
				if !full {
					continue
				}
				p.Labels(&res)
				_ = p.StartTimestamp()
				for hasExemplar := p.Exemplar(&e); hasExemplar; hasExemplar = p.Exemplar(&e) {
				}
			}
		}
	})
}

func main() {
	dir := "../../../upstream/model/textparse/testdata"
	cases := []struct{ name, kind, file string }{
		{"ParsePromText/parser=promtext", "promtext", "alltypes.237mfs.prom.txt"},
		{"ParsePromText/parser=omtext", "omtext", "alltypes.237mfs.prom.txt"},
		{"ParsePromText_NoMeta/parser=promtext", "promtext", "alltypes.237mfs.nometa.prom.txt"},
		{"ParseOMText", "omtext", "alltypes.5mfs.om.txt"},
		{"ParseOM2Text", "om2text", "alltypes.om2.txt"},
		{"ParseOM1VsOM2_AllTypes/parser=omtext", "omtext", "alltypes.bench.om.txt"},
		{"ParseOM1VsOM2_AllTypes/parser=omtext_with_nhcb", "omtext_with_nhcb", "alltypes.bench.om.txt"},
		{"ParseOM1VsOM2_AllTypes/parser=om2text", "om2text", "alltypes.bench.om2.txt"},
		{"ParseOM1VsOM2_AllTypes/parser=om2text_with_nhcb", "om2text_with_nhcb", "alltypes.bench.om2.txt"},
		{"ParseOM1VsOM2_CT/parser=omtext", "omtext", "ct.bench.om.txt"},
		{"ParseOM1VsOM2_CT/parser=om2text", "om2text", "ct.bench.om2.txt"},
		{"ParseOM1VsOM2_Histograms/parser=omtext", "omtext", "histograms.bench.om.txt"},
		{"ParseOM1VsOM2_Histograms/parser=omtext_with_nhcb", "omtext_with_nhcb", "histograms.bench.om.txt"},
		{"ParseOM1VsOM2_Histograms/parser=om2text", "om2text", "histograms.bench.om2.txt"},
		{"ParseOM1VsOM2_Histograms/parser=om2text_with_nhcb", "om2text_with_nhcb", "histograms.bench.om2.txt"},
		{"ParseOpenMetricsNHCB/parser=omtext", "omtext", "1histogram.om.txt"},
		{"ParseOpenMetricsNHCB/parser=omtext_with_nhcb", "omtext_with_nhcb", "1histogram.om.txt"},
		{"ParseOpenMetricsNHCB/parser=omtext_with_nhcb_st", "omtext_with_nhcb_st", "1histogram.om.txt"},
		{"ParsePromProto/parser=promproto", "promproto", "../../../../tests-data/textparse/protobufparse_test/createTestProtoBuf.bin"},
	}
	full := os.Getenv("BENCH_SCRAPE") != "1"
	filter := ""
	if len(os.Args) > 1 {
		filter = os.Args[1]
	}
	for _, c := range cases {
		if filter != "" && !contains(c.name, filter) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, c.file))
		if err != nil {
			fmt.Println(c.name, err)
			continue
		}
		r := run(c.kind, data, full)
		fmt.Printf("%-58s %8d %12d ns/op %9.2f MB/s %9d B/op %7d allocs/op\n", c.name, r.N, r.NsPerOp(),
			float64(len(data))/float64(r.NsPerOp())*1e3, r.AllocedBytesPerOp(), r.AllocsPerOp())
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
