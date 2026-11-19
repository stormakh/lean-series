package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"math/rand"
	"strings"

	"github.com/gogo/protobuf/types"

	dto "github.com/prometheus/prometheus/prompb/io/prometheus/client"
)

// The declarations below are copied unchanged from upstream/model/textparse/protobufparse_test.go (they live in
// a _test.go file and cannot be imported). checkFuzzGenCopy fails the generator if they differ from upstream.

func generateFuzzMetricFamily(
	r *rand.Rand,
) *dto.MetricFamily {
	unit := generateValidLabelName(r)
	metricName := fmt.Sprintf("%s_%s", generateValidMetricName(r), unit)
	metricTypeProto := dto.MetricType(r.Intn(len(dto.MetricType_name)))
	metricFamily := &dto.MetricFamily{
		Name: metricName,
		Help: generateHelp(r),
		Type: metricTypeProto,
		Unit: unit,
	}
	metricsCount := r.Intn(20)
	for range metricsCount {
		metric := dto.Metric{
			Label: generateFuzzLabels(r),
		}
		switch metricTypeProto {
		case dto.MetricType_GAUGE:
			metric.Gauge = &dto.Gauge{Value: r.Float64()}
		case dto.MetricType_COUNTER:
			metric.Counter = &dto.Counter{Value: r.Float64()}
		case dto.MetricType_SUMMARY:
			metric.Summary = &dto.Summary{Quantile: []dto.Quantile{{Quantile: 0.5, Value: r.Float64()}}}
		case dto.MetricType_HISTOGRAM:
			metric.Histogram = &dto.Histogram{Exemplars: generateExemplars(r)}
		}
		metricFamily.Metric = append(metricFamily.Metric, metric)
	}
	return metricFamily
}

func generateExemplars(r *rand.Rand) []*dto.Exemplar {
	exemplarsCount := r.Intn(5)
	exemplars := make([]*dto.Exemplar, 0, exemplarsCount)
	for range exemplarsCount {
		exemplars = append(exemplars, &dto.Exemplar{
			Label: generateFuzzLabels(r),
			Value: r.Float64(),
			Timestamp: &types.Timestamp{
				Seconds: int64(r.Intn(1000000000)),
				Nanos:   int32(r.Intn(1000000000)),
			},
		})
	}
	return exemplars
}

func generateFuzzLabels(r *rand.Rand) []dto.LabelPair {
	labelsCount := r.Intn(10)
	ls := make([]dto.LabelPair, 0, labelsCount)
	for range labelsCount {
		ls = append(ls, dto.LabelPair{
			Name:  generateValidLabelName(r),
			Value: generateValidLabelName(r),
		})
	}
	return ls
}

func generateHelp(r *rand.Rand) string {
	result := make([]string, 1+r.Intn(20))
	for i := range result {
		result[i] = generateValidLabelName(r)
	}
	return strings.Join(result, "_")
}

func generateValidLabelName(r *rand.Rand) string {
	return generateString(r, validFirstRunes, validLabelNameRunes)
}

func generateValidMetricName(r *rand.Rand) string {
	return generateString(r, validFirstRunes, validMetricNameRunes)
}

func generateString(r *rand.Rand, firstRunes, restRunes []rune) string {
	result := make([]rune, 1+r.Intn(20))
	for i := range result {
		if i == 0 {
			result[i] = firstRunes[r.Intn(len(firstRunes))]
		} else {
			result[i] = restRunes[r.Intn(len(restRunes))]
		}
	}
	return string(result)
}

var (
	validMetricNameRunes = []rune{
		'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
		'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
		'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
		'_', ':',
	}
	validLabelNameRunes = validMetricNameRunes[:len(validMetricNameRunes)-1] // skip the colon
	validFirstRunes     = validMetricNameRunes[:52]                          // only the letters
)

// checkFuzzGenCopy compares the declarations of this file with the same declarations in upstream (f).
func checkFuzzGenCopy(upstream *ast.File) {
	fset := token.NewFileSet()
	own, err := parser.ParseFile(fset, "fuzzgen.go", nil, parser.ParseComments)
	if err != nil {
		fail("%v", err)
	}
	print := func(n ast.Node) string {
		var b bytes.Buffer
		if err := format.Node(&b, token.NewFileSet(), n); err != nil {
			fail("%v", err)
		}
		return b.String()
	}
	decls := map[string]string{}
	key := func(d ast.Decl) string {
		switch d := d.(type) {
		case *ast.FuncDecl:
			return d.Name.Name
		case *ast.GenDecl:
			if d.Tok == token.VAR && len(d.Specs) > 0 {
				return d.Specs[0].(*ast.ValueSpec).Names[0].Name
			}
		}
		return ""
	}
	for _, d := range upstream.Decls {
		decls[key(d)] = print(d)
	}
	n := 0
	for _, d := range own.Decls {
		k := key(d)
		if k == "" || k == "checkFuzzGenCopy" {
			continue
		}
		if decls[k] != print(d) {
			fail("fuzzgen.go: %s differs from upstream protobufparse_test.go", k)
		}
		n++
	}
	if n != 8 {
		fail("fuzzgen.go: checked %d declarations, want 8", n)
	}
}
