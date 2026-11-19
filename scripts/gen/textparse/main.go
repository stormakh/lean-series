// Command textparse generates the binary protobuf inputs of the ported textparse tests.
//
// It reads the Go test sources (go/ast), finds every text-format MetricFamily input and writes it as Go's
// metricFamiliesToProtobuf does (proto.UnmarshalText, proto.Marshal, uvarint length prefix) to
// tests-data/textparse/*.bin. See README.md for how to run it.
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"

	dto "github.com/prometheus/prometheus/prompb/io/prometheus/client"
)

const (
	upstreamDir = "../../../upstream"
	outDir      = "../../../tests-data/textparse"
)

func main() {
	// Each Go test file gets its own output directory; it is recreated on every run.
	for _, d := range []string{"protobufparse_test", "nhcbparse_test", "decoder_test"} {
		if err := os.RemoveAll(filepath.Join(outDir, d)); err != nil {
			fail("%v", err)
		}
		if err := os.MkdirAll(filepath.Join(outDir, d), 0o755); err != nil {
			fail("%v", err)
		}
	}

	for _, src := range []string{
		"model/textparse/protobufparse_test.go",
		"model/textparse/nhcbparse_test.go",
	} {
		genMetricFamiliesToProtobufCalls(src)
	}
	genDecoderTest("prompb/io/prometheus/client/decoder_test.go")
	genFuzzProtobufParserLabels("model/textparse/protobufparse_test.go")

	written, _ := filepath.Glob(filepath.Join(outDir, "*", "*"))
	sort.Strings(written)
	for _, f := range written {
		rel, _ := filepath.Rel(outDir, f)
		fmt.Println(rel)
	}
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "gen textparse: "+format+"\n", args...)
	os.Exit(1)
}

// metricFamiliesToProtobuf is the same as the Go test helper of the same name.
func metricFamiliesToProtobuf(testMetricFamilies []string) []byte {
	varintBuf := make([]byte, binary.MaxVarintLen32)
	buf := &bytes.Buffer{}
	for _, tmf := range testMetricFamilies {
		pb := &dto.MetricFamily{}
		if err := proto.UnmarshalText(tmf, pb); err != nil {
			fail("UnmarshalText: %v\n%s", err, tmf)
		}
		protoBuf, err := proto.Marshal(pb)
		if err != nil {
			fail("Marshal: %v", err)
		}
		varintLength := binary.PutUvarint(varintBuf, uint64(len(protoBuf)))
		buf.Write(varintBuf[:varintLength])
		buf.Write(protoBuf)
	}
	return buf.Bytes()
}

func stem(src string) string {
	return strings.TrimSuffix(filepath.Base(src), ".go")
}

func writeBin(name string, data []byte) {
	p := filepath.Join(outDir, name+".bin")
	if _, err := os.Stat(p); err == nil {
		fail("duplicate output name %s", name)
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		fail("%v", err)
	}
}

// sanitize turns a Go subtest name into a file name part.
func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r < 128 && (r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

func parseFile(src string) (*token.FileSet, *ast.File) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filepath.Join(upstreamDir, src), nil, parser.ParseComments)
	if err != nil {
		fail("%v", err)
	}
	return fset, f
}

// evalString evaluates a constant string expression: literals, concatenation and identifiers bound in fn or at
// file level.
func evalString(f *ast.File, fn *ast.FuncDecl, e ast.Expr) string {
	switch e := e.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			fail("not a string literal: %v", e.Value)
		}
		s, err := strconv.Unquote(e.Value)
		if err != nil {
			fail("%v", err)
		}
		return s
	case *ast.BinaryExpr:
		if e.Op != token.ADD {
			fail("unsupported string operator %v", e.Op)
		}
		return evalString(f, fn, e.X) + evalString(f, fn, e.Y)
	case *ast.ParenExpr:
		return evalString(f, fn, e.X)
	case *ast.Ident:
		return evalString(f, fn, lookupIdent(f, fn, e.Name))
	}
	fail("unsupported string expression %T", e)
	return ""
}

// lookupIdent finds the single value bound to name: an assignment or var in fn, else a file-level const or var.
func lookupIdent(f *ast.File, fn *ast.FuncDecl, name string) ast.Expr {
	var found []ast.Expr
	if fn != nil {
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.AssignStmt:
				for i, l := range n.Lhs {
					if id, ok := l.(*ast.Ident); ok && id.Name == name && len(n.Rhs) == len(n.Lhs) {
						found = append(found, n.Rhs[i])
					}
				}
			case *ast.ValueSpec:
				for i, id := range n.Names {
					if id.Name == name && i < len(n.Values) {
						found = append(found, n.Values[i])
					}
				}
			}
			return true
		})
	}
	if len(found) == 0 {
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, s := range gd.Specs {
				vs, ok := s.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, id := range vs.Names {
					if id.Name == name && i < len(vs.Values) {
						found = append(found, vs.Values[i])
					}
				}
			}
		}
	}
	if len(found) != 1 {
		fail("identifier %s: %d bindings", name, len(found))
	}
	return found[0]
}

// stringSlice evaluates a []string composite literal (or an identifier bound to one).
func stringSlice(f *ast.File, fn *ast.FuncDecl, e ast.Expr) []string {
	if id, ok := e.(*ast.Ident); ok {
		e = lookupIdent(f, fn, id.Name)
	}
	cl, ok := e.(*ast.CompositeLit)
	if !ok {
		fail("not a composite literal: %T", e)
	}
	var out []string
	for _, el := range cl.Elts {
		out = append(out, evalString(f, fn, el))
	}
	return out
}

// structField returns the value of field name in a keyed struct literal.
func structField(cl *ast.CompositeLit, name string) ast.Expr {
	for _, el := range cl.Elts {
		kv, ok := el.(*ast.KeyValueExpr)
		if !ok {
			continue
		}
		if id, ok := kv.Key.(*ast.Ident); ok && id.Name == name {
			return kv.Value
		}
	}
	return nil
}

// genMetricFamiliesToProtobufCalls writes one file for every metricFamiliesToProtobuf call in src, and one file
// per table row when the call takes a table field (`[]string{tc.input}`).
func genMetricFamiliesToProtobufCalls(src string) {
	fset, f := parseFile(src)
	calls := 0
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Name.Name == "metricFamiliesToProtobuf" {
			continue
		}
		var stack []ast.Node
		var inFn []string
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if n == nil {
				stack = stack[:len(stack)-1]
				return true
			}
			stack = append(stack, n)
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); !ok || id.Name != "metricFamiliesToProtobuf" {
				return true
			}
			calls++
			arg := call.Args[1]
			// Table-driven: []string{tc.input} inside `for _, tc := range []struct{...}{...}`.
			if cl, ok := arg.(*ast.CompositeLit); ok && len(cl.Elts) == 1 {
				if sel, ok := cl.Elts[0].(*ast.SelectorExpr); ok {
					rows := tableRows(f, fn, stack, sel)
					for _, r := range rows {
						name := stem(src) + "/" + fn.Name.Name + "__" + sanitize(r[0])
						writeBin(name, metricFamiliesToProtobuf([]string{r[1]}))
						inFn = append(inFn, name)
					}
					return true
				}
			}
			name := stem(src) + "/" + fn.Name.Name
			if len(inFn) > 0 {
				fail("%s: more than one metricFamiliesToProtobuf call in %s", fset.Position(call.Pos()), fn.Name.Name)
			}
			writeBin(name, metricFamiliesToProtobuf(stringSlice(f, fn, arg)))
			inFn = append(inFn, name)
			return true
		})
	}
	if calls == 0 {
		fail("%s: no metricFamiliesToProtobuf calls", src)
	}
}

// tableRows resolves `tc.<field>` to the rows of the range statement over a struct table that binds tc. Returns
// (name, value) per row.
func tableRows(f *ast.File, fn *ast.FuncDecl, stack []ast.Node, sel *ast.SelectorExpr) [][2]string {
	tc := sel.X.(*ast.Ident).Name
	for i := len(stack) - 1; i >= 0; i-- {
		rs, ok := stack[i].(*ast.RangeStmt)
		if !ok {
			continue
		}
		if v, ok := rs.Value.(*ast.Ident); !ok || v.Name != tc {
			continue
		}
		x := rs.X
		if id, ok := x.(*ast.Ident); ok {
			x = lookupIdent(f, fn, id.Name)
		}
		table := x.(*ast.CompositeLit)
		var rows [][2]string
		for _, el := range table.Elts {
			row := el.(*ast.CompositeLit)
			nameExpr := structField(row, "name")
			if nameExpr == nil {
				fail("table row without name")
			}
			rows = append(rows, [2]string{evalString(f, fn, nameExpr), evalString(f, fn, structField(row, sel.Sel.Name))})
		}
		return rows
	}
	fail("no range statement binds %s", tc)
	return nil
}

// genDecoderTest writes the inputs of decoder_test.go: the two families of TestMetricStreamingDecoder, and
// deterministic inputs for TestMetricStreamingDecoder_LabelsCorruption.
func genDecoderTest(src string) {
	_, f := parseFile(src)
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok {
			continue
		}
		switch fn.Name.Name {
		case "TestMetricStreamingDecoder":
			rs := firstRange(fn)
			writeBin(stem(src)+"/"+fn.Name.Name, metricFamiliesToProtobuf(stringSlice(f, fn, rs.X)))
		case "TestMetricStreamingDecoder_LabelsCorruption":
			// The Go test uses the unseeded global rand for names and values; use a fixed seed instead.
			r := rand.New(rand.NewSource(1))
			rs := firstRange(fn)
			for i, el := range rs.X.(*ast.CompositeLit).Elts {
				labelsCount, err := strconv.Atoi(el.(*ast.BasicLit).Value)
				if err != nil {
					fail("%v", err)
				}
				data := generateMetricFamilyText(r, labelsCount)
				writeBin(fmt.Sprintf("%s/%s__%d_labels%d", stem(src), fn.Name.Name, i, labelsCount), metricFamiliesToProtobuf([]string{data}))
			}
		}
	}
}

func firstRange(fn *ast.FuncDecl) *ast.RangeStmt {
	var rs *ast.RangeStmt
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		if r, ok := n.(*ast.RangeStmt); ok && rs == nil {
			rs = r
		}
		return rs == nil
	})
	if rs == nil {
		fail("%s: no range statement", fn.Name.Name)
	}
	return rs
}

// generateLabels and generateMetricFamilyText are the decoder_test.go helpers with r in place of the global rand.
func generateLabels(r *rand.Rand) string {
	randomName := fmt.Sprintf("instance_%d", r.Intn(1000))
	randomValue := fmt.Sprintf("value_%d", r.Intn(1000))
	return fmt.Sprintf(`label: <
    name: "%s"
    value: "%s"
  >`, randomName, randomValue)
}

func generateMetricFamilyText(r *rand.Rand, labelsCount int) string {
	randomName := fmt.Sprintf("metric_%d", r.Intn(1000))
	randomHelp := fmt.Sprintf("Test metric to demonstrate forced corruption %d.", r.Intn(1000))
	var labels10 strings.Builder
	for range labelsCount {
		labels10.WriteString(generateLabels(r))
	}
	return fmt.Sprintf(`name: "%s"
help: "%s"
type: GAUGE
metric: <
  %s
  gauge: <
    value: 1.0
  >
>
`, randomName, randomHelp, labels10.String())
}

// genFuzzProtobufParserLabels runs the seed corpus of FuzzProtobufParser_Labels: for each f.Add seed, the 20
// generated families (one file per iteration) and an index with the expected name, help and unit.
func genFuzzProtobufParserLabels(src string) {
	_, f := parseFile(src)
	checkFuzzGenCopy(f)
	var fn *ast.FuncDecl
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "FuzzProtobufParser_Labels" {
			fn = fd
		}
	}
	if fn == nil {
		fail("FuzzProtobufParser_Labels not found")
	}
	var index strings.Builder
	index.WriteString("# parse_classic_histogram\tenable_type_and_unit_labels\tseed\titeration\tfile\tname\thelp\tunit\n")
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Add" {
			return true
		}
		classic := call.Args[0].(*ast.Ident).Name
		typeAndUnit := call.Args[1].(*ast.Ident).Name
		seed := seedValue(call.Args[2])

		r := rand.New(rand.NewSource(seed))
		_ = 1 + r.Intn(128)    // pool.New min size
		_ = 128 + r.Intn(1024) // pool.New max size
		for i := range 20 {
			mf := generateFuzzMetricFamily(r)
			protoBuf, err := proto.Marshal(mf)
			if err != nil {
				fail("%v", err)
			}
			sizeBuf := make([]byte, binary.MaxVarintLen32)
			sizeBufSize := binary.PutUvarint(sizeBuf, uint64(len(protoBuf)))
			name := fmt.Sprintf("%s/%s__seed%d__%02d", stem(src), fn.Name.Name, seed, i)
			writeBin(name, append(sizeBuf[:sizeBufSize], protoBuf...))
			fmt.Fprintf(&index, "%s\t%s\t%d\t%d\t%s.bin\t%s\t%s\t%s\n", classic, typeAndUnit, seed, i, name, mf.Name, mf.Help, mf.Unit)
		}
		return true
	})
	p := filepath.Join(outDir, stem(src)+"/"+fn.Name.Name+".tsv")
	if err := os.WriteFile(p, []byte(index.String()), 0o644); err != nil {
		fail("%v", err)
	}
}

// seedValue evaluates int64(<int literal>) with an optional minus sign.
func seedValue(e ast.Expr) int64 {
	if c, ok := e.(*ast.CallExpr); ok {
		e = c.Args[0]
	}
	neg := false
	if u, ok := e.(*ast.UnaryExpr); ok && u.Op == token.SUB {
		neg = true
		e = u.X
	}
	v, err := strconv.ParseInt(e.(*ast.BasicLit).Value, 0, 64)
	if err != nil {
		fail("%v", err)
	}
	if neg {
		v = -v
	}
	return v
}
