// Dumps the expected values of the upstream promql/parser test tables to JSON fixtures for the Rust port.
//
// It reads the tables themselves (testExpr, tests, testSeries, and the local `inputs` tables of
// printer_test.go and prettier_test.go); it never runs the upstream parser. Run it with
// scripts/gen/promql_parser_fixtures.sh, which adds this file to the upstream package with `go test -overlay`.

package parser

import (
	"encoding/hex"
	"encoding/json"
	"go/ast"
	goparser "go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"unicode/utf8"

	"github.com/prometheus/prometheus/model/labels"
)

func dumpStr(s string) any {
	if utf8.ValidString(s) {
		return s
	}
	return map[string]any{"$bytes": hex.EncodeToString([]byte(s))}
}

func dumpFloat(f float64) string {
	if f == 0 {
		return "0" // reflect.DeepEqual treats -0 and 0 as equal.
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

var matcherType = reflect.TypeOf(labels.Matcher{})

// dumpValue converts an AST value to JSON-friendly data with reflection. Struct fields keep their Go names,
// unexported ones included. Nil slices are written as empty arrays (Rust has no nil slice).
func dumpValue(v reflect.Value) any {
	switch v.Kind() {
	case reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return dumpValue(v.Elem())
	case reflect.Pointer:
		if v.IsNil() {
			return nil
		}
		return dumpValue(v.Elem())
	case reflect.Struct:
		t := v.Type()
		if t == matcherType {
			return map[string]any{
				"Type":  int(v.FieldByName("Type").Int()),
				"Name":  dumpStr(v.FieldByName("Name").String()),
				"Value": dumpStr(v.FieldByName("Value").String()),
			}
		}
		out := map[string]any{"$t": t.Name()}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			fv := v.Field(i)
			if f.Name == "UnexpandedSeriesSet" || f.Name == "Series" {
				if !fv.IsNil() {
					panic("unexpected non-nil " + f.Name)
				}
				continue
			}
			out[f.Name] = dumpValue(fv)
		}
		return out
	case reflect.Slice:
		arr := make([]any, 0, v.Len())
		for i := 0; i < v.Len(); i++ {
			arr = append(arr, dumpValue(v.Index(i)))
		}
		return arr
	case reflect.String:
		return dumpStr(v.String())
	case reflect.Bool:
		return v.Bool()
	case reflect.Float64:
		return dumpFloat(v.Float())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint()
	}
	panic("unhandled kind " + v.Kind().String() + " of " + v.Type().String())
}

// dumpErrors writes Query as {"$input": true} when it equals the test input, to keep the fixture small.
func dumpErrors(errs ParseErrors, input string) []any {
	out := make([]any, 0, len(errs))
	for _, e := range errs {
		var query any = dumpStr(e.Query)
		if e.Query == input {
			query = map[string]any{"$input": true}
		}
		out = append(out, map[string]any{
			"Start":      int(e.PositionRange.Start),
			"End":        int(e.PositionRange.End),
			"Err":        dumpStr(e.Err.Error()),
			"Query":      query,
			"LineOffset": e.LineOffset,
		})
	}
	return out
}

func dumpLabels(ls labels.Labels) []any {
	out := []any{}
	ls.Range(func(l labels.Label) {
		out = append(out, []any{dumpStr(l.Name), dumpStr(l.Value)})
	})
	return out
}

func dumpSeq(vals []SequenceValue) any {
	if vals == nil {
		return nil
	}
	out := make([]any, 0, len(vals))
	for _, v := range vals {
		out = append(out, map[string]any{
			"Value":               dumpFloat(v.Value),
			"Omitted":             v.Omitted,
			"Histogram":           dumpValue(reflect.ValueOf(v.Histogram)),
			"CounterResetHintSet": v.CounterResetHintSet,
		})
	}
	return out
}

// localTables extracts every slice-of-struct composite literal with keyed string fields from the Test functions
// of a Go test file, keyed by the test function name.
func localTables(t *testing.T, file string) map[string][]map[string]any {
	fset := token.NewFileSet()
	f, err := goparser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var evalStr func(e ast.Expr) (string, bool)
	evalStr = func(e ast.Expr) (string, bool) {
		switch x := e.(type) {
		case *ast.BasicLit:
			if x.Kind != token.STRING {
				return "", false
			}
			s, err := strconv.Unquote(x.Value)
			if err != nil {
				t.Fatal(err)
			}
			return s, true
		case *ast.BinaryExpr:
			if x.Op != token.ADD {
				return "", false
			}
			a, ok1 := evalStr(x.X)
			b, ok2 := evalStr(x.Y)
			return a + b, ok1 && ok2
		case *ast.ParenExpr:
			return evalStr(x.X)
		}
		return "", false
	}
	out := map[string][]map[string]any{}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			cl, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			at, ok := cl.Type.(*ast.ArrayType)
			if !ok {
				return true
			}
			if _, ok := at.Elt.(*ast.StructType); !ok {
				return true
			}
			for _, el := range cl.Elts {
				ecl, ok := el.(*ast.CompositeLit)
				if !ok {
					continue
				}
				row := map[string]any{}
				for _, kv := range ecl.Elts {
					kve, ok := kv.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					k := kve.Key.(*ast.Ident).Name
					if s, ok := evalStr(kve.Value); ok {
						row[k] = dumpStr(s)
					}
				}
				out[fd.Name.Name] = append(out[fd.Name.Name], row)
			}
			return false
		})
	}
	return out
}

func writeJSON(t *testing.T, dir, name string, v any) {
	b, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestDumpRustFixtures(t *testing.T) {
	dir := os.Getenv("DUMP_DIR")
	if dir == "" {
		t.Skip("DUMP_DIR not set")
	}

	var exprs []any
	for _, tc := range testExpr {
		row := map[string]any{
			"input":   dumpStr(tc.input),
			"fail":    tc.fail,
			"printed": dumpStr(tc.printed),
		}
		if tc.fail {
			row["errors"] = dumpErrors(tc.errors, tc.input)
		} else {
			row["expected"] = dumpValue(reflect.ValueOf(tc.expected))
		}
		exprs = append(exprs, row)
	}
	writeJSON(t, dir, "parse_test_expr.json", exprs)

	var lex []any
	for _, group := range tests {
		var cases []any
		for _, tc := range group.tests {
			items := []any{}
			for _, it := range tc.expected {
				items = append(items, map[string]any{"Typ": int(it.Typ), "Pos": int(it.Pos), "Val": dumpStr(it.Val)})
			}
			cases = append(cases, map[string]any{
				"input":      dumpStr(tc.input),
				"expected":   items,
				"fail":       tc.fail,
				"seriesDesc": tc.seriesDesc,
			})
		}
		lex = append(lex, map[string]any{"name": group.name, "tests": cases})
	}
	writeJSON(t, dir, "lex_test.json", lex)

	var series []any
	for _, tc := range testSeries {
		row := map[string]any{"input": dumpStr(tc.input), "fail": tc.fail}
		if !tc.fail {
			row["expectedMetric"] = dumpLabels(tc.expectedMetric)
			row["expectedValues"] = dumpSeq(tc.expectedValues)
		}
		series = append(series, row)
	}
	writeJSON(t, dir, "parse_test_series.json", series)

	writeJSON(t, dir, "printer_test.json", localTables(t, "printer_test.go"))
	writeJSON(t, dir, "prettier_test.json", localTables(t, "prettier_test.go"))
}
