#!/usr/bin/env python3
"""Regenerates tests-data/promql/engine/preprocess_expr.json from the expected ASTs of upstream TestPreprocessExpr
("wrap step-invariant expressions"). The table literal is copied from upstream/promql/engine_test.go into a Go test
file that is added to the upstream package with `go test -overlay`; it dumps the values with reflection in the
same JSON form as scripts/gen/promql_parser_dump_test.go. Needs the Go toolchain (tools/goenv.sh)."""
import os, re, subprocess, tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
UP = os.path.realpath(os.path.join(ROOT, 'upstream'))
src = open(os.path.join(UP, 'promql/engine_test.go')).read()
start = src.index('t.Run("wrap step-invariant expressions"')
lit_start = src.index('testCases := []struct {', start)
lit_end = src.index('\t\tfor _, test := range testCases {', lit_start)
literal = src[lit_start:lit_end]

dump = open(os.path.join(ROOT, 'scripts/gen/promql_parser_dump_test.go')).read()
fn_start = dump.index('func dumpStr(')
fn_end = dump.index('// dumpErrors')
helpers = dump[fn_start:fn_end]

go = '''package promql_test

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
)

var _ = time.Second

''' + helpers + '''
func TestDumpRustEngineFixtures(t *testing.T) {
	startTime := time.Unix(1000, 0)
	endTime := time.Unix(9999, 0)
''' + literal + '''
	out := []any{}
	for _, tc := range testCases {
		out = append(out, map[string]any{"input": tc.input, "outputTest": tc.outputTest, "expected": dumpValue(reflect.ValueOf(tc.expected))})
	}
	b, err := json.MarshalIndent(out, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("DUMP_FILE"), b, 0o644); err != nil {
		t.Fatal(err)
	}
}
'''
tmp = tempfile.mkdtemp(dir=os.environ.get('GOTMPDIR'))
gofile = os.path.join(tmp, 'zz_rust_engine_dump_test.go')
open(gofile, 'w').write(go)
overlay = os.path.join(tmp, 'overlay.json')
open(overlay, 'w').write('{"Replace":{"%s":"%s"}}' % (os.path.join(UP, 'promql/zz_rust_engine_dump_test.go'), gofile))
os.makedirs(os.path.join(ROOT, 'tests-data/promql/engine'), exist_ok=True)
env = dict(os.environ, DUMP_FILE=os.path.join(ROOT, 'tests-data/promql/engine/preprocess_expr.json'))
subprocess.run(['go', 'test', '-count=1', '-overlay', overlay, '-run', '^TestDumpRustEngineFixtures$', './promql/'], cwd=UP, env=env, check=True)
