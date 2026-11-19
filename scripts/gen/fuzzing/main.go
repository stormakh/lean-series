// Writes the seed corpus of upstream FuzzParseProtobuf (util/fuzzing.GetCorpusForFuzzParseProtobuf) to
// tests-data/fuzzing/protobuf_corpus.bin, for crates/util/tests/fuzz_test.rs.
//
// Format, per seed: u32 little-endian length, the payload bytes, then 4 bytes (0 or 1): ignoreNative,
// parseClassic, convertNHCB, typeAndUnit.
//
// Rerun from this directory with the Go env of tools/goenv.sh (offline: GOFLAGS=-mod=mod GOPROXY=off): go run .
package main

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/prometheus/prometheus/util/fuzzing"
)

func b2u(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func main() {
	corpus, err := fuzzing.GetCorpusForFuzzParseProtobuf()
	if err != nil {
		panic(err)
	}
	var out []byte
	for _, s := range corpus {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(s.Data)))
		out = append(out, s.Data...)
		out = append(out, b2u(s.IgnoreNative), b2u(s.ParseClassic), b2u(s.ConvertNHCB), b2u(s.TypeAndUnit))
	}
	if err := os.WriteFile("../../../tests-data/fuzzing/protobuf_corpus.bin", out, 0o644); err != nil {
		panic(err)
	}
	fmt.Printf("%d seeds, %d bytes\n", len(corpus), len(out))
}
