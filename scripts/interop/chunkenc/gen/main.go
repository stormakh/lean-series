// Command gen writes chunks encoded by upstream Prometheus (tsdb/chunkenc) for the Rust byte compatibility check
// (crates/chunkenc/tests/interop_test.rs).
//
// Usage: gen <out dir>
//
// For every sample sequence and every encoder it writes one text file <out dir>/<encoder>-<sequence>.txt:
//
//	encoding <encoding byte>
//	resume <index>                 (0: one appender; k > 0: at sample k the chunk is reset to a copy of its bytes
//	                                and a new appender continues, as when a chunk is loaded from disk)
//	bytes <chunk bytes, lower-case hex>
//	sample <st> <t> <float64 bits, hex>      (one line per input sample, in append order)
//	decoded <st> <t> <float64 bits, hex>     (one line per sample decoded by Go's iterator from the bytes)
//
// To add an encoding, add one function with the encodeFunc signature and one entry in encoders. The histogram
// encodings are in hist.go (their manifest format is described there).
package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"

	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type sample struct {
	st, t int64
	v     float64
}

type sequence struct {
	name    string
	samples []sample
	// resumeAt is the sample index at which appending continues on a chunk reloaded from bytes (0: never).
	resumeAt int
}

// encodeFunc encodes the samples as one chunk and returns it.
type encodeFunc func(seq sequence) (chunkenc.Chunk, error)

type encoder struct {
	name string
	fn   encodeFunc
}

var encoders = []encoder{
	{"xor", encodeXOR},
	{"xor2", encodeXOR2},
}

// encodeFloats appends float samples to a new chunk of encoding e, resuming from the bytes at seq.resumeAt.
func encodeFloats(e chunkenc.Encoding, seq sequence) (chunkenc.Chunk, error) {
	c, err := chunkenc.NewEmptyChunk(e)
	if err != nil {
		return nil, err
	}
	app, err := c.Appender()
	if err != nil {
		return nil, err
	}
	for i, s := range seq.samples {
		if seq.resumeAt > 0 && i == seq.resumeAt {
			c.Reset(append([]byte(nil), c.Bytes()...))
			if app, err = c.Appender(); err != nil {
				return nil, err
			}
		}
		app.Append(s.st, s.t, s.v)
	}
	return c, nil
}

func encodeXOR(seq sequence) (chunkenc.Chunk, error) {
	return encodeFloats(chunkenc.EncXOR, seq)
}

func encodeXOR2(seq sequence) (chunkenc.Chunk, error) {
	return encodeFloats(chunkenc.EncXOR2, seq)
}

func f(bits uint64) float64 { return math.Float64frombits(bits) }

func floats(name string, ts []int64, vs []float64) sequence {
	s := sequence{name: name}
	for i := range ts {
		s.samples = append(s.samples, sample{t: ts[i], v: vs[i]})
	}
	return s
}

func regular(n int, start, step int64, v func(i int) float64) ([]int64, []float64) {
	ts := make([]int64, n)
	vs := make([]float64, n)
	for i := range n {
		ts[i] = start + int64(i)*step
		vs[i] = v(i)
	}
	return ts, vs
}

func withST(s sequence, name string, st func(i int, t int64) int64) sequence {
	out := sequence{name: name, resumeAt: s.resumeAt}
	for i, x := range s.samples {
		out.samples = append(out.samples, sample{st: st(i, x.t), t: x.t, v: x.v})
	}
	return out
}

func resumed(s sequence, name string, at int) sequence {
	out := s
	out.name = name
	out.resumeAt = at
	return out
}

func sequences() []sequence {
	var seqs []sequence
	const t0 = int64(1700000000000)

	seqs = append(seqs, sequence{name: "empty"})
	seqs = append(seqs, floats("one", []int64{t0}, []float64{42.5}))
	seqs = append(seqs, floats("two", []int64{t0, t0 + 15000}, []float64{1, 2}))

	ts, vs := regular(120, t0, 15000, func(int) float64 { return 1e9 })
	constant := floats("constant", ts, vs)
	seqs = append(seqs, constant)

	ts, vs = regular(120, t0, 15000, func(i int) float64 { return 1e9 + float64(i) })
	counter := floats("counter", ts, vs)
	seqs = append(seqs, counter)

	ts, vs = regular(120, t0, 15000, func(i int) float64 { return float64(i % 3) })
	seqs = append(seqs, floats("repeated", ts, vs))

	r := rand.New(rand.NewPCG(7, 11))
	ts, vs = regular(240, t0, 15000, func(int) float64 { return r.Float64() * 1000 })
	for i := range ts {
		ts[i] += int64(r.IntN(201) - 100)
	}
	jitter := floats("irregular-random", ts, vs)
	seqs = append(seqs, jitter)

	ts, vs = regular(120, t0, 1000, func(i int) float64 { return float64(i) + float64(i)/10 + float64(i)/100 })
	seqs = append(seqs, floats("fractional", ts, vs))

	// Large and negative dod values: every dod bucket of XOR (14/17/20/64 bits) and XOR2 (13/20/64 bits).
	largeTs := []int64{
		0, 1000, 2000, 2000 + 1<<20, 2000 + 1<<21, 3000 + 1<<21, 3000 + 1<<40, 4000 + 1<<40,
		4000 + 1<<40 + 4095, 4000 + 1<<40 + 8191, 4000 + 1<<40 + 8191 - 4096, 1 << 50, 1<<50 + 1,
		1<<50 + 2 + 524287, 1<<50 + 3 + 2*524287, 1<<50 + 3 + 2*524287 + 70000, 1<<50 + 3 + 2*524287 + 140001,
		1<<50 + 3 + 2*524287 + 140001 + 8192, 1<<50 + 3 + 2*524287 + 140001 + 8192 + 8193,
	}
	largeVs := make([]float64, len(largeTs))
	for i := range largeVs {
		largeVs[i] = float64(i * i)
	}
	seqs = append(seqs, floats("large-dod", largeTs, largeVs))

	// Negative and extreme timestamps.
	seqs = append(seqs, floats("extreme-ts",
		[]int64{math.MinInt64 / 4, math.MinInt64/4 + 1, 0, 5, math.MaxInt64 / 4, math.MaxInt64/4 + 15000},
		[]float64{1, 2, 3, 4, 5, 6}))

	// Special floats: NaN payloads, stale NaN, infinities, signed zero, subnormals, extremes.
	special := []float64{
		f(value.StaleNaN), 1, f(value.StaleNaN), f(value.StaleNaN), 2, math.NaN(), f(0x7ff8000000000012),
		math.Inf(1), math.Inf(-1), math.Copysign(0, -1), 0, math.Copysign(0, -1), math.SmallestNonzeroFloat64,
		math.MaxFloat64, -math.MaxFloat64, f(1), f(0xfff0000000000001), 1, 1, f(value.StaleNaN), 1,
		-1e300, 1e-300, 3, 3, 3, f(value.StaleNaN),
	}
	ts, _ = regular(len(special), t0, 15000, func(int) float64 { return 0 })
	specials := floats("special-floats", ts, special)
	seqs = append(seqs, specials)
	// Stale NaN with dod != 0.
	ts2 := append([]int64(nil), ts...)
	for i := range ts2 {
		ts2[i] += int64(i * i * 7)
	}
	seqs = append(seqs, floats("special-floats-jitter", ts2, special))

	// Shifted XOR windows (window reuse, widening, union).
	ts, vs = regular(120, t0, 15000, func(i int) float64 {
		deltas := [...]uint64{0, 0xff << 27, 0xff<<27 ^ 0xff<<32, 0xff << 32, 1<<63 | 1<<5, 1<<58 | 1}
		return f(math.Float64bits(1) ^ deltas[i%len(deltas)])
	})
	seqs = append(seqs, floats("shifted-windows", ts, vs))

	// A full chunk (65535 samples, the maximum count).
	ts, vs = regular(math.MaxUint16, t0, 15000, func(i int) float64 { return float64(i / 7) })
	full := floats("full", ts, vs)
	seqs = append(seqs, full)

	// Start timestamps (ignored by XOR, encoded by XOR2).
	seqs = append(seqs, withST(counter, "st-constant", func(int, int64) int64 { return t0 - 10000 }))
	seqs = append(seqs, withST(counter, "st-every-sample", func(_ int, t int64) int64 { return t - 1000 }))
	seqs = append(seqs, withST(counter, "st-first-only-zero-later", func(i int, _ int64) int64 {
		if i == 0 {
			return t0 - 5
		}
		return 0
	}))
	seqs = append(seqs, withST(counter, "st-resets", func(i int, _ int64) int64 {
		return t0 - 10000 + int64(i/60)*900000
	}))
	seqs = append(seqs, withST(counter, "st-change-at-1", func(i int, _ int64) int64 {
		if i == 0 {
			return 0
		}
		return t0 + 500
	}))
	seqs = append(seqs, withST(counter, "st-change-at-127", func(i int, t int64) int64 {
		if i < 127 {
			return 0
		}
		return t - 1
	}))
	seqs = append(seqs, withST(counter, "st-change-at-118", func(i int, t int64) int64 {
		if i < 118 {
			return 0
		}
		return t - 3
	}))
	seqs = append(seqs, withST(jitter, "st-random", func(i int, t int64) int64 {
		return t - int64(r.IntN(1<<uint(i%40+1)))
	}))
	seqs = append(seqs, withST(specials, "st-special-floats", func(i int, t int64) int64 { return t - int64(i%5)*300 }))
	seqs = append(seqs, withST(floats("st-large-dod", largeTs, largeVs), "st-large-dod", func(i int, t int64) int64 {
		return t - int64(i)*int64(i)*1000003
	}))
	seqs = append(seqs, withST(floats("", largeTs, largeVs), "st-after-t", func(_ int, t int64) int64 { return t + 77 }))

	// Appending continued on a chunk reloaded from its bytes (Go sets the write bit position in XOR2Chunk.Appender).
	for _, at := range []int{1, 2, 3, 5, 17, 61} {
		seqs = append(seqs, resumed(jitter, fmt.Sprintf("resume-%d-irregular-random", at), at))
		seqs = append(seqs, resumed(specials, fmt.Sprintf("resume-%d-special-floats", at), at))
		seqs = append(seqs, resumed(withST(jitter, "", func(i int, t int64) int64 { return t - int64(i%9)*13 }),
			fmt.Sprintf("resume-%d-st-irregular", at), at))
	}
	seqs = append(seqs, resumed(full, "resume-40000-full", 40000))
	return seqs
}

func writeCase(dir string, e encoder, seq sequence) error {
	c, err := e.fn(seq)
	if err != nil {
		return fmt.Errorf("%s/%s: %w", e.name, seq.name, err)
	}
	file, err := os.Create(filepath.Join(dir, e.name+"-"+seq.name+".txt"))
	if err != nil {
		return err
	}
	defer file.Close()
	w := bufio.NewWriter(file)
	fmt.Fprintf(w, "encoding %d\n", c.Encoding())
	fmt.Fprintf(w, "resume %d\n", seq.resumeAt)
	fmt.Fprintf(w, "bytes %s\n", hex.EncodeToString(c.Bytes()))
	for _, s := range seq.samples {
		fmt.Fprintf(w, "sample %d %d %016x\n", s.st, s.t, math.Float64bits(s.v))
	}
	it := c.Iterator(nil)
	for it.Next() == chunkenc.ValFloat {
		t, v := it.At()
		fmt.Fprintf(w, "decoded %d %d %016x\n", it.AtST(), t, math.Float64bits(v))
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("%s/%s: iterate: %w", e.name, seq.name, err)
	}
	return w.Flush()
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: gen <out dir>")
		os.Exit(2)
	}
	dir := os.Args[1]
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	n := 0
	for _, seq := range sequences() {
		for _, e := range encoders {
			if err := writeCase(dir, e, seq); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			n++
		}
	}
	hn, err := writeHistCases(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %d float and %d histogram cases to %s\n", n, hn, dir)
}
