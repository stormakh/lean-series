package main

// Histogram encodings (histogram, float histogram, and their start timestamp variants).
//
// Manifest format of a histogram case (<out dir>/<encoder>-<sequence>.txt):
//
//	encoding <encoding byte>
//	resume <index>        (as for floats; 0: never)
//	cut <n>               (0: never; n > 0: before every n-th sample the caller cuts a new chunk itself and
//	                       passes the old appender as prev, as the TSDB head does)
//	hsample <st> <t> <h>  (integer histogram input, in append order) or
//	fsample <st> <t> <fh> (float histogram input)
//	bytes <hex>           (one line per chunk produced, in order; recoded chunks replace the current chunk)
//	hdecoded <chunk index> <st> <t> <h>    (AtHistogram(nil) of every sample, integer chunks only)
//	fdecoded <chunk index> <st> <t> <fh>   (AtFloatHistogram(nil) of every sample)
//	decoderr <chunk index> <error text>    (the iterator error after the last sample, if any)
//
// <h>:  <hint> <schema> <zero threshold bits> <zero count> <count> <sum bits> <pos spans> <pos buckets>
//       <neg spans> <neg buckets> <custom values>
// <fh>: the same with float bits for zero count, count and buckets.
// Spans are "offset:length" joined by ",", buckets and custom values are joined by ","; "-" is an empty list.
// Float values are written as the 16 hex digits of their bits.

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type hsample struct {
	st, t int64
	h     *histogram.Histogram
	fh    *histogram.FloatHistogram
}

type hsequence struct {
	name     string
	float    bool
	samples  []hsample
	resumeAt int
	cutEvery int
}

type hencoder struct {
	name  string
	enc   chunkenc.Encoding
	float bool
}

var hencoders = []hencoder{
	{"histogram", chunkenc.EncHistogram, false},
	{"histogramst", chunkenc.EncHistogramST, false},
	{"floathistogram", chunkenc.EncFloatHistogram, true},
	{"floathistogramst", chunkenc.EncFloatHistogramST, true},
}

func fbits(f float64) string { return fmt.Sprintf("%016x", math.Float64bits(f)) }

func fmtSpans(spans []histogram.Span) string {
	if len(spans) == 0 {
		return "-"
	}
	parts := make([]string, len(spans))
	for i, s := range spans {
		parts[i] = fmt.Sprintf("%d:%d", s.Offset, s.Length)
	}
	return strings.Join(parts, ",")
}

func fmtInts(v []int64) string {
	if len(v) == 0 {
		return "-"
	}
	parts := make([]string, len(v))
	for i, x := range v {
		parts[i] = strconv.FormatInt(x, 10)
	}
	return strings.Join(parts, ",")
}

func fmtFloats(v []float64) string {
	if len(v) == 0 {
		return "-"
	}
	parts := make([]string, len(v))
	for i, x := range v {
		parts[i] = fbits(x)
	}
	return strings.Join(parts, ",")
}

func fmtH(h *histogram.Histogram) string {
	return fmt.Sprintf("%d %d %s %d %d %s %s %s %s %s %s", h.CounterResetHint, h.Schema, fbits(h.ZeroThreshold),
		h.ZeroCount, h.Count, fbits(h.Sum), fmtSpans(h.PositiveSpans), fmtInts(h.PositiveBuckets),
		fmtSpans(h.NegativeSpans), fmtInts(h.NegativeBuckets), fmtFloats(h.CustomValues))
}

func fmtFH(h *histogram.FloatHistogram) string {
	return fmt.Sprintf("%d %d %s %s %s %s %s %s %s %s %s", h.CounterResetHint, h.Schema, fbits(h.ZeroThreshold),
		fbits(h.ZeroCount), fbits(h.Count), fbits(h.Sum), fmtSpans(h.PositiveSpans), fmtFloats(h.PositiveBuckets),
		fmtSpans(h.NegativeSpans), fmtFloats(h.NegativeBuckets), fmtFloats(h.CustomValues))
}

// encodeHist appends the samples and returns the bytes of every chunk produced, in order.
func encodeHist(e hencoder, seq hsequence) ([][]byte, error) {
	var out [][]byte
	c, err := chunkenc.NewEmptyChunk(e.enc)
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
		var prev chunkenc.Appender
		if seq.cutEvery > 0 && i > 0 && i%seq.cutEvery == 0 {
			out = append(out, append([]byte(nil), c.Bytes()...))
			prev = app
			if c, err = chunkenc.NewEmptyChunk(e.enc); err != nil {
				return nil, err
			}
			if app, err = c.Appender(); err != nil {
				return nil, err
			}
		}
		var (
			nc       chunkenc.Chunk
			recoded  bool
			newApp   chunkenc.Appender
			appendTo error
		)
		if e.float {
			fh := s.fh
			if fh == nil {
				fh = s.h.ToFloat(nil)
			}
			nc, recoded, newApp, appendTo = app.AppendFloatHistogram(prev, s.st, s.t, fh.Copy(), false)
		} else {
			nc, recoded, newApp, appendTo = app.AppendHistogram(prev, s.st, s.t, s.h.Copy(), false)
		}
		if appendTo != nil {
			return nil, appendTo
		}
		if nc != nil {
			if !recoded {
				out = append(out, append([]byte(nil), c.Bytes()...))
			}
			c = nc
		}
		app = newApp
	}
	return append(out, append([]byte(nil), c.Bytes()...)), nil
}

func writeHistCase(dir string, e hencoder, seq hsequence) error {
	chunks, err := encodeHist(e, seq)
	if err != nil {
		return fmt.Errorf("%s/%s: %w", e.name, seq.name, err)
	}
	file, err := os.Create(filepath.Join(dir, e.name+"-"+seq.name+".txt"))
	if err != nil {
		return err
	}
	defer file.Close()
	w := bufio.NewWriter(file)
	fmt.Fprintf(w, "encoding %d\n", e.enc)
	fmt.Fprintf(w, "resume %d\n", seq.resumeAt)
	fmt.Fprintf(w, "cut %d\n", seq.cutEvery)
	for _, s := range seq.samples {
		if s.fh != nil {
			fmt.Fprintf(w, "fsample %d %d %s\n", s.st, s.t, fmtFH(s.fh))
		} else {
			fmt.Fprintf(w, "hsample %d %d %s\n", s.st, s.t, fmtH(s.h))
		}
	}
	for _, b := range chunks {
		fmt.Fprintf(w, "bytes %s\n", hex.EncodeToString(b))
	}
	for ci, b := range chunks {
		c, err := chunkenc.FromData(e.enc, b)
		if err != nil {
			return err
		}
		it := c.Iterator(nil)
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			if vt == chunkenc.ValHistogram {
				t, h := it.AtHistogram(nil)
				fmt.Fprintf(w, "hdecoded %d %d %d %s\n", ci, it.AtST(), t, fmtH(h))
			}
			t, fh := it.AtFloatHistogram(nil)
			fmt.Fprintf(w, "fdecoded %d %d %d %s\n", ci, it.AtST(), t, fmtFH(fh))
		}
		if err := it.Err(); err != nil {
			// Go cannot always read back a plain histogram chunk that was appended to after a reload (the
			// write position is not restored). Record the error: Rust must fail the same way.
			fmt.Fprintf(w, "decoderr %d %s\n", ci, err)
		}
	}
	return w.Flush()
}

func span(o int32, l uint32) histogram.Span { return histogram.Span{Offset: o, Length: l} }

// counterHist is a growing counter histogram with a fixed layout.
func counterHist(i int) *histogram.Histogram {
	k := uint64(i + 1)
	return &histogram.Histogram{
		Schema:          1,
		ZeroThreshold:   0.001,
		ZeroCount:       2 * k,
		Count:           2*k + 3*k + 4*k + 5*k + 6*k,
		Sum:             18.4 * float64(k),
		PositiveSpans:   []histogram.Span{span(0, 2), span(1, 2)},
		PositiveBuckets: []int64{int64(k), 0, int64(k), int64(k)}, // k, k, 2k, 3k.
		NegativeSpans:   []histogram.Span{span(-1, 1)},
		NegativeBuckets: []int64{int64(k)},
	}
}

func histSequences() []hsequence {
	var seqs []hsequence
	const t0 = int64(1700000000000)
	ints := func(name string, n int, step int64, mk func(i int) *histogram.Histogram) hsequence {
		s := hsequence{name: name}
		for i := range n {
			s.samples = append(s.samples, hsample{t: t0 + int64(i)*step, h: mk(i)})
		}
		return s
	}

	seqs = append(seqs, ints("one", 1, 15000, counterHist))
	counter := ints("counter", 120, 15000, counterHist)
	seqs = append(seqs, counter)

	// Irregular timestamps and a sum with changing XOR windows.
	r := rand.New(rand.NewPCG(3, 5))
	irr := ints("irregular", 150, 15000, func(i int) *histogram.Histogram {
		h := counterHist(i)
		h.Sum = r.Float64() * 1e6 * float64(i+1)
		return h
	})
	for i := range irr.samples {
		irr.samples[i].t += int64(r.IntN(2001) - 1000)
	}
	seqs = append(seqs, irr)

	// Layout growth: new buckets appear over time (forward inserts, recoding), and buckets missing in a new
	// sample that are empty in the chunk (backward inserts).
	seqs = append(seqs, ints("layout-changes", 60, 15000, func(i int) *histogram.Histogram {
		k := int64(i + 1)
		h := &histogram.Histogram{Schema: 0, ZeroThreshold: 1e-128, Count: 0, Sum: float64(k)}
		switch {
		case i < 10:
			h.PositiveSpans = []histogram.Span{span(2, 2)}
			h.PositiveBuckets = []int64{k, 0}
		case i < 20:
			h.PositiveSpans = []histogram.Span{span(0, 1), span(1, 2), span(3, 1)}
			h.PositiveBuckets = []int64{0, k, 0, k}
		case i < 30:
			// The empty bucket at index 0 is dropped (backward insert).
			h.PositiveSpans = []histogram.Span{span(2, 2), span(3, 1)}
			h.PositiveBuckets = []int64{k, 0, k}
		default:
			h.PositiveSpans = []histogram.Span{span(-3, 2), span(4, 3), span(1, 2)}
			h.PositiveBuckets = []int64{0, 0, k, 0, 0, 0, k}
			h.NegativeSpans = []histogram.Span{span(0, 2)}
			h.NegativeBuckets = []int64{k, 0}
		}
		var cnt int64
		var cur int64
		for _, d := range h.PositiveBuckets {
			cur += d
			cnt += cur
		}
		cur = 0
		for _, d := range h.NegativeBuckets {
			cur += d
			cnt += cur
		}
		h.Count = uint64(cnt)
		return h
	}))

	// Stale markers between samples, and a stale first sample.
	seqs = append(seqs, ints("stale", 40, 15000, func(i int) *histogram.Histogram {
		if i%7 == 3 || i == 0 {
			return &histogram.Histogram{Sum: math.Float64frombits(value.StaleNaN)}
		}
		return counterHist(i)
	}))

	// Counter resets (new chunks with the counter reset header).
	seqs = append(seqs, ints("counter-resets", 90, 15000, func(i int) *histogram.Histogram {
		return counterHist(i % 30)
	}))

	// Gauge histograms with going up and down and layout changes (both ways).
	seqs = append(seqs, ints("gauge", 80, 15000, func(i int) *histogram.Histogram {
		h := counterHist((i * 7) % 13)
		h.CounterResetHint = histogram.GaugeType
		if i%10 >= 5 {
			h.PositiveSpans = []histogram.Span{span(0, 1), span(2, 2)}
			h.PositiveBuckets = []int64{int64(i), -int64(i) + 1, 3}
			h.Count = uint64(i) + 1 + 4 + h.ZeroCount + uint64(h.NegativeBuckets[0])
		}
		if i == 60 {
			h.Schema = 2 // Schema change: new chunk.
		}
		return h
	}))

	// Custom buckets (NHCB) with bounds of both encodings (multiples of 0.001 and full float bits).
	cv := []float64{0.001, 0.1, 0.5, 1, 2.5, 10, 33554.43, 33554.431, 1e10, -5, 0.0005}
	cvSorted := []float64{-5, 0.0005, 0.001, 0.1, 0.5, 1, 2.5, 10, 33554.43, 33554.431, 1e10}
	_ = cv
	seqs = append(seqs, ints("nhcb", 50, 60000, func(i int) *histogram.Histogram {
		k := int64(i + 1)
		h := &histogram.Histogram{
			Schema:          histogram.CustomBucketsSchema,
			Sum:             3.5 * float64(k),
			PositiveSpans:   []histogram.Span{span(0, 3), span(2, 4), span(1, 3)},
			PositiveBuckets: []int64{k, 0, 1, k, -1, 2, 0, k, 0, -k + 1},
			CustomValues:    cvSorted,
		}
		var cnt, cur int64
		for _, d := range h.PositiveBuckets {
			cur += d
			cnt += cur
		}
		h.Count = uint64(cnt)
		if i == 30 {
			h.CustomValues = []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11} // Different bounds: new chunk.
		}
		return h
	}))

	// A reserved schema above 8 (read back with reduced resolution) and a big layout.
	seqs = append(seqs, ints("schema-10", 20, 15000, func(i int) *histogram.Histogram {
		k := int64(i + 1)
		return &histogram.Histogram{
			Schema: 10, Count: uint64(4 * k), Sum: float64(k),
			PositiveSpans: []histogram.Span{span(1000, 4)}, PositiveBuckets: []int64{k, 0, 0, 0},
		}
	}))
	big := histogram.GenerateBigTestHistograms(64, 500)
	seqs = append(seqs, ints("big", len(big), 15000, func(i int) *histogram.Histogram { return big[i] }))

	// Head-style chunk cutting with prev appenders (counter reset header from prev.appendable).
	cut := ints("cut-every-25", 100, 15000, func(i int) *histogram.Histogram { return counterHist(i % 60) })
	cut.cutEvery = 25
	seqs = append(seqs, cut)

	// Start timestamps (ignored by the plain encodings).
	withST := func(s hsequence, name string, st func(i int, t int64) int64) hsequence {
		out := hsequence{name: name, float: s.float, resumeAt: s.resumeAt, cutEvery: s.cutEvery}
		for i, x := range s.samples {
			out.samples = append(out.samples, hsample{st: st(i, x.t), t: x.t, h: x.h, fh: x.fh})
		}
		return out
	}
	seqs = append(seqs, withST(counter, "st-constant", func(int, int64) int64 { return t0 - 10000 }))
	seqs = append(seqs, withST(counter, "st-every-sample", func(_ int, t int64) int64 { return t - 1000 }))
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
	big2 := ints("counter-200", 200, 15000, counterHist)
	seqs = append(seqs, withST(big2, "st-change-at-150", func(i int, t int64) int64 {
		if i < 150 {
			return 0
		}
		return t - 7
	}))
	seqs = append(seqs, withST(irr, "st-random", func(i int, t int64) int64 { return t - int64(r.IntN(1<<uint(i%40+1))) }))
	seqs = append(seqs, withST(seqs[3], "st-layout-changes", func(i int, t int64) int64 { return t - int64(i%4)*1000 }))
	seqs = append(seqs, withST(cut, "st-cut-every-25", func(i int, t int64) int64 { return t - int64(i)*3 }))

	// Appending continued on a chunk reloaded from its bytes.
	for _, at := range []int{1, 2, 7, 33} {
		s := irr
		s.name = fmt.Sprintf("resume-%d-irregular", at)
		s.resumeAt = at
		seqs = append(seqs, s)
		s2 := withST(irr, fmt.Sprintf("resume-%d-st-irregular", at), func(i int, t int64) int64 { return t - int64(i%9)*13 })
		s2.resumeAt = at
		seqs = append(seqs, s2)
	}

	// Float histograms with fractional counts.
	fseq := hsequence{name: "float-fractional", float: true}
	for i := range 120 {
		fh := counterHist(i).ToFloat(nil)
		fh.Mul(1.0 / 3.0)
		fh.Sum = r.NormFloat64() * 100
		fseq.samples = append(fseq.samples, hsample{t: t0 + int64(i)*15000, fh: fh})
	}
	seqs = append(seqs, fseq)
	seqs = append(seqs, withST(fseq, "st-float-fractional", func(i int, t int64) int64 { return t - int64(i/30)*100000 }))
	return seqs
}

func writeHistCases(dir string) (int, error) {
	n := 0
	for _, seq := range histSequences() {
		for _, e := range hencoders {
			if seq.float && !e.float {
				continue
			}
			if err := writeHistCase(dir, e, seq); err != nil {
				return n, err
			}
			n++
		}
	}
	return n, nil
}
