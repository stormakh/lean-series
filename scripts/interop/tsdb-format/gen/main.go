// Command gen is the Go side of the tsdb-format interop check (scripts/interop/tsdb-format/run.sh).
//
//	gen write <root>     writes TSDB files with upstream Prometheus code to <root>/go and their content dump to
//	                     <root>/go.dump.txt (made by reading the files back with upstream readers).
//	gen dump <dir> <out> writes the content dump of the files in <dir> to <out>.
//	gen verify <root>    reads the files that the Rust port wrote to <root>/rust with upstream readers, checks that
//	                     their content dump equals <root>/go.dump.txt, and checks that the files whose bytes are
//	                     deterministic are byte-identical to the Go files.
//
// Layout of <root>/go and <root>/rust:
//
//	index                      block index (format v2) of all series
//	chunks/000001...           block chunk segments (small segment size, so there are several)
//	chunks_head/000001...      head chunk files written by ChunkDiskMapper (two files, some OOO chunks)
//	wal-none/, wal-snappy/, wal-zstd/  WAL segments with every record type, plus one checkpoint each
//	tombstones                 tombstones file
//
// The dump format is line based; crates/tsdb-format/tests/interop_test.rs writes the same format.
package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/prometheus/prometheus/tsdb/wlog"
	"github.com/prometheus/prometheus/util/compression"
)

// Parameters shared with the Rust side (crates/tsdb-format/tests/interop_test.rs).
const (
	numSeries         = 300
	chunkSegmentSize  = 64 * 1024
	walSegmentSize    = 4 * 32 * 1024
	// compressedWALSegmentSize is large enough that snappy and zstd WALs never cut a segment because it is full.
	compressedWALSegmentSize = 32 * 32 * 1024
	checkpointMint    = 1_000_000
	headCutAfterChunk = 150
)

var logger = slog.New(slog.NewTextHandler(io.Discard, nil))

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: gen write|verify <root> | gen dump <dir> <out>")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "write":
		err = write(os.Args[2])
	case "dump":
		var s string
		if s, err = dump(os.Args[2]); err == nil {
			err = os.WriteFile(os.Args[3], []byte(s), 0o644)
		}
	case "verify":
		err = verify(os.Args[2])
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// ---------------------------------------------------------------------------------------------------------------
// Content.

func seriesLabels() []labels.Labels {
	jobs := []string{"api", "db", "web"}
	var out []labels.Labels
	for i := 0; i < numSeries; i++ {
		b := labels.NewScratchBuilder(8)
		b.Add("__name__", fmt.Sprintf("metric_%d", i%7))
		b.Add("instance", fmt.Sprintf("host-%d:9100", i%13))
		b.Add("job", jobs[i%3])
		b.Add("series", fmt.Sprint(i))
		if i%5 == 0 {
			b.Add("path", fmt.Sprintf("/a/ü/%d with space", i%4))
		}
		if i%11 == 0 {
			b.Add("le", "+Inf")
		}
		if i == 42 {
			for k := 0; k < 20; k++ {
				b.Add(fmt.Sprintf("extra_%02d", k), strings.Repeat("x", k+1))
			}
		}
		b.Sort()
		out = append(out, b.Labels())
	}
	sort.Slice(out, func(i, j int) bool { return labels.Compare(out[i], out[j]) < 0 })
	return out
}

// makeChunk builds one chunk of the encoding kind k (0..5) for series i, chunk j.
func makeChunk(i, j, k int) chunks.Meta {
	base := int64(j)*10_000_000 + int64(i)
	var c chunkenc.Chunk
	switch k {
	case 0:
		c = chunkenc.NewXORChunk()
	case 1:
		c = chunkenc.NewHistogramChunk()
	case 2:
		c = chunkenc.NewFloatHistogramChunk()
	case 3:
		c = chunkenc.NewXOR2Chunk()
	case 4:
		c = chunkenc.NewHistogramSTChunk()
	case 5:
		c = chunkenc.NewFloatHistogramSTChunk()
	}
	app, err := c.Appender()
	must(err)
	n := 120
	if k != 0 && k != 3 {
		n = 30
	}
	var st int64
	if k >= 3 {
		st = base - 5000
	}
	t := base
	for s := 0; s < n; s++ {
		t = base + int64(s)*15_000 + int64((s*7)%5)
		switch k {
		case 0, 3:
			v := float64(i*1000+s) + 0.25*float64(s%3)
			if s == 17 {
				v = math.Float64frombits(value.StaleNaN)
			}
			if s == 18 {
				v = math.Inf(-1)
			}
			app.Append(st, t, v)
		case 1, 4:
			nc, _, a, err := app.AppendHistogram(nil, st, t, tsdbutil.GenerateTestHistogram(int64(s+i)), false)
			must(err)
			if nc != nil {
				panic("unexpected new chunk")
			}
			app = a
		case 2, 5:
			nc, _, a, err := app.AppendFloatHistogram(nil, st, t, tsdbutil.GenerateTestFloatHistogram(int64(s+i)), false)
			must(err)
			if nc != nil {
				panic("unexpected new chunk")
			}
			app = a
		}
	}
	return chunks.Meta{Chunk: c, MinTime: base, MaxTime: t}
}

func numChunks(i int) int { return 1 + i%3 }

func walRecords() [][]byte {
	lsets := seriesLabels()
	var recs [][]byte
	enc := record.Encoder{}
	encST := record.Encoder{EnableSTStorage: true}
	for round := 0; round < 6; round++ {
		var series []record.RefSeries
		for i := round * 50; i < (round+1)*50; i++ {
			series = append(series, record.RefSeries{Ref: chunks.HeadSeriesRef(i + 1), Labels: lsets[i]})
		}
		recs = append(recs, enc.Series(series, nil))

		var md []record.RefMetadata
		for i := round * 50; i < (round+1)*50; i += 3 {
			md = append(md, record.RefMetadata{Ref: chunks.HeadSeriesRef(i + 1), Type: uint8(i % 6), Unit: "seconds", Help: fmt.Sprintf("help text %d, round %d", i, round)})
		}
		recs = append(recs, enc.Metadata(md, nil))

		base := int64(round) * 600_000
		var samples []record.RefSample
		for s := 0; s < 1500; s++ {
			ref := chunks.HeadSeriesRef(1 + (s*37)%numSeries)
			v := float64(s) * 1.5
			if s%97 == 0 {
				v = math.Float64frombits(value.StaleNaN)
			}
			samples = append(samples, record.RefSample{Ref: ref, T: base + int64(s%50)*1000 - 3000, V: v})
		}
		recs = append(recs, enc.Samples(samples, nil))

		var samplesST []record.RefSample
		for s := 0; s < 800; s++ {
			ref := chunks.HeadSeriesRef(1 + (s*13)%numSeries)
			var st int64
			switch s % 4 {
			case 1, 2:
				st = base - 60_000
			case 3:
				st = base - int64(s)*7
			}
			samplesST = append(samplesST, record.RefSample{Ref: ref, ST: st, T: base + int64(s)*100, V: float64(s) / 3})
		}
		recs = append(recs, encST.Samples(samplesST, nil))

		recs = append(recs, enc.Tombstones([]tombstones.Stone{
			{Ref: storage.SeriesRef(round + 1), Intervals: tombstones.Intervals{{Mint: base, Maxt: base + 100}, {Mint: base + 500, Maxt: base + 900}}},
			{Ref: storage.SeriesRef(round + 100), Intervals: tombstones.Intervals{{Mint: math.MinInt64, Maxt: base}}},
		}, nil))

		var exemplars []record.RefExemplar
		for e := 0; e < 40; e++ {
			exemplars = append(exemplars, record.RefExemplar{
				Ref: chunks.HeadSeriesRef(1 + e*7), T: base + int64(e)*10, V: float64(e) + 0.5,
				Labels: labels.FromStrings("trace_id", fmt.Sprintf("%016x", e*12345+round)),
			})
		}
		recs = append(recs, enc.Exemplars(exemplars, nil))

		var markers []record.RefMmapMarker
		for m := 0; m < 30; m++ {
			markers = append(markers, record.RefMmapMarker{Ref: chunks.HeadSeriesRef(1 + m*9), MmapRef: chunks.ChunkDiskMapperRef(uint64(round+1)<<32 | uint64(m*1000+8))})
		}
		recs = append(recs, enc.MmapMarkers(markers, nil))

		var hs []record.RefHistogramSample
		var fhs []record.RefFloatHistogramSample
		for h := 0; h < 60; h++ {
			ref := chunks.HeadSeriesRef(1 + (h*11)%numSeries)
			t := base + int64(h)*1000
			var st int64
			if h%3 != 0 {
				st = base - 1000
			}
			if h%4 == 3 {
				hs = append(hs, record.RefHistogramSample{Ref: ref, ST: st, T: t, H: tsdbutil.GenerateTestCustomBucketsHistogram(int64(h))})
				fhs = append(fhs, record.RefFloatHistogramSample{Ref: ref, ST: st, T: t, FH: tsdbutil.GenerateTestCustomBucketsFloatHistogram(int64(h))})
			} else if h%5 == 0 {
				hs = append(hs, record.RefHistogramSample{Ref: ref, ST: st, T: t, H: tsdbutil.GenerateTestGaugeHistogram(int64(h))})
				fhs = append(fhs, record.RefFloatHistogramSample{Ref: ref, ST: st, T: t, FH: tsdbutil.GenerateTestGaugeFloatHistogram(int64(h))})
			} else {
				hs = append(hs, record.RefHistogramSample{Ref: ref, ST: st, T: t, H: tsdbutil.GenerateTestHistogram(int64(h))})
				fhs = append(fhs, record.RefFloatHistogramSample{Ref: ref, ST: st, T: t, FH: tsdbutil.GenerateTestFloatHistogram(int64(h))})
			}
		}
		// V1 records: regular histograms and custom bucket histograms go to separate records.
		rec, custom := enc.HistogramSamples(hs, nil)
		recs = append(recs, rec)
		if len(custom) > 0 {
			recs = append(recs, enc.CustomBucketsHistogramSamples(custom, nil))
		}
		frec, fcustom := enc.FloatHistogramSamples(fhs, nil)
		recs = append(recs, frec)
		if len(fcustom) > 0 {
			recs = append(recs, enc.CustomBucketsFloatHistogramSamples(fcustom, nil))
		}
		// V2 records (start timestamps), mixed regular and custom buckets in one record.
		rec2, _ := encST.HistogramSamples(hs, nil)
		recs = append(recs, rec2)
		frec2, _ := encST.FloatHistogramSamples(fhs, nil)
		recs = append(recs, frec2)

		recs = append(recs, enc.MinValidTime(base-1, nil))
	}
	// One large record (> one 32 KiB page) to get first/middle/last fragments.
	var big []record.RefSample
	for s := 0; s < 5000; s++ {
		big = append(big, record.RefSample{Ref: chunks.HeadSeriesRef(1 + s%numSeries), T: 4_000_000 + int64(s), V: float64(s)})
	}
	recs = append(recs, enc.Samples(big, nil))
	return recs
}

// ---------------------------------------------------------------------------------------------------------------
// Write.

func write(root string) error {
	dir := filepath.Join(root, "go")
	must(os.MkdirAll(dir, 0o777))
	lsets := seriesLabels()

	// Block chunks and index.
	cw, err := chunks.NewWriter(filepath.Join(dir, "chunks"), chunks.WithSegmentSize(chunkSegmentSize))
	must(err)
	metas := make([][]chunks.Meta, len(lsets))
	for i := range lsets {
		for j := 0; j < numChunks(i); j++ {
			metas[i] = append(metas[i], makeChunk(i, j, (i+j)%6))
		}
		must(cw.WriteChunks(metas[i]...))
	}
	must(cw.Close())

	iw, err := index.NewWriter(context.Background(), filepath.Join(dir, "index"))
	must(err)
	syms := map[string]struct{}{}
	for _, ls := range lsets {
		ls.Range(func(l labels.Label) { syms[l.Name] = struct{}{}; syms[l.Value] = struct{}{} })
	}
	var symList []string
	for s := range syms {
		symList = append(symList, s)
	}
	sort.Strings(symList)
	for _, s := range symList {
		must(iw.AddSymbol(s))
	}
	for i, ls := range lsets {
		must(iw.AddSeries(storage.SeriesRef(i+1), ls, metas[i]...))
	}
	must(iw.Close())

	// Head chunks.
	cdm, err := chunks.NewChunkDiskMapper(nil, filepath.Join(dir, "chunks_head"), chunkenc.NewPool(), chunks.DefaultWriteBufferSize, chunks.DefaultWriteQueueSize)
	must(err)
	n := 0
	var cbErr error
	for i := range lsets {
		for j := 0; j < numChunks(i); j++ {
			m := makeChunk(i, j, (i+2*j)%6)
			if n == headCutAfterChunk {
				cdm.CutNewFile()
			}
			cdm.WriteChunk(chunks.HeadSeriesRef(i+1), m.MinTime, m.MaxTime, m.Chunk, i%4 == 3, func(err error) {
				if err != nil {
					cbErr = err
				}
			})
			n++
		}
	}
	must(cdm.Close())
	must(cbErr)

	// WALs with one checkpoint each.
	recs := walRecords()
	for _, c := range []compression.Type{compression.None, compression.Snappy, compression.Zstd} {
		// The uncompressed WAL cuts segments when they are full; compressed WALs get large segments, because the
		// compressed sizes depend on the compressor implementation. All WALs also cut a segment at a fixed record.
		size := walSegmentSize
		if c != compression.None {
			size = compressedWALSegmentSize
		}
		w, err := wlog.NewSize(logger, nil, filepath.Join(dir, "wal-"+string(c)), size, c)
		must(err)
		for i, r := range recs {
			if i == len(recs)/2 {
				_, err := w.NextSegment()
				must(err)
			}
			must(w.Log(r))
		}
		_, last, err := wlog.Segments(w.Dir())
		must(err)
		keep := func(id chunks.HeadSeriesRef) bool { return id%3 != 0 }
		_, err = wlog.Checkpoint(logger, w, 0, last-1, keep, checkpointMint, false, true)
		must(err)
		must(w.Close())
	}

	// Tombstones.
	ts := tombstones.NewMemTombstones()
	for r := 1; r <= 50; r++ {
		ts.AddInterval(storage.SeriesRef(r*3), tombstones.Interval{Mint: int64(r) * 100, Maxt: int64(r)*100 + 50})
		if r%4 == 0 {
			ts.AddInterval(storage.SeriesRef(r*3), tombstones.Interval{Mint: int64(r) * 1000, Maxt: math.MaxInt64})
		}
	}
	_, err = tombstones.WriteFile(logger, dir, ts)
	must(err)

	s, err := dump(dir)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, "go.dump.txt"), []byte(s), 0o644)
}

// ---------------------------------------------------------------------------------------------------------------
// Dump.

func lstr(ls labels.Labels) string {
	var sb strings.Builder
	ls.Range(func(l labels.Label) {
		if sb.Len() > 0 {
			sb.WriteByte(';')
		}
		sb.WriteString(l.Name)
		sb.WriteByte('=')
		sb.WriteString(l.Value)
	})
	return sb.String()
}

func dump(dir string) (string, error) {
	var out strings.Builder
	p := func(format string, args ...any) { fmt.Fprintf(&out, format+"\n", args...) }
	ctx := context.Background()

	// Index and block chunks.
	ir, err := index.NewFileReader(filepath.Join(dir, "index"), index.DecodePostingsRaw)
	if err != nil {
		return "", fmt.Errorf("open index: %w", err)
	}
	p("index version %d", ir.Version())
	it := ir.Symbols()
	for it.Next() {
		p("symbol %s", it.At())
	}
	if it.Err() != nil {
		return "", it.Err()
	}
	names, err := ir.LabelNames(ctx)
	if err != nil {
		return "", err
	}
	p("labelnames %s", strings.Join(names, " "))
	for _, n := range names {
		vals, err := ir.SortedLabelValues(ctx, n, nil)
		if err != nil {
			return "", err
		}
		for _, v := range vals {
			pp, err := ir.Postings(ctx, n, v)
			if err != nil {
				return "", err
			}
			refs, err := index.ExpandPostings(pp)
			if err != nil {
				return "", err
			}
			var rs []string
			for _, r := range refs {
				rs = append(rs, fmt.Sprint(r))
			}
			p("postings %s\t%s\t%s", n, v, strings.Join(rs, " "))
		}
	}
	cr, err := chunks.NewDirReader(filepath.Join(dir, "chunks"), nil)
	if err != nil {
		return "", fmt.Errorf("open chunks: %w", err)
	}
	k, v := index.AllPostingsKey()
	all, err := ir.Postings(ctx, k, v)
	if err != nil {
		return "", err
	}
	var b labels.ScratchBuilder
	var metas []chunks.Meta
	for all.Next() {
		if err := ir.Series(all.At(), &b, &metas); err != nil {
			return "", err
		}
		var cs []string
		for _, m := range metas {
			cs = append(cs, fmt.Sprintf("%d:%d:%d", m.Ref, m.MinTime, m.MaxTime))
		}
		p("series %d\t%s\t%s", all.At(), lstr(b.Labels()), strings.Join(cs, " "))
		for _, m := range metas {
			c, iterable, err := cr.ChunkOrIterable(m)
			if err != nil {
				return "", err
			}
			if iterable != nil {
				return "", errors.New("unexpected iterable")
			}
			p("chunk %d %d %s", m.Ref, c.Encoding(), hex.EncodeToString(c.Bytes()))
		}
	}
	if all.Err() != nil {
		return "", all.Err()
	}
	must(cr.Close())
	must(ir.Close())

	// Head chunks.
	cdm, err := chunks.NewChunkDiskMapper(nil, filepath.Join(dir, "chunks_head"), chunkenc.NewPool(), chunks.DefaultWriteBufferSize, chunks.DefaultWriteQueueSize)
	if err != nil {
		return "", err
	}
	err = cdm.IterateAllChunks(func(seriesRef chunks.HeadSeriesRef, chunkRef chunks.ChunkDiskMapperRef, mint, maxt int64, numSamples uint16, enc chunkenc.Encoding, isOOO bool) error {
		c, err := cdm.Chunk(chunkRef)
		if err != nil {
			return err
		}
		p("headchunk %d %d %d %d %d %d %t %s", seriesRef, chunkRef, mint, maxt, numSamples, enc, isOOO, hex.EncodeToString(c.Bytes()))
		return nil
	})
	if err != nil {
		return "", err
	}
	must(cdm.Close())

	// WALs and checkpoints.
	for _, c := range []string{"none", "snappy", "zstd"} {
		wdir := filepath.Join(dir, "wal-"+c)
		first, last, err := wlog.Segments(wdir)
		if err != nil {
			return "", err
		}
		p("wal %s segments %d %d", c, first, last)
		sr, err := wlog.NewSegmentsReader(wdir)
		if err != nil {
			return "", err
		}
		r := wlog.NewReader(sr)
		for r.Next() {
			rec := r.Record()
			p("rec %s %d %s", c, rec[0], hex.EncodeToString(rec))
		}
		if r.Err() != nil {
			return "", r.Err()
		}
		sr.Close()

		cp, idx, err := wlog.LastCheckpoint(wdir)
		if err != nil {
			return "", err
		}
		p("checkpoint %s %s %d", c, filepath.Base(cp), idx)
		sr, err = wlog.NewSegmentsReader(cp)
		if err != nil {
			return "", err
		}
		r = wlog.NewReader(sr)
		var md []record.RefMetadata
		dec := record.NewDecoder(nil, logger)
		for r.Next() {
			rec := r.Record()
			if dec.Type(rec) == record.Metadata {
				md, err = dec.Metadata(rec, md)
				if err != nil {
					return "", err
				}
				continue
			}
			p("cprec %s %d %s", c, rec[0], hex.EncodeToString(rec))
		}
		if r.Err() != nil {
			return "", r.Err()
		}
		sr.Close()
		// Metadata records come from a Go map, in random order: dump them sorted.
		sort.Slice(md, func(i, j int) bool { return md[i].Ref < md[j].Ref })
		for _, m := range md {
			p("cpmeta %s %d %d %s\t%s", c, m.Ref, m.Type, m.Unit, m.Help)
		}
	}

	// Tombstones (a Go map: dump sorted by ref).
	tr, _, err := tombstones.ReadTombstones(dir)
	if err != nil {
		return "", err
	}
	type stone struct {
		ref storage.SeriesRef
		ivs tombstones.Intervals
	}
	var stones []stone
	must(tr.Iter(func(ref storage.SeriesRef, ivs tombstones.Intervals) error {
		stones = append(stones, stone{ref, ivs})
		return nil
	}))
	sort.Slice(stones, func(i, j int) bool { return stones[i].ref < stones[j].ref })
	for _, s := range stones {
		var is []string
		for _, iv := range s.ivs {
			is = append(is, fmt.Sprintf("%d:%d", iv.Mint, iv.Maxt))
		}
		p("tombstone %d %s", s.ref, strings.Join(is, " "))
	}
	return out.String(), nil
}

// ---------------------------------------------------------------------------------------------------------------
// Verify.

func verify(root string) error {
	want, err := os.ReadFile(filepath.Join(root, "go.dump.txt"))
	if err != nil {
		return err
	}
	got, err := dump(filepath.Join(root, "rust"))
	if err != nil {
		return fmt.Errorf("Go cannot read the Rust files: %w", err)
	}
	must(os.WriteFile(filepath.Join(root, "rust.dump-by-go.txt"), []byte(got), 0o644))
	failed := false
	if !bytes.Equal(want, []byte(got)) {
		failed = true
		wl, gl := strings.Split(string(want), "\n"), strings.Split(got, "\n")
		shown := 0
		for i := 0; i < len(wl) || i < len(gl); i++ {
			var a, b string
			if i < len(wl) {
				a = wl[i]
			}
			if i < len(gl) {
				b = gl[i]
			}
			if a != b && shown < 5 {
				fmt.Printf("content differs at line %d:\n  go:   %.200s\n  rust: %.200s\n", i+1, a, b)
				shown++
			}
		}
		fmt.Println("FAIL content: Go reading the Rust files gives a different dump (see rust.dump-by-go.txt)")
	} else {
		fmt.Printf("ok   content: Go reads the Rust files with the same content (%d dump lines)\n", strings.Count(got, "\n"))
	}

	// Byte-identical files (deterministic formats). Snappy and zstd segments depend on the compressor
	// implementation, so they are reported but not required.
	gdir, rdir := filepath.Join(root, "go"), filepath.Join(root, "rust")
	required := []string{"index", "chunks", "chunks_head", "wal-none"}
	optional := []string{"wal-snappy", "wal-zstd"}
	for _, group := range [][]string{required, optional} {
		for _, name := range group {
			files, err := listFiles(filepath.Join(gdir, name))
			if err != nil {
				return err
			}
			rfiles, err := listFiles(filepath.Join(rdir, name))
			if err != nil {
				return err
			}
			same := len(files) == len(rfiles)
			var diffs []string
			for i, f := range files {
				if i >= len(rfiles) || rfiles[i] != f {
					same = false
					continue
				}
				// Checkpoints hold metadata records in Go map order: compared by content only.
				if strings.Contains(f, "checkpoint.") {
					continue
				}
				a, _ := os.ReadFile(filepath.Join(gdir, name, f))
				b, _ := os.ReadFile(filepath.Join(rdir, name, f))
				if !bytes.Equal(a, b) {
					same = false
					diffs = append(diffs, fmt.Sprintf("%s (go %d bytes, rust %d bytes, first diff at %d)", f, len(a), len(b), firstDiff(a, b)))
				}
			}
			isReq := name != "wal-snappy" && name != "wal-zstd"
			switch {
			case same:
				fmt.Printf("ok   bytes: %s identical (%d files)\n", name, len(files))
			case isReq:
				failed = true
				fmt.Printf("FAIL bytes: %s differs: files go=%v rust=%v %s\n", name, files, rfiles, strings.Join(diffs, "; "))
			default:
				fmt.Printf("info bytes: %s differs (compressor output; records are compared above): %s\n", name, strings.Join(diffs, "; "))
			}
		}
	}
	if failed {
		return errors.New("interop check failed")
	}
	fmt.Println("PASS: Go reads the Rust files; deterministic formats are byte-identical")
	return nil
}

func firstDiff(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return min(len(a), len(b))
}

// listFiles returns the relative paths of the regular files under dir (or dir itself when it is a file), sorted.
func listFiles(dir string) ([]string, error) {
	st, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() {
		return []string{""}, nil
	}
	var out []string
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			rel, _ := filepath.Rel(dir, path)
			out = append(out, rel)
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

