// Memory of the upstream Go TSDB for the same workload as crates/tsdb/examples/tsdb_mem.rs.
// usage: go run . <dir> <series> <samples>
// Prints PSS (KiB, from /proc/self/smaps_rollup) after opening, after creating the series and after the samples.
package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/util/compression"
)

func pss() int64 {
	b, _ := os.ReadFile("/proc/self/smaps_rollup")
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "Pss:") {
			f := strings.Fields(l)
			v, _ := strconv.ParseInt(f[1], 10, 64)
			return v
		}
	}
	return 0
}

func report(stage string, n int) {
	runtime.GC()
	runtime.GC()
	p := pss()
	per := int64(0)
	if n > 0 {
		per = p * 1024 / int64(n)
	}
	fmt.Printf("go %s pss_kib=%d bytes_per_series=%d\n", stage, p, per)
}

func lbls(i int) labels.Labels {
	return labels.FromStrings("__name__", "bench_metric_"+strconv.Itoa(i%600), "instance", "host-"+strconv.Itoa(i/600),
		"job", "bench", "series_id", strconv.Itoa(i))
}

func main() {
	dir := os.Args[1]
	n, _ := strconv.Atoi(os.Args[2])
	s, _ := strconv.Atoi(os.Args[3])
	opts := tsdb.DefaultOptions()
	// The main.go defaults that matter here.
	opts.WALCompression = compression.Snappy
	opts.RetentionDuration = 15 * 24 * 3600 * 1000
	opts.MaxBlockDuration = opts.RetentionDuration / 10
	db, err := tsdb.Open(dir, nil, nil, opts, nil)
	if err != nil {
		panic(err)
	}
	report("open", 0)
	now := time.Now().UnixMilli()
	start := now - now%15000 - int64(s)*15000
	refs := make([]storage.SeriesRef, n)
	seed := uint64(1)
	for step := 0; step < s; step++ {
		app := db.Appender(context.Background())
		t := start + int64(step)*15000
		for i := 0; i < n; i++ {
			seed = seed*6364136223846793005 + 1442695040888963407
			v := float64((seed >> 33) % 101)
			ref, err := app.Append(refs[i], lbls(i), t, v)
			if err != nil {
				panic(err)
			}
			refs[i] = ref
		}
		if err := app.Commit(); err != nil {
			panic(err)
		}
		if step == 0 {
			report("series", n)
		}
	}
	report("samples", n)
	if err := db.Close(); err != nil {
		panic(err)
	}
}
