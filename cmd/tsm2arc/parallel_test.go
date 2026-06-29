package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/basekick-labs/tsm2arc/internal/checkpoint"
	"github.com/basekick-labs/tsm2arc/internal/discover"
	"github.com/basekick-labs/tsm2arc/internal/sink"
	itsm "github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// writeMultiShard creates `nShards` shards under one database, each with
// `seriesPerShard` single-point series, and returns the datadir root.
func writeMultiShard(t *testing.T, db string, nShards, seriesPerShard int) string {
	t.Helper()
	root := t.TempDir()
	for s := 0; s < nShards; s++ {
		dir := filepath.Join(root, db, "autogen", fmt.Sprintf("%d", s+1))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		f, _ := os.Create(filepath.Join(dir, "000000001-000000001.tsm"))
		w, _ := itsm.NewTSMWriter(f)
		keys := make([]string, 0, seriesPerShard)
		vals := map[string]itsm.Values{}
		for i := 0; i < seriesPerShard; i++ {
			// globally-unique series so distinct-line counting verifies no loss
			k := fmt.Sprintf("cpu,shard=%d,host=node%04d#!~#usage", s, i)
			keys = append(keys, k)
			vals[k] = itsm.Values{itsm.NewFloatValue(int64(1700000000+s*100000+i)*1e9, float64(i)+0.5)}
		}
		sort.Strings(keys)
		for _, k := range keys {
			if err := w.Write([]byte(k), vals[k]); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.WriteIndex(); err != nil {
			t.Fatal(err)
		}
		w.Close()
		f.Close()
	}
	return root
}

// countingArc records all accepted LP lines under a lock (concurrent workers).
type countingArc struct {
	mu    sync.Mutex
	lines map[string]int
	rows  int
}

func (a *countingArc) handler(w http.ResponseWriter, r *http.Request) {
	f, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "nofile", 400)
		return
	}
	defer f.Close()
	data, _ := io.ReadAll(f)
	gz, _ := gzip.NewReader(bytes.NewReader(data))
	lp, _ := io.ReadAll(gz)
	a.mu.Lock()
	for _, line := range bytes.Split(lp, []byte("\n")) {
		if len(line) > 0 {
			a.lines[string(line)]++
			a.rows++
		}
	}
	a.mu.Unlock()
	fmt.Fprintf(w, `{"status":"ok","result":{"database":%q,"rows_imported":%d}}`,
		r.Header.Get("x-arc-database"), bytes.Count(lp, []byte("\n")))
}

// H-1 regression: the checkpoint must key on the stable SourceID, not the
// display Database name. A 2.x migration run with influxd.bolt present (shard
// keyed by bucket NAME) and then resumed without it (same shard now labeled by
// bucket ID) must NOT re-migrate — the SourceID is identical, so the shard is
// recognized as done. Before the fix, the checkpoint keyed on Database and the
// id↔name flip silently re-sent the whole bucket.
func TestResumeStableAcrossBucketNameFlip(t *testing.T) {
	datadir := writeTSMShard(t, "f549d32159486e62", 30) // dir name = bucket id
	arc := &countingArc{lines: map[string]int{}}
	srv := httptest.NewServer(http.HandlerFunc(arc.handler))
	defer srv.Close()

	base, err := discover.Walk(datadir, "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(base) != 1 {
		t.Fatalf("want 1 shard, got %d", len(base))
	}
	cpPath := filepath.Join(t.TempDir(), "cp.db")
	cfg := runConfig{start: math.MinInt64, end: math.MaxInt64, chunkSize: 256, workers: 2}

	// Run 1: bolt PRESENT → Database resolved to the bucket name; SourceID = id.
	run1 := []discover.Shard{base[0]}
	run1[0].Database = "telemetry" // as main.go would set from influxd.bolt
	// SourceID stays the bucket id (set by Walk).
	cfg.shards = run1
	cp1, _ := checkpoint.Open(cpPath)
	r1, err := load(context.Background(), cfg, sink.New(srv.URL, "t", "ns", fastRetry()), cp1)
	cp1.Close()
	if err != nil {
		t.Fatal(err)
	}
	if r1.Chunks == 0 {
		t.Fatal("run 1 sent nothing")
	}
	rowsAfterRun1 := arc.rows

	// Run 2: bolt ABSENT → Database falls back to the bucket id; SourceID same.
	run2 := []discover.Shard{base[0]}
	run2[0].Database = run2[0].SourceID // name unresolved → id
	cfg.shards = run2
	cp2, _ := checkpoint.Open(cpPath)
	r2, err := load(context.Background(), cfg, sink.New(srv.URL, "t", "ns", fastRetry()), cp2)
	cp2.Close()
	if err != nil {
		t.Fatal(err)
	}

	if r2.SkippedShards != 1 {
		t.Errorf("run 2 should skip the done shard via SourceID; skippedShards=%d", r2.SkippedShards)
	}
	if r2.Chunks != 0 {
		t.Errorf("run 2 re-sent %d chunks — name/id flip broke resume (H-1)", r2.Chunks)
	}
	if arc.rows != rowsAfterRun1 {
		t.Errorf("run 2 re-migrated data: rows %d → %d (H-1 regression)", rowsAfterRun1, arc.rows)
	}
}

func TestParallelLoadCorrectness(t *testing.T) {
	const nShards, seriesPerShard = 8, 50
	const total = nShards * seriesPerShard
	datadir := writeMultiShard(t, "metrics", nShards, seriesPerShard)

	arc := &countingArc{lines: map[string]int{}}
	srv := httptest.NewServer(http.HandlerFunc(arc.handler))
	defer srv.Close()

	shards, err := discover.Walk(datadir, "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != nShards {
		t.Fatalf("discovered %d shards, want %d", len(shards), nShards)
	}

	cpPath := filepath.Join(t.TempDir(), "cp.db")
	cp, err := checkpoint.Open(cpPath)
	if err != nil {
		t.Fatal(err)
	}
	defer cp.Close()

	cfg := runConfig{
		shards:    shards,
		start:     math.MinInt64,
		end:       math.MaxInt64,
		chunkSize: 256, // many chunks per shard
		workers:   4,   // real concurrency
	}

	res, err := load(context.Background(), cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp)
	if err != nil {
		t.Fatalf("parallel load failed: %v", err)
	}

	// Every point present exactly once — no loss, no corruption from interleaving.
	if len(arc.lines) != total {
		t.Fatalf("distinct lines = %d, want %d (loss or corruption)", len(arc.lines), total)
	}
	for line, n := range arc.lines {
		if n != 1 {
			t.Errorf("line duplicated %dx in a clean parallel run: %q", n, line)
		}
	}
	if res.Rows != int64(total) {
		t.Errorf("reported rows = %d, want %d", res.Rows, total)
	}

	// Every shard marked done in the checkpoint.
	for _, sh := range shards {
		done, _ := cp.IsShardDone(sh.Database, sh.ShardID)
		if !done {
			t.Errorf("shard %s not marked done after parallel load", sh.ShardID)
		}
	}

	// Resume on a fully-done set → no work, all shards skipped.
	res2, err := load(context.Background(), cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Chunks != 0 || int(res2.SkippedShards) != nShards {
		t.Errorf("resume did work: chunks=%d skippedShards=%d (want 0/%d)", res2.Chunks, res2.SkippedShards, nShards)
	}
}
