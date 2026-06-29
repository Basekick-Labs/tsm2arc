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
	"time"

	"github.com/basekick-labs/tsm2arc/internal/checkpoint"
	"github.com/basekick-labs/tsm2arc/internal/discover"
	"github.com/basekick-labs/tsm2arc/internal/sink"
	itsm "github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// writeTSMShard writes nSeries single-point float series into one shard and
// returns the datadir root.
func writeTSMShard(t *testing.T, db string, nSeries int) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, db, "autogen", "1")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(dir, "000000001-000000001.tsm"))
	if err != nil {
		t.Fatal(err)
	}
	w, err := itsm.NewTSMWriter(f)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, nSeries)
	vals := map[string]itsm.Values{}
	for i := 0; i < nSeries; i++ {
		k := fmt.Sprintf("cpu,host=node%03d#!~#usage", i)
		keys = append(keys, k)
		vals[k] = itsm.Values{itsm.NewFloatValue(int64(1700000000+i)*1e9, float64(i)+0.5)}
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
	return root
}

// crashingArc mocks Arc's import endpoint: it accepts the first crashAfter
// chunks, then fails every request (simulating a crash where the in-flight
// chunk's checkpoint is never written). It records all accepted LP for gap/dup
// verification.
type crashingArc struct {
	mu         sync.Mutex
	accepts    int
	crashAfter int // -1 = never crash
	acceptedLP [][]byte
	rows       int
}

func (a *crashingArc) handler(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.crashAfter >= 0 && a.accepts >= a.crashAfter {
		http.Error(w, "crashed", http.StatusServiceUnavailable)
		return
	}
	f, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "nofile", 400)
		return
	}
	defer f.Close()
	data, _ := io.ReadAll(f)
	gz, _ := gzip.NewReader(bytes.NewReader(data))
	lp, _ := io.ReadAll(gz)
	n := bytes.Count(lp, []byte("\n"))
	a.accepts++
	a.rows += n
	a.acceptedLP = append(a.acceptedLP, append([]byte(nil), lp...))
	fmt.Fprintf(w, `{"status":"ok","result":{"database":%q,"rows_imported":%d}}`,
		r.Header.Get("x-arc-database"), n)
}

func (a *crashingArc) distinctLines() map[string]int {
	set := map[string]int{}
	for _, lp := range a.acceptedLP {
		for _, line := range bytes.Split(lp, []byte("\n")) {
			if len(line) > 0 {
				set[string(line)]++
			}
		}
	}
	return set
}

func fastRetry() sink.Option { return sink.WithRetry(2, time.Millisecond, 5*time.Millisecond) }

// TestResumePersistedButUncheckpointed simulates the real duplicate-producing
// crash window: Arc persists a chunk (returns 2xx) but the migrator dies before
// writing that chunk's checkpoint. On resume the chunk is re-sent → exactly one
// chunk of duplicate rows, which the test asserts is bounded (not unbounded
// replay and not data loss). This is the window DESIGN.md §6.5 describes.
func TestResumePersistedButUncheckpointed(t *testing.T) {
	const nSeries = 40
	datadir := writeTSMShard(t, "metrics", nSeries)

	arc := &crashingArc{crashAfter: -1}
	srv := httptest.NewServer(http.HandlerFunc(arc.handler))
	defer srv.Close()

	cpPath := filepath.Join(t.TempDir(), "cp.db")
	shards, _ := discover.Walk(datadir, "", nil, false)
	cfg := runConfig{shards: shards, start: math.MinInt64, end: math.MaxInt64, chunkSize: 256}
	ctx := context.Background()

	// Run 1: open checkpoint, send a couple chunks, but DROP the last checkpoint
	// write to simulate a crash after Arc persisted but before we recorded it.
	// We do this by sending 2 chunks normally, then manually rewinding the
	// committed seq by 1 (as if that last commit never landed).
	cp1, _ := checkpoint.Open(cpPath)
	snk := sink.New(srv.URL, "tok", "ns", fastRetry())
	// send just the first shard's first 2 chunks via a bounded run, then stop.
	// Simplest: do a full load, then rewind the checkpoint to mimic a lost commit.
	if _, err := load(ctx, cfg, snk, cp1); err != nil {
		t.Fatal(err)
	}
	cp1.Close()
	acceptedRun1 := arc.rows
	if acceptedRun1 != nSeries {
		t.Fatalf("run1 imported %d, want %d", acceptedRun1, nSeries)
	}

	// Simulate a lost last-commit: clear shard_done and rewind committed_seq by 1
	// so the highest chunk looks un-acknowledged even though Arc has it.
	rewindLastCommit(t, cpPath, "metrics", "1")

	// Run 2: resume. The rewound chunk is re-sent (Arc already had it → dup).
	cp2, _ := checkpoint.Open(cpPath)
	if _, err := load(ctx, cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp2); err != nil {
		t.Fatal(err)
	}
	cp2.Close()

	// Verify: no data loss (all distinct points present), bounded duplication
	// (exactly one chunk re-sent → overlap == that chunk's row count, > 0).
	lines := arc.distinctLines()
	if len(lines) != nSeries {
		t.Fatalf("distinct points = %d, want %d", len(lines), nSeries)
	}
	overlap := arc.rows - nSeries
	if overlap <= 0 {
		t.Fatalf("expected a re-sent chunk (overlap>0), got overlap=%d", overlap)
	}
	if overlap > 8 { // one chunk ≈ 6 lines; allow headroom
		t.Fatalf("overlap %d exceeds one chunk — unbounded replay", overlap)
	}
	t.Logf("DONE: %d distinct, %d total rows, bounded overlap=%d (one re-sent chunk)",
		len(lines), arc.rows, overlap)
}

// rewindLastCommit lowers a shard's committed_seq by 1 and clears its done mark,
// simulating a crash where the last chunk was persisted by Arc but its
// checkpoint commit was lost.
func rewindLastCommit(t *testing.T, path, db, shard string) {
	t.Helper()
	s, err := checkpoint.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.RewindForTest(db, shard); err != nil {
		t.Fatal(err)
	}
}

func TestResumeAfterCrash(t *testing.T) {
	const nSeries = 60
	datadir := writeTSMShard(t, "metrics", nSeries)

	arc := &crashingArc{crashAfter: 3} // accept 3 chunks, then crash
	srv := httptest.NewServer(http.HandlerFunc(arc.handler))
	defer srv.Close()

	cpPath := filepath.Join(t.TempDir(), "cp.db")
	shards, err := discover.Walk(datadir, "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	// ~40 bytes/line, chunk=256 → ~6 lines/chunk → ~10 chunks for 60 points.
	cfg := runConfig{shards: shards, start: math.MinInt64, end: math.MaxInt64, chunkSize: 256}
	ctx := context.Background()

	// --- Run 1: crashes after 3 accepted chunks.
	cp1, err := checkpoint.Open(cpPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = load(ctx, cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp1)
	cp1.Close()
	if err == nil {
		t.Fatal("expected first pass to fail at the simulated crash")
	}
	if arc.accepts != 3 {
		t.Fatalf("expected exactly 3 accepts before crash, got %d", arc.accepts)
	}

	// checkpoint must record the 3 acknowledged chunks (0-indexed → highest = 2)
	cpCheck, _ := checkpoint.Open(cpPath)
	committed, _ := cpCheck.CommittedSeq("metrics", "1")
	done, _ := cpCheck.IsShardDone("metrics", "1")
	cpCheck.Close()
	if committed != 2 {
		t.Fatalf("committed seq after crash = %d, want 2", committed)
	}
	if done {
		t.Fatal("shard marked done despite crash")
	}
	t.Logf("after crash: committed=%d, arc accepted %d chunks / %d rows", committed, arc.accepts, arc.rows)

	// --- Run 2: stop crashing, resume.
	arc.crashAfter = -1
	cp2, err := checkpoint.Open(cpPath)
	if err != nil {
		t.Fatal(err)
	}
	res, err := load(ctx, cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp2)
	cp2.Close()
	if err != nil {
		t.Fatalf("resume run failed: %v", err)
	}
	if res.SkippedChunks != 3 {
		t.Errorf("resume skipped %d chunks, want 3", res.SkippedChunks)
	}

	// --- Verify: every point present exactly once-or-more, no gaps.
	lines := arc.distinctLines()
	if len(lines) != nSeries {
		t.Fatalf("distinct points in Arc = %d, want %d (GAP or LOSS)", len(lines), nSeries)
	}
	overlap := arc.rows - nSeries
	if overlap < 0 {
		t.Fatalf("accepted rows %d < %d points (DATA LOST)", arc.rows, nSeries)
	}
	// Overlap bounded to at most one chunk's worth (the in-flight chunk at crash).
	// With ~6 lines/chunk, allow a small bound.
	if overlap > 6 {
		t.Errorf("overlap %d rows exceeds one chunk (resume re-sent too much)", overlap)
	}
	t.Logf("DONE: %d distinct points, %d total rows (overlap=%d)", len(lines), arc.rows, overlap)

	// --- Run 3: resume again on a fully-done shard → no work.
	cp3, _ := checkpoint.Open(cpPath)
	res3, err := load(ctx, cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp3)
	cp3.Close()
	if err != nil {
		t.Fatal(err)
	}
	if res3.Chunks != 0 || res3.SkippedShards != 1 {
		t.Errorf("third run did work: chunks=%d skippedShards=%d (want 0/1)", res3.Chunks, res3.SkippedShards)
	}
}
