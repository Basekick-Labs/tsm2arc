package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/tsm2arc/internal/checkpoint"
	"github.com/basekick-labs/tsm2arc/internal/discover"
	"github.com/basekick-labs/tsm2arc/internal/sink"
)

// jitter wraps a handler with random latency so responses complete out of
// order — the condition the ordered committer must be correct under.
func jitter(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
		h(w, r)
	}
}

// A clean run at --inflight 3 under randomized response latency must deliver
// exactly the corpus: every point once, shard done, zero duplicates.
func TestInflightCleanRunExact(t *testing.T) {
	const nSeries = 60
	datadir := writeTSMShard(t, "metrics", nSeries)
	shards, _ := discover.Walk(datadir, "", nil, false)
	cfg := runConfig{shards: shards, start: math.MinInt64, end: math.MaxInt64,
		chunkSize: 256, pipeline: true, inflight: 3}

	arc := &crashingArc{crashAfter: -1}
	srv := httptest.NewServer(jitter(arc.handler))
	defer srv.Close()
	cp, _ := checkpoint.Open(filepath.Join(t.TempDir(), "cp.db"))
	defer cp.Close()

	res, err := load(context.Background(), cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp)
	if err != nil {
		t.Fatal(err)
	}
	if lines := arc.distinctLines(); len(lines) != nSeries || arc.rows != nSeries {
		t.Fatalf("distinct=%d rows=%d, want %d/%d (clean inflight run must not duplicate)",
			len(lines), arc.rows, nSeries, nSeries)
	}
	if res.Rows != int64(nSeries) {
		t.Errorf("reported rows = %d, want %d", res.Rows, nSeries)
	}
	if done, _ := cp.IsShardDone("metrics", "1"); !done {
		t.Error("shard not done after clean inflight run")
	}
}

// failingSeqArc rejects any request whose LP contains the marker (all
// attempts), while accepting everything else — a deterministic mid-stream
// failure while LATER chunks keep landing.
type failingSeqArc struct {
	crashingArc
	failMu     sync.Mutex
	failMarker string
	failing    bool
}

func (a *failingSeqArc) handler(w http.ResponseWriter, r *http.Request) {
	f, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "nofile", 400)
		return
	}
	data, _ := io.ReadAll(f)
	f.Close()
	gz, _ := gzip.NewReader(bytes.NewReader(data))
	lpBody, _ := io.ReadAll(gz)

	a.failMu.Lock()
	shouldFail := a.failing && bytes.Contains(lpBody, []byte(a.failMarker))
	a.failMu.Unlock()
	if shouldFail {
		http.Error(w, "injected failure", http.StatusServiceUnavailable)
		return
	}
	// Re-wrap the body for the embedded accounting handler.
	a.crashingArc.acceptRaw(w, r, lpBody)
}

// acceptRaw records a pre-extracted LP body (mirrors crashingArc.handler's
// accounting without re-reading the request).
func (a *crashingArc) acceptRaw(w http.ResponseWriter, r *http.Request, lp []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := bytes.Count(lp, []byte("\n"))
	a.accepts++
	a.rows += n
	a.acceptedLP = append(a.acceptedLP, append([]byte(nil), lp...))
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"ok","result":{"rows_imported":` + itoa(n) + `}}`))
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// The widened-window contract: seq n fails while n+1.. were already accepted.
// The committer must stop the committed prefix BEFORE n (later chunks stay
// uncommitted despite being in Arc), the shard must fail, and a resume — at a
// DIFFERENT inflight — must close the gap with duplicates bounded by K chunks.
func TestInflightMiddleFailureBounded(t *testing.T) {
	const nSeries = 60 // ~10 chunks at 256B
	const k = 3
	datadir := writeTSMShard(t, "metrics", nSeries)
	shards, _ := discover.Walk(datadir, "", nil, false)
	cfg := runConfig{shards: shards, start: math.MinInt64, end: math.MaxInt64,
		chunkSize: 256, pipeline: true, inflight: k}

	// node030 sits mid-corpus; its chunk fails on every attempt while others land.
	arc := &failingSeqArc{failMarker: "node030", failing: true}
	srv := httptest.NewServer(jitter(arc.handler))
	defer srv.Close()

	cpPath := filepath.Join(t.TempDir(), "cp.db")
	cp1, _ := checkpoint.Open(cpPath)
	_, err := load(context.Background(), cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp1)
	if err == nil || !strings.Contains(err.Error(), "injected failure") {
		t.Fatalf("expected the injected mid-stream failure, got %v", err)
	}
	done, _ := cp1.IsShardDone("metrics", "1")
	committed, _ := cp1.CommittedSeq("metrics", "1")
	cp1.Close()
	if done {
		t.Fatal("shard marked done despite a failed chunk — ordered commit broken")
	}
	// The failed chunk's rows must NOT be committed: everything committed is a
	// contiguous prefix strictly before it. (Exact seq depends on chunk layout;
	// the invariant is committed < seq(node030), checked via resume delivery.)
	t.Logf("after failure: committed=%d, accepted=%d chunks", committed, arc.accepts)

	// Resume serially; the previously failing chunk now succeeds.
	arc.failMu.Lock()
	arc.failing = false
	arc.failMu.Unlock()
	cfg.inflight = 1
	cp2, _ := checkpoint.Open(cpPath)
	if _, err := load(context.Background(), cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp2); err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	cp2.Close()

	lines := arc.distinctLines()
	if len(lines) != nSeries {
		t.Fatalf("distinct points = %d, want %d (GAP after mid-flight failure)", len(lines), nSeries)
	}
	// Duplicates = accepted-but-uncommitted chunks re-sent on resume, bounded by
	// the inflight window (~6 lines per chunk at this size).
	overlap := arc.rows - nSeries
	if overlap < 0 {
		t.Fatalf("rows %d < %d — data lost", arc.rows, nSeries)
	}
	if maxDup := k * 8; overlap > maxDup {
		t.Errorf("overlap %d rows exceeds the <=%d-chunk inflight window", overlap, k)
	}
	t.Logf("resume closed the gap: %d distinct, %d total, overlap=%d (bound %d chunks)", len(lines), arc.rows, overlap, k)
}
