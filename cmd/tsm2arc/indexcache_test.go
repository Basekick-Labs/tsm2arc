package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The cache must parse each file once and serve later reopens from the cached
// index — the whole point is removing the per-(series,run) reparse cost.
func TestIndexCacheParsesOnce(t *testing.T) {
	datadir := writeTSMShard(t, "metrics", 5)
	tsmPath := filepath.Join(datadir, "metrics", "autogen", "1", "000000001-000000001.tsm")

	c := newIndexCache(1 << 30)
	for i := 0; i < 4; i++ {
		r, err := c.open(tsmPath)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		if len(r.Keys()) != 5 {
			t.Fatalf("open %d: keys = %d, want 5", i, len(r.Keys()))
		}
		r.Close()
	}
	if c.parses != 1 || c.hits != 3 {
		t.Errorf("parses=%d hits=%d, want 1/3", c.parses, c.hits)
	}
}

// A budget too small to cache anything must degrade gracefully to a full parse
// per open — never an error, never a partial cache inconsistency.
func TestIndexCacheBudgetFallback(t *testing.T) {
	datadir := writeTSMShard(t, "metrics", 5)
	tsmPath := filepath.Join(datadir, "metrics", "autogen", "1", "000000001-000000001.tsm")

	c := newIndexCache(1) // nothing fits
	for i := 0; i < 3; i++ {
		r, err := c.open(tsmPath)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		r.Close()
	}
	if c.parses != 3 || c.hits != 0 || len(c.idx) != 0 {
		t.Errorf("parses=%d hits=%d cached=%d, want 3/0/0", c.parses, c.hits, len(c.idx))
	}
}

// A source file that changes mid-shard must fail loudly on the next cached
// reopen, not silently read against a stale index.
func TestIndexCacheDetectsChangedFile(t *testing.T) {
	datadir := writeTSMShard(t, "metrics", 5)
	tsmPath := filepath.Join(datadir, "metrics", "autogen", "1", "000000001-000000001.tsm")

	c := newIndexCache(1 << 30)
	r, err := c.open(tsmPath)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()

	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(tsmPath, future, future); err != nil {
		t.Fatal(err)
	}
	if _, err := c.open(tsmPath); err == nil {
		t.Fatal("cached reopen accepted a changed file")
	}
}
