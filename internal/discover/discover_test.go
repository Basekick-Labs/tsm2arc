package discover

import (
	"os"
	"path/filepath"
	"testing"
)

func touch(t *testing.T, path string, size int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, size)
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

// A shard whose data was never flushed to TSM lives only in the WAL. Discovery
// must include it when --waldir is provided, else cold-volume data is lost.
func TestWalkIncludesWALOnlyShard(t *testing.T) {
	data := t.TempDir()
	wal := t.TempDir()

	// db "m", shard 1: TSM-only
	touch(t, filepath.Join(data, "m", "autogen", "1", "x.tsm"), 100)
	// db "m", shard 2: WAL-only (no tsm, non-empty wal)
	if err := os.MkdirAll(filepath.Join(data, "m", "autogen", "2"), 0o755); err != nil {
		t.Fatal(err)
	}
	touch(t, filepath.Join(wal, "m", "autogen", "2", "_00001.wal"), 50)
	// db "m", shard 3: empty (no tsm, 0-byte wal) → must be skipped
	if err := os.MkdirAll(filepath.Join(data, "m", "autogen", "3"), 0o755); err != nil {
		t.Fatal(err)
	}
	touch(t, filepath.Join(wal, "m", "autogen", "3", "_00001.wal"), 0)

	shards, err := Walk(data, wal, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]Shard{}
	for _, s := range shards {
		got[s.ShardID] = s
	}
	if len(shards) != 2 {
		t.Fatalf("got %d shards, want 2 (TSM-only + WAL-only): %v", len(shards), got)
	}
	if s, ok := got["1"]; !ok || len(s.TSMFiles) != 1 {
		t.Errorf("shard 1 (TSM-only) missing or wrong: %+v", got["1"])
	}
	if s, ok := got["2"]; !ok || len(s.WALFiles) != 1 || len(s.TSMFiles) != 0 {
		t.Errorf("shard 2 (WAL-only) missing or wrong: %+v", got["2"])
	}
	if _, ok := got["3"]; ok {
		t.Error("shard 3 (empty 0-byte WAL) should have been skipped")
	}
}

// Without --waldir, a WAL-only shard is skipped (we have no way to read it).
func TestWalkSkipsWALOnlyShardWithoutWaldir(t *testing.T) {
	data := t.TempDir()
	if err := os.MkdirAll(filepath.Join(data, "m", "autogen", "2"), 0o755); err != nil {
		t.Fatal(err)
	}
	shards, err := Walk(data, "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(shards) != 0 {
		t.Errorf("got %d shards, want 0 (no tsm, no waldir)", len(shards))
	}
}

// Detect must recognize a 2.x engine layout from the root, the engine dir, or
// the engine/data dir, and a 1.x layout from the root or the data dir.
func TestDetectVersion(t *testing.T) {
	// 2.x: <root>/engine/data/<bucketid>/autogen/<shard>
	root2 := t.TempDir()
	bkt := filepath.Join(root2, "engine", "data", "f549d32159486e62", "autogen", "1")
	touch(t, filepath.Join(bkt, "x.tsm"), 10)
	for _, in := range []string{root2, filepath.Join(root2, "engine"), filepath.Join(root2, "engine", "data")} {
		dir, ver := Detect(in)
		if ver != Version2 {
			t.Errorf("Detect(%q) = %v, want 2.x", in, ver)
		}
		if dir != filepath.Join(root2, "engine", "data") {
			t.Errorf("Detect(%q) dir = %q, want engine/data", in, dir)
		}
	}

	// 1.x: <root>/data/<db>/<rp>/<shard>
	root1 := t.TempDir()
	touch(t, filepath.Join(root1, "data", "telemetry", "autogen", "1", "x.tsm"), 10)
	for _, in := range []string{root1, filepath.Join(root1, "data")} {
		dir, ver := Detect(in)
		if ver != Version1 {
			t.Errorf("Detect(%q) = %v, want 1.x", in, ver)
		}
		if dir != filepath.Join(root1, "data") {
			t.Errorf("Detect(%q) dir = %q, want data", in, dir)
		}
	}

	// unknown
	if _, ver := Detect(t.TempDir()); ver != VersionUnknown {
		t.Errorf("empty dir detected as %v, want unknown", ver)
	}
}

func TestIsBucketID(t *testing.T) {
	if !isBucketID("f549d32159486e62") {
		t.Error("valid bucket id rejected")
	}
	for _, bad := range []string{"telemetry", "F549D32159486E62", "f549d321", "f549d32159486e6z"} {
		if isBucketID(bad) {
			t.Errorf("%q wrongly accepted as bucket id", bad)
		}
	}
}

func TestWalkSkipsInternal(t *testing.T) {
	data := t.TempDir()
	touch(t, filepath.Join(data, "_internal", "monitor", "1", "x.tsm"), 100)
	touch(t, filepath.Join(data, "real", "autogen", "1", "x.tsm"), 100)

	shards, _ := Walk(data, "", nil, false)
	if len(shards) != 1 || shards[0].Database != "real" {
		t.Errorf("expected only 'real', got %+v", shards)
	}

	withInternal, _ := Walk(data, "", nil, true)
	if len(withInternal) != 2 {
		t.Errorf("with includeInternal, expected 2 shards, got %d", len(withInternal))
	}
}
