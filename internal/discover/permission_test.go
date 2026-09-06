package discover

import (
	"os"
	"path/filepath"
	"testing"
)

// mk1xShard creates <root>/data/<db>/autogen/1/000000001-000000001.tsm (content
// irrelevant — discovery only looks at names).
func mk1xShard(t *testing.T, root, db string) string {
	t.Helper()
	dir := filepath.Join(root, "data", db, "autogen", "1")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "000000001-000000001.tsm"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(root, "data", db)
}

// The field failure: per-db dirs 0700 root-owned under a readable <root>/data.
// The old looksLike1x accepted <root> itself (any dir with a dir-grandchild),
// so Walk ran one level too high and died opening "data/_internal" — a
// directory the run was configured to skip. Detection must resolve to
// <root>/data in both the mixed and the all-unreadable case.
func TestDetectUnreadableDbDirs(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits are ignored")
	}

	t.Run("only _internal unreadable", func(t *testing.T) {
		root := t.TempDir()
		internal := mk1xShard(t, root, "_internal")
		mk1xShard(t, root, "metrics")
		if err := os.Chmod(internal, 0o000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chmod(internal, 0o755) })

		dataDir, ver := Detect(root)
		if ver != Version1 || dataDir != filepath.Join(root, "data") {
			t.Fatalf("Detect = (%q, %v), want (%q, Version1)", dataDir, ver, filepath.Join(root, "data"))
		}
		// Walk must skip _internal BEFORE opening it and succeed on metrics.
		shards, err := Walk(dataDir, "", nil, false)
		if err != nil {
			t.Fatalf("Walk failed on an excluded unreadable db: %v", err)
		}
		if len(shards) != 1 || shards[0].Database != "metrics" {
			t.Fatalf("shards = %+v, want just metrics/1", shards)
		}
	})

	t.Run("all db dirs unreadable", func(t *testing.T) {
		root := t.TempDir()
		internal := mk1xShard(t, root, "_internal")
		metrics := mk1xShard(t, root, "metrics")
		for _, d := range []string{internal, metrics} {
			if err := os.Chmod(d, 0o000); err != nil {
				t.Fatal(err)
			}
		}
		t.Cleanup(func() {
			os.Chmod(internal, 0o755)
			os.Chmod(metrics, 0o755)
		})

		// Detection can't verify the shape but must still resolve to the
		// data-shaped level, so the walk's permission error names the REAL
		// problem (an included, unreadable db) — not data/_internal one level up.
		dataDir, ver := Detect(root)
		if ver != Version1 || dataDir != filepath.Join(root, "data") {
			t.Fatalf("Detect = (%q, %v), want (%q, Version1)", dataDir, ver, filepath.Join(root, "data"))
		}
		_, err := Walk(dataDir, "", nil, false)
		if err == nil {
			t.Fatal("Walk succeeded with unreadable included dbs — silent data loss")
		}
		if got := err.Error(); !contains(got, "metrics") || contains(got, "_internal") {
			t.Fatalf("error should name the included db, not the excluded one: %v", err)
		}
	})
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}
