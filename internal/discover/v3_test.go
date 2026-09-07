package discover

import (
	"os"
	"path/filepath"
	"testing"
)

// mkV3Store lays out a minimal InfluxDB 3 object store on local disk:
// {node_id}/{wal,snapshots,dbs}/... plus a catalog dir.
func mkV3Store(t *testing.T, root, node string) {
	t.Helper()
	for _, d := range []string{
		filepath.Join(root, node, "wal"),
		filepath.Join(root, node, "snapshots"),
		filepath.Join(root, node, "dbs", "0", "0", "2026-01-01", "00-00"),
		filepath.Join(root, node, "catalog", "v2", "logs"),
	} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	os.WriteFile(filepath.Join(root, node, "wal", "00000000001.wal"), []byte("idb3.001"), 0o644)
}

// A v3 object store must detect as Version3 — pointed at the store root OR
// directly at the node prefix — and must NOT fall through to the 1.x probe
// (node/dbs/<numeric>/... parses as a plausible db/rp/shard chain, which is
// exactly the mis-detection this ordering prevents).
func TestDetectV3(t *testing.T) {
	root := t.TempDir()
	mkV3Store(t, root, "my-node-01")

	dataDir, ver := Detect(root)
	if ver != Version3 || dataDir != root {
		t.Fatalf("Detect(root) = (%q, %v), want (%q, Version3)", dataDir, ver, root)
	}

	node := filepath.Join(root, "my-node-01")
	dataDir, ver = Detect(node)
	if ver != Version3 || dataDir != node {
		t.Fatalf("Detect(node) = (%q, %v), want (%q, Version3)", dataDir, ver, node)
	}
}

// A 1.x tree must still detect as 1.x: a single v3-ish marker name (e.g. a db
// literally called "wal") must not flip detection.
func TestDetectV3NoFalsePositiveOn1x(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{
		filepath.Join(root, "data", "metrics", "autogen", "1"),
		filepath.Join(root, "data", "wal", "autogen", "2"), // a db named "wal"
	} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	_, ver := Detect(root)
	if ver != Version1 {
		t.Fatalf("1.x tree with a db named %q detected as %v, want Version1", "wal", ver)
	}
}
