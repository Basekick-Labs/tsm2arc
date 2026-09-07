package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/basekick-labs/tsm2arc/internal/discover"
)

// mkdirs creates a directory tree under a fresh temp root.
func mkdirs(t *testing.T, dirs ...string) string {
	t.Helper()
	root := t.TempDir()
	for _, d := range dirs {
		if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(d)), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// The layouts here mirror REAL stores observed on live servers: Core 3.x
// (node prefix holds everything), Enterprise Parquet (catalog under the
// CLUSTER prefix, compactor state under the node), and the Pacha-tree
// engine (cv2/ cluster artifacts, hashed .pt data prefixes, no parquet node).

func TestV3NodePrefixCore(t *testing.T) {
	root := mkdirs(t, "node0/wal", "node0/snapshots", "node0/dbs")
	nroot, node, cluster, err := v3NodePrefix(root)
	if err != nil {
		t.Fatal(err)
	}
	if nroot != root || node != "node0" || cluster != "" {
		t.Fatalf("got (%q, %q, %q)", nroot, node, cluster)
	}
}

func TestV3NodePrefixEnterpriseCluster(t *testing.T) {
	root := mkdirs(t, "ent10/wal", "ent10/snapshots", "ent10/dbs", "ent10/cs", "cl02/catalog/v3", "cl02/enterprise")
	_, node, cluster, err := v3NodePrefix(root)
	if err != nil {
		t.Fatal(err)
	}
	if node != "ent10" || cluster != "cl02" {
		t.Fatalf("got node %q cluster %q, want ent10/cl02", node, cluster)
	}
}

func TestV3NodePrefixPacha(t *testing.T) {
	root := mkdirs(t, "cl01/cv2", "cl01/catalog/v3", "cl01/enterprise", "b2c/0/pt_wal")
	_, _, _, err := v3NodePrefix(root)
	if err == nil || !strings.Contains(err.Error(), "Pacha-tree") {
		t.Fatalf("err = %v, want Pacha-tree explanation", err)
	}
	// And discovery must classify the tree as v3 so the message is reachable.
	_, ver := discover.Detect(root)
	if ver != discover.Version3 {
		t.Fatalf("Detect = %v, want Version3", ver)
	}
}

func TestV3NodePrefixMultiNode(t *testing.T) {
	root := mkdirs(t, "n1/wal", "n1/snapshots", "n2/wal", "n2/snapshots")
	_, _, _, err := v3NodePrefix(root)
	if err == nil || !strings.Contains(err.Error(), "multi-node") {
		t.Fatalf("err = %v, want multi-node refusal", err)
	}
}

func TestSetupV3RefusesCompactorState(t *testing.T) {
	root := mkdirs(t, "ent10/wal", "ent10/snapshots", "ent10/dbs", "ent10/cd/1/0", "cl02/catalog/v3")
	_, _, err := setupV3(root, v3Options{}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "COMPACTOR") {
		t.Fatalf("err = %v, want compactor refusal", err)
	}
	// --analyze reports instead of refusing: it must get PAST the compactor
	// gate (and then fail later on the empty store, which is fine here).
	_, _, err = setupV3(root, v3Options{analyze: true}, io.Discard)
	if err != nil && strings.Contains(err.Error(), "COMPACTOR") {
		t.Fatalf("analyze mode must not refuse on compactor state, got %v", err)
	}
}
