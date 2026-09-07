package vfs

import (
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func writeTree(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for p, content := range files {
		abs := filepath.Join(root, filepath.FromSlash(p))
		if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(abs, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLocalListSortedRecursive(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{
		"node/wal/00000000002.wal":                         "b",
		"node/wal/00000000001.wal":                         "a",
		"node/snapshots/x.info.json":                       "{}",
		"node/dbs/0/0/2026-01-01/00-00/0000000001.parquet": "p",
	})
	f := NewLocal(root)

	got, err := f.List("node/wal")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"node/wal/00000000001.wal", "node/wal/00000000002.wal"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("List = %v, want %v (sorted)", got, want)
	}

	all, err := f.List("node")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 4 {
		t.Fatalf("recursive List found %d objects, want 4: %v", len(all), all)
	}
}

func TestLocalListMissingPrefixIsEmpty(t *testing.T) {
	f := NewLocal(t.TempDir())
	got, err := f.List("does/not/exist")
	if err != nil || got != nil {
		t.Fatalf("missing prefix: got (%v, %v), want (nil, nil) — object-store semantics", got, err)
	}
}

func TestLocalReadFileAndReaderAt(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{"a/b.json": "hello world"})
	f := NewLocal(root)

	b, err := f.ReadFile("a/b.json")
	if err != nil || string(b) != "hello world" {
		t.Fatalf("ReadFile = (%q, %v)", b, err)
	}

	ra, size, err := f.ReaderAt("a/b.json")
	if err != nil {
		t.Fatal(err)
	}
	defer ra.Close()
	if size != 11 {
		t.Fatalf("size = %d, want 11", size)
	}
	buf := make([]byte, 5)
	if _, err := ra.ReadAt(buf, 6); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if string(buf) != "world" {
		t.Fatalf("range read = %q, want %q", buf, "world")
	}
}

func TestHasPrefix(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, map[string]string{"node/wal/1.wal": "x"})
	f := NewLocal(root)
	if !HasPrefix(f, "node/wal") {
		t.Error("existing prefix reported absent")
	}
	if HasPrefix(f, "node/nope") {
		t.Error("missing prefix reported present")
	}
}
