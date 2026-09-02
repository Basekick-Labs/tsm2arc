package tsm

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	itsm "github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// writeIndexFixture writes a small real TSM file and returns its path.
func writeIndexFixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "000000001-000000001.tsm")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := itsm.NewTSMWriter(f)
	if err != nil {
		t.Fatal(err)
	}
	// Keys must be written in sorted order (TSM writer requirement).
	for _, k := range []string{"cpu,host=a#!~#cores", "cpu,host=a#!~#usage", "mem,host=a#!~#free"} {
		if err := w.Write([]byte(k), itsm.Values{
			itsm.NewFloatValue(1700000000000000000, 1.5),
			itsm.NewFloatValue(1700000001000000000, 2.5),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.WriteIndex(); err != nil {
		t.Fatal(err)
	}
	w.Close()
	f.Close()
	return path
}

// OpenWithIndex must behave identically to Open — same keys, same blocks, same
// decoded values — while skipping the parse.
func TestOpenWithIndexMatchesOpen(t *testing.T) {
	path := writeIndexFixture(t)

	r1, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ix := r1.Index()
	if ix.ApproxBytes() <= 0 {
		t.Errorf("ApproxBytes = %d, want > 0", ix.ApproxBytes())
	}
	keys1 := r1.Keys()
	vals1, err := r1.ReadKeyByName("cpu,host=a#!~#usage")
	if err != nil {
		t.Fatal(err)
	}
	r1.Close()

	// Reopen with the cached index — Index stays valid after Close.
	r2, err := OpenWithIndex(path, ix)
	if err != nil {
		t.Fatalf("OpenWithIndex: %v", err)
	}
	defer r2.Close()
	if !reflect.DeepEqual(keys1, r2.Keys()) {
		t.Errorf("keys differ: %v vs %v", keys1, r2.Keys())
	}
	vals2, err := r2.ReadKeyByName("cpu,host=a#!~#usage")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(vals1, vals2) {
		t.Errorf("values differ across cached reopen")
	}
	if len(r2.Blocks("cpu,host=a#!~#cores")) == 0 {
		t.Error("Blocks empty on cached reopen")
	}
}

// A file that changed since its index was parsed must be rejected loudly —
// extraction order (and resume) is only meaningful against unchanged source.
func TestOpenWithIndexRejectsChangedFile(t *testing.T) {
	path := writeIndexFixture(t)
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ix := r.Index()
	r.Close()

	// Same size, different mtime.
	future := time.Now().Add(2 * time.Second)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenWithIndex(path, ix); err == nil {
		t.Fatal("OpenWithIndex accepted a file with changed mtime")
	}

	// Different size.
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{0})
	f.Close()
	if _, err := OpenWithIndex(path, ix); err == nil {
		t.Fatal("OpenWithIndex accepted a file with changed size")
	}
}
