package extract

import (
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/golang/snappy"

	"github.com/basekick-labs/tsm2arc/internal/tsm"
	"github.com/basekick-labs/tsm2arc/internal/wal"
	itsm "github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// walReadFileForTest adapts wal.ReadFile to the extract.WALReader signature.
func walReadFileForTest(path string, fn func(key string, vals []tsm.Value)) error {
	return wal.ReadFile(path, fn)
}

// realTSM writes a TSM file and returns its path.
func realTSM(t *testing.T, dir string, entries map[string]itsm.Values) string {
	t.Helper()
	path := filepath.Join(dir, "000000001-000000001.tsm")
	f, _ := os.Create(path)
	w, _ := itsm.NewTSMWriter(f)
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := w.Write([]byte(k), entries[k]); err != nil {
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

// realWAL writes a .wal segment and returns its path.
func realWAL(t *testing.T, dir string, entries map[string][]itsm.Value) string {
	t.Helper()
	path := filepath.Join(dir, "_00001.wal")
	f, _ := os.Create(path)
	w := itsm.NewWALSegmentWriter(f)
	entry := &itsm.WriteWALEntry{Values: entries}
	b, _ := entry.MarshalBinary()
	if err := w.Write(entry.Type(), snappy.Encode(nil, b)); err != nil {
		t.Fatal(err)
	}
	w.Flush()
	f.Close()
	return path
}

// openTSMFile adapts tsm.Open to the extract.OpenTSM signature for the test.
func openTSMFile(path string) (TSMFile, error) { return tsm.Open(path) }

// walReader adapts wal.ReadFile without importing the wal package (avoid any
// cycle in tests) — re-decode inline via a tiny shim that calls our own reader.
// We import the real wal package here since tests can.
func TestShardMergesTSMAndWAL(t *testing.T) {
	dir := t.TempDir()

	// TSM has cpu (2 fields) — the compacted data.
	tsmPath := realTSM(t, dir, map[string]itsm.Values{
		"cpu,host=a#!~#usage": {itsm.NewFloatValue(1000, 98.5)},
		"cpu,host=a#!~#cores": {itsm.NewIntegerValue(1000, 8)},
	})
	// WAL has status (3 fields) — never-flushed data, plus a NEW cpu point.
	walPath := realWAL(t, dir, map[string][]itsm.Value{
		"status,host=a#!~#healthy": {itsm.NewBooleanValue(1000, true)},
		"status,host=a#!~#note":    {itsm.NewStringValue(1000, "ok")},
		"cpu,host=a#!~#usage":      {itsm.NewFloatValue(2000, 50.0)}, // new ts for cpu
	})

	var lines []string
	st, err := Shard(
		[]string{tsmPath}, []string{walPath},
		openTSMFile, walReadFileForTest,
		math.MinInt64, math.MaxInt64,
		func(p Point) { lines = append(lines, strings.TrimRight(EncodePoint(p), "\n")) },
	)
	if err != nil {
		t.Fatal(err)
	}

	// Expected reconstructed points:
	//   cpu,host=a cores=8i,usage=98.5 1000   (TSM)
	//   cpu,host=a usage=50            2000   (WAL — cpu at a new ts, only usage)
	//   status,host=a healthy=true,note="ok" 1000   (WAL)
	want := []string{
		"cpu,host=a cores=8i,usage=98.5 1000",
		"cpu,host=a usage=50 2000",
		`status,host=a healthy=true,note="ok" 1000`,
	}
	sort.Strings(lines)
	sort.Strings(want)
	if len(lines) != len(want) {
		t.Fatalf("got %d points, want %d:\n%v", len(lines), len(want), lines)
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Errorf("[%d]\n got %q\nwant %q", i, lines[i], want[i])
		}
	}
	if st.Points != 3 {
		t.Errorf("stats points = %d, want 3", st.Points)
	}
}

// TestShardWALOverridesTSMSameTimestamp verifies last-write-wins when the same
// (series, field, ts) exists in both TSM and WAL (partial compaction). WAL is
// applied after TSM, so the WAL value wins.
func TestShardWALOverridesTSMSameTimestamp(t *testing.T) {
	dir := t.TempDir()
	tsmPath := realTSM(t, dir, map[string]itsm.Values{
		"m,t=x#!~#v": {itsm.NewFloatValue(1000, 1.0)},
	})
	walPath := realWAL(t, dir, map[string][]itsm.Value{
		"m,t=x#!~#v": {itsm.NewFloatValue(1000, 999.0)}, // same ts, newer value
	})

	var lines []string
	_, err := Shard([]string{tsmPath}, []string{walPath}, openTSMFile, walReadFileForTest,
		math.MinInt64, math.MaxInt64,
		func(p Point) { lines = append(lines, strings.TrimRight(EncodePoint(p), "\n")) })
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 1 {
		t.Fatalf("got %d points, want 1 (dedup): %v", len(lines), lines)
	}
	if lines[0] != "m,t=x v=999 1000" {
		t.Errorf("last-write-wins failed: got %q, want WAL value 999", lines[0])
	}
}
