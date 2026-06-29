package wal

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/golang/snappy"

	"github.com/basekick-labs/tsm2arc/internal/tsm"
	itsm "github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// writeRealWAL builds a real .wal segment with influxdata's WAL writer from the
// given entries (key → values), so we validate our decoder against the exact
// format InfluxDB wrote.
func writeRealWAL(t *testing.T, path string, entries map[string][]itsm.Value) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := itsm.NewWALSegmentWriter(f)
	entry := &itsm.WriteWALEntry{Values: entries}
	b, err := entry.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	compressed := snappy.Encode(nil, b)
	if err := w.Write(entry.Type(), compressed); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	f.Close()
}

func TestReadWALAllTypes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "_00001.wal")

	entries := map[string][]itsm.Value{
		"cpu,host=a#!~#usage": {
			itsm.NewFloatValue(1700000000000000000, 98.5),
			itsm.NewFloatValue(1700000001000000000, 97.2),
		},
		"cpu,host=a#!~#cores": {
			itsm.NewIntegerValue(1700000000000000000, 8),
		},
		"status,host=a#!~#reboots": {
			itsm.NewUnsignedValue(1700000000000000000, 3),
		},
		"status,host=a#!~#healthy": {
			itsm.NewBooleanValue(1700000000000000000, true),
		},
		"status,host=a#!~#note": {
			itsm.NewStringValue(1700000000000000000, "all systems nominal"),
		},
		"legacy,host=mainframe#!~#value": {
			itsm.NewFloatValue(-317001600000000000, 42.0), // pre-epoch
		},
	}
	writeRealWAL(t, path, entries)

	got := map[string][]tsm.Value{}
	if err := ReadFile(path, func(key string, vals []tsm.Value) {
		got[key] = append(got[key], vals...)
	}); err != nil {
		t.Fatal(err)
	}

	if len(got) != len(entries) {
		t.Fatalf("got %d keys, want %d: %v", len(got), len(entries), keysOf(got))
	}

	// float
	u := got["cpu,host=a#!~#usage"]
	if len(u) != 2 || u[0].Float != 98.5 || u[1].Float != 97.2 || u[0].UnixNano != 1700000000000000000 {
		t.Errorf("usage wrong: %+v", u)
	}
	// integer
	if c := got["cpu,host=a#!~#cores"]; len(c) != 1 || c[0].Integer != 8 || c[0].Type != tsm.BlockInteger {
		t.Errorf("cores wrong: %+v", c)
	}
	// unsigned
	if r := got["status,host=a#!~#reboots"]; len(r) != 1 || r[0].Unsigned != 3 || r[0].Type != tsm.BlockUnsigned {
		t.Errorf("reboots wrong: %+v", r)
	}
	// boolean
	if h := got["status,host=a#!~#healthy"]; len(h) != 1 || h[0].Boolean != true {
		t.Errorf("healthy wrong: %+v", h)
	}
	// string
	if n := got["status,host=a#!~#note"]; len(n) != 1 || n[0].String != "all systems nominal" {
		t.Errorf("note wrong: %+v", n)
	}
	// pre-epoch float
	if l := got["legacy,host=mainframe#!~#value"]; len(l) != 1 || l[0].UnixNano != -317001600000000000 || l[0].Float != 42.0 {
		t.Errorf("legacy wrong: %+v", l)
	}
}

// Multiple write entries in one segment must all be read.
func TestReadWALMultipleEntries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "_00002.wal")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := itsm.NewWALSegmentWriter(f)
	for i := 0; i < 5; i++ {
		entry := &itsm.WriteWALEntry{Values: map[string][]itsm.Value{
			"m,t=x#!~#v": {itsm.NewFloatValue(int64(i+1)*1e9, float64(i))},
		}}
		b, _ := entry.MarshalBinary()
		if err := w.Write(entry.Type(), snappy.Encode(nil, b)); err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()
	f.Close()

	var count int
	if err := ReadFile(path, func(key string, vals []tsm.Value) {
		count += len(vals)
	}); err != nil {
		t.Fatal(err)
	}
	if count != 5 {
		t.Errorf("read %d values across entries, want 5", count)
	}
}

// A truncated tail (simulating a WAL never cleanly closed) must not error; all
// complete entries before the truncation are returned.
func TestReadWALTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "_00003.wal")
	writeRealWAL(t, path, map[string][]itsm.Value{
		"m,t=x#!~#v": {itsm.NewFloatValue(1e9, 1.0), itsm.NewFloatValue(2e9, 2.0)},
	})

	// append a bogus partial header (1 byte type + partial length) to simulate a
	// torn write at the tail
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write([]byte{0x01, 0x00, 0x00}) // type + 2/4 length bytes → truncated header
	f.Close()

	var count int
	if err := ReadFile(path, func(key string, vals []tsm.Value) { count += len(vals) }); err != nil {
		t.Fatalf("truncated tail should not error, got %v", err)
	}
	if count != 2 {
		t.Errorf("read %d values, want 2 (complete entry before torn tail)", count)
	}
}

func keysOf(m map[string][]tsm.Value) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
