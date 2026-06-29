package tsm

import (
	"os"
	"path/filepath"
	"testing"

	itsm "github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// TestReadRealTSMFile writes a real TSM file with influxdata's TSMWriter, then
// reads it back with our Reader and asserts keys + decoded values. This
// exercises the full file path: header, index, footer, block offsets, CRC, and
// every codec — against the exact writer the customer's InfluxDB used.
func TestReadRealTSMFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "000000001-000000001.tsm")

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := itsm.NewTSMWriter(f)
	if err != nil {
		t.Fatal(err)
	}

	// Two fields of one series (cpu,host=node-a) — separate keys, shared ts.
	// Keys MUST be written in sorted order.
	type kv struct {
		key  string
		vals itsm.Values
	}
	entries := []kv{
		{"cpu,host=node-a#!~#cores", itsm.Values{
			itsm.NewIntegerValue(1700000000000000000, 8),
			itsm.NewIntegerValue(1700000001000000000, 8),
		}},
		{"cpu,host=node-a#!~#usage", itsm.Values{
			itsm.NewFloatValue(1700000000000000000, 98.5),
			itsm.NewFloatValue(1700000001000000000, 97.2),
		}},
		{"legacy,host=mainframe#!~#value", itsm.Values{
			itsm.NewFloatValue(-317001600000000000, 42.0), // pre-epoch
		}},
		{"status,host=node-a#!~#healthy", itsm.Values{
			itsm.NewBooleanValue(1700000000000000000, true),
		}},
		{"status,host=node-a#!~#note", itsm.Values{
			itsm.NewStringValue(1700000000000000000, "all systems nominal"),
		}},
	}
	for _, e := range entries {
		if err := w.Write([]byte(e.key), e.vals); err != nil {
			t.Fatalf("write %s: %v", e.key, err)
		}
	}
	if err := w.WriteIndex(); err != nil {
		t.Fatal(err)
	}
	w.Close()
	f.Close()

	// Read back with our Reader.
	r, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	gotKeys := r.Keys()
	if len(gotKeys) != len(entries) {
		t.Fatalf("keys: got %d want %d (%v)", len(gotKeys), len(entries), gotKeys)
	}

	// spot-check a float key
	vals, err := r.ReadKeyByName("cpu,host=node-a#!~#usage")
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 2 || vals[0].Float != 98.5 || vals[1].Float != 97.2 {
		t.Errorf("usage values wrong: %+v", vals)
	}
	if vals[0].UnixNano != 1700000000000000000 {
		t.Errorf("usage ts wrong: %d", vals[0].UnixNano)
	}

	// pre-epoch float
	leg, _ := r.ReadKeyByName("legacy,host=mainframe#!~#value")
	if len(leg) != 1 || leg[0].UnixNano != -317001600000000000 || leg[0].Float != 42.0 {
		t.Errorf("pre-epoch wrong: %+v", leg)
	}

	// boolean + string
	hb, _ := r.ReadKeyByName("status,host=node-a#!~#healthy")
	if len(hb) != 1 || hb[0].Boolean != true {
		t.Errorf("bool wrong: %+v", hb)
	}
	note, _ := r.ReadKeyByName("status,host=node-a#!~#note")
	if len(note) != 1 || note[0].String != "all systems nominal" {
		t.Errorf("string wrong: %+v", note)
	}
}
