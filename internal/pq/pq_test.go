package pq

import (
	"context"
	"os"
	"reflect"
	"testing"
)

// The fixtures are real parquet data files written by InfluxDB 3 Core 3.0.3
// (v30-*), 3.4.2 (v34-*), and 3.11.2 (v311-*) — one from every format era,
// which is the spike this package's first commit was required to be.

func openFixture(t *testing.T, name string) *File {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	f, err := Open(bytesReaderAt(b), int64(len(b)))
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

type bytesReaderAt []byte

func (b bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n := copy(p, b[off:])
	return n, nil
}

func TestSchemaClassificationAllEras(t *testing.T) {
	wantCPU := []Column{
		{"host", KindTag}, {"region", KindTag},
		{"usage", KindFieldFloat}, {"load", KindFieldInt},
		{"time", KindTime},
	}
	for _, name := range []string{"v30-cpu.parquet", "v34-cpu.parquet", "v311-cpu.parquet"} {
		f := openFixture(t, name)
		if got := f.Schema().Columns; !reflect.DeepEqual(got, wantCPU) {
			t.Errorf("%s schema = %v, want %v", name, got, wantCPU)
		}
		if got := f.Schema().Tags(); !reflect.DeepEqual(got, []string{"host", "region"}) {
			t.Errorf("%s tags = %v", name, got)
		}
	}

	// Same table, later file, after schema evolution: different column ORDER.
	// Classification must be per-file.
	f := openFixture(t, "v311-cpu-nulltag.parquet")
	got := map[string]ColumnKind{}
	for _, c := range f.Schema().Columns {
		got[c.Name] = c.Kind
	}
	want := map[string]ColumnKind{"host": KindTag, "region": KindTag, "usage": KindFieldFloat, "load": KindFieldInt, "time": KindTime}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("nulltag schema = %v, want %v", got, want)
	}
}

func TestReadAllValuesAndOrder(t *testing.T) {
	f := openFixture(t, "v30-cpu.parquet")
	if f.NumRows() != 5 {
		t.Fatalf("NumRows = %d, want 5", f.NumRows())
	}
	var hosts []string
	var usage []float64
	var times []int64
	err := f.ReadAll(context.Background(), func(b *Batch) error {
		var host, us, tm *BatchColumn
		for i := range b.Columns {
			switch b.Columns[i].Name {
			case "host":
				host = &b.Columns[i]
			case "usage":
				us = &b.Columns[i]
			case "time":
				tm = &b.Columns[i]
			}
		}
		for r := 0; r < b.N; r++ {
			if !host.Valid[r] || !us.Valid[r] || !tm.Valid[r] {
				t.Fatal("unexpected NULL in fully populated file")
			}
			hosts = append(hosts, host.Strings[r])
			usage = append(usage, us.Floats[r])
			times = append(times, tm.Ints[r])
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(hosts, []string{"web-0", "web-1", "web-2", "web-3", "web-4"}) {
		t.Fatalf("hosts = %v (dictionary decode broken?)", hosts)
	}
	if !reflect.DeepEqual(usage, []float64{10.5, 11.5, 12.5, 13.5, 14.5}) {
		t.Fatalf("usage = %v", usage)
	}
	for _, ts := range times {
		if ts < 1e18 {
			t.Fatalf("time %d not in nanoseconds", ts)
		}
	}
}

// Every fixture must stream exactly footer-many rows, sorted by (series-key
// columns in file order, NULLS FIRST, then time) — the in-file invariant the
// cross-file merge in the extractor will rely on.
func TestRowCountAndSortInvariantAllFixtures(t *testing.T) {
	names, err := os.ReadDir("testdata")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range names {
		f := openFixture(t, e.Name())
		s := f.Schema()
		var tagIdx []int
		for i, c := range s.Columns {
			if c.Kind == KindTag {
				tagIdx = append(tagIdx, i)
			}
		}
		var rows int64
		var prev []string
		var prevTime int64
		havePrev := false
		err := f.ReadAll(context.Background(), func(b *Batch) error {
			for r := 0; r < b.N; r++ {
				rows++
				key := make([]string, len(tagIdx))
				for j, i := range tagIdx {
					if b.Columns[i].Valid[r] {
						key[j] = "\x01" + b.Columns[i].Strings[r] // NULL ("") sorts first
					}
				}
				ts := b.Columns[s.TimeIndex].Ints[r]
				if havePrev {
					c := compareKeys(prev, key)
					if c > 0 || (c == 0 && ts < prevTime) {
						t.Fatalf("%s: rows out of (series key, time) order at row %d: %v@%d after %v@%d",
							e.Name(), rows, key, ts, prev, prevTime)
					}
					if c == 0 && ts == prevTime {
						t.Fatalf("%s: duplicate (series key, time) within one file at row %d", e.Name(), rows)
					}
				}
				prev, prevTime, havePrev = key, ts, true
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if rows != f.NumRows() {
			t.Fatalf("%s: streamed %d rows, footer says %d", e.Name(), rows, f.NumRows())
		}
	}
}

func compareKeys(a, b []string) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return len(a) - len(b)
}

func TestNullTags(t *testing.T) {
	f := openFixture(t, "v311-cpu-nulltag.parquet")
	sawNullRegion := 0
	var regionCol, hostCol *BatchColumn
	err := f.ReadAll(context.Background(), func(b *Batch) error {
		for i := range b.Columns {
			switch b.Columns[i].Name {
			case "region":
				regionCol = &b.Columns[i]
			case "host":
				hostCol = &b.Columns[i]
			}
		}
		for r := 0; r < b.N; r++ {
			if !hostCol.Valid[r] {
				t.Fatal("host must always be set in this fixture")
			}
			if !regionCol.Valid[r] {
				sawNullRegion++
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if sawNullRegion == 0 {
		t.Fatal("fixture should contain rows with NULL region (written without that tag)")
	}
}
