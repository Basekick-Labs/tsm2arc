package extract

import (
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/basekick-labs/tsm2arc/internal/tsm"
	itsm "github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// The on-disk WAL preserves client WRITE ORDER, not time order. These tests feed
// non-ascending WAL streams (backfills / late-arriving / batched mixed-ts writes
// — the tool's headline recovery case) and assert the streaming merge still
// produces correct, time-ordered, deduped output. They are the regression guard
// for review findings B1 (misorder), B2 (range-filter data loss), B3 (split
// points / dropped fields).

// B1: a single field written out of order in the WAL must emit ascending by ts.
func TestOutOfOrderWALSingleField(t *testing.T) {
	dir := t.TempDir()
	wal := realWAL(t, dir, map[string][]itsm.Value{
		// written 3000, 1000, 2000 (NOT sorted)
		"cpu,host=x#!~#v": {
			itsm.NewFloatValue(3000, 3),
			itsm.NewFloatValue(1000, 1),
			itsm.NewFloatValue(2000, 2),
		},
	})
	lines, st := collectLP(t, nil, []string{wal})
	want := []string{
		"cpu,host=x v=1 1000",
		"cpu,host=x v=2 2000",
		"cpu,host=x v=3 3000",
	}
	if st.Points != 3 {
		t.Fatalf("points=%d want 3", st.Points)
	}
	if len(lines) != 3 {
		t.Fatalf("got %d lines: %v", len(lines), lines)
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Errorf("[%d] got %q want %q (output not time-ordered — B1)", i, lines[i], want[i])
		}
	}
}

// B2: --end filter with an out-of-range value BEFORE an in-range one in write
// order must NOT drop the in-range value.
func TestOutOfOrderWALWithEndFilter(t *testing.T) {
	dir := t.TempDir()
	wal := realWAL(t, dir, map[string][]itsm.Value{
		// 9000 (out of range) written before 1000 (in range), end=5000
		"cpu,host=x#!~#v": {
			itsm.NewFloatValue(9000, 9),
			itsm.NewFloatValue(1000, 1),
		},
	})
	var lines []string
	st, err := Shard(nil, []string{wal}, openTSMFile, walReadFileForTest,
		math.MinInt64, 5000, // end = 5000
		func(p Point) { lines = append(lines, strings.TrimRight(EncodePoint(p), "\n")) })
	if err != nil {
		t.Fatal(err)
	}
	if st.Points != 1 || len(lines) != 1 || lines[0] != "cpu,host=x v=1 1000" {
		t.Fatalf("filter dropped in-range data behind an out-of-range head (B2): points=%d lines=%v", st.Points, lines)
	}
}

// B3: two fields, one written out of order, must merge into the same points
// (not split into extra points or drop a field).
func TestOutOfOrderWALMultiField(t *testing.T) {
	dir := t.TempDir()
	wal := realWAL(t, dir, map[string][]itsm.Value{
		"cpu,host=x#!~#a": {itsm.NewFloatValue(1000, 1), itsm.NewFloatValue(2000, 2)},   // ascending
		"cpu,host=x#!~#b": {itsm.NewFloatValue(2000, 20), itsm.NewFloatValue(1000, 10)}, // descending
	})
	lines, st := collectLP(t, nil, []string{wal})
	want := []string{
		"cpu,host=x a=1,b=10 1000",
		"cpu,host=x a=2,b=20 2000",
	}
	if st.Points != 2 {
		t.Fatalf("points=%d want 2 (out-of-order field split the point — B3): %v", st.Points, lines)
	}
	sort.Strings(lines)
	sort.Strings(want)
	for i := range want {
		if lines[i] != want[i] {
			t.Errorf("[%d] got %q want %q", i, lines[i], want[i])
		}
	}
}

// Intra-stream duplicate timestamps (an out-of-order WAL re-write of the same
// field@ts) must collapse to one point, last value winning — not emit two points
// at the same timestamp.
func TestOutOfOrderWALDuplicateTimestamp(t *testing.T) {
	dir := t.TempDir()
	wal := realWAL(t, dir, map[string][]itsm.Value{
		"cpu,host=x#!~#v": {
			itsm.NewFloatValue(1000, 1),
			itsm.NewFloatValue(1000, 99), // re-write of same ts, later → wins
		},
	})
	lines, st := collectLP(t, nil, []string{wal})
	if st.Points != 1 || len(lines) != 1 {
		t.Fatalf("duplicate ts not collapsed: points=%d lines=%v", st.Points, lines)
	}
	if lines[0] != "cpu,host=x v=99 1000" {
		t.Errorf("got %q, want last-write-wins v=99", lines[0])
	}
}

// H1/File() path: File() shares mergeSeries, so it must also handle a
// non-ascending single stream correctly.
func TestFilePathOutOfOrder(t *testing.T) {
	r := &fakeReader{data: map[string][]tsm.Value{
		"m,t=x#!~#v": {
			{UnixNano: 3000, Type: tsm.BlockFloat, Float: 3},
			{UnixNano: 1000, Type: tsm.BlockFloat, Float: 1},
			{UnixNano: 2000, Type: tsm.BlockFloat, Float: 2},
		},
	}}
	var lines []string
	_, err := File(r, math.MinInt64, math.MaxInt64,
		func(p Point) { lines = append(lines, strings.TrimRight(EncodePoint(p), "\n")) })
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"m,t=x v=1 1000", "m,t=x v=2 2000", "m,t=x v=3 3000"}
	if len(lines) != 3 {
		t.Fatalf("got %v", lines)
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Errorf("[%d] got %q want %q (File path not time-ordered)", i, lines[i], want[i])
		}
	}
}

// All values below --start (after sort) must yield zero points without hanging
// or miscounting.
func TestAllValuesBelowStart(t *testing.T) {
	dir := t.TempDir()
	wal := realWAL(t, dir, map[string][]itsm.Value{
		"cpu,host=x#!~#v": {itsm.NewFloatValue(100, 1), itsm.NewFloatValue(50, 2)},
	})
	var n int
	st, err := Shard(nil, []string{wal}, openTSMFile, walReadFileForTest,
		1000, math.MaxInt64, func(p Point) { n++ })
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 || st.Points != 0 {
		t.Fatalf("expected 0 points for all-below-start, got n=%d points=%d", n, st.Points)
	}
}
