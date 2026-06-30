package extract

import (
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	itsm "github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// collectLP runs Shard and returns the emitted LP lines in emission order.
func collectLP(t *testing.T, tsmFiles, walFiles []string) ([]string, Stats) {
	t.Helper()
	var lines []string
	st, err := Shard(tsmFiles, walFiles, openTSMFile, walReadFileForTest,
		math.MinInt64, math.MaxInt64,
		func(p Point) { lines = append(lines, strings.TrimRight(EncodePoint(p), "\n")) })
	if err != nil {
		t.Fatal(err)
	}
	return lines, st
}

// Determinism is load-bearing for crash-safe resume: re-running a shard must
// produce byte-identical output in the same order, regardless of map iteration.
func TestShardDeterministicAcrossRuns(t *testing.T) {
	dir := t.TempDir()
	// 50 series × 3 fields, shuffled write order, across TWO tsm files.
	f1 := realTSM(t, mkDir(t, dir, "f1"), genSeries(0, 50, []string{"a", "b", "c"}, 1000))
	f2 := realTSM(t, mkDir(t, dir, "f2"), genSeries(25, 75, []string{"a", "b", "c"}, 2000)) // overlapping series range, later ts

	run1, st1 := collectLP(t, []string{f1, f2}, nil)
	run2, st2 := collectLP(t, []string{f1, f2}, nil)

	if len(run1) != len(run2) || st1.Points != st2.Points {
		t.Fatalf("run length differs: %d vs %d", len(run1), len(run2))
	}
	for i := range run1 {
		if run1[i] != run2[i] {
			t.Fatalf("line %d differs between runs:\n %q\n %q", i, run1[i], run2[i])
		}
	}

	// Within each series, timestamps must be strictly ascending in emission order
	// (the deterministic order resume relies on). Group consecutive lines by their
	// "measurement,tags " prefix and check the trailing timestamp increases.
	assertPerSeriesAscending(t, run1)
}

// assertPerSeriesAscending checks that, for runs of LP lines sharing the same
// "measurement,tags" prefix, the trailing nanosecond timestamp is strictly
// increasing — i.e. each series is emitted time-ordered with no duplicate ts.
func assertPerSeriesAscending(t *testing.T, lines []string) {
	t.Helper()
	prevPrefix, prevTS := "", int64(math.MinInt64)
	for _, ln := range lines {
		sp := strings.LastIndexByte(ln, ' ')
		fp := strings.IndexByte(ln, ' ')
		if sp < 0 || fp < 0 {
			t.Fatalf("malformed LP line: %q", ln)
		}
		prefix := ln[:fp] // measurement,tags
		var ts int64
		for _, c := range ln[sp+1:] {
			if c == '-' {
				continue
			}
			ts = ts*10 + int64(c-'0')
		}
		if ln[sp+1] == '-' {
			ts = -ts
		}
		if prefix == prevPrefix && ts <= prevTS {
			t.Fatalf("series %q not strictly ascending: ts %d after %d", prefix, ts, prevTS)
		}
		prevPrefix, prevTS = prefix, ts
	}
}

// A series split across multiple TSM files must be merged (not duplicated), with
// later-file values winning on a same-(ts,field) tie.
func TestShardSeriesSplitAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	// same series cpu,host=x with field v; file1 ts=1000 val=1, file2 ts=1000 val=9 (tie) + ts=2000 val=2
	f1 := realTSM(t, mkDir(t, dir, "f1"), map[string]itsm.Values{
		"cpu,host=x#!~#v": {itsm.NewFloatValue(1000, 1)},
	})
	f2 := realTSM(t, mkDir(t, dir, "f2"), map[string]itsm.Values{
		"cpu,host=x#!~#v": {itsm.NewFloatValue(1000, 9), itsm.NewFloatValue(2000, 2)},
	})
	lines, st := collectLP(t, []string{f1, f2}, nil)

	// Two distinct timestamps → two points (1000 deduped to one).
	if st.Points != 2 {
		t.Fatalf("points=%d want 2 (ts 1000 must dedupe across files)", st.Points)
	}
	want := []string{
		"cpu,host=x v=9 1000", // later file wins the tie
		"cpu,host=x v=2 2000",
	}
	sort.Strings(lines)
	sort.Strings(want)
	for i := range want {
		if lines[i] != want[i] {
			t.Errorf("[%d] got %q want %q", i, lines[i], want[i])
		}
	}
}

// helpers

func mkDir(t *testing.T, base, sub string) string {
	t.Helper()
	d := filepath.Join(base, sub)
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	return d
}

func genSeries(lo, hi int, fields []string, baseTS int64) map[string]itsm.Values {
	m := map[string]itsm.Values{}
	for s := lo; s < hi; s++ {
		for _, f := range fields {
			key := seriesKeyName(s) + "#!~#" + f
			m[key] = itsm.Values{
				itsm.NewFloatValue(baseTS, float64(s)),
				itsm.NewFloatValue(baseTS+1e9, float64(s)+0.5),
			}
		}
	}
	return m
}

func seriesKeyName(s int) string {
	// e.g. cpu,host=node0007
	return "cpu,host=node" + pad4(s)
}

func pad4(n int) string {
	s := []byte("0000")
	i := len(s) - 1
	for n > 0 && i >= 0 {
		s[i] = byte('0' + n%10)
		n /= 10
		i--
	}
	return string(s)
}
