package extract

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/basekick-labs/tsm2arc/internal/tsm"
	itsm "github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// forceWindowing shrinks the run-size threshold so small fixtures split, and
// restores it. Output must be byte-identical at ANY threshold, so this only
// makes the machinery fire — it can't change what the tests assert.
func forceWindowing(t *testing.T) {
	t.Helper()
	old := splitRunThreshold
	splitRunThreshold = 1
	t.Cleanup(func() { splitRunThreshold = old })
}

// extractAll runs a full shard extraction and returns the encoded lines + stats.
func extractAll(t *testing.T, tsmFiles, walFiles []string, cur *Cursor, opt SplitOptions) ([]string, Stats) {
	t.Helper()
	var lines []string
	st, err := ShardResumeSplit(tsmFiles, walFiles, openTSMFile, walReadFileForTest,
		math.MinInt64, math.MaxInt64, cur, opt, func(p Point) {
			lines = append(lines, strings.TrimRight(EncodePoint(p), "\n"))
		})
	if err != nil {
		t.Fatalf("extract (workers=%d): %v", opt.Workers, err)
	}
	return lines, st
}

// assertIdentical compares serial output against split output line by line,
// and the Stats — the load-bearing claim behind keeping SplitOptions out of
// the resume fingerprint.
func assertIdentical(t *testing.T, tsmFiles, walFiles []string, cur *Cursor, workers int) {
	t.Helper()
	want, wantSt := extractAll(t, tsmFiles, walFiles, cur, SplitOptions{})
	got, gotSt := extractAll(t, tsmFiles, walFiles, cur, SplitOptions{Workers: workers, MemoryBudget: 1 << 30})
	if len(got) != len(want) {
		t.Fatalf("workers=%d: %d lines vs %d serial", workers, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("workers=%d line %d differs:\n got %s\nwant %s", workers, i, got[i], want[i])
		}
	}
	if gotSt != wantSt {
		t.Errorf("workers=%d stats differ: %+v vs %+v", workers, gotSt, wantSt)
	}
}

// splitFixture builds the adversarial shard shape: two multi-block series, an
// overlapping newer generation, and a WAL with out-of-order writes, duplicate
// timestamps, and a WAL-only field.
func splitFixture(t *testing.T) (tsmFiles, walFiles []string) {
	t.Helper()
	dir := t.TempDir()
	base := int64(1700000000) * sec

	f1 := map[string]itsm.Values{}
	for _, field := range []string{"load", "usage"} {
		f1["cpu,host=node-a#!~#"+field] = floatRun(base, sec, 2500)
	}
	f1["mem,host=node-a#!~#free"] = floatRun(base, sec, 1200)
	p1 := realTSMNamed(t, dir, "000000001-000000001.tsm", f1)

	// Newer generation overwrites the first 500 usage values (overlap → same run).
	over := itsm.Values{}
	for i := 0; i < 500; i++ {
		over = append(over, itsm.NewFloatValue(base+int64(i)*sec, 9000+float64(i)))
	}
	p2 := realTSMNamed(t, dir, "000000002-000000001.tsm",
		map[string]itsm.Values{"cpu,host=node-a#!~#usage": over})

	// WAL: out of order, a duplicate ts (last write wins), and a new field.
	wp := realWAL(t, dir, map[string][]itsm.Value{
		"cpu,host=node-a#!~#usage": {
			itsm.NewFloatValue(base+700*sec, 7777),
			itsm.NewFloatValue(base+300*sec, 3333),
			itsm.NewFloatValue(base+300*sec, 3334), // dup ts — later wins
		},
		"cpu,host=node-a#!~#extra": {itsm.NewFloatValue(base+42*sec, 42)},
	})
	return []string{p1, p2}, []string{wp}
}

func TestSplitByteIdenticalOverlappingWithWAL(t *testing.T) {
	forceWindowing(t)
	tsmFiles, walFiles := splitFixture(t)
	for _, w := range []int{2, 4, 13} {
		assertIdentical(t, tsmFiles, walFiles, nil, w)
	}
}

func TestSplitByteIdenticalDisjointGenerations(t *testing.T) {
	forceWindowing(t)
	dir := t.TempDir()
	base := int64(1700000000) * sec
	var files []string
	for i := 0; i < 6; i++ {
		files = append(files, realTSMNamed(t, dir, fmt.Sprintf("00000000%d-000000001.tsm", i+1),
			map[string]itsm.Values{
				"cpu,host=node-a#!~#usage": floatRun(base+int64(i)*10000*sec, sec, 2000),
			}))
	}
	assertIdentical(t, files, nil, nil, 4)
}

// The invariant test: TSM legally places the SAME timestamp at the boundary of
// two adjacent blocks of one key. With windowing forced, a split point lands on
// that timestamp; a block-assignment implementation would emit the point twice
// (or flip the last-write-wins winner). Timestamp-partitioned windows must
// produce the identical single point.
func TestSplitByteIdenticalEqualTsAcrossBlockBoundary(t *testing.T) {
	forceWindowing(t)
	dir := t.TempDir()
	base := int64(1700000000) * sec

	// 3 blocks of 1000; vals[1000] repeats vals[999]'s timestamp with a
	// different value, so block 1 ends and block 2 begins at the same ts.
	vals := make(itsm.Values, 0, 3000)
	for i := 0; i < 3000; i++ {
		ts := base + int64(i)*sec
		if i >= 1000 {
			ts -= sec // shift everything after the dup back one step
		}
		vals = append(vals, itsm.NewFloatValue(ts, float64(i)))
	}
	// vals[999].ts == base+999s, vals[1000].ts == base+999s (values 999 vs 1000).
	path := realTSMNamed(t, dir, "000000001-000000001.tsm",
		map[string]itsm.Values{"cpu,host=node-a#!~#usage": vals})

	// Sanity: serial output must resolve the dup to ONE point with the later
	// value (intra-stream last-write-wins).
	serial, st := extractAll(t, []string{path}, nil, nil, SplitOptions{})
	if st.Points != 2999 {
		t.Fatalf("serial points = %d, want 2999 (dup ts collapses)", st.Points)
	}
	wantDup := fmt.Sprintf("cpu,host=node-a usage=%s %d", formatFloat(1000), base+999*sec)
	if serial[999] != wantDup {
		t.Fatalf("serial dup line = %q, want %q (later block's value wins)", serial[999], wantDup)
	}

	for _, w := range []int{2, 4} {
		assertIdentical(t, []string{path}, nil, nil, w)
	}
}

func TestSplitByteIdenticalNonAscendingFallback(t *testing.T) {
	forceWindowing(t)
	fake := &nonAscendingFake{vals: []tsm.Value{
		{UnixNano: 300, Type: tsm.BlockFloat, Float: 3},
		{UnixNano: 400, Type: tsm.BlockFloat, Float: 4},
		{UnixNano: 100, Type: tsm.BlockFloat, Float: 1},
		{UnixNano: 200, Type: tsm.BlockFloat, Float: 2},
	}}
	open := func(string) (TSMFile, error) { return fake, nil }

	var serial, split []string
	if _, err := ShardResumeSplit([]string{"fake.tsm"}, nil, open, walReadFileForTest,
		math.MinInt64, math.MaxInt64, nil, SplitOptions{}, func(p Point) {
			serial = append(serial, strings.TrimRight(EncodePoint(p), "\n"))
		}); err != nil {
		t.Fatal(err)
	}
	if _, err := ShardResumeSplit([]string{"fake.tsm"}, nil, open, walReadFileForTest,
		math.MinInt64, math.MaxInt64, nil, SplitOptions{Workers: 4, MemoryBudget: 1 << 30}, func(p Point) {
			split = append(split, strings.TrimRight(EncodePoint(p), "\n"))
		}); err != nil {
		t.Fatal(err)
	}
	if len(serial) != 4 || len(split) != len(serial) {
		t.Fatalf("lines: serial %d split %d, want 4", len(serial), len(split))
	}
	for i := range serial {
		if serial[i] != split[i] {
			t.Fatalf("line %d differs: %q vs %q", i, split[i], serial[i])
		}
	}
}

// Seek-resume must produce the identical suffix regardless of worker count —
// including a cursor placed exactly where a window boundary falls.
func TestSplitSeekResumeIdentical(t *testing.T) {
	forceWindowing(t)
	tsmFiles, walFiles := splitFixture(t)
	full, _ := extractAll(t, tsmFiles, walFiles, nil, SplitOptions{})

	base := int64(1700000000) * sec
	for _, cur := range []*Cursor{
		{SeriesKey: "cpu,host=node-a", UnixNano: base + 1234*sec}, // mid-series
		{SeriesKey: "cpu,host=node-a", UnixNano: base + 999*sec},  // block boundary
		{SeriesKey: "cpu,host=node-a", UnixNano: math.MaxInt64},   // series done
	} {
		want, _ := extractAll(t, tsmFiles, walFiles, cur, SplitOptions{})
		got, _ := extractAll(t, tsmFiles, walFiles, cur, SplitOptions{Workers: 4, MemoryBudget: 1 << 30})
		if len(got) != len(want) {
			t.Fatalf("cursor ts=%d: %d lines vs %d serial", cur.UnixNano, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("cursor ts=%d line %d differs", cur.UnixNano, i)
			}
		}
		// And the suffix property against the full run still holds.
		if len(want) > 0 {
			tail := full[len(full)-len(want):]
			for i := range want {
				if want[i] != tail[i] {
					t.Fatalf("cursor ts=%d: serial resume is not a suffix of the full run at %d", cur.UnixNano, i)
				}
			}
		}
	}
}

// A budget smaller than every task estimate serializes execution (each task
// clamps to the whole budget and runs alone) — output identical, no deadlock.
func TestSplitTinyBudgetRunsSerially(t *testing.T) {
	forceWindowing(t)
	tsmFiles, walFiles := splitFixture(t)
	want, _ := extractAll(t, tsmFiles, walFiles, nil, SplitOptions{})
	got, _ := extractAll(t, tsmFiles, walFiles, nil, SplitOptions{Workers: 4, MemoryBudget: 1})
	if len(got) != len(want) {
		t.Fatalf("%d lines vs %d serial", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("line %d differs under tiny budget", i)
		}
	}
}

func TestSplitRequiresMemoryBudget(t *testing.T) {
	tsmFiles, _ := splitFixture(t)
	_, err := ShardResumeSplit(tsmFiles, nil, openTSMFile, walReadFileForTest,
		math.MinInt64, math.MaxInt64, nil, SplitOptions{Workers: 2}, nil)
	if err == nil || !strings.Contains(err.Error(), "MemoryBudget") {
		t.Fatalf("Workers>1 without budget: err = %v, want MemoryBudget error", err)
	}
}

// failingOpener succeeds for the first n opens, then fails — exercising the
// worker-phase error path (build phase has already opened every file twice).
// Mutex-guarded: workers open concurrently.
type failingOpener struct {
	mu    sync.Mutex
	n     int
	count int
}

func (f *failingOpener) open(path string) (TSMFile, error) {
	f.mu.Lock()
	f.count++
	n := f.count
	f.mu.Unlock()
	if n > f.n {
		return nil, fmt.Errorf("injected open failure at open %d", n)
	}
	return openTSMFile(path)
}

func TestSplitWorkerErrorPropagates(t *testing.T) {
	forceWindowing(t)
	tsmFiles, walFiles := splitFixture(t)
	// Phase 1 opens each file once, buildTasks opens per (run,file); allow those
	// and fail during task execution.
	fo := &failingOpener{n: 2*len(tsmFiles) + 1}
	_, err := ShardResumeSplit(tsmFiles, walFiles, fo.open, walReadFileForTest,
		math.MinInt64, math.MaxInt64, nil, SplitOptions{Workers: 4, MemoryBudget: 1 << 30}, func(Point) {})
	if err == nil || !strings.Contains(err.Error(), "injected open failure") {
		t.Fatalf("worker error not propagated: %v", err)
	}
}
