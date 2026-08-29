package extract

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/basekick-labs/tsm2arc/internal/tsm"
	itsm "github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// usage records how the extractor drove the TSM readers, so the tests below can
// assert on the *mechanism* (block-at-a-time reads, pruned blocks, bounded open
// handles) rather than only on the output.
type usage struct {
	openNow, openMax int
	blockReads       int // ReadBlockAt calls
	wholeKeyReads    int // ReadKeyByName calls — the materializing path
	maxBlockValues   int // largest slice any single read returned
}

// countingTSM wraps a real reader and records its use in a shared usage.
type countingTSM struct {
	inner TSMFile
	u     *usage
}

func (c *countingTSM) Keys() []string { return c.inner.Keys() }

func (c *countingTSM) ReadKeyByName(k string) ([]tsm.Value, error) {
	v, err := c.inner.ReadKeyByName(k)
	c.u.wholeKeyReads++
	if len(v) > c.u.maxBlockValues {
		c.u.maxBlockValues = len(v)
	}
	return v, err
}

func (c *countingTSM) Blocks(k string) []tsm.IndexEntry { return c.inner.Blocks(k) }

func (c *countingTSM) ReadBlockAt(k string, e tsm.IndexEntry) ([]tsm.Value, error) {
	v, err := c.inner.ReadBlockAt(k, e)
	c.u.blockReads++
	if len(v) > c.u.maxBlockValues {
		c.u.maxBlockValues = len(v)
	}
	return v, err
}

func (c *countingTSM) Close() error {
	c.u.openNow--
	return c.inner.Close()
}

// countingOpener returns an OpenTSM that tracks concurrently-open readers.
func countingOpener(u *usage) OpenTSM {
	return func(path string) (TSMFile, error) {
		r, err := tsm.Open(path)
		if err != nil {
			return nil, err
		}
		u.openNow++
		if u.openNow > u.openMax {
			u.openMax = u.openNow
		}
		return &countingTSM{inner: r, u: u}, nil
	}
}

// pointsPerBlock mirrors InfluxDB's tsdb.DefaultMaxPointsPerBlock: the
// compactor caps a TSM block at 1000 values, so any real key holding more than
// that is stored as several blocks. itsm.TSMWriter.Write does NOT chunk (one
// call = one block), so the fixtures below chunk explicitly to match what the
// customer's files actually look like.
const pointsPerBlock = 1000

// realTSMNamed writes a TSM file under an explicit name (file order = TSM
// generation order, which is what decides last-write-wins across files), with
// each key split into 1000-value blocks like a real compaction would.
func realTSMNamed(t *testing.T, dir, name string, entries map[string]itsm.Values) string {
	t.Helper()
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w, err := itsm.NewTSMWriter(f)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		vals := entries[k]
		for i := 0; i < len(vals); i += pointsPerBlock {
			j := i + pointsPerBlock
			if j > len(vals) {
				j = len(vals)
			}
			if err := w.Write([]byte(k), vals[i:j]); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := w.WriteIndex(); err != nil {
		t.Fatal(err)
	}
	w.Close()
	f.Close()
	return path
}

// floatRun builds n float values at ts = base + i*step.
func floatRun(base, step int64, n int) itsm.Values {
	vals := make(itsm.Values, n)
	for i := 0; i < n; i++ {
		vals[i] = itsm.NewFloatValue(base+int64(i)*step, float64(i))
	}
	return vals
}

const sec = int64(1e9)

// TestShardStreamsBlocksNeverMaterializesKey is the regression test for the
// memory wall: a single key far bigger than one block must be read one block at
// a time, never as a whole slice. Before 0.1.4 the extractor called
// ReadKeyByName per key, so peak memory scaled with the largest series —
// terabytes for a 250 GB one.
func TestShardStreamsBlocksNeverMaterializesKey(t *testing.T) {
	dir := t.TempDir()
	const n = 5000 // > 1000 values per TSM block, so this spans several blocks
	path := realTSMNamed(t, dir, "000000001-000000001.tsm", map[string]itsm.Values{
		"cpu,host=node-a#!~#usage": floatRun(1700000000*sec, sec, n),
	})

	var u usage
	var points int
	st, err := Shard([]string{path}, nil, countingOpener(&u), walReadFileForTest,
		math.MinInt64, math.MaxInt64, func(Point) { points++ })
	if err != nil {
		t.Fatal(err)
	}
	if st.Points != n || points != n {
		t.Fatalf("points: stats %d, emitted %d, want %d", st.Points, points, n)
	}
	if u.wholeKeyReads != 0 {
		t.Errorf("ReadKeyByName called %d times — the key was materialized instead of streamed", u.wholeKeyReads)
	}
	if u.blockReads < 2 {
		t.Errorf("block reads: got %d, want the key read as multiple blocks", u.blockReads)
	}
	// The whole point: no single read ever holds the whole key.
	if u.maxBlockValues >= n {
		t.Errorf("largest single read was %d values (whole key is %d) — not bounded by block size",
			u.maxBlockValues, n)
	}
}

// TestShardPrunesBlocksOutsideWindow proves --start/--end bounds WORK, not just
// output. Blocks that cannot intersect the window must never be read from disk.
func TestShardPrunesBlocksOutsideWindow(t *testing.T) {
	dir := t.TempDir()
	const n = 5000
	base := int64(1700000000) * sec
	path := realTSMNamed(t, dir, "000000001-000000001.tsm", map[string]itsm.Values{
		"cpu,host=node-a#!~#usage": floatRun(base, sec, n),
	})

	// Full pass first, to learn how many blocks the file actually has.
	var full usage
	if _, err := Shard([]string{path}, nil, countingOpener(&full), walReadFileForTest,
		math.MinInt64, math.MaxInt64, nil); err != nil {
		t.Fatal(err)
	}

	// Now a window covering only values 100..199.
	start, end := base+100*sec, base+199*sec
	var win usage
	var got []int64
	st, err := Shard([]string{path}, nil, countingOpener(&win), walReadFileForTest,
		start, end, func(p Point) { got = append(got, p.UnixNano) })
	if err != nil {
		t.Fatal(err)
	}
	if st.Points != 100 || len(got) != 100 {
		t.Fatalf("windowed points: stats %d, emitted %d, want 100", st.Points, len(got))
	}
	if got[0] != start || got[len(got)-1] != end {
		t.Fatalf("window bounds: got [%d,%d] want [%d,%d]", got[0], got[len(got)-1], start, end)
	}
	if win.blockReads >= full.blockReads {
		t.Errorf("windowed run read %d blocks vs %d for the full file — blocks were not pruned",
			win.blockReads, full.blockReads)
	}
	if win.blockReads > 2 {
		t.Errorf("windowed run read %d blocks for a 100-value window — expected 1-2", win.blockReads)
	}
}

// TestShardBoundsOpenFileHandles covers the fd cost of streaming. A key large
// enough to be split across TSM files by the writer's size cap lives in many
// files holding disjoint ascending time ranges; lazy open + eager close must
// keep the concurrently-open count near one, not one per file.
func TestShardBoundsOpenFileHandles(t *testing.T) {
	dir := t.TempDir()
	base := int64(1700000000) * sec
	var files []string
	const nFiles = 8
	for i := 0; i < nFiles; i++ {
		// Disjoint, ascending windows — one file's values all precede the next's.
		files = append(files, realTSMNamed(t, dir, fmt.Sprintf("00000000%d-000000001.tsm", i+1),
			map[string]itsm.Values{
				"cpu,host=node-a#!~#usage": floatRun(base+int64(i)*10000*sec, sec, 2000),
			}))
	}

	var u usage
	var points int
	st, err := Shard(files, nil, countingOpener(&u), walReadFileForTest,
		math.MinInt64, math.MaxInt64, func(Point) { points++ })
	if err != nil {
		t.Fatal(err)
	}
	if want := nFiles * 2000; st.Points != want || points != want {
		t.Fatalf("points: stats %d, emitted %d, want %d", st.Points, points, want)
	}
	// The indexing pass opens one file at a time; the merge should too, because
	// each file's stream is exhausted (and released) before the next is touched.
	if u.openMax > 2 {
		t.Errorf("max concurrently open TSM files: %d, want <= 2 (fd bound for a split key)", u.openMax)
	}
	if u.openNow != 0 {
		t.Errorf("%d reader(s) left open after Shard returned", u.openNow)
	}
}

// nonAscendingFake reports blocks that overlap/descend in time — something a
// well-formed TSM file never does, but which would silently misorder output if
// streamed. The extractor must detect it and fall back to reading + sorting that
// key.
type nonAscendingFake struct {
	vals []tsm.Value
}

func (f *nonAscendingFake) Keys() []string { return []string{"cpu,host=node-a#!~#usage"} }

func (f *nonAscendingFake) ReadKeyByName(string) ([]tsm.Value, error) { return f.vals, nil }

// Blocks claims two blocks in DESCENDING time order.
func (f *nonAscendingFake) Blocks(string) []tsm.IndexEntry {
	return []tsm.IndexEntry{
		{MinTime: 300, MaxTime: 400},
		{MinTime: 100, MaxTime: 200},
	}
}

func (f *nonAscendingFake) ReadBlockAt(string, tsm.IndexEntry) ([]tsm.Value, error) {
	return nil, fmt.Errorf("ReadBlockAt must not be used for a non-ascending key")
}

func (f *nonAscendingFake) Close() error { return nil }

func TestShardFallsBackWhenBlocksNotAscending(t *testing.T) {
	fake := &nonAscendingFake{vals: []tsm.Value{
		{UnixNano: 300, Type: tsm.BlockFloat, Float: 3},
		{UnixNano: 400, Type: tsm.BlockFloat, Float: 4},
		{UnixNano: 100, Type: tsm.BlockFloat, Float: 1},
		{UnixNano: 200, Type: tsm.BlockFloat, Float: 2},
	}}
	open := func(string) (TSMFile, error) { return fake, nil }

	var ts []int64
	st, err := Shard([]string{"fake.tsm"}, nil, open, walReadFileForTest,
		math.MinInt64, math.MaxInt64, func(p Point) { ts = append(ts, p.UnixNano) })
	if err != nil {
		t.Fatal(err)
	}
	if st.Points != 4 {
		t.Fatalf("points: got %d want 4", st.Points)
	}
	want := []int64{100, 200, 300, 400}
	for i := range want {
		if ts[i] != want[i] {
			t.Fatalf("timestamps: got %v want %v", ts, want)
		}
	}
}

// TestShardStreamingMatchesReference is the correctness net for the rewrite:
// multi-block, multi-file, multi-field, with a WAL that overwrites some values
// and adds out-of-order ones. The expected output is built independently here
// with a plain map, then compared line for line.
func TestShardStreamingMatchesReference(t *testing.T) {
	dir := t.TempDir()
	base := int64(1700000000) * sec

	// Reference model: ts -> field -> rendered value. Applied in the same
	// precedence order the extractor must use (older file, newer file, WAL).
	ref := map[int64]map[string]float64{}
	put := func(ts int64, field string, v float64) {
		if ref[ts] == nil {
			ref[ts] = map[string]float64{}
		}
		ref[ts][field] = v
	}

	// File 1: 2500 values of "usage" and "load" (spans several blocks each).
	f1 := map[string]itsm.Values{}
	for _, field := range []string{"load", "usage"} {
		vals := make(itsm.Values, 2500)
		for i := 0; i < 2500; i++ {
			ts := base + int64(i)*sec
			v := float64(i)
			if field == "load" {
				v = float64(i) / 2
			}
			vals[i] = itsm.NewFloatValue(ts, v)
			put(ts, field, v)
		}
		f1["cpu,host=node-a#!~#"+field] = vals
	}
	p1 := realTSMNamed(t, dir, "000000001-000000001.tsm", f1)

	// File 2 (newer generation): overwrites "usage" on the first 10 timestamps.
	f2 := itsm.Values{}
	for i := 0; i < 10; i++ {
		ts := base + int64(i)*sec
		v := 1000 + float64(i)
		f2 = append(f2, itsm.NewFloatValue(ts, v))
		put(ts, "usage", v)
	}
	p2 := realTSMNamed(t, dir, "000000002-000000001.tsm",
		map[string]itsm.Values{"cpu,host=node-a#!~#usage": f2})

	// WAL (newest): overwrites usage at ts 5, adds a new field, and writes an
	// out-of-order pair the WAL is allowed to contain.
	walVals := []itsm.Value{
		itsm.NewFloatValue(base+7*sec, 7777),
		itsm.NewFloatValue(base+5*sec, 5555), // out of order on purpose
	}
	put(base+7*sec, "usage", 7777)
	put(base+5*sec, "usage", 5555)
	extra := []itsm.Value{itsm.NewFloatValue(base+3*sec, 33)}
	put(base+3*sec, "extra", 33)
	wp := realWAL(t, dir, map[string][]itsm.Value{
		"cpu,host=node-a#!~#usage": walVals,
		"cpu,host=node-a#!~#extra": extra,
	})

	// Expected LP, in the extractor's deterministic order: ts ascending, fields
	// sorted by name.
	tsList := make([]int64, 0, len(ref))
	for ts := range ref {
		tsList = append(tsList, ts)
	}
	sort.Slice(tsList, func(i, j int) bool { return tsList[i] < tsList[j] })
	var want []string
	for _, ts := range tsList {
		fields := make([]string, 0, len(ref[ts]))
		for f := range ref[ts] {
			fields = append(fields, f)
		}
		sort.Strings(fields)
		parts := make([]string, len(fields))
		for i, f := range fields {
			parts[i] = fmt.Sprintf("%s=%s", f, formatFloat(ref[ts][f]))
		}
		want = append(want, fmt.Sprintf("cpu,host=node-a %s %d", strings.Join(parts, ","), ts))
	}

	var got []string
	st, err := Shard([]string{p1, p2}, []string{wp}, openTSMFile, walReadFileForTest,
		math.MinInt64, math.MaxInt64, func(p Point) {
			got = append(got, strings.TrimRight(EncodePoint(p), "\n"))
		})
	if err != nil {
		t.Fatal(err)
	}
	if st.Points != len(want) {
		t.Fatalf("points: got %d want %d", st.Points, len(want))
	}
	if len(got) != len(want) {
		t.Fatalf("lines: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("line %d:\n got %s\nwant %s", i, got[i], want[i])
		}
	}
}

// formatFloat renders a float the way the LP encoder does ('g', -1), so the
// reference model above can be compared as text.
func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// TestShardRunsOrderedByTimeNotFileIndex covers the case where run order and
// file order disagree: a NEWER TSM generation holding OLDER data. The runs must
// be emitted in time order (so output stays ascending), even though that is the
// reverse of the file order used for tie-breaking.
func TestShardRunsOrderedByTimeNotFileIndex(t *testing.T) {
	dir := t.TempDir()
	base := int64(1700000000) * sec

	// Generation 1 holds the LATER window; generation 2 holds the EARLIER one.
	late := realTSMNamed(t, dir, "000000001-000000001.tsm", map[string]itsm.Values{
		"cpu,host=node-a#!~#usage": floatRun(base+5000*sec, sec, 1500),
	})
	early := realTSMNamed(t, dir, "000000002-000000001.tsm", map[string]itsm.Values{
		"cpu,host=node-a#!~#usage": floatRun(base, sec, 1500),
	})

	var u usage
	var ts []int64
	st, err := Shard([]string{late, early}, nil, countingOpener(&u), walReadFileForTest,
		math.MinInt64, math.MaxInt64, func(p Point) { ts = append(ts, p.UnixNano) })
	if err != nil {
		t.Fatal(err)
	}
	if st.Points != 3000 {
		t.Fatalf("points: got %d want 3000", st.Points)
	}
	for i := 1; i < len(ts); i++ {
		if ts[i] <= ts[i-1] {
			t.Fatalf("output not ascending at %d: %d then %d", i, ts[i-1], ts[i])
		}
	}
	if ts[0] != base {
		t.Fatalf("first timestamp: got %d want %d (earlier window must come first)", ts[0], base)
	}
	if u.openMax > 2 {
		t.Errorf("max concurrently open files: %d, want <= 2 (disjoint ranges merge separately)", u.openMax)
	}
}

// TestShardOverlappingGenerationsStayInOneRun is the guard on the other side:
// files whose ranges overlap must NOT be split into separate runs, or the newer
// generation would stop winning a same-timestamp tie.
func TestShardOverlappingGenerationsStayInOneRun(t *testing.T) {
	dir := t.TempDir()
	base := int64(1700000000) * sec

	old := realTSMNamed(t, dir, "000000001-000000001.tsm", map[string]itsm.Values{
		"cpu,host=node-a#!~#usage": floatRun(base, sec, 1500),
	})
	// Same window, newer generation, distinct values.
	newer := itsm.Values{}
	for i := 0; i < 1500; i++ {
		newer = append(newer, itsm.NewFloatValue(base+int64(i)*sec, 9000+float64(i)))
	}
	newest := realTSMNamed(t, dir, "000000002-000000001.tsm",
		map[string]itsm.Values{"cpu,host=node-a#!~#usage": newer})

	var lines []string
	st, err := Shard([]string{old, newest}, nil, openTSMFile, walReadFileForTest,
		math.MinInt64, math.MaxInt64, func(p Point) {
			lines = append(lines, strings.TrimRight(EncodePoint(p), "\n"))
		})
	if err != nil {
		t.Fatal(err)
	}
	if st.Points != 1500 {
		t.Fatalf("points: got %d want 1500 (overlapping generations must collapse, not duplicate)", st.Points)
	}
	want := fmt.Sprintf("cpu,host=node-a usage=%s %d", formatFloat(9000), base)
	if lines[0] != want {
		t.Fatalf("newer generation did not win:\n got %s\nwant %s", lines[0], want)
	}
}
