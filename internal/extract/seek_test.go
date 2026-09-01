package extract

import (
	"math"
	"strings"
	"testing"

	itsm "github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// seekFixture writes two multi-block series and returns the shard's full
// deterministic output (as encoded lines) plus the TSM path.
func seekFixture(t *testing.T) (path string, full []string) {
	t.Helper()
	dir := t.TempDir()
	base := int64(1700000000) * sec
	path = realTSMNamed(t, dir, "000000001-000000001.tsm", map[string]itsm.Values{
		"cpu,host=node-a#!~#usage": floatRun(base, sec, 2500),
		"cpu,host=node-b#!~#usage": floatRun(base, sec, 1500),
	})
	_, err := Shard([]string{path}, nil, openTSMFile, walReadFileForTest,
		math.MinInt64, math.MaxInt64, func(p Point) {
			full = append(full, strings.TrimRight(EncodePoint(p), "\n"))
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(full) != 4000 {
		t.Fatalf("fixture emitted %d points, want 4000", len(full))
	}
	return path, full
}

// A cursor mid-series must resume with EXACTLY the suffix of the full output —
// the byte-level foundation of seek-resume.
func TestShardResumeMidSeries(t *testing.T) {
	path, full := seekFixture(t)
	base := int64(1700000000) * sec

	// Cursor after the 1700th point of node-a (mid-series, mid-block).
	cur := &Cursor{SeriesKey: "cpu,host=node-a", UnixNano: base + 1699*sec}
	var got []string
	_, err := ShardResume([]string{path}, nil, openTSMFile, walReadFileForTest,
		math.MinInt64, math.MaxInt64, cur, func(p Point) {
			got = append(got, strings.TrimRight(EncodePoint(p), "\n"))
		})
	if err != nil {
		t.Fatal(err)
	}
	want := full[1700:]
	if len(got) != len(want) {
		t.Fatalf("resumed points = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("line %d differs:\n got %s\nwant %s", i, got[i], want[i])
		}
	}
}

// A cursor at a series' last point must emit only the following series, and a
// series before the cursor must never be read at all (block pruning is what
// makes seek fast, not just correct).
func TestShardResumeSkipsDeliveredBlocks(t *testing.T) {
	path, full := seekFixture(t)
	base := int64(1700000000) * sec

	var u usage
	cur := &Cursor{SeriesKey: "cpu,host=node-a", UnixNano: base + 2499*sec} // node-a fully delivered
	var got []string
	_, err := ShardResume([]string{path}, nil, countingOpener(&u), walReadFileForTest,
		math.MinInt64, math.MaxInt64, cur, func(p Point) {
			got = append(got, strings.TrimRight(EncodePoint(p), "\n"))
		})
	if err != nil {
		t.Fatal(err)
	}
	want := full[2500:]
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("resumed %d points starting %q, want %d starting %q", len(got), got[0], len(want), want[0])
	}
	// node-a spans 3 blocks (2500 values), node-b 2 (1500). Only node-b's may be
	// read; the +1 tolerates one boundary block probe.
	if u.blockReads > 3 {
		t.Errorf("seek read %d blocks, want <= 3 (delivered blocks must be pruned, not decoded)", u.blockReads)
	}
}

// A cursor whose timestamp is MaxInt64 marks the series complete; +1 must not
// overflow into re-emitting the whole series.
func TestShardResumeMaxInt64Cursor(t *testing.T) {
	path, full := seekFixture(t)
	cur := &Cursor{SeriesKey: "cpu,host=node-a", UnixNano: math.MaxInt64}
	var got []string
	_, err := ShardResume([]string{path}, nil, openTSMFile, walReadFileForTest,
		math.MinInt64, math.MaxInt64, cur, func(p Point) {
			got = append(got, strings.TrimRight(EncodePoint(p), "\n"))
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1500 || got[0] != full[2500] {
		t.Fatalf("MaxInt64 cursor emitted %d points, want node-b's 1500 only", len(got))
	}
}

// A cursor naming a series the shard doesn't hold means the source data changed
// since the checkpoint — that must be a loud error, never a silent mis-seek.
func TestShardResumeMissingSeriesFails(t *testing.T) {
	path, _ := seekFixture(t)
	cur := &Cursor{SeriesKey: "cpu,host=gone", UnixNano: 0}
	_, err := ShardResume([]string{path}, nil, openTSMFile, walReadFileForTest,
		math.MinInt64, math.MaxInt64, cur, nil)
	if err == nil || !strings.Contains(err.Error(), "source data changed") {
		t.Fatalf("missing cursor series: got err=%v, want loud source-changed error", err)
	}
}
