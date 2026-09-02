package extract

import (
	"testing"

	itsm "github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// Fully overlapping generations: every file spans the whole timeline, so every
// window of a split still touches every stream — the analysis must call that
// out (max window streams == total streams).
func TestAnalyzeShardOverlapping(t *testing.T) {
	dir := t.TempDir()
	base := int64(1700000000) * sec
	var files []string
	for i := 0; i < 3; i++ {
		files = append(files, realTSMNamed(t, dir, tsmName(i), map[string]itsm.Values{
			"cpu,host=node-a#!~#usage": floatRun(base, sec, 3000),
			"cpu,host=node-a#!~#load":  floatRun(base, sec, 3000),
		}))
	}
	an, err := AnalyzeShard(files, openTSMFile, 4)
	if err != nil {
		t.Fatal(err)
	}
	if an.Series != 1 || an.Files != 3 || an.Keys != 6 {
		t.Fatalf("series/files/keys = %d/%d/%d, want 1/3/6", an.Series, an.Files, an.Keys)
	}
	if len(an.Runs) != 1 {
		t.Fatalf("runs = %d, want 1 (fully overlapping files must merge as one run)", len(an.Runs))
	}
	r := an.Runs[0]
	if r.Streams != 6 {
		t.Fatalf("streams = %d, want 6 (3 files × 2 fields)", r.Streams)
	}
	for wi, ws := range r.WindowStreams {
		if ws != r.Streams {
			t.Errorf("window %d touches %d of %d streams — fully overlapping data must touch all", wi, ws, r.Streams)
		}
	}
}

// Chained generations (A overlaps B, B overlaps C, A disjoint from C) form one
// transitive run, but windows must touch only the files under them — the shape
// where a window split genuinely prunes work.
func TestAnalyzeShardChained(t *testing.T) {
	dir := t.TempDir()
	base := int64(1700000000) * sec
	const n = 3000 // 3 blocks per key
	span := int64(n) * sec
	files := []string{
		realTSMNamed(t, dir, tsmName(0), map[string]itsm.Values{
			"cpu,host=node-a#!~#usage": floatRun(base, sec, n)}),
		realTSMNamed(t, dir, tsmName(1), map[string]itsm.Values{
			"cpu,host=node-a#!~#usage": floatRun(base+span/2, sec, n)}),
		realTSMNamed(t, dir, tsmName(2), map[string]itsm.Values{
			"cpu,host=node-a#!~#usage": floatRun(base+span, sec, n)}),
	}
	an, err := AnalyzeShard(files, openTSMFile, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(an.Runs) != 1 {
		t.Fatalf("runs = %d, want 1 (chained overlap is one transitive run)", len(an.Runs))
	}
	r := an.Runs[0]
	if r.Streams != 3 {
		t.Fatalf("streams = %d, want 3", r.Streams)
	}
	// The first window covers the earliest part of the timeline, which file 2
	// (disjoint from file 0) cannot reach; likewise the last window excludes
	// file 0. So at least one window must touch fewer than all streams.
	pruned := false
	for _, ws := range r.WindowStreams {
		if ws < r.Streams {
			pruned = true
		}
		if ws == 0 {
			t.Error("a window touches zero streams — split points degenerate")
		}
	}
	if !pruned {
		t.Errorf("no window pruned any stream (profile %v of %d) — chained shape must be split-friendly", r.WindowStreams, r.Streams)
	}
}

// Time-disjoint files never share a run; the analysis reports one run per file.
func TestAnalyzeShardDisjointRuns(t *testing.T) {
	dir := t.TempDir()
	base := int64(1700000000) * sec
	var files []string
	for i := 0; i < 4; i++ {
		files = append(files, realTSMNamed(t, dir, tsmName(i), map[string]itsm.Values{
			"cpu,host=node-a#!~#usage": floatRun(base+int64(i)*10000*sec, sec, 1500)}))
	}
	an, err := AnalyzeShard(files, openTSMFile, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(an.Runs) != 4 {
		t.Fatalf("runs = %d, want 4 (disjoint files = separate runs)", len(an.Runs))
	}
}

func tsmName(i int) string {
	return string(rune('1'+i)) + "00000001-000000001.tsm"
}
