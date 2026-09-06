package main

import (
	"encoding/json"
	"io"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/basekick-labs/tsm2arc/internal/discover"
)

// --format=json must be complete by construction: every run of every shard, no
// cap, valid JSON as the only stdout content — machine consumers size real
// migrations from this, and a silent truncation makes them confidently wrong.
func TestAnalyzeJSONComplete(t *testing.T) {
	// 7 measurements → 7 single-run series groups per shard: more runs than the
	// text default of 5, so completeness is actually exercised.
	names := []string{"m_a", "m_b", "m_c", "m_d", "m_e", "m_f", "m_g"}
	datadir := writeTSMShardMeasurements(t, "metrics", names, 3)
	shards, err := discover.Walk(datadir, "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	cfg := runConfig{shards: shards, start: math.MinInt64, end: math.MaxInt64,
		jsonOut: true, analyzeRuns: 5} // analyzeRuns must NOT apply to JSON

	old := os.Stdout
	rp, wp, _ := os.Pipe()
	os.Stdout = wp
	runAnalyze(cfg)
	wp.Close()
	os.Stdout = old
	out, _ := io.ReadAll(rp)

	var rep analyzeReport
	if err := json.Unmarshal(out, &rep); err != nil {
		t.Fatalf("stdout is not a single valid JSON document: %v\n%s", err, out)
	}
	if rep.Version != 1 || rep.SplitThresholdBytes != analyzeSplitThreshold {
		t.Errorf("version/threshold = %d/%d", rep.Version, rep.SplitThresholdBytes)
	}
	if len(rep.Shards) != 1 {
		t.Fatalf("shards = %d, want 1", len(rep.Shards))
	}
	s := rep.Shards[0]
	if s.SeriesGroups != 7 || s.RunsTotal != 7 || len(s.Runs) != 7 {
		t.Fatalf("series_groups=%d runs_total=%d len(runs)=%d, want 7/7/7 (JSON must be uncapped)",
			s.SeriesGroups, s.RunsTotal, len(s.Runs))
	}
	for i, r := range s.Runs {
		if r.Verdict == "" || r.Windows != len(r.WindowStreams) || r.Bytes <= 0 {
			t.Errorf("run %d incomplete: %+v", i, r)
		}
		if i > 0 && r.Bytes > s.Runs[i-1].Bytes {
			t.Errorf("runs not sorted by bytes desc at %d", i)
		}
	}

	// Redacted JSON must leak no identifiers and stay complete.
	cfg.redact = true
	rp2, wp2, _ := os.Pipe()
	os.Stdout = wp2
	runAnalyze(cfg)
	wp2.Close()
	os.Stdout = old
	out2, _ := io.ReadAll(rp2)
	if strings.Contains(string(out2), "m_a") || strings.Contains(string(out2), "metrics") {
		t.Error("redacted JSON leaks identifiers")
	}
	var rep2 analyzeReport
	if err := json.Unmarshal(out2, &rep2); err != nil || len(rep2.Shards[0].Runs) != 7 {
		t.Errorf("redacted JSON invalid or truncated: %v", err)
	}
}

// The text cap must announce itself; the whole complaint was an unlabeled
// truncation standing in for a complete report.
func TestAnalyzeTextCapLabeled(t *testing.T) {
	names := []string{"m_a", "m_b", "m_c", "m_d", "m_e", "m_f", "m_g"}
	datadir := writeTSMShardMeasurements(t, "metrics", names, 3)
	shards, _ := discover.Walk(datadir, "", nil, false)
	cfg := runConfig{shards: shards, start: math.MinInt64, end: math.MaxInt64, analyzeRuns: 5}

	out := captureAnalyze(t, cfg)
	if !strings.Contains(out, "showing 5 of 7 runs") {
		t.Errorf("capped text output must say so:\n%s", out)
	}
	// --analyze-runs=0 shows everything and no cap line.
	cfg.analyzeRuns = 0
	out = captureAnalyze(t, cfg)
	if strings.Contains(out, "showing") || strings.Count(out, "  run: series=") != 7 {
		t.Errorf("--analyze-runs=0 must print all runs without a cap line:\n%s", out)
	}
}
