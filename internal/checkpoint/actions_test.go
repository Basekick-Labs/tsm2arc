package checkpoint

import (
	"path/filepath"
	"testing"
)

// Action deltas ride chunk commits (and FinishShard for the trailing window)
// and ACCUMULATE: the audit trail must stay exact across seek-resumes, where a
// resumed run only ever sees — and re-counts — the un-committed tail.
func TestMeasurementActionDeltas(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// empty log → empty report, no error
	rows, err := s.MeasurementReport()
	if err != nil || len(rows) != 0 {
		t.Fatalf("empty report: %v rows, err=%v", rows, err)
	}

	rename := func(pts int64) ActionDelta {
		return ActionDelta{Measurement: "edge-prod.gateway", Action: "renamed",
			RenamedTo: "edge_prod_gateway", Origin: "explicit", Points: pts}
	}
	skip := func(pts int64) ActionDelta {
		return ActionDelta{Measurement: "test.skip_me", Action: "skipped", Points: pts}
	}

	// Shard 1: two chunk commits carrying deltas, then trailing deltas at finish.
	if err := s.Commit("metrics", "1", 0, 10, Cursor{SeriesKey: "a", UnixNano: 1},
		[]ActionDelta{rename(60), skip(2)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit("metrics", "1", 1, 10, Cursor{SeriesKey: "a", UnixNano: 2},
		[]ActionDelta{rename(40)}); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishShard("metrics", "1", 2, []ActionDelta{skip(5)}); err != nil {
		t.Fatal(err)
	}
	// Shard 2: same measurement aggregates across shards in the report.
	if err := s.Commit("metrics", "2", 0, 10, Cursor{SeriesKey: "a", UnixNano: 1},
		[]ActionDelta{rename(50)}); err != nil {
		t.Fatal(err)
	}

	rows, err = s.MeasurementReport()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("report rows = %d, want 2: %+v", len(rows), rows)
	}
	// ordered by action then measurement: renamed < skipped
	if r := rows[0]; r.Measurement != "edge-prod.gateway" || r.Action != "renamed" ||
		r.RenamedTo != "edge_prod_gateway" || r.Origin != "explicit" || r.Points != 150 || r.Shards != 2 {
		t.Errorf("renamed row = %+v", r)
	}
	if r := rows[1]; r.Measurement != "test.skip_me" || r.Action != "skipped" || r.Points != 7 || r.Shards != 1 {
		t.Errorf("skipped row = %+v", r)
	}

	// nil/empty delta sets are no-ops
	if err := s.Commit("metrics", "1", 2, 0, Cursor{SeriesKey: "a", UnixNano: 3}, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishShard("metrics", "1", 3, nil); err != nil {
		t.Fatal(err)
	}
	rows, _ = s.MeasurementReport()
	if rows[0].Points != 150 || rows[1].Points != 7 {
		t.Errorf("no-op commits changed counts: %+v", rows)
	}
}
