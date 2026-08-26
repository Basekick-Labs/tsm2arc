package checkpoint

import (
	"path/filepath"
	"testing"
)

func TestMeasurementActions(t *testing.T) {
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

	acts := []MeasurementAction{
		{Measurement: "edge-prod.gateway", Action: "renamed", RenamedTo: "edge_prod_gateway", Origin: "explicit", Points: 100},
		{Measurement: "test.skip_me", Action: "skipped", Points: 7},
	}
	if err := s.RecordMeasurementActions("metrics", "1", acts); err != nil {
		t.Fatal(err)
	}
	// same measurement in a second shard aggregates
	if err := s.RecordMeasurementActions("metrics", "2", []MeasurementAction{
		{Measurement: "edge-prod.gateway", Action: "renamed", RenamedTo: "edge_prod_gateway", Origin: "explicit", Points: 50},
	}); err != nil {
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

	// Re-recording a shard (deterministic re-derive on resume) OVERWRITES its
	// counts instead of accumulating — no double-counting.
	if err := s.RecordMeasurementActions("metrics", "1", acts); err != nil {
		t.Fatal(err)
	}
	rows, _ = s.MeasurementReport()
	if rows[0].Points != 150 {
		t.Errorf("after re-record, renamed points = %d, want 150 (overwrite, not accumulate)", rows[0].Points)
	}

	// nil/empty batch is a no-op
	if err := s.RecordMeasurementActions("metrics", "1", nil); err != nil {
		t.Fatal(err)
	}
}
