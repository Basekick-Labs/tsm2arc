package checkpoint

import (
	"path/filepath"
	"testing"
)

func openTemp(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// commit is the test shorthand for a plain commit with a cursor and no deltas.
func commit(t *testing.T, s *Store, db, shard string, seq int, rows int64) {
	t.Helper()
	if err := s.Commit(db, shard, seq, rows, Cursor{SeriesKey: "s", UnixNano: int64(seq)}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestCommittedSeqEmpty(t *testing.T) {
	s := openTemp(t)
	seq, cur, err := s.Progress("db", "1")
	if err != nil {
		t.Fatal(err)
	}
	if seq != -1 || cur != nil {
		t.Errorf("empty shard = (%d, %v), want (-1, nil)", seq, cur)
	}
}

func TestCommitAdvances(t *testing.T) {
	s := openTemp(t)
	for i := 0; i < 5; i++ {
		commit(t, s, "db", "1", i, 100)
	}
	seq, cur, _ := s.Progress("db", "1")
	if seq != 4 {
		t.Errorf("seq = %d, want 4", seq)
	}
	// The cursor tracks the latest commit atomically with the seq.
	if cur == nil || cur.UnixNano != 4 || cur.SeriesKey != "s" {
		t.Errorf("cursor = %+v, want {s 4}", cur)
	}
	// rows accumulate
	started, _, rows, err := s.Summary()
	if err != nil {
		t.Fatal(err)
	}
	if started != 1 || rows != 500 {
		t.Errorf("summary started=%d rows=%d, want 1/500", started, rows)
	}
}

// Commit must never move progress backwards (defensive against replayed
// commits) — and the cursor must stay with the retained seq, not the replay's.
func TestCommitNeverRegresses(t *testing.T) {
	s := openTemp(t)
	commit(t, s, "db", "1", 10, 0)
	commit(t, s, "db", "1", 3, 0) // out of order / replay
	seq, cur, _ := s.Progress("db", "1")
	if seq != 10 {
		t.Errorf("seq regressed to %d, want 10", seq)
	}
	if cur == nil || cur.UnixNano != 10 {
		t.Errorf("cursor regressed to %+v, want ts 10", cur)
	}
}

func TestShardIsolation(t *testing.T) {
	s := openTemp(t)
	commit(t, s, "db", "1", 7, 0)
	commit(t, s, "db", "2", 2, 0)
	commit(t, s, "other", "1", 99, 0)
	if v, _ := s.CommittedSeq("db", "1"); v != 7 {
		t.Errorf("db/1 = %d want 7", v)
	}
	if v, _ := s.CommittedSeq("db", "2"); v != 2 {
		t.Errorf("db/2 = %d want 2", v)
	}
	if v, _ := s.CommittedSeq("other", "1"); v != 99 {
		t.Errorf("other/1 = %d want 99", v)
	}
}

func TestShardDone(t *testing.T) {
	s := openTemp(t)
	done, _ := s.IsShardDone("db", "1")
	if done {
		t.Fatal("fresh shard reported done")
	}
	commit(t, s, "db", "1", 2, 300)
	if err := s.FinishShard("db", "1", 3, nil); err != nil {
		t.Fatal(err)
	}
	done, _ = s.IsShardDone("db", "1")
	if !done {
		t.Fatal("shard not marked done")
	}
	_, doneCount, _, _ := s.Summary()
	if doneCount != 1 {
		t.Errorf("done count = %d, want 1", doneCount)
	}
}

// FinishShard must record the CUMULATIVE rows from shard_progress, not just the
// finishing run's: a shard finished by a resume has rows from earlier runs too.
func TestFinishShardRowsSpanRuns(t *testing.T) {
	s := openTemp(t)
	commit(t, s, "db", "1", 0, 100) // "run 1"
	commit(t, s, "db", "1", 1, 250) // "run 2" (resumed)
	if err := s.FinishShard("db", "1", 2, nil); err != nil {
		t.Fatal(err)
	}
	var rows int64
	if err := s.db.QueryRow(
		`SELECT rows_sent FROM shard_done WHERE source_db='db' AND shard_id='1'`,
	).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 350 {
		t.Errorf("done rows = %d, want 350 (cumulative across runs)", rows)
	}
}

func TestRewindRestoresPreviousCursor(t *testing.T) {
	s := openTemp(t)
	commit(t, s, "db", "1", 0, 10)
	commit(t, s, "db", "1", 1, 10)
	s.FinishShard("db", "1", 2, nil)

	prev := &Cursor{SeriesKey: "s", UnixNano: 0}
	if err := s.RewindForTest("db", "1", prev); err != nil {
		t.Fatal(err)
	}
	seq, cur, _ := s.Progress("db", "1")
	if seq != 0 || cur == nil || cur.UnixNano != 0 {
		t.Errorf("after rewind: seq=%d cur=%+v, want 0/{s 0}", seq, cur)
	}
	if done, _ := s.IsShardDone("db", "1"); done {
		t.Error("rewind left the shard done")
	}

	// nil prev = the pre-cursor NULL state (legacy re-derive path).
	if err := s.RewindForTest("db", "1", nil); err != nil {
		t.Fatal(err)
	}
	if _, cur, _ := s.Progress("db", "1"); cur != nil {
		t.Errorf("nil rewind left cursor %+v, want NULL", cur)
	}
}

// A checkpoint written by <= 0.1.4 (no cursor columns) must open cleanly, and
// its orphaned in-progress measurement_actions rows (overwrite semantics) must
// be dropped exactly once so the new accumulate semantics can't double-count.
func TestMigrateFromV1(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	// Reshape to the v1 schema: drop the cursor columns and the version marker.
	for _, stmt := range []string{
		`ALTER TABLE shard_progress DROP COLUMN last_series`,
		`ALTER TABLE shard_progress DROP COLUMN last_ts`,
		`DELETE FROM meta WHERE key='schema_version'`,
		// v1 state: shard "1" in progress with orphaned action rows (the old code
		// wrote actions and done in separate transactions), shard "2" done with
		// final action rows.
		`INSERT INTO shard_progress (source_db, shard_id, committed_seq) VALUES ('db','1',4)`,
		`INSERT INTO measurement_actions (source_db, shard_id, measurement, action, points)
		 VALUES ('db','1','a.b','skipped',100)`,
		`INSERT INTO shard_progress (source_db, shard_id, committed_seq) VALUES ('db','2',1)`,
		`INSERT INTO shard_done (source_db, shard_id, total_chunks, rows_sent) VALUES ('db','2',2,50)`,
		`INSERT INTO measurement_actions (source_db, shard_id, measurement, action, points)
		 VALUES ('db','2','a.b','skipped',7)`,
	} {
		if _, err := s.db.Exec(stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	s.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("open v1 checkpoint: %v", err)
	}
	defer s2.Close()

	// In-progress shard: seq kept, cursor NULL (legacy path), actions dropped.
	seq, cur, err := s2.Progress("db", "1")
	if err != nil || seq != 4 || cur != nil {
		t.Errorf("migrated progress = (%d, %+v, %v), want (4, nil, nil)", seq, cur, err)
	}
	rows, err := s2.MeasurementReport()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Points != 7 {
		t.Errorf("after migration report = %+v, want only the done shard's 7 points", rows)
	}

	// The delete must not fire again: accumulate new-style rows, reopen, verify.
	if err := s2.Commit("db", "1", 5, 0, Cursor{SeriesKey: "s", UnixNano: 5},
		[]ActionDelta{{Measurement: "a.b", Action: "skipped", Points: 3}}); err != nil {
		t.Fatal(err)
	}
	s2.Close()
	s3, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	rows, _ = s3.MeasurementReport()
	var total int64
	for _, r := range rows {
		total += r.Points
	}
	if total != 10 {
		t.Errorf("after reopen total action points = %d, want 10 (3 accumulated + 7 kept)", total)
	}
}

// M3 regression: CheckConfig records the fingerprint on first use and rejects a
// resume whose fingerprint differs (changed chunk-bytes / start / end / db-map),
// which would otherwise misalign chunk sequence numbers and corrupt the resume.
func TestCheckConfigGuardsResume(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cp.db")

	s1, _ := Open(path)
	if err := s1.CheckConfig("chunk=450;start=0;end=1"); err != nil {
		t.Fatalf("first CheckConfig should record, got %v", err)
	}
	// same fingerprint on the same store → ok (idempotent)
	if err := s1.CheckConfig("chunk=450;start=0;end=1"); err != nil {
		t.Errorf("same fingerprint should pass, got %v", err)
	}
	s1.Close()

	// reopen + same fingerprint → ok (resume allowed)
	s2, _ := Open(path)
	if err := s2.CheckConfig("chunk=450;start=0;end=1"); err != nil {
		t.Errorf("matching fingerprint on reopen should pass, got %v", err)
	}
	// reopen + different fingerprint → rejected
	if err := s2.CheckConfig("chunk=200;start=0;end=1"); err == nil {
		t.Error("changed fingerprint should be rejected")
	}
	s2.Close()
}

// Reopening the store must see prior commits (durability across "process restart").
func TestPersistAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cp.db")

	s1, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	commit(t, s1, "db", "1", 42, 1000)
	commit(t, s1, "db", "2", 4, 500)
	s1.FinishShard("db", "2", 5, nil)
	s1.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	seq, cur, _ := s2.Progress("db", "1")
	if seq != 42 || cur == nil || cur.UnixNano != 42 {
		t.Errorf("after reopen = (%d, %+v), want (42, ts 42)", seq, cur)
	}
	if done, _ := s2.IsShardDone("db", "2"); !done {
		t.Error("after reopen shard 2 not done")
	}
}
