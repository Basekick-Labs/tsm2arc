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

func TestCommittedSeqEmpty(t *testing.T) {
	s := openTemp(t)
	seq, err := s.CommittedSeq("db", "1")
	if err != nil {
		t.Fatal(err)
	}
	if seq != -1 {
		t.Errorf("empty shard seq = %d, want -1", seq)
	}
}

func TestCommitAdvances(t *testing.T) {
	s := openTemp(t)
	for i := 0; i < 5; i++ {
		if err := s.Commit("db", "1", i, 100); err != nil {
			t.Fatal(err)
		}
	}
	seq, _ := s.CommittedSeq("db", "1")
	if seq != 4 {
		t.Errorf("seq = %d, want 4", seq)
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

// Commit must never move progress backwards (defensive against replayed commits).
func TestCommitNeverRegresses(t *testing.T) {
	s := openTemp(t)
	s.Commit("db", "1", 10, 0)
	s.Commit("db", "1", 3, 0) // out of order / replay
	seq, _ := s.CommittedSeq("db", "1")
	if seq != 10 {
		t.Errorf("seq regressed to %d, want 10", seq)
	}
}

func TestShardIsolation(t *testing.T) {
	s := openTemp(t)
	s.Commit("db", "1", 7, 0)
	s.Commit("db", "2", 2, 0)
	s.Commit("other", "1", 99, 0)
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
	if err := s.MarkShardDone("db", "1", 3, 300); err != nil {
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
	s1.Commit("db", "1", 42, 1000)
	s1.MarkShardDone("db", "2", 5, 500)
	s1.Close()

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if v, _ := s2.CommittedSeq("db", "1"); v != 42 {
		t.Errorf("after reopen seq = %d, want 42", v)
	}
	if done, _ := s2.IsShardDone("db", "2"); !done {
		t.Error("after reopen shard 2 not done")
	}
}
