package buckets

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func writeBolt(t *testing.T, recs []string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "influxd.bolt")
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketsBucket))
		if err != nil {
			return err
		}
		for i, r := range recs {
			if err := b.Put([]byte{byte(i)}, []byte(r)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	return path
}

func TestLoadResolvesNamesAndSystem(t *testing.T) {
	path := writeBolt(t, []string{
		`{"id":"f549d32159486e62","name":"telemetry","type":0}`,
		`{"id":"26d752b14fa6b482","name":"_tasks","type":1}`,
		`{"id":"49e58572939d28ee","name":"_monitoring","type":1}`,
	})
	m, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Name("f549d32159486e62"); got != "telemetry" {
		t.Errorf("name = %q, want telemetry", got)
	}
	if m.IsSystem("f549d32159486e62") {
		t.Error("user bucket flagged system")
	}
	if !m.IsSystem("26d752b14fa6b482") || !m.IsSystem("49e58572939d28ee") {
		t.Error("system buckets not flagged")
	}
	// unknown id falls back to itself
	if got := m.Name("deadbeefdeadbeef"); got != "deadbeefdeadbeef" {
		t.Errorf("unknown id = %q, want passthrough", got)
	}
}

// A missing bolt file is not an error — Load returns a nil-safe empty mapping so
// the caller transparently falls back to bucket IDs.
func TestLoadMissingFileFallsBack(t *testing.T) {
	m, err := Load(filepath.Join(t.TempDir(), "nope.bolt"))
	if err != nil {
		t.Fatalf("missing file should not error: %v", err)
	}
	if got := m.Name("f549d32159486e62"); got != "f549d32159486e62" {
		t.Errorf("fallback name = %q, want the id", got)
	}
}

// Load must open even while another handle holds the DB (copy-aside), since a
// cold volume's bolt may be locked or read-only.
func TestLoadCopiesAside(t *testing.T) {
	path := writeBolt(t, []string{`{"id":"aaaaaaaaaaaaaaaa","name":"b1","type":0}`})
	// hold an open handle (exclusive lock) to simulate a busy/locked DB
	held, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()

	m, err := Load(path)
	if err != nil {
		t.Fatalf("Load should copy aside and succeed despite the lock: %v", err)
	}
	if got := m.Name("aaaaaaaaaaaaaaaa"); got != "b1" {
		t.Errorf("name = %q, want b1", got)
	}
}

// nil Mapping is safe to use (defensive).
func TestNilMapping(t *testing.T) {
	var m *Mapping
	if m.Name("x") != "x" || m.IsSystem("x") || !m.Empty() {
		t.Error("nil mapping not safe")
	}
}

// H-2 support: Empty() drives the "system buckets can't be skipped" warning.
func TestEmpty(t *testing.T) {
	// missing file → empty
	m, _ := Load(filepath.Join(t.TempDir(), "nope.bolt"))
	if !m.Empty() {
		t.Error("missing-file mapping should be Empty")
	}
	// populated → not empty
	path := writeBolt(t, []string{`{"id":"aaaaaaaaaaaaaaaa","name":"b1","type":0}`})
	m2, _ := Load(path)
	if m2.Empty() {
		t.Error("populated mapping should not be Empty")
	}
}

// M-2: a corrupt / non-bolt file must return an error (the caller turns it into
// a warn + fallback) rather than panicking.
func TestLoadCorruptFileErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.bolt")
	if err := os.WriteFile(path, []byte("this is not a bolt file at all"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Error("expected error on a non-bolt file")
	}
}
