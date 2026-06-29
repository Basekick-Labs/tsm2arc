// Package buckets recovers InfluxDB 2.x bucket-id → name mappings from an
// influxd.bolt metadata file, so a 2.x migration can target Arc databases by
// readable bucket NAME rather than the 16-hex bucket ID used in the on-disk
// directory layout.
//
// The mapping is needed only for 2.x; 1.x directories are already named by
// database. On a cold-volume migration the bolt file must be copied alongside
// the engine/ directory — without it, names are unrecoverable and the caller
// falls back to bucket IDs.
package buckets

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// bucketsBucket is the bbolt bucket holding bucket metadata records in
// influxd.bolt (InfluxDB 2.x). Values are JSON-encoded bucket records.
const bucketsBucket = "bucketsv1"

// record is the subset of an InfluxDB 2.x bucket record we need. type 0 is a
// user bucket; type 1 is a system bucket (_monitoring, _tasks) — skipped.
type record struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type int    `json:"type"`
}

// Mapping maps bucket ID (the on-disk directory name) → bucket name, and records
// which IDs are system buckets so callers can skip them.
type Mapping struct {
	idToName map[string]string
	system   map[string]bool
}

// Name returns the bucket name for an ID, or the ID itself if unknown (so a
// migration still proceeds with the ID as the target name).
func (m *Mapping) Name(id string) string {
	if m == nil {
		return id
	}
	if n, ok := m.idToName[id]; ok {
		return n
	}
	return id
}

// IsSystem reports whether a bucket ID is an InfluxDB system bucket.
func (m *Mapping) IsSystem(id string) bool {
	if m == nil {
		return false
	}
	return m.system[id]
}

// Empty reports whether no bucket names were loaded (missing/unreadable bolt, or
// a schema with no user buckets). When empty, names fall back to IDs and system
// buckets cannot be identified — callers should warn.
func (m *Mapping) Empty() bool {
	return m == nil || len(m.idToName) == 0
}

// Load reads bucket metadata from an influxd.bolt file. It opens a COPY of the
// file (bbolt takes an exclusive lock; the original may be in use or on a
// read-only mount), so callers can point it straight at the source volume. A
// missing file is not an error — it returns a nil-safe empty Mapping so the
// caller transparently falls back to bucket IDs.
func Load(boltPath string) (*Mapping, error) {
	if _, err := os.Stat(boltPath); err != nil {
		if os.IsNotExist(err) {
			return &Mapping{idToName: map[string]string{}, system: map[string]bool{}}, nil
		}
		return nil, err
	}

	// Copy aside: bbolt wants an exclusive flock and may refuse a file that is
	// locked or on a read-only mount. Read-only open still flocks shared, which
	// can fail against a concurrently-open DB; a private copy is the robust path.
	tmp, err := copyToTemp(boltPath)
	if err != nil {
		return nil, fmt.Errorf("copy influxd.bolt: %w", err)
	}
	defer os.Remove(tmp)

	db, err := bolt.Open(tmp, 0o600, &bolt.Options{ReadOnly: true, Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open influxd.bolt: %w", err)
	}
	defer db.Close()

	m := &Mapping{idToName: map[string]string{}, system: map[string]bool{}}
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketsBucket))
		if b == nil {
			// Schema differs / not a 2.x bolt — leave mapping empty (fall back to IDs).
			return nil
		}
		return b.ForEach(func(_, v []byte) error {
			var rec record
			if err := json.Unmarshal(v, &rec); err != nil {
				return nil // skip records we can't parse; don't fail the whole load
			}
			if rec.ID == "" || rec.Name == "" {
				return nil
			}
			m.idToName[rec.ID] = rec.Name
			if rec.Type != 0 {
				m.system[rec.ID] = true
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return m, nil
}

// maxBoltSize caps the influxd.bolt copy. The metadata DB is normally a few MB;
// 512 MB is far above any real instance and guards against filling the temp dir
// from a corrupt/sparse file.
const maxBoltSize = 512 << 20

func copyToTemp(src string) (string, error) {
	in, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer in.Close()

	tmp, err := os.CreateTemp("", "tsm2arc-bolt-*.bolt")
	if err != nil {
		return "", err
	}
	// Cap the copy: read at most maxBoltSize+1 and reject if it overflows.
	n, err := io.Copy(tmp, io.LimitReader(in, maxBoltSize+1))
	if err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", err
	}
	if n > maxBoltSize {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", fmt.Errorf("influxd.bolt exceeds %d bytes — refusing (corrupt file?)", maxBoltSize)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return "", err
	}
	return tmp.Name(), nil
}

// DefaultBoltPath guesses the influxd.bolt location given an engine/data dir.
// 2.x layout is <root>/engine/data and <root>/influxd.bolt, so from a datadir
// of <root>/engine/data the bolt is two levels up.
func DefaultBoltPath(dataDir string) string {
	// dataDir = <root>/engine/data  →  <root>/influxd.bolt
	return filepath.Join(filepath.Dir(filepath.Dir(dataDir)), "influxd.bolt")
}
