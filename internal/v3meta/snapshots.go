package v3meta

import (
	"encoding/json"
	"fmt"
	"math"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/basekick-labs/tsm2arc/internal/vfs"
)

// SnapshotSeqFromName recovers the snapshot sequence number from a
// snapshots/*.info.json object name. Filenames are u64::MAX minus the
// sequence, 20 digits zero-padded, so a lexicographic listing is newest-first.
func SnapshotSeqFromName(name string) (uint64, bool) {
	stem, ok := strings.CutSuffix(path.Base(name), ".info.json")
	if !ok || len(stem) != 20 {
		return 0, false
	}
	n, err := strconv.ParseUint(stem, 10, 64)
	if err != nil {
		return 0, false
	}
	return math.MaxUint64 - n, true
}

// LoadSnapshots reads every snapshot manifest under {node}/snapshots and
// returns them sorted by sequence number ascending (fold order).
func LoadSnapshots(f vfs.FS, node string) ([]PersistedSnapshot, error) {
	names, err := f.List(node + "/snapshots")
	if err != nil {
		return nil, err
	}
	snaps := make([]PersistedSnapshot, 0, len(names))
	for _, name := range names {
		seq, ok := SnapshotSeqFromName(name)
		if !ok {
			continue // foreign object in the prefix; not a manifest
		}
		b, err := f.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("read snapshot manifest %s: %w", name, err)
		}
		var s PersistedSnapshot
		if err := json.Unmarshal(b, &s); err != nil {
			return nil, fmt.Errorf("parse snapshot manifest %s: %w", name, err)
		}
		if s.Version != "1" {
			return nil, fmt.Errorf("snapshot manifest %s has unknown version %q (this build understands version 1; the store was written by a newer InfluxDB 3)", name, s.Version)
		}
		if s.SnapshotSequenceNumber != seq {
			return nil, fmt.Errorf("snapshot manifest %s: name encodes sequence %d but body says %d", name, seq, s.SnapshotSequenceNumber)
		}
		snaps = append(snaps, s)
	}
	sort.Slice(snaps, func(i, j int) bool {
		return snaps[i].SnapshotSequenceNumber < snaps[j].SnapshotSequenceNumber
	})
	return snaps, nil
}

// LoadTableIndexes reads every db-indices/{db}/{table}/index.info.json plus
// any not-yet-merged table-snapshots deltas. Real stores frequently have
// NEITHER (they are created lazily, e.g. by retention — a 3.11.2 store that
// has snapshotted still carries only the conversion marker), so an empty
// result is normal and means "fold the snapshot manifests instead".
func LoadTableIndexes(f vfs.FS, node string) (map[TableKey]CoreTableIndex, map[TableKey][]TableIndexSnapshot, error) {
	indexes := map[TableKey]CoreTableIndex{}
	names, err := f.List(node + "/db-indices")
	if err != nil {
		return nil, nil, err
	}
	for _, name := range names {
		if path.Base(name) != "index.info.json" {
			continue
		}
		b, err := f.ReadFile(name)
		if err != nil {
			return nil, nil, fmt.Errorf("read table index %s: %w", name, err)
		}
		var ix CoreTableIndex
		if err := json.Unmarshal(b, &ix); err != nil {
			return nil, nil, fmt.Errorf("parse table index %s: %w", name, err)
		}
		indexes[TableKey{DBID: ix.DBID, TableID: ix.TableID}] = ix
	}

	deltas := map[TableKey][]TableIndexSnapshot{}
	names, err = f.List(node + "/table-snapshots")
	if err != nil {
		return nil, nil, err
	}
	for _, name := range names {
		if !strings.HasSuffix(name, ".info.json") {
			continue
		}
		b, err := f.ReadFile(name)
		if err != nil {
			return nil, nil, fmt.Errorf("read table snapshot %s: %w", name, err)
		}
		var d TableIndexSnapshot
		if err := json.Unmarshal(b, &d); err != nil {
			return nil, nil, fmt.Errorf("parse table snapshot %s: %w", name, err)
		}
		k := TableKey{DBID: d.DBID, TableID: d.TableID}
		deltas[k] = append(deltas[k], d)
	}
	for k := range deltas {
		sort.Slice(deltas[k], func(i, j int) bool {
			return deltas[k][i].SnapshotSequenceNumber < deltas[k][j].SnapshotSequenceNumber
		})
	}
	return indexes, deltas, nil
}

// LiveSet is the resolved set of parquet objects to migrate, per table.
type LiveSet struct {
	// Tables maps each table to its live files keyed by parquet file id.
	Tables map[TableKey]map[uint64]ParquetFile
	// NewestSnapshot is the highest snapshot sequence folded in (0 if none).
	NewestSnapshot uint64
	// WALFileSequenceNumber is the newest snapshot's WAL high-water mark:
	// WAL files with a higher sequence hold data not yet in any parquet file.
	WALFileSequenceNumber uint64
}

// BuildLiveSet resolves the live parquet set from snapshot manifests plus
// (when present) table indexes.
//
// Where both exist for a table, the table index is the primary truth: it is
// REWRITTEN by retention and hard deletes, which purge parquet objects
// without ever updating old snapshot manifests — folding manifests alone
// would resurrect deleted files. Manifests newer than the index's
// latest_snapshot_sequence_number (plus unmerged table-snapshot deltas) are
// applied on top. Tables with no index fold every manifest in sequence order:
// adds first, then removed_files, per snapshot.
func BuildLiveSet(snaps []PersistedSnapshot, indexes map[TableKey]CoreTableIndex, deltas map[TableKey][]TableIndexSnapshot) *LiveSet {
	ls := &LiveSet{Tables: map[TableKey]map[uint64]ParquetFile{}}
	if n := len(snaps); n > 0 {
		ls.NewestSnapshot = snaps[n-1].SnapshotSequenceNumber
		ls.WALFileSequenceNumber = snaps[n-1].WALFileSequenceNumber
	}

	// Seed indexed tables from their index.
	indexFloor := map[TableKey]uint64{}
	for k, ix := range indexes {
		files := make(map[uint64]ParquetFile, len(ix.Files))
		for _, pf := range ix.Files {
			files[pf.ID] = pf
		}
		ls.Tables[k] = files
		indexFloor[k] = ix.LatestSnapshotSequenceNumber
	}

	apply := func(k TableKey, seq uint64, added []ParquetFile, removed []uint64) {
		if floor, ok := indexFloor[k]; ok && seq <= floor {
			return // already folded into the table index
		}
		files := ls.Tables[k]
		if files == nil {
			files = map[uint64]ParquetFile{}
			ls.Tables[k] = files
		}
		for _, pf := range added {
			files[pf.ID] = pf
		}
		for _, id := range removed {
			delete(files, id)
		}
	}

	for _, s := range snaps {
		seq := s.SnapshotSequenceNumber
		for k, added := range s.Databases {
			apply(k, seq, added, nil)
		}
		for k, removed := range s.RemovedFiles {
			ids := make([]uint64, len(removed))
			for i, pf := range removed {
				ids[i] = pf.ID
			}
			apply(k, seq, nil, ids)
		}
	}
	for k, ds := range deltas {
		for _, d := range ds {
			apply(k, d.SnapshotSequenceNumber, d.Files, d.RemovedFiles)
		}
	}
	return ls
}

// Reconciliation is the result of checking the live set against the objects
// actually present in the store.
type Reconciliation struct {
	// Dangling lists files the metadata claims are live but that do not exist
	// in the store, per table. Non-empty means retention, a hard delete, or —
	// on Enterprise — the compactor removed data after the metadata we could
	// read was written. The caller decides how loud to be; on Enterprise
	// stores this is the refuse signal.
	Dangling map[TableKey][]ParquetFile
	// Present counts live files confirmed to exist.
	Present int
}

// Reconcile verifies every live file exists under {node}/dbs with a single
// listing (no per-object stats).
func Reconcile(f vfs.FS, node string, ls *LiveSet) (*Reconciliation, error) {
	names, err := f.List(node + "/dbs")
	if err != nil {
		return nil, err
	}
	exists := make(map[string]bool, len(names))
	for _, n := range names {
		exists[n] = true
	}
	rec := &Reconciliation{Dangling: map[TableKey][]ParquetFile{}}
	for k, files := range ls.Tables {
		for _, pf := range files {
			if exists[pf.Path] {
				rec.Present++
			} else {
				rec.Dangling[k] = append(rec.Dangling[k], pf)
			}
		}
	}
	for k := range rec.Dangling {
		sort.Slice(rec.Dangling[k], func(i, j int) bool {
			return rec.Dangling[k][i].Path < rec.Dangling[k][j].Path
		})
	}
	return rec, nil
}
