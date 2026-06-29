// Package discover walks an InfluxDB data directory and enumerates the shards to
// migrate, grouped by source database/bucket.
//
// The on-disk shard layout is the same shape for InfluxDB 1.x and 2.x:
//
//	1.x:  <datadir>/<db>/<rp>/<shardID>/*.tsm           (datadir = <root>/data)
//	2.x:  <datadir>/<bucket-id>/autogen/<shardID>/*.tsm (datadir = <root>/engine/data)
//
// so the same Walk handles both — the only difference is that 2.x names the
// top-level directory by bucket ID (resolved to a name via the buckets package)
// and always uses the "autogen" retention slot. The matching WAL lives under
// <waldir>/<db|bucket>/<rp>/<shardID>/*.wal.
package discover

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Version identifies the detected InfluxDB on-disk layout.
type Version int

const (
	VersionUnknown Version = iota
	Version1
	Version2
)

func (v Version) String() string {
	switch v {
	case Version1:
		return "1.x"
	case Version2:
		return "2.x"
	default:
		return "unknown"
	}
}

// Detect inspects a path and resolves it to the TSM data directory plus the
// detected InfluxDB version. It accepts either the precise data dir or a sensible
// parent, so operators can point --datadir at the obvious place for each version:
//
//	2.x: <root> (containing engine/), <root>/engine, or <root>/engine/data
//	1.x: <root> (containing data/) or <root>/data
//
// Returns the resolved data dir (the one whose children are db/bucket dirs) and
// the version. dataDir is returned unchanged with VersionUnknown if nothing
// recognizable is found (the caller can still try it as a raw 1.x data dir).
func Detect(path string) (dataDir string, version Version) {
	// Check 2.x FIRST and require the structural marker (16-hex bucket id +
	// autogen): a 1.x tree can't satisfy isEngineData (db names aren't 16-hex),
	// and a 2.x engine/data won't match looksLike1x at the same path, so the
	// 2.x-first order is unambiguous. (A 1.x db literally named like a 16-hex
	// string with an autogen rp is the only theoretical overlap; that would be
	// detected as 2.x and its bucket-name lookup would simply fall back to the
	// id — extraction is identical either way, so no data is lost.)
	for _, p := range []string{
		filepath.Join(path, "engine", "data"),
		filepath.Join(path, "data"), // path == <root>/engine
		path,                        // path == <root>/engine/data
	} {
		if isEngineData(p) {
			return p, Version2
		}
	}
	// 1.x: a data dir whose children are <db>/<rp>/<shard>
	for _, p := range []string{filepath.Join(path, "data"), path} {
		if looksLike1x(p) {
			return p, Version1
		}
	}
	return path, VersionUnknown
}

// isEngineData reports whether dir is a 2.x engine/data dir: its child dirs are
// 16-hex bucket IDs each containing an "autogen" subdir.
func isEngineData(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		if !e.IsDir() || !isBucketID(e.Name()) {
			continue
		}
		if fi, err := os.Stat(filepath.Join(dir, e.Name(), "autogen")); err == nil && fi.IsDir() {
			return true
		}
	}
	return false
}

// looksLike1x reports whether dir resembles a 1.x data dir: a child db dir with
// a retention-policy subdir (any name) that holds a shard dir.
func looksLike1x(dir string) bool {
	dbs, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, db := range dbs {
		if !db.IsDir() {
			continue
		}
		rps, err := os.ReadDir(filepath.Join(dir, db.Name()))
		if err != nil {
			continue
		}
		for _, rp := range rps {
			if rp.IsDir() && rp.Name() != "_series" {
				return true
			}
		}
	}
	return false
}

// isBucketID reports whether s is a 16-char lowercase-hex InfluxDB platform ID.
func isBucketID(s string) bool {
	if len(s) != 16 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// Shard is one InfluxDB shard directory and its TSM/WAL files.
type Shard struct {
	// SourceID is the STABLE on-disk identity of the shard's database/bucket: the
	// 1.x database name, or the 2.x bucket ID. It never changes across runs and
	// is what the checkpoint keys on, so resume stays correct even if the
	// bucket-id→name mapping (influxd.bolt) is absent on a later run.
	SourceID string
	// Database is the display/routing name — the 1.x database name, or the 2.x
	// bucket NAME once resolved from influxd.bolt (falls back to the bucket ID).
	// This is what data is written to in Arc and what --db-map/--database-filter
	// operate on. It must NOT be used as a checkpoint key.
	Database  string
	Retention string
	ShardID   string
	Dir       string   // absolute path to the shard directory under datadir
	TSMFiles  []string // sorted absolute paths
	WALFiles  []string // sorted absolute paths (may be empty)
}

// Walk enumerates shards under datadir, attaching WAL files from waldir when it
// is non-empty. Results are deterministic: sorted by (database, retention,
// shardID), and files within a shard sorted by name.
func Walk(datadir, waldir string, dbFilter map[string]bool, includeInternal bool) ([]Shard, error) {
	var shards []Shard

	dbs, err := os.ReadDir(datadir)
	if err != nil {
		return nil, err
	}
	for _, dbe := range dbs {
		if !dbe.IsDir() {
			continue
		}
		db := dbe.Name()
		// _internal is InfluxDB's own monitoring database — not user data.
		// Skip it by default; --include-internal overrides.
		if db == "_internal" && !includeInternal {
			continue
		}
		if len(dbFilter) > 0 && !dbFilter[db] {
			continue
		}
		rps, err := os.ReadDir(filepath.Join(datadir, db))
		if err != nil {
			return nil, err
		}
		for _, rpe := range rps {
			if !rpe.IsDir() {
				continue
			}
			rp := rpe.Name()
			shardDirs, err := os.ReadDir(filepath.Join(datadir, db, rp))
			if err != nil {
				return nil, err
			}
			for _, sde := range shardDirs {
				if !sde.IsDir() {
					continue
				}
				shardID := sde.Name()
				dir := filepath.Join(datadir, db, rp, shardID)
				tsm := globSorted(dir, ".tsm")

				var walFiles []string
				if waldir != "" {
					wdir := filepath.Join(waldir, db, rp, shardID)
					walFiles = globSorted(wdir, ".wal")
				}

				// Include a shard if it has TSM OR (non-empty) WAL data. A shard
				// whose data was never flushed to TSM lives ONLY in the WAL —
				// skipping it (the old TSM-only rule) would silently drop data on
				// cold-volume migrations. Empty WAL segments (0 bytes) don't count.
				if len(tsm) == 0 && !hasData(walFiles) {
					continue
				}
				shards = append(shards, Shard{
					SourceID:  db, // stable: 1.x db name OR 2.x bucket id (the dir name)
					Database:  db, // 2.x: rewritten to the resolved bucket name by the caller
					Retention: rp,
					ShardID:   shardID,
					Dir:       dir,
					TSMFiles:  tsm,
					WALFiles:  walFiles,
				})
			}
		}
	}

	sort.Slice(shards, func(i, j int) bool {
		a, b := shards[i], shards[j]
		if a.Database != b.Database {
			return a.Database < b.Database
		}
		if a.Retention != b.Retention {
			return a.Retention < b.Retention
		}
		return a.ShardID < b.ShardID
	})
	return shards, nil
}

// hasData reports whether any of the given files is non-empty. InfluxDB opens a
// fresh 0-byte WAL segment for new writes after a flush; those carry no data and
// must not cause an otherwise-empty shard to be migrated.
func hasData(files []string) bool {
	for _, f := range files {
		if fi, err := os.Stat(f); err == nil && fi.Size() > 0 {
			return true
		}
	}
	return false
}

func globSorted(dir, ext string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ext) {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(out)
	return out
}
