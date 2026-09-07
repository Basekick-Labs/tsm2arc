package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/basekick-labs/tsm2arc/internal/discover"
	"github.com/basekick-labs/tsm2arc/internal/extract"
	"github.com/basekick-labs/tsm2arc/internal/extract3"
	"github.com/basekick-labs/tsm2arc/internal/v3meta"
	"github.com/basekick-labs/tsm2arc/internal/vfs"
)

// v3Source carries everything the run needs to extract from an InfluxDB 3
// object store. It plugs into the existing pipeline at exactly one seam
// (forEachPoint); chunking, sending, checkpointing, census, and audit are
// untouched.
type v3Source struct {
	fs        vfs.FS
	node      string
	tables    map[string]extract3.Table // keyed by SourceID+"/"+ShardID
	live      *v3meta.LiveSet
	wal       *v3meta.WALStatus
	catalog   *v3meta.Catalog // nil when the catalog is binary and flags covered it
	skipWAL   bool
	snapshots int
}

// v3Options are the operator inputs specific to v3 stores.
type v3Options struct {
	skipWAL  bool
	dbNames  []string // --v3-db id=name
	tables   []string // --v3-table db/table=name:tag1,tag2
	dbFilter map[string]bool
	internal bool // --include-internal
	analyze  bool // analyze mode reports problems instead of refusing
	dryRun   bool
}

// setupV3 resolves an InfluxDB 3 store into synthesized shards (one per live
// table) plus the v3Source the extraction seam uses. Fatal errors here are
// pre-flight: nothing has been sent yet.
func setupV3(root string, opt v3Options, infow io.Writer) ([]discover.Shard, *v3Source, error) {
	root, node, err := v3NodePrefix(root)
	if err != nil {
		return nil, nil, err
	}
	f := vfs.NewLocal(root)

	snaps, err := v3meta.LoadSnapshots(f, node)
	if err != nil {
		return nil, nil, fmt.Errorf("read snapshot manifests: %w", err)
	}
	indexes, deltas, err := v3meta.LoadTableIndexes(f, node)
	if err != nil {
		return nil, nil, fmt.Errorf("read table indexes: %w", err)
	}
	live := v3meta.BuildLiveSet(snaps, indexes, deltas)

	wal, err := v3meta.CheckWAL(f, node, live.WALFileSequenceNumber)
	if err != nil {
		return nil, nil, err
	}
	if n := len(wal.Unsnapshotted); n > 0 && !opt.analyze {
		if !opt.skipWAL {
			return nil, nil, fmt.Errorf(
				"%d WAL file(s) (sequences %d-%d) hold writes not yet persisted to any parquet file — up to ~10 minutes of the newest data.\n"+
					"  A clean server shutdown does NOT flush them (InfluxDB 3 never snapshots on shutdown). Options:\n"+
					"    a) run the server until a snapshot NEWER than WAL sequence %d appears under %s/snapshots/ (a brief restart is not enough), then re-run;\n"+
					"    b) accept the gap explicitly with --skip-wal (the skipped span is printed and your Arc data will be missing those writes);\n"+
					"    c) native WAL decoding is planned (https://github.com/Basekick-Labs/tsm2arc/issues/10).\n"+
					"  Note: WAL files holding only no-op entries trip this gate conservatively.",
				n, wal.Unsnapshotted[0], wal.Unsnapshotted[len(wal.Unsnapshotted)-1], wal.MaxSeq, node)
		}
		fmt.Fprintf(infow, "WARN: --skip-wal: leaving %d un-snapshotted WAL file(s) behind (sequences %d-%d) — the newest writes will be MISSING from Arc\n",
			n, wal.Unsnapshotted[0], wal.Unsnapshotted[len(wal.Unsnapshotted)-1])
	}

	// Names: catalog first, operator flags on top. A binary (3.10+) catalog
	// with no preserved JSON fallback is fine as long as the flags cover
	// every live table.
	cat, catErr := v3meta.LoadCatalog(f, node)
	if catErr != nil && !errors.Is(catErr, v3meta.ErrBinaryCatalog) {
		return nil, nil, catErr
	}
	if cat != nil && cat.Stale {
		fmt.Fprintf(infow, "WARN: catalog is the binary 3.10+ format; using the preserved pre-migration JSON catalog — correct for data written before the upgrade, blind to databases/tables created after it. Override with --v3-db/--v3-table if needed.\n")
	}
	if cat != nil && len(cat.UnknownOps) > 0 {
		fmt.Fprintf(infow, "WARN: catalog log replay skipped unrecognized operation(s) %v; names may be stale (e.g. a rename)\n", cat.UnknownOps)
	}
	names, err := v3Names(cat, opt.dbNames, opt.tables)
	if err != nil {
		return nil, nil, err
	}

	// Reconcile the live set against the store: metadata can reference
	// objects that retention, a hard delete, or (Enterprise) the compactor
	// already removed. Migrating around a hole silently is data loss, so the
	// load refuses; --analyze reports instead.
	rec, err := v3meta.Reconcile(f, node, live)
	if err != nil {
		return nil, nil, err
	}
	if len(rec.Dangling) > 0 && !opt.analyze {
		var parts []string
		for k, fs := range rec.Dangling {
			parts = append(parts, fmt.Sprintf("%s: %d file(s)", names.label(k), len(fs)))
		}
		sort.Strings(parts)
		return nil, nil, fmt.Errorf(
			"the store's metadata lists %d parquet file(s) that do not exist (%s).\n"+
				"  Retention, a hard delete, or - on Enterprise - the compactor removed data after the readable metadata was written;\n"+
				"  on a compacting Enterprise store the replacement files are referenced only by the proprietary compactor state and\n"+
				"  cannot be read by this tool. Run --analyze for the full picture; see https://github.com/Basekick-Labs/tsm2arc/issues/10.",
			danglingCount(rec), strings.Join(parts, ", "))
	}

	src := &v3Source{fs: f, node: node, tables: map[string]extract3.Table{}, live: live, wal: wal, catalog: cat, skipWAL: opt.skipWAL, snapshots: len(snaps)}
	var shards []discover.Shard
	var unresolved []string
	for k, files := range live.Tables {
		dbName, tblName, seriesKey, ok := names.resolve(k)
		if !ok {
			unresolved = append(unresolved, k.String())
			continue
		}
		if dbName == "_internal" && !opt.internal {
			continue
		}
		if len(opt.dbFilter) > 0 && !opt.dbFilter[dbName] {
			continue
		}
		fl := make([]v3meta.ParquetFile, 0, len(files))
		for _, pf := range files {
			fl = append(fl, pf)
		}
		sh := discover.Shard{
			SourceID:  fmt.Sprintf("v3db-%d", k.DBID), // stable id, never the name
			Database:  dbName,
			Retention: "gen1",
			ShardID:   strconv.FormatUint(uint64(k.TableID), 10),
			Dir:       filepath.Join(root, node),
		}
		src.tables[sh.SourceID+"/"+sh.ShardID] = extract3.Table{
			Key:         k,
			Measurement: tblName,
			SeriesKey:   seriesKey,
			Files:       fl,
		}
		shards = append(shards, sh)
	}
	if len(unresolved) > 0 {
		sort.Strings(unresolved)
		return nil, nil, fmt.Errorf(
			"cannot resolve names for %d live table(s): %s.\n"+
				"  The store's catalog is the binary 3.10+ format this build cannot read (and no usable JSON fallback exists).\n"+
				"  Provide the mapping yourself: --v3-db <db_id>=<name> and --v3-table <db_id>/<table_id>=<name>:<tag1>,<tag2>,...\n"+
				"  (tags in the order they were FIRST written - the table's series key; run the server's HTTP API or check your schema).\n"+
				"  Native binary-catalog support is planned (https://github.com/Basekick-Labs/tsm2arc/issues/10).",
			len(unresolved), strings.Join(unresolved, "; "))
	}
	sort.Slice(shards, func(i, j int) bool {
		if shards[i].Database != shards[j].Database {
			return shards[i].Database < shards[j].Database
		}
		return sortableTableID(shards[i].ShardID) < sortableTableID(shards[j].ShardID)
	})
	fmt.Fprintf(infow, "InfluxDB 3 store: %d live table(s) across %d snapshot(s), newest snapshot %d, WAL high-water %d\n",
		len(shards), len(snaps), live.NewestSnapshot, live.WALFileSequenceNumber)
	return shards, src, nil
}

func danglingCount(rec *v3meta.Reconciliation) int {
	n := 0
	for _, fs := range rec.Dangling {
		n += len(fs)
	}
	return n
}

func sortableTableID(s string) string { return fmt.Sprintf("%020s", s) }

// v3NodePrefix finds the single node prefix of a store, normalizing to the
// STORE root: manifests record object paths prefixed with the node id, so
// when --datadir points directly at the node directory, the effective root is
// its parent. Multi-node (Enterprise cluster) stores are refused for now:
// their tables span nodes and merging them correctly is future work.
func v3NodePrefix(root string) (normRoot, node string, err error) {
	markers := func(dir string) int {
		n := 0
		for _, m := range []string{"wal", "snapshots", "dbs", "catalog", "catalogs"} {
			if fi, err := os.Stat(filepath.Join(dir, m)); err == nil && fi.IsDir() {
				n++
			}
		}
		return n
	}
	if markers(root) >= 2 {
		abs, err := filepath.Abs(root)
		if err != nil {
			return "", "", err
		}
		return filepath.Dir(abs), filepath.Base(abs), nil
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", "", err
	}
	var nodes []string
	for _, e := range entries {
		if e.IsDir() && markers(filepath.Join(root, e.Name())) >= 2 {
			nodes = append(nodes, e.Name())
		}
	}
	switch len(nodes) {
	case 1:
		return root, nodes[0], nil
	case 0:
		return "", "", fmt.Errorf("no InfluxDB 3 node prefix found under %s", root)
	default:
		sort.Strings(nodes)
		return "", "", fmt.Errorf("store has %d node prefixes (%s): multi-node stores are not supported yet (https://github.com/Basekick-Labs/tsm2arc/issues/10)", len(nodes), strings.Join(nodes, ", "))
	}
}

// v3nameSet resolves table ids to (db name, table name, series key).
type v3nameSet struct {
	dbs       map[uint32]string
	tables    map[v3meta.TableKey]string
	seriesKey map[v3meta.TableKey][]string
}

func (n *v3nameSet) resolve(k v3meta.TableKey) (db, table string, key []string, ok bool) {
	db, okDB := n.dbs[k.DBID]
	table, okT := n.tables[k]
	key, okK := n.seriesKey[k]
	return db, table, key, okDB && okT && okK
}

func (n *v3nameSet) label(k v3meta.TableKey) string {
	if db, t, _, ok := n.resolve(k); ok {
		return db + "/" + t
	}
	return k.String()
}

// v3Names merges catalog names with --v3-db/--v3-table overrides.
func v3Names(cat *v3meta.Catalog, dbFlags, tableFlags []string) (*v3nameSet, error) {
	n := &v3nameSet{dbs: map[uint32]string{}, tables: map[v3meta.TableKey]string{}, seriesKey: map[v3meta.TableKey][]string{}}
	if cat != nil {
		for id, db := range cat.Databases {
			n.dbs[id] = db.Name
			for tid, t := range db.Tables {
				k := v3meta.TableKey{DBID: id, TableID: tid}
				n.tables[k] = t.Name
				n.seriesKey[k] = t.SeriesKey
			}
		}
	}
	for _, s := range dbFlags {
		id, name, ok := strings.Cut(s, "=")
		dbID, err := strconv.ParseUint(id, 10, 32)
		if !ok || err != nil || name == "" {
			return nil, fmt.Errorf("bad --v3-db %q: want <db_id>=<name>", s)
		}
		n.dbs[uint32(dbID)] = name
	}
	for _, s := range tableFlags {
		ids, rest, ok := strings.Cut(s, "=")
		dbStr, tblStr, ok2 := strings.Cut(ids, "/")
		name, tags, ok3 := strings.Cut(rest, ":")
		dbID, err1 := strconv.ParseUint(dbStr, 10, 32)
		tblID, err2 := strconv.ParseUint(tblStr, 10, 32)
		if !ok || !ok2 || !ok3 || err1 != nil || err2 != nil || name == "" {
			return nil, fmt.Errorf("bad --v3-table %q: want <db_id>/<table_id>=<name>:<tag1>,<tag2>,... (tags in first-write order; ':' with no tags for a tagless table)", s)
		}
		k := v3meta.TableKey{DBID: uint32(dbID), TableID: uint32(tblID)}
		n.tables[k] = name
		var key []string
		if tags != "" {
			key = strings.Split(tags, ",")
		}
		n.seriesKey[k] = key
	}
	return n, nil
}

// forEachPoint is the v3 side of the extraction seam.
func (s *v3Source) forEachPoint(cfg runConfig, sh discover.Shard, cur *extract.Cursor, onPoint func(extract.Point), onShardStats func(extract.Stats), prog *progress) error {
	tbl, ok := s.tables[sh.SourceID+"/"+sh.ShardID]
	if !ok {
		return fmt.Errorf("internal: no v3 table registered for %s/%s", sh.SourceID, sh.ShardID)
	}
	if prog != nil {
		prog.logf("table %s/%s: %d live parquet file(s)", sh.Database, tbl.Measurement, len(tbl.Files))
	} else if cfg.verbose {
		fmt.Printf("  table %s/%s: %d live parquet file(s)\n", sh.Database, tbl.Measurement, len(tbl.Files))
	}
	warn := func(format string, args ...any) {
		if prog != nil {
			prog.notef("WARN: "+format, args...)
		} else {
			fmt.Printf("WARN: "+format+"\n", args...)
		}
	}
	st, err := extract3.Extract(s.fs, tbl, cfg.start, cfg.end, cur, extract3.Options{Warn: warn}, onPoint)
	if err != nil {
		return err
	}
	if onShardStats != nil {
		onShardStats(st)
	}
	return nil
}

// v3Fingerprint hashes the live set and WAL mark into the config fingerprint:
// a store that gained or lost files between a run and its resume must fail as
// "source changed", not misalign chunk sequences or surface as a cryptic
// missing-cursor error mid-run.
func (s *v3Source) fingerprint() string {
	h := sha256.New()
	keys := make([]string, 0, len(s.live.Tables))
	for k := range s.live.Tables {
		keys = append(keys, fmt.Sprintf("%d/%d", k.DBID, k.TableID))
	}
	sort.Strings(keys)
	for _, ks := range keys {
		var k v3meta.TableKey
		fmt.Sscanf(ks, "%d/%d", &k.DBID, &k.TableID)
		files := s.live.Tables[k]
		ids := make([]string, 0, len(files))
		for _, pf := range files {
			ids = append(ids, fmt.Sprintf("%d:%s", pf.ID, pf.Path))
		}
		sort.Strings(ids)
		fmt.Fprintf(h, "%s=%s;", ks, strings.Join(ids, ","))
	}
	fmt.Fprintf(h, "wal=%d", s.live.WALFileSequenceNumber)
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// ---- --analyze for v3 ----

type v3AnalyzeTable struct {
	Database      string  `json:"database"`
	Table         string  `json:"table"`
	Files         int     `json:"files"`
	Bytes         uint64  `json:"bytes"`
	Rows          uint64  `json:"rows"`
	Buckets       int     `json:"buckets"`
	MaxFilesPer   int     `json:"max_files_per_bucket"`
	OverlapFactor float64 `json:"overlap_factor"` // files/buckets: >1 means cross-file dedup work
	MinTime       int64   `json:"min_time"`
	MaxTime       int64   `json:"max_time"`
	Dangling      int     `json:"dangling_files"`
}

type v3AnalyzeReport struct {
	Store            string           `json:"store"`
	Snapshots        int              `json:"snapshots"`
	NewestSnapshot   uint64           `json:"newest_snapshot"`
	WALFiles         int              `json:"wal_files"`
	UnsnapshottedWAL int              `json:"unsnapshotted_wal_files"`
	Tables           []v3AnalyzeTable `json:"tables"`
}

func runAnalyzeV3(cfg runConfig, root string) {
	src := cfg.v3
	rec, err := v3meta.Reconcile(src.fs, src.node, src.live)
	if err != nil {
		fatal("reconcile: %v", err)
	}
	rep := v3AnalyzeReport{
		Store:            root,
		NewestSnapshot:   src.live.NewestSnapshot,
		WALFiles:         src.wal.Files,
		UnsnapshottedWAL: len(src.wal.Unsnapshotted),
	}
	for _, sh := range cfg.shards {
		tbl := src.tables[sh.SourceID+"/"+sh.ShardID]
		row := v3AnalyzeTable{
			Database: redactIf(cfg.redact, "db", sh.Database),
			Table:    redactIf(cfg.redact, "table", tbl.Measurement),
			Files:    len(tbl.Files),
			Dangling: len(rec.Dangling[tbl.Key]),
			MinTime:  int64(1<<63 - 1),
			MaxTime:  int64(-1 << 63),
		}
		buckets := map[int64]int{}
		for _, pf := range tbl.Files {
			row.Bytes += pf.SizeBytes
			row.Rows += pf.RowCount
			buckets[pf.ChunkTime]++
			if pf.MinTime < row.MinTime {
				row.MinTime = pf.MinTime
			}
			if pf.MaxTime > row.MaxTime {
				row.MaxTime = pf.MaxTime
			}
		}
		row.Buckets = len(buckets)
		for _, n := range buckets {
			if n > row.MaxFilesPer {
				row.MaxFilesPer = n
			}
		}
		if row.Buckets > 0 {
			row.OverlapFactor = float64(row.Files) / float64(row.Buckets)
		}
		rep.Tables = append(rep.Tables, row)
	}
	rep.Snapshots = src.snapshots

	if cfg.jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(rep); err != nil {
			fatal("encode report: %v", err)
		}
		return
	}
	fmt.Printf("InfluxDB 3 store %s: %d snapshot(s), newest %d; %d WAL file(s), %d NOT yet snapshotted\n",
		rep.Store, rep.Snapshots, rep.NewestSnapshot, rep.WALFiles, rep.UnsnapshottedWAL)
	if rep.UnsnapshottedWAL > 0 {
		fmt.Printf("  WARNING: un-snapshotted WAL holds the newest writes; a load will refuse without --skip-wal\n")
	}
	for _, t := range rep.Tables {
		fmt.Printf("  %s/%s: %d file(s), %.1f MiB, %d row(s), %d bucket(s), overlap %.2f (max %d files/bucket)",
			t.Database, t.Table, t.Files, float64(t.Bytes)/(1<<20), t.Rows, t.Buckets, t.OverlapFactor, t.MaxFilesPer)
		if t.Dangling > 0 {
			fmt.Printf("  [%d DANGLING file(s): metadata lists them, store lacks them]", t.Dangling)
		}
		fmt.Println()
	}
}

func redactIf(redact bool, prefix, name string) string {
	if !redact {
		return name
	}
	return redactName(prefix, name)
}
