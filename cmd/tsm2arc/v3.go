package main

import (
	"context"
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
	// walDecodedMax is the highest WAL sequence folded into extraction (0
	// when nothing was decoded); part of the config fingerprint because
	// decoded WAL rows shape chunk boundaries.
	walDecodedMax uint64
	// compactor reports Enterprise compactor state in the store — the
	// refuse signal for loads; --analyze reports it instead.
	compactor bool
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

// setupV3 resolves a LOCAL InfluxDB 3 store into synthesized shards plus the
// v3Source the extraction seam uses.
func setupV3(root string, opt v3Options, infow io.Writer) ([]discover.Shard, *v3Source, error) {
	root, node, cluster, err := v3NodePrefix(root)
	if err != nil {
		return nil, nil, err
	}
	return setupV3FS(vfs.NewLocal(root), node, cluster, filepath.Join(root, node), opt, infow)
}

// setupV3S3 is setupV3 for an s3://bucket[/prefix] store, reading it in
// place with listings and range GETs — no scratch-volume sync.
func setupV3S3(ctx context.Context, rawURL string, s3opt vfs.S3Options, opt v3Options, infow io.Writer) ([]discover.Shard, *v3Source, error) {
	f, err := vfs.NewS3(ctx, rawURL, s3opt)
	if err != nil {
		return nil, nil, err
	}
	node, cluster, f, err := v3NodeOnS3(f)
	if err != nil {
		return nil, nil, err
	}
	return setupV3FS(f, node, cluster, f.URL()+"/"+node, opt, infow)
}

// v3NodeOnS3 locates the single node prefix in an S3 store, normalizing a
// URL that points AT the node prefix back to the store root (manifest paths
// are node-prefixed, so the FS root must be the node's parent).
func v3NodeOnS3(f *vfs.S3) (string, string, *vfs.S3, error) {
	sub := func(prefix, m string) string {
		if prefix == "" {
			return m
		}
		return prefix + "/" + m
	}
	markers := func(fs *vfs.S3, prefix string) int {
		n := 0
		for _, m := range []string{"wal", "snapshots", "dbs", "catalog", "catalogs"} {
			if vfs.HasPrefix(fs, sub(prefix, m)) {
				n++
			}
		}
		return n
	}
	if markers(f, "") >= 2 {
		// URL points at the node prefix itself; rebase to its parent.
		pfx := f.Prefix()
		i := strings.LastIndexByte(pfx, '/')
		if pfx == "" {
			return "", "", nil, fmt.Errorf("%s holds the node's contents at the bucket root, but InfluxDB 3 object paths are node-prefixed (node_id/dbs/...); sync or point at the parent so the node prefix is preserved", f.URL())
		}
		var parent, node string
		if i < 0 {
			parent, node = "", pfx
		} else {
			parent, node = pfx[:i], pfx[i+1:]
		}
		return node, "", vfs.NewS3Rebased(f, parent), nil
	}
	children, err := f.ListDir("")
	if err != nil {
		return "", "", nil, err
	}
	var nodes []string
	var cluster string
	pacha := false
	for _, c := range children {
		switch {
		case markers(f, c) >= 2:
			nodes = append(nodes, c)
		case vfs.HasPrefix(f, c+"/cv2") || vfs.HasPrefix(f, c+"/pt_snapshots"):
			pacha = true
		case vfs.HasPrefix(f, c+"/catalog") || vfs.HasPrefix(f, c+"/catalogs"):
			cluster = c
		}
	}
	switch len(nodes) {
	case 1:
		return nodes[0], cluster, f, nil
	case 0:
		if pacha {
			return "", "", nil, fmt.Errorf(
				"the store under %s uses the Pacha-tree storage engine (Enterprise 3.11+ .pt format), which is proprietary and cannot be read by this tool.\n"+
					"  Options:\n"+
					"    a) clusters UPGRADED from the Parquet engine retain their pre-upgrade parquet until 'influxdb3 cleanup-parquet' is run - point --datadir at that data BEFORE cleanup;\n"+
					"    b) export from the running server (influxdb3 query --format parquet preserves all type metadata and arrives deduplicated) - an export-ingestion mode is tracked in https://github.com/Basekick-Labs/tsm2arc/issues/10.",
				f.URL())
		}
		return "", "", nil, fmt.Errorf("no InfluxDB 3 node prefix found under %s", f.URL())
	default:
		return "", "", nil, fmt.Errorf("store has %d node prefixes (%s): multi-node stores are not supported yet (https://github.com/Basekick-Labs/tsm2arc/issues/10)", len(nodes), strings.Join(nodes, ", "))
	}
}

// setupV3FS is the FS-agnostic body shared by the local and S3 paths. label
// names the store in messages; cluster is the Enterprise cluster prefix when
// one sits next to the node prefix ("" otherwise). Fatal errors here are
// pre-flight: nothing has been sent yet.
func setupV3FS(f vfs.FS, node, cluster, label string, opt v3Options, infow io.Writer) ([]discover.Shard, *v3Source, error) {
	// Enterprise compactor state (cs/, cd/, c/ under the node) means the
	// compactor has run: gen1 parquet gets superseded and DELETED
	// (compaction-cleanup-wait, ~10 minutes), and the replacement data is
	// referenced only by the proprietary run-set index this tool cannot
	// read. Migrating around that is silent data loss, so the load refuses;
	// --analyze reports instead.
	compactor := vfs.HasPrefix(f, node+"/c") || vfs.HasPrefix(f, node+"/cs") || vfs.HasPrefix(f, node+"/cd")
	if compactor && !opt.analyze {
		return nil, nil, fmt.Errorf(
			"this is an InfluxDB 3 Enterprise store whose COMPACTOR has run (compactor state exists under %s/cs|cd|c).\n"+
				"  Compaction rewrites data into a proprietary format and deletes the original gen1 parquet shortly after;\n"+
				"  a migration from this store could silently miss whatever was already compacted.\n"+
				"  Supported recipes:\n"+
				"    a) run the ingest/query nodes WITHOUT compact mode (--mode ingest,query) and migrate that store;\n"+
				"    b) migrate a copy taken BEFORE the compactor first ran;\n"+
				"    c) export from the running server (influxdb3 query --format parquet).\n"+
				"  Details: https://github.com/Basekick-Labs/tsm2arc/issues/10.",
			node)
	}

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

	// Names: catalog first, operator flags on top. On Enterprise stores the
	// catalog lives under the CLUSTER prefix, not the node prefix — try the
	// node first (Core layout), then the cluster.
	cat, catErr := v3meta.LoadCatalog(f, node)
	if errors.Is(catErr, v3meta.ErrNoCatalog) && cluster != "" {
		cat, catErr = v3meta.LoadCatalog(f, cluster)
	}
	if catErr != nil && !errors.Is(catErr, v3meta.ErrBinaryCatalog) {
		return nil, nil, catErr
	}

	// Un-snapshotted WAL holds the newest writes (a clean server shutdown
	// does NOT flush them — no snapshot on shutdown). Decode it natively so
	// nothing is dropped; --skip-wal skips the decode explicitly. Decoding
	// needs the catalog's column-id map (WAL rows reference columns by id
	// only), which a binary 3.10+ catalog cannot provide yet.
	var walExtras map[v3meta.TableKey]map[int64][]extract3.MemRow
	var walDecodedMax uint64
	if n := len(wal.Unsnapshotted); n > 0 && !opt.analyze {
		switch {
		case opt.skipWAL:
			fmt.Fprintf(infow, "WARN: --skip-wal: leaving %d un-snapshotted WAL file(s) behind (sequences %d-%d) — the newest writes will be MISSING from Arc\n",
				n, wal.Unsnapshotted[0], wal.Unsnapshotted[len(wal.Unsnapshotted)-1])
		case cat == nil || cat.Stale:
			return nil, nil, fmt.Errorf(
				"%d WAL file(s) (sequences %d-%d) hold writes not yet persisted to any parquet file, and the catalog is the binary 3.10+ format, so the WAL's column ids cannot be named.\n"+
					"  Options: run the server until a snapshot NEWER than WAL sequence %d appears under %s/snapshots/ (a brief restart is not enough), then re-run;\n"+
					"  or accept the gap explicitly with --skip-wal. Binary-catalog decoding is planned (https://github.com/Basekick-Labs/tsm2arc/issues/10).",
				n, wal.Unsnapshotted[0], wal.Unsnapshotted[len(wal.Unsnapshotted)-1], wal.MaxSeq, node)
		default:
			rows := 0
			walExtras, rows, err = decodeWAL(f, node, wal.Unsnapshotted, cat)
			if err != nil {
				return nil, nil, fmt.Errorf(
					"decoding %d un-snapshotted WAL file(s) (sequences %d-%d): %w\n"+
						"  Fallbacks: run the server until a newer snapshot appears, then re-run; or accept the gap with --skip-wal.",
					n, wal.Unsnapshotted[0], wal.Unsnapshotted[len(wal.Unsnapshotted)-1], err)
			}
			walDecodedMax = wal.MaxSeq
			fmt.Fprintf(infow, "decoded %d row(s) from %d un-snapshotted WAL file(s) (sequences %d-%d); nothing left behind\n",
				rows, n, wal.Unsnapshotted[0], wal.Unsnapshotted[len(wal.Unsnapshotted)-1])
		}
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

	src := &v3Source{fs: f, node: node, tables: map[string]extract3.Table{}, live: live, wal: wal, catalog: cat, skipWAL: opt.skipWAL, snapshots: len(snaps), walDecodedMax: walDecodedMax, compactor: compactor}
	var shards []discover.Shard
	var unresolved []string
	// Iterate the union of live-set tables and WAL-only tables: a table whose
	// data was never snapshotted has no parquet files yet and exists ONLY in
	// the decoded WAL.
	allTables := map[v3meta.TableKey]bool{}
	for k := range live.Tables {
		allTables[k] = true
	}
	for k := range walExtras {
		allTables[k] = true
	}
	for k := range allTables {
		files := live.Tables[k]
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
			Dir:       label,
		}
		src.tables[sh.SourceID+"/"+sh.ShardID] = extract3.Table{
			Key:         k,
			Measurement: tblName,
			SeriesKey:   seriesKey,
			Files:       fl,
			Extra:       walExtras[k],
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
func v3NodePrefix(root string) (normRoot, node, cluster string, err error) {
	has := func(parts ...string) bool {
		fi, err := os.Stat(filepath.Join(parts...))
		return err == nil && fi.IsDir()
	}
	markers := func(dir string) int {
		n := 0
		for _, m := range []string{"wal", "snapshots", "dbs", "catalog", "catalogs"} {
			if has(dir, m) {
				n++
			}
		}
		return n
	}
	if markers(root) >= 2 {
		abs, err := filepath.Abs(root)
		if err != nil {
			return "", "", "", err
		}
		return filepath.Dir(abs), filepath.Base(abs), "", nil
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", "", "", err
	}
	var nodes []string
	pacha := false
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(root, e.Name())
		switch {
		case markers(dir) >= 2:
			nodes = append(nodes, e.Name())
		case has(dir, "cv2") || has(dir, "pt_snapshots"):
			// Pacha-tree engine artifacts (Enterprise 3.11+ new clusters).
			pacha = true
		case has(dir, "catalog") || has(dir, "catalogs"):
			// Enterprise CLUSTER prefix: the catalog (and license/config)
			// live here, next to the node prefixes.
			cluster = e.Name()
		}
	}
	switch len(nodes) {
	case 1:
		return root, nodes[0], cluster, nil
	case 0:
		if pacha {
			return "", "", "", fmt.Errorf(
				"the store under %s uses the Pacha-tree storage engine (Enterprise 3.11+ .pt format), which is proprietary and cannot be read by this tool.\n"+
					"  Options:\n"+
					"    a) clusters UPGRADED from the Parquet engine retain their pre-upgrade parquet until 'influxdb3 cleanup-parquet' is run - point --datadir at that data BEFORE cleanup;\n"+
					"    b) export from the running server (influxdb3 query --format parquet preserves all type metadata and arrives deduplicated) - an export-ingestion mode is tracked in https://github.com/Basekick-Labs/tsm2arc/issues/10.",
				root)
		}
		return "", "", "", fmt.Errorf("no InfluxDB 3 node prefix found under %s", root)
	default:
		sort.Strings(nodes)
		return "", "", "", fmt.Errorf("store has %d node prefixes (%s): multi-node stores are not supported yet (https://github.com/Basekick-Labs/tsm2arc/issues/10)", len(nodes), strings.Join(nodes, ", "))
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
	if s.walDecodedMax > 0 {
		fmt.Fprintf(h, ";waldecoded=%d", s.walDecodedMax)
	}
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
	CompactorState   bool             `json:"enterprise_compactor_state"`
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
		CompactorState:   src.compactor,
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
		fmt.Printf("  NOTE: un-snapshotted WAL holds the newest writes; a load decodes it natively (or skips it with --skip-wal)\n")
	}
	if rep.CompactorState {
		fmt.Printf("  WARNING: Enterprise COMPACTOR state present - compacted data is unreadable and gen1 files get deleted; a load will refuse (see the supported recipes in the refusal message)\n")
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
