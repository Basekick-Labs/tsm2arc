// Command tsm2arc extracts InfluxDB 1.x TSM data and loads it into Arc.
//
// Phase 1: discovery + extraction + --dry-run validation.
// Phase 2: chunked gzip load into Arc /api/v1/import/lp with per-database
//
//	routing. (Resume/checkpoint is Phase 3; WAL is Phase 4.)
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/tsm2arc/internal/buckets"
	"github.com/basekick-labs/tsm2arc/internal/checkpoint"
	"github.com/basekick-labs/tsm2arc/internal/chunk"
	"github.com/basekick-labs/tsm2arc/internal/discover"
	"github.com/basekick-labs/tsm2arc/internal/sink"
)

// Build metadata, injected by GoReleaser via -ldflags (-X main.version=…).
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

func main() {
	var (
		datadir      = flag.String("datadir", "", "InfluxDB data directory (…/data) [required]")
		waldir       = flag.String("waldir", "", "InfluxDB WAL directory [optional; auto-detected for 2.x]")
		boltFile     = flag.String("bolt", "", "InfluxDB 2.x influxd.bolt path for bucket names [optional; auto-detected]")
		arcURL       = flag.String("arc-url", "", "Arc base URL (e.g. https://arc.example.net) [required unless --dry-run]")
		token        = flag.String("token", os.Getenv("ARC_TOKEN"), "Arc admin token (or ARC_TOKEN env)")
		startStr     = flag.String("start", "", "only points >= this RFC3339 UTC time (filter)")
		endStr       = flag.String("end", "", "only points <= this RFC3339 UTC time (filter)")
		precision    = flag.String("precision", "ns", "LP timestamp precision: ns|us|ms|s")
		chunkBytes   = flag.Int("chunk-bytes", chunk.DefaultMaxBytes, "max raw LP bytes per chunk (<500MB)")
		checkpointDB = flag.String("checkpoint", "tsm2arc.checkpoint.db", "SQLite resume store path")
		workers      = flag.Int("workers", 2, "concurrent shards to migrate (each holds ~chunk-bytes; Arc buffers each server-side)")
		dryRun       = flag.Bool("dry-run", false, "extract + count, do not write to Arc")
		sampleN      = flag.Int("sample", 5, "print up to N sample LP lines per database (dry-run)")
		verbose      = flag.Bool("verbose", false, "verbose per-shard/chunk logging")
		inclInternal = flag.Bool("include-internal", false, "include InfluxDB's _internal monitoring database")
		showVersion  = flag.Bool("version", false, "print version and exit")
		dbFilterArg  multiFlag
		dbMapArg     multiFlag
	)
	flag.Var(&dbFilterArg, "database-filter", "only migrate this source database (repeatable)")
	flag.Var(&dbMapArg, "db-map", "rename source DB to Arc DB, form old=new (repeatable)")
	flag.Parse()

	if *showVersion {
		fmt.Printf("tsm2arc %s (commit %s, built %s)\n", version, commit, date)
		return
	}

	if *datadir == "" {
		fatal("--datadir is required")
	}
	if !*dryRun {
		if *arcURL == "" {
			fatal("--arc-url is required (or use --dry-run)")
		}
		if *token == "" {
			fatal("--token (or ARC_TOKEN) is required (or use --dry-run)")
		}
	}
	if *chunkBytes >= 500*1024*1024 {
		fatal("--chunk-bytes must be < 500MB (Arc import cap is on decompressed bytes)")
	}
	if *precision != "ns" && *precision != "us" && *precision != "ms" && *precision != "s" {
		fatal("--precision must be ns, us, ms, or s")
	}
	if *workers < 1 {
		fatal("--workers must be >= 1")
	}

	start, end := int64(math.MinInt64), int64(math.MaxInt64)
	if *startStr != "" {
		start = mustTime(*startStr, "--start")
	}
	if *endStr != "" {
		end = mustTime(*endStr, "--end")
	}

	dbMap := map[string]string{}
	for _, m := range dbMapArg {
		parts := strings.SplitN(m, "=", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			fatal("bad --db-map %q: want old=new", m)
		}
		dbMap[parts[0]] = parts[1]
	}

	// Detect 1.x vs 2.x and resolve the actual TSM data dir. For 2.x we also
	// auto-resolve the WAL dir (engine/wal) and load bucket-id → name from
	// influxd.bolt so shards key on readable names.
	resolvedData, ver := discover.Detect(*datadir)
	wd := *waldir
	var bucketMap *buckets.Mapping
	var err error
	switch ver {
	case discover.Version2:
		fmt.Printf("detected InfluxDB 2.x layout at %s\n", resolvedData)
		if wd == "" {
			// engine/wal sits next to engine/data
			cand := filepath.Join(filepath.Dir(resolvedData), "wal")
			if fi, err := os.Stat(cand); err == nil && fi.IsDir() {
				wd = cand
				fmt.Printf("using WAL dir %s\n", wd)
			}
		}
		boltPath := *boltFile
		if boltPath == "" {
			boltPath = buckets.DefaultBoltPath(resolvedData)
		}
		bucketMap, err = buckets.Load(boltPath)
		if err != nil {
			// A bad --bolt path shouldn't hard-fail a TB migration: warn and fall
			// back to bucket IDs (same as a missing bolt). The operator can re-run
			// with a correct --bolt to get names; data still migrates.
			fmt.Printf("WARN: could not read bucket metadata (%s): %v\n"+
				"      falling back to bucket IDs as database names; system buckets cannot be identified\n",
				boltPath, err)
			bucketMap = nil
		}
		if bucketMap.Empty() {
			fmt.Printf("WARN: no bucket name mapping (influxd.bolt at %s missing or empty).\n"+
				"      Buckets will migrate under their 16-hex IDs and the _monitoring/_tasks\n"+
				"      system buckets CANNOT be skipped. Provide --bolt or copy influxd.bolt\n"+
				"      next to the engine/ dir to get names and system-bucket filtering.\n", boltPath)
		}
	case discover.Version1:
		fmt.Printf("detected InfluxDB 1.x layout at %s\n", resolvedData)
	default:
		fmt.Printf("WARN: could not detect InfluxDB layout at %s; treating as a 1.x data dir\n", *datadir)
	}

	// For 1.x, filter by db name in discovery; for 2.x, discovery keys on bucket
	// IDs, so we filter AFTER resolving names below (pass an empty filter here).
	walkFilter := map[string]bool{}
	if ver != discover.Version2 {
		for _, d := range dbFilterArg {
			walkFilter[d] = true
		}
	}

	shards, err := discover.Walk(resolvedData, wd, walkFilter, *inclInternal)
	if err != nil {
		fatal("discovery failed: %v", err)
	}

	// 2.x: resolve bucket IDs → names on each shard, drop system buckets, and
	// apply the (name-based) database filter.
	if ver == discover.Version2 {
		nameFilter := map[string]bool{}
		for _, d := range dbFilterArg {
			nameFilter[d] = true
		}
		// Filter down in place (safe: write index never passes read index).
		// sh.SourceID stays the bucket ID (set by Walk); only Database (the
		// routing/display name) is rewritten to the resolved bucket name.
		resolved := shards[:0]
		for _, sh := range shards {
			// IsSystem only knows system buckets when the bolt mapping loaded; if
			// it's absent, system buckets fall through and migrate as hex IDs
			// (warned about above).
			if bucketMap.IsSystem(sh.SourceID) {
				continue
			}
			name := bucketMap.Name(sh.SourceID)
			if len(nameFilter) > 0 && !nameFilter[name] {
				continue
			}
			sh.Database = name
			resolved = append(resolved, sh)
		}
		shards = resolved
	}

	if len(shards) == 0 {
		fatal("no shards with TSM/WAL data found under %s", *datadir)
	}

	fmt.Printf("discovered %d shard(s) [%s]\n", len(shards), ver)
	if wd == "" {
		fmt.Println("NOTE: no WAL dir; uncompacted WAL data (if any) will not be read")
	}

	ctx := context.Background()
	cfg := runConfig{
		shards:    shards,
		start:     start,
		end:       end,
		chunkSize: *chunkBytes,
		dbMap:     dbMap,
		verbose:   *verbose,
		workers:   *workers,
	}

	if *dryRun {
		runDryRun(ctx, cfg, *sampleN)
		return
	}

	cp, err := checkpoint.Open(*checkpointDB)
	if err != nil {
		fatal("open checkpoint %s: %v", *checkpointDB, err)
	}
	defer cp.Close()

	// Refuse to resume a checkpoint created with different chunk-shaping config —
	// it would misalign chunk sequence numbers and corrupt the resume. Token and
	// arc-url are deliberately NOT part of the fingerprint (you may legitimately
	// resume against the same Arc with a rotated token).
	fp := configFingerprint(cfg, *precision)
	if err := cp.CheckConfig(fp); err != nil {
		fatal("%v", err)
	}

	snk := sink.New(*arcURL, *token, *precision)
	runLoad(ctx, cfg, snk, cp)
}

// configFingerprint captures the migration-shaping inputs that determine chunk
// boundaries. Resuming with a different fingerprint is rejected (see
// checkpoint.CheckConfig). db-map is included sorted for stable ordering.
func configFingerprint(cfg runConfig, precision string) string {
	maps := make([]string, 0, len(cfg.dbMap))
	for k, v := range cfg.dbMap {
		maps = append(maps, k+"="+v)
	}
	sort.Strings(maps)
	return fmt.Sprintf("chunk=%d;start=%d;end=%d;precision=%s;dbmap=%s",
		cfg.chunkSize, cfg.start, cfg.end, precision, strings.Join(maps, ","))
}

// runConfig is the shared input for both dry-run and load.
type runConfig struct {
	shards    []discover.Shard
	start     int64
	end       int64
	chunkSize int
	dbMap     map[string]string
	verbose   bool
	workers   int
}

func (c runConfig) arcDB(sourceDB string) string {
	if v, ok := c.dbMap[sourceDB]; ok {
		return v
	}
	return sourceDB
}

// shardsByDB groups discovered shards by source database, preserving order.
func shardsByDB(shards []discover.Shard) ([]string, map[string][]discover.Shard) {
	order := []string{}
	m := map[string][]discover.Shard{}
	for _, sh := range shards {
		if _, ok := m[sh.Database]; !ok {
			order = append(order, sh.Database)
		}
		m[sh.Database] = append(m[sh.Database], sh)
	}
	sort.Strings(order)
	return order, m
}

func mustTime(s, flagName string) int64 {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		fatal("bad %s: %v", flagName, err)
	}
	return t.UTC().UnixNano()
}

func fmtNano(ns int64) string {
	return fmt.Sprintf("%s (%d)", time.Unix(0, ns).UTC().Format(time.RFC3339Nano), ns)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "tsm2arc: "+format+"\n", args...)
	os.Exit(1)
}
