package main

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/basekick-labs/tsm2arc/internal/checkpoint"
	"github.com/basekick-labs/tsm2arc/internal/chunk"
	"github.com/basekick-labs/tsm2arc/internal/discover"
	"github.com/basekick-labs/tsm2arc/internal/extract"
	"github.com/basekick-labs/tsm2arc/internal/measure"
	"github.com/basekick-labs/tsm2arc/internal/sink"
	"github.com/basekick-labs/tsm2arc/internal/tsm"
	"github.com/basekick-labs/tsm2arc/internal/wal"
)

// runDryRun extracts and reports counts + sample LP per database, no network.
// It also resolves every measurement name through the rename map + policy and
// reports what a load would do — the pre-flight answer to "which of my
// measurement names would Arc reject, and what happens to them?".
func runDryRun(ctx context.Context, cfg runConfig, sampleN int) {
	type dbAgg struct {
		points, fields, keys, skipped int64
		minT, maxT                    int64
		hasT                          bool
		samples                       []string
		// source measurement → resolution + affected point count, for every
		// measurement that is NOT a plain pass-through.
		actions map[string]measure.Resolution
		actPts  map[string]int64
	}
	aggs := map[string]*dbAgg{}

	order, byDB := shardsByDB(cfg.shards)
	for _, db := range order {
		ag := &dbAgg{minT: math.MaxInt64, maxT: math.MinInt64,
			actions: map[string]measure.Resolution{}, actPts: map[string]int64{}}
		aggs[db] = ag
		for _, sh := range byDB[db] {
			forEachPoint(cfg, sh, func(p extract.Point) {
				res := cfg.resolver.Resolve(p.Measurement)
				if res.Action != measure.ActionPass {
					ag.actions[p.Measurement] = res
					ag.actPts[p.Measurement]++
				}
				// Samples show what a load would SEND: renamed lines under their
				// final name; skipped/invalid measurements never sampled.
				if len(ag.samples) < sampleN && res.Name != "" {
					p.Measurement = res.Name
					ag.samples = append(ag.samples, strings.TrimRight(extract.EncodePoint(p), "\n"))
				}
			}, func(st extract.Stats) {
				ag.points += int64(st.Points)
				ag.fields += int64(st.Fields)
				ag.keys += int64(st.Keys)
				ag.skipped += int64(st.SkippedKey)
				if st.Points > 0 {
					if st.MinTime < ag.minT {
						ag.minT = st.MinTime
					}
					if st.MaxTime > ag.maxT {
						ag.maxT = st.MaxTime
					}
					ag.hasT = true
				}
			}, nil)
		}
	}

	var totalPoints, totalFields, totalInvalid int64
	fmt.Println("\n=== DRY RUN SUMMARY ===")
	for _, db := range order {
		ag := aggs[db]
		totalPoints += ag.points
		totalFields += ag.fields
		fmt.Printf("\ndatabase %q  → Arc database %q\n", db, cfg.arcDB(db))
		fmt.Printf("  points: %d   fields: %d   keys: %d   skipped-keys: %d\n",
			ag.points, ag.fields, ag.keys, ag.skipped)
		if ag.hasT {
			fmt.Printf("  time range: %s … %s\n", fmtNano(ag.minT), fmtNano(ag.maxT))
		}
		for _, m := range sortedKeys(ag.actions) {
			res, n := ag.actions[m], ag.actPts[m]
			switch res.Action {
			case measure.ActionRenamed:
				fmt.Printf("  rename: %q → %q (explicit map, %d points)\n", m, res.Name, n)
			case measure.ActionAutoRenamed:
				fmt.Printf("  rename: %q → %q (auto, %d points)\n", m, res.Name, n)
			case measure.ActionSkipped:
				fmt.Printf("  SKIP:   %q (%d points would NOT be migrated)\n", m, n)
			case measure.ActionInvalid:
				totalInvalid++
				fmt.Printf("  INVALID: %q (%d points) — a load would abort on this name\n", m, n)
			}
		}
		for _, s := range ag.samples {
			fmt.Printf("    %s\n", s)
		}
	}
	fmt.Printf("\nTOTAL: %d points, %d field-values across %d database(s)\n",
		totalPoints, totalFields, len(order))
	if totalInvalid > 0 {
		fmt.Printf("\nWARNING: %d measurement name(s) violate Arc's rule (%s) and would abort a load.\n"+
			"Rename them with --measurement-map old=new (or --measurement-map-file), or set\n"+
			"--on-invalid-measurement=skip (drop + report) or =map (deterministic auto-rename).\n",
			totalInvalid, measure.ArcNameRule)
	}
}

// sortedKeys returns m's keys in ascending order (deterministic reporting).
func sortedKeys(m map[string]measure.Resolution) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// runLoad extracts and pushes chunked LP to Arc with crash-safe resume.
//
// One accumulator PER SHARD (not per database): chunk sequence numbers are
// shard-scoped, and because a shard's extraction order + the byte bound are
// deterministic, the same shard re-extracted produces byte-identical chunks in
// the same order. That lets the checkpoint skip already-acknowledged chunks
// exactly. Each chunk's checkpoint is committed only after Arc returns 2xx.
func runLoad(ctx context.Context, cfg runConfig, snk *sink.Sink, cp *checkpoint.Store) {
	res, err := load(ctx, cfg, snk, cp)
	if err != nil {
		fatal("%v", err)
	}
	fmt.Printf("\nDONE: imported %d rows in %d new chunk(s) across %d database(s)",
		res.Rows, res.Chunks, res.Databases)
	if res.SkippedChunks > 0 || res.SkippedShards > 0 {
		fmt.Printf(" (resume: skipped %d already-done shard(s), %d already-sent chunk(s))",
			res.SkippedShards, res.SkippedChunks)
	}
	fmt.Println()
	printMeasurementReport(cp)
	if res.SkippedPoints > 0 {
		fmt.Printf("\nWARNING: %d point(s) in invalid measurements were skipped and are NOT in Arc\n"+
			"(--on-invalid-measurement=skip). The skipped names are recorded in the checkpoint\n"+
			"and listed above; migrate them later with --measurement-map old=new.\n", res.SkippedPoints)
	}
}

// printMeasurementReport prints the audit trail of measurement renames/skips
// accumulated in the checkpoint (across this run AND prior resumed runs).
func printMeasurementReport(cp *checkpoint.Store) {
	rows, err := cp.MeasurementReport()
	if err != nil {
		fmt.Printf("WARN: could not read measurement action log: %v\n", err)
		return
	}
	if len(rows) == 0 {
		return
	}
	fmt.Println("\nMeasurement renames/skips (recorded in the checkpoint, table measurement_actions):")
	for _, r := range rows {
		switch r.Action {
		case "renamed":
			fmt.Printf("  [%s] %q → %q (%s, %d points, %d shard(s))\n",
				r.SourceDB, r.Measurement, r.RenamedTo, r.Origin, r.Points, r.Shards)
		case "skipped":
			fmt.Printf("  [%s] %q SKIPPED (%d points NOT migrated, %d shard(s))\n",
				r.SourceDB, r.Measurement, r.Points, r.Shards)
		}
	}
}

// loadResult aggregates the outcome of a load pass (returned for testability).
type loadResult struct {
	Rows          int64
	Chunks        int64
	SkippedChunks int64
	SkippedShards int64
	SkippedPoints int64 // points dropped under --on-invalid-measurement=skip
	Databases     int
}

// shardJob pairs a shard with its resolved Arc database name.
type shardJob struct {
	shard discover.Shard
	arcDB string
}

// load runs the per-shard, resumable load across up to cfg.workers shards
// concurrently, and returns aggregate stats or the first error (which the
// caller turns into a fatal). Shards are the unit of parallelism: each has its
// own accumulator, chunk sequence, and checkpoint rows, so they're fully
// independent. The checkpoint store and sink are safe for concurrent use.
//
// Returning an error instead of calling fatal() keeps the real load path
// directly testable, including the crash/resume scenario. On the first shard
// error the context is cancelled and in-flight shards stop; their partial
// progress is already durably checkpointed (commit-after-2xx), so a later resume
// continues correctly.
func load(ctx context.Context, cfg runConfig, snk *sink.Sink, cp *checkpoint.Store) (loadResult, error) {
	order, byDB := shardsByDB(cfg.shards)

	// Flatten to a deterministic job list (DB order, then shard order).
	var jobs []shardJob
	for _, db := range order {
		arcDB := cfg.arcDB(db)
		for _, sh := range byDB[db] {
			jobs = append(jobs, shardJob{shard: sh, arcDB: arcDB})
		}
	}

	res := loadResult{Databases: len(order)}
	results := make([]shardResult, len(jobs))

	workers := cfg.workers
	if workers < 1 {
		workers = 1
	}

	prog := newProgress(int64(len(jobs)), cfg.verbose)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	for i := range jobs {
		i := i
		g.Go(func() error {
			sr, err := loadShard(gctx, cfg, snk, cp, jobs[i].shard, jobs[i].arcDB, prog)
			if err != nil {
				return err
			}
			results[i] = sr // distinct index per goroutine — no shared mutation
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return res, err
	}

	for _, sr := range results {
		res.Rows += sr.rows
		res.Chunks += sr.sent
		res.SkippedChunks += sr.skipped
		res.SkippedPoints += sr.skippedPoints
		if sr.alreadyDone {
			res.SkippedShards++
		}
	}
	prog.finish()
	return res, nil
}

type shardResult struct {
	rows, sent, skipped int64
	skippedPoints       int64
	alreadyDone         bool
}

// loadShard migrates one shard, resuming past any previously committed chunks.
// It is safe to run concurrently with other loadShard calls: each shard has its
// own accumulator/sequence/result, and the checkpoint store and sink are
// concurrency-safe. Progress is reported through prog (thread-safe).
func loadShard(ctx context.Context, cfg runConfig, snk *sink.Sink, cp *checkpoint.Store, sh discover.Shard, arcDB string, prog *progress) (shardResult, error) {
	var r shardResult

	// Checkpoint keys on the STABLE SourceID (1.x db name / 2.x bucket id), NOT
	// the display name: the bucket-id→name mapping (influxd.bolt) may be absent
	// on a later resume, and keying on the name would then re-migrate the whole
	// bucket (and into the wrong Arc db). The label is just for log lines.
	cpKey := sh.SourceID
	label := sh.Database

	done, err := cp.IsShardDone(cpKey, sh.ShardID)
	if err != nil {
		return r, fmt.Errorf("checkpoint read (%s/%s): %w", label, sh.ShardID, err)
	}
	if done {
		prog.logf("[%s/%s] already done — skipping", label, sh.ShardID)
		r.alreadyDone = true
		prog.shardDone()
		return r, nil
	}

	committed, err := cp.CommittedSeq(cpKey, sh.ShardID)
	if err != nil {
		return r, fmt.Errorf("checkpoint read (%s/%s): %w", label, sh.ShardID, err)
	}
	if committed >= 0 {
		prog.logf("[%s/%s] resuming after committed chunk %d", label, sh.ShardID, committed)
	}

	acc := chunk.New(cfg.chunkSize, func(ctx context.Context, seq int, lp []byte) error {
		// Skip chunks already durably in Arc (re-derived but not re-sent).
		if seq <= committed {
			r.skipped++
			prog.addSkipped(1)
			return nil
		}
		sres, err := snk.Send(ctx, arcDB, lp)
		if err != nil {
			return fmt.Errorf("send %s/%s chunk %d to %q: %w", label, sh.ShardID, seq, arcDB, err)
		}
		// Commit AFTER 2xx — this is the durability barrier for resume.
		if err := cp.Commit(cpKey, sh.ShardID, seq, sres.Result.RowsImported); err != nil {
			return fmt.Errorf("checkpoint commit %s/%s chunk %d: %w", label, sh.ShardID, seq, err)
		}
		r.sent++
		r.rows += sres.Result.RowsImported
		prog.addChunk(int64(len(lp)), sres.Result.RowsImported)
		prog.logf("[%s/%s] chunk %d: %d bytes raw → %d rows", label, sh.ShardID, seq, len(lp), sres.Result.RowsImported)
		return nil
	})

	// Measurement resolution is cached per name: shards emit points grouped by
	// series, so the same measurement string recurs in long runs. actPts tallies
	// affected points per source measurement for the checkpoint audit record.
	resCache := map[string]measure.Resolution{}
	actPts := map[string]int64{}

	var appendErr error
	_ = forEachPoint(cfg, sh, func(p extract.Point) {
		if appendErr != nil {
			return
		}
		res, ok := resCache[p.Measurement]
		if !ok {
			res = cfg.resolver.Resolve(p.Measurement)
			resCache[p.Measurement] = res
			switch res.Action {
			case measure.ActionRenamed:
				prog.logf("[%s/%s] renaming measurement %q → %q (explicit map)", label, sh.ShardID, p.Measurement, res.Name)
			case measure.ActionAutoRenamed:
				prog.logf("[%s/%s] renaming measurement %q → %q (auto)", label, sh.ShardID, p.Measurement, res.Name)
			case measure.ActionSkipped:
				prog.logf("[%s/%s] skipping invalid measurement %q", label, sh.ShardID, p.Measurement)
			}
		}
		switch res.Action {
		case measure.ActionInvalid:
			appendErr = errInvalidMeasurement(p.Measurement)
			return
		case measure.ActionSkipped:
			actPts[p.Measurement]++
			r.skippedPoints++
			return
		case measure.ActionRenamed, measure.ActionAutoRenamed:
			actPts[p.Measurement]++
			p.Measurement = res.Name
		}
		appendErr = acc.Append(ctx, []byte(extract.EncodePoint(p)))
	}, nil, prog)
	if appendErr != nil {
		return r, fmt.Errorf("load %s/%s: %w", label, sh.ShardID, appendErr)
	}
	if err := acc.Flush(ctx); err != nil {
		return r, fmt.Errorf("final flush %s/%s: %w", label, sh.ShardID, err)
	}

	// Persist the shard's rename/skip audit records BEFORE marking it done, so
	// a done shard always has its actions on record. Counts are deterministic
	// per shard (extraction order is), so re-recording on a resume overwrites
	// with identical values.
	if len(actPts) > 0 {
		acts := make([]checkpoint.MeasurementAction, 0, len(actPts))
		for _, m := range sortedKeys(resCache) {
			n, affected := actPts[m]
			if !affected {
				continue
			}
			a := checkpoint.MeasurementAction{Measurement: m, Points: n}
			switch resCache[m].Action {
			case measure.ActionSkipped:
				a.Action = "skipped"
			case measure.ActionRenamed:
				a.Action, a.RenamedTo, a.Origin = "renamed", resCache[m].Name, "explicit"
			case measure.ActionAutoRenamed:
				a.Action, a.RenamedTo, a.Origin = "renamed", resCache[m].Name, "auto"
			}
			acts = append(acts, a)
		}
		if err := cp.RecordMeasurementActions(cpKey, sh.ShardID, acts); err != nil {
			return r, fmt.Errorf("record measurement actions %s/%s: %w", label, sh.ShardID, err)
		}
	}

	// All chunks for this shard acknowledged — mark it done so future resumes
	// skip the whole shard (no re-extraction).
	if err := cp.MarkShardDone(cpKey, sh.ShardID, acc.Seq(), r.rows); err != nil {
		return r, fmt.Errorf("mark done %s/%s: %w", label, sh.ShardID, err)
	}
	prog.shardDone()
	return r, nil
}

// errInvalidMeasurement is the client-side replacement for Arc's mid-load 400:
// it fires before anything is sent and tells the operator exactly how to
// proceed, instead of ending a multi-hour load at chunk 73.
func errInvalidMeasurement(name string) error {
	return fmt.Errorf("invalid measurement name %q: Arc requires measurement names to match %s\n"+
		"  rename it:    --measurement-map '%s=<valid-name>'  (or --measurement-map-file FILE)\n"+
		"  skip it:      --on-invalid-measurement=skip  (drop + report; recorded in the checkpoint)\n"+
		"  auto-rename:  --on-invalid-measurement=map   (deterministic; recorded in the checkpoint)\n"+
		"  preview every invalid name first with --dry-run",
		name, measure.ArcNameRule, name)
}

// openTSM adapts tsm.Open to extract.OpenTSM (returns the interface type).
func openTSM(path string) (extract.TSMFile, error) { return tsm.Open(path) }

// forEachPoint iterates every reconstructed point of a shard, field-rejoining
// across the shard's TSM files AND its WAL files (deterministic order). onPoint
// is called per point (may be nil); onShardStats is called once with the shard's
// combined stats (may be nil). prog, if non-nil, receives a thread-safe verbose
// line (used by the concurrent load path; dry-run passes nil and logs itself).
// A hard decode error aborts via the returned error; truncated WAL tails are
// tolerated inside the WAL reader.
func forEachPoint(cfg runConfig, sh discover.Shard, onPoint func(extract.Point), onShardStats func(extract.Stats), prog *progress) error {
	if prog != nil {
		prog.logf("shard %s/%s/%s: %d tsm + %d wal file(s)",
			sh.Database, sh.Retention, sh.ShardID, len(sh.TSMFiles), len(sh.WALFiles))
	} else if cfg.verbose {
		fmt.Printf("  shard %s/%s/%s: %d tsm + %d wal file(s)\n",
			sh.Database, sh.Retention, sh.ShardID, len(sh.TSMFiles), len(sh.WALFiles))
	}
	st, err := extract.Shard(sh.TSMFiles, sh.WALFiles, openTSM, wal.ReadFile,
		cfg.start, cfg.end, func(p extract.Point) {
			if onPoint != nil {
				onPoint(p)
			}
		})
	if err != nil {
		return err
	}
	if onShardStats != nil {
		onShardStats(st)
	}
	return nil
}
