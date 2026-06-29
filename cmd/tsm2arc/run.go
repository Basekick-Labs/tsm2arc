package main

import (
	"context"
	"fmt"
	"math"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/basekick-labs/tsm2arc/internal/checkpoint"
	"github.com/basekick-labs/tsm2arc/internal/chunk"
	"github.com/basekick-labs/tsm2arc/internal/discover"
	"github.com/basekick-labs/tsm2arc/internal/extract"
	"github.com/basekick-labs/tsm2arc/internal/sink"
	"github.com/basekick-labs/tsm2arc/internal/tsm"
	"github.com/basekick-labs/tsm2arc/internal/wal"
)

// runDryRun extracts and reports counts + sample LP per database, no network.
func runDryRun(ctx context.Context, cfg runConfig, sampleN int) {
	type dbAgg struct {
		points, fields, keys, skipped int64
		minT, maxT                    int64
		hasT                          bool
		samples                       []string
	}
	aggs := map[string]*dbAgg{}

	order, byDB := shardsByDB(cfg.shards)
	for _, db := range order {
		ag := &dbAgg{minT: math.MaxInt64, maxT: math.MinInt64}
		aggs[db] = ag
		for _, sh := range byDB[db] {
			forEachPoint(cfg, sh, func(p extract.Point) {
				if len(ag.samples) < sampleN {
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

	var totalPoints, totalFields int64
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
		for _, s := range ag.samples {
			fmt.Printf("    %s\n", s)
		}
	}
	fmt.Printf("\nTOTAL: %d points, %d field-values across %d database(s)\n",
		totalPoints, totalFields, len(order))
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
}

// loadResult aggregates the outcome of a load pass (returned for testability).
type loadResult struct {
	Rows          int64
	Chunks        int64
	SkippedChunks int64
	SkippedShards int64
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
		if sr.alreadyDone {
			res.SkippedShards++
		}
	}
	prog.finish()
	return res, nil
}

type shardResult struct {
	rows, sent, skipped int64
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

	var appendErr error
	_ = forEachPoint(cfg, sh, func(p extract.Point) {
		if appendErr != nil {
			return
		}
		appendErr = acc.Append(ctx, []byte(extract.EncodePoint(p)))
	}, nil, prog)
	if appendErr != nil {
		return r, fmt.Errorf("load %s/%s: %w", label, sh.ShardID, appendErr)
	}
	if err := acc.Flush(ctx); err != nil {
		return r, fmt.Errorf("final flush %s/%s: %w", label, sh.ShardID, err)
	}

	// All chunks for this shard acknowledged — mark it done so future resumes
	// skip the whole shard (no re-extraction).
	if err := cp.MarkShardDone(cpKey, sh.ShardID, acc.Seq(), r.rows); err != nil {
		return r, fmt.Errorf("mark done %s/%s: %w", label, sh.ShardID, err)
	}
	prog.shardDone()
	return r, nil
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
