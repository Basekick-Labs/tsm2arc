package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/basekick-labs/tsm2arc/internal/checkpoint"
	"github.com/basekick-labs/tsm2arc/internal/measure"
	"github.com/basekick-labs/tsm2arc/internal/series"
	"github.com/basekick-labs/tsm2arc/internal/tsm"
	"github.com/basekick-labs/tsm2arc/internal/wal"
)

// censusMeasurements is the pre-flight name check for the fail policy: every
// measurement name the load WOULD emit is collected from the TSM indexes and
// WAL keys — no data decoded beyond the (small) WAL — and resolved against the
// map before the first POST. A single unmapped invalid name used to surface
// only when a worker emitted that measurement's first row, potentially many
// hours and TiBs into a run; here it aborts in the first minute, listing every
// offending name at once so one map fix covers all of them.
//
// Scope rules, matching the load exactly:
//   - names come from series.ParseKey on raw TSM and WAL keys — byte-identical
//     to p.Measurement (same unescaping); keys without a field separator are
//     skipped in both places.
//   - a name counts only if at least one of its blocks/values intersects
//     [--start, --end]: data wholly outside the window is pruned unread by the
//     load and never reaches the resolver, so it must not abort here either.
//   - shards already done in the checkpoint are skipped: they migrated under
//     the same config fingerprint, so they cannot contain a name this policy
//     would reject.
func censusMeasurements(cfg runConfig, cp *checkpoint.Store, jobs []shardJob) error {
	if cfg.resolver.Policy() != measure.PolicyFail {
		return nil // skip/map cannot abort mid-run; no census needed
	}
	if cfg.v3 != nil {
		// v3: every point of a table carries the table name as its
		// measurement, and the names are already resolved — the census is a
		// direct check of each live table's name, no index reads at all.
		var bad []string
		for _, j := range jobs {
			name := cfg.v3.tables[j.shard.SourceID+"/"+j.shard.ShardID].Measurement
			if res := cfg.resolver.Resolve(name); res.Action == measure.ActionInvalid {
				bad = append(bad, name)
			}
		}
		if len(bad) > 0 {
			sort.Strings(bad)
			return censusFailure(bad, nil)
		}
		return nil
	}
	fmt.Printf("pre-flight: checking measurement names across %d shard(s) (index-only)\n", len(jobs))

	var mu sync.Mutex
	names := map[string]int{} // measurement → shards containing it (in window)

	workers := cfg.workers
	if workers < 1 {
		workers = 1
	}
	g, _ := errgroup.WithContext(context.Background())
	g.SetLimit(workers)
	for i := range jobs {
		j := jobs[i]
		g.Go(func() error {
			done, err := cp.IsShardDone(j.shard.SourceID, j.shard.ShardID)
			if err != nil {
				return fmt.Errorf("census checkpoint read (%s/%s): %w", j.shard.Database, j.shard.ShardID, err)
			}
			if done {
				return nil
			}
			local := map[string]bool{}
			for _, tf := range j.shard.TSMFiles {
				r, err := tsm.Open(tf)
				if err != nil {
					return fmt.Errorf("census open %s: %w", tf, err)
				}
				for _, raw := range r.Keys() {
					k, perr := series.ParseKey(raw)
					if perr != nil {
						continue // no field separator — the load skips it too
					}
					if local[k.Measurement] {
						continue
					}
					if entriesIntersect(r.Blocks(raw), cfg.start, cfg.end) {
						local[k.Measurement] = true
					}
				}
				r.Close()
			}
			for _, wf := range j.shard.WALFiles {
				if err := wal.ReadFile(wf, func(key string, vals []tsm.Value) {
					k, perr := series.ParseKey(key)
					if perr != nil {
						return
					}
					if local[k.Measurement] {
						return
					}
					for _, v := range vals {
						if v.UnixNano >= cfg.start && v.UnixNano <= cfg.end {
							local[k.Measurement] = true
							break
						}
					}
				}); err != nil {
					return fmt.Errorf("census WAL %s: %w", wf, err)
				}
			}
			mu.Lock()
			for m := range local {
				names[m]++
			}
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Resolve serially: the resolver caches internally and is not guaranteed
	// safe for concurrent first-touch of a name.
	var bad []string
	for m := range names {
		if cfg.resolver.Resolve(m).Action == measure.ActionInvalid {
			bad = append(bad, m)
		}
	}
	if len(bad) == 0 {
		fmt.Printf("pre-flight: %d measurement name(s), all valid or mapped\n", len(names))
		return nil
	}
	sort.Strings(bad)
	return censusFailure(bad, names)
}

// censusFailure formats the abort for invalid measurement names under the
// fail policy, listing every offender and all three remedies at once.
func censusFailure(bad []string, counts map[string]int) error {
	var list strings.Builder
	for _, m := range bad {
		if counts != nil {
			fmt.Fprintf(&list, "  %q (in %d shard(s))\n", m, counts[m])
		} else {
			fmt.Fprintf(&list, "  %q\n", m)
		}
	}
	return fmt.Errorf("pre-flight census: %d measurement name(s) violate Arc's rule (%s) — NOTHING was sent:\n%s"+
		"Fix them all at once:\n"+
		"  rename:       --measurement-map old=new (repeatable) or --measurement-map-file FILE\n"+
		"  skip them:    --on-invalid-measurement=skip  (drop + report; recorded in the checkpoint)\n"+
		"  auto-rename:  --on-invalid-measurement=map   (deterministic; recorded in the checkpoint)",
		len(bad), measure.ArcNameRule, list.String())
}

// entriesIntersect reports whether any block entry overlaps [start, end].
func entriesIntersect(ents []tsm.IndexEntry, start, end int64) bool {
	for _, e := range ents {
		if e.MinTime <= end && e.MaxTime >= start {
			return true
		}
	}
	return false
}
