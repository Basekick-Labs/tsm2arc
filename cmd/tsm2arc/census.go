package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/basekick-labs/tsm2arc/internal/checkpoint"
	"github.com/basekick-labs/tsm2arc/internal/lp"
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
	failPolicy := cfg.resolver.Policy() == measure.PolicyFail
	if !failPolicy && !cfg.narrow.enabled() {
		return nil // skip/map cannot abort mid-run, and no --narrow to check
	}
	if cfg.v3 != nil {
		// v3: every point of a table carries the table name as its
		// measurement, and the names are already resolved — the census is a
		// direct check of each live table's name, no index reads at all.
		var bad []string
		names := map[string]int{}
		tags := map[string]map[string]bool{}
		byDB := map[string]map[string]bool{}
		for _, j := range jobs {
			tbl := cfg.v3.tables[j.shard.SourceID+"/"+j.shard.ShardID]
			names[tbl.Measurement]++
			if byDB[j.arcDB] == nil {
				byDB[j.arcDB] = map[string]bool{}
			}
			byDB[j.arcDB][tbl.Measurement] = true
			if cfg.narrow.has(tbl.Measurement) {
				set := tags[tbl.Measurement]
				if set == nil {
					set = map[string]bool{}
					tags[tbl.Measurement] = set
				}
				for _, t := range tbl.SeriesKey {
					set[t] = true
				}
			}
			if failPolicy {
				if res := cfg.resolver.Resolve(tbl.Measurement); res.Action == measure.ActionInvalid {
					bad = append(bad, tbl.Measurement)
				}
			}
		}
		if len(bad) > 0 {
			sort.Strings(bad)
			return censusFailure(bad, nil)
		}
		return checkNarrow(cfg, names, tags, byDB, false)
	}
	fmt.Printf("pre-flight: checking measurement names across %d shard(s) (index-only)\n", len(jobs))

	var mu sync.Mutex
	names := map[string]int{}            // measurement → shards containing it (in window)
	tags := map[string]map[string]bool{} // --narrow measurement → its tag keys
	byDB := map[string]map[string]bool{} // Arc database → source measurements it gets
	resumed := false                     // a shard was skipped as done: its names are not counted

	workers := cfg.workers
	if workers < 1 {
		workers = 1
	}
	g, gctx := errgroup.WithContext(context.Background())
	g.SetLimit(workers)
	for i := range jobs {
		j := jobs[i]
		g.Go(func() error {
			// After a failure, queued shards are not opened at all.
			if err := gctx.Err(); err != nil {
				return err
			}
			done := false
			if cp != nil { // nil: a dry run, which has no checkpoint
				var err error
				done, err = cp.IsShardDone(j.shard.SourceID, j.shard.ShardID)
				if err != nil {
					return fmt.Errorf("census checkpoint read (%s/%s): %w", j.shard.Database, j.shard.ShardID, err)
				}
			}
			if done {
				mu.Lock()
				resumed = true
				mu.Unlock()
				return nil
			}
			local := map[string]bool{}
			localTags := map[string]map[string]bool{}
			note := func(k series.Key) {
				local[k.Measurement] = true
				if !cfg.narrow.has(k.Measurement) {
					return
				}
				set := localTags[k.Measurement]
				if set == nil {
					set = map[string]bool{}
					localTags[k.Measurement] = set
				}
				for _, t := range k.Tags {
					set[t[0]] = true
				}
			}
			for _, tf := range j.shard.TSMFiles {
				if err := gctx.Err(); err != nil {
					return err
				}
				r, err := tsm.Open(tf)
				if err != nil {
					return fmt.Errorf("census open %s: %w", tf, err)
				}
				for _, raw := range r.Keys() {
					k, perr := series.ParseKey(raw)
					if perr != nil {
						continue // no field separator — the load skips it too
					}
					// A --narrow measurement's tags are read for every series;
					// any other's name is settled by its first series in window.
					if local[k.Measurement] && !cfg.narrow.has(k.Measurement) {
						continue
					}
					if entriesIntersect(r.Blocks(raw), cfg.start, cfg.end) {
						note(k)
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
					if local[k.Measurement] && !cfg.narrow.has(k.Measurement) {
						return
					}
					for _, v := range vals {
						if v.UnixNano >= cfg.start && v.UnixNano <= cfg.end {
							note(k)
							break
						}
					}
				}); err != nil {
					return fmt.Errorf("census WAL %s: %w", wf, err)
				}
			}
			mu.Lock()
			if byDB[j.arcDB] == nil {
				byDB[j.arcDB] = map[string]bool{}
			}
			for m := range local {
				names[m]++
				byDB[j.arcDB][m] = true
			}
			for m, set := range localTags {
				all := tags[m]
				if all == nil {
					all = map[string]bool{}
					tags[m] = all
				}
				for t := range set {
					all[t] = true
				}
			}
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	if failPolicy {
		// Resolve serially: the resolver caches internally and is not
		// guaranteed safe for concurrent first-touch of a name.
		var bad []string
		for m := range names {
			if cfg.resolver.Resolve(m).Action == measure.ActionInvalid {
				bad = append(bad, m)
			}
		}
		if len(bad) > 0 {
			sort.Strings(bad)
			return censusFailure(bad, names)
		}
		fmt.Printf("pre-flight: %d measurement name(s), all valid or mapped\n", len(names))
	}
	return checkNarrow(cfg, names, tags, byDB, resumed)
}

// checkNarrow refuses, before the first POST, a --narrow that would fail or
// mislead hours into a load:
//   - (a warning) a name that matches no source measurement: a typo, or the
//     renamed Arc name instead of the source one;
//   - a narrow measurement with a tag the narrow layout cannot write next to
//     (the narrow tag, or a value column's name in any letter case);
//   - an Arc measurement (in one Arc database) fed by both a narrow and a
//     wide source measurement (--measurement-map): one table cannot hold
//     both layouts.
func checkNarrow(cfg runConfig, names map[string]int, tags map[string]map[string]bool, byDB map[string]map[string]bool, resumed bool) error {
	if !cfg.narrow.enabled() {
		return nil
	}
	var unknown []string
	for _, m := range cfg.narrow.sorted() {
		if names[m] == 0 {
			unknown = append(unknown, m)
		}
	}
	// A warning, not a refusal: runs batched by --database-filter or a time
	// window commonly share one --narrow list. A misspelt name leaves its
	// measurement wide, and a wide load of a sparse measurement fails fast
	// with Arc's 413, so the mistake surfaces at once either way.
	if len(unknown) > 0 {
		where := "in the time window"
		if resumed {
			where = "in the shards still to load (shards already done were not read)"
		}
		fmt.Printf("pre-flight: WARNING: --narrow names %q, which no source measurement %s has; check the spelling (source names, as --analyze shows them, not renamed ones)\n", unknown, where)
	}
	var clashes []string
	for _, m := range cfg.narrow.sorted() {
		keys := make([]string, 0, len(tags[m]))
		for t := range tags[m] {
			keys = append(keys, t)
		}
		sort.Strings(keys)
		for _, t := range keys {
			if col, bad := cfg.narrow.narrowClash(t); bad {
				clashes = append(clashes, fmt.Sprintf("  %q has a tag %q, which clashes with the narrow layout's %q", m, t, col))
			}
		}
	}
	if len(clashes) > 0 {
		return fmt.Errorf("pre-flight: --narrow cannot write these measurements; NOTHING was sent:\n%s\nchoose another --narrow-tag (the value columns are %s)",
			strings.Join(clashes, "\n"), strings.Join(lp.NarrowValueColumns, ", "))
	}
	// One Arc table, two layouts: a table is a measurement in one Arc
	// database. Resolve every source name once (serially).
	dbs := make([]string, 0, len(byDB))
	for db := range byDB {
		dbs = append(dbs, db)
	}
	sort.Strings(dbs)
	for _, db := range dbs {
		layout := map[string]bool{} // Arc name → narrow
		from := map[string]string{} // Arc name → the source name that set its layout
		srcs := make([]string, 0, len(byDB[db]))
		for m := range byDB[db] {
			srcs = append(srcs, m)
		}
		sort.Strings(srcs)
		for _, m := range srcs {
			res := cfg.resolver.Resolve(m)
			if res.Name == "" {
				continue // skipped or invalid: never sent
			}
			n := cfg.narrow.has(m)
			if prev, seen := layout[res.Name]; seen && prev != n {
				return fmt.Errorf("pre-flight: Arc measurement %s.%s would get source measurements %q and %q, only one of them in --narrow; one table cannot hold both layouts. NOTHING was sent", db, res.Name, from[res.Name], m)
			}
			layout[res.Name] = n
			from[res.Name] = m
		}
	}
	if len(cfg.narrow.measurements) > 0 {
		fmt.Printf("pre-flight: --narrow: %d measurement(s) in the narrow layout, tag %q\n", len(cfg.narrow.measurements), cfg.narrow.tag)
	}
	return nil
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
