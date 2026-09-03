package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"github.com/basekick-labs/tsm2arc/internal/extract"
)

// analyzeSplitK is the hypothetical window count profiled per run. 8 is enough
// to show whether windows prune streams; the conclusion barely moves with K.
const analyzeSplitK = 8

// runAnalyze prints an index-only profile of every shard: series/file/key
// counts and, for the largest merge runs, whether a K-way window split would
// touch small stream subsets (split-friendly) or nearly all streams per window
// (fully overlapping generations — a split would stay memory-bound). Nothing
// is decoded and nothing is sent; runtime is proportional to index size.
func runAnalyze(cfg runConfig) {
	fmt.Println("\n=== SHARD ANALYSIS (index-only; no data decoded, nothing sent) ===")
	order, byDB := shardsByDB(cfg.shards)
	for _, db := range order {
		for _, sh := range byDB[db] {
			an, err := extract.AnalyzeShard(sh.TSMFiles, openTSM, analyzeSplitK)
			if err != nil {
				fatal("analyze %s/%s: %v", sh.Database, sh.ShardID, err)
			}
			db, rp := sh.Database, sh.Retention
			if cfg.redact {
				db, rp = redactName("db", db), redactName("rp", rp)
			}
			fmt.Printf("\nshard %s/%s/%s: %d series, %d tsm files, %d keys",
				db, rp, sh.ShardID, an.Series, an.Files, an.Keys)
			if an.SkippedKey > 0 {
				fmt.Printf(", %d skipped keys", an.SkippedKey)
			}
			fmt.Println()

			// Largest runs dominate wall-clock; show the top 5 by bytes.
			runs := an.Runs
			sort.Slice(runs, func(i, j int) bool { return runs[i].Bytes > runs[j].Bytes })
			if len(runs) > 5 {
				runs = runs[:5]
			}
			for _, r := range runs {
				name := truncate(r.SeriesKey, 80)
				if cfg.redact {
					name = redactName("series", r.SeriesKey)
				}
				fmt.Printf("  run: series=%s\n", name)
				fmt.Printf("       %d files, %d streams, %d blocks, %s, %s .. %s\n",
					r.Files, r.Streams, r.Blocks, fmtBytes(r.Bytes),
					fmtDay(r.MinTime), fmtDay(r.MaxTime))
				min, med, max := windowStats(r.WindowStreams)
				fmt.Printf("       %d-way window profile: streams/window min=%d median=%d max=%d (of %d total)\n",
					len(r.WindowStreams), min, med, max, r.Streams)
				if r.Bytes < 256<<20 {
					fmt.Printf("       verdict: below the split threshold — this run would never be windowed\n")
					continue
				}
				// Verdict: if the busiest window still touches most streams, a
				// window split cannot shrink the resident decoded-block set.
				if r.Streams > 0 && max*10 >= r.Streams*8 {
					fmt.Printf("       verdict: OVERLAPPING — windows touch >=80%% of streams; a window split stays memory-bound (~%s decoded blocks resident per window)\n",
						fmtBytes(int64(max)*64*1024))
				} else {
					fmt.Printf("       verdict: SPLIT-FRIENDLY — windows prune streams well (busiest window ~%s decoded blocks resident)\n",
						fmtBytes(int64(max)*64*1024))
				}
			}
		}
	}
	fmt.Println("\nShare this output when discussing throughput: it determines whether intra-shard splitting can help your data shape.")
	if !cfg.redact {
		fmt.Println("If the report must leave your organization, re-run with --redact: it replaces database, retention policy, and series names with stable hashed identifiers while keeping every number.")
	}
}

// redactName replaces an internal identifier with a stable pseudonym: a fixed
// prefix plus the first 12 hex chars of the name's SHA-256. Stability is the
// point — the same source produces the same pseudonyms on every run and every
// machine, so a support conversation can keep referring to series_3f9a2c1b04d7
// across re-runs. The hash is deliberately unsalted for that stability; note
// that a short, guessable name could in principle be confirmed by hashing
// guesses, so --redact protects identifiers, not secrets embedded in them.
func redactName(prefix, name string) string {
	sum := sha256.Sum256([]byte(name))
	return prefix + "_" + hex.EncodeToString(sum[:])[:12]
}

func windowStats(ws []int) (min, med, max int) {
	if len(ws) == 0 {
		return 0, 0, 0
	}
	s := append([]int(nil), ws...)
	sort.Ints(s)
	return s[0], s[len(s)/2], s[len(s)-1]
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func fmtBytes(b int64) string {
	switch {
	case b >= 1<<40:
		return fmt.Sprintf("%.1f TiB", float64(b)/(1<<40))
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func fmtDay(ns int64) string {
	return time.Unix(0, ns).UTC().Format("2006-01-02")
}
