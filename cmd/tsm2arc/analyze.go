package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/basekick-labs/tsm2arc/internal/extract"
)

// analyzeSplitK is the hypothetical window count profiled per run. 8 is enough
// to show whether windows prune streams; the conclusion barely moves with K.
const analyzeSplitK = 8

// analyzeSplitThreshold mirrors the extractor's minimum run size for
// windowing; runs below it are never split, so their verdict says so.
const analyzeSplitThreshold = 256 << 20

// Report types — the single source for both text and JSON rendering, so the
// two can never drift. JSON always carries EVERY run of every shard
// (--analyze-runs applies to text only): machine output that silently
// truncates is how a downstream sizing model ends up confidently wrong.
type analyzeReport struct {
	Version int `json:"version"`
	// SplitThresholdBytes lets consumers reproduce the below-threshold verdict.
	SplitThresholdBytes int64          `json:"split_threshold_bytes"`
	Shards              []analyzeShard `json:"shards"`
}

type analyzeShard struct {
	Database  string `json:"database"`
	Retention string `json:"retention"`
	ShardID   string `json:"shard_id"`
	// SeriesGroups counts distinct series keys (measurement + tag set). For
	// tagless data this equals the measurement count — it is NOT the per-field
	// stream count; see KeyEntries.
	SeriesGroups int `json:"series_groups"`
	RunsTotal    int `json:"runs_total"`
	TSMFiles     int `json:"tsm_files"`
	// KeyEntries counts (file, key) index entries: the same series+field
	// appearing in three TSM generations counts three times.
	KeyEntries  int          `json:"key_entries"`
	SkippedKeys int          `json:"skipped_keys,omitempty"`
	Runs        []analyzeRun `json:"runs"` // sorted by bytes descending
}

type analyzeRun struct {
	Series        string `json:"series"`
	Files         int    `json:"files"`
	Streams       int    `json:"streams"`
	Blocks        int    `json:"blocks"`
	Bytes         int64  `json:"bytes"`
	MinTimeNs     int64  `json:"min_time_ns"`
	MaxTimeNs     int64  `json:"max_time_ns"`
	Windows       int    `json:"windows"`
	WindowStreams []int  `json:"window_streams"`
	// Verdict: "split-friendly", "overlapping", or "below-threshold".
	Verdict string `json:"verdict"`
	// EstResidentBytes = busiest window's streams × one decoded block (64 KiB):
	// the rough resident footprint of merging that window.
	EstResidentBytes int64 `json:"est_resident_bytes"`
}

// runAnalyze profiles every shard from its TSM indexes only (nothing decoded,
// nothing sent), then renders text or JSON. The report is built COMPLETELY
// before anything is written in JSON mode, so an error can never leave
// truncated JSON on stdout.
func runAnalyze(cfg runConfig) {
	rep := analyzeReport{Version: 1, SplitThresholdBytes: analyzeSplitThreshold}
	order, byDB := shardsByDB(cfg.shards)
	for _, db := range order {
		for _, sh := range byDB[db] {
			an, err := extract.AnalyzeShard(sh.TSMFiles, openTSM, analyzeSplitK)
			if err != nil {
				fatal("analyze %s/%s: %v", sh.Database, sh.ShardID, err)
			}
			s := analyzeShard{
				Database:     sh.Database,
				Retention:    sh.Retention,
				ShardID:      sh.ShardID,
				SeriesGroups: an.Series,
				RunsTotal:    len(an.Runs),
				TSMFiles:     an.Files,
				KeyEntries:   an.Keys,
				SkippedKeys:  an.SkippedKey,
			}
			if cfg.redact {
				s.Database = redactName("db", s.Database)
				s.Retention = redactName("rp", s.Retention)
			}
			runs := an.Runs
			sort.Slice(runs, func(i, j int) bool { return runs[i].Bytes > runs[j].Bytes })
			for _, r := range runs {
				name := r.SeriesKey
				if cfg.redact {
					name = redactName("series", r.SeriesKey)
				}
				_, _, max := windowStats(r.WindowStreams)
				verdict := "split-friendly"
				switch {
				case r.Bytes < analyzeSplitThreshold:
					verdict = "below-threshold"
				case r.Streams > 0 && max*10 >= r.Streams*8:
					verdict = "overlapping"
				}
				s.Runs = append(s.Runs, analyzeRun{
					Series: name, Files: r.Files, Streams: r.Streams, Blocks: r.Blocks,
					Bytes: r.Bytes, MinTimeNs: r.MinTime, MaxTimeNs: r.MaxTime,
					Windows: len(r.WindowStreams), WindowStreams: r.WindowStreams,
					Verdict: verdict, EstResidentBytes: int64(max) * 64 * 1024,
				})
			}
			rep.Shards = append(rep.Shards, s)
		}
	}

	if cfg.jsonOut {
		enc := json.NewEncoder(os.Stdout)
		if err := enc.Encode(rep); err != nil {
			fatal("encode analyze report: %v", err)
		}
		return
	}
	renderAnalyzeText(cfg, rep)
}

func renderAnalyzeText(cfg runConfig, rep analyzeReport) {
	fmt.Println("\n=== SHARD ANALYSIS (index-only; no data decoded, nothing sent) ===")
	for _, s := range rep.Shards {
		fmt.Printf("\nshard %s/%s/%s: %d series groups, %d runs, %d tsm files, %d key entries",
			s.Database, s.Retention, s.ShardID, s.SeriesGroups, s.RunsTotal, s.TSMFiles, s.KeyEntries)
		if s.SkippedKeys > 0 {
			fmt.Printf(", %d skipped keys", s.SkippedKeys)
		}
		fmt.Println()

		runs := s.Runs
		if cfg.analyzeRuns > 0 && len(runs) > cfg.analyzeRuns {
			fmt.Printf("  showing %d of %d runs (largest by compressed bytes) — NOT a complete list; --analyze-runs=0 for all, --format=json for machine-complete output\n",
				cfg.analyzeRuns, len(runs))
			runs = runs[:cfg.analyzeRuns]
		}
		for _, r := range runs {
			fmt.Printf("  run: series=%s\n", truncate(r.Series, 80))
			fmt.Printf("       %d files, %d streams, %d blocks, %s, %s .. %s\n",
				r.Files, r.Streams, r.Blocks, fmtBytes(r.Bytes),
				fmtDay(r.MinTimeNs), fmtDay(r.MaxTimeNs))
			min, med, max := windowStats(r.WindowStreams)
			fmt.Printf("       %d-way window profile: streams/window min=%d median=%d max=%d (of %d total)\n",
				r.Windows, min, med, max, r.Streams)
			switch r.Verdict {
			case "below-threshold":
				fmt.Printf("       verdict: below the split threshold — this run would never be windowed\n")
			case "overlapping":
				fmt.Printf("       verdict: OVERLAPPING — windows touch >=80%% of streams; a window split stays memory-bound (~%s decoded blocks resident per window)\n",
					fmtBytes(r.EstResidentBytes))
			default:
				fmt.Printf("       verdict: SPLIT-FRIENDLY — windows prune streams well (busiest window ~%s decoded blocks resident)\n",
					fmtBytes(r.EstResidentBytes))
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
