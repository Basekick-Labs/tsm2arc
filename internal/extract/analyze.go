package extract

import (
	"math"
	"sort"

	"github.com/basekick-labs/tsm2arc/internal/series"
	"github.com/basekick-labs/tsm2arc/internal/tsm"
)

// RunProfile describes one merge run of one series: the unit of sequential
// work in shard extraction, and the thing a future intra-shard split would
// divide into time windows. WindowStreams answers the question that decides
// whether such a split can work: how many (file, field) streams would each
// window actually touch? If every window touches nearly all streams (fully
// overlapping generations), a window still pays the whole run's decoded-block
// footprint and parallelism is memory-bound; if windows touch small subsets
// (chained or disjoint generations), a split prunes both I/O and memory.
type RunProfile struct {
	SeriesKey string
	Files     int
	Streams   int   // (file, field-key) streams in the run
	Blocks    int   // index entries across all streams
	Bytes     int64 // compressed bytes (sum of entry sizes)
	MinTime   int64
	MaxTime   int64
	// WindowStreams[i] = streams with at least one block intersecting window i
	// of a byte-balanced K-way split (K = the splitK given to AnalyzeShard).
	WindowStreams []int
}

// ShardAnalysis is the index-only profile of one shard: everything below is
// computed from TSM index regions alone — no block is ever read or decoded.
type ShardAnalysis struct {
	Series     int
	Files      int
	Keys       int
	SkippedKey int
	Runs       []RunProfile // every run, unordered; callers sort/filter
}

// streamMeta is one (file, key) stream's index entries within a run.
type streamMeta struct {
	fi      int
	entries []tsm.IndexEntry
}

// AnalyzeShard profiles a shard from its TSM indexes only. It mirrors Shard's
// phase-1 pass (per-series file ranges, partitionRuns) and then, for each run,
// computes a byte-balanced splitK-way window profile. Fast — proportional to
// index size, not data size — so it is safe to run against a shard of any size.
func AnalyzeShard(tsmFiles []string, openTSM OpenTSM, splitK int) (ShardAnalysis, error) {
	var an ShardAnalysis
	if splitK < 2 {
		splitK = 2
	}

	// Per series: file → time range (for partitionRuns) and file → streams.
	ranges := map[string]map[int]*fileKeys{}
	streams := map[string]map[int][]streamMeta{}
	var order []string

	for fi, tf := range tsmFiles {
		r, err := openTSM(tf)
		if err != nil {
			return an, err
		}
		for _, raw := range r.Keys() {
			k, perr := series.ParseKey(raw)
			if perr != nil {
				an.SkippedKey++
				continue
			}
			an.Keys++
			sk := k.SeriesKey
			if ranges[sk] == nil {
				ranges[sk] = map[int]*fileKeys{}
				streams[sk] = map[int][]streamMeta{}
				order = append(order, sk)
			}
			fk := ranges[sk][fi]
			if fk == nil {
				fk = &fileKeys{min: math.MaxInt64, max: math.MinInt64}
				ranges[sk][fi] = fk
			}
			ents := r.Blocks(raw)
			// Entries alias the reader's parsed index, which stays valid after
			// Close (see tsm.Index) — no copy needed.
			streams[sk][fi] = append(streams[sk][fi], streamMeta{fi: fi, entries: ents})
			for _, e := range ents {
				if e.MinTime < fk.min {
					fk.min = e.MinTime
				}
				if e.MaxTime > fk.max {
					fk.max = e.MaxTime
				}
			}
		}
		r.Close()
	}

	an.Series = len(order)
	an.Files = len(tsmFiles)

	sort.Strings(order)
	for _, sk := range order {
		for _, run := range partitionRuns(ranges[sk], false) {
			rp := RunProfile{SeriesKey: sk, Files: len(run), MinTime: math.MaxInt64, MaxTime: math.MinInt64}
			var all []tsm.IndexEntry
			for _, fi := range run {
				for _, sm := range streams[sk][fi] {
					rp.Streams++
					rp.Blocks += len(sm.entries)
					for _, e := range sm.entries {
						rp.Bytes += int64(e.Size)
						if e.MinTime < rp.MinTime {
							rp.MinTime = e.MinTime
						}
						if e.MaxTime > rp.MaxTime {
							rp.MaxTime = e.MaxTime
						}
					}
					all = append(all, sm.entries...)
				}
			}
			if rp.Blocks == 0 {
				continue
			}
			windows := splitWindows(all, rp.MinTime, rp.MaxTime, splitK)
			rp.WindowStreams = make([]int, len(windows))
			for wi, w := range windows {
				for _, fi := range run {
					for _, sm := range streams[sk][fi] {
						if intersects(sm.entries, w[0], w[1]) {
							rp.WindowStreams[wi]++
						}
					}
				}
			}
			an.Runs = append(an.Runs, rp)
		}
	}
	return an, nil
}

// splitWindows chooses up to k-1 split timestamps so windows carry roughly
// equal compressed bytes, and returns inclusive disjoint [start, end] windows
// covering [min, max]. Split points are block MaxTimes (the same rule a future
// window split would use), deduplicated and strictly increasing.
func splitWindows(entries []tsm.IndexEntry, min, max int64, k int) [][2]int64 {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].MinTime != entries[j].MinTime {
			return entries[i].MinTime < entries[j].MinTime
		}
		return entries[i].MaxTime < entries[j].MaxTime
	})
	var total int64
	for _, e := range entries {
		total += int64(e.Size)
	}
	var splits []int64
	var cum int64
	next := 1
	for _, e := range entries {
		cum += int64(e.Size)
		if next < k && cum >= total*int64(next)/int64(k) {
			s := e.MaxTime
			if s < max && (len(splits) == 0 || s > splits[len(splits)-1]) {
				splits = append(splits, s)
			}
			next++
		}
	}
	var out [][2]int64
	start := min
	for _, s := range splits {
		if s >= start {
			out = append(out, [2]int64{start, s})
			start = s + 1
		}
	}
	out = append(out, [2]int64{start, max})
	return out
}

// intersects reports whether any entry overlaps [w0, w1].
func intersects(entries []tsm.IndexEntry, w0, w1 int64) bool {
	for _, e := range entries {
		if e.MinTime <= w1 && e.MaxTime >= w0 {
			return true
		}
	}
	return false
}
