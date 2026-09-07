// Package extract3 turns an InfluxDB 3 table's live parquet file set into
// time-ordered line-protocol points, feeding the same fn(Point) contract the
// TSM extractor feeds — so chunking, sending, checkpointing, census, and
// audit all work unchanged downstream.
//
// # Order and determinism
//
// Emission order is (chunk_time bucket ascending, series ascending, time
// ascending), where series order is the files' own physical sort: the
// series-key columns in tag INSERTION order (the catalog's order, not
// lexicographic), NULLS FIRST. The order is a pure function of the live file
// set, so two runs — or a run and its resume — emit byte-identical output.
//
// # Buckets
//
// chunk_time is a pure function of the row timestamp (late data lands in old
// buckets via new files), so buckets partition rows and can be processed
// independently — GUARDED: bucket spans are verified disjoint against the
// next bucket's start, because gen1-duration is a restart flag operators are
// only warned not to change. On violation the table falls back to one global
// merge, loudly, rather than emitting duplicates.
//
// # Duplicates (LWW)
//
// Rows are unique within one file; across files the same (series, time) can
// recur. The winner is the row from the newest file: WAL sequence from the
// object name, then parquet file id, then path — deterministic, and stricter
// than the server's own unguaranteed same-window behavior.
package extract3

import (
	"fmt"
	"math"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/basekick-labs/tsm2arc/internal/extract"
	"github.com/basekick-labs/tsm2arc/internal/v3meta"
	"github.com/basekick-labs/tsm2arc/internal/vfs"
)

// Table is one table's extraction input.
type Table struct {
	Key v3meta.TableKey
	// Measurement is the emitted measurement name (the table name, possibly
	// remapped by the caller's measure resolver).
	Measurement string
	// SeriesKey is the table's tag columns in catalog INSERTION order — the
	// merge comparator. It must be complete: a live file containing a tag
	// column absent from this list aborts extraction (an incomplete key
	// would make distinct series compare equal and LWW would drop real data).
	SeriesKey []string
	// Files is the table's live set (from v3meta.BuildLiveSet).
	Files []v3meta.ParquetFile
}

// Options tunes extraction.
type Options struct {
	// Warn receives non-fatal diagnostics (bucket-overlap fallback). Nil
	// discards them.
	Warn func(format string, args ...any)
}

func (o Options) warnf(format string, args ...any) {
	if o.Warn != nil {
		o.Warn(format, args...)
	}
}

// Extract emits the table's points in deterministic order. start/end bound
// timestamps inclusively (math.MinInt64/MaxInt64 for unbounded). A nil cursor
// extracts everything; with a cursor, output resumes immediately after it,
// and a cursor that no longer matches the source fails loudly.
func Extract(f vfs.FS, tbl Table, start, end int64, cur *extract.Cursor, opt Options, fn func(extract.Point)) (extract.Stats, error) {
	var st extract.Stats
	if len(tbl.Files) == 0 {
		return st, nil
	}

	buckets := bucketize(tbl.Files)
	if !bucketsDisjoint(buckets) {
		opt.warnf("table %s: live files span bucket boundaries (gen1-duration changed mid-life?); falling back to one global merge", tbl.Key)
		all := make([]v3meta.ParquetFile, len(tbl.Files))
		copy(all, tbl.Files)
		sortFilesLWW(all) // rank order must not depend on caller's file order
		buckets = []bucket{{chunkTime: buckets[0].chunkTime, files: all}}
	}

	// Resume: the cursor's bucket must still exist, and the exact cursor
	// position must be seen while skipping — anything else means the live
	// set changed since the checkpoint was written.
	var curBucketPrefix string
	cursorSeen := cur == nil
	if cur != nil {
		i := strings.IndexByte(cur.SeriesKey, '|')
		if i != 20 {
			return st, fmt.Errorf("resume cursor %q is not a v3 cursor", cur.SeriesKey)
		}
		curBucketPrefix = cur.SeriesKey[:i]
		found := false
		for _, b := range buckets {
			if bucketPrefix(b.chunkTime) == curBucketPrefix {
				found = true
				break
			}
		}
		if !found {
			return st, fmt.Errorf("resume cursor points at bucket %s which no longer exists in table %s: source changed since checkpoint", curBucketPrefix, tbl.Key)
		}
	}

	var prevSeries string
	for _, b := range buckets {
		prefix := bucketPrefix(b.chunkTime)
		if cur != nil && prefix < curBucketPrefix {
			continue // entire bucket already emitted
		}
		// Skip buckets fully outside [start, end]. MinTime/MaxTime come from
		// the manifests; rows are filtered exactly below either way.
		inRange := false
		for _, pf := range b.files {
			if pf.MinTime <= end && pf.MaxTime >= start {
				inRange = true
				break
			}
		}
		if !inRange {
			continue
		}

		rows, err := readBucket(f, tbl, b, start, end)
		if err != nil {
			return st, err
		}
		for i := range rows {
			r := &rows[i]
			// Rows sort by (series, time, file rank ascending), so within a
			// duplicate group the LAST row is the LWW winner: skip the rest.
			if i+1 < len(rows) && rows[i+1].orderKey == r.orderKey && rows[i+1].ts == r.ts {
				continue
			}
			seriesKey := prefix + "|" + r.orderKey
			if cur != nil {
				if seriesKey < cur.SeriesKey || (seriesKey == cur.SeriesKey && r.ts <= cur.UnixNano) {
					if seriesKey == cur.SeriesKey && r.ts == cur.UnixNano {
						cursorSeen = true
					}
					continue
				}
				if !cursorSeen {
					return st, fmt.Errorf("resume cursor position not found in table %s: source changed since checkpoint", tbl.Key)
				}
			}
			if seriesKey != prevSeries {
				st.Keys++
				prevSeries = seriesKey
			}
			st.Points++
			st.Fields += len(r.fields)
			observe(&st, r.ts)
			fn(extract.Point{
				SeriesKey:   seriesKey,
				Measurement: tbl.Measurement,
				Tags:        r.tags,
				UnixNano:    r.ts,
				Fields:      r.fields,
			})
		}
	}
	if !cursorSeen {
		return st, fmt.Errorf("resume cursor position not found in table %s: source changed since checkpoint", tbl.Key)
	}
	return st, nil
}

func observe(st *extract.Stats, ts int64) {
	if st.Points == 1 {
		st.MinTime, st.MaxTime = ts, ts
		return
	}
	if ts < st.MinTime {
		st.MinTime = ts
	}
	if ts > st.MaxTime {
		st.MaxTime = ts
	}
}

// bucket is the set of live files sharing one chunk_time.
type bucket struct {
	chunkTime int64
	files     []v3meta.ParquetFile
}

func bucketize(files []v3meta.ParquetFile) []bucket {
	byCT := map[int64][]v3meta.ParquetFile{}
	for _, pf := range files {
		byCT[pf.ChunkTime] = append(byCT[pf.ChunkTime], pf)
	}
	out := make([]bucket, 0, len(byCT))
	for ct, fs := range byCT {
		sortFilesLWW(fs)
		out = append(out, bucket{chunkTime: ct, files: fs})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].chunkTime < out[j].chunkTime })
	return out
}

// sortFilesLWW orders files oldest-write-first, so that within a duplicate
// group the later file overwrites: WAL sequence from the object name, then
// parquet file id, then path. Emission determinism requires this order to be
// a pure function of the file set, never of the caller's slice order.
func sortFilesLWW(fs []v3meta.ParquetFile) {
	sort.Slice(fs, func(i, j int) bool {
		wi, wj := walSeqOf(fs[i].Path), walSeqOf(fs[j].Path)
		if wi != wj {
			return wi < wj
		}
		if fs[i].ID != fs[j].ID {
			return fs[i].ID < fs[j].ID
		}
		return fs[i].Path < fs[j].Path
	})
}

// bucketsDisjoint verifies no bucket's data reaches into the next bucket.
func bucketsDisjoint(buckets []bucket) bool {
	for i := 0; i < len(buckets)-1; i++ {
		maxT := int64(math.MinInt64)
		for _, pf := range buckets[i].files {
			if pf.MaxTime > maxT {
				maxT = pf.MaxTime
			}
		}
		if maxT >= buckets[i+1].chunkTime {
			return false
		}
	}
	return true
}

// walSeqOf parses the WAL sequence from a parquet object name
// ({walseq:010}[-ordinal].parquet). Unparseable names rank 0 (oldest).
func walSeqOf(p string) uint64 {
	stem, ok := strings.CutSuffix(path.Base(p), ".parquet")
	if !ok {
		return 0
	}
	if i := strings.IndexByte(stem, '-'); i >= 0 {
		stem = stem[:i]
	}
	n, err := strconv.ParseUint(stem, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// bucketPrefix encodes a chunk_time so lexicographic order equals numeric
// order for all int64 values (bias by 2^63, 20 digits zero-padded).
func bucketPrefix(chunkTime int64) string {
	return fmt.Sprintf("%020d", uint64(chunkTime)^(1<<63))
}
