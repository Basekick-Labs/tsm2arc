// Package extract turns a TSM file's per-field value streams into time-ordered
// multi-field line-protocol points.
//
// The central problem: InfluxDB stores each field of a point as a SEPARATE TSM
// key (e.g. cpu...#!~#usage_idle and cpu...#!~#cores), each with its own
// timestamp+value stream. To reconstruct the original points we must group keys
// by series, then merge their streams on the timestamp axis so that all fields
// sharing a (series, timestamp) collapse back into one LP line.
package extract

import (
	"sort"
	"strings"

	"github.com/basekick-labs/tsm2arc/internal/lp"
	"github.com/basekick-labs/tsm2arc/internal/series"
	"github.com/basekick-labs/tsm2arc/internal/tsm"
)

// Point is a fully reconstructed multi-field point.
type Point struct {
	Measurement string
	Tags        [][2]string
	UnixNano    int64
	Fields      []lp.Field
}

// Stats accumulates counts for --dry-run reporting and verification.
type Stats struct {
	Keys       int
	Points     int
	Fields     int
	SkippedKey int
	MinTime    int64
	MaxTime    int64
	hasTime    bool
}

func (s *Stats) observe(ts int64) {
	if !s.hasTime {
		s.MinTime, s.MaxTime, s.hasTime = ts, ts, true
		return
	}
	if ts < s.MinTime {
		s.MinTime = ts
	}
	if ts > s.MaxTime {
		s.MaxTime = ts
	}
}

// fieldStream is one field's decoded values plus its parsed field name.
type fieldStream struct {
	field string
	vals  []tsm.Value
}

// group accumulates all field streams for one series (across TSM + WAL).
type group struct {
	seriesKey   string
	measurement string
	tags        [][2]string
	streams     []fieldStream
}

// collector groups (key, values) streams by series for later field-rejoin.
type collector struct {
	groups   []*group
	bySeries map[string]*group
	stats    Stats
}

func newCollector() *collector {
	return &collector{bySeries: map[string]*group{}}
}

// add folds one raw TSM/WAL key and its decoded values into the right series
// group. A key that can't be parsed (no field separator) is counted as skipped.
func (c *collector) add(rawKey string, vals []tsm.Value) {
	k, err := series.ParseKey(rawKey)
	if err != nil {
		c.stats.SkippedKey++
		return
	}
	c.stats.Keys++
	g := c.bySeries[k.SeriesKey]
	if g == nil {
		g = &group{seriesKey: k.SeriesKey, measurement: k.Measurement, tags: k.Tags}
		c.bySeries[k.SeriesKey] = g
		c.groups = append(c.groups, g)
	}
	g.streams = append(g.streams, fieldStream{field: k.Field, vals: vals})
}

// emit field-rejoins every series in deterministic order and yields points.
func (c *collector) emit(start, end int64, fn func(Point)) Stats {
	sort.Slice(c.groups, func(i, j int) bool { return c.groups[i].seriesKey < c.groups[j].seriesKey })
	for _, g := range c.groups {
		mergeSeries(g.measurement, g.tags, g.streams, start, end, &c.stats, fn)
	}
	return c.stats
}

// File reads a TSM file and yields reconstructed points to fn, in deterministic
// order (series key ascending, then timestamp ascending). Optional time bounds
// [start,end] (ns, inclusive) filter points; pass minInt64/maxInt64 for none.
// Returns stats. fn may be nil for pure counting (dry-run).
func File(r TSMFile, start, end int64, fn func(Point)) (Stats, error) {
	c := newCollector()
	if err := addTSM(c, r); err != nil {
		return c.stats, err
	}
	return c.emit(start, end, fn), nil
}

// addTSM folds every key of a TSM reader into the collector.
func addTSM(c *collector, r TSMFile) error {
	for _, raw := range r.Keys() {
		vals, err := r.ReadKeyByName(raw)
		if err != nil {
			return err
		}
		c.add(raw, vals)
	}
	return nil
}

// OpenTSM opens a TSM file as a TSMFile. Provided by the caller (cmd) so the
// extract package stays decoupled from the concrete reader type; in practice
// this is tsm.Open.
type OpenTSM func(path string) (TSMFile, error)

// WALReader streams (key, values) pairs from one .wal file to fn. In practice
// this is a thin wrapper over wal.ReadFile.
type WALReader func(path string, fn func(key string, vals []tsm.Value)) error

// Shard reconstructs all points of one shard from its TSM files AND its WAL
// files, field-rejoining across both sources, and yields points to fn in
// deterministic order (series key ascending, then timestamp ascending).
//
// MEMORY: this streams ONE SERIES AT A TIME. It indexes only the key strings of
// each TSM file up front (cheap — strings + block offsets, not values), then for
// each series reads just that series' field values, merges, emits, and lets them
// be collected before the next series. Peak memory is therefore bounded by the
// largest single SERIES, not the whole shard — critical on multi-GB shards where
// loading every decoded value at once cost tens of GB of RAM.
//
// The WAL (un-flushed, typically small) is loaded once into a per-series map and
// merged in alongside the streamed TSM values. TSM values are appended before
// WAL values for each series so the WAL (newer) wins on a same-(ts,field) tie
// (last-write-wins), matching InfluxDB and Arc compaction.
//
// Determinism — required for crash-safe resume — comes from emitting series in
// sorted key order and timestamps ascending (the streaming merge), independent
// of file/map iteration order.
//
// tsmFiles / walFiles are absolute paths. openTSM/readWAL are injected to avoid
// an import cycle. A TSM file that fails to open/parse aborts with an error; WAL
// tails that are truncated are tolerated inside readWAL.
func Shard(tsmFiles, walFiles []string, openTSM OpenTSM, readWAL WALReader, start, end int64, fn func(Point)) (Stats, error) {
	var st Stats

	// 1) Index each TSM file's KEY STRINGS, then CLOSE it. We deliberately do NOT
	//    hold all readers open at once: a shard can have many TSM files and that
	//    would risk exhausting the file-descriptor limit at scale. Values are read
	//    later by re-opening the relevant file per series (TSM open just reads the
	//    footer + index — cheap). For each series we record which file indices
	//    hold its keys.
	seen := map[string]struct{}{}
	var seriesOrder []string
	// series key → file index → the raw (full) keys for that series in that file.
	// Stores only key STRINGS during indexing (cheap); values are read later.
	keysBySeriesFile := map[string]map[int][]string{}
	for fi, tf := range tsmFiles {
		r, err := openTSM(tf)
		if err != nil {
			return st, err
		}
		for _, raw := range r.Keys() {
			k, perr := series.ParseKey(raw)
			if perr != nil {
				st.SkippedKey++ // key with no field separator — can't reconstruct a field
				continue
			}
			if _, ok := seen[k.SeriesKey]; !ok {
				seen[k.SeriesKey] = struct{}{}
				seriesOrder = append(seriesOrder, k.SeriesKey)
			}
			byFile := keysBySeriesFile[k.SeriesKey]
			if byFile == nil {
				byFile = map[int][]string{}
				keysBySeriesFile[k.SeriesKey] = byFile
			}
			byFile[fi] = append(byFile[fi], raw)
		}
		r.Close()
	}

	// 2) Load the WAL (small) into per-series field streams once.
	walBySeries := map[string][]fieldStream{}
	for _, wf := range walFiles {
		if err := readWAL(wf, func(key string, vals []tsm.Value) {
			k, perr := series.ParseKey(key)
			if perr != nil {
				st.SkippedKey++
				return
			}
			if _, ok := seen[k.SeriesKey]; !ok {
				seen[k.SeriesKey] = struct{}{}
				seriesOrder = append(seriesOrder, k.SeriesKey)
			}
			walBySeries[k.SeriesKey] = append(walBySeries[k.SeriesKey], fieldStream{field: k.Field, vals: vals})
		}); err != nil {
			return st, err
		}
	}

	// 3) Emit series in sorted order; per series, re-open only the file(s) that
	//    hold it, read just that series' values, then close. At most one TSM file
	//    is open at a time, and only the current series' values are in memory.
	sort.Strings(seriesOrder)
	for _, sk := range seriesOrder {
		measurement, tags := series.ParseSeriesKey(sk)
		var streams []fieldStream

		// Read this series' field streams from each file that holds it, in
		// ascending file order (deterministic). Re-open the file, read only this
		// series' keys, close — so at most one TSM file is open at a time.
		byFile := keysBySeriesFile[sk]
		for _, fi := range sortedIntKeys(byFile) {
			rawKeys := byFile[fi]
			sort.Strings(rawKeys) // deterministic field order before the merge re-sorts
			r, err := openTSM(tsmFiles[fi])
			if err != nil {
				return st, err
			}
			for _, raw := range rawKeys {
				k, _ := series.ParseKey(raw) // validated during indexing
				vals, rerr := r.ReadKeyByName(raw)
				if rerr != nil {
					r.Close()
					return st, rerr
				}
				st.Keys++
				if len(vals) > 0 {
					streams = append(streams, fieldStream{field: k.Field, vals: vals})
				}
			}
			r.Close()
		}
		// WAL streams for this series (appended AFTER TSM → WAL wins ties).
		streams = append(streams, walBySeries[sk]...)

		mergeSeries(measurement, tags, streams, start, end, &st, fn)
		// streams (and the value slices) become unreachable here and are GC'd
		// before the next series is read — this is what bounds peak memory.
	}

	return st, nil
}

// sortedIntKeys returns the int keys of m in ascending order (deterministic
// file iteration).
func sortedIntKeys(m map[int][]string) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

// mergeSeries merges all field streams of one series on the timestamp axis and
// emits one Point per distinct timestamp. It is a streaming k-way merge: it
// advances a cursor per stream and gathers, at each distinct timestamp, the
// value from every stream that has one. Memory is O(#fields) per series — NOT
// O(#values) — which is the whole point of the rewrite (the old map-of-maps
// materialized a second full copy of every value plus per-entry map overhead,
// dominating peak RSS on large shards).
//
// IMPORTANT: each stream's values MUST be ascending by timestamp for the merge.
// Compacted TSM blocks are written time-ordered, but the on-disk WAL preserves
// CLIENT WRITE ORDER and is NOT sorted — un-flushed backfills / late-arriving /
// batched-mixed-timestamp writes produce non-ascending streams, and that
// un-flushed WAL data is exactly what this tool recovers. So we defensively sort
// every stream by timestamp here (stable, cheap when already sorted). Without
// this, a non-ascending stream silently misorders output (breaking resume
// determinism), drops in-range data after an out-of-range value, and splits one
// logical point into several.
//
// Semantics: when two streams have a value at the same (ts, field) — e.g. a
// point in both a TSM file and the WAL of a partially-compacted shard —
// last-write-wins. Streams are appended TSM-then-WAL (see Shard) and the gather
// loop lets a later stream overwrite, so the WAL (newer) value wins. Field order
// in the emitted line is deterministic (streams sorted by field name).
func mergeSeries(measurement string, tags [][2]string, streams []fieldStream, start, end int64, st *Stats, fn func(Point)) {
	if len(streams) == 0 {
		return
	}

	// Sort streams by field name for deterministic field order in the output.
	// Stable sort preserves TSM-before-WAL order among equal field names, so the
	// last-write-wins tie-break (later stream overwrites) stays correct.
	sort.SliceStable(streams, func(i, j int) bool { return streams[i].field < streams[j].field })

	// Defensively ensure each stream is ascending by timestamp (see note above).
	// Stable so that, within a stream, equal timestamps keep their original order
	// (last occurrence consumed last → last-write-wins for intra-stream dupes too).
	for i := range streams {
		if !ascendingByTime(streams[i].vals) {
			vals := streams[i].vals
			sort.SliceStable(vals, func(a, b int) bool { return vals[a].UnixNano < vals[b].UnixNano })
		}
	}

	// cursor[i] is the index of the next unconsumed value in streams[i].vals.
	cursor := make([]int, len(streams))

	// fieldBuf is reused across timestamps (cleared each iteration) to avoid a
	// per-point allocation; values are tiny structs copied by value.
	var fieldBuf []lp.Field

	for {
		// Find the minimum next timestamp across all streams (within [start,end]).
		minTS := int64(0)
		have := false
		for i, sdef := range streams {
			// skip out-of-range values at the head of this stream. Streams are
			// ascending (sorted above), so once we pass `end` the rest are too.
			for cursor[i] < len(sdef.vals) {
				ts := sdef.vals[cursor[i]].UnixNano
				if ts < start {
					cursor[i]++
					continue
				}
				if ts > end {
					cursor[i] = len(sdef.vals) // ascending → nothing past here is in range
					break
				}
				if !have || ts < minTS {
					minTS = ts
					have = true
				}
				break
			}
		}
		if !have {
			break // all streams exhausted
		}

		// Gather every stream's value AT minTS (advancing those cursors). A stream
		// may hold MORE THAN ONE value at minTS (an out-of-order WAL with repeated
		// timestamps for a field) — consume ALL of them so we never emit a second
		// point at the same timestamp; the last value consumed wins, matching the
		// old map-based last-write-wins. Across streams, a later stream (WAL after
		// TSM) likewise overwrites an earlier one for the same field.
		fieldBuf = fieldBuf[:0]
		for i, sdef := range streams {
			for cursor[i] < len(sdef.vals) && sdef.vals[cursor[i]].UnixNano == minTS {
				v := sdef.vals[cursor[i]]
				cursor[i]++
				replaced := false
				for j := range fieldBuf {
					if fieldBuf[j].Name == sdef.field {
						fieldBuf[j].Value = v // tie → later occurrence wins
						replaced = true
						break
					}
				}
				if !replaced {
					fieldBuf = append(fieldBuf, lp.Field{Name: sdef.field, Value: v})
				}
			}
		}

		st.Points++
		st.Fields += len(fieldBuf)
		st.observe(minTS)
		if fn != nil {
			// Copy fieldBuf into a right-sized slice the callback may retain.
			fields := make([]lp.Field, len(fieldBuf))
			copy(fields, fieldBuf)
			fn(Point{Measurement: measurement, Tags: tags, UnixNano: minTS, Fields: fields})
		}
	}
}

// ascendingByTime reports whether vals is already sorted ascending by UnixNano,
// so the common (compacted-TSM) case skips the sort entirely.
func ascendingByTime(vals []tsm.Value) bool {
	for i := 1; i < len(vals); i++ {
		if vals[i].UnixNano < vals[i-1].UnixNano {
			return false
		}
	}
	return true
}

// EncodePoint renders a Point as a line-protocol line (with trailing newline).
func EncodePoint(p Point) string {
	var b strings.Builder
	lp.EncodePoint(&b, p.Measurement, p.Tags, p.Fields, p.UnixNano)
	return b.String()
}

// TSMFile is the minimal interface extract needs from a TSM reader, so the
// reader internals stay decoupled and the extractor is testable with fakes.
// Close releases the underlying file handle (statically guaranteed so a TSM
// reader can't silently leak fds across a large multi-shard migration).
type TSMFile interface {
	Keys() []string
	ReadKeyByName(key string) ([]tsm.Value, error)
	Close() error
}
