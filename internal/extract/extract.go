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
// Determinism — required for crash-safe resume — comes from processing files in
// the given (sorted) order and from emit()'s series+time sort. TSM and WAL data
// for the same (series, field) are merged on the timestamp axis, so a value that
// exists in both (e.g. a partially-compacted shard) collapses to one point per
// timestamp rather than duplicating.
//
// tsmFiles / walFiles are absolute paths in deterministic order. openTSM/readWAL
// are injected to avoid an import cycle. A file that fails to open/parse is
// reported via the returned error only if it is a TSM file; WAL tails that are
// truncated are tolerated inside readWAL.
func Shard(tsmFiles, walFiles []string, openTSM OpenTSM, readWAL WALReader, start, end int64, fn func(Point)) (Stats, error) {
	c := newCollector()

	for _, tf := range tsmFiles {
		r, err := openTSM(tf)
		if err != nil {
			return c.stats, err
		}
		err = addTSM(c, r)
		r.Close()
		if err != nil {
			return c.stats, err
		}
	}

	for _, wf := range walFiles {
		if err := readWAL(wf, func(key string, vals []tsm.Value) {
			c.add(key, vals)
		}); err != nil {
			return c.stats, err
		}
	}

	return c.emit(start, end, fn), nil
}

// mergeSeries merges all field streams of one series on the timestamp axis.
// Each stream is independently time-sorted; we collect the union of timestamps
// and, for each, gather whichever fields have a value at that exact ts.
func mergeSeries(measurement string, tags [][2]string, streams []fieldStream, start, end int64, st *Stats, fn func(Point)) {
	// Build ts → (field name → value). Keying by field name dedupes a (ts, field)
	// that appears in more than one stream (e.g. the same point present in both a
	// TSM file and the WAL of a partially-compacted shard, or a re-written value
	// in the WAL). Last value wins — matching InfluxDB's last-write-wins and
	// Arc's compaction dedup (ORDER BY time DESC). Streams are added in
	// TSM-then-WAL order, so the WAL (newer) value correctly overwrites.
	tsFields := map[int64]map[string]tsm.Value{}
	var order []int64

	for _, sdef := range streams {
		for _, v := range sdef.vals {
			if v.UnixNano < start || v.UnixNano > end {
				continue
			}
			fm := tsFields[v.UnixNano]
			if fm == nil {
				fm = map[string]tsm.Value{}
				tsFields[v.UnixNano] = fm
				order = append(order, v.UnixNano)
			}
			fm[sdef.field] = v // last write wins per (ts, field)
		}
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })

	for _, ts := range order {
		fm := tsFields[ts]
		names := make([]string, 0, len(fm))
		for name := range fm {
			names = append(names, name)
		}
		sort.Strings(names) // deterministic field order in the LP line
		fields := make([]lp.Field, len(names))
		for i, name := range names {
			fields[i] = lp.Field{Name: name, Value: fm[name]}
		}
		st.Points++
		st.Fields += len(fields)
		st.observe(ts)
		if fn != nil {
			fn(Point{Measurement: measurement, Tags: tags, UnixNano: ts, Fields: fields})
		}
	}
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
