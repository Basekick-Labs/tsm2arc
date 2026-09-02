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
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/basekick-labs/tsm2arc/internal/lp"
	"github.com/basekick-labs/tsm2arc/internal/series"
	"github.com/basekick-labs/tsm2arc/internal/tsm"
)

// Point is a fully reconstructed multi-field point.
//
// SeriesKey is the RAW on-disk series key (measurement + escaped tags, without
// the field suffix) — the extraction-order identity of the series. Resume
// cursors must be built from it, never from the (possibly renamed) Measurement:
// tag unescaping is lossy, so a re-escaped round trip is not byte-exact.
//
// Fields is only valid for the duration of the emit callback — the underlying
// buffer is reused for the next point. A callback that needs the values past
// its return must copy them (every current caller encodes immediately).
type Point struct {
	SeriesKey   string
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

// ---------------------------------------------------------------------------
// Value streams
//
// A stream is one field's values for one series, ascending by timestamp and
// already filtered to [start,end]. mergeSeries sees only this interface, so a
// stream can be a materialized slice (WAL — small) or a lazy block cursor over a
// TSM file (a series of any size, at bounded memory).
// ---------------------------------------------------------------------------

// stream is one field's ascending, range-filtered value cursor.
type stream interface {
	// name is the field name this stream carries.
	name() string
	// peek returns the next value without consuming it. ok is false when the
	// stream is exhausted; a non-nil error is a decode/IO failure and aborts.
	peek() (tsm.Value, bool, error)
	// advance consumes the value last returned by peek.
	advance()
	// release drops buffered values and any file-handle reference. Idempotent.
	release()
}

// sliceStream is a fully materialized stream. Used for WAL values (small) and
// for the single-file File() path.
type sliceStream struct {
	field      string
	vals       []tsm.Value
	i          int
	start, end int64
}

// newSliceStream sorts vals if needed and range-filters on read.
//
// The sort matters: the on-disk WAL preserves CLIENT WRITE ORDER, not time
// order, and un-flushed out-of-order writes are exactly what this tool recovers.
// Stable, so equal timestamps keep their original order — the last occurrence is
// consumed last, preserving last-write-wins within a stream.
func newSliceStream(field string, vals []tsm.Value, start, end int64) *sliceStream {
	if !ascendingByTime(vals) {
		sort.SliceStable(vals, func(a, b int) bool { return vals[a].UnixNano < vals[b].UnixNano })
	}
	return &sliceStream{field: field, vals: vals, start: start, end: end}
}

func (s *sliceStream) name() string { return s.field }

func (s *sliceStream) peek() (tsm.Value, bool, error) {
	for s.i < len(s.vals) {
		v := s.vals[s.i]
		if v.UnixNano < s.start {
			s.i++
			continue
		}
		if v.UnixNano > s.end {
			s.i = len(s.vals) // ascending → nothing past here is in range
			break
		}
		return v, true, nil
	}
	return tsm.Value{}, false, nil
}

func (s *sliceStream) advance() { s.i++ }

func (s *sliceStream) release() { s.vals, s.i = nil, 0 }

// blockStream is the memory bound. It decodes ONE TSM block of one (file, key)
// at a time instead of materializing the key's whole value slice. A decoded
// tsm.Value is 64 bytes against ~2-8 compressed bytes on disk, so materializing
// a multi-GB key costs tens of GB; a block caps at 1000 values (~64 KB), so an
// arbitrarily large series now costs ~one block per open stream.
//
// Blocks whose [MinTime,MaxTime] does not intersect [start,end] are skipped
// straight from the index, without being read or decoded — that is what makes
// --start/--end bound work and memory instead of merely filtering output.
type blockStream struct {
	field      string
	fs         *fileSet
	fi         int // index into fs.paths
	key        string
	start, end int64

	loaded   bool             // index entries resolved
	entries  []tsm.IndexEntry // pruned to those intersecting [start,end]
	nextE    int
	buf      []tsm.Value // the currently decoded block (or the whole key, on fallback)
	i        int
	done     bool
	released bool
	err      error
}

func (s *blockStream) name() string { return s.field }

func (s *blockStream) peek() (tsm.Value, bool, error) {
	for {
		if s.err != nil {
			return tsm.Value{}, false, s.err
		}
		if s.done {
			return tsm.Value{}, false, nil
		}
		if !s.loaded && !s.loadIndex() {
			continue // loadIndex set done or err
		}
		if s.i >= len(s.buf) {
			s.loadNext()
			continue // loadNext set buf, done, or err
		}
		v := s.buf[s.i]
		if v.UnixNano < s.start {
			s.i++
			continue
		}
		if v.UnixNano > s.end {
			// Blocks are ascending, so nothing later is in range either.
			s.finish()
			continue
		}
		return v, true, nil
	}
}

func (s *blockStream) advance() { s.i++ }

// loadIndex resolves the key's block entries on first use, prunes them to the
// time window, and picks the streaming or fallback path. Returns false if the
// stream finished or failed (the caller re-loops).
func (s *blockStream) loadIndex() bool {
	s.loaded = true
	r, err := s.fs.get(s.fi)
	if err != nil {
		s.fail(err)
		return false
	}
	ents := r.Blocks(s.key)
	if len(ents) == 0 {
		s.finish()
		return false
	}
	// Streaming relies on a key's blocks being ascending and non-overlapping,
	// which is a TSM invariant for a written/compacted file. A file that violates
	// it would silently misorder output, so fall back to reading and sorting the
	// whole key — for that ONE key, in that ONE file, not the whole series.
	if !entriesAscending(ents) {
		vals, rerr := r.ReadKeyByName(s.key)
		if rerr != nil {
			s.fail(rerr)
			return false
		}
		if !ascendingByTime(vals) {
			sort.SliceStable(vals, func(a, b int) bool { return vals[a].UnixNano < vals[b].UnixNano })
		}
		s.buf, s.i = vals, 0
		s.entries, s.nextE = nil, 0
		return true
	}
	s.entries = pruneEntries(ents, s.start, s.end)
	return true
}

// loadNext decodes the next in-range block into buf.
func (s *blockStream) loadNext() {
	if s.nextE >= len(s.entries) {
		s.finish()
		return
	}
	r, err := s.fs.get(s.fi)
	if err != nil {
		s.fail(err)
		return
	}
	vals, err := r.ReadBlockAt(s.key, s.entries[s.nextE])
	if err != nil {
		s.fail(err)
		return
	}
	s.nextE++
	// Defensive, and cheap: ascendingByTime short-circuits on the normal path.
	if !ascendingByTime(vals) {
		sort.SliceStable(vals, func(a, b int) bool { return vals[a].UnixNano < vals[b].UnixNano })
	}
	s.buf, s.i = vals, 0
}

func (s *blockStream) finish() {
	s.done = true
	s.release()
}

func (s *blockStream) fail(err error) {
	s.err = err
	s.done = true
	s.release()
}

// release drops the decoded block and the file-handle reference. Dropping the
// reference eagerly is what keeps the open-fd count at one or two even when a
// single key is split across a hundred TSM files.
func (s *blockStream) release() {
	if s.released {
		return
	}
	s.released = true
	s.buf, s.i, s.entries = nil, 0, nil
	s.fs.release(s.fi)
}

// pruneEntries narrows ascending, non-overlapping block entries to those that
// intersect [start,end]. Pruned blocks are never read from disk.
func pruneEntries(ents []tsm.IndexEntry, start, end int64) []tsm.IndexEntry {
	lo := 0
	for lo < len(ents) && ents[lo].MaxTime < start {
		lo++
	}
	hi := lo
	for hi < len(ents) && ents[hi].MinTime <= end {
		hi++
	}
	return ents[lo:hi]
}

// entriesAscending reports whether a key's blocks are ordered and
// non-overlapping in time. Equal boundary timestamps are fine — the merge
// consumes every value at a timestamp, across block boundaries.
func entriesAscending(ents []tsm.IndexEntry) bool {
	for i := range ents {
		if ents[i].MinTime > ents[i].MaxTime {
			return false
		}
		if i > 0 && ents[i].MinTime < ents[i-1].MaxTime {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// File handles
// ---------------------------------------------------------------------------

// fileSet opens the TSM files one series needs, lazily and at most once, and
// closes each as soon as the last stream referencing it is done.
//
// Streaming means more than one file can be open at a time (the pre-0.1.4 code
// kept exactly one), and that needs managing: a key big enough to be split
// across files by the TSM writer's size cap can touch a hundred of them. Those
// parts hold DISJOINT ascending time ranges, so lazy open plus eager close keeps
// the concurrently-open count at one or two rather than the whole set.
type fileSet struct {
	paths []string
	open  OpenTSM
	rs    map[int]TSMFile
	refs  map[int]int
}

func newFileSet(paths []string, open OpenTSM) *fileSet {
	return &fileSet{paths: paths, open: open, rs: map[int]TSMFile{}, refs: map[int]int{}}
}

// newBlockStream registers a reference to file fi and returns its lazy stream.
func (fs *fileSet) newBlockStream(field string, fi int, key string, start, end int64) *blockStream {
	fs.refs[fi]++
	return &blockStream{field: field, fs: fs, fi: fi, key: key, start: start, end: end}
}

// get returns the open reader for file fi, opening it on first use.
func (fs *fileSet) get(fi int) (TSMFile, error) {
	if r, ok := fs.rs[fi]; ok {
		return r, nil
	}
	r, err := fs.open(fs.paths[fi])
	if err != nil {
		return nil, err
	}
	fs.rs[fi] = r
	return r, nil
}

// release drops one reference to file fi, closing it at zero.
func (fs *fileSet) release(fi int) {
	fs.refs[fi]--
	if fs.refs[fi] > 0 {
		return
	}
	delete(fs.refs, fi)
	if r, ok := fs.rs[fi]; ok {
		r.Close()
		delete(fs.rs, fi)
	}
}

// closeAll closes every still-open reader (safety net on the error path).
func (fs *fileSet) closeAll() {
	for fi, r := range fs.rs {
		r.Close()
		delete(fs.rs, fi)
	}
	fs.refs = map[int]int{}
}

// ---------------------------------------------------------------------------
// Single-file path (File)
// ---------------------------------------------------------------------------

// group accumulates all field streams for one series.
type group struct {
	seriesKey   string
	measurement string
	tags        [][2]string
	streams     []stream
}

// collector groups (key, values) streams by series for later field-rejoin.
type collector struct {
	groups     []*group
	bySeries   map[string]*group
	stats      Stats
	start, end int64
}

func newCollector(start, end int64) *collector {
	return &collector{bySeries: map[string]*group{}, start: start, end: end}
}

// add folds one raw TSM key and its decoded values into the right series group.
// A key that can't be parsed (no field separator) is counted as skipped.
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
	g.streams = append(g.streams, newSliceStream(k.Field, vals, c.start, c.end))
}

// emit field-rejoins every series in deterministic order and yields points.
func (c *collector) emit(fn func(Point)) (Stats, error) {
	sort.Slice(c.groups, func(i, j int) bool { return c.groups[i].seriesKey < c.groups[j].seriesKey })
	for _, g := range c.groups {
		if err := mergeSeries(g.seriesKey, g.measurement, g.tags, g.streams, &c.stats, fn); err != nil {
			return c.stats, err
		}
	}
	return c.stats, nil
}

// File reads a TSM file and yields reconstructed points to fn, in deterministic
// order (series key ascending, then timestamp ascending). Optional time bounds
// [start,end] (ns, inclusive) filter points; pass minInt64/maxInt64 for none.
// Returns stats. fn may be nil for pure counting (dry-run).
//
// This path materializes the whole file; it exists for tests and single-file
// use. The migration path is Shard, which streams.
func File(r TSMFile, start, end int64, fn func(Point)) (Stats, error) {
	c := newCollector(start, end)
	if err := addTSM(c, r); err != nil {
		return c.stats, err
	}
	return c.emit(fn)
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

// walStream is one WAL-sourced field stream awaiting its series' turn.
type walStream struct {
	field string
	vals  []tsm.Value
}

// ---------------------------------------------------------------------------
// Shard extraction
// ---------------------------------------------------------------------------

// Shard reconstructs all points of one shard from its TSM files AND its WAL
// files, field-rejoining across both sources, and yields points to fn in
// deterministic order (series key ascending, then timestamp ascending).
//
// MEMORY: this streams ONE SERIES AT A TIME and, within a series, ONE BLOCK AT A
// TIME per field. It indexes only the key strings of each TSM file up front
// (cheap — strings, not values), then merges each series straight off lazy block
// cursors. Peak memory is therefore bounded by the number of concurrent field
// streams times one decoded block (~64 KB) — NOT by the largest series, and not
// by the shard. That distinction is the difference between migrating a shard
// with a 250 GB single series and needing terabytes of RAM to do it: a decoded
// tsm.Value is 64 bytes against ~2-8 compressed bytes on disk, so materializing
// a series costs 8-32x its on-disk size.
//
// The WAL (un-flushed, typically small) is loaded once into a per-series map and
// merged in alongside the streamed TSM values. TSM streams are ordered before
// WAL streams for each series so the WAL (newer) wins on a same-(ts,field) tie
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
	return ShardResume(tsmFiles, walFiles, openTSM, readWAL, start, end, nil, fn)
}

// Cursor identifies the last point of a shard already delivered downstream, as
// (raw series key, timestamp). Because a shard's emission order is deterministic
// (series key ascending, then strictly ascending unique timestamps within a
// series), a cursor splits the shard's output exactly: everything at or before
// it has been emitted, everything after has not.
type Cursor struct {
	SeriesKey string
	UnixNano  int64
}

// ShardResume is Shard with an optional resume cursor. A nil cursor extracts
// everything. With a cursor, series ordered before cursor.SeriesKey are skipped
// wholesale (no block reads), and the cursor series is emitted from
// cursor.UnixNano+1 — block pruning then skips its already-delivered blocks, so
// resuming deep into a large shard reads almost nothing it doesn't need.
//
// The cursor series must exist in the shard: extraction order is only
// meaningful against the same source data, so a missing series means the shard
// changed since the cursor was written, and ShardResume fails loudly rather
// than silently mis-aligning.
func ShardResume(tsmFiles, walFiles []string, openTSM OpenTSM, readWAL WALReader, start, end int64, cur *Cursor, fn func(Point)) (Stats, error) {
	return ShardResumeSplit(tsmFiles, walFiles, openTSM, readWAL, start, end, cur, SplitOptions{}, fn)
}

// SplitOptions controls intra-shard parallelism. The zero value (Workers <= 1)
// is the serial path — the exact 0.1.5 code, bypassing the split machinery
// entirely. Workers > 1 extracts a shard with concurrent window-merge tasks
// while emitting BYTE-IDENTICAL output in the identical order (see split.go);
// because output is unchanged, these options are pure performance knobs: they
// are not part of the resume fingerprint and may change between runs, including
// between a crash and its resume.
type SplitOptions struct {
	// Workers is the maximum number of concurrent merge tasks.
	Workers int
	// MemoryBudget bounds the estimated decoded-block footprint of concurrently
	// admitted tasks. Required (> 0) when Workers > 1: worker count alone is not
	// a safe knob — a single window over fully overlapping generations can hold
	// one decoded block per (file × field). A task whose estimate exceeds the
	// whole budget runs alone, degrading to the serial memory profile.
	MemoryBudget int64
}

// shardIndex is the phase-1/2 product shared by the serial and split paths:
// which series exist, which files hold them (with time ranges), and the WAL
// values per series.
type shardIndex struct {
	seriesOrder      []string
	keysBySeriesFile map[string]map[int]*fileKeys
	walBySeries      map[string][]walStream
}

// indexShard runs the index pass (phase 1) and WAL load (phase 2).
//
// Phase 1 opens each TSM file's KEY STRINGS, then CLOSEs it. We deliberately do
// NOT hold all readers open through this pass: a shard can have many TSM files
// and that would risk exhausting the file-descriptor limit at scale. Values are
// read later by re-opening the relevant file per series (TSM open just reads
// the footer + index — cheap, and cheaper still behind an index cache). For
// each series we record which file indices hold its keys and the time range
// they span — the range comes free from the block index and lets phase 3 avoid
// holding files open whose data cannot interleave (see partitionRuns).
func indexShard(tsmFiles, walFiles []string, openTSM OpenTSM, readWAL WALReader, st *Stats) (*shardIndex, error) {
	si := &shardIndex{
		keysBySeriesFile: map[string]map[int]*fileKeys{},
		walBySeries:      map[string][]walStream{},
	}
	seen := map[string]struct{}{}
	for fi, tf := range tsmFiles {
		r, err := openTSM(tf)
		if err != nil {
			return nil, err
		}
		for _, raw := range r.Keys() {
			k, perr := series.ParseKey(raw)
			if perr != nil {
				st.SkippedKey++ // key with no field separator — can't reconstruct a field
				continue
			}
			if _, ok := seen[k.SeriesKey]; !ok {
				seen[k.SeriesKey] = struct{}{}
				si.seriesOrder = append(si.seriesOrder, k.SeriesKey)
			}
			byFile := si.keysBySeriesFile[k.SeriesKey]
			if byFile == nil {
				byFile = map[int]*fileKeys{}
				si.keysBySeriesFile[k.SeriesKey] = byFile
			}
			fk := byFile[fi]
			if fk == nil {
				fk = &fileKeys{min: math.MaxInt64, max: math.MinInt64}
				byFile[fi] = fk
			}
			fk.keys = append(fk.keys, raw)
			for _, e := range r.Blocks(raw) {
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

	for _, wf := range walFiles {
		if err := readWAL(wf, func(key string, vals []tsm.Value) {
			k, perr := series.ParseKey(key)
			if perr != nil {
				st.SkippedKey++
				return
			}
			if _, ok := seen[k.SeriesKey]; !ok {
				seen[k.SeriesKey] = struct{}{}
				si.seriesOrder = append(si.seriesOrder, k.SeriesKey)
			}
			si.walBySeries[k.SeriesKey] = append(si.walBySeries[k.SeriesKey], walStream{field: k.Field, vals: vals})
		}); err != nil {
			return nil, err
		}
	}
	return si, nil
}

// applyCursor sorts the series order and, given a resume cursor, drops every
// series wholly before it. The cursor series must be present — a shard whose
// series set changed since the cursor was written cannot be resumed by
// position.
func (si *shardIndex) applyCursor(cur *Cursor) error {
	sort.Strings(si.seriesOrder)
	if cur == nil {
		return nil
	}
	i := sort.SearchStrings(si.seriesOrder, cur.SeriesKey)
	if i == len(si.seriesOrder) || si.seriesOrder[i] != cur.SeriesKey {
		return fmt.Errorf("resume cursor series %q not found in shard: source data changed since the checkpoint was written", cur.SeriesKey)
	}
	si.seriesOrder = si.seriesOrder[i:]
	return nil
}

// seriesStartFor returns the effective lower time bound for one series under a
// resume cursor, and whether the series has anything left to emit. Timestamps
// within a series are strictly ascending and unique, so cursor.UnixNano+1 is an
// exact boundary; MaxInt64 marks the series fully delivered (+1 would overflow).
func seriesStartFor(sk string, start int64, cur *Cursor) (int64, bool) {
	if cur == nil || sk != cur.SeriesKey {
		return start, true
	}
	if cur.UnixNano == math.MaxInt64 {
		return 0, false
	}
	if s := cur.UnixNano + 1; s > start {
		return s, true
	}
	return start, true
}

// ShardResumeSplit is ShardResume with intra-shard parallelism options. Output
// is byte-identical across every SplitOptions value; see SplitOptions.
func ShardResumeSplit(tsmFiles, walFiles []string, openTSM OpenTSM, readWAL WALReader, start, end int64, cur *Cursor, opt SplitOptions, fn func(Point)) (Stats, error) {
	var st Stats
	si, err := indexShard(tsmFiles, walFiles, openTSM, readWAL, &st)
	if err != nil {
		return st, err
	}
	if err := si.applyCursor(cur); err != nil {
		return st, err
	}

	if opt.Workers > 1 {
		return shardParallel(si, tsmFiles, openTSM, start, end, cur, opt, st, fn)
	}

	// Serial path (the 0.1.5 behavior, untouched): emit series in sorted order.
	// Each series' fields become lazy cursors over the file(s) holding them; the
	// merge pulls one block at a time, so only the blocks currently being merged
	// are resident.
	fs := newFileSet(tsmFiles, openTSM)
	defer fs.closeAll()

	keysBySeriesFile, walBySeries := si.keysBySeriesFile, si.walBySeries
	for _, sk := range si.seriesOrder {
		// Within the cursor series, resume from the next timestamp; block pruning
		// (pruneEntries / sliceStream filtering) then skips everything already
		// delivered. Timestamps within a series are strictly ascending and unique,
		// so cursor.UnixNano+1 is an exact boundary.
		seriesStart := start
		if cur != nil && sk == cur.SeriesKey {
			if cur.UnixNano == math.MaxInt64 {
				continue // series fully delivered; +1 would overflow
			}
			if s := cur.UnixNano + 1; s > seriesStart {
				seriesStart = s
			}
		}
		measurement, tags := series.ParseSeriesKey(sk)
		byFile := keysBySeriesFile[sk]
		walStreams := walBySeries[sk]

		// Files whose time ranges cannot interleave are merged as separate,
		// sequential runs — see partitionRuns. Each run opens only its own files.
		for _, run := range partitionRuns(byFile, len(walStreams) > 0) {
			var streams []stream

			// One cursor per (file, key), in ascending FILE INDEX order. Files are
			// sorted by name, i.e. by TSM generation, so a later file is the newer
			// copy of a key and — being later in this slice, which the stable sort
			// in mergeSeries preserves — wins a same-(ts,field) tie.
			for _, fi := range run {
				rawKeys := byFile[fi].keys
				sort.Strings(rawKeys) // deterministic field order before the merge re-sorts
				for _, raw := range rawKeys {
					k, _ := series.ParseKey(raw) // validated during indexing
					st.Keys++
					streams = append(streams, fs.newBlockStream(k.Field, fi, raw, seriesStart, end))
				}
			}
			// WAL streams for this series (appended AFTER TSM → WAL wins ties).
			// partitionRuns guarantees a single run whenever the series has WAL
			// data, so these are merged against every TSM stream, not a subset.
			for _, ws := range walStreams {
				streams = append(streams, newSliceStream(ws.field, ws.vals, seriesStart, end))
			}

			err := mergeSeries(sk, measurement, tags, streams, &st, fn)
			// Release every cursor — including ones the merge never reached — so
			// the run's file handles and blocks are gone before the next starts.
			for _, s := range streams {
				s.release()
			}
			if err != nil {
				return st, err
			}
		}
		delete(walBySeries, sk) // drop this series' WAL values too
	}

	return st, nil
}

// fileKeys is one series' keys within one TSM file, plus the time range those
// keys span (folded from the block index during the indexing pass).
type fileKeys struct {
	keys     []string
	min, max int64
}

// partitionRuns splits a series' files into groups that must be merged together,
// ordered so that emitting run after run still yields globally ascending
// timestamps.
//
// Why: a k-way merge has to peek EVERY stream to find the next timestamp, so a
// single run holds one file descriptor per file containing the series. A key big
// enough to be split across TSM files by the writer's size cap can live in a
// hundred of them — times --workers, that is enough to exhaust the fd limit. But
// those parts hold DISJOINT time ranges, and disjoint ranges cannot interleave:
// merging them separately, in time order, produces exactly the same output while
// holding only one file open.
//
// Files whose ranges overlap stay in the same run, so last-write-wins between
// two generations of the same key is untouched (a same-timestamp conflict
// implies overlap implies same run). A series with WAL data is always a single
// run: WAL values can land at any timestamp and must be able to overwrite any
// TSM value, so they have to see every TSM stream at once.
func partitionRuns(byFile map[int]*fileKeys, hasWAL bool) [][]int {
	fis := sortedIntKeys(byFile)
	if hasWAL || len(fis) < 2 {
		return [][]int{fis}
	}

	// Order by start time (file index breaks ties) to find disjoint groups.
	order := append([]int(nil), fis...)
	sort.SliceStable(order, func(a, b int) bool {
		fa, fb := byFile[order[a]], byFile[order[b]]
		if fa.min != fb.min {
			return fa.min < fb.min
		}
		return order[a] < order[b]
	})

	var runs [][]int
	var cur []int
	curMax := int64(math.MinInt64)
	for _, fi := range order {
		fk := byFile[fi]
		if len(cur) > 0 && fk.min > curMax {
			runs = append(runs, cur)
			cur, curMax = nil, math.MinInt64
		}
		cur = append(cur, fi)
		if fk.max > curMax {
			curMax = fk.max
		}
	}
	if len(cur) > 0 {
		runs = append(runs, cur)
	}
	// Within a run, restore ascending file-index order: that is what makes the
	// newer generation win a same-timestamp tie in mergeSeries.
	for _, r := range runs {
		sort.Ints(r)
	}
	return runs
}

// sortedIntKeys returns the int keys of m in ascending order (deterministic
// file iteration).
func sortedIntKeys(m map[int]*fileKeys) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

// mergeSeries merges all field streams of one series on the timestamp axis and
// emits one Point per distinct timestamp. It is a streaming k-way merge: it
// peeks each stream and gathers, at each distinct timestamp, the value from
// every stream that has one. Memory is O(#streams × one block) — not O(#values).
//
// Streams arrive ascending by timestamp and pre-filtered to the time window (see
// sliceStream / blockStream), so this loop is purely the merge.
//
// COST: the find-min and gather passes below scan every stream once per emitted
// timestamp. Before coalescing, a series had one stream per (file, field), so a
// shard with hundreds of overlapping TSM generations and wide rows paid
// O(files × fields) peeks per row — the dominant CPU cost of a migration.
// coalesceByField collapses that to one stream per field, and fieldMergeStream
// makes each per-field advance O(log files) via a heap, so the per-row cost is
// O(fields + values·log files).
//
// Semantics: when two streams have a value at the same (ts, field) — e.g. a
// point in both a TSM file and the WAL of a partially-compacted shard —
// last-write-wins. Streams are ordered TSM-then-WAL (see Shard) and the gather
// loop lets a later stream overwrite, so the WAL (newer) value wins; a
// fieldMergeStream yields equal-timestamp values in exactly that child order,
// so coalescing preserves the winner byte for byte. Field order in the emitted
// line is deterministic (streams sorted by field name).
func mergeSeries(seriesKey, measurement string, tags [][2]string, streams []stream, st *Stats, fn func(Point)) error {
	if len(streams) == 0 {
		return nil
	}

	// Collapse to one stream per field (preserving the original stream order
	// within each field — that order IS the last-write-wins precedence).
	streams = coalesceByField(streams)

	// Sort streams by field name for deterministic field order in the output.
	// Stable sort preserves TSM-before-WAL order (and older-before-newer file
	// order) among equal field names, so the last-write-wins tie-break — a later
	// stream overwrites — stays correct. (After coalescing names are unique, but
	// stability is kept so the invariant doesn't depend on that.)
	sort.SliceStable(streams, func(i, j int) bool { return streams[i].name() < streams[j].name() })

	// fieldBuf is reused across timestamps (cleared each iteration) to avoid a
	// per-point allocation; values are tiny structs copied by value.
	var fieldBuf []lp.Field

	for {
		// Find the minimum next timestamp across all streams.
		minTS := int64(0)
		have := false
		for _, s := range streams {
			v, ok, err := s.peek()
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if !have || v.UnixNano < minTS {
				minTS, have = v.UnixNano, true
			}
		}
		if !have {
			return nil // all streams exhausted
		}

		// Gather every stream's value AT minTS (advancing those cursors). A stream
		// may hold MORE THAN ONE value at minTS (an out-of-order WAL with repeated
		// timestamps for a field) — consume ALL of them so we never emit a second
		// point at the same timestamp; the last value consumed wins. Across
		// streams, a later stream (WAL after TSM) likewise overwrites an earlier
		// one for the same field.
		fieldBuf = fieldBuf[:0]
		for _, s := range streams {
			for {
				v, ok, err := s.peek()
				if err != nil {
					return err
				}
				if !ok || v.UnixNano != minTS {
					break
				}
				s.advance()
				replaced := false
				for j := range fieldBuf {
					if fieldBuf[j].Name == s.name() {
						fieldBuf[j].Value = v // tie → later occurrence wins
						replaced = true
						break
					}
				}
				if !replaced {
					fieldBuf = append(fieldBuf, lp.Field{Name: s.name(), Value: v})
				}
			}
		}

		st.Points++
		st.Fields += len(fieldBuf)
		st.observe(minTS)
		if fn != nil {
			// fieldBuf is reused for the next timestamp: Fields is only valid
			// during the callback (documented on Point). Not copying here removes
			// a per-row allocation that dominated GC churn on wide rows.
			fn(Point{SeriesKey: seriesKey, Measurement: measurement, Tags: tags, UnixNano: minTS, Fields: fieldBuf})
		}
	}
}

// coalesceByField groups streams by field name, wrapping each multi-stream
// field into one fieldMergeStream. Within a field the original stream order is
// preserved — older TSM generation first, WAL last — because that order is the
// last-write-wins precedence the merge relies on.
func coalesceByField(streams []stream) []stream {
	byName := map[string][]stream{}
	var order []string
	for _, s := range streams {
		n := s.name()
		if _, ok := byName[n]; !ok {
			order = append(order, n)
		}
		byName[n] = append(byName[n], s)
	}
	if len(order) == len(streams) {
		return streams // every field has one stream — nothing to wrap
	}
	out := make([]stream, 0, len(order))
	for _, n := range order {
		if g := byName[n]; len(g) == 1 {
			out = append(out, g[0])
		} else {
			out = append(out, &fieldMergeStream{field: n, children: g})
		}
	}
	return out
}

// fieldMergeStream merges every stream of ONE field (its copies across TSM
// generations, plus WAL) into a single ascending cursor, so the outer merge
// scans one stream per field instead of one per (file, field).
//
// It yields ALL values, including duplicates at equal timestamps, in exactly
// the order the outer gather loop used to consume them: timestamp ascending,
// and at equal timestamps in child order (older generation before newer before
// WAL, each child's own order within itself). The outer loop's
// "later value overwrites" rule therefore picks the identical winner, keeping
// output byte-identical with the uncoalesced merge.
//
// The heap is keyed on (timestamp, child ordinal). After advancing a child, its
// next value re-enters at the same ordinal, so a child's consecutive
// equal-timestamp values drain fully before a later child's — matching the old
// per-stream inner loop.
type fieldMergeStream struct {
	field    string
	children []stream
	h        []fmsEntry // min-heap on (v.UnixNano, idx)
	inited   bool
	err      error
	released bool
}

// fmsEntry is one child's memoized head value.
type fmsEntry struct {
	v   tsm.Value
	idx int // ordinal in children — the equal-timestamp tie-break
}

func (s *fieldMergeStream) name() string { return s.field }

func (s *fieldMergeStream) peek() (tsm.Value, bool, error) {
	if s.err != nil {
		return tsm.Value{}, false, s.err
	}
	if !s.inited {
		s.inited = true
		for i, c := range s.children {
			v, ok, err := c.peek()
			if err != nil {
				s.err = err
				return tsm.Value{}, false, err
			}
			if ok {
				s.h = append(s.h, fmsEntry{v: v, idx: i})
			}
		}
		for i := len(s.h)/2 - 1; i >= 0; i-- {
			s.siftDown(i)
		}
	}
	if len(s.h) == 0 {
		return tsm.Value{}, false, nil
	}
	return s.h[0].v, true, nil
}

// advance consumes the current minimum and refills from that child, restoring
// the heap in O(log children).
func (s *fieldMergeStream) advance() {
	if len(s.h) == 0 {
		return
	}
	c := s.children[s.h[0].idx]
	c.advance()
	v, ok, err := c.peek()
	if err != nil {
		s.err = err // surfaced by the next peek
		return
	}
	if ok {
		s.h[0].v = v
		s.siftDown(0)
		return
	}
	last := len(s.h) - 1
	s.h[0] = s.h[last]
	s.h = s.h[:last]
	if len(s.h) > 0 {
		s.siftDown(0)
	}
}

func (s *fieldMergeStream) release() {
	if s.released {
		return
	}
	s.released = true
	for _, c := range s.children {
		c.release()
	}
	s.h = nil
}

func (s *fieldMergeStream) less(a, b fmsEntry) bool {
	if a.v.UnixNano != b.v.UnixNano {
		return a.v.UnixNano < b.v.UnixNano
	}
	return a.idx < b.idx
}

func (s *fieldMergeStream) siftDown(i int) {
	for {
		l, r := 2*i+1, 2*i+2
		min := i
		if l < len(s.h) && s.less(s.h[l], s.h[min]) {
			min = l
		}
		if r < len(s.h) && s.less(s.h[r], s.h[min]) {
			min = r
		}
		if min == i {
			return
		}
		s.h[i], s.h[min] = s.h[min], s.h[i]
		i = min
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
//
// Blocks + ReadBlockAt are the bounded-memory path: they let the extractor skip
// out-of-window blocks from the index and decode the rest one at a time.
// ReadKeyByName materializes a whole key and is used only by File() and by the
// fallback for a file whose blocks are not ascending.
//
// Close releases the underlying file handle (statically guaranteed so a TSM
// reader can't silently leak fds across a large multi-shard migration).
type TSMFile interface {
	Keys() []string
	ReadKeyByName(key string) ([]tsm.Value, error)
	Blocks(key string) []tsm.IndexEntry
	ReadBlockAt(key string, e tsm.IndexEntry) ([]tsm.Value, error)
	Close() error
}
