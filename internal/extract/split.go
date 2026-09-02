// Intra-shard parallel extraction (--shard-split).
//
// The shard's work is decomposed into an ORDERED task list — series in key
// order, runs in time order (partitionRuns), and large runs split into
// disjoint, inclusive time windows — executed by N workers, with a single
// consumer re-emitting completed tasks strictly in list order through the
// caller's fn. Output is therefore BYTE-IDENTICAL to the serial path: same
// points, same order, same everything downstream (chunks, cursors, audit).
//
// INVARIANT (do not weaken): window membership is a pure function of
// TIMESTAMP. Windows are inclusive, disjoint, integer-partitioned
// ([prev+1, split]); block index entries are only a mechanism for choosing
// split points and pruning I/O, and a block whose time range spans a boundary
// is decoded by BOTH adjacent window tasks, each filtering to its own window.
// TSM legally places the same timestamp at the edge of two adjacent blocks
// (entriesAscending accepts MinTime == prev MaxTime); assigning BLOCKS to
// windows would emit that point twice. Timestamp filtering handles it by
// construction: every value at ts T belongs to exactly the window containing
// T, where all of the run's streams participate, so last-write-wins resolves
// identically to the full merge.
//
// INVARIANT (admission): tasks are admitted strictly in list order — task i+1
// never takes budget or a worker slot while task i is unadmitted (guaranteed
// here by a single sequential scheduler goroutine). Budget is released at task
// RETIREMENT (consumer finished draining), not worker completion. A task whose
// estimate exceeds the whole budget is clamped to it and therefore waits until
// nothing else runs, then runs alone — the serial memory profile, never a
// deadlock.
//
// Shared state: workers share the immutable tsm.Index cache (via the injected
// OpenTSM) and the WAL value slices. Both are made read-only BEFORE workers
// exist: per-file key slices are sorted at build time, and WAL streams are
// pre-sorted so newSliceStream's lazy in-place sort never fires.
package extract

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/basekick-labs/tsm2arc/internal/lp"
	"github.com/basekick-labs/tsm2arc/internal/series"
	"github.com/basekick-labs/tsm2arc/internal/tsm"
)

// splitRunThreshold is the minimum compressed run size worth windowing. A
// variable only so tests can force windowing on small fixtures; output is
// byte-identical at every value by construction.
var splitRunThreshold int64 = 256 << 20

const (
	// batchArenaFields sizes a point batch's field arena (~10 MiB of lp.Field).
	// Batches are bounded by FIELD COUNT, never point count: at ~80 B per field
	// a wide row is ~120 KB, and point-bounded batches would be gigabytes.
	batchArenaFields = 1 << 17
	// decodedBlockEstimate is the resident cost of one decoded numeric block.
	decodedBlockEstimate = 64 << 10
)

// taskStream identifies one (file, key) stream participating in a task.
type taskStream struct {
	fi    int
	key   string
	field string
}

// splitTask is one unit of parallel work: one series, one run, one window.
type splitTask struct {
	sk          string
	measurement string
	tags        [][2]string
	streams     []taskStream // pruned to the window, in tie-break order (file asc)
	wal         []walStream  // the series' WAL streams (windows filter by ts)
	start, end  int64        // inclusive window bounds ∩ [seriesStart, global end]
	est         int64        // admission estimate (decoded blocks + batch overhead)
}

// pointBatch carries merged points from a worker to the consumer. Fields
// subslice the arena, which never reallocates while non-empty (emit seals the
// batch first), so subslices stay valid until the batch is recycled.
type pointBatch struct {
	pts   []Point
	arena []lp.Field
}

type batchPool struct{ ch chan *pointBatch }

func (p *batchPool) get() *pointBatch {
	select {
	case b := <-p.ch:
		b.pts = b.pts[:0]
		b.arena = b.arena[:0]
		return b
	default:
		return &pointBatch{
			pts:   make([]Point, 0, 512),
			arena: make([]lp.Field, 0, batchArenaFields),
		}
	}
}

func (p *batchPool) put(b *pointBatch) {
	select {
	case p.ch <- b:
	default: // pool full — let it be collected
	}
}

// batchEmitter adapts mergeSeries' fn(Point) callback into batched, arena-
// backed sends. On context cancellation it drops output and lets the merge run
// out — mergeSeries has no abort hook, and a window's remainder is bounded.
type batchEmitter struct {
	ctx     context.Context
	out     chan<- *pointBatch
	pool    *batchPool
	cur     *pointBatch
	dropped bool
}

func (e *batchEmitter) emit(p Point) {
	if e.dropped {
		return
	}
	need := len(p.Fields)
	if e.cur == nil {
		e.cur = e.pool.get()
	}
	// Seal before overflow so a non-empty arena never reallocates (earlier
	// points' Fields subslice it). An oversized single row grows the arena only
	// while the batch is empty, which is safe.
	if len(e.cur.pts) > 0 && len(e.cur.arena)+need > cap(e.cur.arena) {
		e.flush()
		if e.dropped {
			return
		}
		e.cur = e.pool.get()
	}
	if need > cap(e.cur.arena) {
		e.cur.arena = make([]lp.Field, 0, need)
	}
	off := len(e.cur.arena)
	e.cur.arena = append(e.cur.arena, p.Fields...)
	p.Fields = e.cur.arena[off : off+need]
	e.cur.pts = append(e.cur.pts, p)
}

func (e *batchEmitter) flush() {
	if e.cur == nil || len(e.cur.pts) == 0 {
		return
	}
	select {
	case e.out <- e.cur:
	case <-e.ctx.Done():
		e.dropped = true
	}
	e.cur = nil
}

// memBudget is the admission budget. Only the (single, sequential) scheduler
// acquires, so head-of-line ordering is structural; release comes from the
// consumer at task retirement. Cancellation wakes any waiter.
type memBudget struct {
	mu     sync.Mutex
	cond   *sync.Cond
	limit  int64
	used   int64
	closed bool
}

func newMemBudget(ctx context.Context, limit int64) *memBudget {
	b := &memBudget{limit: limit}
	b.cond = sync.NewCond(&b.mu)
	go func() {
		<-ctx.Done()
		b.mu.Lock()
		b.closed = true
		b.cond.Broadcast()
		b.mu.Unlock()
	}()
	return b
}

// acquire blocks until n fits (n is pre-clamped to the limit). Returns false
// on cancellation.
func (b *memBudget) acquire(n int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for b.used+n > b.limit && !b.closed {
		b.cond.Wait()
	}
	if b.closed {
		return false
	}
	b.used += n
	return true
}

func (b *memBudget) release(n int64) {
	b.mu.Lock()
	b.used -= n
	b.cond.Broadcast()
	b.mu.Unlock()
}

// buildTasks decomposes the shard into the ordered task list. Serial: it may
// mutate shared per-series state (key sorting, WAL pre-sort) precisely because
// no worker exists yet. st receives the Keys/SkippedKey accounting exactly as
// the serial path would count it (once per (run, file, key)).
func buildTasks(si *shardIndex, tsmFiles []string, openTSM OpenTSM, start, end int64, cur *Cursor, opt SplitOptions, st *Stats) ([]splitTask, error) {
	var tasks []splitTask
	for _, sk := range si.seriesOrder {
		seriesStart, ok := seriesStartFor(sk, start, cur)
		if !ok {
			continue
		}
		measurement, tags := series.ParseSeriesKey(sk)
		byFile := si.keysBySeriesFile[sk]
		walStreams := si.walBySeries[sk]

		// Make shared state read-only before workers touch it: key order fixed
		// here (the serial path sorts in place at stream build), WAL values
		// pre-sorted so newSliceStream's lazy sort (also in place) never fires.
		for _, fk := range byFile {
			sort.Strings(fk.keys)
		}
		var walBytes int64
		for i := range walStreams {
			vals := walStreams[i].vals
			if !ascendingByTime(vals) {
				sort.SliceStable(vals, func(a, b int) bool { return vals[a].UnixNano < vals[b].UnixNano })
			}
			walBytes += int64(len(vals)) * 64
		}

		for _, run := range partitionRuns(byFile, len(walStreams) > 0) {
			// Collect every stream's index entries (one open per file — cheap
			// behind the index cache) and detect the non-ascending fallback case.
			type keyEnts struct {
				ts      taskStream
				ents    []tsm.IndexEntry
				maxSize uint32
			}
			var kes []keyEnts
			var all []tsm.IndexEntry
			var runBytes int64
			runMin, runMax := int64(math.MaxInt64), int64(math.MinInt64)
			allAscending := true
			for _, fi := range run {
				r, err := openTSM(tsmFiles[fi])
				if err != nil {
					return nil, err
				}
				for _, raw := range byFile[fi].keys {
					k, _ := series.ParseKey(raw) // validated during indexing
					st.Keys++
					ents := r.Blocks(raw)
					if len(ents) == 0 {
						continue
					}
					if !entriesAscending(ents) {
						allAscending = false
					}
					ke := keyEnts{ts: taskStream{fi: fi, key: raw, field: k.Field}}
					ke.ents = ents
					for _, e := range ents {
						runBytes += int64(e.Size)
						if e.Size > ke.maxSize {
							ke.maxSize = e.Size
						}
						if e.MinTime < runMin {
							runMin = e.MinTime
						}
						if e.MaxTime > runMax {
							runMax = e.MaxTime
						}
					}
					kes = append(kes, ke)
					all = append(all, ents...)
				}
				r.Close()
			}
			if len(kes) == 0 && len(walStreams) == 0 {
				continue
			}
			if len(walStreams) > 0 {
				// WAL values can extend the run's span beyond the TSM index.
				for _, ws := range walStreams {
					for _, v := range ws.vals {
						if v.UnixNano < runMin {
							runMin = v.UnixNano
						}
						if v.UnixNano > runMax {
							runMax = v.UnixNano
						}
					}
				}
			}

			// Window count: split only sizable, well-formed runs. A run holding a
			// non-ascending key stays whole — its fallback path materializes the
			// key per task, which windows would multiply.
			k := 1
			if allAscending && runBytes >= splitRunThreshold {
				k = int(runBytes / splitRunThreshold)
				if cap := opt.Workers * 4; k > cap {
					k = cap
				}
			}
			var windows [][2]int64
			if k <= 1 {
				windows = [][2]int64{{runMin, runMax}}
			} else {
				windows = splitWindows(all, runMin, runMax, k)
			}

			for _, w := range windows {
				effStart, effEnd := w[0], w[1]
				if seriesStart > effStart {
					effStart = seriesStart
				}
				if end < effEnd {
					effEnd = end
				}
				if effStart > effEnd {
					continue // window entirely before the cursor or after --end
				}
				t := splitTask{
					sk: sk, measurement: measurement, tags: tags,
					start: effStart, end: effEnd,
					wal: walStreams,
					est: walBytes + 2*batchArenaFields*80,
				}
				for _, ke := range kes {
					if intersects(ke.ents, effStart, effEnd) {
						t.streams = append(t.streams, ke.ts)
						est := int64(ke.maxSize) * 8 // decoded expansion guess; covers string blocks
						if est < decodedBlockEstimate {
							est = decodedBlockEstimate
						}
						t.est += est
					}
				}
				if len(t.streams) == 0 && len(t.wal) == 0 {
					continue
				}
				tasks = append(tasks, t)
			}
		}
	}
	return tasks, nil
}

// taskHandle carries one task's output to the consumer. err is written before
// batches is closed; the consumer reads it only after draining, so the channel
// close orders the access.
type taskHandle struct {
	batches chan *pointBatch
	stats   Stats
	err     error
	est     int64
}

// shardParallel executes the split pipeline. fn is invoked ONLY from this
// (the caller's) goroutine, preserving the single-threaded callback contract.
func shardParallel(si *shardIndex, tsmFiles []string, openTSM OpenTSM, start, end int64, cur *Cursor, opt SplitOptions, st Stats, fn func(Point)) (Stats, error) {
	if opt.MemoryBudget <= 0 {
		return st, fmt.Errorf("shard split: SplitOptions.MemoryBudget must be > 0 when Workers > 1 (worker count alone cannot bound the decoded-block footprint)")
	}
	tasks, err := buildTasks(si, tsmFiles, openTSM, start, end, cur, opt, &st)
	if err != nil {
		return st, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	budget := newMemBudget(ctx, opt.MemoryBudget)
	pool := &batchPool{ch: make(chan *pointBatch, opt.Workers*4)}
	ordered := make(chan *taskHandle, opt.Workers)
	sem := make(chan struct{}, opt.Workers)
	var wg sync.WaitGroup

	// Scheduler: single goroutine, admits strictly in task order.
	go func() {
		defer close(ordered)
		for i := range tasks {
			t := &tasks[i]
			est := t.est
			if est > opt.MemoryBudget {
				est = opt.MemoryBudget // oversized → waits for an empty budget, runs alone
			}
			if !budget.acquire(est) {
				return // cancelled
			}
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				budget.release(est)
				return
			}
			h := &taskHandle{batches: make(chan *pointBatch, 2), est: est}
			select {
			case ordered <- h:
			case <-ctx.Done():
				budget.release(est)
				<-sem
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				runSplitTask(ctx, t, tsmFiles, openTSM, h, pool)
			}()
		}
	}()

	// Consumer: drain tasks in order, re-emit through fn. On error, cancel and
	// keep draining so workers and the scheduler unwind without deadlock.
	var firstErr error
	for h := range ordered {
		for b := range h.batches {
			if firstErr == nil {
				for i := range b.pts {
					fn(b.pts[i])
				}
			}
			pool.put(b)
		}
		if firstErr == nil {
			if h.err != nil {
				firstErr = h.err
				cancel()
			} else {
				st.Points += h.stats.Points
				st.Fields += h.stats.Fields
				if h.stats.hasTime {
					st.observe(h.stats.MinTime)
					st.observe(h.stats.MaxTime)
				}
			}
		}
		budget.release(h.est)
	}
	wg.Wait()
	return st, firstErr
}

// runSplitTask merges one window on a worker goroutine. Each task owns its own
// file descriptors (per-task fileSet); only the immutable parsed indexes and
// the pre-sorted WAL slices are shared.
func runSplitTask(ctx context.Context, t *splitTask, tsmFiles []string, openTSM OpenTSM, h *taskHandle, pool *batchPool) {
	defer close(h.batches)
	fs := newFileSet(tsmFiles, openTSM)
	defer fs.closeAll()

	streams := make([]stream, 0, len(t.streams)+len(t.wal))
	for _, ts := range t.streams {
		streams = append(streams, fs.newBlockStream(ts.field, ts.fi, ts.key, t.start, t.end))
	}
	for _, ws := range t.wal {
		streams = append(streams, newSliceStream(ws.field, ws.vals, t.start, t.end))
	}

	em := &batchEmitter{ctx: ctx, out: h.batches, pool: pool}
	err := mergeSeries(t.sk, t.measurement, t.tags, streams, &h.stats, em.emit)
	for _, s := range streams {
		s.release()
	}
	if err == nil {
		em.flush()
	}
	if err == nil && em.dropped {
		err = ctx.Err()
	}
	h.err = err
}
