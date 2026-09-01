// Package chunk accumulates line-protocol bytes and flushes them in bounded
// chunks. The bound is measured against the RAW (uncompressed) LP, because
// Arc's import endpoint enforces its 500 MB limit on the decompressed payload.
//
// Flush boundaries are deterministic: given the same sequence of Append calls
// with the same MaxBytes, the same cut points are produced every run. This is a
// prerequisite for crash-safe resume (Phase 3) — a re-derived chunk must be
// byte-identical so the skip math is exact and any re-pushed overlap is
// collapsible by Arc compaction.
package chunk

import "context"

// DefaultMaxBytes is the per-chunk raw-LP limit: 450 MB, leaving headroom under
// Arc's 500 MB decompressed cap.
const DefaultMaxBytes = 450 * 1024 * 1024

// Marker annotates one appended LP line with its position in the shard's
// deterministic emission order, plus an opaque audit snapshot. The flush
// callback receives the marker of the LAST line in the flushed chunk — the
// exact resume cursor for "everything up to and including this chunk".
//
// Tally is opaque to this package: the caller attaches its running
// measurement-action snapshot as of this line (copy-on-write, so sharing a map
// across markers is safe) and reads it back at flush time.
type Marker struct {
	SeriesKey string
	UnixNano  int64
	Tally     map[string]int64
}

// FlushFunc receives one full chunk of raw LP bytes, its sequence number within
// the current Accumulator, and the marker of the chunk's last line. Returning
// an error aborts accumulation.
//
// The buffer passed to FlushFunc is owned by the Accumulator and reused after
// the call returns — a FlushFunc that wants to keep it (e.g. to hand it to a
// sender goroutine) must call Detach() during the callback, and should return
// the buffer later via Recycle to bound allocation.
type FlushFunc func(ctx context.Context, seq int, lp []byte, last Marker) error

// Accumulator buffers LP lines and flushes at the byte bound.
type Accumulator struct {
	maxBytes int
	flush    FlushFunc

	buf        []byte
	seq        int
	lastMarker Marker

	inFlush  bool
	detached bool
	spares   [][]byte // recycled buffers for detach replacement (small, bounded)
}

// New creates an Accumulator starting at sequence 0. maxBytes <= 0 uses
// DefaultMaxBytes.
func New(maxBytes int, flush FlushFunc) *Accumulator {
	return NewAt(maxBytes, 0, flush)
}

// NewAt creates an Accumulator whose first flushed chunk has sequence startSeq.
// A seek-resume continues a shard's chunk numbering at committed+1 without
// re-deriving the earlier chunks, so the sequence must start there.
func NewAt(maxBytes, startSeq int, flush FlushFunc) *Accumulator {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	return &Accumulator{
		maxBytes: maxBytes,
		flush:    flush,
		buf:      make([]byte, 0, 1<<20),
		seq:      startSeq,
	}
}

// Append adds one already-encoded LP line (which MUST include its trailing
// newline) annotated with its marker. If adding it would exceed maxBytes, the
// current buffer is flushed first — that flush carries the marker of the
// previously appended line, which is the last line of the flushed chunk — then
// the line starts a new chunk. A single line larger than maxBytes is flushed on
// its own (LP points are tiny, so this is a safety valve, not a normal path);
// that flush carries this line's own marker.
func (a *Accumulator) Append(ctx context.Context, line []byte, m Marker) error {
	if len(a.buf) > 0 && len(a.buf)+len(line) > a.maxBytes {
		if err := a.doFlush(ctx); err != nil {
			return err
		}
	}
	a.buf = append(a.buf, line...)
	a.lastMarker = m
	// Oversized single line: flush immediately so we never exceed the cap.
	if len(a.buf) >= a.maxBytes {
		if err := a.doFlush(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Flush emits any buffered bytes as a final chunk. Safe to call when empty.
func (a *Accumulator) Flush(ctx context.Context) error {
	if len(a.buf) == 0 {
		return nil
	}
	return a.doFlush(ctx)
}

// Detach transfers ownership of the buffer currently being flushed to the
// caller. Only valid from inside the flush callback; the accumulator then
// continues on a spare (or fresh) buffer instead of reusing the detached one.
func (a *Accumulator) Detach() {
	if a.inFlush {
		a.detached = true
	}
}

// Recycle returns a previously detached buffer for reuse. Safe to skip — a
// missing recycle only costs an allocation. NOT safe for concurrent use: with a
// sender goroutine, route recycling through the goroutine that calls Append
// (e.g. collect completed buffers over a channel).
func (a *Accumulator) Recycle(buf []byte) {
	if len(a.spares) < 2 {
		a.spares = append(a.spares, buf[:0])
	}
}

func (a *Accumulator) doFlush(ctx context.Context) error {
	a.inFlush = true
	err := a.flush(ctx, a.seq, a.buf, a.lastMarker)
	a.inFlush = false
	if err != nil {
		return err
	}
	a.seq++
	if a.detached {
		a.detached = false
		if n := len(a.spares); n > 0 {
			a.buf = a.spares[n-1]
			a.spares = a.spares[:n-1]
		} else {
			a.buf = make([]byte, 0, 1<<20)
		}
	} else {
		a.buf = a.buf[:0]
	}
	return nil
}

// Seq returns the sequence number the NEXT flushed chunk will carry — after the
// final Flush, the total chunk count of the shard (including any chunks skipped
// by starting at NewAt's startSeq).
func (a *Accumulator) Seq() int { return a.seq }
