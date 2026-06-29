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

// FlushFunc receives one full chunk of raw LP bytes and its zero-based sequence
// number within the current Accumulator. Returning an error aborts accumulation.
// The buffer passed to FlushFunc is owned by the Accumulator and reused after
// the call returns — the FlushFunc must not retain it.
type FlushFunc func(ctx context.Context, seq int, lp []byte) error

// Accumulator buffers LP lines and flushes at the byte bound.
type Accumulator struct {
	maxBytes int
	flush    FlushFunc

	buf []byte
	seq int
}

// New creates an Accumulator. maxBytes <= 0 uses DefaultMaxBytes.
func New(maxBytes int, flush FlushFunc) *Accumulator {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	return &Accumulator{
		maxBytes: maxBytes,
		flush:    flush,
		buf:      make([]byte, 0, 1<<20),
	}
}

// Append adds one already-encoded LP line (which MUST include its trailing
// newline). If adding it would exceed maxBytes, the current buffer is flushed
// first, then the line starts a new chunk. A single line larger than maxBytes is
// flushed on its own (LP points are tiny, so this is a safety valve, not a
// normal path).
func (a *Accumulator) Append(ctx context.Context, line []byte) error {
	if len(a.buf) > 0 && len(a.buf)+len(line) > a.maxBytes {
		if err := a.doFlush(ctx); err != nil {
			return err
		}
	}
	a.buf = append(a.buf, line...)
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

func (a *Accumulator) doFlush(ctx context.Context) error {
	if err := a.flush(ctx, a.seq, a.buf); err != nil {
		return err
	}
	a.seq++
	a.buf = a.buf[:0]
	return nil
}

// Seq returns the number of chunks flushed so far.
func (a *Accumulator) Seq() int { return a.seq }
