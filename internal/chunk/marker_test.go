package chunk

import (
	"context"
	"testing"
)

// The flush callback must receive the marker of the LAST line inside the
// flushed chunk — that marker becomes the resume cursor, so an off-by-one here
// silently loses or duplicates a chunk's worth of data on resume.
func TestFlushCarriesLastLineMarker(t *testing.T) {
	// 10-byte lines, max 25 → chunks are lines [0,1], [2,3], [4].
	lines := []string{"0123456789", "0123456789", "0123456789", "0123456789", "0123456789"}
	_, _, lastIdx := collectMarked(25, lines)
	// Chunk 0 flushes while APPENDING line 2 — its marker must be line 1's, not
	// line 2's. Chunk 1 likewise line 3's. The final Flush carries line 4's.
	want := []int64{1, 3, 4}
	if len(lastIdx) != len(want) {
		t.Fatalf("flushes = %d, want %d", len(lastIdx), len(want))
	}
	for i := range want {
		if lastIdx[i] != want[i] {
			t.Errorf("flush %d marker = line %d, want line %d", i, lastIdx[i], want[i])
		}
	}
}

// An oversized line flushes alone and its flush must carry ITS OWN marker.
func TestOversizedLineCarriesOwnMarker(t *testing.T) {
	big := make([]byte, 100)
	for i := range big {
		big[i] = 'x'
	}
	_, _, lastIdx := collectMarked(50, []string{string(big), "small\n"})
	want := []int64{0, 1}
	for i := range want {
		if lastIdx[i] != want[i] {
			t.Errorf("flush %d marker = line %d, want line %d", i, lastIdx[i], want[i])
		}
	}
}

// NewAt continues a shard's chunk numbering on a seek-resume.
func TestNewAtContinuesSequence(t *testing.T) {
	var seqs []int
	a := NewAt(25, 7, func(ctx context.Context, seq int, lp []byte, m Marker) error {
		seqs = append(seqs, seq)
		return nil
	})
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := a.Append(ctx, []byte("0123456789"), Marker{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(seqs) != 3 || seqs[0] != 7 || seqs[2] != 9 {
		t.Errorf("seqs = %v, want [7 8 9]", seqs)
	}
	if a.Seq() != 10 {
		t.Errorf("Seq() = %d, want 10 (total including the pre-seek chunks)", a.Seq())
	}
}

// Detach hands the buffer to the callback (for a pipelined sender); the
// accumulator must continue on a different buffer, and Recycle must feed
// detached buffers back.
func TestDetachAndRecycle(t *testing.T) {
	var kept [][]byte
	var a *Accumulator
	a = New(25, func(ctx context.Context, seq int, lp []byte, m Marker) error {
		a.Detach()
		kept = append(kept, lp) // retained WITHOUT copying — Detach makes this legal
		return nil
	})
	ctx := context.Background()
	lines := []string{"aaaaaaaaaa", "bbbbbbbbbb", "cccccccccc", "dddddddddd", "eeeeeeeeee"}
	for i, l := range lines {
		if err := a.Append(ctx, []byte(l), Marker{}); err != nil {
			t.Fatal(err)
		}
		if i == 3 {
			// Simulate the sender finishing with the first buffer.
			a.Recycle(kept[0][:0])
		}
	}
	if err := a.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	want := []string{"aaaaaaaaaabbbbbbbbbb", "ccccccccccdddddddddd", "eeeeeeeeee"}
	if len(kept) != len(want) {
		t.Fatalf("chunks = %d, want %d", len(kept), len(want))
	}
	// kept[0] was recycled into the accumulator after chunk 1 flushed, so by now
	// it holds chunk 2's bytes — that's the expected aliasing, assert chunk 1
	// and 2 (untouched since detach) and the final chunk's content.
	if string(kept[1]) != want[1] {
		t.Errorf("chunk 1 = %q, want %q", kept[1], want[1])
	}
	if string(kept[2]) != want[2] {
		t.Errorf("chunk 2 = %q, want %q", kept[2], want[2])
	}
}
