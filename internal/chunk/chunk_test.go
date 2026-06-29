package chunk

import (
	"context"
	"fmt"
	"testing"
)

func collect(maxBytes int, lines []string) (chunks [][]byte, seqs []int) {
	a := New(maxBytes, func(ctx context.Context, seq int, lp []byte) error {
		cp := make([]byte, len(lp)) // buffer is reused; copy for assertion
		copy(cp, lp)
		chunks = append(chunks, cp)
		seqs = append(seqs, seq)
		return nil
	})
	ctx := context.Background()
	for _, l := range lines {
		if err := a.Append(ctx, []byte(l)); err != nil {
			panic(err)
		}
	}
	if err := a.Flush(ctx); err != nil {
		panic(err)
	}
	return
}

func TestChunkBoundaries(t *testing.T) {
	// each line is 10 bytes; maxBytes=25 → 2 lines per chunk (20), the 3rd
	// would make 30 > 25 so it flushes first.
	lines := []string{"0123456789", "0123456789", "0123456789", "0123456789", "0123456789"}
	chunks, seqs := collect(25, lines)
	if len(chunks) != 3 {
		t.Fatalf("got %d chunks want 3", len(chunks))
	}
	wantSizes := []int{20, 20, 10}
	for i, c := range chunks {
		if len(c) != wantSizes[i] {
			t.Errorf("chunk %d size %d want %d", i, len(c), wantSizes[i])
		}
		if seqs[i] != i {
			t.Errorf("chunk %d seq %d want %d", i, seqs[i], i)
		}
	}
}

// Determinism: same input + same maxBytes ⇒ identical chunk boundaries.
func TestChunkDeterministic(t *testing.T) {
	var lines []string
	for i := 0; i < 1000; i++ {
		lines = append(lines, fmt.Sprintf("m,t=x v=%di %d\n", i, i))
	}
	c1, _ := collect(4096, lines)
	c2, _ := collect(4096, lines)
	if len(c1) != len(c2) {
		t.Fatalf("chunk count differs: %d vs %d", len(c1), len(c2))
	}
	for i := range c1 {
		if string(c1[i]) != string(c2[i]) {
			t.Fatalf("chunk %d differs between runs", i)
		}
	}
}

func TestChunkOversizedSingleLine(t *testing.T) {
	// a single line larger than maxBytes must still flush on its own
	big := make([]byte, 100)
	for i := range big {
		big[i] = 'x'
	}
	chunks, _ := collect(50, []string{string(big), "small\n"})
	if len(chunks) != 2 {
		t.Fatalf("got %d chunks want 2", len(chunks))
	}
	if len(chunks[0]) != 100 {
		t.Errorf("oversized chunk size %d want 100", len(chunks[0]))
	}
}

func TestChunkEmptyFlush(t *testing.T) {
	chunks, _ := collect(100, nil)
	if len(chunks) != 0 {
		t.Fatalf("empty input produced %d chunks", len(chunks))
	}
}
