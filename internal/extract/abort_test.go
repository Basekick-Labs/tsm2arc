package extract

import (
	"context"
	"errors"
	"math"
	"testing"
)

// A load that failed must stop extraction: cancelling SplitOptions.Ctx ends
// the shard with an error at once, on the serial and the split path, instead
// of decoding the rest of it.

func extractCancelAfter(t *testing.T, workers, after int) (emitted int, err error) {
	t.Helper()
	tsmFiles, walFiles := splitFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := SplitOptions{Workers: workers, MemoryBudget: 1 << 30, Ctx: ctx}
	_, err = ShardResumeSplit(tsmFiles, walFiles, openTSMFile, walReadFileForTest,
		math.MinInt64, math.MaxInt64, nil, opt, func(Point) {
			emitted++
			if emitted == after {
				cancel()
			}
		})
	return emitted, err
}

func TestCancelStopsSerialExtraction(t *testing.T) {
	tf, wf := splitFixture(t)
	full, _ := extractAll(t, tf, wf, nil, SplitOptions{Workers: 1})
	emitted, err := extractCancelAfter(t, 1, 10)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	// Stops within one check interval of the cancel, not at the end.
	if emitted > 10+cancelCheckEvery || emitted >= len(full) {
		t.Fatalf("emitted %d points after cancelling at 10 (shard has %d)", emitted, len(full))
	}
}

func TestCancelStopsSplitExtraction(t *testing.T) {
	forceWindowing(t)
	tf, wf := splitFixture(t)
	full, _ := extractAll(t, tf, wf, nil, SplitOptions{Workers: 1})
	emitted, err := extractCancelAfter(t, 4, 10)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if emitted >= len(full) {
		t.Fatalf("emitted the whole shard (%d points) after cancelling at 10", emitted)
	}
}

// A cancel that lands where no task reports it (before the first task, or
// between tasks) must still fail the shard: a nil error there let the caller
// mark a partly read shard done, and a resume then skipped the rest.
func TestCancelledExtractionNeverReturnsNil(t *testing.T) {
	forceWindowing(t)
	tsmFiles, walFiles := splitFixture(t)
	for _, workers := range []int{1, 4} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		n := 0
		_, err := ShardResumeSplit(tsmFiles, walFiles, openTSMFile, walReadFileForTest,
			math.MinInt64, math.MaxInt64, nil, SplitOptions{Workers: workers, MemoryBudget: 1 << 30, Ctx: ctx},
			func(Point) { n++ })
		if err == nil {
			t.Fatalf("workers=%d: a cancelled extraction returned nil after %d points", workers, n)
		}
		if n != 0 {
			t.Fatalf("workers=%d: emitted %d points with the context already cancelled", workers, n)
		}
	}
}

// Without a cancel, Ctx changes nothing: output is byte-identical.
func TestCtxDoesNotChangeOutput(t *testing.T) {
	forceWindowing(t)
	tsmFiles, walFiles := splitFixture(t)
	for _, workers := range []int{1, 4} {
		plain, _ := extractAll(t, tsmFiles, walFiles, nil, SplitOptions{Workers: workers, MemoryBudget: 1 << 30})
		withCtx, _ := extractAll(t, tsmFiles, walFiles, nil, SplitOptions{Workers: workers, MemoryBudget: 1 << 30, Ctx: context.Background()})
		if len(plain) != len(withCtx) {
			t.Fatalf("workers=%d: %d vs %d lines", workers, len(plain), len(withCtx))
		}
		for i := range plain {
			if plain[i] != withCtx[i] {
				t.Fatalf("workers=%d: line %d differs", workers, i)
			}
		}
	}
}

// The split scheduler stops admitting tasks when the caller's context ends and
// closes its queue as if the shard were done; tasks never started carry no
// error. shardParallel must still fail, or the caller marks a partly read
// shard done (and a resume skips the rest).
func TestSplitCancelledBeforeAnyTaskFails(t *testing.T) {
	forceWindowing(t)
	tsmFiles, walFiles := splitFixture(t)
	var st Stats
	si, err := indexShard(context.Background(), tsmFiles, walFiles, openTSMFile, walReadFileForTest, &st)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n := 0
	_, err = shardParallel(si, tsmFiles, openTSMFile, math.MinInt64, math.MaxInt64, nil,
		SplitOptions{Workers: 4, MemoryBudget: 1 << 30, Ctx: ctx}, st, func(Point) { n++ })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v after %d points, want context.Canceled", err, n)
	}
}
