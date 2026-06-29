package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// progress is a thread-safe reporter shared by all concurrent shard workers.
// It tracks shard/chunk/row/byte counters and prints a periodic one-line
// heartbeat with throughput, plus serialized verbose log lines (so concurrent
// workers don't interleave mid-line on stdout).
//
// time.Now is used only for elapsed/throughput display — it never affects
// correctness or control flow, so it's fine here (unlike workflow scripts).
type progress struct {
	totalShards int64
	verbose     bool
	start       time.Time

	shardsDone atomic.Int64
	chunks     atomic.Int64
	rows       atomic.Int64
	bytes      atomic.Int64
	skipped    atomic.Int64

	mu       sync.Mutex // serializes stdout writes (log lines + heartbeat)
	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func newProgress(totalShards int64, verbose bool) *progress {
	p := &progress{
		totalShards: totalShards,
		verbose:     verbose,
		start:       time.Now(),
		stop:        make(chan struct{}),
	}
	p.wg.Add(1)
	go p.heartbeatLoop()
	return p
}

func (p *progress) addChunk(nbytes, rows int64) {
	p.chunks.Add(1)
	p.bytes.Add(nbytes)
	p.rows.Add(rows)
}
func (p *progress) addSkipped(n int64) { p.skipped.Add(n) }
func (p *progress) shardDone()         { p.shardsDone.Add(1) }

// logf prints a verbose line under the stdout lock (no-op unless verbose).
func (p *progress) logf(format string, args ...any) {
	if !p.verbose {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Printf("  "+format+"\n", args...)
}

// heartbeatLoop prints a periodic status line until finish() is called. In
// verbose mode the per-chunk lines already give detail, so the heartbeat is
// quieter (every 30s); otherwise every 5s for a live sense of progress.
func (p *progress) heartbeatLoop() {
	defer p.wg.Done()
	interval := 5 * time.Second
	if p.verbose {
		interval = 30 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
			p.printStatus("")
		}
	}
}

func (p *progress) printStatus(prefix string) {
	elapsed := time.Since(p.start).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	rows := p.rows.Load()
	mb := float64(p.bytes.Load()) / (1024 * 1024)
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Printf("%s[%d/%d shards] %d chunks, %d rows, %.1f MB raw — %.0f rows/s, %.1f MB/s (%.0fs)\n",
		prefix,
		p.shardsDone.Load(), p.totalShards,
		p.chunks.Load(), rows, mb,
		float64(rows)/elapsed, mb/elapsed, elapsed)
}

// finish stops the heartbeat and prints a final status line.
func (p *progress) finish() {
	p.stopOnce.Do(func() { close(p.stop) })
	p.wg.Wait()
	p.printStatus("final: ")
}
