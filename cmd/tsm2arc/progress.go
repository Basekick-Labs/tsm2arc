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
	stallWarn   time.Duration // 0 disables the stall warning

	shardsDone atomic.Int64
	chunks     atomic.Int64
	rows       atomic.Int64
	bytes      atomic.Int64
	skipped    atomic.Int64

	mu       sync.Mutex // serializes stdout writes (log lines + heartbeat)
	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	// actMu guards per-shard activity state and the last send error. Activity =
	// a committed chunk OR a legacy-resume skip; ages are measured from
	// max(shard start, last activity) so a freshly started shard is never
	// instantly "stalled" during its index pass.
	actMu    sync.Mutex
	activity map[string]time.Time // active shards → last activity (seeded at start)
	warned   map[string]bool      // stall warning armed-once per quiet period
	sendErr  string               // last failed-send log line (cleared on activity)
	sendErrT time.Time
}

func newProgress(totalShards int64, verbose bool, stallWarn time.Duration) *progress {
	p := &progress{
		totalShards: totalShards,
		verbose:     verbose,
		stallWarn:   stallWarn,
		start:       time.Now(),
		stop:        make(chan struct{}),
		activity:    map[string]time.Time{},
		warned:      map[string]bool{},
	}
	p.wg.Add(1)
	go p.heartbeatLoop()
	return p
}

// shardStarted seeds the activity clock for a shard (see actMu comment).
func (p *progress) shardStarted(key string) {
	p.actMu.Lock()
	p.activity[key] = time.Now()
	p.warned[key] = false
	p.actMu.Unlock()
}

// noteActivity records forward progress on a shard and clears any pending
// stall state and send-error banner.
func (p *progress) noteActivity(key string) {
	p.actMu.Lock()
	p.activity[key] = time.Now()
	p.warned[key] = false
	p.sendErr = ""
	p.actMu.Unlock()
}

// shardFinished removes a shard from stall tracking.
func (p *progress) shardFinished(key string) {
	p.actMu.Lock()
	delete(p.activity, key)
	delete(p.warned, key)
	p.actMu.Unlock()
}

// noteSendError records the latest failed-send message for the heartbeat, so
// the difference between "slow merge" and "dying sends" is visible without
// scrolling for log lines.
func (p *progress) noteSendError(msg string) {
	p.actMu.Lock()
	p.sendErr = msg
	p.sendErrT = time.Now()
	p.actMu.Unlock()
}

// stallState returns the oldest active shard's idle age (ok=false when no
// shard is active), any shards newly crossing the stall threshold (marked
// warned), and the current send-error banner.
func (p *progress) stallState() (oldest time.Duration, ok bool, newlyStalled []string, sendErr string, sendErrAge time.Duration) {
	p.actMu.Lock()
	defer p.actMu.Unlock()
	now := time.Now()
	for key, last := range p.activity {
		idle := now.Sub(last)
		if idle > oldest {
			oldest = idle
		}
		ok = true
		if p.stallWarn > 0 && idle >= p.stallWarn && !p.warned[key] {
			p.warned[key] = true
			newlyStalled = append(newlyStalled, key)
		}
	}
	if p.sendErr != "" {
		sendErr, sendErrAge = p.sendErr, now.Sub(p.sendErrT)
	}
	return
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
	p.notef(format, args...)
}

// notef prints a line under the stdout lock regardless of verbosity — for
// operator-actionable notices (e.g. a budget limiting the index cache).
func (p *progress) notef(format string, args ...any) {
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
	// A resume that is skipping already-sent chunks must say so: without the
	// skipped counter the heartbeat reads "0 chunks, 0 rows" for the whole
	// catch-up phase and is indistinguishable from a hang.
	skipped := ""
	if n := p.skipped.Load(); n > 0 {
		skipped = fmt.Sprintf(" (+%d skipped on resume)", n)
	}
	oldest, active, newlyStalled, sendErr, sendErrAge := p.stallState()
	idle := ""
	if active {
		idle = fmt.Sprintf(", oldest shard idle %s", oldest.Round(time.Second))
	}
	p.mu.Lock()
	fmt.Printf("%s[%d/%d shards] %d chunks%s, %d rows, %.1f MB raw — %.0f rows/s, %.1f MB/s (%.0fs)%s\n",
		prefix,
		p.shardsDone.Load(), p.totalShards,
		p.chunks.Load(), skipped, rows, mb,
		float64(rows)/elapsed, mb/elapsed, elapsed, idle)
	if sendErr != "" {
		fmt.Printf("  last send failure %s ago: %s\n", sendErrAge.Round(time.Second), sendErr)
	}
	p.mu.Unlock()
	for _, key := range newlyStalled {
		p.notef("WARN: shard %s has made no progress for over %s — sends may be stalled or retrying (see --send-timeout, --stall-warn)",
			key, p.stallWarn)
	}
}

// finish stops the heartbeat and prints a final status line.
func (p *progress) finish() {
	p.stopOnce.Do(func() { close(p.stop) })
	p.wg.Wait()
	p.printStatus("final: ")
}
