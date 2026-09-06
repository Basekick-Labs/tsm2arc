package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/basekick-labs/tsm2arc/internal/checkpoint"
	"github.com/basekick-labs/tsm2arc/internal/measure"
	"github.com/basekick-labs/tsm2arc/internal/sink"
)

// A name whose data lies wholly outside [--start, --end] never reaches the
// resolver during the load (its blocks are pruned unread), so the census must
// not abort on it either — otherwise a windowed load that would succeed dies
// in pre-flight.
func TestCensusRespectsTimeWindow(t *testing.T) {
	resolver, _ := measure.NewResolver(nil, measure.PolicyFail)
	cfg, arc, srv, cpPath := newMeasurementTestEnv(t, resolver)
	// Fixture points sit at ts 1700000000..1700000002 s. Window excludes them.
	cfg.start = 1800000000 * 1e9
	cfg.end = 1900000000 * 1e9
	cp, _ := checkpoint.Open(cpPath)
	defer cp.Close()

	res, err := load(context.Background(), cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp)
	if err != nil {
		t.Fatalf("windowed load aborted although the invalid names are outside the window: %v", err)
	}
	if res.Rows != 0 || arc.requests != 0 {
		t.Errorf("expected an empty windowed load, got rows=%d requests=%d", res.Rows, arc.requests)
	}
}

// A resume must not re-abort on names that only exist in already-done shards.
func TestCensusSkipsDoneShards(t *testing.T) {
	resolver, _ := measure.NewResolver(nil, measure.PolicyFail)
	cfg, _, srv, cpPath := newMeasurementTestEnv(t, resolver)
	cp, _ := checkpoint.Open(cpPath)
	defer cp.Close()

	// Mark the only shard done, as if migrated under an earlier (valid) map.
	sh := cfg.shards[0]
	if err := cp.FinishShard(sh.SourceID, sh.ShardID, 0, nil); err != nil {
		t.Fatal(err)
	}
	res, err := load(context.Background(), cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp)
	if err != nil {
		t.Fatalf("census aborted on a done shard: %v", err)
	}
	if res.SkippedShards != 1 {
		t.Errorf("skippedShards = %d, want 1", res.SkippedShards)
	}
}

// Retries must not be silent: each failed attempt logs, and the recovery does
// too, so a severed connection is visible while it is happening.
func TestSendRetryIsLogged(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			http.Error(w, "rolling restart", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, `{"status":"ok","result":{"rows_imported":1}}`)
	}))
	defer srv.Close()

	var logs []string
	snk := sink.New(srv.URL, "tok", "ns", fastRetry())
	snk.SetLogger(func(f string, a ...any) { logs = append(logs, fmt.Sprintf(f, a...)) })

	if _, err := snk.Send(context.Background(), "db", []byte("m v=1 1\n")); err != nil {
		t.Fatalf("send should recover after one failure: %v", err)
	}
	joined := strings.Join(logs, "\n")
	if !strings.Contains(joined, "attempt 1/") || !strings.Contains(joined, "retrying in") {
		t.Errorf("missing per-attempt failure log:\n%s", joined)
	}
	if !strings.Contains(joined, "recovered on attempt 2/") {
		t.Errorf("missing recovery log:\n%s", joined)
	}
}
