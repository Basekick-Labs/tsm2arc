package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/basekick-labs/tsm2arc/internal/checkpoint"
	"github.com/basekick-labs/tsm2arc/internal/discover"
	"github.com/basekick-labs/tsm2arc/internal/measure"
	"github.com/basekick-labs/tsm2arc/internal/sink"
	itsm "github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// writeKeysShard writes one 1.x shard holding exactly the given TSM keys
// ("measurement[,tags]#!~#field").
func writeKeysShard(t *testing.T, db string, vals map[string]itsm.Values) string {
	t.Helper()
	root := t.TempDir()
	writeKeysShardInto(t, root, db, vals)
	return root
}

// writeKeysShardInto adds database db (shard 1) under root.
func writeKeysShardInto(t *testing.T, root, db string, vals map[string]itsm.Values) {
	t.Helper()
	dir := filepath.Join(root, db, "autogen", "1")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(dir, "000000001-000000001.tsm"))
	if err != nil {
		t.Fatal(err)
	}
	w, err := itsm.NewTSMWriter(f)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(vals))
	for k := range vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if err := w.Write([]byte(k), vals[k]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.WriteIndex(); err != nil {
		t.Fatal(err)
	}
	w.Close()
	f.Close()
}

// manySeries is n tagged series of one point each.
func manySeries(n int) map[string]itsm.Values {
	vals := map[string]itsm.Values{}
	for i := 0; i < n; i++ {
		vals[fmt.Sprintf("cpu,host=node%05d#!~#usage", i)] = itsm.Values{itsm.NewFloatValue(int64(1700000000+i)*1e9, float64(i))}
	}
	return vals
}

// recordingArc answers every import with status (200 accepts and records),
// and calls onRequest first when set.
type recordingArc struct {
	mu        sync.Mutex
	status    int
	requests  int
	lines     []string
	onRequest func()
}

func (a *recordingArc) handler(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	a.requests++
	on := a.onRequest
	status := a.status
	a.mu.Unlock()
	if on != nil {
		on()
	}
	if status != http.StatusOK {
		http.Error(w, `{"error":"write refused: too many empty cells"}`, status)
		return
	}
	f, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "nofile", 400)
		return
	}
	defer f.Close()
	data, _ := io.ReadAll(f)
	gz, _ := gzip.NewReader(bytes.NewReader(data))
	lp, _ := io.ReadAll(gz)
	n := 0
	a.mu.Lock()
	for _, line := range strings.Split(strings.TrimRight(string(lp), "\n"), "\n") {
		if line != "" {
			a.lines = append(a.lines, line)
			n++
		}
	}
	a.mu.Unlock()
	fmt.Fprintf(w, `{"status":"ok","result":{"rows_imported":%d}}`, n)
}

func testShards(t *testing.T, datadir string) []discover.Shard {
	t.Helper()
	shards, err := discover.Walk(datadir, "", nil, false)
	if err != nil || len(shards) == 0 {
		t.Fatalf("discover: %v (%d shards)", err, len(shards))
	}
	return shards
}

// ---- stop on failure ----

// A chunk Arc refuses for good stops the shard's extraction at once, and the
// error the load returns is that refusal, not "context canceled". 0.2.0 kept
// decoding the rest of the shard (and every other shard) for hours.
func TestPermanentRefusalStopsExtraction(t *testing.T) {
	const n = 5000
	for _, mode := range []struct {
		pipeline bool
		inflight int
	}{{true, 1}, {true, 2}, {false, 1}} {
		pipeline, inflight := mode.pipeline, mode.inflight
		arc := &recordingArc{status: http.StatusRequestEntityTooLarge}
		srv := httptest.NewServer(http.HandlerFunc(arc.handler))
		shards := testShards(t, writeKeysShard(t, "metrics", manySeries(n)))
		cp, _ := checkpoint.Open(filepath.Join(t.TempDir(), "cp.db"))
		cfg := runConfig{shards: shards, start: math.MinInt64, end: math.MaxInt64, chunkSize: 256, pipeline: pipeline, inflight: inflight}
		prog := newProgress(1, false, 0)
		r, err := loadShard(context.Background(), cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp, shards[0], "metrics", prog)
		srv.Close()
		if err == nil || !strings.Contains(err.Error(), "413") {
			t.Fatalf("pipeline=%v inflight=%d: err = %v, want the 413", pipeline, inflight, err)
		}
		if r.extracted >= n/2 {
			t.Fatalf("pipeline=%v inflight=%d: extraction ran on to %d of %d points after the refusal", pipeline, inflight, r.extracted, n)
		}
		if done, _ := cp.IsShardDone("metrics", "1"); done {
			t.Fatalf("pipeline=%v inflight=%d: shard marked done after a refused chunk", pipeline, inflight)
		}
		cp.Close()
	}
}

// A shard queued behind a failed one does not start.
func TestQueuedShardDoesNotStartAfterFailure(t *testing.T) {
	arc := &recordingArc{status: http.StatusOK}
	srv := httptest.NewServer(http.HandlerFunc(arc.handler))
	defer srv.Close()
	shards := testShards(t, writeKeysShard(t, "metrics", manySeries(50)))
	cp, _ := checkpoint.Open(filepath.Join(t.TempDir(), "cp.db"))
	defer cp.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := runConfig{shards: shards, start: math.MinInt64, end: math.MaxInt64, chunkSize: 256, pipeline: true}
	r, err := loadShard(ctx, cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp, shards[0], "metrics", newProgress(1, false, 0))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if r.extracted != 0 || arc.requests != 0 {
		t.Fatalf("queued shard ran: %d points extracted, %d requests", r.extracted, arc.requests)
	}
}

// Another shard's failure mid-run (the run's context cancelled while this
// shard extracts) must never leave this shard marked done: a resume would
// skip the rest of it.
func TestShardCancelledMidRunIsNotMarkedDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	arc := &recordingArc{status: http.StatusOK}
	arc.onRequest = cancel // a sibling fails as this shard's first chunk lands
	srv := httptest.NewServer(http.HandlerFunc(arc.handler))
	defer srv.Close()
	shards := testShards(t, writeKeysShard(t, "metrics", manySeries(2000)))
	cp, _ := checkpoint.Open(filepath.Join(t.TempDir(), "cp.db"))
	defer cp.Close()
	for _, split := range []int{1, 4} {
		cfg := runConfig{shards: shards, start: math.MinInt64, end: math.MaxInt64, chunkSize: 256, pipeline: true}
		cfg.split.Workers, cfg.split.MemoryBudget = split, 1<<30
		_, err := loadShard(ctx, cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp, shards[0], "metrics", newProgress(1, false, 0))
		if err == nil {
			t.Fatalf("split=%d: a cancelled shard finished without an error", split)
		}
		if done, _ := cp.IsShardDone("metrics", "1"); done {
			t.Fatalf("split=%d: a cancelled shard was marked done", split)
		}
	}
}

// ---- narrow layout ----

func TestParseNarrow(t *testing.T) {
	n, err := parseNarrow([]string{"a, b", "b,c"}, "field", false)
	if err != nil || len(n.measurements) != 3 || !n.has("a") || !n.has("c") {
		t.Fatalf("parse = %+v, %v", n, err)
	}
	if _, err := parseNarrow(nil, "x", true); err == nil {
		t.Fatal("--narrow-tag without --narrow accepted")
	}
	for _, tag := range []string{"time", "TIME", "value", "Value_Int", "1x", "a-b", ""} {
		if _, err := parseNarrow([]string{"a"}, tag, true); err == nil {
			t.Errorf("--narrow-tag %q accepted", tag)
		}
	}
	if off, err := parseNarrow(nil, defaultNarrowTag, false); err != nil || off.enabled() || off.fingerprint() != "" {
		t.Fatalf("narrow off: %+v %v %q", off, err, off.fingerprint())
	}
}

func TestNarrowFingerprint(t *testing.T) {
	a, _ := parseNarrow([]string{"b,a"}, "field", false)
	b, _ := parseNarrow([]string{"a", "b"}, "field", false)
	c, _ := parseNarrow([]string{"a", "b"}, "f2", true)
	d, _ := parseNarrow([]string{"a,b"}, "field", false)
	e, _ := parseNarrow([]string{"a"}, "field", false)
	if a.fingerprint() != b.fingerprint() || a.fingerprint() != d.fingerprint() {
		t.Fatalf("order or form changed the fingerprint: %q %q", a.fingerprint(), b.fingerprint())
	}
	if a.fingerprint() == c.fingerprint() || a.fingerprint() == e.fingerprint() {
		t.Fatal("a different tag or set gave the same fingerprint")
	}
	// The fingerprint must change, so a 0.2.0 checkpoint refuses a resume
	// that adds --narrow.
	base := runConfig{chunkSize: 1}
	withNarrow := base
	withNarrow.narrow = a
	if configFingerprint(base, "ns") == configFingerprint(withNarrow, "ns") {
		t.Fatal("--narrow does not change the resume fingerprint")
	}
}

// A narrow measurement is sent one line per field, the name as a tag and the
// value in the column of its type; other measurements stay wide.
func TestNarrowLoad(t *testing.T) {
	ts := int64(1700000000) * 1e9
	vals := map[string]itsm.Values{
		"wide#!~#a":                    {itsm.NewFloatValue(ts, 1.5)},
		"wide#!~#b":                    {itsm.NewIntegerValue(ts, 7)},
		"wide#!~#c x":                  {itsm.NewStringValue(ts, `he said "hi"`)},
		"wide#!~#d":                    {itsm.NewBooleanValue(ts, true)},
		"other,host=h1#!~#usage":       {itsm.NewFloatValue(ts, 2)},
		"other,host=h1#!~#usage_count": {itsm.NewIntegerValue(ts, 3)},
	}
	arc := &recordingArc{status: http.StatusOK}
	srv := httptest.NewServer(http.HandlerFunc(arc.handler))
	defer srv.Close()
	narrow, _ := parseNarrow([]string{"wide"}, "field", false)
	cfg := runConfig{shards: testShards(t, writeKeysShard(t, "metrics", vals)), start: math.MinInt64, end: math.MaxInt64,
		chunkSize: 1 << 20, pipeline: true, narrow: narrow}
	cp, _ := checkpoint.Open(filepath.Join(t.TempDir(), "cp.db"))
	defer cp.Close()
	if _, err := load(context.Background(), cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp); err != nil {
		t.Fatalf("load: %v", err)
	}
	want := []string{
		fmt.Sprintf(`other,host=h1 usage=2,usage_count=3i %d`, ts),
		fmt.Sprintf(`wide,field=a value=1.5 %d`, ts),
		fmt.Sprintf(`wide,field=b value_int=7i %d`, ts),
		fmt.Sprintf(`wide,field=c\ x value_str="he said \"hi\"" %d`, ts),
		fmt.Sprintf(`wide,field=d value_bool=true %d`, ts),
	}
	got := append([]string(nil), arc.lines...)
	sort.Strings(got)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("sent:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// The census refuses, before any request, a --narrow name no source
// measurement has, a tag the narrow layout cannot write next to, and one Arc
// table fed by a narrow and a wide source.
func TestNarrowCensus(t *testing.T) {
	ts := int64(1700000000) * 1e9
	cases := []struct {
		name   string
		vals   map[string]itsm.Values
		narrow []string
		mapArg []string
		want   string
	}{
		{"narrow tag clash", map[string]itsm.Values{"wide,Field=x#!~#a": {itsm.NewFloatValue(ts, 1)}}, []string{"wide"}, nil, "clashes"},
		{"value column clash", map[string]itsm.Values{"wide,value=x#!~#a": {itsm.NewFloatValue(ts, 1)}}, []string{"wide"}, nil, "clashes"},
		{"mixed layouts in one table", map[string]itsm.Values{
			"wide#!~#a":  {itsm.NewFloatValue(ts, 1)},
			"wide2#!~#a": {itsm.NewFloatValue(ts, 1)},
		}, []string{"wide"}, []string{"wide2=wide"}, "both layouts"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			arc := &recordingArc{status: http.StatusOK}
			srv := httptest.NewServer(http.HandlerFunc(arc.handler))
			defer srv.Close()
			mmap := map[string]string{}
			if err := measure.AddEntries(mmap, tc.mapArg, "--measurement-map"); err != nil {
				t.Fatal(err)
			}
			resolver, err := measure.NewResolver(mmap, measure.PolicyFail)
			if err != nil {
				t.Fatal(err)
			}
			narrow, _ := parseNarrow(tc.narrow, "field", false)
			cfg := runConfig{shards: testShards(t, writeKeysShard(t, "metrics", tc.vals)), start: math.MinInt64, end: math.MaxInt64,
				chunkSize: 1 << 20, pipeline: true, narrow: narrow, resolver: resolver}
			cp, _ := checkpoint.Open(filepath.Join(t.TempDir(), "cp.db"))
			defer cp.Close()
			_, err = load(context.Background(), cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want one mentioning %q", err, tc.want)
			}
			if arc.requests != 0 {
				t.Fatalf("%d requests sent before the census refused", arc.requests)
			}
		})
	}
}

// The sender reports its first error once, so the shard stops extracting at
// the refusal instead of at its next chunk (up to --chunk-bytes later, or
// never if the shard ends first).
func TestSenderReportsFirstErrorOnce(t *testing.T) {
	calls := 0
	snd := newSender(context.Background(), senderConfig{
		pipelined: true, inflight: 2, firstSeq: 0,
		post: func(context.Context, sendJob) (sink.Result, error) {
			return sink.Result{}, errors.New("arc 413 (permanent)")
		},
		commit: func(context.Context, sendJob, sink.Result) error { return nil },
		onErr:  func() { calls++ },
	})
	_ = snd.submit(context.Background(), sendJob{seq: 0})
	_ = snd.submit(context.Background(), sendJob{seq: 1})
	if err := snd.wait(); err == nil {
		t.Fatal("no error after a refused chunk")
	}
	if calls != 1 {
		t.Fatalf("onErr called %d times, want 1", calls)
	}
}

// The load-time backstop for a narrow tag clash (the census normally refuses
// it first): the shard fails with the clash and stops.
func TestNarrowClashAtLoadStopsShard(t *testing.T) {
	ts := int64(1700000000) * 1e9
	vals := manySeries(3000)
	vals["wide,field=x#!~#a"] = itsm.Values{itsm.NewFloatValue(ts, 1)}
	arc := &recordingArc{status: http.StatusOK}
	srv := httptest.NewServer(http.HandlerFunc(arc.handler))
	defer srv.Close()
	narrow, _ := parseNarrow([]string{"wide"}, "field", false)
	shards := testShards(t, writeKeysShard(t, "metrics", vals))
	cfg := runConfig{shards: shards, start: math.MinInt64, end: math.MaxInt64, chunkSize: 256, pipeline: true, narrow: narrow}
	cp, _ := checkpoint.Open(filepath.Join(t.TempDir(), "cp.db"))
	defer cp.Close()
	_, err := loadShard(context.Background(), cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp, shards[0], "metrics", newProgress(1, false, 0))
	if err == nil || !strings.Contains(err.Error(), "narrow layout") {
		t.Fatalf("err = %v, want the narrow clash", err)
	}
}

// A --narrow name no measurement has is a warning, not a refusal: batched runs
// share one list, and a misspelt name only leaves that measurement wide.
func TestNarrowUnknownNameWarns(t *testing.T) {
	ts := int64(1700000000) * 1e9
	arc := &recordingArc{status: http.StatusOK}
	srv := httptest.NewServer(http.HandlerFunc(arc.handler))
	defer srv.Close()
	narrow, _ := parseNarrow([]string{"wdie"}, "field", false)
	resolver, _ := measure.NewResolver(nil, measure.PolicyFail)
	cfg := runConfig{shards: testShards(t, writeKeysShard(t, "metrics", map[string]itsm.Values{"wide#!~#a": {itsm.NewFloatValue(ts, 1)}})),
		start: math.MinInt64, end: math.MaxInt64, chunkSize: 1 << 20, pipeline: true, narrow: narrow, resolver: resolver}
	cp, _ := checkpoint.Open(filepath.Join(t.TempDir(), "cp.db"))
	defer cp.Close()
	if _, err := load(context.Background(), cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp); err != nil {
		t.Fatalf("an unknown --narrow name refused the load: %v", err)
	}
	if len(arc.lines) != 1 || !strings.HasPrefix(arc.lines[0], "wide a=1 ") {
		t.Fatalf("sent %q, want the measurement wide", arc.lines)
	}
}

// One Arc measurement name in two Arc databases is two tables: one narrow and
// one wide is fine.
func TestNarrowLayoutCheckedPerDatabase(t *testing.T) {
	ts := int64(1700000000) * 1e9
	root := t.TempDir()
	writeKeysShardInto(t, root, "dbx", map[string]itsm.Values{"a#!~#f": {itsm.NewFloatValue(ts, 1)}})
	writeKeysShardInto(t, root, "dby", map[string]itsm.Values{"b#!~#f": {itsm.NewFloatValue(ts, 1)}})
	mmap := map[string]string{}
	if err := measure.AddEntries(mmap, []string{"a=t", "b=t"}, "--measurement-map"); err != nil {
		t.Fatal(err)
	}
	resolver, _ := measure.NewResolver(mmap, measure.PolicyFail)
	narrow, _ := parseNarrow([]string{"a"}, "field", false)
	cfg := runConfig{shards: testShards(t, root), start: math.MinInt64, end: math.MaxInt64, chunkSize: 1 << 20, narrow: narrow, resolver: resolver}
	_, jobs := shardJobs(cfg)
	if err := censusMeasurements(cfg, nil, jobs); err != nil {
		t.Fatalf("one name in two databases refused: %v", err)
	}
}

// The dry run runs the narrow pre-flight with no checkpoint (nil store).
func TestNarrowCensusWithoutCheckpoint(t *testing.T) {
	ts := int64(1700000000) * 1e9
	narrow, _ := parseNarrow([]string{"wide"}, "field", false)
	resolver, _ := measure.NewResolver(nil, measure.PolicyFail)
	cfg := runConfig{shards: testShards(t, writeKeysShard(t, "metrics", map[string]itsm.Values{"wide,field=x#!~#a": {itsm.NewFloatValue(ts, 1)}})),
		start: math.MinInt64, end: math.MaxInt64, chunkSize: 1 << 20, narrow: narrow, resolver: resolver}
	_, jobs := shardJobs(cfg)
	if err := censusMeasurements(cfg, nil, jobs); err == nil || !strings.Contains(err.Error(), "clashes") {
		t.Fatalf("err = %v, want the clash", err)
	}
}

// The sender stops the shard at the refusal itself, not at the shard's next
// chunk: with chunks of about a third of the shard, extraction must not run
// on into the second third.
func TestRefusalStopsBeforeTheNextChunk(t *testing.T) {
	const n = 6000
	arc := &recordingArc{status: http.StatusRequestEntityTooLarge}
	srv := httptest.NewServer(http.HandlerFunc(arc.handler))
	defer srv.Close()
	shards := testShards(t, writeKeysShard(t, "metrics", manySeries(n)))
	cp, _ := checkpoint.Open(filepath.Join(t.TempDir(), "cp.db"))
	defer cp.Close()
	// A line is ~47 bytes ("cpu,host=node00001 usage=1 1700000001000000000"):
	// a chunk of about n/3 lines.
	cfg := runConfig{shards: shards, start: math.MinInt64, end: math.MaxInt64, chunkSize: 47 * n / 3, pipeline: true, inflight: 1}
	r, err := loadShard(context.Background(), cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp, shards[0], "metrics", newProgress(1, false, 0))
	if err == nil || !strings.Contains(err.Error(), "413") {
		t.Fatalf("err = %v, want the 413", err)
	}
	// Stopped at the refusal: a little past the first chunk. Waiting for the
	// next chunk to notice would run on to about 2n/3.
	if r.extracted >= n/2 {
		t.Fatalf("extraction reached point %d of %d: it waited for the next chunk to see the refusal", r.extracted, n)
	}
}

// A run cancelled (another shard failed) as this shard reads its last point,
// with its only chunk still in the accumulator: nothing more goes out and the
// shard is not marked done, so a resume redoes it.
func TestCancelAtTheEndOfExtractionIsNotDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	arc := &recordingArc{status: http.StatusOK}
	srv := httptest.NewServer(http.HandlerFunc(arc.handler))
	defer srv.Close()
	const n = 200
	shards := testShards(t, writeKeysShard(t, "metrics", manySeries(n)))
	cp, _ := checkpoint.Open(filepath.Join(t.TempDir(), "cp.db"))
	defer cp.Close()
	old := onShardPoint
	t.Cleanup(func() { onShardPoint = old })
	onShardPoint = func(extracted int64) {
		if extracted == n {
			cancel()
		}
	}
	cfg := runConfig{shards: shards, start: math.MinInt64, end: math.MaxInt64, chunkSize: 1 << 20, pipeline: true}
	r, err := loadShard(ctx, cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp, shards[0], "metrics", newProgress(1, false, 0))
	if err == nil {
		t.Fatalf("finished without an error after %d points", r.extracted)
	}
	if done, _ := cp.IsShardDone("metrics", "1"); done {
		t.Fatal("a shard cancelled before its final chunk went out was marked done")
	}
	if arc.requests != 0 {
		t.Fatalf("%d chunks sent after the run was cancelled", arc.requests)
	}
}
