package main

import (
	"bytes"
	"compress/gzip"
	"context"
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

// writeTSMShardMeasurements writes one shard with pointsPer float points for
// each given measurement (one tagged series per measurement) and returns the
// datadir root. Measurement names may contain characters Arc rejects (dots
// etc.) — that's the point.
func writeTSMShardMeasurements(t *testing.T, db string, measurements []string, pointsPer int) string {
	t.Helper()
	root := t.TempDir()
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
	keys := make([]string, 0, len(measurements))
	vals := map[string]itsm.Values{}
	for _, m := range measurements {
		k := m + ",host=a#!~#value"
		keys = append(keys, k)
		vv := make(itsm.Values, 0, pointsPer)
		for i := 0; i < pointsPer; i++ {
			vv = append(vv, itsm.NewFloatValue(int64(1700000000+i)*1e9, float64(i)+0.25))
		}
		vals[k] = vv
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
	return root
}

// validatingArc mocks Arc's import endpoint INCLUDING its measurement-name
// validation (^[a-zA-Z][a-zA-Z0-9_-]*$): any line whose measurement violates
// the rule 400s the whole request, exactly like Arc 26.06.x. It records
// accepted lines so tests can assert what landed under which name.
type validatingArc struct {
	mu       sync.Mutex
	requests int
	rejects  int
	lines    []string
}

func (a *validatingArc) handler(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests++
	f, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "nofile", 400)
		return
	}
	defer f.Close()
	data, _ := io.ReadAll(f)
	gz, _ := gzip.NewReader(bytes.NewReader(data))
	lp, _ := io.ReadAll(gz)
	var accepted []string
	for _, line := range strings.Split(string(lp), "\n") {
		if line == "" {
			continue
		}
		m := line
		if i := strings.IndexAny(m, ", "); i >= 0 {
			m = m[:i]
		}
		if !measure.Valid(m) {
			a.rejects++
			http.Error(w, fmt.Sprintf(`{"error":"invalid measurement name %q in LP data"}`, m), 400)
			return
		}
		accepted = append(accepted, line)
	}
	a.lines = append(a.lines, accepted...)
	fmt.Fprintf(w, `{"status":"ok","result":{"database":%q,"rows_imported":%d}}`,
		r.Header.Get("x-arc-database"), len(accepted))
}

func (a *validatingArc) measurementCounts() map[string]int {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := map[string]int{}
	for _, line := range a.lines {
		m := line
		if i := strings.IndexAny(m, ", "); i >= 0 {
			m = m[:i]
		}
		out[m]++
	}
	return out
}

// measurements mixing valid names with the customer-shaped dotted ones.
var testMeasurements = []string{
	"edge-prod.gateway_services", // invalid: dot
	"cpu",                        // valid
	"core_metrics",               // valid
	"qa.node-b",                  // invalid: dot
}

func newMeasurementTestEnv(t *testing.T, resolver *measure.Resolver) (runConfig, *validatingArc, *httptest.Server, string) {
	t.Helper()
	datadir := writeTSMShardMeasurements(t, "metrics", testMeasurements, 3)
	arc := &validatingArc{}
	srv := httptest.NewServer(http.HandlerFunc(arc.handler))
	t.Cleanup(srv.Close)
	shards, err := discover.Walk(datadir, "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	cfg := runConfig{shards: shards, start: math.MinInt64, end: math.MaxInt64,
		chunkSize: 1 << 20, resolver: resolver, pipeline: true}
	cpPath := filepath.Join(t.TempDir(), "cp.db")
	return cfg, arc, srv, cpPath
}

func TestLoadFailsFastOnInvalidMeasurement(t *testing.T) {
	resolver, err := measure.NewResolver(nil, measure.PolicyFail)
	if err != nil {
		t.Fatal(err)
	}
	cfg, arc, srv, cpPath := newMeasurementTestEnv(t, resolver)
	cp, _ := checkpoint.Open(cpPath)
	defer cp.Close()

	_, err = load(context.Background(), cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp)
	if err == nil {
		t.Fatal("load succeeded with invalid measurement names under fail policy")
	}
	for _, want := range []string{"edge-prod.gateway_services", "--measurement-map", "--on-invalid-measurement", "--dry-run"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q; got:\n%v", want, err)
		}
	}
	// The failure is client-side: nothing was sent, so no Arc 400 mid-load.
	if arc.requests != 0 {
		t.Errorf("Arc saw %d request(s); fail policy must abort before sending", arc.requests)
	}
}

func TestLoadSkipPolicy(t *testing.T) {
	resolver, _ := measure.NewResolver(nil, measure.PolicySkip)
	cfg, arc, srv, cpPath := newMeasurementTestEnv(t, resolver)
	cp, _ := checkpoint.Open(cpPath)

	res, err := load(context.Background(), cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp)
	if err != nil {
		t.Fatal(err)
	}
	if arc.rejects != 0 {
		t.Fatalf("Arc rejected %d request(s); skip policy must never send invalid names", arc.rejects)
	}
	if res.Rows != 6 { // cpu + core_metrics, 3 points each
		t.Errorf("rows imported = %d, want 6", res.Rows)
	}
	if res.SkippedPoints != 6 { // two dotted measurements, 3 points each
		t.Errorf("skipped points = %d, want 6", res.SkippedPoints)
	}
	counts := arc.measurementCounts()
	if counts["cpu"] != 3 || counts["core_metrics"] != 3 || len(counts) != 2 {
		t.Errorf("landed measurements = %v", counts)
	}

	// The skips are durably recorded in the checkpoint.
	rows, err := cp.MeasurementReport()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("measurement report rows = %d, want 2: %+v", len(rows), rows)
	}
	for _, r := range rows {
		if r.Action != "skipped" || r.Points != 3 || r.SourceDB != "metrics" {
			t.Errorf("bad skip record: %+v", r)
		}
	}
	cp.Close()

	// Resume: shard is done, nothing re-sent, records still there (overwritten
	// identically at most).
	cp2, _ := checkpoint.Open(cpPath)
	defer cp2.Close()
	res2, err := load(context.Background(), cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp2)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Chunks != 0 || res2.SkippedShards != 1 {
		t.Errorf("resume did work: %+v", res2)
	}
	rows2, _ := cp2.MeasurementReport()
	if len(rows2) != 2 || rows2[0].Points != 3 {
		t.Errorf("report changed across resume: %+v", rows2)
	}
}

func TestLoadAutoMapPolicy(t *testing.T) {
	resolver, _ := measure.NewResolver(nil, measure.PolicyMap)
	cfg, arc, srv, cpPath := newMeasurementTestEnv(t, resolver)
	cp, _ := checkpoint.Open(cpPath)
	defer cp.Close()

	res, err := load(context.Background(), cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp)
	if err != nil {
		t.Fatal(err)
	}
	if arc.rejects != 0 {
		t.Fatalf("Arc rejected %d request(s) under map policy", arc.rejects)
	}
	if res.Rows != 12 || res.SkippedPoints != 0 {
		t.Errorf("rows=%d skipped=%d, want 12/0", res.Rows, res.SkippedPoints)
	}
	counts := arc.measurementCounts()
	for _, m := range []string{"edge-prod_gateway_services", "qa_node-b", "cpu", "core_metrics"} {
		if counts[m] != 3 {
			t.Errorf("measurement %q landed %d points, want 3 (all: %v)", m, counts[m], counts)
		}
	}

	rows, _ := cp.MeasurementReport()
	if len(rows) != 2 {
		t.Fatalf("report rows = %d, want 2 auto-renames: %+v", len(rows), rows)
	}
	for _, r := range rows {
		if r.Action != "renamed" || r.Origin != "auto" || r.Points != 3 {
			t.Errorf("bad auto-rename record: %+v", r)
		}
		if r.RenamedTo != measure.Sanitize(r.Measurement) {
			t.Errorf("recorded rename %q → %q is not Sanitize()", r.Measurement, r.RenamedTo)
		}
	}
}

func TestLoadExplicitMap(t *testing.T) {
	resolver, err := measure.NewResolver(map[string]string{
		"edge-prod.gateway_services": "edgeprod_gateway_services",
		"qa.node-b":                  "qa_node_b",
	}, measure.PolicyFail) // explicit map covers every invalid name → fail policy never fires
	if err != nil {
		t.Fatal(err)
	}
	cfg, arc, srv, cpPath := newMeasurementTestEnv(t, resolver)
	cp, _ := checkpoint.Open(cpPath)
	defer cp.Close()

	res, err := load(context.Background(), cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp)
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows != 12 {
		t.Errorf("rows = %d, want 12", res.Rows)
	}
	counts := arc.measurementCounts()
	if counts["edgeprod_gateway_services"] != 3 || counts["qa_node_b"] != 3 {
		t.Errorf("explicit renames didn't land: %v", counts)
	}
	rows, _ := cp.MeasurementReport()
	if len(rows) != 2 {
		t.Fatalf("report rows = %d, want 2: %+v", len(rows), rows)
	}
	for _, r := range rows {
		if r.Action != "renamed" || r.Origin != "explicit" {
			t.Errorf("bad explicit record: %+v", r)
		}
	}
}

// TestSkipAuditExactAcrossSeekResume: the checkpoint's skip/rename counts must
// come out EXACT after a crash + seek resume — the resumed run only sees the
// tail, so the counts survive via per-chunk deltas, not end-of-shard rewrites.
func TestSkipAuditExactAcrossSeekResume(t *testing.T) {
	resolver, _ := measure.NewResolver(nil, measure.PolicySkip)
	datadir := writeTSMShardMeasurements(t, "metrics", testMeasurements, 3)
	arc := &crashingArc{crashAfter: 2} // accept 2 chunks, then fail
	srv := httptest.NewServer(http.HandlerFunc(arc.handler))
	defer srv.Close()
	shards, err := discover.Walk(datadir, "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	// ~50-byte lines, chunk=64 → one line per chunk → 6 chunks of valid lines.
	cfg := runConfig{shards: shards, start: math.MinInt64, end: math.MaxInt64,
		chunkSize: 64, resolver: resolver, pipeline: true}
	cpPath := filepath.Join(t.TempDir(), "cp.db")
	ctx := context.Background()

	cp1, _ := checkpoint.Open(cpPath)
	if _, err := load(ctx, cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp1); err == nil {
		t.Fatal("expected crash")
	}
	cp1.Close()

	arc.crashAfter = -1
	cp2, _ := checkpoint.Open(cpPath)
	res, err := load(ctx, cfg, sink.New(srv.URL, "tok", "ns", fastRetry()), cp2)
	if err != nil {
		t.Fatalf("seek resume failed: %v", err)
	}
	if res.SkippedChunks != 0 {
		t.Errorf("seek resume re-derived %d chunks, want 0", res.SkippedChunks)
	}

	// No loss, no duplication of the valid lines...
	if lines := arc.distinctLines(); len(lines) != 6 || arc.rows != 6 {
		t.Errorf("valid lines: distinct=%d total=%d, want 6/6", len(lines), arc.rows)
	}
	// ...and the audit counts are exact: two skipped measurements, 3 points each,
	// counted once despite the crash landing mid-shard.
	rows, err := cp2.MeasurementReport()
	if err != nil {
		t.Fatal(err)
	}
	cp2.Close()
	if len(rows) != 2 {
		t.Fatalf("report rows = %d, want 2: %+v", len(rows), rows)
	}
	for _, r := range rows {
		if r.Action != "skipped" || r.Points != 3 {
			t.Errorf("audit not exact across seek resume: %+v (want skipped/3)", r)
		}
	}
}

// TestDryRunReportsInvalidMeasurements: --dry-run must surface every name Arc
// would reject BEFORE anything is posted (the 0.1.2 dry-run never did, so the
// first 400 arrived hours into a load).
func TestDryRunReportsInvalidMeasurements(t *testing.T) {
	resolver, _ := measure.NewResolver(
		map[string]string{"qa.node-b": "qa_node_b"}, measure.PolicyFail)
	datadir := writeTSMShardMeasurements(t, "metrics", testMeasurements, 3)
	shards, err := discover.Walk(datadir, "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	cfg := runConfig{shards: shards, start: math.MinInt64, end: math.MaxInt64,
		chunkSize: 1 << 20, resolver: resolver}

	// capture stdout around runDryRun
	old := os.Stdout
	rp, wp, _ := os.Pipe()
	os.Stdout = wp
	runDryRun(context.Background(), cfg, 10)
	wp.Close()
	os.Stdout = old
	out, _ := io.ReadAll(rp)

	for _, want := range []string{
		`INVALID: "edge-prod.gateway_services" (3 points)`,  // unmapped invalid, loud
		`rename: "qa.node-b" → "qa_node_b" (explicit`,       // mapped invalid, shown
		"WARNING: 1 measurement name(s) violate Arc's rule", // final banner
		"--measurement-map", // remediation hint
	} {
		if !strings.Contains(string(out), want) {
			t.Errorf("dry-run output missing %q:\n%s", want, out)
		}
	}
	// samples must show the FINAL (renamed) name, and never a skipped/invalid one
	if strings.Contains(string(out), "    edge-prod.gateway_services,") {
		t.Errorf("dry-run sampled an invalid measurement line:\n%s", out)
	}
	if !strings.Contains(string(out), "qa_node_b,host=a") {
		t.Errorf("dry-run samples don't show the renamed line:\n%s", out)
	}
}

// TestFingerprintBackCompat: default measurement settings must produce the
// exact fingerprint of tsm2arc <= 0.1.2, so old checkpoints still resume.
func TestFingerprintBackCompat(t *testing.T) {
	base := runConfig{chunkSize: 256, start: math.MinInt64, end: math.MaxInt64}
	legacy := configFingerprint(base, "ns") // nil resolver = pre-0.1.3 shape

	withDefault := base
	withDefault.resolver, _ = measure.NewResolver(nil, measure.PolicyFail)
	if got := configFingerprint(withDefault, "ns"); got != legacy {
		t.Errorf("default-flags fingerprint %q != legacy %q", got, legacy)
	}

	withMap := base
	withMap.resolver, _ = measure.NewResolver(map[string]string{"a.b": "a_b"}, measure.PolicySkip)
	got := configFingerprint(withMap, "ns")
	if got == legacy || !strings.Contains(got, "mmap=a.b=a_b;on-invalid=skip") {
		t.Errorf("mapped fingerprint = %q", got)
	}
}
