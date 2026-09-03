package main

import (
	"io"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/basekick-labs/tsm2arc/internal/discover"
)

// captureAnalyze runs runAnalyze against the given config and returns stdout.
func captureAnalyze(t *testing.T, cfg runConfig) string {
	t.Helper()
	old := os.Stdout
	rp, wp, _ := os.Pipe()
	os.Stdout = wp
	runAnalyze(cfg)
	wp.Close()
	os.Stdout = old
	out, _ := io.ReadAll(rp)
	return string(out)
}

// redactName must be a stable pseudonym: same input, same output, everywhere.
func TestRedactNameStable(t *testing.T) {
	a := redactName("series", "cpu,host=node-a")
	b := redactName("series", "cpu,host=node-a")
	if a != b {
		t.Fatalf("redactName not deterministic: %q vs %q", a, b)
	}
	if !strings.HasPrefix(a, "series_") || len(a) != len("series_")+12 {
		t.Fatalf("unexpected pseudonym shape: %q", a)
	}
	if redactName("series", "cpu,host=node-b") == a {
		t.Fatal("different names collided")
	}
	if redactName("db", "cpu,host=node-a") == a {
		t.Fatal("prefix not part of the pseudonym")
	}
}

// --redact must strip every internal identifier from the analyze report while
// leaving all shape numbers identical to the unredacted run.
func TestAnalyzeRedactStripsIdentifiers(t *testing.T) {
	datadir := writeTSMShardMeasurements(t, "secretdb", testMeasurements, 3)
	shards, err := discover.Walk(datadir, "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	cfg := runConfig{shards: shards, start: math.MinInt64, end: math.MaxInt64}

	plain := captureAnalyze(t, cfg)
	cfg.redact = true
	redacted := captureAnalyze(t, cfg)

	// Identifiers present in the plain report must be gone in the redacted one.
	for _, name := range append([]string{"secretdb"}, testMeasurements...) {
		if !strings.Contains(plain, name) {
			t.Fatalf("plain report missing %q — fixture assumption broken", name)
		}
		if strings.Contains(redacted, name) {
			t.Errorf("redacted report leaks identifier %q", name)
		}
	}
	if !strings.Contains(redacted, "series_") || !strings.Contains(redacted, "db_") {
		t.Errorf("redacted report lacks pseudonyms:\n%s", redacted)
	}

	// Shape data must be untouched: strip both reports down to their digits and
	// compare. (Identifiers are hex-free-form, so compare only numeric runs per
	// line position after removing name tokens is fragile; instead assert the
	// headline counters match.)
	for _, marker := range []string{"4 series", "1 tsm files", "4 keys"} {
		if !strings.Contains(redacted, marker) {
			t.Errorf("redacted report lost shape data %q", marker)
		}
		if !strings.Contains(plain, marker) {
			t.Errorf("plain report lost shape data %q", marker)
		}
	}

	// Stability across runs: pseudonyms must not change.
	redacted2 := captureAnalyze(t, cfg)
	if redacted2 != redacted {
		t.Error("redacted report differs between runs — pseudonyms must be stable")
	}
}
