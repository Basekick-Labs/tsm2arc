package extract3

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/tsm2arc/internal/extract"
	"github.com/basekick-labs/tsm2arc/internal/lp"
	"github.com/basekick-labs/tsm2arc/internal/tsm"
	"github.com/basekick-labs/tsm2arc/internal/v3meta"
	"github.com/basekick-labs/tsm2arc/internal/vfs"
)

// testdata/store is a real InfluxDB 3 Core 3.11.2 object store (metadata +
// the fleet/cpu table's data files). Its history was crafted for these tests:
//   - schema evolution: early files lack the "load" field and "region" tag
//     (solo-* hosts, NULL region);
//   - cross-file duplicates: bucket 2026-09-07T18:52 has THREE files, where
//     later writes overwrote (host=web-0,region=us-east) at :02/:08/:14 with
//     usage=999.9,load=99 — the LWW winners.
//
// The store's catalog is the binary catalog/v3 format, so the series key
// here is provided the way an operator would (--table-key): host,region in
// insertion order.

func cpuTable(t *testing.T) (vfs.FS, Table) {
	t.Helper()
	f := vfs.NewLocal("testdata/store")
	snaps, err := v3meta.LoadSnapshots(f, "node0")
	if err != nil {
		t.Fatal(err)
	}
	ls := v3meta.BuildLiveSet(snaps, nil, nil)
	k := v3meta.TableKey{DBID: 1, TableID: 0}
	files := make([]v3meta.ParquetFile, 0, len(ls.Tables[k]))
	for _, pf := range ls.Tables[k] {
		files = append(files, pf)
	}
	if len(files) < 15 {
		t.Fatalf("live set for fleet/cpu has %d files, expected the full fixture", len(files))
	}
	return f, Table{
		Key:         k,
		Measurement: "cpu",
		SeriesKey:   []string{"host", "region"},
		Files:       files,
	}
}

func runExtract(t *testing.T, f vfs.FS, tbl Table, start, end int64, cur *extract.Cursor) ([]extract.Point, string, extract.Stats) {
	t.Helper()
	var pts []extract.Point
	var b strings.Builder
	st, err := Extract(f, tbl, start, end, cur, Options{}, func(p extract.Point) {
		cp := p
		cp.Tags = append([][2]string(nil), p.Tags...)
		cp.Fields = append([]lp.Field(nil), p.Fields...)
		pts = append(pts, cp)
		lp.EncodePoint(&b, p.Measurement, p.Tags, p.Fields, p.UnixNano)
	})
	if err != nil {
		t.Fatal(err)
	}
	return pts, b.String(), st
}

func TestGoldenDeterminism(t *testing.T) {
	f, tbl := cpuTable(t)
	_, out1, st1 := runExtract(t, f, tbl, math.MinInt64, math.MaxInt64, nil)
	_, out2, st2 := runExtract(t, f, tbl, math.MinInt64, math.MaxInt64, nil)
	if out1 != out2 {
		t.Fatal("two runs produced different LP output")
	}
	if st1 != st2 {
		t.Fatalf("stats differ: %+v vs %+v", st1, st2)
	}
	if st1.Points == 0 {
		t.Fatal("no points extracted")
	}
}

func TestEmissionOrderIsCursorOrder(t *testing.T) {
	f, tbl := cpuTable(t)
	pts, _, _ := runExtract(t, f, tbl, math.MinInt64, math.MaxInt64, nil)
	for i := 1; i < len(pts); i++ {
		a, b := pts[i-1], pts[i]
		if a.SeriesKey > b.SeriesKey || (a.SeriesKey == b.SeriesKey && a.UnixNano >= b.UnixNano) {
			t.Fatalf("emission not in (SeriesKey, UnixNano) order at %d: %q@%d then %q@%d",
				i, a.SeriesKey, a.UnixNano, b.SeriesKey, b.UnixNano)
		}
	}
}

func TestLWWCrossFileDuplicates(t *testing.T) {
	f, tbl := cpuTable(t)
	bucketStart := time.Date(2026, 9, 7, 18, 52, 0, 0, time.UTC).UnixNano()
	pts, _, _ := runExtract(t, f, tbl, bucketStart, bucketStart+int64(time.Minute)-1, nil)

	// The bucket's three files hold 30 distinct (series, time) pairs plus 3
	// overwrites; exactly 30 must come out.
	if len(pts) != 30 {
		t.Fatalf("bucket emitted %d points, want 30 (LWW dedup)", len(pts))
	}
	winners := 0
	for _, p := range pts {
		var host string
		for _, tag := range p.Tags {
			if tag[0] == "host" {
				host = tag[1]
			}
		}
		sec := time.Unix(0, p.UnixNano).UTC().Second()
		if host == "web-0" && (sec == 2 || sec == 8 || sec == 14) {
			winners++
			for _, fld := range p.Fields {
				switch fld.Name {
				case "usage":
					if fld.Value.Float != 999.9 {
						t.Fatalf("LWW loser emitted at :%02d: usage=%v, want 999.9", sec, fld.Value.Float)
					}
				case "load":
					if fld.Value.Integer != 99 {
						t.Fatalf("LWW loser emitted at :%02d: load=%v, want 99", sec, fld.Value.Integer)
					}
				}
			}
		}
	}
	if winners != 3 {
		t.Fatalf("found %d overwritten points, want 3", winners)
	}
}

func TestNullTagsOmitted(t *testing.T) {
	f, tbl := cpuTable(t)
	pts, _, _ := runExtract(t, f, tbl, math.MinInt64, math.MaxInt64, nil)
	solo := 0
	for _, p := range pts {
		isSolo := false
		for _, tag := range p.Tags {
			if tag[0] == "host" && strings.HasPrefix(tag[1], "solo-") {
				isSolo = true
			}
		}
		if !isSolo {
			continue
		}
		solo++
		// solo rows were written without region and without load: the NULL
		// tag and NULL field must be absent, not empty.
		if len(p.Tags) != 1 {
			t.Fatalf("solo point has tags %v, want only host", p.Tags)
		}
		if len(p.Fields) != 1 || p.Fields[0].Name != "usage" {
			t.Fatalf("solo point has fields %v, want only usage", p.Fields)
		}
	}
	if solo != 5 {
		t.Fatalf("saw %d solo points, want 5", solo)
	}
}

func TestResumeSuffixByteIdentical(t *testing.T) {
	f, tbl := cpuTable(t)
	pts, full, _ := runExtract(t, f, tbl, math.MinInt64, math.MaxInt64, nil)

	lines := strings.SplitAfter(full, "\n")
	for _, cut := range []int{0, 1, len(pts) / 3, len(pts) / 2, len(pts) - 2, len(pts) - 1} {
		cur := &extract.Cursor{SeriesKey: pts[cut].SeriesKey, UnixNano: pts[cut].UnixNano}
		_, resumed, _ := runExtract(t, f, tbl, math.MinInt64, math.MaxInt64, cur)
		want := strings.Join(lines[cut+1:], "")
		if resumed != want {
			t.Fatalf("resume after point %d: output differs from the full run's suffix", cut)
		}
	}
}

func TestResumeSourceChangedFailsLoudly(t *testing.T) {
	f, tbl := cpuTable(t)
	pts, _, _ := runExtract(t, f, tbl, math.MinInt64, math.MaxInt64, nil)

	// A cursor whose bucket vanished.
	gone := &extract.Cursor{SeriesKey: bucketPrefix(42) + "|x", UnixNano: 1}
	if _, err := Extract(f, tbl, math.MinInt64, math.MaxInt64, gone, Options{}, func(extract.Point) {}); err == nil {
		t.Fatal("cursor with missing bucket must fail")
	}

	// A cursor in an existing bucket but at a (series, time) that was never
	// emitted there.
	mid := pts[len(pts)/2]
	fake := &extract.Cursor{SeriesKey: mid.SeriesKey, UnixNano: mid.UnixNano + 1}
	if _, err := Extract(f, tbl, math.MinInt64, math.MaxInt64, fake, Options{}, func(extract.Point) {}); err == nil {
		t.Fatal("cursor at never-emitted position must fail")
	}
}

func TestIncompleteSeriesKeyRefused(t *testing.T) {
	f, tbl := cpuTable(t)
	tbl.SeriesKey = []string{"host"} // region missing
	_, err := Extract(f, tbl, math.MinInt64, math.MaxInt64, nil, Options{}, func(extract.Point) {})
	if err == nil || !strings.Contains(err.Error(), "series key incomplete") {
		t.Fatalf("err = %v, want series-key-incomplete refusal", err)
	}
}

func TestBucketOverlapFallsBackToGlobalMerge(t *testing.T) {
	f, tbl := cpuTable(t)
	_, clean, _ := runExtract(t, f, tbl, math.MinInt64, math.MaxInt64, nil)

	// Forge one file's chunk_time so its span crosses the next bucket start:
	// the guard must fire and the global merge must still emit the exact
	// same deduplicated stream.
	forged := make([]v3meta.ParquetFile, len(tbl.Files))
	copy(forged, tbl.Files)
	minCT := forged[0].ChunkTime
	for _, pf := range forged {
		if pf.ChunkTime < minCT {
			minCT = pf.ChunkTime
		}
	}
	for i := range forged {
		if forged[i].ChunkTime != minCT {
			forged[i].ChunkTime = minCT // everyone claims the first bucket...
		}
	}
	forged[0].ChunkTime = minCT - int64(time.Minute) // ...except one earlier, overlapping file
	tbl.Files = forged

	warned := false
	var b strings.Builder
	_, err := Extract(f, tbl, math.MinInt64, math.MaxInt64, nil,
		Options{Warn: func(string, ...any) { warned = true }},
		func(p extract.Point) { lp.EncodePoint(&b, p.Measurement, p.Tags, p.Fields, p.UnixNano) })
	if err != nil {
		t.Fatal(err)
	}
	if !warned {
		t.Fatal("overlap guard did not warn")
	}
	// The fallback merges globally, so emission ORDER differs from the
	// bucketed run (and stays deterministic within the fallback mode); the
	// deduplicated CONTENT must be identical.
	if got, want := lineSet(b.String()), lineSet(clean); !reflect.DeepEqual(got, want) {
		t.Fatalf("global-merge fallback emitted different point set: %d lines vs %d", len(got), len(want))
	}
}

func lineSet(s string) map[string]int {
	m := map[string]int{}
	for _, l := range strings.Split(strings.TrimSuffix(s, "\n"), "\n") {
		m[l]++
	}
	return m
}

func TestOrderKeyEncoding(t *testing.T) {
	var b strings.Builder
	s := func(v string) *string { return &v }
	null := (*string)(nil)
	cases := [][]*string{
		{null, null},
		{null, s("a")},
		{s(""), null},
		{s("a"), null},
		{s("a"), s("a")},
		{s("a\x00b"), s("a")},
		{s("ab"), s("")},
	}
	var prev string
	for i, vals := range cases {
		k := orderKey(&b, vals)
		if i > 0 && !(prev < k) {
			t.Fatalf("orderKey not strictly increasing at case %d", i)
		}
		prev = k
	}
}

func TestExtraRowsWALSemantics(t *testing.T) {
	f, tbl := cpuTable(t)
	pts, _, _ := runExtract(t, f, tbl, math.MinInt64, math.MaxInt64, nil)

	// Pick a persisted point and overwrite it via an "extra" (WAL) row, add a
	// WAL-internal duplicate (rank decides), and a row in a brand-new bucket
	// (a WAL-only table window).
	victim := pts[len(pts)/2]
	bucketOf := func(ts int64) int64 { return ts - ts%int64(time.Minute) }
	newBucket := bucketOf(pts[len(pts)-1].UnixNano) + int64(10*time.Minute)

	mkFields := func(v float64) []lp.Field {
		return []lp.Field{{Name: "usage", Value: tsm.Value{Type: tsm.BlockFloat, Float: v}}}
	}
	tbl.Extra = map[int64][]MemRow{
		bucketOf(victim.UnixNano): {
			{Time: victim.UnixNano, Tags: victim.Tags, Fields: mkFields(111.5), Rank: 0}, // loses to rank 1
			{Time: victim.UnixNano, Tags: victim.Tags, Fields: mkFields(222.5), Rank: 1}, // WAL-internal LWW winner
		},
		newBucket: {
			{Time: newBucket + 1, Tags: [][2]string{{"host", "wal-only"}}, Fields: mkFields(1.0), Rank: 2},
		},
	}

	pts2, _, _ := runExtract(t, f, tbl, math.MinInt64, math.MaxInt64, nil)
	if len(pts2) != len(pts)+1 {
		t.Fatalf("with extras got %d points, want %d (one overwrite + one new)", len(pts2), len(pts)+1)
	}
	var sawWinner, sawWALOnly bool
	for _, p := range pts2 {
		if p.UnixNano == victim.UnixNano && reflect.DeepEqual(p.Tags, victim.Tags) {
			if len(p.Fields) != 1 || p.Fields[0].Value.Float != 222.5 {
				t.Fatalf("WAL overwrite lost: fields = %+v", p.Fields)
			}
			sawWinner = true
		}
		if p.UnixNano == newBucket+1 {
			if len(p.Tags) != 1 || p.Tags[0][1] != "wal-only" {
				t.Fatalf("WAL-only row malformed: %+v", p.Tags)
			}
			sawWALOnly = true
		}
	}
	if !sawWinner || !sawWALOnly {
		t.Fatalf("winner=%v walOnly=%v, want both", sawWinner, sawWALOnly)
	}

	// Determinism with extras: two runs, byte-identical.
	_, out1, _ := runExtract(t, f, tbl, math.MinInt64, math.MaxInt64, nil)
	_, out2, _ := runExtract(t, f, tbl, math.MinInt64, math.MaxInt64, nil)
	if out1 != out2 {
		t.Fatal("extraction with extras is not deterministic")
	}
}

func TestExtraRowUnknownTagRefused(t *testing.T) {
	f, tbl := cpuTable(t)
	tbl.Extra = map[int64][]MemRow{0: {{Time: 1, Tags: [][2]string{{"rogue", "x"}},
		Fields: []lp.Field{{Name: "usage", Value: tsm.Value{Type: tsm.BlockFloat, Float: 1}}}}}}
	_, err := Extract(f, tbl, math.MinInt64, math.MaxInt64, nil, Options{}, func(extract.Point) {})
	if err == nil || !strings.Contains(err.Error(), "series key incomplete") {
		t.Fatalf("err = %v, want series-key-incomplete refusal", err)
	}
}
