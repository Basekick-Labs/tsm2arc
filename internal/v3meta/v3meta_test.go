package v3meta

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/basekick-labs/tsm2arc/internal/vfs"
)

// The testdata trees are real stores written by InfluxDB 3 Core 3.0.3 (v30,
// node30: catalogs/ era), 3.4.2 (v34, node34: catalog/v2 era), and 3.11.2
// (v311, node0: catalog/v3 binary era), loaded with the same synthetic
// fleet/telemetry data.

func TestSnapshotSeqFromName(t *testing.T) {
	seq, ok := SnapshotSeqFromName("node0/snapshots/18446744073709551614.info.json")
	if !ok || seq != 1 {
		t.Fatalf("got (%d, %v), want (1, true)", seq, ok)
	}
	if _, ok := SnapshotSeqFromName("node0/snapshots/readme.txt"); ok {
		t.Fatal("non-manifest name parsed as snapshot")
	}
	seq, ok = SnapshotSeqFromName("node0/snapshots/00000000000000000000.info.json")
	if !ok || seq != math.MaxUint64 {
		t.Fatalf("all-zero stem: got (%d, %v)", seq, ok)
	}
}

func TestLoadCatalogEraA(t *testing.T) {
	// The 3.0.3 fixture's _catalog_checkpoint contains ZERO databases — the
	// fixed-path checkpoint is written lazily and everything user-created
	// lives only in catalogs/*.catalog logs. This test is the proof that log
	// replay is mandatory, not an optimization.
	c, err := LoadCatalog(vfs.NewLocal("testdata/v30"), "node30")
	if err != nil {
		t.Fatal(err)
	}
	if c.Era != EraCatalogs || c.Stale {
		t.Fatalf("era = %v stale = %v, want catalogs/ era, not stale", c.Era, c.Stale)
	}
	assertCatalogShape(t, c)
}

func TestLoadCatalogEraB(t *testing.T) {
	c, err := LoadCatalog(vfs.NewLocal("testdata/v34"), "node34")
	if err != nil {
		t.Fatal(err)
	}
	if c.Era != EraCatalogV2 || c.Stale {
		t.Fatalf("era = %v stale = %v, want catalog/v2 era, not stale", c.Era, c.Stale)
	}
	assertCatalogShape(t, c)
}

func TestLoadCatalogEraCBinary(t *testing.T) {
	_, err := LoadCatalog(vfs.NewLocal("testdata/v311"), "node0")
	if !errors.Is(err, ErrBinaryCatalog) {
		t.Fatalf("err = %v, want ErrBinaryCatalog", err)
	}
}

// assertCatalogShape checks the parts both JSON-era fixtures share: db and
// table names by id, and cpu's series key in tag-INSERTION order.
func assertCatalogShape(t *testing.T, c *Catalog) {
	t.Helper()
	names := map[uint32]string{}
	for id, db := range c.Databases {
		names[id] = db.Name
	}
	want := map[uint32]string{0: "_internal", 1: "fleet", 2: "telemetry"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("databases = %v, want %v", names, want)
	}
	fleet := c.Databases[1]
	cpu, mem := fleet.Tables[0], fleet.Tables[1]
	if cpu == nil || cpu.Name != "cpu" || mem == nil || mem.Name != "mem" {
		t.Fatalf("fleet tables = %+v", fleet.Tables)
	}
	if got := cpu.SeriesKey; !reflect.DeepEqual(got, []string{"host", "region"}) {
		t.Fatalf("cpu series key = %v, want [host region] (insertion order)", got)
	}
	if got := mem.SeriesKey; !reflect.DeepEqual(got, []string{"host"}) {
		t.Fatalf("mem series key = %v, want [host]", got)
	}
	if len(c.UnknownOps) != 0 {
		t.Fatalf("unknown ops skipped during replay: %v", c.UnknownOps)
	}
}

func TestLoadSnapshotsAndLiveSetRealStores(t *testing.T) {
	for _, tc := range []struct {
		dir, node string
		tables    int // fleet+telemetry cpu/mem; the v311 store also has fleet/heartbeat
	}{
		{"testdata/v30", "node30", 4},
		{"testdata/v34", "node34", 4},
		{"testdata/v311", "node0", 5},
	} {
		t.Run(tc.dir, func(t *testing.T) {
			f := vfs.NewLocal(tc.dir)
			snaps, err := LoadSnapshots(f, tc.node)
			if err != nil {
				t.Fatal(err)
			}
			if len(snaps) == 0 {
				t.Fatal("no snapshot manifests in fixture")
			}
			for i := 1; i < len(snaps); i++ {
				if snaps[i].SnapshotSequenceNumber <= snaps[i-1].SnapshotSequenceNumber {
					t.Fatal("snapshots not sorted ascending by sequence")
				}
			}
			indexes, deltas, err := LoadTableIndexes(f, tc.node)
			if err != nil {
				t.Fatal(err)
			}
			// Real stores create table indexes lazily; none of the fixtures
			// have them, which is exactly the case the fold must handle.
			if len(indexes) != 0 || len(deltas) != 0 {
				t.Fatalf("fixture unexpectedly has table indexes (%d) / deltas (%d)", len(indexes), len(deltas))
			}
			ls := BuildLiveSet(snaps, indexes, deltas)
			if ls.NewestSnapshot != snaps[len(snaps)-1].SnapshotSequenceNumber {
				t.Fatalf("NewestSnapshot = %d", ls.NewestSnapshot)
			}
			if ls.WALFileSequenceNumber == 0 {
				t.Fatal("WAL high-water mark missing")
			}
			if len(ls.Tables) != tc.tables {
				t.Fatalf("live set has %d tables, want %d: %v", len(ls.Tables), tc.tables, keysOf(ls.Tables))
			}
			var rows uint64
			for k, files := range ls.Tables {
				if len(files) == 0 {
					t.Fatalf("table %v has no live files", k)
				}
				for _, pf := range files {
					if !strings.HasPrefix(pf.Path, tc.node+"/dbs/") {
						t.Fatalf("file path %q not under %s/dbs/", pf.Path, tc.node)
					}
					if pf.RowCount == 0 || pf.SizeBytes == 0 {
						t.Fatalf("file %s has zero rows/bytes", pf.Path)
					}
					rows += pf.RowCount
				}
			}
			var manifestRows uint64
			for _, s := range snaps {
				manifestRows += s.RowCount
			}
			if rows != manifestRows {
				t.Fatalf("live rows %d != Σ manifest row_count %d (no removed_files in fixtures)", rows, manifestRows)
			}
		})
	}
}

func TestBuildLiveSetFoldAndIndexPrecedence(t *testing.T) {
	k := TableKey{DBID: 1, TableID: 0}
	pf := func(id uint64) ParquetFile {
		return ParquetFile{ID: id, Path: "n/dbs/1/0/f", RowCount: 1, SizeBytes: 1}
	}
	snaps := []PersistedSnapshot{
		{SnapshotSequenceNumber: 1, WALFileSequenceNumber: 10,
			Databases: SnapshotDatabases{k: {pf(1), pf(2)}}},
		{SnapshotSequenceNumber: 2, WALFileSequenceNumber: 20,
			Databases:    SnapshotDatabases{k: {pf(3)}},
			RemovedFiles: SnapshotDatabases{k: {pf(1)}}},
	}

	ls := BuildLiveSet(snaps, nil, nil)
	if got := idsOf(ls.Tables[k]); !reflect.DeepEqual(got, []uint64{2, 3}) {
		t.Fatalf("plain fold live ids = %v, want [2 3]", got)
	}
	if ls.WALFileSequenceNumber != 20 {
		t.Fatalf("WAL mark = %d, want 20", ls.WALFileSequenceNumber)
	}

	// A table index outranks manifests at or below its high-water mark: the
	// index says only file 9 is live as of snapshot 2 (retention purged the
	// rest and rewrote the index; the old manifests still list files 1-3 and
	// must NOT resurrect them). Snapshot 3 is newer and applies on top.
	indexes := map[TableKey]CoreTableIndex{
		k: {DBID: 1, TableID: 0, Files: []ParquetFile{pf(9)}, LatestSnapshotSequenceNumber: 2},
	}
	snaps = append(snaps, PersistedSnapshot{
		SnapshotSequenceNumber: 3, WALFileSequenceNumber: 30,
		Databases: SnapshotDatabases{k: {pf(4)}},
	})
	ls = BuildLiveSet(snaps, indexes, nil)
	if got := idsOf(ls.Tables[k]); !reflect.DeepEqual(got, []uint64{4, 9}) {
		t.Fatalf("index-primary live ids = %v, want [4 9]", got)
	}

	// Unmerged table-snapshot deltas above the mark apply too.
	deltas := map[TableKey][]TableIndexSnapshot{
		k: {{DBID: 1, TableID: 0, SnapshotSequenceNumber: 4, Files: []ParquetFile{pf(5)}, RemovedFiles: []uint64{9}}},
	}
	ls = BuildLiveSet(snaps, indexes, deltas)
	if got := idsOf(ls.Tables[k]); !reflect.DeepEqual(got, []uint64{4, 5}) {
		t.Fatalf("with deltas live ids = %v, want [4 5]", got)
	}
}

func TestCheckWAL(t *testing.T) {
	f := vfs.NewLocal("testdata/v311")
	snaps, err := LoadSnapshots(f, "node0")
	if err != nil {
		t.Fatal(err)
	}
	mark := snaps[len(snaps)-1].WALFileSequenceNumber

	st, err := CheckWAL(f, "node0", mark)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture keeps the two newest WAL files, both above the snapshot
	// mark: real un-snapshotted data a parquet-only migration would drop.
	if len(st.Unsnapshotted) != 2 || st.MaxSeq <= mark {
		t.Fatalf("unsnapshotted = %v (max %d, mark %d), want 2 files above mark", st.Unsnapshotted, st.MaxSeq, mark)
	}

	st, err = CheckWAL(f, "node0", st.MaxSeq)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Unsnapshotted) != 0 {
		t.Fatalf("with mark at max seq, unsnapshotted = %v, want none", st.Unsnapshotted)
	}
}

func TestReconcile(t *testing.T) {
	f := vfs.NewLocal("testdata/v311")
	k := TableKey{DBID: 1, TableID: 0}
	ls := &LiveSet{Tables: map[TableKey]map[uint64]ParquetFile{k: {
		1: {ID: 1, Path: "node0/dbs/1/0/2026-09-07/18-51/0000000015.parquet"},
		2: {ID: 2, Path: "node0/dbs/1/0/2026-09-07/18-51/9999999999.parquet"}, // gone
	}}}
	// The fixture has no dbs/ objects (metadata-only tree), so build the
	// existence picture from a full store listing instead: both dangling.
	rec, err := Reconcile(f, "node0", ls)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Present != 0 || len(rec.Dangling[k]) != 2 {
		t.Fatalf("present=%d dangling=%d, want 0/2 on metadata-only fixture", rec.Present, len(rec.Dangling[k]))
	}
}

func keysOf[K comparable, V any](m map[K]V) []K {
	out := make([]K, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func idsOf(files map[uint64]ParquetFile) []uint64 {
	ids := make([]uint64, 0, len(files))
	for id := range files {
		ids = append(ids, id)
	}
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
	return ids
}
