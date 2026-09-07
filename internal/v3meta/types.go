// Package v3meta reads InfluxDB 3 (Core/Enterprise, Parquet engine) object
// store metadata: snapshot manifests, table indexes, catalogs, and the WAL
// listing. It answers three questions the extractor needs:
//
//  1. the LIVE parquet file set per (database, table) — which objects hold the
//     data to migrate (snapshots.go),
//  2. the names and series-key column order for those ids (catalog.go),
//  3. whether un-snapshotted WAL exists, i.e. whether the parquet set is
//     complete (wal.go).
//
// All structures below mirror the upstream Rust structs' serde output exactly,
// verified both against the influxdata/influxdb source at v3.0.0/v3.4.2/v3.10.0
// and against stores written by real 3.0.3, 3.4.2, and 3.11.2 servers (the
// testdata fixtures are those real stores' metadata).
package v3meta

import (
	"encoding/json"
	"fmt"
)

// TableKey identifies a table by its stable on-disk ids. Ids, not names, key
// everything downstream (checkpointing included): names can be re-resolved
// differently across runs, ids never change.
type TableKey struct {
	DBID    uint32
	TableID uint32
}

func (k TableKey) String() string { return fmt.Sprintf("db %d/table %d", k.DBID, k.TableID) }

// ParquetFile is one data object as recorded in snapshot manifests and table
// indexes (identical shape in both, all versions 3.0–3.11). Path is the full
// store-relative object path — upstream always records it and readers must
// never reconstruct it from convention (the naming scheme changed at 3.6).
type ParquetFile struct {
	ID        uint64 `json:"id"`
	Path      string `json:"path"`
	SizeBytes uint64 `json:"size_bytes"`
	RowCount  uint64 `json:"row_count"`
	ChunkTime int64  `json:"chunk_time"`
	MinTime   int64  `json:"min_time"`
	MaxTime   int64  `json:"max_time"`
}

// PersistedSnapshot is one snapshots/*.info.json manifest. The upstream
// wrapper enum is internally tagged, so the JSON is flat with a "version"
// STRING field ("1" is the only variant through 3.11).
type PersistedSnapshot struct {
	Version                string            `json:"version"`
	NodeID                 string            `json:"node_id"`
	NextFileID             uint64            `json:"next_file_id"`
	SnapshotSequenceNumber uint64            `json:"snapshot_sequence_number"`
	WALFileSequenceNumber  uint64            `json:"wal_file_sequence_number"`
	CatalogSequenceNumber  uint64            `json:"catalog_sequence_number"`
	ParquetSizeBytes       uint64            `json:"parquet_size_bytes"`
	RowCount               uint64            `json:"row_count"`
	MinTime                int64             `json:"min_time"`
	MaxTime                int64             `json:"max_time"`
	Databases              SnapshotDatabases `json:"databases"`
	// RemovedFiles was added in 3.2.0 and is absent from older manifests.
	RemovedFiles SnapshotDatabases `json:"removed_files"`
}

// UnmarshalJSON accepts the pre-3.x "writer_id" alias for node_id, mirroring
// the upstream serde alias.
func (s *PersistedSnapshot) UnmarshalJSON(data []byte) error {
	type alias PersistedSnapshot
	aux := struct {
		*alias
		WriterID string `json:"writer_id"`
	}{alias: (*alias)(s)}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if s.NodeID == "" {
		s.NodeID = aux.WriterID
	}
	return nil
}

// SnapshotDatabases is upstream's SerdeVecMap<DbId, DatabaseTables>: a JSON
// array of [db_id, {"tables": [[table_id, [ParquetFile, ...]], ...]}] pairs,
// NOT an object. Flattened here to the per-table map the fold works in.
type SnapshotDatabases map[TableKey][]ParquetFile

func (m *SnapshotDatabases) UnmarshalJSON(data []byte) error {
	*m = SnapshotDatabases{}
	var dbs []pair[uint32, struct {
		Tables []pair[uint32, []ParquetFile] `json:"tables"`
	}]
	if err := json.Unmarshal(data, &dbs); err != nil {
		return err
	}
	for _, db := range dbs {
		for _, t := range db.Val.Tables {
			(*m)[TableKey{DBID: db.Key, TableID: t.Key}] = t.Val
		}
	}
	return nil
}

// pair decodes one [key, value] element of a SerdeVecMap array.
type pair[K, V any] struct {
	Key K
	Val V
}

func (p *pair[K, V]) UnmarshalJSON(data []byte) error {
	var raw [2]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if err := json.Unmarshal(raw[0], &p.Key); err != nil {
		return err
	}
	return json.Unmarshal(raw[1], &p.Val)
}

// CoreTableIndex is db-indices/{db_id}/{table_id}/index.info.json (3.3+): the
// rolled-up live file list for one table, plus the high-water mark saying
// which snapshot manifests are already folded in. Retention and hard deletes
// REWRITE this file in place while purging parquet objects directly, which is
// why (when present) it outranks older snapshot manifests as truth.
type CoreTableIndex struct {
	NodeID                       string        `json:"node_id"`
	DBID                         uint32        `json:"db_id"`
	TableID                      uint32        `json:"table_id"`
	Files                        []ParquetFile `json:"files"`
	LatestSnapshotSequenceNumber uint64        `json:"latest_snapshot_sequence_number"`
	RowCount                     int64         `json:"row_count"`
	ParquetSizeBytes             int64         `json:"parquet_size_bytes"`
	MinTime                      int64         `json:"min_time"`
	MaxTime                      int64         `json:"max_time"`
}

// TableIndexSnapshot is table-snapshots/{db_id}/{table_id}/{seq}.info.json: a
// per-snapshot delta that exists only until it is merged into the
// CoreTableIndex and deleted.
type TableIndexSnapshot struct {
	NodeID                 string        `json:"node_id"`
	DBID                   uint32        `json:"db_id"`
	TableID                uint32        `json:"table_id"`
	SnapshotSequenceNumber uint64        `json:"snapshot_sequence_number"`
	Files                  []ParquetFile `json:"files"`
	RemovedFiles           []uint64      `json:"removed_files"`
}
