package v3meta

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/basekick-labs/tsm2arc/internal/vfs"
)

// CatalogEra identifies which on-disk catalog layout a store uses.
type CatalogEra int

const (
	EraUnknown CatalogEra = iota
	// EraCatalogs is 3.0–3.3: catalogs/{seq}.catalog logs + a _catalog_checkpoint
	// file at the store prefix, JSON payloads.
	EraCatalogs
	// EraCatalogV2 is 3.4–3.9: catalog/v2/logs/{seq}.catalog + catalog/v2/snapshot,
	// JSON payloads.
	EraCatalogV2
	// EraCatalogV3 is 3.10+: catalog/v3/, binary IDB3 records with bitcode
	// payloads — not readable by this package.
	EraCatalogV3
)

func (e CatalogEra) String() string {
	switch e {
	case EraCatalogs:
		return "catalogs/ (3.0-3.3)"
	case EraCatalogV2:
		return "catalog/v2 (3.4-3.9)"
	case EraCatalogV3:
		return "catalog/v3 (3.10+, binary)"
	default:
		return "unknown"
	}
}

// ErrBinaryCatalog reports a catalog/v3 (3.10+) binary catalog with no
// readable JSON fallback. Callers translate this into operator guidance
// (--db-name/--table-name flags, or the native decoder once it ships).
var ErrBinaryCatalog = errors.New("catalog is in the binary catalog/v3 format (InfluxDB 3.10+); names cannot be read by this build")

// Catalog is the resolved id→name mapping plus per-table series-key order.
type Catalog struct {
	Era      CatalogEra
	Sequence uint64
	// Stale is true when the catalog was read from a preserved pre-migration
	// JSON tree on a store whose current catalog is binary (catalog/v3):
	// correct for everything created before the 3.10 upgrade, blind to
	// anything after it.
	Stale     bool
	Databases map[uint32]*DatabaseMeta
	// UnknownOps lists log operation names the replay skipped, sorted and
	// deduplicated. Non-empty is worth a warning: the catalog state may be
	// missing whatever those ops did (e.g. a rename).
	UnknownOps []string
}

// DatabaseMeta names one database.
type DatabaseMeta struct {
	ID      uint32
	Name    string
	Deleted bool
	Tables  map[uint32]*TableMeta
}

// TableMeta names one table and carries its series key: the tag column names
// in INSERTION order — the order tags first appeared in writes. This is the
// table's physical sort order in every parquet file (with time appended), so
// the extractor's merge comparator depends on it. It is NOT lexicographic.
type TableMeta struct {
	ID        uint32
	Name      string
	Deleted   bool
	SeriesKey []string
}

// LoadCatalog detects the catalog era under the given node/store prefix and
// resolves the newest reachable JSON state: checkpoint (when present) plus
// every log with a higher sequence, replayed in order. Real stores REQUIRE
// the replay: the fixed-path checkpoint is written lazily, and a freshly
// created database can exist only in the logs (observed on a live 3.0.3).
//
// On a catalog/v3 (binary) store, a preserved catalog/v2 tree is read as a
// stale fallback (Catalog.Stale=true); with no fallback, ErrBinaryCatalog.
func LoadCatalog(f vfs.FS, node string) (*Catalog, error) {
	v3 := vfs.HasPrefix(f, node+"/catalog/v3")
	v2 := vfs.HasPrefix(f, node+"/catalog/v2")
	v1 := vfs.HasPrefix(f, node+"/catalogs")
	if !v1 {
		if _, err := f.ReadFile(node + "/_catalog_checkpoint"); err == nil {
			v1 = true
		}
	}

	switch {
	case v3 && v2:
		c, err := loadJSONCatalog(f, node+"/catalog/v2/snapshot", node+"/catalog/v2/logs")
		if err != nil {
			return nil, fmt.Errorf("catalog/v3 store: reading preserved catalog/v2 fallback: %w", err)
		}
		c.Era = EraCatalogV3
		c.Stale = true
		return c, nil
	case v3:
		return nil, ErrBinaryCatalog
	case v2:
		c, err := loadJSONCatalog(f, node+"/catalog/v2/snapshot", node+"/catalog/v2/logs")
		if err != nil {
			return nil, err
		}
		c.Era = EraCatalogV2
		return c, nil
	case v1:
		c, err := loadJSONCatalog(f, node+"/_catalog_checkpoint", node+"/catalogs")
		if err != nil {
			return nil, err
		}
		c.Era = EraCatalogs
		return c, nil
	default:
		return nil, fmt.Errorf("no InfluxDB 3 catalog found under %s (looked for catalogs/, catalog/v2, catalog/v3)", node)
	}
}

// frame headers: 10-byte identifier, 4-byte big-endian CRC32 (IEEE) of the
// payload, then the payload. idb3.001.* payloads are bitcode (written only by
// pre-release builds); .002-.004 are JSON.
var binaryMagic = []byte("IDB3")

func readFramed(b []byte, what string) (ident string, payload []byte, err error) {
	if bytes.HasPrefix(b, binaryMagic) {
		return "", nil, ErrBinaryCatalog
	}
	if len(b) < 14 || !bytes.HasPrefix(b, []byte("idb3.")) {
		return "", nil, fmt.Errorf("%s: not a catalog file (bad header)", what)
	}
	ident = string(b[:10])
	payload = b[14:]
	if got, want := binary.BigEndian.Uint32(b[10:14]), crc32.ChecksumIEEE(payload); got != want {
		return "", nil, fmt.Errorf("%s: CRC mismatch (file corrupt or truncated)", what)
	}
	if ident == "idb3.001.l" || ident == "idb3.001.s" {
		return "", nil, fmt.Errorf("%s: bitcode-encoded catalog payload (%s) from a pre-release build; not supported", what, ident)
	}
	return ident, payload, nil
}

func loadJSONCatalog(f vfs.FS, checkpointPath, logsPrefix string) (*Catalog, error) {
	c := &Catalog{Databases: map[uint32]*DatabaseMeta{}}

	if b, err := f.ReadFile(checkpointPath); err == nil {
		_, payload, err := readFramed(b, checkpointPath)
		if err != nil {
			return nil, err
		}
		if err := c.applyCheckpoint(payload, checkpointPath); err != nil {
			return nil, err
		}
	}

	names, err := f.List(logsPrefix)
	if err != nil {
		return nil, err
	}
	sort.Strings(names) // plain ascending sequence numbers, zero-padded
	unknown := map[string]bool{}
	for _, name := range names {
		b, err := f.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("read catalog log %s: %w", name, err)
		}
		_, payload, err := readFramed(b, name)
		if err != nil {
			return nil, err
		}
		if err := c.applyLog(payload, name, unknown); err != nil {
			return nil, err
		}
	}
	for op := range unknown {
		c.UnknownOps = append(c.UnknownOps, op)
	}
	sort.Strings(c.UnknownOps)
	return c, nil
}

// ---- checkpoint (CatalogSnapshot) ----

type catalogSnapshotJSON struct {
	Sequence  uint64 `json:"sequence"`
	Databases struct {
		Repo []pair[uint32, databaseSnapshotJSON] `json:"repo"`
	} `json:"databases"`
}

type databaseSnapshotJSON struct {
	ID        uint32          `json:"id"`
	Name      string          `json:"name"`
	Deleted   bool            `json:"deleted"`
	RawTables json.RawMessage `json:"tables"`
}

type tableSnapshotJSON struct {
	TableID   uint32          `json:"table_id"`
	TableName string          `json:"table_name"`
	Deleted   bool            `json:"deleted"`
	Key       []uint32        `json:"key"`
	Columns   json.RawMessage `json:"columns"`
}

func (c *Catalog) applyCheckpoint(payload []byte, what string) error {
	var snap catalogSnapshotJSON
	if err := json.Unmarshal(payload, &snap); err != nil {
		return fmt.Errorf("parse catalog checkpoint %s: %w", what, err)
	}
	c.Sequence = snap.Sequence
	for _, dbp := range snap.Databases.Repo {
		db := &DatabaseMeta{ID: dbp.Val.ID, Name: dbp.Val.Name, Deleted: dbp.Val.Deleted, Tables: map[uint32]*TableMeta{}}
		var tables struct {
			Repo []pair[uint32, tableSnapshotJSON] `json:"repo"`
		}
		if len(dbp.Val.RawTables) > 0 {
			if err := json.Unmarshal(dbp.Val.RawTables, &tables); err != nil {
				return fmt.Errorf("parse tables of db %d in %s: %w", db.ID, what, err)
			}
		}
		for _, tp := range tables.Repo {
			t := tp.Val
			key, err := seriesKeyNames(t.Key, t.Columns)
			if err != nil {
				return fmt.Errorf("resolve series key of table %d (%s) in %s: %w", t.TableID, t.TableName, what, err)
			}
			db.Tables[t.TableID] = &TableMeta{ID: t.TableID, Name: t.TableName, Deleted: t.Deleted, SeriesKey: key}
		}
		c.Databases[db.ID] = db
	}
	return nil
}

// seriesKeyNames resolves a checkpoint table's key ids to column names.
// The columns repo has two era shapes, distinguished by element form:
//
//	3.0-3.3 (idb3.002/003.s): repo is [[column_id, {name, id, influx_type,...}]]
//	  pairs, and key holds COLUMN ids.
//	3.4-3.9 (idb3.004.s): repo is a plain array of externally tagged enums
//	  ({"tag":{id, column_id, name}} / {"field":...} / {"timestamp":...}),
//	  and key holds TAG ids matching tag.id.
func seriesKeyNames(key []uint32, columnsRaw json.RawMessage) ([]string, error) {
	if len(key) == 0 {
		return nil, nil
	}
	var wrapper struct {
		Repo []json.RawMessage `json:"repo"`
	}
	if err := json.Unmarshal(columnsRaw, &wrapper); err != nil {
		return nil, err
	}
	idToName := map[uint32]string{}
	for _, el := range wrapper.Repo {
		trimmed := bytes.TrimLeft(el, " \t\r\n")
		if len(trimmed) > 0 && trimmed[0] == '[' {
			var p pair[uint32, struct {
				Name string `json:"name"`
			}]
			if err := json.Unmarshal(el, &p); err != nil {
				return nil, err
			}
			idToName[p.Key] = p.Val.Name
		} else {
			var enum map[string]struct {
				ID   uint32 `json:"id"`
				Name string `json:"name"`
			}
			if err := json.Unmarshal(el, &enum); err != nil {
				return nil, err
			}
			if tag, ok := enum["tag"]; ok {
				idToName[tag.ID] = tag.Name
			}
		}
	}
	names := make([]string, len(key))
	for i, id := range key {
		name, ok := idToName[id]
		if !ok {
			return nil, fmt.Errorf("series key references unknown column/tag id %d", id)
		}
		names[i] = name
	}
	return names, nil
}

// ---- log replay (OrderedCatalogBatch) ----

type orderedBatchJSON struct {
	SequenceNumber uint64                     `json:"sequence_number"`
	CatalogBatch   map[string]json.RawMessage `json:"catalog_batch"`
}

type databaseBatchJSON struct {
	DatabaseID   uint32            `json:"database_id"`
	DatabaseName string            `json:"database_name"`
	Ops          []json.RawMessage `json:"ops"`
}

func (c *Catalog) applyLog(payload []byte, what string, unknown map[string]bool) error {
	var batch orderedBatchJSON
	if err := json.Unmarshal(payload, &batch); err != nil {
		return fmt.Errorf("parse catalog log %s: %w", what, err)
	}
	if batch.SequenceNumber <= c.Sequence {
		return nil // already folded into the checkpoint
	}
	c.Sequence = batch.SequenceNumber

	raw, ok := batch.CatalogBatch["Database"]
	if !ok {
		return nil // Node/Token/Generation batches carry nothing we need
	}
	var db databaseBatchJSON
	if err := json.Unmarshal(raw, &db); err != nil {
		return fmt.Errorf("parse database batch in %s: %w", what, err)
	}
	for _, rawOp := range db.Ops {
		var op map[string]json.RawMessage
		if err := json.Unmarshal(rawOp, &op); err != nil {
			return fmt.Errorf("parse op in %s: %w", what, err)
		}
		for name, body := range op {
			if err := c.applyOp(name, body, unknown); err != nil {
				return fmt.Errorf("apply %s in %s: %w", name, what, err)
			}
		}
	}
	return nil
}

func (c *Catalog) applyOp(name string, body json.RawMessage, unknown map[string]bool) error {
	switch name {
	case "CreateDatabase":
		var op struct {
			DatabaseID   uint32 `json:"database_id"`
			DatabaseName string `json:"database_name"`
		}
		if err := json.Unmarshal(body, &op); err != nil {
			return err
		}
		if _, ok := c.Databases[op.DatabaseID]; !ok {
			c.Databases[op.DatabaseID] = &DatabaseMeta{ID: op.DatabaseID, Name: op.DatabaseName, Tables: map[uint32]*TableMeta{}}
		}

	case "CreateTable":
		var op struct {
			DatabaseID       uint32 `json:"database_id"`
			TableID          uint32 `json:"table_id"`
			TableName        string `json:"table_name"`
			FieldDefinitions []struct {
				Name     string          `json:"name"`
				ID       uint32          `json:"id"`
				DataType json.RawMessage `json:"data_type"`
			} `json:"field_definitions"`
			Key []json.RawMessage `json:"key"`
		}
		if err := json.Unmarshal(body, &op); err != nil {
			return err
		}
		db := c.Databases[op.DatabaseID]
		if db == nil {
			return fmt.Errorf("CreateTable for unknown database id %d", op.DatabaseID)
		}
		t := db.Tables[op.TableID]
		if t == nil {
			t = &TableMeta{ID: op.TableID, Name: op.TableName}
			db.Tables[op.TableID] = t
		}
		// 3.0-3.3 explicit creates carry the series key as column ids or
		// names; resolve either form against field_definitions.
		idToName := map[uint32]string{}
		for _, fd := range op.FieldDefinitions {
			idToName[fd.ID] = fd.Name
		}
		for _, k := range op.Key {
			var id uint32
			if err := json.Unmarshal(k, &id); err == nil {
				name, ok := idToName[id]
				if !ok {
					return fmt.Errorf("CreateTable key references unknown column id %d", id)
				}
				t.SeriesKey = append(t.SeriesKey, name)
				continue
			}
			var s string
			if err := json.Unmarshal(k, &s); err != nil {
				return fmt.Errorf("CreateTable key entry is neither id nor name: %s", k)
			}
			t.SeriesKey = append(t.SeriesKey, s)
		}

	case "AddFields": // 3.0-3.3: tags arrive as fields with data_type "Tag"
		var op struct {
			DatabaseID       uint32 `json:"database_id"`
			TableID          uint32 `json:"table_id"`
			FieldDefinitions []struct {
				Name     string          `json:"name"`
				DataType json.RawMessage `json:"data_type"`
			} `json:"field_definitions"`
		}
		if err := json.Unmarshal(body, &op); err != nil {
			return err
		}
		t, err := c.table(op.DatabaseID, op.TableID, name)
		if err != nil {
			return err
		}
		for _, fd := range op.FieldDefinitions {
			var dt string
			if json.Unmarshal(fd.DataType, &dt) == nil && dt == "Tag" {
				t.appendSeriesKey(fd.Name)
			}
		}

	case "AddColumns": // 3.4+: externally tagged column definitions
		var op struct {
			DatabaseID        uint32            `json:"database_id"`
			TableID           uint32            `json:"table_id"`
			ColumnDefinitions []json.RawMessage `json:"column_definitions"`
		}
		if err := json.Unmarshal(body, &op); err != nil {
			return err
		}
		t, err := c.table(op.DatabaseID, op.TableID, name)
		if err != nil {
			return err
		}
		for _, cd := range op.ColumnDefinitions {
			var enum map[string]struct {
				Name string `json:"name"`
			}
			if err := json.Unmarshal(cd, &enum); err != nil {
				return err
			}
			if tag, ok := enum["tag"]; ok {
				t.appendSeriesKey(tag.Name)
			}
		}

	case "SoftDeleteDatabase", "DeleteDatabase":
		var op struct {
			DatabaseID uint32 `json:"database_id"`
		}
		if err := json.Unmarshal(body, &op); err != nil {
			return err
		}
		if db := c.Databases[op.DatabaseID]; db != nil {
			db.Deleted = true
		}

	case "SoftDeleteTable", "DeleteTable":
		var op struct {
			DatabaseID uint32 `json:"database_id"`
			TableID    uint32 `json:"table_id"`
		}
		if err := json.Unmarshal(body, &op); err != nil {
			return err
		}
		if db := c.Databases[op.DatabaseID]; db != nil {
			if t := db.Tables[op.TableID]; t != nil {
				t.Deleted = true
			}
		}

	default:
		unknown[name] = true
	}
	return nil
}

func (c *Catalog) table(dbID, tableID uint32, op string) (*TableMeta, error) {
	db := c.Databases[dbID]
	if db == nil {
		return nil, fmt.Errorf("%s for unknown database id %d", op, dbID)
	}
	t := db.Tables[tableID]
	if t == nil {
		return nil, fmt.Errorf("%s for unknown table id %d in database %d", op, tableID, dbID)
	}
	return t, nil
}

func (t *TableMeta) appendSeriesKey(name string) {
	for _, existing := range t.SeriesKey {
		if existing == name {
			return
		}
	}
	t.SeriesKey = append(t.SeriesKey, name)
}
