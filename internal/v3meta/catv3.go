package v3meta

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/basekick-labs/tsm2arc/internal/vfs"
	"github.com/basekick-labs/tsm2arc/internal/wal3"
)

// The catalog/v3 (3.10+) binary format has NO snapshot state struct: the
// fixed-path snapshot file is the catalog's entire applied-record history in
// replay order, framed exactly like a log file. Reading the catalog =
// decoding records (bitcode derive mode, via the embedded wasm decoder) and
// applying them in order — snapshot first, then every log with a HIGHER
// sequence (the snapshot is checkpointed lazily, by default only every 100
// sequences or 1 hour, and shutdown does not checkpoint, so recent
// databases/tables can exist only in logs — same lag we observed on the
// JSON-era checkpoints).
//
// File framing (identical 3.10.0-3.11.4): 64-byte LE header — magic "IDB3",
// format_version u32 (=1), header CRC32 at 0x08 over bytes 0x0C..0x40,
// flags u16 (bit0 = snapshot), catalog UUID (16 bytes), sequence u64 at
// 0x20, record_count u32, group_count u32, payload_len u64, payload CRC32 —
// then the payload: group_count x 24-byte legacy group-index entries (a
// compat artifact, skipped), then record_count records, each with a 16-byte
// LE header (id u16, flags u16, sequence u64, length u32) and a raw bitcode
// body. No compression anywhere.

var catV3Magic = []byte("IDB3")

type catV3File struct {
	sequence uint64
	snapshot bool
	records  []catV3Record
}

type catV3Record struct {
	id   uint16
	body []byte
}

func parseCatV3File(b []byte, what string) (*catV3File, error) {
	if len(b) < 64 || !bytes.HasPrefix(b, catV3Magic) {
		return nil, fmt.Errorf("%s: not a catalog/v3 file (bad magic)", what)
	}
	if v := binary.LittleEndian.Uint32(b[4:8]); v != 1 {
		return nil, fmt.Errorf("%s: catalog/v3 format version %d (this build understands 1)", what, v)
	}
	if got, want := binary.LittleEndian.Uint32(b[8:12]), crc32.ChecksumIEEE(b[12:64]); got != want {
		return nil, fmt.Errorf("%s: header CRC mismatch", what)
	}
	f := &catV3File{
		snapshot: binary.LittleEndian.Uint16(b[12:14])&1 != 0,
		sequence: binary.LittleEndian.Uint64(b[32:40]),
	}
	recordCount := binary.LittleEndian.Uint32(b[40:44])
	groupCount := binary.LittleEndian.Uint32(b[44:48])
	payloadLen := binary.LittleEndian.Uint64(b[48:56])
	payloadCRC := binary.LittleEndian.Uint32(b[56:60])
	if uint64(len(b)-64) < payloadLen {
		return nil, fmt.Errorf("%s: truncated payload (%d of %d bytes)", what, len(b)-64, payloadLen)
	}
	payload := b[64 : 64+payloadLen]
	if crc32.ChecksumIEEE(payload) != payloadCRC {
		return nil, fmt.Errorf("%s: payload CRC mismatch", what)
	}
	off := int(groupCount) * 24 // legacy group index: skip
	if off > len(payload) {
		return nil, fmt.Errorf("%s: group index overruns payload", what)
	}
	for i := uint32(0); i < recordCount; i++ {
		if len(payload)-off < 16 {
			return nil, fmt.Errorf("%s: truncated record header (record %d)", what, i)
		}
		id := binary.LittleEndian.Uint16(payload[off : off+2])
		length := binary.LittleEndian.Uint32(payload[off+12 : off+16])
		off += 16
		if uint32(len(payload)-off) < length {
			return nil, fmt.Errorf("%s: truncated record body (record %d, id %d)", what, i, id)
		}
		f.records = append(f.records, catV3Record{id: id, body: payload[off : off+int(length)]})
		off += int(length)
	}
	return f, nil
}

// loadBinaryCatalog reads a catalog/v3 tree: the fixed snapshot plus newer
// logs, decoded through the embedded wasm decoder and applied in order.
func loadBinaryCatalog(f vfs.FS, node string) (*Catalog, error) {
	ctx := context.Background()
	dec, err := wal3.NewDecoder(ctx)
	if err != nil {
		return nil, err
	}
	defer dec.Close(ctx)

	c := &Catalog{Era: EraCatalogV3, Databases: map[uint32]*DatabaseMeta{}}

	var files []*catV3File
	if b, err := f.ReadFile(node + "/catalog/v3/snapshot"); err == nil {
		snap, err := parseCatV3File(b, node+"/catalog/v3/snapshot")
		if err != nil {
			return nil, err
		}
		files = append(files, snap)
		c.Sequence = snap.sequence
	}
	names, err := f.List(node + "/catalog/v3/logs")
	if err != nil {
		return nil, err
	}
	sort.Strings(names) // zero-padded sequences: lexicographic == numeric
	for _, name := range names {
		b, err := f.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("read catalog log %s: %w", name, err)
		}
		lf, err := parseCatV3File(b, name)
		if err != nil {
			return nil, err
		}
		if lf.sequence <= c.Sequence {
			continue // already folded into the snapshot
		}
		files = append(files, lf)
		if lf.sequence > c.Sequence {
			c.Sequence = lf.sequence
		}
	}

	// One decoder pass over the concatenated record stream, order preserved.
	var stream []byte
	var flat []catV3Record
	for _, cf := range files {
		for _, r := range cf.records {
			stream = wal3.AppendCatalogRecord(stream, r.id, r.body)
			flat = append(flat, r)
		}
	}
	recs, err := dec.DecodeCatalogRecords(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("decode catalog/v3 records: %w", err)
	}
	if len(recs) != len(flat) {
		return nil, fmt.Errorf("catalog/v3 decoder returned %d records for %d inputs", len(recs), len(flat))
	}
	unknown := map[string]bool{}
	for _, r := range recs {
		if r.Skip {
			continue
		}
		if err := c.applyCatV3(r, unknown); err != nil {
			return nil, err
		}
	}
	for op := range unknown {
		c.UnknownOps = append(c.UnknownOps, op)
	}
	sort.Strings(c.UnknownOps)
	return c, nil
}

func (c *Catalog) applyCatV3(r wal3.CatRecord, unknown map[string]bool) error {
	switch r.ID {
	case wal3.CatCreateDatabase:
		var rec wal3.CatCreateDatabaseRec
		if err := json.Unmarshal(r.Record, &rec); err != nil {
			return err
		}
		if _, ok := c.Databases[rec.DatabaseID]; !ok {
			c.Databases[rec.DatabaseID] = &DatabaseMeta{ID: rec.DatabaseID, Name: rec.DatabaseName, Tables: map[uint32]*TableMeta{}}
		}

	case wal3.CatCreateTable:
		var rec wal3.CatCreateTableRec
		if err := json.Unmarshal(r.Record, &rec); err != nil {
			return err
		}
		db := c.Databases[rec.DatabaseID]
		if db == nil {
			return fmt.Errorf("catalog/v3 CreateTable for unknown database id %d", rec.DatabaseID)
		}
		if _, ok := db.Tables[rec.TableID]; !ok {
			db.Tables[rec.TableID] = &TableMeta{ID: rec.TableID, Name: rec.TableName}
		}

	case wal3.CatAddColumns:
		var rec wal3.CatAddColumnsRec
		if err := json.Unmarshal(r.Record, &rec); err != nil {
			return err
		}
		db := c.Databases[rec.DatabaseID]
		if db == nil {
			return fmt.Errorf("catalog/v3 AddColumns for unknown database id %d", rec.DatabaseID)
		}
		t := db.Tables[rec.TableID]
		if t == nil {
			return fmt.Errorf("catalog/v3 AddColumns for unknown table id %d in database %d", rec.TableID, rec.DatabaseID)
		}
		for _, cd := range rec.Columns {
			switch {
			case cd.Tag != nil:
				if cd.Tag.ColumnID != nil {
					t.setColumn(uint32(*cd.Tag.ColumnID), cd.Tag.Name, ColTag)
				}
				t.appendSeriesKey(cd.Tag.Name)
			case cd.Timestamp != nil:
				if cd.Timestamp.ColumnID != nil {
					t.setColumn(uint32(*cd.Timestamp.ColumnID), cd.Timestamp.Name, ColTime)
				}
			case cd.Field != nil:
				if cd.Field.ColumnID != nil {
					t.setColumn(uint32(*cd.Field.ColumnID), cd.Field.Name, ColField)
				}
			}
		}

	case wal3.CatSoftDeleteDatabase, wal3.CatHardDeleteDatabase:
		var rec wal3.CatDeleteRec
		if err := json.Unmarshal(r.Record, &rec); err != nil {
			return err
		}
		id := rec.DatabaseID
		if r.ID == wal3.CatHardDeleteDatabase {
			id = rec.DBID
		}
		if db := c.Databases[id]; db != nil {
			db.Deleted = true
		}

	case wal3.CatSoftDeleteTable, wal3.CatHardDeleteTable:
		var rec wal3.CatDeleteRec
		if err := json.Unmarshal(r.Record, &rec); err != nil {
			return err
		}
		id := rec.DatabaseID
		if r.ID == wal3.CatHardDeleteTable {
			id = rec.DBID
		}
		if db := c.Databases[id]; db != nil {
			if t := db.Tables[rec.TableID]; t != nil {
				t.Deleted = true
			}
		}

	default:
		unknown[fmt.Sprintf("catalog/v3 record id %d", r.ID)] = true
	}
	return nil
}
