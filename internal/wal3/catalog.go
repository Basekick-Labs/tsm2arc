package wal3

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// CatRecordID values this decoder understands (the binary catalog's core
// record-type partition; bit 15 marks Enterprise types).
const (
	CatCreateDatabase     uint16 = 4
	CatSoftDeleteDatabase uint16 = 5
	CatCreateTable        uint16 = 6
	CatSoftDeleteTable    uint16 = 7
	CatAddColumns         uint16 = 8
	CatHardDeleteDatabase uint16 = 22
	CatHardDeleteTable    uint16 = 23
)

// CatRecord is one decoded catalog record. Skip is true for record types the
// migration does not need (tokens, caches, triggers, nodes, ...); Record is
// then nil.
type CatRecord struct {
	ID     uint16          `json:"id"`
	Skip   bool            `json:"skip"`
	Record json.RawMessage `json:"record"`
}

// CatCreateDatabaseRec mirrors the upstream CreateDatabase record body.
type CatCreateDatabaseRec struct {
	DatabaseID   uint32 `json:"database_id"`
	DatabaseName string `json:"database_name"`
}

// CatCreateTableRec mirrors the upstream CreateTable record body.
type CatCreateTableRec struct {
	DatabaseID   uint32 `json:"database_id"`
	DatabaseName string `json:"database_name"`
	TableName    string `json:"table_name"`
	TableID      uint32 `json:"table_id"`
}

// CatColumnDef is one entry of an AddColumns record: the externally tagged
// ColumnDefinition enum — exactly one variant set.
type CatColumnDef struct {
	Timestamp *struct {
		ColumnID *uint16 `json:"column_id"`
		Name     string  `json:"name"`
	} `json:"Timestamp"`
	Tag *struct {
		ID       uint16  `json:"id"`
		ColumnID *uint16 `json:"column_id"`
		Name     string  `json:"name"`
	} `json:"Tag"`
	Field *struct {
		ColumnID *uint16 `json:"column_id"`
		Name     string  `json:"name"`
	} `json:"Field"`
}

// CatAddColumnsRec mirrors the upstream AddColumns record body.
type CatAddColumnsRec struct {
	DatabaseID uint32         `json:"database_id"`
	TableID    uint32         `json:"table_id"`
	Columns    []CatColumnDef `json:"columns"`
}

// CatDeleteRec covers the soft/hard delete bodies. Upstream names the
// database id field "database_id" on soft deletes and "db_id" on hard
// deletes — pick by record id (both can legitimately be 0: _internal).
type CatDeleteRec struct {
	DatabaseID uint32 `json:"database_id"`
	DBID       uint32 `json:"db_id"`
	TableID    uint32 `json:"table_id"`
}

// DecodeCatalogRecords decodes a stream of raw catalog record bodies. The
// input is [u16 LE id][u32 LE length][body] per record, in replay order (the
// caller strips file and record-header framing). Output preserves order.
func (d *Decoder) DecodeCatalogRecords(ctx context.Context, stream []byte) ([]CatRecord, error) {
	if len(stream) == 0 {
		return nil, nil
	}
	stdout, err := d.run(ctx, stream, "catalog")
	if err != nil {
		return nil, err
	}
	var out []CatRecord
	sc := bufio.NewScanner(bytes.NewReader(stdout))
	sc.Buffer(make([]byte, 0, 1<<20), 1<<26)
	for sc.Scan() {
		var r CatRecord
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return nil, fmt.Errorf("parse catalog decoder output: %w", err)
		}
		out = append(out, r)
	}
	return out, sc.Err()
}

// AppendCatalogRecord frames one record onto a stream for
// DecodeCatalogRecords.
func AppendCatalogRecord(stream []byte, id uint16, body []byte) []byte {
	stream = binary.LittleEndian.AppendUint16(stream, id)
	stream = binary.LittleEndian.AppendUint32(stream, uint32(len(body)))
	return append(stream, body...)
}
