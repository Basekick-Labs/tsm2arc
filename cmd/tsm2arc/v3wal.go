package main

import (
	"context"
	"fmt"

	"github.com/basekick-labs/tsm2arc/internal/extract3"
	"github.com/basekick-labs/tsm2arc/internal/lp"
	"github.com/basekick-labs/tsm2arc/internal/tsm"
	"github.com/basekick-labs/tsm2arc/internal/v3meta"
	"github.com/basekick-labs/tsm2arc/internal/vfs"
	"github.com/basekick-labs/tsm2arc/internal/wal3"
)

// decodeWAL reads and decodes the given un-snapshotted WAL sequences and
// converts their rows into per-table, per-bucket MemRows for extraction.
// WAL rows reference columns by id; the catalog provides the names/kinds.
// Ranks increase in (WAL sequence, op, row) order so duplicates within the
// WAL resolve to the newest write.
func decodeWAL(f vfs.FS, node string, seqs []uint64, cat *v3meta.Catalog) (map[v3meta.TableKey]map[int64][]extract3.MemRow, int, error) {
	ctx := context.Background()
	dec, err := wal3.NewDecoder(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer dec.Close(ctx)

	out := map[v3meta.TableKey]map[int64][]extract3.MemRow{}
	rank, total := 0, 0
	for _, seq := range seqs {
		path := fmt.Sprintf("%s/wal/%011d.wal", node, seq)
		b, err := f.ReadFile(path)
		if err != nil {
			return nil, 0, fmt.Errorf("read %s: %w", path, err)
		}
		wc, err := dec.Decode(ctx, b)
		if err != nil {
			return nil, 0, fmt.Errorf("%s: %w", path, err)
		}
		for oi := range wc.Ops {
			w := wc.Ops[oi].Write
			if w == nil {
				continue // Noop: forced flush with no writes
			}
			db := cat.Databases[w.DatabaseID]
			if db == nil {
				return nil, 0, fmt.Errorf("%s: WAL write for database id %d (%q) not present in the catalog", path, w.DatabaseID, w.DatabaseName)
			}
			for ti, tableID := range w.TableIDs {
				tm := db.Tables[tableID]
				if tm == nil {
					return nil, 0, fmt.Errorf("%s: WAL write for table id %d of database %q not present in the catalog", path, tableID, w.DatabaseName)
				}
				k := v3meta.TableKey{DBID: w.DatabaseID, TableID: tableID}
				chunks, err := w.TableChunks[ti].Chunks()
				if err != nil {
					return nil, 0, fmt.Errorf("%s: %w", path, err)
				}
				for ct, chunk := range chunks {
					for _, r := range chunk.Rows {
						mr, err := memRow(tm, r, rank)
						if err != nil {
							return nil, 0, fmt.Errorf("%s (table %s): %w", path, tm.Name, err)
						}
						rank++
						if len(mr.Fields) == 0 {
							continue // tags/time only; nothing to emit
						}
						if out[k] == nil {
							out[k] = map[int64][]extract3.MemRow{}
						}
						out[k][ct] = append(out[k][ct], mr)
						total++
					}
				}
			}
		}
	}
	return out, total, nil
}

// memRow converts one decoded WAL row using the table's column-id map. The
// duplicated Timestamp field is dropped (Row.Time is authoritative); absent
// fields are NULLs by construction and simply don't appear.
func memRow(tm *v3meta.TableMeta, r wal3.Row, rank int) (extract3.MemRow, error) {
	mr := extract3.MemRow{Time: r.Time, Rank: rank}
	for _, fd := range r.Fields {
		col, ok := tm.Columns[uint32(fd.ID)]
		if !ok {
			return mr, fmt.Errorf("WAL row references column id %d not present in the catalog", fd.ID)
		}
		v := fd.Value
		switch {
		case v.Timestamp != nil:
			// Row.Time carries the same value; nothing to add.
		case v.Tag != nil:
			mr.Tags = append(mr.Tags, [2]string{col.Name, *v.Tag})
		case v.Float != nil:
			mr.Fields = append(mr.Fields, lp.Field{Name: col.Name, Value: tsm.Value{Type: tsm.BlockFloat, Float: *v.Float}})
		case v.Integer != nil:
			mr.Fields = append(mr.Fields, lp.Field{Name: col.Name, Value: tsm.Value{Type: tsm.BlockInteger, Integer: *v.Integer}})
		case v.UInteger != nil:
			mr.Fields = append(mr.Fields, lp.Field{Name: col.Name, Value: tsm.Value{Type: tsm.BlockUnsigned, Unsigned: *v.UInteger}})
		case v.Boolean != nil:
			mr.Fields = append(mr.Fields, lp.Field{Name: col.Name, Value: tsm.Value{Type: tsm.BlockBoolean, Boolean: *v.Boolean}})
		case v.String != nil:
			mr.Fields = append(mr.Fields, lp.Field{Name: col.Name, Value: tsm.Value{Type: tsm.BlockString, String: *v.String}})
		case v.Key != nil:
			return mr, fmt.Errorf("WAL row carries a v3 series-key value (column id %d); v3 line protocol is not supported", fd.ID)
		default:
			return mr, fmt.Errorf("WAL row field (column id %d) has no recognized value variant", fd.ID)
		}
	}
	return mr, nil
}
