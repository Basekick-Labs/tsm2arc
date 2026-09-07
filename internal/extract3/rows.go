package extract3

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/basekick-labs/tsm2arc/internal/lp"
	"github.com/basekick-labs/tsm2arc/internal/pq"
	"github.com/basekick-labs/tsm2arc/internal/tsm"
	"github.com/basekick-labs/tsm2arc/internal/v3meta"
	"github.com/basekick-labs/tsm2arc/internal/vfs"
)

// row is one materialized point of a bucket, ready to merge.
type row struct {
	// orderKey encodes the series-key column values (catalog insertion
	// order, NULLS FIRST) so that plain string comparison reproduces the
	// files' physical sort order. See orderKey().
	orderKey string
	ts       int64
	rank     int // position of the source file in the bucket's LWW order
	tags     [][2]string
	fields   []lp.Field
}

// readBucket materializes every in-range row of the bucket's files and sorts
// them into emission order: (series, time, file rank) — rank last, so the
// final row of a duplicate group is the LWW winner.
//
// Memory: one bucket's rows are resident at once. Buckets are gen1-duration
// slices (10 minutes by default), which keeps this bounded in practice; a
// streaming row-group merge is the optimization path if a store proves
// otherwise, and changes nothing about emission order.
func readBucket(f vfs.FS, tbl Table, b bucket, start, end int64) ([]row, error) {
	keyPos := make(map[string]int, len(tbl.SeriesKey))
	for i, name := range tbl.SeriesKey {
		keyPos[name] = i
	}

	var rows []row
	for rank, pf := range b.files {
		if err := readFile(f, tbl, pf, rank, keyPos, start, end, &rows); err != nil {
			return nil, err
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		a, b := &rows[i], &rows[j]
		if a.orderKey != b.orderKey {
			return a.orderKey < b.orderKey
		}
		if a.ts != b.ts {
			return a.ts < b.ts
		}
		return a.rank < b.rank
	})
	return rows, nil
}

func readFile(f vfs.FS, tbl Table, pf v3meta.ParquetFile, rank int, keyPos map[string]int, start, end int64, rows *[]row) error {
	r, size, err := f.ReaderAt(pf.Path)
	if err != nil {
		return fmt.Errorf("open live parquet %s: %w", pf.Path, err)
	}
	defer r.Close()
	pqf, err := pq.Open(r, size)
	if err != nil {
		return fmt.Errorf("%s: %w", pf.Path, err)
	}
	defer pqf.Close()

	s := pqf.Schema()
	// The comparator is only sound if every tag column is part of the
	// series key; an unknown tag would make distinct series compare equal
	// and LWW would silently drop real data. Hard error, never guess.
	for _, c := range s.Columns {
		if c.Kind == pq.KindTag {
			if _, ok := keyPos[c.Name]; !ok {
				return fmt.Errorf("%s: tag column %q is not in the table's series key %v — series key incomplete (wrong catalog state or operator-provided key)", pf.Path, c.Name, tbl.SeriesKey)
			}
		}
	}

	var kb strings.Builder
	err = pqf.ReadAll(context.Background(), func(batch *pq.Batch) error {
		// Locate columns each batch (cheap; guards against schema surprises).
		timeCol := &batch.Columns[s.TimeIndex]
		for rIdx := 0; rIdx < batch.N; rIdx++ {
			ts := timeCol.Ints[rIdx]
			if ts < start || ts > end {
				continue
			}
			// Series-key values in catalog order; columns missing from this
			// file (older schema) stay NULL.
			vals := make([]*string, len(tbl.SeriesKey))
			var tags [][2]string
			var fields []lp.Field
			for ci := range batch.Columns {
				col := &batch.Columns[ci]
				if !col.Valid[rIdx] {
					continue
				}
				switch col.Kind {
				case pq.KindTag:
					v := col.Strings[rIdx]
					vals[keyPos[col.Name]] = &v
					tags = append(tags, [2]string{col.Name, v})
				case pq.KindFieldFloat:
					fields = append(fields, lp.Field{Name: col.Name, Value: tsm.Value{Type: tsm.BlockFloat, Float: col.Floats[rIdx]}})
				case pq.KindFieldInt:
					fields = append(fields, lp.Field{Name: col.Name, Value: tsm.Value{Type: tsm.BlockInteger, Integer: col.Ints[rIdx]}})
				case pq.KindFieldUInt:
					fields = append(fields, lp.Field{Name: col.Name, Value: tsm.Value{Type: tsm.BlockUnsigned, Unsigned: col.UInts[rIdx]}})
				case pq.KindFieldBool:
					fields = append(fields, lp.Field{Name: col.Name, Value: tsm.Value{Type: tsm.BlockBoolean, Boolean: col.Bools[rIdx]}})
				case pq.KindFieldString:
					fields = append(fields, lp.Field{Name: col.Name, Value: tsm.Value{Type: tsm.BlockString, String: col.Strings[rIdx]}})
				}
			}
			if len(fields) == 0 {
				continue // row carries no field values in range (all NULL)
			}
			// Canonical LP: tags sorted by name; catalog order is merge-only.
			sort.Slice(tags, func(i, j int) bool { return tags[i][0] < tags[j][0] })
			sort.Slice(fields, func(i, j int) bool { return fields[i].Name < fields[j].Name })
			*rows = append(*rows, row{
				orderKey: orderKey(&kb, vals),
				ts:       ts,
				rank:     rank,
				tags:     tags,
				fields:   fields,
			})
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("%s: %w", pf.Path, err)
	}
	return nil
}

// orderKey encodes series-key values so that byte comparison of the result
// equals the files' physical sort order: per column, NULL is 0x00 (sorting
// before any value); a value is 0x01 + the value with 0x00 escaped as
// 0x00 0xFF, terminated by 0x00. The terminator (0x00 at end or before the
// next column's marker) sorts below every escaped continuation, so prefixes
// order correctly.
func orderKey(b *strings.Builder, vals []*string) string {
	b.Reset()
	for _, v := range vals {
		if v == nil {
			b.WriteByte(0x00)
			continue
		}
		b.WriteByte(0x01)
		s := *v
		for i := 0; i < len(s); i++ {
			b.WriteByte(s[i])
			if s[i] == 0x00 {
				b.WriteByte(0xFF)
			}
		}
		b.WriteByte(0x00)
	}
	return b.String()
}
