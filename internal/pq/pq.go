// Package pq reads InfluxDB 3 parquet data files.
//
// Files are self-describing: every column carries iox::column::type metadata
// (tag / timestamp / field::<type>) in the embedded Arrow schema, tags are
// Dictionary(Int32, Utf8) or plain Utf8, and time is a nanosecond timestamp
// named "time". Classification is strictly per-file and metadata-driven —
// real stores contain sibling files of one table with DIFFERENT column
// orders and column sets (schema evolution appends columns; older files
// simply lack them), so nothing here assumes position or table-wide shape.
//
// Rows inside a file are sorted by (series-key columns in tag insertion
// order, NULLS FIRST, then time) and are duplicate-free; duplicates exist
// only ACROSS files and are the extractor's problem.
package pq

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// ColumnKind classifies a column for line-protocol emission.
type ColumnKind int

const (
	KindTag ColumnKind = iota
	KindTime
	KindFieldFloat
	KindFieldInt
	KindFieldUInt
	KindFieldBool
	KindFieldString
)

func (k ColumnKind) String() string {
	switch k {
	case KindTag:
		return "tag"
	case KindTime:
		return "time"
	case KindFieldFloat:
		return "field::float"
	case KindFieldInt:
		return "field::integer"
	case KindFieldUInt:
		return "field::uinteger"
	case KindFieldBool:
		return "field::boolean"
	case KindFieldString:
		return "field::string"
	default:
		return "unknown"
	}
}

// Column is one classified column of a file.
type Column struct {
	Name string
	Kind ColumnKind
}

// Schema is the classified column set of one file, in the file's own order.
type Schema struct {
	Columns []Column
	// TimeIndex is the position of the time column in Columns.
	TimeIndex int
}

// Tags returns the tag column names in file order (= series-key insertion
// order for the columns present in this file).
func (s *Schema) Tags() []string {
	var out []string
	for _, c := range s.Columns {
		if c.Kind == KindTag {
			out = append(out, c.Name)
		}
	}
	return out
}

// File is an open parquet data file.
type File struct {
	pf     *file.Reader
	ar     *pqarrow.FileReader
	schema *Schema
	arrow  *arrow.Schema
}

// Open reads the footer and classifies the schema. The reader must stay
// valid until Close; Close does NOT close the underlying ReaderAt.
func Open(r io.ReaderAt, size int64) (*File, error) {
	pf, err := file.NewParquetReader(io.NewSectionReader(r, 0, size))
	if err != nil {
		return nil, fmt.Errorf("open parquet: %w", err)
	}
	ar, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 64 << 10}, memory.DefaultAllocator)
	if err != nil {
		pf.Close()
		return nil, fmt.Errorf("open parquet as arrow: %w", err)
	}
	as, err := ar.Schema()
	if err != nil {
		pf.Close()
		return nil, fmt.Errorf("read arrow schema: %w", err)
	}
	schema, err := classify(as)
	if err != nil {
		pf.Close()
		return nil, err
	}
	return &File{pf: pf, ar: ar, schema: schema, arrow: as}, nil
}

func (f *File) Close() error { return f.pf.Close() }

// Schema returns the classified columns.
func (f *File) Schema() *Schema { return f.schema }

// NumRows reports the file's total row count (from the footer).
func (f *File) NumRows() int64 { return f.pf.NumRows() }

// classify maps every column to its kind using iox::column::type metadata,
// falling back to structural typing for files without it (the fallback the
// review demanded: metadata-first, but never helpless without it).
func classify(as *arrow.Schema) (*Schema, error) {
	s := &Schema{TimeIndex: -1}
	for i, fld := range as.Fields() {
		kind, err := classifyField(fld)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", fld.Name, err)
		}
		if kind == KindTime {
			if s.TimeIndex >= 0 {
				return nil, fmt.Errorf("two timestamp columns (%q and %q)", as.Field(s.TimeIndex).Name, fld.Name)
			}
			s.TimeIndex = i
		}
		s.Columns = append(s.Columns, Column{Name: fld.Name, Kind: kind})
	}
	if s.TimeIndex < 0 {
		return nil, fmt.Errorf("no timestamp column")
	}
	return s, nil
}

func classifyField(fld arrow.Field) (ColumnKind, error) {
	if v, ok := fld.Metadata.GetValue("iox::column::type"); ok {
		switch v {
		case "iox::column_type::tag":
			return KindTag, nil
		case "iox::column_type::timestamp":
			return KindTime, nil
		case "iox::column_type::field::float":
			return KindFieldFloat, nil
		case "iox::column_type::field::integer":
			return KindFieldInt, nil
		case "iox::column_type::field::uinteger":
			return KindFieldUInt, nil
		case "iox::column_type::field::boolean":
			return KindFieldBool, nil
		case "iox::column_type::field::string":
			return KindFieldString, nil
		default:
			return 0, fmt.Errorf("unknown iox::column::type %q", v)
		}
	}
	// Structural fallback. Plain Utf8 is ambiguous between tag and string
	// field; dictionary encoding is how IOx writes tags, so that is the
	// discriminator here, and plain strings classify as fields.
	switch dt := fld.Type.(type) {
	case *arrow.TimestampType:
		return KindTime, nil
	case *arrow.DictionaryType:
		if dt.ValueType.ID() == arrow.STRING || dt.ValueType.ID() == arrow.LARGE_STRING {
			return KindTag, nil
		}
		return 0, fmt.Errorf("unsupported dictionary value type %s", dt.ValueType)
	case *arrow.Float64Type:
		return KindFieldFloat, nil
	case *arrow.Int64Type:
		return KindFieldInt, nil
	case *arrow.Uint64Type:
		return KindFieldUInt, nil
	case *arrow.BooleanType:
		return KindFieldBool, nil
	case *arrow.StringType, *arrow.LargeStringType:
		return KindFieldString, nil
	default:
		return 0, fmt.Errorf("unsupported column type %s (no iox metadata)", fld.Type)
	}
}

// Batch is one materialized chunk of rows in columnar form. Column order and
// indexes match Schema.Columns; a column's slice is nil-valid-aware via
// Valid (nil Valid means every value is set).
type Batch struct {
	N       int
	Columns []BatchColumn
}

// BatchColumn holds one column's values for a batch. Exactly one of the
// value slices is populated, per the column's kind; Valid[i]==false means
// NULL (for tags: the tag is absent from that row's series key).
type BatchColumn struct {
	Column
	Valid   []bool
	Strings []string // tags and string fields
	Floats  []float64
	Ints    []int64 // also time (nanoseconds)
	UInts   []uint64
	Bools   []bool
}

// ReadAll streams every row group through fn, one materialized Batch per
// arrow record. The Batch is reused between calls — fn must not retain it.
func (f *File) ReadAll(ctx context.Context, fn func(*Batch) error) error {
	rr, err := f.ar.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return fmt.Errorf("record reader: %w", err)
	}
	defer rr.Release()
	var b Batch
	for rr.Next() {
		rec := rr.RecordBatch()
		if err := materialize(&b, f.schema, rec); err != nil {
			return err
		}
		if err := fn(&b); err != nil {
			return err
		}
	}
	return rr.Err()
}

func materialize(b *Batch, s *Schema, rec arrow.RecordBatch) error {
	n := int(rec.NumRows())
	b.N = n
	if cap(b.Columns) < len(s.Columns) {
		b.Columns = make([]BatchColumn, len(s.Columns))
	}
	b.Columns = b.Columns[:len(s.Columns)]
	for i, col := range s.Columns {
		bc := &b.Columns[i]
		bc.Column = col
		arr := rec.Column(i)
		if arr.Len() != n {
			return fmt.Errorf("column %q length %d != %d", col.Name, arr.Len(), n)
		}
		bc.Valid = grow(bc.Valid, n)
		for r := 0; r < n; r++ {
			bc.Valid[r] = arr.IsValid(r)
		}
		var err error
		switch col.Kind {
		case KindTag, KindFieldString:
			bc.Strings, err = stringValues(bc.Strings, arr, n)
		case KindTime:
			bc.Ints, err = timeValues(bc.Ints, arr, n)
		case KindFieldFloat:
			a, ok := arr.(*array.Float64)
			if !ok {
				err = typeErr(arr)
			} else {
				bc.Floats = grow(bc.Floats, n)
				copy(bc.Floats, a.Float64Values())
			}
		case KindFieldInt:
			a, ok := arr.(*array.Int64)
			if !ok {
				err = typeErr(arr)
			} else {
				bc.Ints = grow(bc.Ints, n)
				copy(bc.Ints, a.Int64Values())
			}
		case KindFieldUInt:
			a, ok := arr.(*array.Uint64)
			if !ok {
				err = typeErr(arr)
			} else {
				bc.UInts = grow(bc.UInts, n)
				copy(bc.UInts, a.Uint64Values())
			}
		case KindFieldBool:
			a, ok := arr.(*array.Boolean)
			if !ok {
				err = typeErr(arr)
			} else {
				bc.Bools = grow(bc.Bools, n)
				for r := 0; r < n; r++ {
					bc.Bools[r] = a.Value(r)
				}
			}
		}
		if err != nil {
			return fmt.Errorf("column %q: %w", col.Name, err)
		}
	}
	return nil
}

// stringValues decodes Utf8, LargeUtf8, or Dictionary(*, Utf8) into out.
func stringValues(out []string, arr arrow.Array, n int) ([]string, error) {
	out = grow(out, n)
	switch a := arr.(type) {
	case *array.String:
		for r := 0; r < n; r++ {
			if a.IsValid(r) {
				out[r] = a.Value(r)
			} else {
				out[r] = ""
			}
		}
	case *array.LargeString:
		for r := 0; r < n; r++ {
			if a.IsValid(r) {
				out[r] = a.Value(r)
			} else {
				out[r] = ""
			}
		}
	case *array.Dictionary:
		dict := a.Dictionary()
		var value func(int) string
		switch d := dict.(type) {
		case *array.String:
			value = d.Value
		case *array.LargeString:
			value = d.Value
		default:
			return out, fmt.Errorf("dictionary values are %s, want strings", dict.DataType())
		}
		for r := 0; r < n; r++ {
			if a.IsValid(r) {
				out[r] = value(a.GetValueIndex(r))
			} else {
				out[r] = ""
			}
		}
	default:
		return out, typeErr(arr)
	}
	return out, nil
}

// timeValues decodes a nanosecond timestamp column. Other units are scaled.
func timeValues(out []int64, arr arrow.Array, n int) ([]int64, error) {
	a, ok := arr.(*array.Timestamp)
	if !ok {
		return out, typeErr(arr)
	}
	tt, ok := a.DataType().(*arrow.TimestampType)
	if !ok {
		return out, typeErr(arr)
	}
	var mult int64
	switch tt.Unit {
	case arrow.Nanosecond:
		mult = 1
	case arrow.Microsecond:
		mult = 1e3
	case arrow.Millisecond:
		mult = 1e6
	case arrow.Second:
		mult = 1e9
	default:
		return out, fmt.Errorf("unsupported time unit %s", tt.Unit)
	}
	out = grow(out, n)
	for r := 0; r < n; r++ {
		out[r] = int64(a.Value(r)) * mult
	}
	return out, nil
}

func typeErr(arr arrow.Array) error {
	return fmt.Errorf("unexpected arrow type %s", strings.TrimPrefix(arr.DataType().String(), "*"))
}

func grow[T any](s []T, n int) []T {
	if cap(s) < n {
		return make([]T, n)
	}
	return s[:n]
}
