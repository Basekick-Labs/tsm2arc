// Package wal reads InfluxDB 1.x write-ahead-log (.wal) segment files directly
// from disk, decoding write entries into per-key Values that match the tsm
// package's representation. Cold/unmounted InfluxDB volumes routinely have data
// that was never flushed to TSM (InfluxDB does not snapshot the cache to TSM on
// shutdown for small/idle shards), so a complete migration must read the WAL.
//
// Segment framing (influxdata tsm1 WALSegmentReader):
//
//	repeated: entryType(1) | length(4, BE) | snappy-compressed payload(length)
//
// We only decode WriteWALEntry (type 0x01). Delete / DeleteRange entries
// (0x02 / 0x03) are skipped — they represent tombstones, and reconstructing
// "this series was deleted" against a fresh Arc target is meaningless for a
// one-shot migration of the surviving data.
//
// Write-entry payload (after snappy decode), repeated per key:
//
//	valueType(1) | keyLen(2, BE) | key | nvals(4, BE) | nvals × (ts(8, BE) | value)
//
// where value is: float64/int64/uint64 = 8 bytes; boolean = 1 byte; string =
// len(4, BE) + bytes. Values are stored RAW (not the TSM block codecs).
package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"

	"github.com/golang/snappy"

	"github.com/basekick-labs/tsm2arc/internal/tsm"
)

// WAL entry types (segment framing).
const (
	writeEntryType       = 0x01
	deleteEntryType      = 0x02
	deleteRangeEntryType = 0x03
)

// Per-key value types inside a write entry.
const (
	floatValueType    = 1
	integerValueType  = 2
	booleanValueType  = 3
	stringValueType   = 4
	unsignedValueType = 5
)

var errCorrupt = errors.New("wal: corrupt entry")

// maxWALEntrySize bounds a single (compressed) WAL frame before allocation.
// InfluxDB's default WAL segment is ~10 MB and individual entries are far
// smaller; 256 MB is comfortably above any real entry while preventing a
// corrupt 4 GB length field from triggering a huge allocation.
const maxWALEntrySize = 256 << 20

// ReadFile decodes a single .wal segment file, invoking fn for every (key,
// values) pair found in write entries, in the order they appear in the file.
// Keys use the same measurement,tags#!~#field encoding as TSM. Delete entries
// are skipped. A corrupt tail (truncated final entry, common in a WAL that was
// never cleanly closed) stops reading without erroring — everything decoded up
// to that point is delivered.
func ReadFile(path string, fn func(key string, vals []tsm.Value)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return read(f, fn)
}

func read(r io.Reader, fn func(key string, vals []tsm.Value)) error {
	var hdr [5]byte
	for {
		_, err := io.ReadFull(r, hdr[:])
		if err == io.EOF {
			return nil // clean end
		}
		if err == io.ErrUnexpectedEOF {
			return nil // truncated header on an unclosed WAL — stop cleanly
		}
		if err != nil {
			return err
		}
		entryType := hdr[0]
		length := binary.BigEndian.Uint32(hdr[1:5])

		// Bound the declared frame length before allocating. The field is a
		// uint32 (up to 4 GB); a corrupt/bit-flipped header would otherwise
		// allocate that much before the read fails. Real WAL entries are far
		// below maxWALEntrySize; an over-cap length means corruption — stop
		// cleanly with everything decoded so far (same as a torn tail).
		if length > maxWALEntrySize {
			return nil
		}

		comp := make([]byte, length)
		if _, err := io.ReadFull(r, comp); err != nil {
			// truncated payload on an unclosed WAL — stop cleanly with what we have
			if err == io.ErrUnexpectedEOF || err == io.EOF {
				return nil
			}
			return err
		}

		// Only write entries carry data we migrate.
		if entryType != writeEntryType {
			if entryType != deleteEntryType && entryType != deleteRangeEntryType {
				// unknown type — be conservative and stop rather than misparse
				return errCorrupt
			}
			continue
		}

		data, err := snappy.Decode(nil, comp)
		if err != nil {
			// a corrupt compressed block at the tail — stop cleanly
			return nil
		}
		if err := decodeWriteEntry(data, fn); err != nil {
			return err
		}
	}
}

// decodeWriteEntry parses one decompressed write-entry payload.
func decodeWriteEntry(b []byte, fn func(key string, vals []tsm.Value)) error {
	i := 0
	for i < len(b) {
		if i+1 > len(b) {
			return errCorrupt
		}
		typ := b[i]
		i++

		if i+2 > len(b) {
			return errCorrupt
		}
		keyLen := int(binary.BigEndian.Uint16(b[i : i+2]))
		i += 2
		if i+keyLen > len(b) {
			return errCorrupt
		}
		key := string(b[i : i+keyLen])
		i += keyLen

		if i+4 > len(b) {
			return errCorrupt
		}
		nvals := int(binary.BigEndian.Uint32(b[i : i+4]))
		i += 4
		if nvals < 0 {
			return errCorrupt
		}

		vals := make([]tsm.Value, 0, nvals)
		for j := 0; j < nvals; j++ {
			if i+8 > len(b) {
				return errCorrupt
			}
			ts := int64(binary.BigEndian.Uint64(b[i : i+8]))
			i += 8

			switch typ {
			case floatValueType:
				if i+8 > len(b) {
					return errCorrupt
				}
				v := math.Float64frombits(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				vals = append(vals, tsm.Value{UnixNano: ts, Type: tsm.BlockFloat, Float: v})
			case integerValueType:
				if i+8 > len(b) {
					return errCorrupt
				}
				v := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				vals = append(vals, tsm.Value{UnixNano: ts, Type: tsm.BlockInteger, Integer: v})
			case unsignedValueType:
				if i+8 > len(b) {
					return errCorrupt
				}
				v := binary.BigEndian.Uint64(b[i : i+8])
				i += 8
				vals = append(vals, tsm.Value{UnixNano: ts, Type: tsm.BlockUnsigned, Unsigned: v})
			case booleanValueType:
				if i+1 > len(b) {
					return errCorrupt
				}
				v := b[i] == 1
				i++
				vals = append(vals, tsm.Value{UnixNano: ts, Type: tsm.BlockBoolean, Boolean: v})
			case stringValueType:
				if i+4 > len(b) {
					return errCorrupt
				}
				slen := int(binary.BigEndian.Uint32(b[i : i+4]))
				i += 4
				if i+slen > len(b) {
					return errCorrupt
				}
				v := string(b[i : i+slen])
				i += slen
				vals = append(vals, tsm.Value{UnixNano: ts, Type: tsm.BlockString, String: v})
			default:
				return errCorrupt
			}
		}
		fn(key, vals)
	}
	return nil
}
