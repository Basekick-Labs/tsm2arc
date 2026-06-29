// Package tsm reads InfluxDB 1.x TSM (Time-Structured Merge tree) files directly
// from disk, without a running influxd. It decodes the block codecs (timestamp,
// float, integer, unsigned, boolean, string) and exposes values keyed by series.
//
// Format references (stable across all InfluxDB 1.x):
//
//	file   = header | blocks | index | footer
//	header = magic(4 = 0x16D116D1) | version(1 = 0x01)
//	footer = indexOffset(8, big-endian) at EOF-8
//	index  = repeated: keyLen(2) key(keyLen) type(1) count(2)
//	                   then count × [ minTime(8) maxTime(8) offset(8) size(4) ]
//	block  = crc32(4) | data   (data layout is codec-specific)
//
// Series keys embed measurement+tags and the field name, separated by the
// field-key separator. See series.ParseKey.
package tsm

import "errors"

// TSM file constants.
const (
	magic          uint32 = 0x16D116D1
	version        byte   = 0x01
	headerSize            = 5  // 4 magic + 1 version
	footerSize            = 8  // index offset
	indexEntrySize        = 28 // minTime(8) maxTime(8) offset(8) size(4)
)

// Block type codes (first byte of a decompressed block's value section).
const (
	BlockFloat    byte = 0
	BlockInteger  byte = 1
	BlockBoolean  byte = 2
	BlockString   byte = 3
	BlockUnsigned byte = 4
)

var (
	errBadMagic     = errors.New("tsm: bad magic number (not a TSM v1 file)")
	errBadVersion   = errors.New("tsm: unsupported version")
	errShortBuffer  = errors.New("tsm: short buffer while decoding")
	errBadBlock     = errors.New("tsm: malformed block")
	errUnknownType  = errors.New("tsm: unknown block type")
	errBadTimeEnc   = errors.New("tsm: unknown timestamp encoding")
	errBadFloatEnc  = errors.New("tsm: unknown float encoding")
	errBadIntEnc    = errors.New("tsm: unknown integer encoding")
	errBadBoolEnc   = errors.New("tsm: unknown boolean encoding")
	errBadStringEnc = errors.New("tsm: unknown string encoding")
)

// Value is a single decoded point for one field: a timestamp (ns, UTC) and the
// typed value. Exactly one of the typed fields is meaningful, per Type.
type Value struct {
	UnixNano int64
	Type     byte // BlockFloat | BlockInteger | BlockUnsigned | BlockBoolean | BlockString

	Float    float64
	Integer  int64
	Unsigned uint64
	Boolean  bool
	String   string
}

// IndexEntry locates one block for one key within the file.
type IndexEntry struct {
	MinTime int64
	MaxTime int64
	Offset  int64
	Size    uint32
}

// keyBlocks is the set of blocks for a single series+field key, in file order.
type keyBlocks struct {
	Key     string
	Type    byte
	Entries []IndexEntry
}
