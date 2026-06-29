package tsm

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
)

// Reader reads a single TSM file. Open parses the index; ReadAll decodes every
// block into per-key value slices. For TB-scale migration we read one shard at a
// time but a whole file's index fits comfortably in memory.
type Reader struct {
	f    *os.File
	size int64
	keys []keyBlocks // sorted by key, file order within a key
}

// Open opens a TSM file and parses its index. The file is kept open for block
// reads; call Close when done.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	r := &Reader{f: f, size: st.Size()}
	if err := r.readIndex(); err != nil {
		f.Close()
		return nil, err
	}
	return r, nil
}

func (r *Reader) Close() error { return r.f.Close() }

// Keys returns the series+field keys present in the file, sorted.
func (r *Reader) Keys() []string {
	out := make([]string, len(r.keys))
	for i, k := range r.keys {
		out[i] = k.Key
	}
	return out
}

func (r *Reader) readIndex() error {
	if r.size < headerSize+footerSize {
		return errBadBlock
	}
	// header
	hdr := make([]byte, headerSize)
	if _, err := r.f.ReadAt(hdr, 0); err != nil {
		return err
	}
	if binary.BigEndian.Uint32(hdr[:4]) != magic {
		return errBadMagic
	}
	if hdr[4] != version {
		return fmt.Errorf("%w: got 0x%02x", errBadVersion, hdr[4])
	}
	// footer = index offset
	foot := make([]byte, footerSize)
	if _, err := r.f.ReadAt(foot, r.size-footerSize); err != nil {
		return err
	}
	indexOffset := int64(binary.BigEndian.Uint64(foot))
	if indexOffset < headerSize || indexOffset > r.size-footerSize {
		return fmt.Errorf("%w: bad index offset %d", errBadBlock, indexOffset)
	}

	// read the whole index region
	indexLen := r.size - footerSize - indexOffset
	idx := make([]byte, indexLen)
	if _, err := r.f.ReadAt(idx, indexOffset); err != nil {
		return err
	}

	// parse index entries:
	//   keyLen(2) key(keyLen) type(1) count(2)
	//   count × [ minTime(8) maxTime(8) offset(8) size(4) ]
	p := 0
	for p < len(idx) {
		if p+2 > len(idx) {
			return errBadBlock
		}
		keyLen := int(binary.BigEndian.Uint16(idx[p:]))
		p += 2
		if p+keyLen+3 > len(idx) {
			return errBadBlock
		}
		key := string(idx[p : p+keyLen])
		p += keyLen
		typ := idx[p]
		p++
		count := int(binary.BigEndian.Uint16(idx[p:]))
		p += 2

		kb := keyBlocks{Key: key, Type: typ, Entries: make([]IndexEntry, 0, count)}
		for i := 0; i < count; i++ {
			if p+indexEntrySize > len(idx) {
				return errBadBlock
			}
			e := IndexEntry{
				MinTime: int64(binary.BigEndian.Uint64(idx[p:])),
				MaxTime: int64(binary.BigEndian.Uint64(idx[p+8:])),
				Offset:  int64(binary.BigEndian.Uint64(idx[p+16:])),
				Size:    binary.BigEndian.Uint32(idx[p+24:]),
			}
			p += indexEntrySize
			kb.Entries = append(kb.Entries, e)
		}
		r.keys = append(r.keys, kb)
	}

	sort.Slice(r.keys, func(i, j int) bool { return r.keys[i].Key < r.keys[j].Key })
	return nil
}

// ReadKey decodes all blocks for one key into a flat, time-ordered slice of
// Values. Blocks within a key are already non-overlapping and ascending by the
// index ordering; we concatenate in entry order.
func (r *Reader) ReadKey(kb keyBlocks) ([]Value, error) {
	var out []Value
	for _, e := range kb.Entries {
		// Bound the declared block size against the file before allocating: a
		// corrupt/crafted index entry can claim up to 4 GB (uint32), which would
		// otherwise allocate before ReadAt fails. Offset+Size must fit in the file.
		if e.Offset < 0 || e.Offset+int64(e.Size) > r.size {
			return nil, fmt.Errorf("%w: block at off=%d size=%d exceeds file size %d",
				errBadBlock, e.Offset, e.Size, r.size)
		}
		buf := make([]byte, e.Size)
		if _, err := r.f.ReadAt(buf, e.Offset); err != nil {
			return nil, err
		}
		vals, err := decodeBlock(kb.Type, buf)
		if err != nil {
			return nil, fmt.Errorf("decode block for key %q: %w", kb.Key, err)
		}
		out = append(out, vals...)
	}
	return out, nil
}

// ReadKeyByName decodes all blocks for the named key. Returns nil if the key is
// not present in this file.
func (r *Reader) ReadKeyByName(key string) ([]Value, error) {
	// keys are sorted; binary search
	i := sort.Search(len(r.keys), func(i int) bool { return r.keys[i].Key >= key })
	if i >= len(r.keys) || r.keys[i].Key != key {
		return nil, nil
	}
	return r.ReadKey(r.keys[i])
}

// decodeBlock splits a raw block (crc32 prefix already included) into its
// timestamp and value sections and decodes both, zipping into Values.
//
// Block layout after the 4-byte CRC:
//
//	blockType(1) | tsLen(uvarint) | tsBytes | valueBytes
//
// where the timestamp section and value section each begin with their own
// encoding byte (handled by the per-codec decoders).
func decodeBlock(typ byte, raw []byte) ([]Value, error) {
	if len(raw) < 4+1 {
		return nil, errBadBlock
	}
	// raw[:4] is CRC32 of the rest; we trust ReadAt + index sizes and skip
	// verification for throughput (can be enabled behind a flag later).
	body := raw[4:]

	blockType := body[0]
	if blockType != typ {
		// index type and block type should agree; trust the block's own byte
		typ = blockType
	}
	body = body[1:]

	tsLen, n := binary.Uvarint(body)
	if n <= 0 || uint64(len(body)) < uint64(n)+tsLen {
		return nil, errBadBlock
	}
	body = body[n:]
	tsBytes := body[:tsLen]
	valBytes := body[tsLen:]

	times, err := decodeTimestamps(tsBytes)
	if err != nil {
		return nil, err
	}

	switch typ {
	case BlockFloat:
		vals, err := decodeFloats(valBytes)
		if err != nil {
			return nil, err
		}
		return zipFloat(times, vals)
	case BlockInteger:
		vals, err := decodeIntegers(valBytes)
		if err != nil {
			return nil, err
		}
		return zipInteger(times, vals)
	case BlockUnsigned:
		vals, err := decodeIntegers(valBytes)
		if err != nil {
			return nil, err
		}
		return zipUnsigned(times, vals)
	case BlockBoolean:
		vals, err := decodeBooleans(valBytes)
		if err != nil {
			return nil, err
		}
		return zipBoolean(times, vals)
	case BlockString:
		vals, err := decodeStrings(valBytes)
		if err != nil {
			return nil, err
		}
		return zipString(times, vals)
	default:
		return nil, errUnknownType
	}
}

func zipLenCheck(nt, nv int) error {
	if nt != nv {
		return fmt.Errorf("%w: %d timestamps vs %d values", errBadBlock, nt, nv)
	}
	return nil
}

func zipFloat(t []int64, v []float64) ([]Value, error) {
	if err := zipLenCheck(len(t), len(v)); err != nil {
		return nil, err
	}
	out := make([]Value, len(t))
	for i := range t {
		out[i] = Value{UnixNano: t[i], Type: BlockFloat, Float: v[i]}
	}
	return out, nil
}
func zipInteger(t []int64, v []int64) ([]Value, error) {
	if err := zipLenCheck(len(t), len(v)); err != nil {
		return nil, err
	}
	out := make([]Value, len(t))
	for i := range t {
		out[i] = Value{UnixNano: t[i], Type: BlockInteger, Integer: v[i]}
	}
	return out, nil
}
func zipUnsigned(t []int64, v []int64) ([]Value, error) {
	if err := zipLenCheck(len(t), len(v)); err != nil {
		return nil, err
	}
	out := make([]Value, len(t))
	for i := range t {
		out[i] = Value{UnixNano: t[i], Type: BlockUnsigned, Unsigned: uint64(v[i])}
	}
	return out, nil
}
func zipBoolean(t []int64, v []bool) ([]Value, error) {
	if err := zipLenCheck(len(t), len(v)); err != nil {
		return nil, err
	}
	out := make([]Value, len(t))
	for i := range t {
		out[i] = Value{UnixNano: t[i], Type: BlockBoolean, Boolean: v[i]}
	}
	return out, nil
}
func zipString(t []int64, v []string) ([]Value, error) {
	if err := zipLenCheck(len(t), len(v)); err != nil {
		return nil, err
	}
	out := make([]Value, len(t))
	for i := range t {
		out[i] = Value{UnixNano: t[i], Type: BlockString, String: v[i]}
	}
	return out, nil
}
