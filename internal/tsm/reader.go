package tsm

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"time"
)

// Index is one TSM file's parsed block index, decoupled from the file handle so
// it can be parsed ONCE and reused across many re-opens of the same file — a
// shard extraction re-opens each file once per (series, run), and re-parsing
// the ENTIRE index every time (all series' keys, not just the one being read)
// was measured as a series×files CPU cost on wide shards.
//
// IMMUTABILITY CONTRACT: an Index is never written after parse. Accessors must
// keep it that way — Keys() returns a fresh slice precisely so callers can sort
// their copy without corrupting a cached Index shared across readers (and, in
// the future, across concurrent extraction tasks). Do not "optimize" an
// accessor into returning internal state that a caller might mutate.
type Index struct {
	keys    []keyBlocks // sorted by key, file order within a key
	size    int64       // file size the index was parsed from
	modTime time.Time   // file mtime at parse — staleness guard together with size
	approx  int64       // estimated in-memory footprint (see parseIndex)
}

// ApproxBytes estimates the Index's resident memory, for cache budgeting.
func (ix *Index) ApproxBytes() int64 { return ix.approx }

// Reader reads a single TSM file. Open parses the index; ReadAll decodes every
// block into per-key value slices. For TB-scale migration we read one shard at a
// time but a whole file's index fits comfortably in memory.
type Reader struct {
	f  *os.File
	ix *Index
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
	ix, err := parseIndex(f, st.Size(), st.ModTime())
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Reader{f: f, ix: ix}, nil
}

// OpenWithIndex opens a TSM file reusing a previously parsed Index, skipping
// the parse entirely. The file must be the same one the Index came from: size
// and mtime are verified, and a mismatch fails loudly — extraction order (and
// therefore resume) is only meaningful against unchanged source data.
func OpenWithIndex(path string, ix *Index) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() != ix.size || !st.ModTime().Equal(ix.modTime) {
		f.Close()
		return nil, fmt.Errorf("tsm: %s changed since its index was parsed (size %d→%d, mtime %s→%s): source data must not change during a migration",
			path, ix.size, st.Size(), ix.modTime.Format(time.RFC3339Nano), st.ModTime().Format(time.RFC3339Nano))
	}
	return &Reader{f: f, ix: ix}, nil
}

// Index returns the file's parsed index for reuse via OpenWithIndex. The Index
// is immutable and remains valid after Close.
func (r *Reader) Index() *Index { return r.ix }

func (r *Reader) Close() error { return r.f.Close() }

// Keys returns the series+field keys present in the file, sorted. The slice is
// freshly allocated on every call — callers may reorder it freely (and do; see
// the Index immutability contract).
func (r *Reader) Keys() []string {
	out := make([]string, len(r.ix.keys))
	for i, k := range r.ix.keys {
		out[i] = k.Key
	}
	return out
}

// parseIndex reads and parses a TSM file's index region into an immutable
// Index.
func parseIndex(f *os.File, size int64, modTime time.Time) (*Index, error) {
	if size < headerSize+footerSize {
		return nil, errBadBlock
	}
	// header
	hdr := make([]byte, headerSize)
	if _, err := f.ReadAt(hdr, 0); err != nil {
		return nil, err
	}
	if binary.BigEndian.Uint32(hdr[:4]) != magic {
		return nil, errBadMagic
	}
	if hdr[4] != version {
		return nil, fmt.Errorf("%w: got 0x%02x", errBadVersion, hdr[4])
	}
	// footer = index offset
	foot := make([]byte, footerSize)
	if _, err := f.ReadAt(foot, size-footerSize); err != nil {
		return nil, err
	}
	indexOffset := int64(binary.BigEndian.Uint64(foot))
	if indexOffset < headerSize || indexOffset > size-footerSize {
		return nil, fmt.Errorf("%w: bad index offset %d", errBadBlock, indexOffset)
	}

	// read the whole index region
	indexLen := size - footerSize - indexOffset
	idx := make([]byte, indexLen)
	if _, err := f.ReadAt(idx, indexOffset); err != nil {
		return nil, err
	}

	ix := &Index{size: size, modTime: modTime}

	// parse index entries:
	//   keyLen(2) key(keyLen) type(1) count(2)
	//   count × [ minTime(8) maxTime(8) offset(8) size(4) ]
	p := 0
	for p < len(idx) {
		if p+2 > len(idx) {
			return nil, errBadBlock
		}
		keyLen := int(binary.BigEndian.Uint16(idx[p:]))
		p += 2
		if p+keyLen+3 > len(idx) {
			return nil, errBadBlock
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
				return nil, errBadBlock
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
		ix.keys = append(ix.keys, kb)
	}

	sort.Slice(ix.keys, func(i, j int) bool { return ix.keys[i].Key < ix.keys[j].Key })

	// Footprint estimate for cache budgeting: the parsed form is roughly the
	// on-disk index (key bytes + 28B/entry, already counted by indexLen) with
	// slightly wider entries, plus per-key struct/slice/string overhead.
	ix.approx = indexLen*2 + int64(len(ix.keys))*96
	return ix, nil
}

// ReadKey decodes all blocks for one key into a flat, time-ordered slice of
// Values. Blocks within a key are already non-overlapping and ascending by the
// index ordering; we concatenate in entry order.
//
// MEMORY: this materializes every value of the key at once. A decoded Value is
// 64 bytes against ~2-8 compressed bytes on disk, so a multi-GB key costs tens
// of GB here. Prefer Blocks + ReadBlockAt, which decode one block at a time; the
// extractor uses those and calls ReadKey only as a fallback for the rare file
// whose blocks are not ascending.
func (r *Reader) ReadKey(kb keyBlocks) ([]Value, error) {
	var out []Value
	for _, e := range kb.Entries {
		vals, err := r.readEntry(kb.Type, kb.Key, e)
		if err != nil {
			return nil, err
		}
		out = append(out, vals...)
	}
	return out, nil
}

// readEntry reads and decodes exactly one block.
func (r *Reader) readEntry(typ byte, key string, e IndexEntry) ([]Value, error) {
	// Bound the declared block size against the file before allocating: a
	// corrupt/crafted index entry can claim up to 4 GB (uint32), which would
	// otherwise allocate before ReadAt fails. Offset+Size must fit in the file.
	if e.Offset < 0 || e.Offset+int64(e.Size) > r.ix.size {
		return nil, fmt.Errorf("%w: block at off=%d size=%d exceeds file size %d",
			errBadBlock, e.Offset, e.Size, r.ix.size)
	}
	buf := make([]byte, e.Size)
	if _, err := r.f.ReadAt(buf, e.Offset); err != nil {
		return nil, err
	}
	vals, err := decodeBlock(typ, buf)
	if err != nil {
		return nil, fmt.Errorf("decode block for key %q: %w", key, err)
	}
	return vals, nil
}

// search returns the index of key in r.ix.keys, or -1 if absent.
func (r *Reader) search(key string) int {
	i := sort.Search(len(r.ix.keys), func(i int) bool { return r.ix.keys[i].Key >= key })
	if i >= len(r.ix.keys) || r.ix.keys[i].Key != key {
		return -1
	}
	return i
}

// Blocks returns the index entries for key, in file order, or nil if the key is
// not present. Each entry carries the block's [MinTime,MaxTime], so a caller can
// skip blocks outside a time window WITHOUT reading or decoding them. The slice
// is owned by the Reader and must not be mutated.
func (r *Reader) Blocks(key string) []IndexEntry {
	i := r.search(key)
	if i < 0 {
		return nil
	}
	return r.ix.keys[i].Entries
}

// ReadBlockAt decodes one block of key, identified by an entry from Blocks.
// This is the bounded-memory read path: peak allocation is one block (a TSM
// block holds at most 1000 values) rather than the whole key.
func (r *Reader) ReadBlockAt(key string, e IndexEntry) ([]Value, error) {
	i := r.search(key)
	if i < 0 {
		return nil, nil
	}
	return r.readEntry(r.ix.keys[i].Type, key, e)
}

// ReadKeyByName decodes all blocks for the named key. Returns nil if the key is
// not present in this file. See ReadKey on memory.
func (r *Reader) ReadKeyByName(key string) ([]Value, error) {
	i := r.search(key)
	if i < 0 {
		return nil, nil
	}
	return r.ReadKey(r.ix.keys[i])
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
