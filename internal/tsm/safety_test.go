package tsm

import (
	"encoding/binary"
	"testing"
)

// H1 regression: a boolean block whose declared count is enormous must not
// allocate that much before bounds-checking. We hand it a tiny block with a
// huge uvarint count and assert it returns quickly with at most len(b)*8 values
// (clamped), not OOM.
func TestBooleanDecodeClampsHugeCount(t *testing.T) {
	// encoding byte (enc=1 in high nibble) + uvarint(huge) + 1 data byte
	var b []byte
	b = append(b, boolCompressed<<4)
	var uv [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(uv[:], 1<<62) // absurd count
	b = append(b, uv[:n]...)
	b = append(b, 0xFF) // 1 data byte → at most 8 bools available

	out, err := decodeBooleans(b)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) > 8 {
		t.Fatalf("decoded %d bools from 1 data byte — count not clamped", len(out))
	}
}

// H2 regression: a TSM index entry that declares a block extending past the end
// of the file must be rejected before allocating e.Size bytes.
func TestReadKeyRejectsOversizedBlock(t *testing.T) {
	r := &Reader{
		size: 100, // pretend the file is 100 bytes
	}
	kb := keyBlocks{
		Key:  "m#!~#f",
		Type: BlockFloat,
		Entries: []IndexEntry{
			{Offset: 10, Size: 0xFFFFFFFF}, // 4 GB block in a 100-byte file
		},
	}
	_, err := r.ReadKey(kb)
	if err == nil {
		t.Fatal("expected error for block exceeding file size, got nil")
	}
}
