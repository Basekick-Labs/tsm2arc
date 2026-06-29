package wal

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/basekick-labs/tsm2arc/internal/tsm"
)

// H2 regression: a WAL frame header declaring a huge length must not allocate
// that much before reading. A crafted/bit-flipped header with length > the cap
// is treated as corruption and reading stops cleanly with what came before.
func TestWALRejectsHugeFrameLength(t *testing.T) {
	var buf bytes.Buffer
	// one frame: type=write(0x01), length=0xFFFFFFFF (4 GB), no payload
	buf.WriteByte(writeEntryType)
	var l [4]byte
	binary.BigEndian.PutUint32(l[:], 0xFFFFFFFF)
	buf.Write(l[:])
	// (no payload — if the reader tried to allocate 4 GB it would OOM, not return)

	var got int
	err := read(&buf, func(key string, vals []tsm.Value) { got += len(vals) })
	if err != nil {
		t.Fatalf("expected clean stop on oversized frame, got error: %v", err)
	}
	if got != 0 {
		t.Fatalf("decoded %d values from a corrupt oversized frame", got)
	}
}
