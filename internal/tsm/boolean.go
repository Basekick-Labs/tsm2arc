package tsm

import "encoding/binary"

// Boolean block decoding, ported from InfluxData tsdb/engine/tsm1
// (BooleanArrayDecodeAll). Layout: byte0 high 4 bits = encoding (only 1 defined,
// "bit-packed"). Then a uvarint count of booleans, then ceil(count/8) bytes of
// bits, MSB-first within each byte.

const boolCompressed = 1

func decodeBooleans(b []byte) ([]bool, error) {
	if len(b) == 0 {
		return nil, nil
	}
	enc := b[0] >> 4
	if enc != boolCompressed {
		return nil, errBadBoolEnc
	}
	b = b[1:]
	count, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, errBadBoolEnc
	}
	b = b[n:]
	// Clamp count to the bits actually available before allocating, matching
	// upstream (batch_boolean.go). The uvarint count is attacker/corruption-
	// controlled (up to 2^64-1); without this clamp a crafted or bit-flipped
	// block triggers a huge allocation before the per-bit bounds check below.
	if maxN := uint64(len(b)) * 8; count > maxN {
		count = maxN
	}
	out := make([]bool, count)
	for i := uint64(0); i < count; i++ {
		byteIdx := i / 8
		if int(byteIdx) >= len(b) {
			return nil, errShortBuffer
		}
		bitIdx := 7 - (i % 8) // MSB-first
		out[i] = (b[byteIdx]>>uint(bitIdx))&1 == 1
	}
	return out, nil
}
