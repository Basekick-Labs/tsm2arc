package tsm

import "encoding/binary"

// Integer & unsigned block decoding, ported from InfluxData tsdb/engine/tsm1
// (IntegerArrayDecodeAll). Both signed and unsigned use the same on-disk codec;
// signed values are zig-zag encoded so they pack as small unsigned deltas. The
// only difference at the boundary is how we present the result.
//
// Layout: byte0 high 4 bits = encoding (0 uncompressed, 1 simple8b packed, 2 RLE).
// Values are stored as zig-zag uint64s; the simple8b/RLE forms encode them and
// the consumer zig-zag-decodes back to int64. Unsigned fields reinterpret the
// resulting int64 bit pattern as uint64.

const (
	intUncompressed = 0
	intSimple8b     = 1
	intRLE          = 2
)

// decodeIntegers returns the raw zig-zag-decoded int64 values. Callers that want
// unsigned reinterpret each as uint64.
func decodeIntegers(b []byte) ([]int64, error) {
	if len(b) == 0 {
		return nil, nil
	}
	enc := b[0] >> 4
	b = b[1:]

	// Integer values are DELTA-encoded then zig-zagged (Write: delta = v-prev;
	// ZigZagEncode(delta)). Every encoding stores zig-zag deltas and the decoder
	// prefix-sums them: value[i] = value[i-1] + ZigZagDecode(stored[i]). The
	// first stored delta is v-0 = v. RLE is the closed-form of the same thing.
	// (See influxdata IntegerDecoder.Read: v = ZigZagDecode(...) + d.prev.)
	switch enc {
	case intUncompressed:
		if len(b)%8 != 0 {
			return nil, errBadIntEnc
		}
		n := len(b) / 8
		out := make([]int64, n)
		var acc int64
		for i := 0; i < n; i++ {
			acc += zigZagDecode(binary.BigEndian.Uint64(b[i*8:]))
			out[i] = acc
		}
		return out, nil

	case intRLE:
		// first(8, zig-zag) | delta (uvarint, zig-zag) | count (uvarint)
		// value[i] = ZigZagDecode(first) + i*ZigZagDecode(delta)
		if len(b) < 8 {
			return nil, errBadIntEnc
		}
		first := zigZagDecode(binary.BigEndian.Uint64(b[:8]))
		rest := b[8:]
		dz, n1 := binary.Uvarint(rest)
		if n1 <= 0 {
			return nil, errBadIntEnc
		}
		rest = rest[n1:]
		count, n2 := binary.Uvarint(rest)
		if n2 <= 0 {
			return nil, errBadIntEnc
		}
		delta := zigZagDecode(dz)
		out := make([]int64, count+1)
		v := first
		out[0] = v
		for i := uint64(0); i < count; i++ {
			v += delta
			out[i+1] = v
		}
		return out, nil

	case intSimple8b:
		// first(8, zig-zag delta) | simple8b-packed zig-zag deltas. Prefix-sum.
		if len(b) < 8 {
			return nil, errBadIntEnc
		}
		packed := b[8:]
		if len(packed)%8 != 0 {
			return nil, errBadIntEnc
		}
		words := make([]uint64, len(packed)/8)
		total := 0
		for i := range words {
			words[i] = binary.BigEndian.Uint64(packed[i*8:])
			total += simple8bCount(words[i])
		}
		raw := make([]uint64, total)
		got, err := simple8bDecodeAll(raw, words)
		if err != nil {
			return nil, err
		}
		raw = raw[:got]

		out := make([]int64, got+1)
		acc := zigZagDecode(binary.BigEndian.Uint64(b[:8])) // first delta = first value
		out[0] = acc
		for i, z := range raw {
			acc += zigZagDecode(z)
			out[i+1] = acc
		}
		return out, nil

	default:
		return nil, errBadIntEnc
	}
}
