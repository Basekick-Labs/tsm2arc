package tsm

import "encoding/binary"

// Timestamp block decoding, ported from InfluxData tsdb/engine/tsm1
// (TimeDecoder / TimeArrayDecodeAll).
//
// Layout of a timestamp block's value section:
//   byte0 high 4 bits  = encoding (0 packed/simple8b, 1 RLE, 2 uncompressed)
//   byte0 low  4 bits  = log10 of the divisor that was factored out of the
//                        deltas before encoding (scaling for coarse precision)
//
// The encoder stores delta-of-delta (zig-zag) values; we reverse: first value
// is absolute, subsequent are running sums of decoded deltas × divisor.

const (
	timeUncompressed = 0
	timePackedSimple = 1
	timeRLE          = 2
)

// decodeTimestamps decodes a timestamp block value section into out (appended).
func decodeTimestamps(b []byte) ([]int64, error) {
	if len(b) == 0 {
		return nil, nil
	}
	enc := b[0] >> 4
	scale := pow10(b[0] & 0x0f)
	b = b[1:]

	switch enc {
	case timeUncompressed:
		// "Uncompressed" still stores prefix-summed deltas (first absolute, rest
		// are running sums). NOT scaled by the divisor. (See influxdata
		// TimeDecoder.decodeRaw.)
		if len(b)%8 != 0 {
			return nil, errBadTimeEnc
		}
		n := len(b) / 8
		out := make([]int64, n)
		var acc int64
		for i := 0; i < n; i++ {
			d := int64(binary.BigEndian.Uint64(b[i*8:]))
			if i == 0 {
				acc = d
			} else {
				acc += d
			}
			out[i] = acc
		}
		return out, nil

	case timeRLE:
		// first value (8) | delta (uvarint) | count (uvarint)
		if len(b) < 8 {
			return nil, errBadTimeEnc
		}
		first := int64(binary.BigEndian.Uint64(b[:8]))
		rest := b[8:]
		delta, n1 := binary.Uvarint(rest)
		if n1 <= 0 {
			return nil, errBadTimeEnc
		}
		rest = rest[n1:]
		count, n2 := binary.Uvarint(rest)
		if n2 <= 0 {
			return nil, errBadTimeEnc
		}
		out := make([]int64, count)
		step := int64(delta) * scale
		v := first
		for i := uint64(0); i < count; i++ {
			out[i] = v
			v += step
		}
		return out, nil

	case timePackedSimple:
		// first value (8, big-endian) | simple8b-packed deltas.
		// NOTE: timestamp deltas are NOT zig-zag encoded — timestamps are
		// monotonic so deltas are non-negative. They are prefix-summed and the
		// divisor scales each delta back up. (See influxdata TimeDecoder.decodePacked.)
		if len(b) < 8 {
			return nil, errBadTimeEnc
		}
		first := int64(binary.BigEndian.Uint64(b[:8]))
		packed := b[8:]
		if len(packed)%8 != 0 {
			return nil, errBadTimeEnc
		}
		words := make([]uint64, len(packed)/8)
		total := 0
		for i := range words {
			words[i] = binary.BigEndian.Uint64(packed[i*8:])
			total += simple8bCount(words[i])
		}
		deltas := make([]uint64, total)
		got, err := simple8bDecodeAll(deltas, words)
		if err != nil {
			return nil, err
		}
		deltas = deltas[:got]

		out := make([]int64, got+1)
		out[0] = first
		acc := first
		for i, d := range deltas {
			acc += int64(d) * scale
			out[i+1] = acc
		}
		return out, nil

	default:
		return nil, errBadTimeEnc
	}
}

func pow10(n byte) int64 {
	v := int64(1)
	for i := byte(0); i < n; i++ {
		v *= 10
	}
	return v
}

// zigZagDecode reverses zig-zag encoding (maps unsigned back to signed).
func zigZagDecode(v uint64) int64 {
	return int64(v>>1) ^ -int64(v&1)
}
