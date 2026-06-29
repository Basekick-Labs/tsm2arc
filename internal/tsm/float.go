package tsm

import "math"

// Float block decoding — Gorilla XOR compression, ported from InfluxData
// tsdb/engine/tsm1 (FloatDecoder / FloatArrayDecodeAll), which itself follows
// the Facebook Gorilla paper.
//
// Layout: byte0 = encoding (only 1 = "compressed gorilla" is defined). The rest
// is a bitstream: the first value is stored verbatim (64 bits). Each subsequent
// value is XORed with its predecessor; if the XOR is zero the value repeats
// (single 0 control bit). Otherwise a control bit 1 is followed by either a
// reuse of the previous leading/trailing-zero window (control bit 0) or a new
// window (control bit 1) with 5 bits leading + 6 bits meaningful length.

const floatCompressed = 1

// uvnan is the bit pattern the InfluxData float encoder writes as the
// end-of-stream sentinel (the uint64 form of math.NaN() it uses). When a
// reconstructed value equals this, the stream is finished — trailing bits are
// just padding. (See influxdata FloatDecoder: const uvnan = 0x7FF8000000000001.)
const uvnan = 0x7FF8000000000001

func decodeFloats(b []byte) ([]float64, error) {
	if len(b) == 0 {
		return nil, nil
	}
	enc := b[0] >> 4
	if enc != floatCompressed {
		return nil, errBadFloatEnc
	}
	br := newBitReader(b[1:])

	// first value: 64 raw bits. If it is the sentinel, the block is empty.
	val, err := br.readBits(64)
	if err != nil {
		return nil, err
	}
	if val == uvnan {
		return nil, nil
	}
	out := []float64{math.Float64frombits(val)}

	leading := uint64(0)
	trailing := uint64(0)

	for {
		bit, err := br.readBit()
		if err != nil {
			// ran out of bits without hitting the sentinel — clean end (the
			// encoder always writes the sentinel, but tolerate truncation).
			break
		}
		if bit == 0 {
			// value identical to previous
			out = append(out, math.Float64frombits(val))
			continue
		}
		// control bit 1: there is an XOR delta
		ctrl, err := br.readBit()
		if err != nil {
			return nil, err
		}
		if ctrl == 1 {
			// new leading/meaningful window
			l, err := br.readBits(5)
			if err != nil {
				return nil, err
			}
			m, err := br.readBits(6)
			if err != nil {
				return nil, err
			}
			leading = l
			mbits := m
			if mbits == 0 { // 0 means 64 (overflow), per encoder
				mbits = 64
			}
			trailing = 64 - leading - mbits
		}
		// reuse window when ctrl == 0
		mbits := uint(64 - leading - trailing)
		v, err := br.readBits(int(mbits))
		if err != nil {
			return nil, err
		}
		// reconstruct: shift meaningful bits back into place, XOR with prev
		val ^= v << trailing
		if val == uvnan {
			break // end-of-stream sentinel
		}
		out = append(out, math.Float64frombits(val))
	}
	return out, nil
}

// bitReader reads MSB-first from a byte slice, matching the InfluxData
// bitstream writer.
type bitReader struct {
	data  []byte
	pos   int // byte index
	count uint8
	cache byte
}

func newBitReader(b []byte) *bitReader {
	br := &bitReader{data: b}
	return br
}

func (r *bitReader) readBit() (uint64, error) {
	if r.count == 0 {
		if r.pos >= len(r.data) {
			return 0, errShortBuffer
		}
		r.cache = r.data[r.pos]
		r.pos++
		r.count = 8
	}
	r.count--
	return uint64((r.cache >> r.count) & 1), nil
}

func (r *bitReader) readBits(n int) (uint64, error) {
	var v uint64
	for i := 0; i < n; i++ {
		b, err := r.readBit()
		if err != nil {
			return 0, err
		}
		v = (v << 1) | b
	}
	return v, nil
}
