package tsm

import (
	"encoding/binary"

	"github.com/golang/snappy"
)

// String block decoding, ported from InfluxData tsdb/engine/tsm1
// (StringArrayDecodeAll). Layout: byte0 high 4 bits = encoding (only 1 defined,
// "snappy"). The remainder is snappy-compressed; once decompressed it is a
// sequence of length-prefixed strings: uvarint(len) followed by len bytes.

const stringCompressed = 1

func decodeStrings(b []byte) ([]string, error) {
	if len(b) == 0 {
		return nil, nil
	}
	enc := b[0] >> 4
	if enc != stringCompressed {
		return nil, errBadStringEnc
	}
	data, err := snappy.Decode(nil, b[1:])
	if err != nil {
		return nil, err
	}

	var out []string
	for len(data) > 0 {
		l, n := binary.Uvarint(data)
		if n <= 0 {
			return nil, errBadStringEnc
		}
		data = data[n:]
		if uint64(len(data)) < l {
			return nil, errShortBuffer
		}
		out = append(out, string(data[:l]))
		data = data[l:]
	}
	return out, nil
}
