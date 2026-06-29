package tsm

import (
	"testing"

	isimple8b "github.com/influxdata/influxdb/pkg/encoding/simple8b"
)

// Validate our simple8b decoder against influxdata's, word by word.
func TestSimple8bAgainstInflux(t *testing.T) {
	inputs := [][]uint64{
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		{0, 0, 0, 0, 0},
		{1000000, 2000000, 3000000, 1000000},
		func() []uint64 {
			s := make([]uint64, 300)
			for i := range s {
				s[i] = uint64(i % 13)
			}
			return s
		}(),
	}
	for ti, in := range inputs {
		// encode with influx
		words, err := isimple8b.EncodeAll(append([]uint64(nil), in...))
		if err != nil {
			t.Fatalf("[%d] influx encode: %v", ti, err)
		}
		// decode with ours
		var total int
		for _, w := range words {
			total += simple8bCount(w)
		}
		dst := make([]uint64, total)
		got, err := simple8bDecodeAll(dst, words)
		if err != nil {
			t.Fatalf("[%d] our decode: %v", ti, err)
		}
		dst = dst[:got]
		if len(dst) != len(in) {
			t.Fatalf("[%d] len got %d want %d (words=%d)", ti, len(dst), len(in), len(words))
		}
		for i := range in {
			if dst[i] != in[i] {
				t.Errorf("[%d][%d] got %d want %d", ti, i, dst[i], in[i])
			}
		}
	}
}
