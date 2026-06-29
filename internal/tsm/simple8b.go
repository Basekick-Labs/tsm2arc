package tsm

// simple8b decoding, ported from the InfluxData encoding/simple8b package.
//
// simple8b packs multiple unsigned integers into a single 64-bit word. The top
// 4 bits ("selector") choose one of 16 packings; the low 60 bits hold the
// values. Selectors 0 and 1 are special "run of N ones/zeros" forms used to
// represent long runs of identical small deltas cheaply.
//
// We only need the DECODE path. This is used by the integer and timestamp
// block decoders.

// selector table: for each selector value, how many ints are packed (n) and how
// many bits each occupies (bits). Selectors 0 and 1 encode 240 and 120 zero-bit
// values respectively (runs of zero).
var selector = [16]struct {
	n    int
	bits int
}{
	{240, 0},
	{120, 0},
	{60, 1},
	{30, 2},
	{20, 3},
	{15, 4},
	{12, 5},
	{10, 6},
	{8, 7},
	{7, 8},
	{6, 10},
	{5, 12},
	{4, 15},
	{3, 20},
	{2, 30},
	{1, 60},
}

// simple8bDecodeAll decodes every packed value in src (a slice of 64-bit
// big-endian-free words already read as uint64) into dst, returning the number
// of values written. dst must be large enough; callers size it from the block.
func simple8bDecodeAll(dst []uint64, src []uint64) (int, error) {
	j := 0
	for _, v := range src {
		sel := v >> 60
		s := selector[sel]
		if s.bits == 0 {
			// run of s.n zeros
			for i := 0; i < s.n; i++ {
				if j >= len(dst) {
					return j, errShortBuffer
				}
				dst[j] = 0
				j++
			}
			continue
		}
		mask := uint64(1)<<uint(s.bits) - 1
		for i := 0; i < s.n; i++ {
			if j >= len(dst) {
				return j, errShortBuffer
			}
			dst[j] = (v >> uint(i*s.bits)) & mask
			j++
		}
	}
	return j, nil
}

// simple8bCount returns how many values are encoded by a single packed word.
func simple8bCount(v uint64) int {
	return selector[v>>60].n
}
