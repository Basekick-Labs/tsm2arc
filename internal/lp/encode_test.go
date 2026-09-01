package lp

import (
	"math"
	"strings"
	"testing"

	"github.com/basekick-labs/tsm2arc/internal/tsm"
)

// AppendPoint is the load path's allocation-free encoder; EncodePoint is the
// reference. They must produce identical bytes for every value type and every
// escaping case, or resume determinism breaks between dry-run and load.
func TestAppendPointMatchesEncodePoint(t *testing.T) {
	cases := []struct {
		name        string
		measurement string
		tags        [][2]string
		fields      []Field
		ts          int64
	}{
		{"all types", "cpu", [][2]string{{"host", "a"}, {"dc", "b"}}, []Field{
			{Name: "f", Value: tsm.Value{Type: tsm.BlockFloat, Float: 3.14159}},
			{Name: "g", Value: tsm.Value{Type: tsm.BlockFloat, Float: math.MaxFloat64}},
			{Name: "h", Value: tsm.Value{Type: tsm.BlockFloat, Float: -0.0000001}},
			{Name: "i", Value: tsm.Value{Type: tsm.BlockInteger, Integer: -42}},
			{Name: "u", Value: tsm.Value{Type: tsm.BlockUnsigned, Unsigned: math.MaxUint64}},
			{Name: "bt", Value: tsm.Value{Type: tsm.BlockBoolean, Boolean: true}},
			{Name: "bf", Value: tsm.Value{Type: tsm.BlockBoolean, Boolean: false}},
			{Name: "s", Value: tsm.Value{Type: tsm.BlockString, String: `plain`}},
		}, 1700000000000000000},
		{"escaping", `my measure,ment`, [][2]string{{`k ey`, `v=al,ue`}}, []Field{
			{Name: `fi eld,=`, Value: tsm.Value{Type: tsm.BlockString, String: `quote " back \ slash`}},
		}, -631152000000000000}, // pre-epoch
		{"no tags", "m", nil, []Field{
			{Name: "v", Value: tsm.Value{Type: tsm.BlockFloat, Float: 1}},
		}, 0},
	}
	for _, c := range cases {
		var b strings.Builder
		EncodePoint(&b, c.measurement, c.tags, c.fields, c.ts)
		got := string(AppendPoint(nil, c.measurement, c.tags, c.fields, c.ts))
		if got != b.String() {
			t.Errorf("%s:\nAppendPoint %q\nEncodePoint %q", c.name, got, b.String())
		}
	}
}

func BenchmarkAppendPoint(b *testing.B) {
	tags := [][2]string{{"host", "node-a"}, {"region", "eu"}}
	fields := make([]Field, 100)
	for i := range fields {
		fields[i] = Field{Name: "field_name_" + string(rune('a'+i%26)), Value: tsm.Value{Type: tsm.BlockFloat, Float: float64(i) * 1.5}}
	}
	buf := make([]byte, 0, 1<<16)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = AppendPoint(buf[:0], "wide_measurement", tags, fields, int64(i))
	}
}
