package extract

import (
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/basekick-labs/tsm2arc/internal/tsm"
)

// fakeReader implements the tsmFile interface with in-memory data, so we can
// test field-rejoin and LP output without a real TSM file.
type fakeReader struct {
	data map[string][]tsm.Value
}

func (f *fakeReader) Keys() []string {
	ks := make([]string, 0, len(f.data))
	for k := range f.data {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}
func (f *fakeReader) ReadKeyByName(k string) ([]tsm.Value, error) { return f.data[k], nil }
func (f *fakeReader) Close() error                                { return nil }

func TestFieldRejoin(t *testing.T) {
	// cpu series with two fields at two timestamps → 2 multi-field points.
	r := &fakeReader{data: map[string][]tsm.Value{
		"cpu,host=node-a#!~#usage": {
			{UnixNano: 1700000000000000000, Type: tsm.BlockFloat, Float: 98.5},
			{UnixNano: 1700000001000000000, Type: tsm.BlockFloat, Float: 97.2},
		},
		"cpu,host=node-a#!~#cores": {
			{UnixNano: 1700000000000000000, Type: tsm.BlockInteger, Integer: 8},
			{UnixNano: 1700000001000000000, Type: tsm.BlockInteger, Integer: 8},
		},
	}}

	var lines []string
	st, err := File(r, math.MinInt64, math.MaxInt64, func(p Point) {
		lines = append(lines, strings.TrimRight(EncodePoint(p), "\n"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.Points != 2 {
		t.Fatalf("points: got %d want 2", st.Points)
	}
	want := []string{
		"cpu,host=node-a cores=8i,usage=98.5 1700000000000000000",
		"cpu,host=node-a cores=8i,usage=97.2 1700000001000000000",
	}
	if len(lines) != len(want) {
		t.Fatalf("lines: got %d want %d: %v", len(lines), len(want), lines)
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Errorf("[%d]\n got %q\nwant %q", i, lines[i], want[i])
		}
	}
}

func TestEncodeAllTypesAndPreEpoch(t *testing.T) {
	r := &fakeReader{data: map[string][]tsm.Value{
		"m,t=x#!~#f_float":  {{UnixNano: 1, Type: tsm.BlockFloat, Float: 1.5}},
		"m,t=x#!~#g_int":    {{UnixNano: 1, Type: tsm.BlockInteger, Integer: -7}},
		"m,t=x#!~#h_uint":   {{UnixNano: 1, Type: tsm.BlockUnsigned, Unsigned: 42}},
		"m,t=x#!~#i_bool":   {{UnixNano: 1, Type: tsm.BlockBoolean, Boolean: true}},
		"m,t=x#!~#j_string": {{UnixNano: 1, Type: tsm.BlockString, String: `a "b" c`}},
		// tagless measurement at a pre-epoch timestamp
		"pressure#!~#value": {{UnixNano: -317001600000000000, Type: tsm.BlockFloat, Float: 1013.25}},
	}}

	got := map[string]string{}
	_, err := File(r, math.MinInt64, math.MaxInt64, func(p Point) {
		got[p.Measurement] = strings.TrimRight(EncodePoint(p), "\n")
	})
	if err != nil {
		t.Fatal(err)
	}

	wantM := "m,t=x f_float=1.5,g_int=-7i,h_uint=42u,i_bool=true,j_string=\"a \\\"b\\\" c\" 1"
	if got["m"] != wantM {
		t.Errorf("multi-type:\n got %q\nwant %q", got["m"], wantM)
	}
	wantP := "pressure value=1013.25 -317001600000000000"
	if got["pressure"] != wantP {
		t.Errorf("tagless/pre-epoch:\n got %q\nwant %q", got["pressure"], wantP)
	}
}

func TestTimeFilter(t *testing.T) {
	r := &fakeReader{data: map[string][]tsm.Value{
		"m#!~#v": {
			{UnixNano: 100, Type: tsm.BlockFloat, Float: 1},
			{UnixNano: 200, Type: tsm.BlockFloat, Float: 2},
			{UnixNano: 300, Type: tsm.BlockFloat, Float: 3},
		},
	}}
	st, err := File(r, 150, 250, nil)
	if err != nil {
		t.Fatal(err)
	}
	if st.Points != 1 {
		t.Errorf("filter: got %d points want 1", st.Points)
	}
}
