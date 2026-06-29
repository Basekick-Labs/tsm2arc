package tsm

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"testing"

	itsm "github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// These tests validate our pure-Go block decoders against the REAL InfluxData
// tsm1 encoder. We encode known Values with influxdata's library (the exact
// codec that wrote the customer's files), then decode with our implementation
// and assert equality. This catches bit-level codec bugs without needing a
// running InfluxDB.
//
// Values.Encode(buf) returns the block BODY: blockType(1) | tsLen(uvarint) |
// tsBytes | valueBytes — i.e. exactly what decodeBlock expects after the 4-byte
// CRC. We prepend a real CRC so the whole path (including the CRC skip) is
// exercised.

func encodeBlock(t *testing.T, vals itsm.Values) []byte {
	t.Helper()
	body, err := vals.Encode(nil)
	if err != nil {
		t.Fatalf("influx encode: %v", err)
	}
	out := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(out[:4], crc32.ChecksumIEEE(body))
	copy(out[4:], body)
	return out
}

func TestDecodeFloatBlock(t *testing.T) {
	in := []struct {
		ts int64
		v  float64
	}{
		{1700000000000000000, 98.5},
		{1700000001000000000, 97.2},
		{1700000002000000000, 0},
		{1700000003000000000, -317.125},
		{1700000004000000000, math.MaxFloat64},
		{-317001600000000000, 42.0}, // pre-epoch timestamp
	}
	var vals itsm.Values
	for _, p := range in {
		vals = append(vals, itsm.NewFloatValue(p.ts, p.v))
	}
	got, err := decodeBlock(BlockFloat, encodeBlock(t, vals))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(in) {
		t.Fatalf("count: got %d want %d", len(got), len(in))
	}
	for i, p := range in {
		if got[i].UnixNano != p.ts || got[i].Float != p.v {
			t.Errorf("[%d] got (%d,%v) want (%d,%v)", i, got[i].UnixNano, got[i].Float, p.ts, p.v)
		}
	}
}

func TestDecodeIntegerBlock(t *testing.T) {
	in := []struct {
		ts int64
		v  int64
	}{
		{1700000000000000000, 8},
		{1700000001000000000, 16},
		{1700000002000000000, -5},
		{1700000003000000000, 0},
		{1700000004000000000, math.MaxInt64},
		{1700000005000000000, math.MinInt64},
	}
	var vals itsm.Values
	for _, p := range in {
		vals = append(vals, itsm.NewIntegerValue(p.ts, p.v))
	}
	got, err := decodeBlock(BlockInteger, encodeBlock(t, vals))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(in) {
		t.Fatalf("count: got %d want %d", len(got), len(in))
	}
	for i, p := range in {
		if got[i].UnixNano != p.ts || got[i].Integer != p.v {
			t.Errorf("[%d] got (%d,%v) want (%d,%v)", i, got[i].UnixNano, got[i].Integer, p.ts, p.v)
		}
	}
}

func TestDecodeUnsignedBlock(t *testing.T) {
	in := []struct {
		ts int64
		v  uint64
	}{
		{1700000000000000000, 3},
		{1700000001000000000, 12},
		{1700000002000000000, 0},
		{1700000003000000000, math.MaxUint64},
	}
	var vals itsm.Values
	for _, p := range in {
		vals = append(vals, itsm.NewUnsignedValue(p.ts, p.v))
	}
	got, err := decodeBlock(BlockUnsigned, encodeBlock(t, vals))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(in) {
		t.Fatalf("count: got %d want %d", len(got), len(in))
	}
	for i, p := range in {
		if got[i].UnixNano != p.ts || got[i].Unsigned != p.v {
			t.Errorf("[%d] got (%d,%v) want (%d,%v)", i, got[i].UnixNano, got[i].Unsigned, p.ts, p.v)
		}
	}
}

func TestDecodeBooleanBlock(t *testing.T) {
	in := []bool{true, false, false, true, true, false, true}
	var vals itsm.Values
	for i, b := range in {
		vals = append(vals, itsm.NewBooleanValue(int64(i+1)*1e9, b))
	}
	got, err := decodeBlock(BlockBoolean, encodeBlock(t, vals))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(in) {
		t.Fatalf("count: got %d want %d", len(got), len(in))
	}
	for i, b := range in {
		if got[i].Boolean != b {
			t.Errorf("[%d] got %v want %v", i, got[i].Boolean, b)
		}
	}
}

func TestDecodeStringBlock(t *testing.T) {
	in := []string{"all systems nominal", "degraded link", "", `with "quotes" and \backslash`, "unicode: αβγ"}
	var vals itsm.Values
	for i, s := range in {
		vals = append(vals, itsm.NewStringValue(int64(i+1)*1e9, s))
	}
	got, err := decodeBlock(BlockString, encodeBlock(t, vals))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(in) {
		t.Fatalf("count: got %d want %d", len(got), len(in))
	}
	for i, s := range in {
		if got[i].String != s {
			t.Errorf("[%d] got %q want %q", i, got[i].String, s)
		}
	}
}

// Larger run to exercise simple8b packing across many values (not just RLE).
func TestDecodeIntegerLargeRun(t *testing.T) {
	const n = 5000
	var vals itsm.Values
	base := int64(1700000000000000000)
	for i := 0; i < n; i++ {
		// irregular deltas so simple8b uses multiple selectors
		vals = append(vals, itsm.NewIntegerValue(base+int64(i)*int64(1+i%7)*1e6, int64(i*i-i)))
	}
	got, err := decodeBlock(BlockInteger, encodeBlock(t, vals))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != n {
		t.Fatalf("count: got %d want %d", len(got), n)
	}
	for i := 0; i < n; i++ {
		want := int64(i*i - i)
		if got[i].Integer != want {
			t.Fatalf("[%d] got %d want %d", i, got[i].Integer, want)
		}
	}
}
