// Package lp encodes reconstructed InfluxDB points back into line protocol.
//
// Output targets Arc's /api/v1/import/lp endpoint, which parses standard
// InfluxDB line protocol. Field type fidelity matters: integers carry the "i"
// suffix, unsigned "u", strings are double-quoted with escaping, booleans become
// true/false, and floats are formatted without a suffix. Timestamps are emitted
// as nanosecond Unix epoch (Arc's default precision), including negative
// (pre-1970) values.
package lp

import (
	"strconv"
	"strings"

	"github.com/basekick-labs/tsm2arc/internal/tsm"
)

// Field is one field name + typed value at a point.
type Field struct {
	Name  string
	Value tsm.Value // Type + the matching typed field
}

// EncodePoint writes one line-protocol line for a single point:
//
//	measurement[,tagk=tagv...] field=value[,field=value...] timestamp
//
// tags are written in the given order (TSM stores them sorted by key, which is
// also canonical for line protocol). fields must be non-empty.
func EncodePoint(b *strings.Builder, measurement string, tags [][2]string, fields []Field, unixNano int64) {
	b.WriteString(escapeMeasurement(measurement))
	for _, t := range tags {
		b.WriteByte(',')
		b.WriteString(escapeTag(t[0]))
		b.WriteByte('=')
		b.WriteString(escapeTag(t[1]))
	}
	b.WriteByte(' ')
	for i, f := range fields {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(escapeTag(f.Name)) // field keys escape like tag keys
		b.WriteByte('=')
		writeFieldValue(b, f.Value)
	}
	b.WriteByte(' ')
	b.WriteString(strconv.FormatInt(unixNano, 10))
	b.WriteByte('\n')
}

func writeFieldValue(b *strings.Builder, v tsm.Value) {
	switch v.Type {
	case tsm.BlockFloat:
		// 'g' with -1 precision round-trips the float64 minimally and exactly.
		b.WriteString(strconv.FormatFloat(v.Float, 'g', -1, 64))
	case tsm.BlockInteger:
		b.WriteString(strconv.FormatInt(v.Integer, 10))
		b.WriteByte('i')
	case tsm.BlockUnsigned:
		b.WriteString(strconv.FormatUint(v.Unsigned, 10))
		b.WriteByte('u')
	case tsm.BlockBoolean:
		if v.Boolean {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case tsm.BlockString:
		b.WriteByte('"')
		b.WriteString(escapeStringField(v.String))
		b.WriteByte('"')
	}
}

// --- escaping helpers (InfluxDB line-protocol rules) ---

// measurement: escape commas and spaces (not equals).
func escapeMeasurement(s string) string {
	return replacer(s, ",", "\\,", " ", "\\ ")
}

// tag keys, tag values, field keys: escape commas, spaces, equals.
func escapeTag(s string) string {
	return replacer(s, ",", "\\,", " ", "\\ ", "=", "\\=")
}

// string field values: escape double-quotes and backslashes only.
func escapeStringField(s string) string {
	return replacer(s, "\\", "\\\\", "\"", "\\\"")
}

// replacer applies pairs of (old,new) without allocating when no replacement is
// needed (the common case for clean telemetry data).
func replacer(s string, pairs ...string) string {
	needs := false
	for i := 0; i < len(pairs); i += 2 {
		if strings.Contains(s, pairs[i]) {
			needs = true
			break
		}
	}
	if !needs {
		return s
	}
	return strings.NewReplacer(pairs...).Replace(s)
}
