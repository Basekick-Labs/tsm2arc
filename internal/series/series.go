// Package series parses InfluxDB TSM series keys and rejoins per-field value
// streams into multi-field points.
//
// A TSM key encodes BOTH the series (measurement + tags) and the field name,
// joined by the field-key separator "#!~#":
//
//	cpu,host=node-a,region=us-west#!~#usage_idle
//	└──────── series key ────────┘     └─ field ─┘
//
// The series key portion is the InfluxDB "measurement,tagk=tagv,..." form with
// tags already sorted lexicographically by key (InfluxDB stores them sorted).
// Escaping rules match InfluxDB line protocol: within the series key, commas,
// spaces and equals signs in measurement/tag/value tokens are backslash-escaped.
package series

import "strings"

// FieldSeparator joins the series key and the field name inside a TSM key.
const FieldSeparator = "#!~#"

// Key is a parsed TSM series+field key.
type Key struct {
	Measurement string
	Tags        [][2]string // ordered (key, value), as stored (sorted by key)
	Field       string
	SeriesKey   string // the raw "measurement,tags" portion (for grouping/dedup)
}

// ParseKey splits a raw TSM key into measurement, tags, and field.
func ParseKey(raw string) (Key, error) {
	sep := strings.Index(raw, FieldSeparator)
	if sep < 0 {
		// No field separator: whole key is the series, no field. Rare/legacy;
		// treat the trailing token as the field is not possible, so error out
		// so the caller can log and skip rather than emit malformed LP.
		return Key{}, errNoField
	}
	seriesPart := raw[:sep]
	field := raw[sep+len(FieldSeparator):]

	measurement, tags := splitSeries(seriesPart)
	return Key{
		Measurement: measurement,
		Tags:        tags,
		Field:       field,
		SeriesKey:   seriesPart,
	}, nil
}

// ParseSeriesKey parses a bare series key (the "measurement,tagk=tagv,..." part
// with no field separator) into its measurement and ordered tags. Use this when
// you already have a grouped series key and need its measurement/tags without a
// field.
func ParseSeriesKey(seriesKey string) (measurement string, tags [][2]string) {
	return splitSeries(seriesKey)
}

// splitSeries splits "measurement,tagk=tagv,..." honoring backslash escapes for
// commas and equals signs (line-protocol style). Tag order is preserved.
func splitSeries(s string) (string, [][2]string) {
	parts := splitUnescaped(s, ',')
	if len(parts) == 0 {
		return s, nil
	}
	measurement := unescapeMeasurement(parts[0])
	var tags [][2]string
	for _, p := range parts[1:] {
		kv := splitUnescaped(p, '=')
		if len(kv) != 2 {
			// malformed tag token; keep raw key with empty value rather than drop
			tags = append(tags, [2]string{unescapeTag(p), ""})
			continue
		}
		tags = append(tags, [2]string{unescapeTag(kv[0]), unescapeTag(kv[1])})
	}
	return measurement, tags
}

// splitUnescaped splits s on sep, ignoring sep preceded by a backslash.
func splitUnescaped(s string, sep byte) []string {
	var out []string
	var cur strings.Builder
	escaped := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if escaped {
			cur.WriteByte('\\')
			cur.WriteByte(c)
			escaped = false
			continue
		}
		if c == '\\' {
			escaped = true
			continue
		}
		if c == sep {
			out = append(out, cur.String())
			cur.Reset()
			continue
		}
		cur.WriteByte(c)
	}
	if escaped {
		cur.WriteByte('\\')
	}
	out = append(out, cur.String())
	return out
}

// unescapeTag reverses line-protocol tag escaping, mirroring InfluxDB's
// models.unescapeTag EXACTLY: only the three specific escape sequences are
// collapsed. A backslash that is not part of one of these sequences is LITERAL
// data and must be preserved — e.g. a Windows path tag value `C:\Users` is
// stored verbatim and must come back as `C:\Users`, not `C:Users`. Doing a
// greedy "drop any backslash" here silently corrupts such values.
func unescapeTag(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	s = strings.ReplaceAll(s, `\,`, `,`)
	s = strings.ReplaceAll(s, `\ `, ` `)
	s = strings.ReplaceAll(s, `\=`, `=`)
	return s
}

// unescapeMeasurement mirrors InfluxDB's models.unescapeMeasurement: only `\,`
// and `\ ` are collapsed for measurement names (no `\=`).
func unescapeMeasurement(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	s = strings.ReplaceAll(s, `\,`, `,`)
	s = strings.ReplaceAll(s, `\ `, ` `)
	return s
}
