package main

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/basekick-labs/tsm2arc/internal/extract"
	"github.com/basekick-labs/tsm2arc/internal/lp"
)

// defaultNarrowTag is the tag the narrow layout stores a field's name under.
const defaultNarrowTag = "field"

// narrowConfig is --narrow / --narrow-tag: the source measurements to write
// in the narrow layout (lp.AppendNarrow), and the tag holding the field name.
type narrowConfig struct {
	measurements map[string]bool
	tag          string
}

func (n narrowConfig) enabled() bool { return len(n.measurements) > 0 }

func (n narrowConfig) has(measurement string) bool { return n.measurements[measurement] }

// sorted is the narrow measurement names in sorted order.
func (n narrowConfig) sorted() []string {
	out := make([]string, 0, len(n.measurements))
	for m := range n.measurements {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

// narrowTagPattern is what --narrow-tag may be: a plain identifier Arc stores
// as a column name as it is.
var narrowTagPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

// parseNarrow builds the narrow config from the flags. names are --narrow
// values, each a comma-separated list; tag is --narrow-tag, tagSet whether it
// was given explicitly.
func parseNarrow(names []string, tag string, tagSet bool) (narrowConfig, error) {
	n := narrowConfig{measurements: map[string]bool{}, tag: tag}
	for _, v := range names {
		for _, m := range strings.Split(v, ",") {
			if m = strings.TrimSpace(m); m != "" {
				n.measurements[m] = true
			}
		}
	}
	if !n.enabled() {
		if tagSet {
			return n, fmt.Errorf("--narrow-tag needs --narrow")
		}
		return n, nil
	}
	if !narrowTagPattern.MatchString(tag) {
		return n, fmt.Errorf("--narrow-tag %q: use letters, digits and underscores, starting with a letter or underscore", tag)
	}
	if strings.EqualFold(tag, "time") {
		return n, fmt.Errorf("--narrow-tag cannot be %q: Arc keeps that name for the timestamp", tag)
	}
	for _, c := range lp.NarrowValueColumns {
		if strings.EqualFold(tag, c) {
			return n, fmt.Errorf("--narrow-tag cannot be %q: the narrow layout writes values to that column", tag)
		}
	}
	return n, nil
}

// fingerprint is the narrow part of the resume fingerprint, empty when narrow
// is off so checkpoints of earlier runs still resume. Names are length-
// prefixed: a measurement name may contain any character, a comma included.
func (n narrowConfig) fingerprint() string {
	if !n.enabled() {
		return ""
	}
	var b strings.Builder
	b.WriteString("narrow=")
	for i, m := range n.sorted() {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%d:%s", len(m), m)
	}
	b.WriteString(";narrowtag=")
	b.WriteString(n.tag)
	return b.String()
}

// narrowClash reports a tag key that the narrow layout cannot write next to:
// the narrow tag itself, or a name Arc would fold onto one of the value
// columns (Arc refuses column names that differ only in letter case, and
// renames a field named like a tag to "<name>_value").
func (n narrowConfig) narrowClash(tagKey string) (string, bool) {
	if strings.EqualFold(tagKey, n.tag) {
		return n.tag, true
	}
	for _, c := range lp.NarrowValueColumns {
		if strings.EqualFold(tagKey, c) {
			return c, true
		}
	}
	return "", false
}

// encodePoint encodes one point as the load sends it: the narrow layout for a
// --narrow source measurement (srcMeasurement), one line otherwise. The dry
// run samples go through it too, so they show exactly what a load sends.
func encodePoint(dst []byte, cfg runConfig, srcMeasurement string, p extract.Point) []byte {
	if cfg.narrow.has(srcMeasurement) {
		return lp.AppendNarrow(dst, p.Measurement, p.Tags, cfg.narrow.tag, p.Fields, p.UnixNano)
	}
	return lp.AppendPoint(dst, p.Measurement, p.Tags, p.Fields, p.UnixNano)
}

// narrowPointError checks a narrow point's tags at load time. The census
// refuses these before the first POST; this is the backstop for a series the
// census could not see.
func narrowPointError(cfg runConfig, srcMeasurement string, p extract.Point) error {
	if !cfg.narrow.has(srcMeasurement) {
		return nil
	}
	for _, t := range p.Tags {
		if col, bad := cfg.narrow.narrowClash(t[0]); bad {
			return fmt.Errorf("measurement %q has a tag %q, which the narrow layout cannot write next to its %q column; choose another --narrow-tag", srcMeasurement, t[0], col)
		}
	}
	return nil
}
