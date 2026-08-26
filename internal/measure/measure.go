// Package measure enforces Arc's measurement-name rule client-side and applies
// the operator's chosen policy for source measurements that violate it.
//
// Arc rejects measurement names that don't match ^[a-zA-Z][a-zA-Z0-9_-]*$
// (internal/api/lineprotocol.go, enforced identically on the msgpack path).
// InfluxDB is far more permissive — dots in particular are common
// ("<env>.<service>" naming) — so a 1.x/2.x dataset can be full of names Arc
// will 400. Arc will not relax the rule: the dot is its database.measurement
// separator in the query layer and in RBAC grant keys, so a dotted measurement
// would be ambiguous to parse and unsafe to permission.
//
// This package is tsm2arc's answer: validate every name BEFORE it is sent,
// apply an operator-authored rename map, and on any name still invalid follow
// a policy — fail loudly (default), skip-and-report, or deterministically
// auto-rename. Renames and skips are recorded durably in the checkpoint by the
// caller so nothing is silent.
package measure

import (
	"fmt"
	"os"
	"sort"
	"strings"
)

// ArcNameRule is Arc's measurement-name pattern, quoted in operator-facing
// messages. Valid() implements it directly (no regexp on the hot path).
const ArcNameRule = "^[a-zA-Z][a-zA-Z0-9_-]*$"

// Valid reports whether name satisfies Arc's measurement-name rule.
func Valid(name string) bool {
	if name == "" || !isLetter(name[0]) {
		return false
	}
	for i := 1; i < len(name); i++ {
		c := name[i]
		if !isLetter(c) && !(c >= '0' && c <= '9') && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

func isLetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// Sanitize deterministically rewrites name into one that satisfies Arc's rule:
// every disallowed rune becomes a single underscore, and a result that doesn't
// start with a letter gets an "m_" prefix. Same input always yields the same
// output — required so resumed runs re-derive byte-identical chunks.
//
// Distinct inputs CAN collide (e.g. "a.b" and "a_b" both become "a_b"), which
// would silently merge two measurements. That's why auto-renaming is opt-in
// (--on-invalid-measurement=map), every applied rename is recorded, and an
// explicit --measurement-map is the recommended path.
func Sanitize(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	s := b.String()
	if s == "" || !isLetter(s[0]) {
		s = "m_" + s
	}
	return s
}

// Policy is what to do with a measurement name that is still invalid after the
// explicit map has been applied.
type Policy int

const (
	// PolicyFail aborts the load at the first invalid name (default). The
	// failure is client-side — before anything is sent — with an actionable
	// message, instead of Arc's mid-load 400.
	PolicyFail Policy = iota
	// PolicySkip drops points of invalid measurements and reports/records
	// exactly what was skipped, so one bad name can't kill a multi-hour load.
	PolicySkip
	// PolicyMap auto-renames invalid names via Sanitize, recording each rename.
	PolicyMap
)

// ParsePolicy parses the --on-invalid-measurement flag value.
func ParsePolicy(s string) (Policy, error) {
	switch s {
	case "fail":
		return PolicyFail, nil
	case "skip":
		return PolicySkip, nil
	case "map":
		return PolicyMap, nil
	}
	return PolicyFail, fmt.Errorf("invalid --on-invalid-measurement %q: want fail, skip, or map", s)
}

func (p Policy) String() string {
	switch p {
	case PolicySkip:
		return "skip"
	case PolicyMap:
		return "map"
	default:
		return "fail"
	}
}

// Action classifies what Resolve decided for one measurement name.
type Action int

const (
	// ActionPass — valid and unmapped; send unchanged.
	ActionPass Action = iota
	// ActionRenamed — an explicit --measurement-map entry applied.
	ActionRenamed
	// ActionAutoRenamed — invalid name sanitized under PolicyMap.
	ActionAutoRenamed
	// ActionSkipped — invalid name dropped under PolicySkip.
	ActionSkipped
	// ActionInvalid — invalid name under PolicyFail; the caller must abort.
	ActionInvalid
)

// Resolution is the outcome for one measurement name. Name is the final name
// to send (empty for Skipped/Invalid).
type Resolution struct {
	Name   string
	Action Action
}

// Resolver applies the explicit rename map and the invalid-name policy. It is
// immutable after construction and safe for concurrent use. A nil *Resolver
// passes every name through unchanged (no validation) — used by tests that
// construct configs directly.
type Resolver struct {
	explicit map[string]string
	policy   Policy
}

// NewResolver builds a Resolver, rejecting any map target that Arc would
// refuse (fail fast at startup, not at chunk 73).
func NewResolver(explicit map[string]string, policy Policy) (*Resolver, error) {
	for from, to := range explicit {
		if !Valid(to) {
			return nil, fmt.Errorf("measurement map target %q (for %q) is not a valid Arc measurement name: must match %s",
				to, from, ArcNameRule)
		}
	}
	return &Resolver{explicit: explicit, policy: policy}, nil
}

// Policy returns the resolver's invalid-name policy (PolicyFail for nil).
func (r *Resolver) Policy() Policy {
	if r == nil {
		return PolicyFail
	}
	return r.policy
}

// Resolve decides the final name for one source measurement. Pure function:
// the explicit map is consulted first (it may also rename valid names), then
// validity, then the policy.
func (r *Resolver) Resolve(name string) Resolution {
	if r == nil {
		return Resolution{Name: name, Action: ActionPass}
	}
	if to, ok := r.explicit[name]; ok {
		return Resolution{Name: to, Action: ActionRenamed}
	}
	if Valid(name) {
		return Resolution{Name: name, Action: ActionPass}
	}
	switch r.policy {
	case PolicySkip:
		return Resolution{Action: ActionSkipped}
	case PolicyMap:
		return Resolution{Name: Sanitize(name), Action: ActionAutoRenamed}
	default:
		return Resolution{Action: ActionInvalid}
	}
}

// Fingerprint returns a stable string of the resolver's chunk-shaping inputs
// for the checkpoint config fingerprint, or "" for the defaults (no map,
// PolicyFail). Returning "" for defaults keeps fingerprints — and therefore
// resumability — byte-identical with checkpoints created by tsm2arc <= 0.1.2.
func (r *Resolver) Fingerprint() string {
	if r == nil || (len(r.explicit) == 0 && r.policy == PolicyFail) {
		return ""
	}
	pairs := make([]string, 0, len(r.explicit))
	for from, to := range r.explicit {
		pairs = append(pairs, from+"="+to)
	}
	sort.Strings(pairs)
	return fmt.Sprintf("mmap=%s;on-invalid=%s", strings.Join(pairs, ","), r.policy)
}

// ParseMapEntry splits one "old=new" rename on its LAST '=' — unambiguous
// because the target must be Arc-valid and thus cannot contain '=' (the source
// may: InfluxDB allows '=' in measurement names).
func ParseMapEntry(entry string) (from, to string, err error) {
	i := strings.LastIndex(entry, "=")
	if i <= 0 || i == len(entry)-1 {
		return "", "", fmt.Errorf("bad measurement map entry %q: want old=new", entry)
	}
	return entry[:i], entry[i+1:], nil
}

// AddEntries folds "old=new" entries into dst, rejecting conflicting renames
// of the same source name. src labels the origin (flag/file) in errors.
func AddEntries(dst map[string]string, entries []string, src string) error {
	for _, e := range entries {
		from, to, err := ParseMapEntry(e)
		if err != nil {
			return fmt.Errorf("%s: %w", src, err)
		}
		if prev, ok := dst[from]; ok && prev != to {
			return fmt.Errorf("%s: conflicting renames for measurement %q: %q and %q", src, from, prev, to)
		}
		dst[from] = to
	}
	return nil
}

// LoadMapFile reads a measurement map file into dst: one "old=new" per line,
// blank lines and #-comment lines ignored, surrounding whitespace trimmed.
// Same syntax and conflict rules as the repeated --measurement-map flag.
func LoadMapFile(path string, dst map[string]string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read measurement map file: %w", err)
	}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if err := AddEntries(dst, []string{line}, fmt.Sprintf("%s:%d", path, i+1)); err != nil {
			return err
		}
	}
	return nil
}
