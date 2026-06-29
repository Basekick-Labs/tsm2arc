package series

import "testing"

func TestParseKeyBasic(t *testing.T) {
	k, err := ParseKey("cpu,host=node-a,region=us-west#!~#usage_idle")
	if err != nil {
		t.Fatal(err)
	}
	if k.Measurement != "cpu" {
		t.Errorf("measurement = %q", k.Measurement)
	}
	if k.Field != "usage_idle" {
		t.Errorf("field = %q", k.Field)
	}
	want := [][2]string{{"host", "node-a"}, {"region", "us-west"}}
	if len(k.Tags) != 2 || k.Tags[0] != want[0] || k.Tags[1] != want[1] {
		t.Errorf("tags = %v, want %v", k.Tags, want)
	}
}

func TestParseKeyNoField(t *testing.T) {
	if _, err := ParseKey("cpu,host=a"); err != ErrNoField {
		t.Errorf("expected ErrNoField, got %v", err)
	}
}

// B1 regression: backslash unescaping must mirror InfluxDB exactly. Only the
// three escape sequences \, \space \= collapse; every other backslash is
// LITERAL data and must be preserved. The old greedy implementation dropped all
// backslashes, silently corrupting values like Windows paths.
func TestUnescapePreservesLiteralBackslash(t *testing.T) {
	cases := []struct {
		raw       string // the TSM key (already escaped as InfluxDB stores it)
		wantMeas  string
		wantTagKV [2]string
	}{
		// Windows path tag value: backslash is literal, must survive.
		{`evt,path=C:\Users#!~#f`, "evt", [2]string{"path", `C:\Users`}},
		// literal double backslash preserved
		{`evt,p=a\\b#!~#f`, "evt", [2]string{"p", `a\\b`}},
		// genuine escapes still collapse: \, → ,   \= → =   \space → space
		{`evt,p=a\,b#!~#f`, "evt", [2]string{"p", `a,b`}},
		{`evt,p=a\=b#!~#f`, "evt", [2]string{"p", `a=b`}},
		{`evt,p=a\ b#!~#f`, "evt", [2]string{"p", `a b`}},
		// a literal backslash NOT before ,= space is preserved (e.g. \n stays \n)
		{`evt,p=a\nb#!~#f`, "evt", [2]string{"p", `a\nb`}},
	}
	for _, c := range cases {
		k, err := ParseKey(c.raw)
		if err != nil {
			t.Fatalf("%q: %v", c.raw, err)
		}
		if k.Measurement != c.wantMeas {
			t.Errorf("%q: measurement = %q, want %q", c.raw, k.Measurement, c.wantMeas)
		}
		if len(k.Tags) != 1 || k.Tags[0] != c.wantTagKV {
			t.Errorf("%q: tag = %v, want %v", c.raw, k.Tags, c.wantTagKV)
		}
	}
}

// Measurement unescaping collapses only \, and \space (not \=), per InfluxDB.
func TestUnescapeMeasurement(t *testing.T) {
	k, err := ParseKey(`a\,b\ c#!~#f`)
	if err != nil {
		t.Fatal(err)
	}
	if k.Measurement != "a,b c" {
		t.Errorf("measurement = %q, want %q", k.Measurement, "a,b c")
	}
}
