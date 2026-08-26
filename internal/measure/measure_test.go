package measure

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValid(t *testing.T) {
	valid := []string{"cpu", "core_metrics", "edge-prod", "a", "A9", "m_", "x-y_z2"}
	invalid := []string{
		"", "edge-prod.gateway_services", "2fast", "_lead", "-lead", "9",
		"with space", "tab\there", "utf8µ", "semi;colon", "eq=ual", "dot.",
	}
	for _, s := range valid {
		if !Valid(s) {
			t.Errorf("Valid(%q) = false, want true", s)
		}
	}
	for _, s := range invalid {
		if Valid(s) {
			t.Errorf("Valid(%q) = true, want false", s)
		}
	}
}

func TestSanitize(t *testing.T) {
	cases := map[string]string{
		"edge-prod.gateway_services": "edge-prod_gateway_services",
		"test.foo":                   "test_foo",
		"2fast":                      "m_2fast",
		"_lead":                      "m__lead",
		"":                           "m_",
		"a b.c":                      "a_b_c",
		"µx":                         "m__x", // one underscore per rune, then prefix
		"already_valid":              "already_valid",
	}
	for in, want := range cases {
		got := Sanitize(in)
		if got != want {
			t.Errorf("Sanitize(%q) = %q, want %q", in, got, want)
		}
		if !Valid(got) {
			t.Errorf("Sanitize(%q) = %q is not Arc-valid", in, got)
		}
	}
	// determinism
	if Sanitize("a.b") != Sanitize("a.b") {
		t.Error("Sanitize is not deterministic")
	}
}

func TestParsePolicy(t *testing.T) {
	for s, want := range map[string]Policy{"fail": PolicyFail, "skip": PolicySkip, "map": PolicyMap} {
		got, err := ParsePolicy(s)
		if err != nil || got != want {
			t.Errorf("ParsePolicy(%q) = %v, %v", s, got, err)
		}
		if got.String() != s {
			t.Errorf("Policy(%q).String() = %q", s, got.String())
		}
	}
	if _, err := ParsePolicy("explode"); err == nil {
		t.Error("ParsePolicy(explode) succeeded, want error")
	}
}

func TestResolver(t *testing.T) {
	m := map[string]string{
		"edge-prod.gateway_services": "edge_prod_gateway_services",
		"cpu":                        "cpu_renamed", // explicit map also renames valid names
	}

	t.Run("fail", func(t *testing.T) {
		r, err := NewResolver(m, PolicyFail)
		if err != nil {
			t.Fatal(err)
		}
		if res := r.Resolve("mem"); res.Action != ActionPass || res.Name != "mem" {
			t.Errorf("valid unmapped: %+v", res)
		}
		if res := r.Resolve("cpu"); res.Action != ActionRenamed || res.Name != "cpu_renamed" {
			t.Errorf("explicit valid: %+v", res)
		}
		if res := r.Resolve("edge-prod.gateway_services"); res.Action != ActionRenamed || res.Name != "edge_prod_gateway_services" {
			t.Errorf("explicit invalid: %+v", res)
		}
		if res := r.Resolve("test.unmapped"); res.Action != ActionInvalid {
			t.Errorf("unmapped invalid under fail: %+v", res)
		}
	})

	t.Run("skip", func(t *testing.T) {
		r, _ := NewResolver(nil, PolicySkip)
		if res := r.Resolve("test.unmapped"); res.Action != ActionSkipped || res.Name != "" {
			t.Errorf("skip: %+v", res)
		}
	})

	t.Run("map", func(t *testing.T) {
		r, _ := NewResolver(nil, PolicyMap)
		if res := r.Resolve("test.unmapped"); res.Action != ActionAutoRenamed || res.Name != "test_unmapped" {
			t.Errorf("auto map: %+v", res)
		}
	})

	t.Run("nil resolver passes through", func(t *testing.T) {
		var r *Resolver
		if res := r.Resolve("any.thing"); res.Action != ActionPass || res.Name != "any.thing" {
			t.Errorf("nil resolver: %+v", res)
		}
		if r.Fingerprint() != "" {
			t.Error("nil resolver fingerprint not empty")
		}
	})

	t.Run("invalid target rejected", func(t *testing.T) {
		if _, err := NewResolver(map[string]string{"a.b": "still.dotted"}, PolicyFail); err == nil {
			t.Error("NewResolver accepted an Arc-invalid map target")
		}
	})
}

func TestFingerprint(t *testing.T) {
	// Defaults produce "" so <=0.1.2 checkpoints keep resuming.
	r, _ := NewResolver(nil, PolicyFail)
	if fp := r.Fingerprint(); fp != "" {
		t.Errorf("default fingerprint = %q, want empty", fp)
	}
	r, _ = NewResolver(map[string]string{"b.x": "b_x", "a.x": "a_x"}, PolicySkip)
	want := "mmap=a.x=a_x,b.x=b_x;on-invalid=skip"
	if fp := r.Fingerprint(); fp != want {
		t.Errorf("fingerprint = %q, want %q", fp, want)
	}
	// Policy alone (no map) is still fingerprinted — it shapes chunk bytes.
	r, _ = NewResolver(nil, PolicyMap)
	if fp := r.Fingerprint(); fp != "mmap=;on-invalid=map" {
		t.Errorf("policy-only fingerprint = %q", fp)
	}
}

func TestParseMapEntry(t *testing.T) {
	// splits on the LAST '=' — the source may contain '=', the target cannot.
	from, to, err := ParseMapEntry("weird=name=fixed_name")
	if err != nil || from != "weird=name" || to != "fixed_name" {
		t.Errorf("got %q, %q, %v", from, to, err)
	}
	for _, bad := range []string{"", "=", "noeq", "=lead", "trail="} {
		if _, _, err := ParseMapEntry(bad); err == nil {
			t.Errorf("ParseMapEntry(%q) succeeded, want error", bad)
		}
	}
}

func TestAddEntriesConflict(t *testing.T) {
	dst := map[string]string{}
	if err := AddEntries(dst, []string{"a.b=a_b", "a.b=a_b"}, "flag"); err != nil {
		t.Fatalf("identical duplicate rejected: %v", err)
	}
	if err := AddEntries(dst, []string{"a.b=other"}, "flag"); err == nil {
		t.Error("conflicting rename accepted")
	}
}

func TestLoadMapFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "map.txt")
	content := strings.Join([]string{
		"# rename plan authored by the operator",
		"",
		"edge-prod.gateway_services=edge_prod_gateway_services",
		"  test.foo=test_foo  ",
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	dst := map[string]string{}
	if err := LoadMapFile(path, dst); err != nil {
		t.Fatal(err)
	}
	if len(dst) != 2 || dst["edge-prod.gateway_services"] != "edge_prod_gateway_services" || dst["test.foo"] != "test_foo" {
		t.Errorf("loaded map = %v", dst)
	}

	// malformed line reports the file:line
	bad := filepath.Join(t.TempDir(), "bad.txt")
	os.WriteFile(bad, []byte("fine=ok\nnot-a-mapping\n"), 0o644)
	err := LoadMapFile(bad, map[string]string{})
	if err == nil || !strings.Contains(err.Error(), ":2") {
		t.Errorf("want line-numbered error, got %v", err)
	}
}
