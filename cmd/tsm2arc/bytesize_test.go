package main

import "testing"

func TestParseByteSize(t *testing.T) {
	const (
		ki = 1 << 10
		mi = 1 << 20
		gi = 1 << 30
	)
	ok := []struct {
		in   string
		want int64
	}{
		{"471859200", 471859200}, // bare bytes
		{"0", 0},
		{"450MB", 450 * mi},
		{"450MiB", 450 * mi},
		{"200mb", 200 * mi}, // case-insensitive
		{"1GB", gi},
		{"1G", gi},
		{"512KB", 512 * ki},
		{"512K", 512 * ki},
		{"1024B", 1024},
		{" 450MB ", 450 * mi}, // trimmed
	}
	for _, c := range ok {
		got, err := parseByteSize(c.in)
		if err != nil {
			t.Errorf("parseByteSize(%q) error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("parseByteSize(%q) = %d, want %d", c.in, got, c.want)
		}
	}

	bad := []string{"", "abc", "4zz", "10XB", "-5MB", "1.5MB"}
	for _, in := range bad {
		if _, err := parseByteSize(in); err == nil {
			t.Errorf("parseByteSize(%q) should have errored", in)
		}
	}
}

// The default --chunk-bytes must equal 450 MiB (matches chunk.DefaultMaxBytes).
func TestChunkBytesDefault(t *testing.T) {
	got, err := parseByteSize("450MiB")
	if err != nil {
		t.Fatal(err)
	}
	if got != 450*1024*1024 {
		t.Errorf("450MiB = %d, want %d", got, 450*1024*1024)
	}
}
