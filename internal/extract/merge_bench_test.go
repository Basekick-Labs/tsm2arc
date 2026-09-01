package extract

import (
	"testing"

	"github.com/basekick-labs/tsm2arc/internal/tsm"
)

// BenchmarkMergeWideSplitSeries models the dominant migration shape: one wide
// series whose every field is split across many TSM files holding mostly
// disjoint time ranges (real compaction generations), merged as one run. Each
// row carries one value per field, but before per-field coalescing the two
// per-timestamp scans still peeked EVERY (file, field) stream — O(files ×
// fields) per row. With coalescing a row costs O(fields) peeks plus
// O(fields·log files) heap advances.
func BenchmarkMergeWideSplitSeries(b *testing.B) {
	const files, fields, rowsPerFile = 40, 100, 50
	const rows = files * rowsPerFile
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		streams := make([]stream, 0, files*fields)
		for f := 0; f < fields; f++ {
			name := "field_" + string(rune('a'+f%26)) + string(rune('a'+f/26))
			for fi := 0; fi < files; fi++ {
				// Each file holds its own disjoint slice of the timeline.
				vals := make([]tsm.Value, rowsPerFile)
				for r := 0; r < rowsPerFile; r++ {
					vals[r] = tsm.Value{UnixNano: int64(fi*rowsPerFile + r), Type: tsm.BlockFloat, Float: float64(fi)}
				}
				streams = append(streams, newSliceStream(name, vals, 0, rows))
			}
		}
		var st Stats
		b.StartTimer()
		if err := mergeSeries("cpu,host=x", "cpu", [][2]string{{"host", "x"}}, streams, &st, func(Point) {}); err != nil {
			b.Fatal(err)
		}
		if st.Points != rows {
			b.Fatalf("points = %d, want %d", st.Points, rows)
		}
	}
}
