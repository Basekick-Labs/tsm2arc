package wal3

import (
	"context"
	"hash/crc32"
	"os"
	"testing"
)

// The fixtures are REAL WAL files written by InfluxDB 3 Core servers of three
// eras — 3.0.3 (bitcode 0.6.3 era), 3.4.2 (0.6.6), 3.11.2 (0.6.9) — with
// known synthetic contents. Decoding all of them with the single vendored
// struct graph + bitcode 0.6.9 is the empirical proof of the "one decoder
// covers 3.0-3.11" claim (and of bitcode 0.6.x wire compatibility).

func newDec(t *testing.T) *Decoder {
	t.Helper()
	d, err := NewDecoder(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close(context.Background()) })
	return d
}

func decodeFixture(t *testing.T, d *Decoder, name string) *WalContents {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	wc, err := d.Decode(context.Background(), b)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return wc
}

func TestDecodeRealFilesAllEras(t *testing.T) {
	d := newDec(t)
	for _, name := range []string{"v30-write.wal", "v30-late.wal", "v34-write.wal", "v311-lww.wal"} {
		wc := decodeFixture(t, d, name)
		if len(wc.Ops) == 0 {
			t.Fatalf("%s: no ops", name)
		}
		if wc.WalFileNumber == 0 {
			t.Fatalf("%s: missing wal file number", name)
		}
		for _, op := range wc.Ops {
			if op.Write == nil && op.Noop == nil {
				t.Fatalf("%s: op with no variant", name)
			}
		}
	}
}

// v311-lww.wal is WAL sequence 52 of the 3.11.2 rig: the single overwrite of
// (host=web-0, region=us-east) at 2026-09-07T18:52:14Z with usage=999.9,
// load=99i in database fleet — the write whose LWW victory the extract3 and
// Arc e2e tests assert. Verify the decoder sees exactly that.
func TestDecodeKnownWrite(t *testing.T) {
	d := newDec(t)
	wc := decodeFixture(t, d, "v311-lww.wal")

	var batch *WriteBatch
	for i := range wc.Ops {
		if w := wc.Ops[i].Write; w != nil && w.DatabaseName == "fleet" {
			batch = w
		}
	}
	if batch == nil {
		t.Fatal("no Write op for database fleet")
	}
	found := false
	for ti := range batch.TableChunks {
		chunks, err := batch.TableChunks[ti].Chunks()
		if err != nil {
			t.Fatal(err)
		}
		for ct, chunk := range chunks {
			for _, row := range chunk.Rows {
				if ct != row.Time-row.Time%int64(60_000_000_000) {
					t.Fatalf("chunk key %d is not the gen1 truncation of row time %d", ct, row.Time)
				}
				var tags, floats, ints, times int
				var usage float64
				var load int64
				for _, f := range row.Fields {
					switch v := f.Value; {
					case v.Tag != nil:
						tags++
					case v.Float != nil:
						floats++
						usage = *v.Float
					case v.Integer != nil:
						ints++
						load = *v.Integer
					case v.Timestamp != nil:
						times++
						if *v.Timestamp != row.Time {
							t.Fatalf("timestamp field %d != row time %d", *v.Timestamp, row.Time)
						}
					}
				}
				if tags == 2 && floats == 1 && ints == 1 && times == 1 && usage == 999.9 && load == 99 {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("did not find the known overwrite row (2 tags, usage=999.9, load=99)")
	}
}

// v30 fixtures were written by a 3.0.3 server whose lockfile pinned bitcode
// 0.6.3 — decoding them with 0.6.9 proves cross-patch wire compatibility.
func TestDecodeV30Contents(t *testing.T) {
	d := newDec(t)
	wc := decodeFixture(t, d, "v30-write.wal")
	dbs := map[string]bool{}
	rows := 0
	for i := range wc.Ops {
		w := wc.Ops[i].Write
		if w == nil {
			continue
		}
		dbs[w.DatabaseName] = true
		for ti := range w.TableChunks {
			chunks, err := w.TableChunks[ti].Chunks()
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range chunks {
				rows += len(c.Rows)
			}
		}
	}
	if !dbs["fleet"] && !dbs["telemetry"] {
		t.Fatalf("expected fleet/telemetry writes, got %v", dbs)
	}
	if rows == 0 {
		t.Fatal("no rows decoded")
	}
}

func TestDecodeRejectsCorruption(t *testing.T) {
	d := newDec(t)
	b, err := os.ReadFile("testdata/v311-lww.wal")
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(nil), b...)
	bad[len(bad)-1] ^= 0xFF
	if _, err := d.Decode(context.Background(), bad); err == nil {
		t.Fatal("corrupted payload must fail CRC")
	}
	if _, err := d.Decode(context.Background(), []byte("not a wal file at all")); err == nil {
		t.Fatal("bad magic must fail")
	}
	// Valid framing, garbage payload: the CRC passes but bitcode must refuse.
	garbage := []byte("idb3.001")
	payload := []byte{0xde, 0xad, 0xbe, 0xef, 0x01}
	garbage = append(garbage, 0, 0, 0, 0)
	garbage = append(garbage, payload...)
	// fix CRC
	c := crcOf(payload)
	garbage[8], garbage[9], garbage[10], garbage[11] = byte(c>>24), byte(c>>16), byte(c>>8), byte(c)
	if _, err := d.Decode(context.Background(), garbage); err == nil {
		t.Fatal("garbage bitcode payload must fail decode")
	}
}

func crcOf(b []byte) uint32 { return crc32.ChecksumIEEE(b) }
