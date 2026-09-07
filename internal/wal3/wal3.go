// Package wal3 decodes InfluxDB 3 WAL files.
//
// WAL payloads are bitcode-encoded (serde mode) — a bit-packed Rust
// serialization with no Go implementation, whose wire format depends exactly
// on the upstream struct shape. Rather than porting the codec (rejected:
// bit-level, explicitly unstable across versions by upstream policy), the
// decoder IS the upstream codec: the WalContents struct graph vendored in
// rust/ (identical across all upstream releases 3.0-3.11, verified against
// 23 tags), linked against the same bitcode crate the servers use, compiled
// to wasm32-wasip1, embedded here, and executed in-process via wazero —
// pure Go, no cgo, single static binary.
//
// Framing (constant 3.0-3.11): 8-byte magic "idb3.001", 4-byte big-endian
// CRC32 (IEEE) of the payload, then the raw bitcode payload (uncompressed).
package wal3

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/tetratelabs/wazero"
	wasi "github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

//go:embed decoder.wasm
var decoderWasm []byte

var walMagic = []byte("idb3.001")

// WalContents mirrors the decoder's JSON output — the upstream struct graph.
type WalContents struct {
	PersistTimestampMs int64            `json:"persist_timestamp_ms"`
	MinTimestampNs     int64            `json:"min_timestamp_ns"`
	MaxTimestampNs     int64            `json:"max_timestamp_ns"`
	WalFileNumber      uint64           `json:"wal_file_number"`
	Ops                []WalOp          `json:"ops"`
	Snapshot           *SnapshotDetails `json:"snapshot"`
}

// WalOp is the externally tagged upstream enum: exactly one field is set.
type WalOp struct {
	Write *WriteBatch  `json:"Write"`
	Noop  *NoopDetails `json:"Noop"`
}

// WriteBatch holds one database's rows for the WAL period. TableChunks
// arrives as the upstream SerdeVecMap: an array of [table_id, chunks] pairs.
type WriteBatch struct {
	CatalogSequence uint64        `json:"catalog_sequence"`
	DatabaseID      uint32        `json:"database_id"`
	DatabaseName    string        `json:"database_name"`
	TableChunks     []TableChunks `json:"-"`
	TableIDs        []uint32      `json:"-"`
	MinTimeNs       int64         `json:"min_time_ns"`
	MaxTimeNs       int64         `json:"max_time_ns"`
}

func (w *WriteBatch) UnmarshalJSON(data []byte) error {
	type alias WriteBatch
	aux := struct {
		*alias
		RawChunks []json.RawMessage `json:"table_chunks"`
	}{alias: (*alias)(w)}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	for _, raw := range aux.RawChunks {
		var pair [2]json.RawMessage
		if err := json.Unmarshal(raw, &pair); err != nil {
			return err
		}
		var id uint32
		if err := json.Unmarshal(pair[0], &id); err != nil {
			return err
		}
		var tc TableChunks
		if err := json.Unmarshal(pair[1], &tc); err != nil {
			return err
		}
		w.TableIDs = append(w.TableIDs, id)
		w.TableChunks = append(w.TableChunks, tc)
	}
	return nil
}

// TableChunks maps gen1-truncated chunk times to that window's rows.
type TableChunks struct {
	MinTime          int64                  `json:"min_time"`
	MaxTime          int64                  `json:"max_time"`
	ChunkTimeToChunk map[int64]*TableChunk  `json:"-"`
	RawChunks        map[string]*TableChunk `json:"chunk_time_to_chunk"`
}

// Chunks returns the chunk map with proper int64 keys (JSON object keys are
// strings).
func (t *TableChunks) Chunks() (map[int64]*TableChunk, error) {
	if t.ChunkTimeToChunk != nil {
		return t.ChunkTimeToChunk, nil
	}
	out := make(map[int64]*TableChunk, len(t.RawChunks))
	for k, v := range t.RawChunks {
		var ct int64
		if _, err := fmt.Sscanf(k, "%d", &ct); err != nil {
			return nil, fmt.Errorf("bad chunk_time key %q: %w", k, err)
		}
		out[ct] = v
	}
	t.ChunkTimeToChunk = out
	return out, nil
}

type TableChunk struct {
	Rows []Row `json:"rows"`
}

type Row struct {
	Time   int64   `json:"time"` // nanoseconds
	Fields []Field `json:"fields"`
}

// Field is one column value; the WAL references columns by id only (names
// live in the catalog). Value is the externally tagged FieldData enum.
type Field struct {
	ID    uint16    `json:"id"`
	Value FieldData `json:"value"`
}

// FieldData: exactly one pointer is non-nil, matching the upstream variant.
type FieldData struct {
	Timestamp *int64   `json:"Timestamp"`
	Key       *string  `json:"Key"`
	Tag       *string  `json:"Tag"`
	String    *string  `json:"String"`
	Integer   *int64   `json:"Integer"`
	UInteger  *uint64  `json:"UInteger"`
	Float     *float64 `json:"Float"`
	Boolean   *bool    `json:"Boolean"`
}

type NoopDetails struct {
	TimestampNs int64 `json:"timestamp_ns"`
}

type SnapshotDetails struct {
	SnapshotSequenceNumber uint64 `json:"snapshot_sequence_number"`
	EndTimeMarker          int64  `json:"end_time_marker"`
	FirstWalSequenceNumber uint64 `json:"first_wal_sequence_number"`
	LastWalSequenceNumber  uint64 `json:"last_wal_sequence_number"`
	Forced                 bool   `json:"forced"`
}

// Decoder runs the embedded wasm decoder. It is safe for concurrent use;
// the module is compiled once and instantiated per call.
type Decoder struct {
	runtime  wazero.Runtime
	compiled wazero.CompiledModule
	mu       sync.Mutex
}

// NewDecoder compiles the embedded module. Callers should reuse one Decoder;
// Close releases the runtime.
func NewDecoder(ctx context.Context) (*Decoder, error) {
	r := wazero.NewRuntime(ctx)
	if _, err := wasi.Instantiate(ctx, r); err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("instantiate WASI: %w", err)
	}
	compiled, err := r.CompileModule(ctx, decoderWasm)
	if err != nil {
		r.Close(ctx)
		return nil, fmt.Errorf("compile WAL decoder module: %w", err)
	}
	return &Decoder{runtime: r, compiled: compiled}, nil
}

func (d *Decoder) Close(ctx context.Context) error { return d.runtime.Close(ctx) }

// Decode verifies the file framing and decodes the bitcode payload.
func (d *Decoder) Decode(ctx context.Context, file []byte) (*WalContents, error) {
	if len(file) < 12 || !bytes.HasPrefix(file, walMagic) {
		return nil, fmt.Errorf("not an InfluxDB 3 WAL file (bad magic)")
	}
	payload := file[12:]
	if got, want := binary.BigEndian.Uint32(file[8:12]), crc32.ChecksumIEEE(payload); got != want {
		return nil, fmt.Errorf("WAL CRC mismatch (file corrupt or truncated)")
	}

	var stdout, stderr bytes.Buffer
	cfg := wazero.NewModuleConfig().
		WithStdin(bytes.NewReader(payload)).
		WithStdout(&stdout).
		WithStderr(&stderr).
		WithName("") // anonymous: allows concurrent instantiations

	// wazero instantiation of the same compiled module is cheap; serialize
	// anyway — WAL decode is a pre-flight step, not a hot path.
	d.mu.Lock()
	mod, err := d.runtime.InstantiateModule(ctx, d.compiled, cfg)
	d.mu.Unlock()
	if err != nil {
		// A non-zero exit surfaces as an error here; include stderr.
		if msg := bytes.TrimSpace(stderr.Bytes()); len(msg) > 0 {
			return nil, fmt.Errorf("WAL decoder: %s", msg)
		}
		return nil, fmt.Errorf("WAL decoder: %w", err)
	}
	if mod != nil {
		_ = mod.Close(ctx)
	}

	var out WalContents
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		return nil, fmt.Errorf("parse decoder output: %w", err)
	}
	return &out, nil
}
