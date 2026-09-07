package v3meta

import (
	"bytes"
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/basekick-labs/tsm2arc/internal/vfs"
)

// walMagic is the 8-byte header of every InfluxDB 3 WAL file (constant
// 3.0-3.11), followed by a 4-byte big-endian CRC32 of the bitcode payload.
var walMagic = []byte("idb3.001")

// WALStatus reports the WAL state of a node prefix relative to its newest
// snapshot. Snapshotted WAL files linger in the store by design, so presence
// alone means nothing — only the sequence comparison does.
type WALStatus struct {
	// Files is the total number of *.wal objects.
	Files int
	// MaxSeq is the highest WAL file sequence present (0 if none).
	MaxSeq uint64
	// Unsnapshotted are the WAL sequences above the snapshot high-water mark:
	// data present in the WAL but in no live parquet file. Non-empty means a
	// parquet-only migration silently drops these writes — up to ~10 minutes
	// of the newest data, and a clean server shutdown does NOT flush them
	// (no snapshot happens on shutdown).
	Unsnapshotted []uint64
}

// CheckWAL lists {node}/wal and compares against the newest snapshot's
// wal_file_sequence_number (pass 0 when the store has no snapshots — then
// every WAL file is unsnapshotted). The newest unsnapshotted file's header is
// verified so a foreign object can't masquerade as pending data.
func CheckWAL(f vfs.FS, node string, snapshotWALSeq uint64) (*WALStatus, error) {
	names, err := f.List(node + "/wal")
	if err != nil {
		return nil, err
	}
	st := &WALStatus{}
	newest := ""
	for _, name := range names {
		stem, ok := strings.CutSuffix(path.Base(name), ".wal")
		if !ok {
			continue
		}
		seq, err := strconv.ParseUint(stem, 10, 64)
		if err != nil {
			continue
		}
		st.Files++
		if seq > st.MaxSeq {
			st.MaxSeq = seq
		}
		if seq > snapshotWALSeq {
			st.Unsnapshotted = append(st.Unsnapshotted, seq)
			if newest == "" || seq >= st.MaxSeq {
				newest = name
			}
		}
	}
	if newest != "" {
		r, size, err := f.ReaderAt(newest)
		if err != nil {
			return nil, fmt.Errorf("open WAL file %s: %w", newest, err)
		}
		defer r.Close()
		hdr := make([]byte, len(walMagic))
		if size < int64(len(hdr)) {
			return nil, fmt.Errorf("WAL file %s is only %d bytes; not an InfluxDB 3 WAL", newest, size)
		}
		if _, err := r.ReadAt(hdr, 0); err != nil {
			return nil, fmt.Errorf("read WAL file %s: %w", newest, err)
		}
		if !bytes.Equal(hdr, walMagic) {
			return nil, fmt.Errorf("WAL file %s does not start with the %q magic; not an InfluxDB 3 WAL", newest, walMagic)
		}
	}
	return st, nil
}
