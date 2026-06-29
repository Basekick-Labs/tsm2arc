// Package checkpoint provides a crash-safe resume store for tsm2arc.
//
// The unit of progress is a (sourceDB, shardID) pair. For each shard we record
// the highest chunk sequence number that Arc has acknowledged with a 2xx. On
// resume, chunks with seq <= the committed value are skipped (re-derived but not
// re-sent), and sending continues from committed+1.
//
// Why per-shard chunk sequence: chunk boundaries are a deterministic function of
// a shard's extraction order and the byte bound, so the same shard re-extracted
// produces byte-identical chunks in the same order. The committed seq therefore
// unambiguously identifies "everything up to here is durably in Arc".
//
// Crash semantics: a chunk's checkpoint is written ONLY after its import returns
// 2xx (and Arc's import handler FlushAll()s before returning, so 2xx == durably
// persisted). The only overlap window is a crash between Arc persisting a chunk
// and us recording it: on resume that one chunk is re-sent, producing duplicate
// rows that Arc compaction collapses for tag-bearing series (tagless series
// duplicate — bounded to <=1 chunk per shard per crash; see DESIGN.md §6).
//
// The store uses SQLite in WAL mode with synchronous=FULL so each checkpoint
// commit is durable against process and OS crashes.
package checkpoint

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// Store is the SQLite-backed checkpoint database.
type Store struct {
	db *sql.DB
}

// Open opens (creating if needed) the checkpoint database at path.
func Open(path string) (*Store, error) {
	// _pragma args set WAL + FULL sync at connection time. busy_timeout avoids
	// spurious "database is locked" under the (single-process) workload.
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(5000)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// single writer; keep one connection so WAL checkpointing is simple
	db.SetMaxOpenConns(1)

	s := &Store{db: db}
	if err := s.init(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) init() error {
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS shard_progress (
			source_db   TEXT NOT NULL,
			shard_id    TEXT NOT NULL,
			committed_seq INTEGER NOT NULL,
			rows_sent   INTEGER NOT NULL DEFAULT 0,
			updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
			PRIMARY KEY (source_db, shard_id)
		);
		CREATE TABLE IF NOT EXISTS shard_done (
			source_db  TEXT NOT NULL,
			shard_id   TEXT NOT NULL,
			total_chunks INTEGER NOT NULL,
			rows_sent  INTEGER NOT NULL,
			finished_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
			PRIMARY KEY (source_db, shard_id)
		);
		CREATE TABLE IF NOT EXISTS meta (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);
	`)
	return err
}

// CheckConfig enforces that a resume uses the same migration-shaping config as
// the run that created the checkpoint. Chunk boundaries (and thus the seq-based
// skip) are a deterministic function of these inputs; resuming with a different
// fingerprint would misalign seq numbers and silently create gaps or
// over-duplication. On first use it records the fingerprint; on resume it
// returns an error if the fingerprint differs.
func (s *Store) CheckConfig(fingerprint string) error {
	var existing string
	err := s.db.QueryRow(`SELECT value FROM meta WHERE key='config_fingerprint'`).Scan(&existing)
	if err == sql.ErrNoRows {
		_, werr := s.db.Exec(
			`INSERT INTO meta (key, value) VALUES ('config_fingerprint', ?)`, fingerprint)
		return werr
	}
	if err != nil {
		return err
	}
	if existing != fingerprint {
		return fmt.Errorf("checkpoint was created with different settings (%s) than this run (%s); "+
			"resume requires identical --chunk-bytes/--start/--end/--db-map. "+
			"Use a fresh --checkpoint path or restore the original flags", existing, fingerprint)
	}
	return nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// CommittedSeq returns the highest acknowledged chunk seq for a shard, or -1 if
// the shard has no committed chunks yet. Chunks with seq <= this value are
// already durably in Arc and must be skipped on resume.
func (s *Store) CommittedSeq(sourceDB, shardID string) (int, error) {
	var seq int
	err := s.db.QueryRow(
		`SELECT committed_seq FROM shard_progress WHERE source_db=? AND shard_id=?`,
		sourceDB, shardID,
	).Scan(&seq)
	if err == sql.ErrNoRows {
		return -1, nil
	}
	if err != nil {
		return 0, err
	}
	return seq, nil
}

// IsShardDone reports whether a shard was fully completed in a prior run.
func (s *Store) IsShardDone(sourceDB, shardID string) (bool, error) {
	var n int
	err := s.db.QueryRow(
		`SELECT COUNT(1) FROM shard_done WHERE source_db=? AND shard_id=?`,
		sourceDB, shardID,
	).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// Commit records that chunk seq for a shard has been durably accepted by Arc.
// rowsDelta is the rows imported by this chunk (accumulated into rows_sent). The
// write is a single transaction; on return the checkpoint is durable.
//
// committed_seq is set to MAX(existing, seq) defensively so an out-of-order or
// replayed commit can never move progress backwards.
func (s *Store) Commit(sourceDB, shardID string, seq int, rowsDelta int64) error {
	_, err := s.db.Exec(`
		INSERT INTO shard_progress (source_db, shard_id, committed_seq, rows_sent, updated_at)
		VALUES (?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
		ON CONFLICT(source_db, shard_id) DO UPDATE SET
			committed_seq = MAX(committed_seq, excluded.committed_seq),
			rows_sent     = rows_sent + excluded.rows_sent,
			updated_at    = excluded.updated_at
	`, sourceDB, shardID, seq, rowsDelta)
	return err
}

// MarkShardDone records that a shard has been fully migrated (all chunks sent).
// totalChunks is the number of chunks the shard produced this run.
func (s *Store) MarkShardDone(sourceDB, shardID string, totalChunks int, rowsSent int64) error {
	_, err := s.db.Exec(`
		INSERT INTO shard_done (source_db, shard_id, total_chunks, rows_sent, finished_at)
		VALUES (?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
		ON CONFLICT(source_db, shard_id) DO UPDATE SET
			total_chunks = excluded.total_chunks,
			rows_sent    = excluded.rows_sent,
			finished_at  = excluded.finished_at
	`, sourceDB, shardID, totalChunks, rowsSent)
	return err
}

// RewindForTest lowers a shard's committed_seq by 1 and clears its done mark.
// It exists to simulate the crash window where Arc persisted a chunk but the
// checkpoint commit was lost. Not used in production code paths.
func (s *Store) RewindForTest(sourceDB, shardID string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(
		`UPDATE shard_progress SET committed_seq = committed_seq - 1 WHERE source_db=? AND shard_id=?`,
		sourceDB, shardID); err != nil {
		return err
	}
	if _, err := tx.Exec(
		`DELETE FROM shard_done WHERE source_db=? AND shard_id=?`,
		sourceDB, shardID); err != nil {
		return err
	}
	return tx.Commit()
}

// Summary returns aggregate progress for reporting: number of shards with any
// progress, number fully done, and total rows acknowledged.
func (s *Store) Summary() (shardsStarted, shardsDone int, rowsSent int64, err error) {
	if err = s.db.QueryRow(`SELECT COUNT(1), COALESCE(SUM(rows_sent),0) FROM shard_progress`).
		Scan(&shardsStarted, &rowsSent); err != nil {
		return
	}
	err = s.db.QueryRow(`SELECT COUNT(1) FROM shard_done`).Scan(&shardsDone)
	return
}
