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
			last_series TEXT,
			last_ts     INTEGER,
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
		CREATE TABLE IF NOT EXISTS measurement_actions (
			source_db   TEXT NOT NULL,
			shard_id    TEXT NOT NULL,
			measurement TEXT NOT NULL,
			action      TEXT NOT NULL,
			renamed_to  TEXT NOT NULL DEFAULT '',
			origin      TEXT NOT NULL DEFAULT '',
			points      INTEGER NOT NULL DEFAULT 0,
			updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
			PRIMARY KEY (source_db, shard_id, measurement)
		);
	`)
	if err != nil {
		return err
	}
	return s.migrate()
}

// migrate upgrades a pre-cursor (<= 0.1.4) checkpoint in place. Two changes:
//
//  1. shard_progress gains the resume cursor columns (last_series, last_ts);
//     rows written before the upgrade keep NULL and resume via the legacy
//     re-derive path.
//  2. measurement action counts change semantics from overwrite-at-shard-end to
//     accumulate-per-chunk. Old code wrote action rows and the done mark in two
//     separate transactions, so a crash between them can leave overwrite-style
//     rows for a shard that is NOT done; the new accumulate semantics would then
//     double-count that shard on its (full re-derive) resume. Those orphaned
//     rows are deleted — the resume re-derives the shard fully and rebuilds
//     them. Rows for DONE shards are final and kept.
//
// The one-shot marker is the schema_version meta key, so the delete cannot fire
// again on later opens (by then in-progress shards legitimately have
// accumulated rows).
func (s *Store) migrate() error {
	var v string
	err := s.db.QueryRow(`SELECT value FROM meta WHERE key='schema_version'`).Scan(&v)
	if err == nil && v == "2" {
		return nil
	}
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	var hasCursor int
	if err := s.db.QueryRow(
		`SELECT COUNT(1) FROM pragma_table_info('shard_progress') WHERE name='last_series'`,
	).Scan(&hasCursor); err != nil {
		return err
	}
	if hasCursor == 0 {
		for _, stmt := range []string{
			`ALTER TABLE shard_progress ADD COLUMN last_series TEXT`,
			`ALTER TABLE shard_progress ADD COLUMN last_ts INTEGER`,
		} {
			if _, err := s.db.Exec(stmt); err != nil {
				return err
			}
		}
	}
	if _, err := s.db.Exec(`
		DELETE FROM measurement_actions WHERE (source_db, shard_id) IN (
			SELECT p.source_db, p.shard_id FROM shard_progress p
			LEFT JOIN shard_done d ON d.source_db=p.source_db AND d.shard_id=p.shard_id
			WHERE d.source_db IS NULL
		)
	`); err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', '2')`)
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
			"resume requires identical --chunk-bytes/--start/--end/--db-map/--precision/"+
			"--measurement-map/--on-invalid-measurement. "+
			"Use a fresh --checkpoint path or restore the original flags", existing, fingerprint)
	}
	return nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// Cursor is the resume position stored with a chunk commit: the raw series key
// and timestamp of the last LP line in that chunk. Because shard extraction
// order is deterministic, it lets a resume seek directly past everything
// already acknowledged instead of re-deriving and discarding it.
type Cursor struct {
	SeriesKey string
	UnixNano  int64
}

// ActionDelta is one measurement's action point-count increment carried by a
// chunk commit (or the final shard commit): how many additional points of this
// measurement were renamed/skipped among the lines ordered at or before the
// committed chunk's cursor. Deltas ACCUMULATE into measurement_actions; because
// they travel in the same transaction as the cursor they are exactly-once
// relative to the checkpoint state.
type ActionDelta struct {
	Measurement string // source measurement name
	Action      string // "renamed" or "skipped"
	RenamedTo   string // final Arc name (renames only)
	Origin      string // "explicit" or "auto" (renames only)
	Points      int64  // additional points affected since the previous commit
}

// CommittedSeq returns the highest acknowledged chunk seq for a shard, or -1 if
// the shard has no committed chunks yet. Chunks with seq <= this value are
// already durably in Arc and must be skipped on resume.
func (s *Store) CommittedSeq(sourceDB, shardID string) (int, error) {
	seq, _, err := s.Progress(sourceDB, shardID)
	return seq, err
}

// Progress returns a shard's committed chunk seq (-1 if none) and its resume
// cursor. A nil cursor with committed >= 0 means the checkpoint predates
// cursors (written by <= 0.1.4): the resume must re-derive and skip.
func (s *Store) Progress(sourceDB, shardID string) (int, *Cursor, error) {
	var seq int
	var series sql.NullString
	var ts sql.NullInt64
	err := s.db.QueryRow(
		`SELECT committed_seq, last_series, last_ts FROM shard_progress WHERE source_db=? AND shard_id=?`,
		sourceDB, shardID,
	).Scan(&seq, &series, &ts)
	if err == sql.ErrNoRows {
		return -1, nil, nil
	}
	if err != nil {
		return 0, nil, err
	}
	if series.Valid && ts.Valid {
		return seq, &Cursor{SeriesKey: series.String, UnixNano: ts.Int64}, nil
	}
	return seq, nil, nil
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

// Commit records that chunk seq for a shard has been durably accepted by Arc,
// together with the chunk's resume cursor and the measurement-action deltas of
// the lines it covers — one transaction, so seq, cursor, and audit counts can
// never disagree after a crash. rowsDelta is the rows imported by this chunk
// (accumulated into rows_sent).
//
// committed_seq is set to MAX(existing, seq) defensively so an out-of-order or
// replayed commit can never move progress backwards; the cursor only updates
// when the seq advances (the CASE reads the pre-update row values).
func (s *Store) Commit(sourceDB, shardID string, seq int, rowsDelta int64, cur Cursor, deltas []ActionDelta) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`
		INSERT INTO shard_progress (source_db, shard_id, committed_seq, rows_sent, last_series, last_ts, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
		ON CONFLICT(source_db, shard_id) DO UPDATE SET
			last_series   = CASE WHEN excluded.committed_seq >= committed_seq THEN excluded.last_series ELSE last_series END,
			last_ts       = CASE WHEN excluded.committed_seq >= committed_seq THEN excluded.last_ts ELSE last_ts END,
			committed_seq = MAX(committed_seq, excluded.committed_seq),
			rows_sent     = rows_sent + excluded.rows_sent,
			updated_at    = excluded.updated_at
	`, sourceDB, shardID, seq, rowsDelta, cur.SeriesKey, cur.UnixNano); err != nil {
		return err
	}
	if err := applyDeltas(tx, sourceDB, shardID, deltas); err != nil {
		return err
	}
	return tx.Commit()
}

// applyDeltas accumulates measurement-action point deltas within tx.
func applyDeltas(tx *sql.Tx, sourceDB, shardID string, deltas []ActionDelta) error {
	for _, d := range deltas {
		if _, err := tx.Exec(`
			INSERT INTO measurement_actions (source_db, shard_id, measurement, action, renamed_to, origin, points, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
			ON CONFLICT(source_db, shard_id, measurement) DO UPDATE SET
				action     = excluded.action,
				renamed_to = excluded.renamed_to,
				origin     = excluded.origin,
				points     = points + excluded.points,
				updated_at = excluded.updated_at
		`, sourceDB, shardID, d.Measurement, d.Action, d.RenamedTo, d.Origin, d.Points); err != nil {
			return err
		}
	}
	return nil
}

// FinishShard records that a shard has been fully migrated: any action deltas
// for lines after the last chunk commit (including trailing skipped points that
// never entered a chunk), then the done mark — one transaction, so a done shard
// always has its complete audit trail. totalChunks is the shard's total chunk
// count. The done row's rows_sent is read from shard_progress, which spans
// every run that contributed (a resumed shard's earlier rows included).
func (s *Store) FinishShard(sourceDB, shardID string, totalChunks int, finalDeltas []ActionDelta) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := applyDeltas(tx, sourceDB, shardID, finalDeltas); err != nil {
		return err
	}
	var rows int64
	err = tx.QueryRow(
		`SELECT rows_sent FROM shard_progress WHERE source_db=? AND shard_id=?`,
		sourceDB, shardID,
	).Scan(&rows)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if _, err := tx.Exec(`
		INSERT INTO shard_done (source_db, shard_id, total_chunks, rows_sent, finished_at)
		VALUES (?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
		ON CONFLICT(source_db, shard_id) DO UPDATE SET
			total_chunks = excluded.total_chunks,
			rows_sent    = excluded.rows_sent,
			finished_at  = excluded.finished_at
	`, sourceDB, shardID, totalChunks, rows); err != nil {
		return err
	}
	return tx.Commit()
}

// MeasurementReportRow aggregates one (source db, measurement) action across
// all shards recorded so far (including prior resumed runs).
type MeasurementReportRow struct {
	SourceDB    string
	Measurement string
	Action      string
	RenamedTo   string
	Origin      string
	Points      int64
	Shards      int
}

// MeasurementReport returns the aggregated measurement action log, ordered by
// source db, then action, then measurement — the end-of-run audit summary.
func (s *Store) MeasurementReport() ([]MeasurementReportRow, error) {
	rows, err := s.db.Query(`
		SELECT source_db, measurement, action, renamed_to, origin, SUM(points), COUNT(1)
		FROM measurement_actions
		GROUP BY source_db, measurement, action, renamed_to, origin
		ORDER BY source_db, action, measurement
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MeasurementReportRow
	for rows.Next() {
		var r MeasurementReportRow
		if err := rows.Scan(&r.SourceDB, &r.Measurement, &r.Action, &r.RenamedTo, &r.Origin, &r.Points, &r.Shards); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RewindForTest lowers a shard's committed_seq by 1 and clears its done mark,
// restoring the previous chunk's cursor (nil = the pre-cursor NULL state, which
// also exercises the legacy re-derive path). It simulates the crash window
// where Arc persisted a chunk but the checkpoint commit was lost: in production
// seq, cursor, and action deltas commit atomically, so the lost commit loses
// all three together — the resume then re-derives that chunk's rows AND its
// deltas exactly once. Callers asserting audit counts must therefore also
// rewind the chunk's deltas if they applied any. Not used in production paths.
func (s *Store) RewindForTest(sourceDB, shardID string, prevCur *Cursor) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var series any
	var ts any
	if prevCur != nil {
		series, ts = prevCur.SeriesKey, prevCur.UnixNano
	}
	if _, err := tx.Exec(
		`UPDATE shard_progress SET committed_seq = committed_seq - 1, last_series = ?, last_ts = ?
		 WHERE source_db=? AND shard_id=?`,
		series, ts, sourceDB, shardID); err != nil {
		return err
	}
	if _, err := tx.Exec(
		`DELETE FROM shard_done WHERE source_db=? AND shard_id=?`,
		sourceDB, shardID); err != nil {
		return err
	}
	return tx.Commit()
}

// ClearCursorsForTest nulls every stored resume cursor, reshaping the
// checkpoint to what a <= 0.1.4 run would have written — the fixture for
// exercising the legacy re-derive-and-skip resume path. Not used in production.
func (s *Store) ClearCursorsForTest() error {
	_, err := s.db.Exec(`UPDATE shard_progress SET last_series = NULL, last_ts = NULL`)
	return err
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
