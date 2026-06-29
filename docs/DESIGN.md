# tsm2arc — InfluxDB 1.7 TSM → Arc Migration Tool

**Status:** Implemented
**Author:** Basekick Labs

---

## 1. Problem

The motivating case: terabytes of InfluxDB 1.7/1.8 data sitting on cold,
unmounted volumes (e.g. EBS snapshots) that are **not attached to any running
InfluxDB instance**. The volumes can be mounted to a host's filesystem
(read-only) but the data is cold — there is no `influxd` process serving it. We
need to extract the data as InfluxDB line protocol and load it into Arc, at TB
scale, reliably, resumably.

### Constraints (verified against Arc source, 2026-06-26)

| Constraint | Value | Source |
|---|---|---|
| LP import endpoint | `POST /api/v1/import/lp` | `internal/api/import.go:101` |
| Import auth tier | **admin** token | `import.go:97` (`withAdminAuth`) |
| Import size limit | **500 MB**, enforced on **both compressed and decompressed** bytes | `import.go:186,206`; const `maxImportSize` `import_inprocess.go:25` |
| Request format | `multipart/form-data`, field name `file` | `import.go:157` |
| Gzip | auto-detected by magic bytes `0x1f 0x8b`; gzip saves bandwidth only, NOT logical chunk size | `import.go:194-203` |
| DB selection | `x-arc-database` header (preferred) or `?db=` | `import.go:114-117` |
| Precision | `?precision=ns\|us\|ms\|s`, default `ns` | `import.go:148-154` |
| Server-side buffering | whole file `io.ReadAll` into memory, then `ParseBatchWithPrecision` materializes all records | `import.go:179,222` |
| Dedupe model | **append-only** ingest; dedupe only at compaction, **conditional on `arc:tags`** metadata | `compaction/dedup.go:113-200`; `arrow_writer.go:507-514` |

**Key derived facts:**
- The 500 MB cap is on **decompressed** LP. Gzip does not let us pack more logical data per request. Chunk threshold is therefore **450 MB of raw LP** (10% headroom for size-estimation slop).
- Each in-flight chunk costs the Arc server ~450 MB + parsed-record overhead **transiently in memory**. Worker concurrency must be bounded to avoid OOMing the customer's Arc node.
- Arc never dedupes on ingest and never dedupes at query time. Compaction dedupes only tag-bearing series. **The tool must be correct without relying on compaction.** (This append-with-compaction-dedupe model is documented in the Arc docs.)

---

## 2. Goals / Non-goals

### Goals
- Read InfluxDB 1.7 TSM + WAL files directly from a mounted (read-only) data directory, no running InfluxDB.
- Emit valid InfluxDB line protocol with nanosecond timestamps.
- Push to Arc `/api/v1/import/lp` in chunks safely under the 500 MB decompressed cap, gzipped.
- **Resumable**: survive crashes / interruptions over a multi-hour TB migration without re-pushing already-confirmed data on a clean resume.
- Bounded resource use on both the migration host and the Arc server.
- Observable: progress, throughput, per-shard status, errors.

### Non-goals (v1)
- Live/continuous replication (this is a one-shot bulk migration).
- Schema transformation beyond the natural InfluxDB→LP→Arc mapping.
- Multi-host distributed migration (single host, parallel per-shard is sufficient; revisit if throughput-bound).
- Dedupe on the tool side (Arc compaction handles tag-bearing series; tagless duplicates only arise from crash-resume overlap, which strict per-chunk atomicity prevents on a clean run).

---

## 3. InfluxDB 1.7 on-disk format (what we parse)

### Directory layout

**1.x:**
```
<datadir>/data/<db>/<rp>/<shardID>/*.tsm     # compacted data (the bulk)
<datadir>/wal/<db>/<rp>/<shardID>/*.wal      # recent uncompacted writes
```

**2.x** (same file format; different paths + names):
```
<root>/engine/data/<bucket-id>/autogen/<shardID>/*.tsm
<root>/engine/wal/<bucket-id>/autogen/<shardID>/*.wal
<root>/influxd.bolt                            # BoltDB: bucket-id → name
```
The on-disk TSM key (`measurement,tags#!~#field`), block codecs, and WAL framing
are **byte-identical** between 1.x and 2.x — verified against real InfluxDB 2.7
data and the v2 `influxd inspect dump-tsm` source (which splits keys on `#!~#`).
The measurement/field are NOT stored as the `\x00`/`\xff` tags — those live only
in the TSI query path, not the TSM key. So the whole decoder/parser is reused;
2.x adds only (a) layout auto-detection (`discover.Detect`), (b) bucket-id→name
resolution from `influxd.bolt` (`internal/buckets`, read from a copy so a locked
/ read-only original is fine), and (c) skipping `_monitoring`/`_tasks` system
buckets.

Both trees must be read for completeness. Cold/unmounted volumes that were cleanly shut down may have little/no WAL, but we read it regardless.

### TSM file format (v1)
A TSM file is: `header | blocks | index | footer`.
- **Header**: magic `0x16D116D1` (4 bytes) + version (1 byte).
- **Blocks**: each block is a compressed run of values for one series-field key. Block = `CRC32 (4B) | type (1B) | uvarint(tsLen) | timestamp bytes | value bytes` (the value section runs to the end of the block; its length is implied).
- **Index** (at foot): for each key → list of `(blockType, minTime, maxTime, offset, size)` entries. Lets us seek to any series without scanning all blocks.
- **Footer**: 8 bytes = offset of the index.

**Series key encoding:** `measurement,tagk=tagv,...` + the 4-byte field separator `#!~#` + `fieldName` (InfluxDB's `keyFieldSeparator`). We split on `#!~#` to recover (measurement+tags, field). Tag/measurement tokens use line-protocol backslash escaping (`\,`, `\ `, `\=`); unescaping must collapse ONLY those sequences and preserve any other literal backslash (e.g. a `C:\Users` tag value), mirroring InfluxDB's `models.unescapeTag`/`unescapeMeasurement` exactly. Multiple fields of the same point are stored as **separate keys** sharing identical timestamps — we must **re-join fields by (series, timestamp)** to reconstruct multi-field LP points (otherwise we emit one LP line per field, which is valid but bloats output and breaks point identity).

### Block codecs (the part we port from InfluxData tsdb/engine/tsm1)
Timestamps and values are independently compressed. Encodings to support:
- **Timestamps**: run-length / simple8b / uncompressed (delta-of-delta).
- **Float**: Gorilla XOR compression.
- **Integer / Unsigned**: zig-zag + simple8b / RLE.
- **Boolean**: bit-packed.
- **String**: Snappy-compressed length-prefixed.

These codecs are stable across all of InfluxDB 1.x and 2.x. They are the highest-risk part (correctness of bit-level decoders), so they are validated against the real InfluxDB 1.7.11 encoder as a test-only oracle (`internal/tsm/decode_test.go`, `file_test.go`) and cross-checked against real InfluxDB 2.7 data.

### WAL file format
Sequence of entries: `type (1B) | len (4B, big-endian) | snappy-compressed payload`. (There is no per-entry CRC field in the segment framing — matching InfluxDB's `WALSegmentReader`.) Write-type entries (`0x01`) contain values keyed by series+field, same key scheme as TSM, stored **raw** (8-byte timestamp + raw value, not the TSM block codecs). Delete (`0x02`) / DeleteRange (`0x03`) entries are tombstones and skipped. We decode write entries and feed them through the same field-rejoin path. A truncated final entry (an unclean WAL) is tolerated — we stop cleanly with everything decoded so far.

---

## 4. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ migration host (mounts EBS volume read-only)                      │
│                                                                   │
│  shard discovery ──► work queue (one item per shard)              │
│                          │                                        │
│        ┌─────────────────┼─────────────────┐                     │
│        ▼                 ▼                 ▼   (N bounded workers) │
│   ┌─────────┐       ┌─────────┐       ┌─────────┐                 │
│   │ shard A │       │ shard B │       │ shard C │                 │
│   │ TSM+WAL │       │  ...    │       │  ...    │                 │
│   │ reader  │       │         │       │         │                 │
│   │   │     │       │         │       │         │                 │
│   │ field-rejoin (series,ts)→fields                              │
│   │   │     │                                                     │
│   │ LP encode (precision=ns)                                      │
│   │   │     │                                                     │
│   │ chunk accumulator (flush when next line > 450MB raw)          │
│   │   │     │                                                     │
│   │ gzip ──► multipart POST /api/v1/import/lp ──► Arc            │
│   │   │     │   (backoff on 429/5xx, fail on 4xx)                │
│   │ on 2xx: commit checkpoint(shard, cursor)                      │
│   └─────────┘                                                     │
│                          │                                        │
│                   checkpoint.db (SQLite, WAL mode)                │
└─────────────────────────────────────────────────────────────────┘
```

### Module layout (Go, standalone module)
```
tsm2arc/
  go.mod                      module github.com/basekick-labs/tsm2arc
  cmd/tsm2arc/main.go         CLI, flag parsing, wiring
  internal/tsm/               TSM v1 reader: header, index, block decoders
    reader.go
    index.go
    timestamp.go  float.go  integer.go  boolean.go  string.go   (codecs)
  internal/wal/               WAL entry decoder
  internal/series/            key parse + field-rejoin by (series, ts)
  internal/lp/                record → line-protocol encoder (ns)
  internal/chunk/             byte-bounded accumulator + flush boundaries
  internal/sink/              multipart gzip POST client, retry/backoff
  internal/checkpoint/        SQLite cursor store
  internal/discover/          walk datadir/waldir → shard work items
  internal/progress/          throughput + per-shard status reporting
  docs/DESIGN.md              (this file)
  docs/RUNBOOK.md             operator steps (written with the tool)
```

---

## 5. Chunking & the 450 MB boundary

- The accumulator appends encoded LP lines and tracks a running **byte count of the raw (uncompressed) buffer**.
- Before appending line L, if `currentBytes + len(L) + 1 (newline) > 450 MB`, **flush first**, then start a new chunk with L.
- This guarantees every flushed chunk's decompressed size ≤ 450 MB < 500 MB cap. No single LP line can exceed the cap (InfluxDB points are tiny), so the "next line would overflow" rule is always satisfiable.
- Flush = gzip the buffer → multipart POST → on 2xx, advance checkpoint → reset buffer.
- Gzip ratio on telemetry LP is typically 5–10×, so ~450 MB raw → ~50–90 MB on the wire. Bandwidth-friendly; the cap is logical, not network.

---

## 6. Resume protocol (the correctness core)

Arc is append-only and only dedupes tag-bearing series at compaction. Therefore **the tool, not Arc, owns idempotency.** Rules:

1. **Chunk is the atomic unit.** A chunk is fully POSTed and acknowledged (2xx) or it is not. There is no partial-chunk commit.
2. **Deterministic chunk boundaries.** For a given shard, iteration order over series keys and timestamps is fully deterministic (sorted series key, then ascending time). The 450 MB cut points are a pure function of the input. So re-running a shard produces **byte-identical chunks in the same order**.
3. **Checkpoint = `(sourceDB, shardID, chunkSeq)` committed only after 2xx** for that chunk. Checkpoint write is a single SQLite transaction (WAL mode, synchronous=FULL) so it's crash-atomic. The source database is part of the key because a shard path is `<db>/<rp>/<shardID>` and shardIDs are only unique within a db.
4. **On resume**, for each `(sourceDB, shard)`, read its last committed `chunkSeq`, re-derive chunks from the start, and **skip** chunks `≤ chunkSeq` without POSTing them. Begin POSTing at `chunkSeq+1`.
5. **Crash window:** the only overlap risk is a chunk that Arc *received and persisted* but whose 2xx we never recorded (crash between Arc's commit and our checkpoint write). On resume we re-POST that one chunk → duplicate rows for that chunk only.
   - For **tag-bearing series**: compaction collapses the overlap (last-write-wins, identical rows). Correct end state.
   - For **tagless series**: the duplicate persists. Bounded to **at most one chunk per shard** (the in-flight one at crash time), and only on an unclean crash. Acceptable and documented; an optional post-migration verification (§8) detects it.

This is why boundaries must be deterministic: a resumed chunk must be byte-identical so that (a) the skip math is exact and (b) any re-pushed overlap is collapsible by compaction where tags exist.

### Why not finer-grained (per-series) checkpointing?
Per-chunk is the natural atomic unit because the chunk is what Arc accepts atomically. Per-series checkpointing would require Arc-side partial-chunk acknowledgement, which the import endpoint does not provide (it's all-or-nothing per request). Per-chunk keeps the checkpoint table tiny (one row per shard) and the resume logic trivial.

---

## 7. Concurrency & resource limits

### Arc server side (the binding constraint)
Each import request makes Arc `io.ReadAll` the (≤450 MB) decompressed file into memory **and** materialize all parsed records (`[]*models.Record`) — call it ~2–3× the raw LP size transiently per in-flight request. With `W` concurrent workers each pushing one chunk, peak Arc-side transient memory ≈ `W × ~1.0–1.3 GB`.

**Default `--workers 2`.** Conservative; ~2–2.6 GB transient on the Arc node. Operator can raise it if the Arc node is provisioned for it. The runbook will state the memory math explicitly and tell the operator how to size it against their Arc node's RAM.

### Migration host side
Each worker holds one ~450 MB raw buffer + gzip buffer. `W × ~600 MB`. Plus TSM block decode scratch. Modest; the host is not the bottleneck.

### Backpressure & retry (sink)
- **429 / 5xx**: exponential backoff with full jitter, capped retries (6 attempts, 1s→60s). 429 means Arc is shedding load.
- **4xx other than 429** (incl. 413): permanent — the run aborts with the Arc status and a snippet of the response body. Should never hit 413 given the <500 MB cap. (The failed chunk is not written to disk; the error identifies the shard and chunk seq, and resume re-derives it.)
- **Network errors / timeouts**: treated as transient, backoff+retry. Uses a tuned `http.Client` with sane timeouts, NOT `http.DefaultClient`.

---

## 8. Verification (optional post-migration)

Two cheap checks the operator can run after migration:
1. **Point-count reconciliation** per measurement: count points extracted (tool-side counter) vs `SELECT count(*)` in Arc per measurement. Exact match for tag-bearing data after compaction; tagless data may show a small positive delta = resume-overlap duplicates (bounded by §6.5).
2. **Spot-check** a few series' min/max time and a sampled value against the source (if any read access to a reference InfluxDB exists; often it won't for cold volumes).

For tagless measurements where a clean count is required, the operator can trigger a compaction pass — but note tagless series do **not** dedupe at compaction, so the only true guarantee for tagless data is a clean (non-crash) run. The tool logs every unclean resume and exactly which `(shard, chunkSeq)` was re-pushed, so any tagless duplicate is attributable and quantifiable.

---

## 9. CLI surface

```
tsm2arc \
  --datadir   /var/lib/influxdb           # required: InfluxDB root or data dir (1.x or 2.x, auto-detected)
  --waldir    /var/lib/influxdb/wal       # optional; auto-detected for 2.x (engine/wal)
  --bolt      /var/lib/influxdb2/influxd.bolt  # optional (2.x): bucket-id→name map; auto-detected
  --arc-url   https://arc.example.net     # required (unless --dry-run)
  --token     $ARC_ADMIN_TOKEN            # required (admin tier); or ARC_TOKEN env
  --database-filter <db|bucket>           # optional: migrate only this source DB/bucket (repeatable)
  --db-map old=new                        # optional: rename source DB/bucket → Arc db (repeatable; default identity)
  --start 1959-12-16T00:00:00Z            # optional time FILTER (RFC3339, UTC) — skips out-of-range points
  --end   2024-06-01T00:00:00Z            # optional time FILTER
  --precision ns                          # value sent to Arc's import endpoint (default ns; see note)
  --workers 2                             # concurrent shards (default 2; configurable, no cap — see §7)
  --chunk-bytes 450MB                     # raw LP flush threshold; bytes or a size suffix (default 450MB, must be <500MB)
  --checkpoint ./tsm2arc.checkpoint.db    # SQLite resume store
  --include-internal                      # also migrate 1.x's _internal database
  --dry-run                               # extract + chunk + count, do NOT POST
  --sample N                              # print N sample LP lines per DB in --dry-run
  --verbose
  --version
```

- **No `--db` flag.** Target Arc database = source InfluxDB database/bucket name (passthrough), routed per-request via the `x-arc-database` header. Use `--db-map` only to rename. (1.x: db name; 2.x: bucket name resolved from `influxd.bolt`, falling back to the bucket ID.)
- `--start/--end` are **filters**, not partition controls. Arc partitions by data timestamp automatically; a pre-epoch point creates a pre-epoch partition with no special handling.
- `--precision` is the precision value forwarded to Arc's import endpoint. tsm2arc always emits **nanosecond** integer timestamps (TSM/WAL store ns), so this should normally stay `ns`.
- `--dry-run` validates TSM/WAL parsing and chunk sizing against real data **without writing to Arc** — the safe first contact with the source data.
- Resume requires the same chunk-shaping flags (`--chunk-bytes`, `--start`, `--end`, `--db-map`, `--precision`) as the run that created the checkpoint; tsm2arc refuses a resume with a different fingerprint (see §6).
- All timestamps in logs, checkpoints, and bounds are **UTC**.

---

## 10. Risks & mitigations

| Risk | Mitigation |
|---|---|
| TSM block decoder correctness (bit-level codecs) | Vertical-slice phase: round-trip a real customer-representative TSM file; `--dry-run` count vs source; unit tests per codec against known vectors. |
| Tagless series duplicates on unclean resume | Strict per-chunk atomicity bounds it to ≤1 chunk/shard/crash; logged + attributable; documented limitation. |
| OOM the customer's Arc node | Default `--workers 2`; runbook memory math; backoff on 429. |
| Field-rejoin wrong (multi-field points split) | Rejoin by (series, exact ts); test with multi-field measurements. |
| Version skew (data not actually 1.7) | TSM v1 format is stable across 1.x; reader validates header magic/version and errors clearly on mismatch. |
| Huge single shards bottleneck wall-clock | Per-shard parallelism + optional `--start/--end` to split a shard across runs. Revisit intra-shard parallelism only if measured as the bottleneck. |

---

## 11. Build history (all complete)

The tool was built in phases, all shipped:

0. **Local InfluxDB fixture** (`fixture/`): InfluxDB 1.8 and 2.7 containers seeded with known data (multi-field points, tagless measurement, multiple databases/buckets, a pre-epoch 1959 timestamp, all field types), flushed to TSM. The validation oracle — known input, asserted output.
1. **Extraction**: TSM reader → field-rejoin → LP encode → `--dry-run` with value assertions against the fixture. Codecs validated against the real InfluxDB encoder.
2. **Sink + chunking + per-DB routing**: gzip multipart POST to Arc, byte-bounded chunker, `x-arc-database` per source DB/bucket, retry/backoff.
3. **Checkpoint + resume**: SQLite store keyed on stable `(SourceID, shardID)`, deterministic boundaries, skip-on-resume, config-fingerprint guard.
4. **WAL reader**: decode `.wal`, merge into the same field-rejoin path (last-write-wins).
5. **Parallelism + progress**: bounded `--workers` across shards, live throughput reporting.
6. **InfluxDB 2.x**: layout auto-detection + bucket-id→name resolution from `influxd.bolt`.

(The numbered phases below in "Resolved decisions" record the design choices made before the build.)

---

## Resolved decisions (2026-06-26)
1. **Worker count** — fully configurable via `--workers` (no hard cap). Customer will provision a large dedicated host for the migration, so the binding constraint is the **Arc node's** RAM (each in-flight chunk buffers ~450 MB + parsed records server-side). Default stays modest (`2`) for safety; operator raises it deliberately against the Arc node's headroom. Runbook states the `W × ~1–1.3 GB` server-side math.
2. **Mapping = InfluxDB database → Arc database (namespace), passthrough.** Arc databases and measurements are just namespaces/folders — writing to a database name creates it. So: **source InfluxDB database name → Arc `db` parameter** (per request via `x-arc-database`), and **source measurement name → Arc measurement** unchanged. There is NO single global `--db`. The tool iterates source databases discovered under `<datadir>/data/<db>/...` and routes each shard's chunks to the matching Arc database. `--db-map old=new` is an optional override for renaming; default is identity.
3. **Time ordering = respect data time exactly.** Arc partitions by the **data timestamp** (`{db}/{measurement}/{year}/{month}/{day}/{hour}/`), identical to live ingestion. A pre-epoch point (e.g. 1959-12-16T00:00:00Z) lands in the matching pre-1970 partition. The tool does nothing special — it emits LP with the original nanosecond timestamp and Arc's normal ingest path files it by data time. `--start/--end` remain as optional *filters* (skip out-of-range points), NOT as a partitioning mechanism. Pre-epoch (negative-nanosecond) timestamps are supported end to end.
4. **Sample data = self-host.** Validation runs against self-hosted InfluxDB 1.8 and 2.7 containers (`fixture/`) with known seeded data, so extraction is asserted value-by-value against what was written — stronger than a blind customer-file round-trip.
