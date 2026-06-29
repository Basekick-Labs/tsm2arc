# tsm2arc

[![CI](https://github.com/basekick-labs/tsm2arc/actions/workflows/ci.yml/badge.svg)](https://github.com/basekick-labs/tsm2arc/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/basekick-labs/tsm2arc?sort=semver)](https://github.com/basekick-labs/tsm2arc/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/basekick-labs/tsm2arc.svg)](https://pkg.go.dev/github.com/basekick-labs/tsm2arc)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Migrate **InfluxDB 1.x (1.7/1.8) and 2.x (2.0–2.7)** data into
[Arc](https://github.com/basekick-labs/arc) by reading TSM **and WAL** files
**directly off disk** — no running `influxd` required.

The on-disk TSM/WAL format is the same across 1.x and 2.x; tsm2arc auto-detects
the layout. For 2.x it resolves bucket IDs to readable names from `influxd.bolt`
and skips the `_monitoring`/`_tasks` system buckets.

Built for the case where InfluxDB data sits on cold/unmounted volumes (e.g. EBS
snapshots) that can be mounted read-only but are not served by any InfluxDB
instance. tsm2arc decodes the TSM/WAL block codecs natively in Go, reconstructs
multi-field line-protocol points, and streams them into Arc's
`/api/v1/import/lp` endpoint in resumable, size-bounded, parallel chunks.

> **Reusable beyond Arc.** The extraction side produces standard InfluxDB line
> protocol; the Arc sink is just the first sink. The TSM/WAL decoder is
> Apache-2.0 and can be reused to migrate InfluxDB 1.x data into other systems —
> contributions of new sinks (ClickHouse, QuestDB, TimescaleDB, …) are welcome.
> See [CONTRIBUTING.md](CONTRIBUTING.md).

## Install

Download a prebuilt binary from [Releases](https://github.com/basekick-labs/tsm2arc/releases)
(Linux/macOS/Windows, amd64/arm64), or:

```bash
# from source (Go 1.25+)
go install github.com/basekick-labs/tsm2arc/cmd/tsm2arc@latest

# container
docker run --rm ghcr.io/basekick-labs/tsm2arc:latest --version
```

Each release ships an SBOM (SPDX) and `checksums.txt`; the container image is
multi-arch (linux amd64/arm64) on GHCR.

## Status

**Feature-complete: all phases shipped.**

| Phase | What | State |
|---|---|---|
| 0 | InfluxDB 1.8 fixture (compose + seed) | ✅ |
| 1 | Native TSM reader + field-rejoin + LP encode + `--dry-run` | ✅ |
| 2 | Chunked gzip POST to Arc `/api/v1/import/lp` (per-DB routing) | ✅ |
| 3 | SQLite checkpoint + crash-safe resume | ✅ |
| 4 | WAL (`.wal`) reader — merged with TSM per shard | ✅ |
| 5 | Parallel workers (`--workers`) + live progress reporting | ✅ |

The TSM codecs (timestamp, float, integer, unsigned, boolean, string) are
validated against the **real InfluxDB 1.7.11 encoder** in unit tests — see
`internal/tsm/decode_test.go` and `file_test.go`.

## Quick start (dry-run)

```bash
go build ./cmd/tsm2arc

# InfluxDB 1.x — point at the data dir (or its parent; layout is auto-detected)
./tsm2arc --datadir /var/lib/influxdb --dry-run --sample 10

# InfluxDB 2.x — point at the v2 root (~/.influxdbv2 or the mounted volume);
# engine/data, engine/wal, and influxd.bolt (for bucket names) are auto-detected
./tsm2arc --datadir /var/lib/influxdb2 --dry-run --sample 10
```

Dry-run discovers shards, decodes every block, reconstructs points, and prints
per-database/bucket counts + sample line protocol — **without writing to Arc**.
This is the safe first contact with the source data.

tsm2arc auto-detects 1.x vs 2.x. You can point `--datadir` at the version's
natural root and it resolves the rest:

- **1.x:** the InfluxDB root (containing `data/`), or `…/data` directly.
- **2.x:** the v2 root (containing `engine/` and `influxd.bolt`), `…/engine`, or
  `…/engine/data` directly. Bucket names come from `influxd.bolt`; override its
  location with `--bolt` if it lives elsewhere. Without it, buckets fall back to
  their 16-hex IDs as database names.

## Load into Arc

```bash
./tsm2arc \
  --datadir /mnt/influxdb/data \
  --waldir  /mnt/influxdb/wal \     # IMPORTANT: include the WAL (see below)
  --arc-url https://arc.example.net \
  --token   "$ARC_TOKEN" \          # admin-tier token (or ARC_TOKEN env)
  --verbose

# options:
#   --waldir DIR             InfluxDB WAL directory — read un-flushed data too
#                            (auto-detected for 2.x as engine/wal)
#   --bolt PATH              InfluxDB 2.x influxd.bolt for bucket names (auto-detected)
#   --workers N              concurrent shards to migrate (default 2)
#   --db-map old=new         rename a source DB/bucket to a different Arc DB (repeatable)
#   --database-filter db     migrate only this source DB/bucket (repeatable)
#   --chunk-bytes N          raw-LP bytes per import request, <500MB (default 450MB)
#   --checkpoint PATH        SQLite resume store (default tsm2arc.checkpoint.db)
#   --start / --end          RFC3339 UTC time filters
#   --precision ns|us|ms|s   source timestamp precision (default ns)
#   --include-internal       also migrate InfluxDB 1.x's _internal database
#                            (2.x system buckets _monitoring/_tasks are always skipped)
#   --dry-run                extract + count, do not write to Arc
#   --sample N               print N sample LP lines per DB in --dry-run (default 5)
#   --verbose                per-shard / per-chunk logging
#   --version                print version and exit
```

### Parallelism and the `--workers` knob

`--workers N` migrates N shards concurrently. Shards are fully independent (each
has its own chunk sequence and checkpoint rows), so this scales cleanly. A live
heartbeat reports shards-done, chunks, rows, MB, and throughput as the migration
runs.

Size `--workers` against the **Arc node's** memory, not the migration host's:
Arc's import endpoint buffers each request fully in memory (~`chunk-bytes`
decompressed + parsed records), so peak transient Arc-side memory is roughly
`workers × ~1–1.3 GB` at the default 450 MB chunk size. The default of 2 is
conservative; raise it deliberately if the Arc node has headroom (the customer's
big dedicated migration host is rarely the bottleneck — Arc is). Resume and
correctness are unaffected by the worker count.

### Always pass `--waldir`

InfluxDB does **not** flush the write-ahead log to TSM on shutdown — small or
recently-written shards can live entirely in `.wal` files. On a cold/unmounted
volume this is common. When `--waldir` is given, tsm2arc discovers and migrates
WAL-only shards (a shard with no `.tsm` but non-empty `.wal`). Without
`--waldir`, it sees only `.tsm` files, so a WAL-only shard has nothing to find
and its data never reaches Arc. (For 2.x, `--waldir` is auto-detected from
`engine/wal`.)

With `--waldir`, tsm2arc reads both sources and field-rejoins them per shard. If a
point exists in both a TSM file and the WAL (a partially-compacted shard), the WAL
value wins (last-write-wins, matching InfluxDB and Arc compaction). The WAL
directory mirrors the data directory layout: `<waldir>/<db>/<rp>/<shard>/*.wal`.

Each source InfluxDB database is routed to the Arc database of the same name
(override with `--db-map`). Data is sent in gzipped chunks bounded at
`--chunk-bytes` of raw line protocol — Arc's import endpoint caps requests at
500 MB of **decompressed** LP, so the default 450 MB leaves headroom. Transient
failures (429, 5xx, network) are retried with exponential backoff; 4xx errors
are permanent and abort the run.

### Crash-safe resume

A load is **resumable**. Progress is tracked per `(source database, shard)` in a
SQLite checkpoint file (`--checkpoint`, default `tsm2arc.checkpoint.db`). Each
chunk's progress is committed only **after** Arc returns 2xx (and Arc's import
handler flushes to storage before returning, so 2xx means durably persisted).

If a migration is interrupted — process killed, network drop, host reboot — just
**re-run the exact same command**. tsm2arc:

- skips any shard already fully migrated (no re-extraction),
- for a partially-migrated shard, re-derives its chunks deterministically and
  resumes sending from the first un-acknowledged chunk.

Because chunk boundaries are a deterministic function of a shard's extraction
order and `--chunk-bytes`, a resumed shard produces byte-identical chunks, so the
skip is exact. The only duplication window is a crash *between* Arc persisting a
chunk and tsm2arc recording it: on resume that single chunk is re-sent. Arc
compaction collapses the duplicate for tag-bearing series; tagless series retain
at most one chunk of duplicate rows per shard per crash (see
[docs/DESIGN.md](docs/DESIGN.md) §6). A clean (uninterrupted) run produces zero
duplicates.

> The checkpoint file is safe to keep between runs and is how resume works — keep
> it alongside the migration. Delete it only to force a full re-migration from
> scratch.

## Validate against a local InfluxDB

```bash
cd fixture
docker compose up -d
./seed.sh                       # writes a known dataset (all field types,
                                # tagless measurement, pre-epoch timestamp,
                                # two databases)
docker compose restart influxdb # clean shutdown flushes WAL → TSM

# extract and compare against the oracle printed by seed.sh
../tsm2arc --datadir ./data/influxdb/data --waldir ./data/influxdb/wal \
           --dry-run --sample 20
```

## Design

Full design + the verified Arc ingest constraints (500 MB import cap on
decompressed bytes, admin auth, append-only ingest with compaction-time dedupe,
resume protocol) are in [docs/DESIGN.md](docs/DESIGN.md).

### How extraction works

InfluxDB stores **each field of a point as a separate TSM key**
(`cpu,host=a#!~#usage` and `cpu,host=a#!~#cores`), each with its own
timestamp+value stream. tsm2arc:

1. parses the TSM index (header/index/footer) for each file;
2. decodes every block via native Go codec implementations;
3. **rejoins fields by (series, timestamp)** so multi-field points are
   reconstructed as single line-protocol lines;
4. emits line protocol with original nanosecond timestamps (pre-1970 supported),
   routed to the Arc database matching the source InfluxDB database.

## Development

```bash
go test ./...          # includes round-trip tests vs the real influx encoder
go vet ./...
gofmt -l .
```

`github.com/influxdata/influxdb v1.7.11` is a **test-only** dependency (the
oracle for codec validation); the production binary does not link it.
