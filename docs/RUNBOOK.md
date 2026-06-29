# tsm2arc Migration Runbook

Operator guide for migrating InfluxDB 1.7/1.8 data into Arc with `tsm2arc`.
This covers the common case: **terabytes of InfluxDB data on a cold/unmounted
volume** (e.g. an EBS snapshot) that is not served by any running `influxd`.

> Read [DESIGN.md](DESIGN.md) for *why* the tool works the way it does. This
> runbook is the *how*.

---

## 0. Before you start — checklist

- [ ] The InfluxDB data volume is mounted **read-only** on the migration host.
- [ ] You know the path to the `data` directory (`.../influxdb/data`) and the
      `wal` directory (`.../influxdb/wal`).
- [ ] You have an **admin-tier** Arc API token (the import endpoint requires admin).
- [ ] You know the Arc base URL and that the network path to it is open.
- [ ] You have disk space for the SQLite checkpoint file (tiny — KB/MB).
- [ ] You have built or downloaded the `tsm2arc` binary matching this host's OS/arch.

> **Mount read-only.** `tsm2arc` only ever reads the InfluxDB files, but mount
> with `-o ro` so a mistake can't damage the source. Example:
> `mount -o ro /dev/xvdf1 /mnt/influx`

---

## 1. Understand the source layout

tsm2arc supports **both InfluxDB 1.x and 2.x** and auto-detects which one. The
TSM/WAL file format is identical between them; only the directory layout and
naming differ.

**InfluxDB 1.x:**

```
<root>/data/<database>/<retention-policy>/<shard-id>/*.tsm   ← compacted data
<root>/wal/<database>/<retention-policy>/<shard-id>/*.wal    ← un-flushed data
```

Point `--datadir` at `<root>` or `<root>/data`.

**InfluxDB 2.x** (default root `~/.influxdbv2`, or the mounted volume):

```
<root>/engine/data/<bucket-id>/autogen/<shard-id>/*.tsm     ← compacted data
<root>/engine/wal/<bucket-id>/autogen/<shard-id>/*.wal      ← un-flushed data
<root>/influxd.bolt                                          ← bucket id→name map
```

Point `--datadir` at `<root>` (containing `engine/` and `influxd.bolt`). tsm2arc
resolves `engine/data`, `engine/wal`, and reads `influxd.bolt` to map bucket IDs
to names (so Arc databases are named like the buckets), skipping the
`_monitoring`/`_tasks` system buckets.

> **2.x: grab `influxd.bolt`.** On a cold-volume migration, copy `influxd.bolt`
> alongside the `engine/` directory. Without it, tsm2arc can't recover bucket
> names: it migrates buckets under their 16-hex IDs and **cannot skip the
> `_monitoring`/`_tasks` system buckets** (it warns loudly when this happens).
> Use `--bolt PATH` if it isn't at the auto-detected location. (tsm2arc reads a
> copy of the bolt file, so a locked or read-only original is fine; a bad `--bolt`
> path warns and falls back to IDs rather than aborting.)
>
> Resume is robust to the bolt being present on one run and absent on another:
> the checkpoint keys on the stable bucket **ID**, not the resolved name, so a
> missing bolt on a resume won't re-migrate already-done buckets. (It would,
> however, write any *remaining* buckets under their hex IDs — so for consistent
> Arc database names, keep the bolt available for the whole migration.)

Confirm what you have:

```bash
# databases present
ls /mnt/influx/data

# shards and TSM files for one database
find /mnt/influx/data/<db> -name '*.tsm' | head

# WAL files (data not yet flushed to TSM)
find /mnt/influx/wal/<db> -name '*.wal' -size +0c
```

> **Why the WAL matters.** InfluxDB does **not** flush the WAL to TSM on
> shutdown. A cold volume routinely has recent or small shards living *only* in
> `.wal` files. If you omit `--waldir`, that data is silently skipped. **Always
> pass `--waldir`.**

---

## 2. Dry run — the safe first contact

Always start with `--dry-run`. It decodes everything and reports per-database
counts and sample line protocol **without writing to Arc**. This validates that
the TSM/WAL codecs handle your data shape before anything is sent.

```bash
tsm2arc \
  --datadir /mnt/influx/data \
  --waldir  /mnt/influx/wal \
  --dry-run --sample 10
```

Check the output:

- **Databases** listed match what you expect (note: `_internal` is skipped by
  default — that's InfluxDB's own monitoring DB; pass `--include-internal` only
  if you truly want it).
- **points / fields / keys** are non-zero and roughly the magnitude you expect.
- **skipped-keys** should be 0 or explainable (keys with no field separator).
- **time range** looks sane (watch for surprising min/max — pre-1970 timestamps
  are supported and will show as negative epoch / pre-1970 dates).
- **sample line protocol** lines look correct: right measurement names, tags,
  field types (`i` integer, `u` unsigned, quoted strings, true/false booleans).

If a database you expected is missing, check whether its data is WAL-only and
whether you passed `--waldir`.

---

## 3. Scope the migration (optional)

You can migrate a subset first to validate the round-trip end to end:

```bash
# one database only
tsm2arc ... --database-filter telemetry --dry-run

# a time window (RFC3339, UTC) — these are FILTERS, not partitioning
tsm2arc ... --start 2024-01-01T00:00:00Z --end 2024-02-01T00:00:00Z --dry-run
```

Mapping:

- By default each source InfluxDB **database** maps to an Arc **database** of the
  same name (Arc databases are namespaces). Measurement names pass through
  unchanged.
- Use `--db-map old=new` to rename (repeatable), e.g.
  `--db-map telemetry=telemetry_prod`.

---

## 4. Size `--workers` against the Arc node

`--workers N` migrates N shards concurrently (default 2). **The binding
constraint is the Arc node's memory, not the migration host's.** Arc's import
endpoint buffers each request fully in memory while parsing, so peak transient
Arc-side memory is roughly:

```
workers × (~1 to 1.3 GB)      at the default 450 MB chunk size
```

Guidance:

- Default `2` is safe for almost any Arc node.
- If the Arc node has plenty of RAM headroom, raise it (e.g. `4`–`8`) for
  throughput. The big dedicated migration host is rarely the bottleneck — Arc is.
- If you see Arc return 429s or memory pressure, lower `--workers` (the tool
  backs off on 429 automatically, but fewer workers reduces peak pressure).
- `--workers` has **no effect on correctness or resume** — only speed.

You can also lower `--chunk-bytes` to reduce per-request memory (e.g.
`--chunk-bytes 200MB`), at the cost of more requests.

---

## 5. Run the migration

```bash
export ARC_TOKEN='<admin-tier-token>'

tsm2arc \
  --datadir   /mnt/influx/data \
  --waldir    /mnt/influx/wal \
  --arc-url   https://arc.example.net \
  --token     "$ARC_TOKEN" \
  --workers   4 \
  --checkpoint /var/lib/tsm2arc/migration.checkpoint.db \
  --verbose
```

While it runs, a heartbeat line reports progress:

```
[12/40 shards] 3821 chunks, 18402991 rows, 4210.5 MB raw — 38211 rows/s, 9.4 MB/s (480s)
```

- Put the `--checkpoint` file somewhere durable and **keep it** — it is how
  resume works.
- Avoid running two `tsm2arc` processes against the **same checkpoint file** at
  once. Use one process; scale with `--workers`.

---

## 6. If it stops (crash, network drop, reboot) — just resume

Re-run the **exact same command**. The tool:

- skips shards already fully migrated (no re-extraction),
- for a partially-migrated shard, re-derives its chunks deterministically and
  resumes from the first un-acknowledged chunk.

```bash
# identical command — it picks up where it left off
tsm2arc --datadir /mnt/influx/data --waldir /mnt/influx/wal \
        --arc-url https://arc.example.net --token "$ARC_TOKEN" \
        --workers 4 --checkpoint /var/lib/tsm2arc/migration.checkpoint.db --verbose
```

**Duplicates on resume:** a clean (uninterrupted) run produces **zero**
duplicates. The only duplication window is a crash *between* Arc persisting a
chunk and the tool recording it — on resume that single chunk is re-sent. Arc
compaction collapses the duplicate for **tag-bearing** series automatically.
**Tagless** series (measurements with fields but no tags) can retain up to one
chunk of duplicate rows per shard per crash — see [DESIGN.md](DESIGN.md) §6.
This is bounded and attributable (the `--verbose` log shows which shard/chunk was
re-sent).

---

## 7. Verify

After the run completes (`DONE: imported N rows ...`):

```bash
# 1. Compare extracted counts vs Arc, per database/measurement.
#    Extracted count (tool side): re-run dry-run and read the totals.
tsm2arc --datadir /mnt/influx/data --waldir /mnt/influx/wal --dry-run --sample 0

#    Arc side: query row counts per measurement (example).
curl -s -H "Authorization: Bearer $ARC_TOKEN" \
  "https://arc.example.net/api/v1/query" \
  --data-urlencode 'q=SELECT count(*) FROM <measurement>' --data-urlencode 'db=<database>'
```

- For **tag-bearing** data after Arc compaction runs, counts should match exactly.
- For **tagless** data, a small positive delta on Arc's side = resume-overlap
  duplicates (bounded per §6); a clean run shows no delta.

```bash
# 2. Spot-check a few series' min/max time and sampled values against the
#    dry-run sample lines from step 2.
```

If counts are far off (not a small overlap), investigate before declaring done —
check the `--verbose` log for `WARN`/`skipped-keys` and any shard that errored.

---

## 8. Cleanup

- Keep the `--checkpoint` file until you have **verified** the migration; it's
  your record of exactly what was sent. Delete it only to force a full
  re-migration from scratch.
- Unmount the read-only source volume.
- Rotate the Arc admin token if it was placed on a shared host.

---

## Troubleshooting

| Symptom | Likely cause | Action |
|---|---|---|
| `no shards with .tsm files found` | data is WAL-only and `--waldir` omitted, or wrong `--datadir` | pass `--waldir`; verify the path points at `.../data` |
| A database is missing from output | WAL-only + no `--waldir`, or filtered out | add `--waldir`; check `--database-filter` |
| `arc 401` / permanent error | token not admin-tier or wrong | use an admin token |
| `arc 413` | `--chunk-bytes` too large for Arc's cap | keep `--chunk-bytes` < 500MB (default 450MB is safe) |
| Repeated `arc 429` then backoff | Arc under load / too many workers | lower `--workers` and/or `--chunk-bytes` |
| Arc node OOM | `--workers` too high for Arc's RAM | lower `--workers`; see §4 memory math |
| Run aborts on a corrupt TSM file | damaged source file | note the file from the error; consider `--database-filter`/`--start`/`--end` to skip the affected shard's range, then handle it separately |
| Resume re-sends everything | wrong/missing `--checkpoint` path | always point `--checkpoint` at the same durable file |

---

## Quick reference

```
tsm2arc \
  --datadir PATH        InfluxDB data dir (.../data)            [required]
  --waldir  PATH        InfluxDB WAL dir (.../wal)              [strongly recommended]
  --arc-url URL         Arc base URL                            [required unless --dry-run]
  --token   TOKEN       Arc admin token (or ARC_TOKEN env)      [required unless --dry-run]
  --workers N           concurrent shards (default 2)
  --chunk-bytes N       raw LP bytes/request, <500MB (default 450MB)
  --checkpoint PATH     SQLite resume store (default tsm2arc.checkpoint.db)
  --db-map old=new      rename source DB → Arc DB (repeatable)
  --database-filter DB  migrate only this source DB (repeatable)
  --start / --end       RFC3339 UTC time filters
  --precision ns|us|ms|s  source timestamp precision (default ns)
  --include-internal    also migrate InfluxDB's _internal DB
  --dry-run             extract + count, do not write to Arc
  --sample N            print N sample LP lines/DB in dry-run
  --verbose             per-shard/chunk logging
```
