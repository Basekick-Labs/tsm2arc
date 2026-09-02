# tsm2arc Migration Runbook

Operator guide for migrating InfluxDB **1.x (1.7/1.8) and 2.x (2.0–2.7)** data
into Arc with `tsm2arc`. This covers the common case: **terabytes of InfluxDB
data on a cold/unmounted volume** (e.g. an EBS snapshot) that is not served by
any running `influxd`.

> Read [DESIGN.md](DESIGN.md) for *why* the tool works the way it does. This
> runbook is the *how*.

---

## 0. Before you start — checklist

- [ ] The InfluxDB data volume is mounted **read-only** on the migration host.
- [ ] You know the InfluxDB root/data path (1.x: `.../influxdb` or `.../data`;
      2.x: the v2 root containing `engine/` and `influxd.bolt`).
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
- **`INVALID:` lines** — measurement names Arc would reject (Arc requires
  `^[a-zA-Z][a-zA-Z0-9_-]*$`; dots in particular are not allowed). The dry run
  lists each one with its point count so you can author renames *before* the
  load — see §3a. A load with the default settings aborts (client-side, before
  sending) on the first such name.

If a database you expected is missing, check whether its data is WAL-only and
whether you passed `--waldir`.

---

## 2a. Profile the shards (optional, seconds)

`--analyze` reads only the TSM indexes — no data is decoded, nothing is sent —
and prints per shard: series/file/key counts and, for the largest merge runs,
whether the files' time ranges overlap fully or partition well. Run it when
discussing throughput with support: the profile determines which optimizations
can help your data shape.

```bash
tsm2arc --datadir /mnt/influx/data --analyze
```

---

## 3. Scope the migration (optional)

You can migrate a subset first to validate the round-trip end to end:

```bash
# one database only
tsm2arc ... --database-filter telemetry --dry-run

# a time window (RFC3339, UTC)
tsm2arc ... --start 2024-01-01T00:00:00Z --end 2024-02-01T00:00:00Z --dry-run
```

`--start`/`--end` skip out-of-window TSM blocks straight from the index, so a
window bounds read work and time as well as output — you can migrate a large
source in sequential windows.

**Use a separate `--checkpoint` file per window.** Chunk sequence numbers are
derived from what a run extracts, so a different window means a different chunk
layout. The tool enforces this: `--start`/`--end` are part of the checkpoint's
config fingerprint, and resuming a checkpoint with a different window is refused
with `checkpoint was created with different settings` rather than silently
skipping chunks.

Mapping:

- By default each source InfluxDB **database** maps to an Arc **database** of the
  same name (Arc databases are namespaces). Measurement names pass through
  unchanged (but are validated — see §3a).
- Use `--db-map old=new` to rename (repeatable), e.g.
  `--db-map telemetry=telemetry_prod`.

---

## 3a. Handle measurement names Arc rejects

Arc accepts only measurement names matching `^[a-zA-Z][a-zA-Z0-9_-]*$` (the dot
is Arc's `database.measurement` separator in queries and RBAC grant keys, so it
cannot appear in a name). InfluxDB allows much more — dotted
`<env>.<service>` names are common — and tsm2arc validates **client-side,
before sending**, so a bad name can no longer end a multi-hour load with a
mid-flight Arc 400.

Workflow:

1. **Dry-run first.** Every name Arc would reject is listed with its point
   count (`INVALID:` lines).
2. **Author a rename map** for those names — deterministic targets you choose,
   traceable back to source:

   ```
   # renames.map — one old=new per line
   edge-prod.gateway_services=edge_prod_gateway_services
   qa.node-b=qa_node_b
   ```

   Pass it with `--measurement-map-file renames.map` (or inline, repeatable:
   `--measurement-map 'edge-prod.gateway_services=edge_prod_gateway_services'`).
   Targets are validated at startup — a typo fails immediately, not mid-load.
3. **Pick the policy for anything still invalid** with
   `--on-invalid-measurement`:
   - `fail` (default) — abort with an actionable error before sending. Keep
     this with a map: it guarantees nothing unmapped slips through.
   - `skip` — drop those points, keep loading, report names + point counts at
     the end. Use to land the good data now and deal with stragglers later.
   - `map` — deterministic auto-rename (disallowed chars → `_`, `m_` prefix if
     the name doesn't start with a letter). Beware: distinct names can collide
     after sanitizing (`a.b` and `a_b` both become `a_b`) and would merge;
     prefer an explicit map when many names are involved.

**Hyphens: valid to write, but check your Arc version before you rely on
them.** Names containing `-` (e.g. `has-hyphen`) pass Arc's rule and migrate
cleanly, but Arc **before 26.09.1** cannot query them by any SQL form — quoted
identifiers weren't resolved to storage paths, and an unquoted hyphen is
subtraction in SQL grammar. On Arc **26.09.1+**, always quote them:
`FROM "has-hyphen"` (with `x-arc-database`) or `FROM "db"."has-hyphen"`. If
the target Arc is older and won't be upgraded soon, add the rename to your map
(`has-hyphen=has_hyphen`). Hyphenated data already migrated is stored correctly
and becomes queryable on upgrade — nothing needs re-migrating.

Every rename and skip is **recorded in the checkpoint** (table
`measurement_actions`) and summarized when the run finishes — the audit trail
that makes renames reversible and skips visible. Query it any time:

```bash
sqlite3 migration.checkpoint.db \
  'SELECT source_db, measurement, action, renamed_to, origin, SUM(points)
   FROM measurement_actions GROUP BY source_db, measurement'
```

These flags shape chunk bytes, so they join the resume fingerprint: don't
change them mid-migration (tsm2arc refuses if you do — see §6).

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

### Migration-host memory

Separately from the Arc node, watch the **migration host's** own RAM:

- **Extraction (`--dry-run` and the read side of a load)** streams one series at
  a time and one TSM block at a time within a series. Peak heap is a few MiB and
  does **not** grow with the shard, the dataset, or the largest series — measured
  at 3.7 / 3.8 / 3.9 MiB for series of 500 K / 2 M / 8 M values.
- **The load adds the chunk buffers**: each worker holds up to `--chunk-bytes`
  of raw line protocol being accumulated, and (since 0.1.5) a second chunk in
  flight to Arc — extraction and upload overlap by default. Budget roughly
  `workers × 2 × chunk-bytes` (e.g. `4 × 2 × 450 MB ≈ 3.6 GB`); `--pipeline=false`
  reverts to serial send and the pre-0.1.5 `workers × chunk-bytes`. This
  dominates, and these are the only migration-host knobs worth turning. If the
  host is memory constrained, lower `--chunk-bytes` and/or `--workers` — none of
  the three affects correctness or resume.
- **The index cache adds a bounded budget**: each in-flight shard caches parsed
  TSM file indexes (up to `--index-cache`, default 2 GiB) so per-series file
  reopens don't re-parse them. Worst case adds `workers × index-cache` to the
  host budget; shards with small indexes use far less. If the run prints
  "budget full; raise --index-cache", the shard's indexes exceed the budget and
  extraction is paying re-parse CPU — raise it if the host has headroom.
- **File descriptors**: extraction holds one handle per TSM file containing the
  series currently being merged. Files with non-overlapping time ranges are
  merged in separate passes, so this is normally one or two. If every file in a
  shard spans the same time range, budget `workers × files-per-shard` and raise
  `ulimit -n` accordingly.

**On memory limits.** If the process is being OOM-killed on the *extraction*
side, `--workers` and `--chunk-bytes` are the wrong knobs — they bound the load
buffer, not the read. A binary at 0.1.3 or earlier held one whole series in
memory at ~8–32× its compressed on-disk size, so a shard with a single very large
series could not be migrated at any instance size. Upgrade to 0.1.4+, where
extraction memory is flat.

### Spreading load across a multi-writer cluster (Kubernetes)

If Arc runs as several writer pods behind a standard Kubernetes `Service`, be
aware that a ClusterIP Service balances **per TCP connection**, not per request.
tsm2arc keeps HTTP connections alive and reuses them for its large sequential
POSTs, so a handful of long-lived connections each stay pinned to whichever pod
they first dialed — one writer can end up taking nearly all the traffic while
the others idle.

The fix is on the routing layer, not the client: put an **L7 (HTTP-aware) load
balancer** in front of the writers — an ingress controller, Envoy/HAProxy, or a
cloud ALB — which balances each request independently. With that in place,
import traffic spreads evenly across writers with no tsm2arc changes.

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

On a resume of a pre-0.1.5 checkpoint the line also shows the catch-up phase
(`… 0 chunks (+3401 skipped on resume) …`), so re-derivation is visibly
progressing rather than looking like a hang.

- Put the `--checkpoint` file somewhere durable and **keep it** — it is how
  resume works.
- Avoid running two `tsm2arc` processes against the **same checkpoint file** at
  once. Use one process; scale with `--workers`.

---

## 6. If it stops (crash, network drop, reboot) — just resume

Re-run the **exact same command**. The tool:

- skips shards already fully migrated (no re-extraction),
- for a partially-migrated shard, **seeks** to the stored cursor: series before
  it are never read, blocks at or before it are skipped from the TSM index, and
  sending continues at the first un-acknowledged chunk within seconds to
  minutes — not hours of re-decoding.

A checkpoint written by tsm2arc ≤ 0.1.4 has no cursor; those shards fall back to
the old behavior (re-derive everything, skip already-sent chunks without
re-sending). The heartbeat's `+N skipped on resume` counter shows that phase.
Once 0.1.5 has committed a chunk, the shard has a cursor and later resumes
seek.

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

## 7. Verify (the trust gate — don't skip)

Count reconciliation is how you know the migration is complete and correct.
After the run finishes (`DONE: imported N rows ...`):

**1. Extracted-vs-Arc row counts.**

```bash
# Tool side — re-run --dry-run with the SAME flags (incl. any --waldir / --start
# / --end) and read the per-database totals. Using the same flags matters: the
# extracted count must be measured over the same scope you migrated.
tsm2arc --datadir <same> [--waldir <same>] [--start <same> --end <same>] --dry-run --sample 0

# Arc side — count rows per measurement. If you migrated with --start/--end,
# add the SAME time bounds to the query so both sides cover the same window.
curl -s -H "Authorization: Bearer $ARC_TOKEN" \
  "https://arc.example.net/api/v1/query" \
  --data-urlencode "q=SELECT count(*) FROM <measurement> WHERE time >= '<start>' AND time <= '<end>'" \
  --data-urlencode 'db=<database>'
```

- **Tag-bearing** data: after Arc compaction runs, counts should match exactly.
- **Tagless** data: a small positive delta on Arc's side = resume-overlap
  duplicates (bounded to ≤1 chunk per shard per crash, per §6); a clean,
  uninterrupted run shows no delta.
- **A short count on Arc's side** (fewer rows than extracted) is the signal to
  investigate — that's data that didn't land. Check the `--verbose` log for
  `WARN` lines, non-zero `skipped-keys`, or any shard that errored, then resume
  (re-run the same command — it continues where it left off).
- **If you loaded with `--on-invalid-measurement=skip`**, subtract the skipped
  point counts (printed at the end of the run, and in the checkpoint's
  `measurement_actions` table) from the extracted side before comparing —
  those points are intentionally not in Arc. If you used a rename map, query
  the Arc side under the **renamed** measurement names.

**2. WAL coverage check.** If you did **not** pass `--waldir`, confirm there's no
meaningful un-flushed data you skipped:

```bash
find <waldir> -name '*.wal' -size +0c    # non-empty WAL segments = un-migrated data
```

If that finds non-empty segments, re-run **with** `--waldir` (the same checkpoint
will skip everything already done and add only the WAL-resident data).

**3. Spot-check** a few series' min/max time and sampled values against the
`--dry-run` sample lines from step 2.

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
| `no shards with TSM/WAL data found` | data is WAL-only and `--waldir` omitted, or wrong `--datadir` | pass `--waldir` (auto for 2.x); point `--datadir` at the InfluxDB root or data dir |
| A database/bucket is missing from output | WAL-only + no `--waldir`, filtered out, or (2.x) a system bucket | add `--waldir`; check `--database-filter`; confirm it isn't a system bucket |
| 2.x buckets show as 16-hex IDs | `influxd.bolt` missing/unreadable | provide `--bolt` or copy `influxd.bolt` next to `engine/` |
| `arc 401` / permanent error | token not admin-tier or wrong | use an admin token |
| `invalid measurement name …` (from tsm2arc, before sending) | source measurement names violate Arc's rule (dots etc.) | see §3a: `--dry-run` to list them, then `--measurement-map`/`--measurement-map-file`, or `--on-invalid-measurement=skip\|map` |
| `arc 413` | `--chunk-bytes` too large for Arc's cap | keep `--chunk-bytes` < 500MB (default 450MB is safe) |
| Repeated `arc 429` then backoff | Arc under load / too many workers | lower `--workers` and/or `--chunk-bytes` |
| Arc node OOM | `--workers` too high for Arc's RAM | lower `--workers`; see §4 memory math |
| `checkpoint was created with different settings` | resuming with a changed `--chunk-bytes`/`--start`/`--end`/`--db-map`/`--precision`/`--measurement-map`/`--on-invalid-measurement` | restore the original flags, or use a fresh `--checkpoint` (full re-migration) |
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
  --measurement-map old=new       rename a source measurement (repeatable)
  --measurement-map-file PATH     file of renames, one old=new per line
  --on-invalid-measurement MODE   fail|skip|map for names Arc rejects (default fail)
  --start / --end       RFC3339 UTC time filters
  --precision ns|us|ms|s  precision sent to Arc (default ns; tsm2arc always emits ns)
  --include-internal    also migrate InfluxDB's _internal DB
  --dry-run             extract + count, do not write to Arc
  --sample N            print N sample LP lines/DB in dry-run
  --verbose             per-shard/chunk logging
```
