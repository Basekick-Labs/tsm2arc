# Contributing to tsm2arc

Thanks for your interest! tsm2arc migrates InfluxDB 1.x data into
[Arc](https://github.com/basekick-labs/arc), and we welcome contributions —
including **new output sinks** for other databases (the Arc sink is just the
first; the extraction side is database-agnostic line protocol).

## Ground rules

- By contributing, you agree your work is licensed under the project's
  [Apache-2.0](LICENSE) license.
- Be respectful. See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## Development

Requirements: Go (see `go.mod` for the version).

```bash
go build ./...
go test ./...
go test -race ./...      # required for any concurrency-touching change
go vet ./...
gofmt -l .               # must print nothing
```

The InfluxDB codec correctness tests use `github.com/influxdata/influxdb`
v1.7.11 as a **test-only** dependency (the oracle we validate our decoders
against). It is not linked into the production binary — keep it that way.

### Testing against a real InfluxDB

`fixture/` has a Docker Compose for InfluxDB 1.8 plus a seed script:

```bash
cd fixture
docker compose up -d
./seed.sh
../tsm2arc --datadir fixture/data/influxdb/data \
           --waldir  fixture/data/influxdb/wal --dry-run
```

## Design principles (please preserve these)

These invariants are load-bearing — changing them can corrupt a migration:

1. **Decoders are validated against the real InfluxDB encoder**, not hand-rolled
   golden bytes. Any new/changed codec must round-trip through
   `github.com/influxdata/influxdb/tsdb/engine/tsm1` in a test.
2. **Chunk boundaries must stay deterministic** (a function of extraction order +
   `--chunk-bytes`). Crash-safe resume relies on re-deriving byte-identical
   chunks. Do not introduce map-iteration order into the point/field stream.
3. **Checkpoint commits happen only after a 2xx** from the sink. This is the
   durability barrier; don't move it.
4. **Always read the WAL.** A shard that was never flushed lives only in `.wal`
   files; skipping WAL-only shards silently loses data.
5. **Field-rejoin is last-write-wins per (series, field, timestamp).** Don't emit
   two values for one field on one line.

## Pull requests

- Branch from `main`: `feat/...` or `fix/...`.
- Conventional commit messages: `feat(sink): add clickhouse sink`.
- Include tests. For concurrency changes, include the `-race` result in the PR.
- Keep the production binary dependency-light and cgo-free (the checkpoint store
  uses pure-Go `modernc.org/sqlite` deliberately, so the binary cross-compiles).

## Adding a new sink

The extraction pipeline produces InfluxDB line protocol; sinks consume chunks of
it. See `internal/sink` for the Arc sink as a reference. A new sink should:

- accept raw (uncompressed) LP chunks bounded by `--chunk-bytes`,
- handle its own auth and retry/backoff classification,
- report rows imported so progress/verification works.

Open an issue first to discuss the interface if you're adding a sink — we may
factor a `Sink` interface to keep `run.go` clean.
