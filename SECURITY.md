# Security Policy

## Reporting a vulnerability

Please report security issues privately. Do **not** open a public issue for a
vulnerability.

- Use GitHub's [private security advisory](https://github.com/basekick-labs/tsm2arc/security/advisories/new)
  feature, or
- email **security@basekick.net**.

We'll acknowledge within a few business days and keep you updated through to a
fix and (if warranted) an advisory.

## Scope

tsm2arc reads InfluxDB files from disk and sends data to an Arc endpoint over
HTTP(S) using an admin token. Relevant areas:

- handling of file paths derived from on-disk directory names,
- handling of the Arc API token (it must never be logged or printed),
- parsing of untrusted/corrupt TSM and WAL files (resource exhaustion, panics),
- TLS / transport to the Arc endpoint.

## Operator guidance

- Pass the Arc token via the `ARC_TOKEN` environment variable rather than
  `--token` on the command line where possible (command lines can appear in
  process listings and shell history).
- Mount source InfluxDB volumes **read-only**.
- The checkpoint SQLite file contains migration metadata (database/shard names,
  progress) — not credentials — but treat it as you would other operational data.
