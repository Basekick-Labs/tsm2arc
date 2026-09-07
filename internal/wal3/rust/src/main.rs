//! Decode one InfluxDB 3 WAL payload from stdin to JSON on stdout.
//!
//! Input: the raw bitcode bytes AFTER the 12-byte framing (8-byte magic
//! `idb3.001` + 4-byte big-endian CRC32) — the Go caller verifies framing.
//! Output: `WalContents` as JSON. Errors go to stderr with exit code 1.
//!
//! The structs below are vendored VERBATIM (serialized shape: field order,
//! enum variant order, integer widths) from influxdb3_wal/src/lib.rs and
//! influxdb3_id/src/lib.rs. The shape is identical across all upstream
//! release tags v3.0.0..v3.11.4; bitcode serde-mode decoding depends on it
//! exactly, so DO NOT reorder fields or variants.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Read, Write};

#[derive(Deserialize, Serialize)]
struct WalContents {
    persist_timestamp_ms: i64,
    min_timestamp_ns: i64,
    max_timestamp_ns: i64,
    wal_file_number: u64, // WalFileSequenceNumber newtype
    ops: Vec<WalOp>,
    snapshot: Option<SnapshotDetails>,
}

#[derive(Deserialize, Serialize)]
enum WalOp {
    Write(WriteBatch), // discriminant 0
    Noop(NoopDetails), // discriminant 1
}

#[derive(Deserialize, Serialize)]
struct WriteBatch {
    catalog_sequence: u64,
    database_id: u32, // DbId newtype
    database_name: String,
    // Upstream SerdeVecMap<TableId, TableChunks> serializes as a sequence of
    // (key, value) tuples — exactly what Vec<(K, V)> produces.
    table_chunks: Vec<(u32, TableChunks)>,
    min_time_ns: i64,
    max_time_ns: i64,
}

#[derive(Deserialize, Serialize)]
struct TableChunks {
    min_time: i64,
    max_time: i64,
    chunk_time_to_chunk: HashMap<i64, TableChunk>,
}

#[derive(Deserialize, Serialize)]
struct TableChunk {
    rows: Vec<Row>,
}

#[derive(Deserialize, Serialize)]
struct Row {
    time: i64, // nanoseconds
    fields: Vec<Field>,
}

#[derive(Deserialize, Serialize)]
struct Field {
    id: u16, // ColumnId newtype — u16, not u32
    value: FieldData,
}

#[derive(Deserialize, Serialize)]
enum FieldData {
    Timestamp(i64), // 0
    Key(String),    // 1 (v3-LP series key; kept for the enum shape)
    Tag(String),    // 2
    String(String), // 3
    Integer(i64),   // 4
    UInteger(u64),  // 5
    Float(f64),     // 6
    Boolean(bool),  // 7
}

#[derive(Deserialize, Serialize)]
struct NoopDetails {
    timestamp_ns: i64,
}

#[derive(Deserialize, Serialize)]
struct SnapshotDetails {
    snapshot_sequence_number: u64, // SnapshotSequenceNumber newtype
    end_time_marker: i64,
    first_wal_sequence_number: u64,
    last_wal_sequence_number: u64,
    forced: bool,
}

mod catalog;

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    let mut payload = Vec::new();
    if let Err(e) = std::io::stdin().read_to_end(&mut payload) {
        eprintln!("read stdin: {e}");
        std::process::exit(1);
    }
    match mode.as_str() {
        "catalog" => catalog_mode(&payload),
        _ => wal_mode(&payload),
    }
}

/// WAL mode: stdin is one WAL file's bitcode payload (serde mode); stdout is
/// the WalContents as JSON.
fn wal_mode(payload: &[u8]) {
    let contents: WalContents = match bitcode::deserialize(payload) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("bitcode decode: {e}");
            std::process::exit(1);
        }
    };
    let out = std::io::stdout();
    if let Err(e) = serde_json::to_writer(out.lock(), &contents) {
        eprintln!("encode json: {e}");
        std::process::exit(1);
    }
    let _ = std::io::stdout().flush();
}

/// Catalog mode: stdin is a stream of [u16 LE record id][u32 LE body length]
/// [body] entries (the Go caller strips file and record framing); stdout is
/// one JSON line per record, in order: {"id":N,"record":{...}} for decoded
/// types, {"id":N,"skip":true} for types the migration does not need.
fn catalog_mode(payload: &[u8]) {
    let mut off = 0usize;
    let out = std::io::stdout();
    let mut w = out.lock();
    while off < payload.len() {
        if payload.len() - off < 6 {
            eprintln!("truncated record stream at offset {off}");
            std::process::exit(1);
        }
        let id = u16::from_le_bytes([payload[off], payload[off + 1]]);
        let len = u32::from_le_bytes([
            payload[off + 2],
            payload[off + 3],
            payload[off + 4],
            payload[off + 5],
        ]) as usize;
        off += 6;
        if payload.len() - off < len {
            eprintln!("record id {id} body truncated at offset {off}");
            std::process::exit(1);
        }
        let body = &payload[off..off + len];
        off += len;
        match catalog::decode_record(id, body) {
            Ok(Some(json)) => {
                let _ = writeln!(w, "{{\"id\":{id},\"record\":{json}}}");
            }
            Ok(None) => {
                let _ = writeln!(w, "{{\"id\":{id},\"skip\":true}}");
            }
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }
    }
    let _ = w.flush();
}
