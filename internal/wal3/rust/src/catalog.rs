//! Catalog/v3 record bodies (InfluxDB 3.10+ binary catalog), bitcode DERIVE
//! mode — a different encoding than the WAL's serde mode.
//!
//! Structs are vendored VERBATIM (field order, enum variant order, widths)
//! from influxdb3_catalog/src/format/records/*.rs; the shape is identical
//! across all upstream release tags v3.10.0..v3.11.4. Wire structs carry raw
//! ints — the upstream ID newtypes are in-memory only. Field order is
//! load-bearing (upstream freezes it with assert_roundtrip! hex snapshots,
//! which our tests mirror).
//!
//! Only the record types needed to answer "names, series keys, column ids"
//! are decoded; everything else is skipped by the Go caller via the record
//! length. Enums are positional: DO NOT reorder variants.

use serde::Serialize;

#[derive(bitcode::Decode, Serialize)]
pub struct CreateDatabase {
    pub database_id: u32,
    pub database_name: String,
    pub retention_period: RetentionPeriod,
}

#[derive(bitcode::Decode, Serialize)]
pub struct SoftDeleteDatabase {
    pub database_id: u32,
    pub deletion_time_ns: i64,
    pub hard_deletion_time_ns: Option<i64>,
    pub hard_delete_scope: Option<DeletionScope>,
}

#[derive(bitcode::Decode, Serialize)]
pub struct HardDeleteDatabase {
    pub db_id: u32,
}

#[derive(bitcode::Decode, Serialize)]
pub struct CreateTable {
    pub database_id: u32,
    pub database_name: String,
    pub table_name: String,
    pub table_id: u32,
    pub retention_period: RetentionPeriod,
    pub field_family_mode: FieldFamilyMode,
}

#[derive(bitcode::Decode, Serialize)]
pub struct SoftDeleteTable {
    pub database_id: u32,
    pub table_id: u32,
    pub deletion_time_ns: i64,
    pub hard_deletion_time_ns: Option<i64>,
    pub hard_delete_scope: Option<DeletionScope>,
}

#[derive(bitcode::Decode, Serialize)]
pub struct HardDeleteTable {
    pub db_id: u32,
    pub table_id: u32,
}

#[derive(bitcode::Decode, Serialize)]
pub struct AddColumns {
    pub database_id: u32,
    pub table_id: u32,
    pub columns: Vec<ColumnDefinition>,
    pub field_families: Vec<FieldFamilyDefinition>,
}

#[derive(bitcode::Decode, Serialize)]
pub enum ColumnDefinition {
    Timestamp(TimestampColumn), // 0
    Tag(TagColumn),             // 1
    Field(FieldColumn),         // 2
}

#[derive(bitcode::Decode, Serialize)]
pub struct TimestampColumn {
    pub column_id: Option<u16>,
    pub name: String,
}

#[derive(bitcode::Decode, Serialize)]
pub struct TagColumn {
    pub id: u16,
    pub column_id: Option<u16>,
    pub name: String,
}

#[derive(bitcode::Decode, Serialize)]
pub struct FieldColumn {
    pub id: FieldIdentifier,
    pub column_id: Option<u16>,
    pub name: String,
    pub data_type: FieldDataType,
}

#[derive(bitcode::Decode, Serialize)]
pub struct FieldIdentifier {
    pub family_id: u16,
    pub field_id: u16,
}

#[derive(bitcode::Decode, Serialize)]
pub enum FieldDataType {
    String,   // 0
    Integer,  // 1
    UInteger, // 2
    Float,    // 3
    Boolean,  // 4
    Binary,   // 5
}

#[derive(bitcode::Decode, Serialize)]
pub enum FieldFamilyName {
    User(String), // 0
    Auto(u16),    // 1
}

#[derive(bitcode::Decode, Serialize)]
pub struct FieldFamilyDefinition {
    pub id: u16,
    pub name: FieldFamilyName,
}

#[derive(bitcode::Decode, Serialize)]
pub enum FieldFamilyMode {
    Aware, // 0
    Auto,  // 1
}

#[derive(bitcode::Decode, Serialize)]
pub enum RetentionPeriod {
    Indefinite,                     // 0
    Duration { duration_secs: u64 }, // 1
}

#[derive(bitcode::Decode, Serialize)]
pub enum DeletionScope {
    DataAndCatalog,           // 0
    DataOnlyRemoveTables,     // 1
    DataOnlyKeepResources,    // 2
}

/// Record type ids (core partition; bit 15 set = enterprise partition).
pub const ID_CREATE_DATABASE: u16 = 4;
pub const ID_SOFT_DELETE_DATABASE: u16 = 5;
pub const ID_CREATE_TABLE: u16 = 6;
pub const ID_SOFT_DELETE_TABLE: u16 = 7;
pub const ID_ADD_COLUMNS: u16 = 8;
pub const ID_HARD_DELETE_DATABASE: u16 = 22;
pub const ID_HARD_DELETE_TABLE: u16 = 23;
pub const ID_RESTORE_CATALOG: u16 = 0x8006;

/// Decodes one record body to a JSON value string, or None for record types
/// the migration does not need (skipped upstream by length).
pub fn decode_record(id: u16, body: &[u8]) -> Result<Option<String>, String> {
    macro_rules! dec {
        ($ty:ty) => {{
            let v: $ty = bitcode::decode(body).map_err(|e| format!("record id {id}: {e}"))?;
            Some(serde_json::to_string(&v).map_err(|e| e.to_string())?)
        }};
    }
    Ok(match id {
        ID_CREATE_DATABASE => dec!(CreateDatabase),
        ID_SOFT_DELETE_DATABASE => dec!(SoftDeleteDatabase),
        ID_CREATE_TABLE => dec!(CreateTable),
        ID_SOFT_DELETE_TABLE => dec!(SoftDeleteTable),
        ID_ADD_COLUMNS => dec!(AddColumns),
        ID_HARD_DELETE_DATABASE => dec!(HardDeleteDatabase),
        ID_HARD_DELETE_TABLE => dec!(HardDeleteTable),
        ID_RESTORE_CATALOG => {
            return Err(format!(
                "record id {id} is an Enterprise catalog restore; state was swapped from backup files and cannot be replayed"
            ))
        }
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }

    // Ground-truth byte vectors from upstream's own assert_roundtrip! tests
    // (influxdb3_catalog/src/format/records/*/tests.rs @ v3.10.0). If any of
    // these fail, the vendored shape or bitcode version drifted.
    #[test]
    fn create_database_duration() {
        let v: CreateDatabase = bitcode::decode(&hex("0401046d7964620104100e")).unwrap();
        assert_eq!(v.database_id, 1);
        assert_eq!(v.database_name, "mydb");
        assert!(matches!(v.retention_period, RetentionPeriod::Duration { duration_secs: 3600 }));
    }

    #[test]
    fn create_database_indefinite() {
        let v: CreateDatabase = bitcode::decode(&hex("042a07746573745f646200")).unwrap();
        assert_eq!(v.database_id, 42);
        assert_eq!(v.database_name, "test_db");
        assert!(matches!(v.retention_period, RetentionPeriod::Indefinite));
    }

    #[test]
    fn create_table() {
        let v: CreateTable = bitcode::decode(&hex("0401046d79646203637075040a01028051010001")).unwrap();
        assert_eq!((v.database_id, v.table_id), (1, 10));
        assert_eq!((v.database_name.as_str(), v.table_name.as_str()), ("mydb", "cpu"));
        assert!(matches!(v.retention_period, RetentionPeriod::Duration { duration_secs: 86400 }));
        assert!(matches!(v.field_family_mode, FieldFamilyMode::Auto));
    }

    #[test]
    fn hard_delete_table() {
        let v: HardDeleteTable = bitcode::decode(&hex("0401040a")).unwrap();
        assert_eq!((v.db_id, v.table_id), (1, 10));
    }

    #[test]
    fn add_columns_fields() {
        let v: AddColumns = bitcode::decode(&hex(
            "0401040a0208020000020102010100050876616c75656f766572666c6f770900",
        ))
        .unwrap();
        assert_eq!((v.database_id, v.table_id), (1, 10));
        assert_eq!(v.columns.len(), 2);
        match &v.columns[0] {
            ColumnDefinition::Field(f) => {
                assert_eq!(f.name, "value");
                assert_eq!(f.column_id, Some(1));
                assert!(matches!(f.data_type, FieldDataType::Float));
            }
            _ => panic!("expected field"),
        }
        match &v.columns[1] {
            ColumnDefinition::Field(f) => {
                assert_eq!(f.name, "overflow");
                assert_eq!(f.column_id, None);
                assert!(matches!(f.data_type, FieldDataType::Integer));
            }
            _ => panic!("expected field"),
        }
    }
}
