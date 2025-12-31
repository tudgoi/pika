use redb::TableDefinition;

pub type Entity = str;
pub type Attribute = str;
pub type EavValue = str;
pub type RefName = str;
pub type Blob = Vec<u8>;

/// A 32-byte hash used to identify Blobs int he repo table.
pub type Hash = [u8; 32];


pub const EAV_TABLE: TableDefinition<(&Entity, &Attribute), &EavValue> =
    TableDefinition::new("eav");
pub const REPO_TABLE: TableDefinition<Hash, Blob> = TableDefinition::new("repo");
pub const REFS_TABLE: TableDefinition<&RefName, &Hash> = TableDefinition::new("refs");
pub const ROOT_REF_NAME: &str = "root";