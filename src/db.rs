use crate::mst::Hash;
use crate::mst::MstItem;
use crate::mst::MstNode;
use crate::mst::MstTreeItem;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::path::Path;

pub type Entity = str;
pub type Attribute = str;
pub type EavValue = str;
pub type RefName = str;
pub type Blob = Vec<u8>;

// --- Table Definitions and Constants ---
pub const EAV_TABLE: TableDefinition<(&Entity, &Attribute), &EavValue> =
    TableDefinition::new("eav");
pub const REPO_TABLE: TableDefinition<Hash, Blob> = TableDefinition::new("repo");
pub const REFS_TABLE: TableDefinition<&RefName, &Hash> = TableDefinition::new("refs");
pub const MST_ROOT_REF_NAME: &str = "mst_root";

pub struct Db {
    pub redb: redb::Database,
}

impl Db {
    pub fn new(db_path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let db = Database::create(db_path)?;

        Ok(Db { redb: db })
    }

    pub fn write(
        &self,
        entity: &str,
        attribute: &str,
        value: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let write_txn = self.redb.begin_write()?;
        {
            // update EAV table
            let mut eav_table = write_txn.open_table(EAV_TABLE)?;
            eav_table.insert((entity, attribute), value)?;

            // Update repo table
            let mut refs_table = write_txn.open_table(REFS_TABLE)?;
            let new_root_ref = {
                let mut repo_table = write_txn.open_table(REPO_TABLE)?; // Open as mutable

                let mut node: MstNode<(String, String), String> =
                    match refs_table.get(MST_ROOT_REF_NAME)? {
                        Some(root_ref) => MstNode::load(&repo_table, root_ref.value())?,
                        None => MstNode::new(),
                    };
                // Mst::upsert now takes mutable repo_table
                node.upsert(
                    &mut repo_table, // Pass mutable repo_table
                    (String::from(entity), String::from(attribute)),
                    String::from(value),
                )?
            };
            refs_table.insert(MST_ROOT_REF_NAME, &new_root_ref)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn read(
        &self,
        entity: &str,
        attribute: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let read_txn = self.redb.begin_read()?;
        let table = read_txn.open_table(EAV_TABLE)?;
        if let Some(read_value) = table.get(&(entity, attribute))? {
            Ok(Some(read_value.value().to_string()))
        } else {
            Ok(None)
        }
    }
}

pub fn print_mst_recursive(
    db: &redb::Database,
    hash: Hash,
) -> Result<(), Box<dyn std::error::Error>> {
    let write_txn = db.begin_write()?;
    let repo_table = write_txn.open_table(REPO_TABLE)?;

    let mst_tree_item = MstTreeItem::<(String, String), String> {
        item: MstItem::Ref(hash),
        repo_table: &repo_table,
    };
    ptree::print_tree(&mst_tree_item)?;

    Ok(())
}
