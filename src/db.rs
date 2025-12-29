use crate::mst::Hash;
use crate::mst::MstItem;
use crate::mst::MstNode;
use crate::mst::MstTreeItem;
use crate::pt::{PtItem, PtNode};
use clap::ValueEnum;
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
pub const ROOT_REF_NAME: &str = "root";

#[derive(Debug, Clone, ValueEnum, Copy)]
pub enum Engine {
    Mst,
    Pt,
}

pub struct Db {
    pub redb: redb::Database,
    pub engine: Engine,
}

impl Db {
    pub fn new(db_path: &Path, engine: Engine) -> Result<Self, Box<dyn std::error::Error>> {
        let db = Database::create(db_path)?;

        Ok(Db { redb: db, engine })
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
            let mut repo_table = write_txn.open_table(REPO_TABLE)?; // Open as mutable

            match self.engine {
                Engine::Mst => {
                    let new_root_ref = {
                        let mut node: MstNode<(String, String), String> =
                            match refs_table.get(ROOT_REF_NAME)? {
                                Some(root_ref) => MstNode::load(&repo_table, root_ref.value())?,
                                None => MstNode::new(),
                            };
                        node.upsert(
                            &mut repo_table,
                            (String::from(entity), String::from(attribute)),
                            String::from(value),
                        )?
                    };
                    refs_table.insert(ROOT_REF_NAME, &new_root_ref)?;
                }
                Engine::Pt => {
                    let mut current_refs = {
                        let node: PtNode<(String, String), String> =
                            match refs_table.get(ROOT_REF_NAME)? {
                                Some(root_ref) => PtNode::load(&repo_table, root_ref.value())?,
                                None => PtNode::new(),
                            };
                        node.upsert(
                            &mut repo_table,
                            (String::from(entity), String::from(attribute)),
                            String::from(value),
                        )?
                    };

                    // Prolly Tree root management: 
                    // If upsert returned multiple refs, we must continue chunking up
                    // until we have a single root node.
                    while current_refs.len() > 1 {
                        current_refs = PtNode::chunk_and_save(&mut repo_table, current_refs)?;
                    }

                    if let Some(PtItem::Ref(_, hash)) = current_refs.first() {
                        refs_table.insert(ROOT_REF_NAME, hash)?;
                    }
                }
            }
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

    pub fn stat(&self) -> Result<(), Box<dyn std::error::Error>> {
        let read_txn = self.redb.begin_read()?;

        let mut total_user_bytes: u64 = 0;
        // The EAV table might not be populated if only checking structural overhead of a fresh repo?
        // But write() populates both.

        if let Ok(eav_table) = read_txn.open_table(EAV_TABLE) {
            for result in eav_table.iter()? {
                let (key, value) = result?;
                let (entity, attribute) = key.value();
                total_user_bytes += entity.len() as u64;
                total_user_bytes += attribute.len() as u64;
                total_user_bytes += value.value().len() as u64;
            }
        }

        let mut total_repo_bytes: u64 = 0;
        if let Ok(repo_table) = read_txn.open_table(REPO_TABLE) {
            for result in repo_table.iter()? {
                let (key, value) = result?;
                total_repo_bytes += key.value().len() as u64;
                total_repo_bytes += value.value().len() as u64;
            }
        }

        println!("User Data (EAV): {} bytes", total_user_bytes);
        println!("Repo Data (Nodes): {} bytes", total_repo_bytes);

        if total_repo_bytes >= total_user_bytes {
            println!("Overhead: {} bytes", total_repo_bytes - total_user_bytes);
        } else {
            // Should not happen if repo contains all data + structure, unless EAV has data not in Repo?
            // Or if compression was involved (not here).
            println!(
                "Overhead: -{} bytes (Repo is smaller?)",
                total_user_bytes - total_repo_bytes
            );
        }
        
        if total_user_bytes > 0 {
             println!("Overhead Ratio: {:.2}x", total_repo_bytes as f64 / total_user_bytes as f64);
        }

        Ok(())
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
