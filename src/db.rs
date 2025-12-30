use crate::mst::Hash;
use crate::mst::MstError;
use crate::mst::MstItem;
use crate::mst::MstNode;
use crate::mst::MstTreeItem;
use crate::pt::PtTreeItem;
use crate::pt::{PtError, PtItem, PtNode};
use clap::ValueEnum;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::path::Path;
use thiserror::Error;

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
pub const OPTIONS_TABLE: TableDefinition<&str, &str> = TableDefinition::new("options");
pub const ROOT_REF_NAME: &str = "root";

#[derive(Debug, Clone, ValueEnum, Copy, PartialEq)]
pub enum Engine {
    Mst,
    Pt,
}

#[derive(Error, Debug)]
pub enum DbError {
    #[error("could not access Redb Table")]
    TableError(#[from] redb::TableError),

    #[error("could not access Redb")]
    RedbError(#[from] redb::StorageError),

    #[error("could not start transaction")]
    TxnError(#[from] redb::TransactionError),

    #[error("could not access the Merkle Search Tree")]
    MstError(#[from] MstError),

    #[error("could not access the Prolly Tree")]
    PtError(#[from] PtError),

    #[error("could not read root hash")]
    RootHashNotFound,

    #[error("IO error")]
    IoError(#[from] std::io::Error),

    #[error("Database not initialized: {0}")]
    NotInitialized(String),
}

pub struct Db {
    pub redb: redb::Database,
    pub engine: Engine,
}

impl Db {
    pub fn init(db_path: &Path, engine: Engine) -> Result<Self, Box<dyn std::error::Error>> {
        let db = Database::create(db_path)?;
        let write_txn = db.begin_write()?;
        {
            let mut table = write_txn.open_table(OPTIONS_TABLE)?;
            let engine_str = match engine {
                Engine::Mst => "Mst",
                Engine::Pt => "Pt",
            };
            table.insert("engine", engine_str)?;
        }
        write_txn.commit()?;

        Ok(Db { redb: db, engine })
    }

    pub fn open(db_path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let db = Database::create(db_path)?;
        let read_txn = db.begin_read()?;
        
        // Check if options table exists and has engine
        // We use catch_unwind or just try to open. 
        // If the table was never created, open_table might succeed (returning empty) or fail?
        // In redb, open_table usually succeeds if the definition matches or it's new?
        // Wait, for read_txn, open_table returns error if table does not exist?
        // Documentation says: "Returns TableError::TableDoesNotExist if the table does not exist."
        
        let engine = match read_txn.open_table(OPTIONS_TABLE) {
            Ok(table) => {
                if let Some(val) = table.get("engine")? {
                    let s = val.value();
                    match s {
                        "Mst" => Engine::Mst,
                        "Pt" => Engine::Pt,
                        _ => return Err(DbError::NotInitialized(format!("Unknown engine type: {}", s)).into()),
                    }
                } else {
                    return Err(DbError::NotInitialized("Engine option missing".to_string()).into());
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {
                 return Err(DbError::NotInitialized("Options table missing".to_string()).into());
            }
            Err(e) => return Err(e.into()),
        };

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

    pub fn read_ref(
        &self,
        ref_name: &str,
        entity: &str,
        attribute: &str,
    ) -> Result<Option<String>, DbError> {
        let read_txn = self.redb.begin_read()?;
        let refs_table = read_txn.open_table(REFS_TABLE)?;
        let repo_table = read_txn.open_table(REPO_TABLE)?;
        if let Some(root_hash) = refs_table.get(ref_name)?.map(|guard| *guard.value()) {
            match self.engine {
                Engine::Mst => {
                    let mst_node: MstNode<(String, String), String> =
                        MstNode::load(&repo_table, &root_hash)?;
                    let key = (entity.to_string(), attribute.to_string());
                    Ok(mst_node.find(&repo_table, &key)?)
                }
                Engine::Pt => {
                    let pt_node: PtNode<(String, String), String> =
                        PtNode::load(&repo_table, &root_hash)?;
                    let key = (entity.to_string(), attribute.to_string());
                    Ok(pt_node.find(&repo_table, &key)?)
                }
            }
        } else {
            Err(DbError::RootHashNotFound)
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
            println!(
                "Overhead Ratio: {:.2}x",
                total_repo_bytes as f64 / total_user_bytes as f64
            );
        }

        Ok(())
    }

    pub fn print_ref_recursive(&self, ref_name: &str) -> Result<(), DbError> {
        let read_txn = self.redb.begin_read()?;
        let refs_table = read_txn.open_table(REFS_TABLE)?;
        if let Some(root_hash) = refs_table.get(ref_name)?.map(|guard| *guard.value()) {
            let read_txn = self.redb.begin_read()?;
            let repo_table = read_txn.open_table(REPO_TABLE)?;

            match self.engine {
                Engine::Mst => {
                    let mst_tree_item = MstTreeItem::<(String, String), String, _> {
                        item: MstItem::Ref(root_hash),
                        repo_table: &repo_table,
                    };
                    ptree::print_tree(&mst_tree_item)?;
                }
                Engine::Pt => {
                    // We fake a root Ref item to start the tree
                    // Ideally we would want to display the Root Node itself, but TreeItem usually represents a Node/Item.
                    // PtItem::Ref logic works if we treat the "root" as a Ref to the actual root node.
                    let root_item = PtItem::Ref(("ROOT".to_string(), "".to_string()), root_hash);

                    // Note: K=String, V=String hardcoded for CLI usage, similar to Mst
                    let pt_tree_item = PtTreeItem::<(String, String), String, _> {
                        item: root_item,
                        repo_table: &repo_table,
                    };
                    ptree::print_tree(&pt_tree_item)?;
                }
            }
        } else {
            println!("could not find ref: {}", ref_name);
        }

        Ok(())
    }
}
