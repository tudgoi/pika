mod mst;
mod option;
mod pt;
pub mod sync;
pub mod table;

use crate::db::option::OptionError;
use clap::ValueEnum;
use iroh::EndpointId;
use mst::MstError;
use mst::MstItem;
use mst::MstNode;
use mst::MstTreeItem;
use option::OptionExt;
use pt::PtTreeItem;
use pt::{PtError, PtItem, PtNode};
use redb::{Database, ReadableDatabase, ReadableTable};
use serde::Deserialize;
use serde::Serialize;
use std::path::Path;
use thiserror::Error;

use crate::db::table::EAV_TABLE;
use crate::db::table::REFS_TABLE;
use crate::db::table::REPO_TABLE;
use crate::db::table::ROOT_REF_NAME;

#[derive(Debug, Clone, ValueEnum, Copy, PartialEq, Serialize, Deserialize)]
pub enum Engine {
    Mst,
    Pt,
}

#[derive(Error, Debug)]
pub enum DbError {
    #[error("could not open Redb")]
    DatabaseError(#[from] redb::DatabaseError),

    #[error("could not access Redb Table")]
    TableError(#[from] redb::TableError),

    #[error("could not access Redb")]
    RedbError(#[from] redb::StorageError),

    #[error("could not start transaction")]
    TxnError(#[from] redb::TransactionError),

    #[error("could not commit transaction")]
    CommitError(#[from] redb::CommitError),

    #[error("redb error")]
    RedbGeneric(#[from] redb::Error),

    #[error("option error")]
    OptionError(#[from] OptionError),

    #[error("could not access the Merkle Search Tree")]
    MstError(#[from] MstError),

    #[error("could not access the Prolly Tree")]
    PtError(#[from] PtError),

    #[error("could not read root hash")]
    RootHashNotFound,

    #[error("IO error")]
    IoError(#[from] std::io::Error),
}

#[derive(Debug)]
pub struct Db {
    pub redb: redb::Database,
    pub engine: Engine,
}

impl Db {
    pub fn init(db_path: &Path, engine: Engine) -> Result<Self, DbError> {
        let db = Database::create(db_path)?;
        let write_txn = db.begin_write()?;
        {
            let mut options = write_txn.option_table()?;
            options.set_engine(engine)?;
            options.reset_secret_key()?;
        }
        write_txn.commit()?;

        Ok(Db { redb: db, engine })
    }

    pub fn open(db_path: &Path) -> Result<Self, DbError> {
        let db = Database::create(db_path)?;
        let read_txn = db.begin_read()?;
        let options = read_txn.option_table()?;

        let engine = options.get_engine()?;

        Ok(Db { redb: db, engine })
    }

    pub fn write(&self, entity: &str, attribute: &str, value: &str) -> Result<(), DbError> {
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

    pub fn read(&self, entity: &str, attribute: &str) -> Result<Option<String>, DbError> {
        let read_txn = self.redb.begin_read()?;
        let table = read_txn.open_table(EAV_TABLE)?;
        if let Some(read_value) = table.get(&(entity, attribute))? {
            Ok(Some(read_value.value().to_string()))
        } else {
            Ok(None)
        }
    }

    pub fn add_remote(&self, name: &str, endpoint_id: &EndpointId) -> Result<(), DbError> {
        let write_txn = self.redb.begin_write()?;
        {
            let mut options = write_txn.option_table()?;
            options.add_remote(name, endpoint_id)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn remove_remote(&self, name: &str) -> Result<(), DbError> {
        let write_txn = self.redb.begin_write()?;
        {
            let mut options = write_txn.option_table()?;
            options.remove_remote(name)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    // TODO this is a debug function. Decide later how to expose this in production.
    pub fn print_remotes(&self) -> Result<(), DbError> {
        let read_txn = self.redb.begin_read()?;
        let options = read_txn.option_table()?;
        match options.get_all_remotes() {
            Ok(remotes) => {
                println!("Remotes:");
                for (name, bytes) in remotes {
                    if let Ok(eid) = EndpointId::from_bytes(&bytes) {
                        println!("{}: {}", name, eid);
                    } else {
                        println!("{}: <invalid-id>", name);
                    }
                }
            }
            Err(OptionError::OptionNotSet) => {
                println!("No remotes found.");
            }
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    // TODO This is just a temporary API. Need to refine this later.
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

    pub fn print_stat(&self) -> Result<(), DbError> {
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

    // TODO This is a debug API. Need to decide how to expose this in production verison.
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

/// Converts a byte slice to a hex string.
pub fn hex_string(buf: &[u8]) -> String {
    buf.iter().map(|b| format!("{:02x}", b)).collect::<String>()
}
