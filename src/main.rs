use blake3::hash;
use clap::{Parser, Subcommand, ValueEnum};
use postcard::{from_bytes, to_stdvec};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self},
    path::PathBuf,
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the redb database file
    db_path: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Writes a key-value pair to the database
    Write {
        /// The entity
        entity: String,
        /// The attribute
        attribute: String,
        /// The value to write
        value: String,
    },
    /// Reads a value from the database given a key
    Read {
        /// The entity to read
        entity: String,
        /// The attribute to read
        attribute: String,
    },
    /// Lists all key-value pairs in the database
    List {
        /// The table to list
        #[arg(value_enum, default_value_t = Tables::Eav)]
        table_name: Tables,
    },
    /// Commits all records into repo
    Commit,
    /// Garbage collect unreferenced records from repo
    Gc,
}

#[derive(Debug, Clone, ValueEnum)]
enum Tables {
    Eav,
    Repo,
    Refs,
}

type Entity = str;
type Attribute = str;
type EavValue = str;
type RefName = str;
type Hash = [u8; 32];
type Blob = Vec<u8>;

const EAV_TABLE: TableDefinition<(&Entity, &Attribute), &EavValue> = TableDefinition::new("eav");
const REPO_TABLE: TableDefinition<Hash, Blob> = TableDefinition::new("repo");
const REFS_TABLE: TableDefinition<&RefName, &Hash> = TableDefinition::new("refs");
const MST_ROOT_REF_NAME: &str = "mst_root";

#[derive(Serialize, Deserialize, Debug, PartialEq)]
enum Object<'a> {
    Eav(&'a str, &'a str, &'a str),
    RefList(Vec<Hash>),
    MstNode(MstNode),
}

#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub struct MstNode {
    key: MstKey,
    left_child_hash: Option<Hash>,
    right_child_hash: Option<Hash>,
    value_hash: Option<Hash>,
}

impl fmt::Debug for MstNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MstNode {{ key: {:?}, left_child_hash: {}, right_child_hash: {}, value_hash: {} }}",
            self.key,
            self.left_child_hash
                .map(|buf| hex_string(&buf))
                .unwrap_or(String::from("None")),
            self.right_child_hash
                .map(|buf| hex_string(&buf))
                .unwrap_or(String::from("None")),
            self.value_hash
                .map(|buf| hex_string(&buf))
                .unwrap_or(String::from("None")),
        )
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct MstKey {
    pub entity: String,
    pub attribute: String,
}

fn update_mst(
    repo_table: &mut redb::Table<Hash, Vec<u8>>,
    current_node_hash: Option<Hash>,
    target_mst_key: MstKey,
    target_value_hash: Hash,
) -> Result<Hash, Box<dyn std::error::Error>> {
    let current_node_option = if let Some(hash) = current_node_hash {
        get_mst_node(repo_table, &hash)?
    } else {
        None
    };

    match current_node_option {
        Some(current_node) => {
            // Case 1: Current node is a leaf
            if current_node.value_hash.is_some() {
                if current_node.key == target_mst_key {
                    // Update existing leaf with new value_hash
                    let updated_leaf = MstNode {
                        key: target_mst_key,
                        left_child_hash: None,
                        right_child_hash: None,
                        value_hash: Some(target_value_hash),
                    };
                    put_mst_node(repo_table, &updated_leaf)
                } else {
                    // Leaf found, but key doesn't match. This means we need to insert a new node
                    // and make this current leaf a child. This is a rotation/restructuring.
                    let new_leaf = MstNode {
                        key: target_mst_key.clone(),
                        left_child_hash: None,
                        right_child_hash: None,
                        value_hash: Some(target_value_hash),
                    };
                    let new_leaf_hash = put_mst_node(repo_table, &new_leaf)?;

                    // Determine order of original leaf and new leaf
                    let (left_hash, right_hash) = if current_node.key < target_mst_key {
                        (put_mst_node(repo_table, &current_node)?, new_leaf_hash)
                    } else {
                        (new_leaf_hash, put_mst_node(repo_table, &current_node)?)
                    };

                    // Create a new internal node to be the parent
                    let new_internal_node = MstNode {
                        key: if target_mst_key < current_node.key {
                            current_node.key.clone()
                        } else {
                            target_mst_key.clone()
                        },
                        left_child_hash: Some(left_hash),
                        right_child_hash: Some(right_hash),
                        value_hash: None, // This is an internal node
                    };
                    put_mst_node(repo_table, &new_internal_node)
                }
            }
            // Case 2: Current node is an internal node
            else {
                let mut updated_node = current_node.clone();
                if target_mst_key < current_node.key {
                    updated_node.left_child_hash = Some(update_mst(
                        repo_table,
                        current_node.left_child_hash,
                        target_mst_key,
                        target_value_hash,
                    )?);
                } else {
                    // target_mst_key >= current_node.key
                    updated_node.right_child_hash = Some(update_mst(
                        repo_table,
                        current_node.right_child_hash,
                        target_mst_key,
                        target_value_hash,
                    )?);
                }
                put_mst_node(repo_table, &updated_node)
            }
        }
        None => {
            // No node exists here, create a new leaf node
            let new_leaf = MstNode {
                key: target_mst_key,
                left_child_hash: None,
                right_child_hash: None,
                value_hash: Some(target_value_hash),
            };
            put_mst_node(repo_table, &new_leaf)
        }
    }
}

fn get_mst_node(
    repo_table: &impl ReadableTable<Hash, Vec<u8>>,
    node_hash: &Hash,
) -> Result<Option<MstNode>, Box<dyn std::error::Error>> {
    if let Some(guard) = repo_table.get(node_hash)? {
        let value = guard.value();
        let object = from_bytes(value.as_slice())?;
        match object {
            Object::MstNode(node) => Ok(Some(node)),
            _ => Err(format!("Object not an MstNode").into()),
        }
    } else {
        Ok(None)
    }
}

fn put_mst_node(
    repo_table: &mut redb::Table<Hash, Vec<u8>>,
    node: &MstNode,
) -> Result<Hash, Box<dyn std::error::Error>> {
    let (encoded, node_hash) = serialize_and_hash_mst_node(node)?;
    if repo_table.get(&node_hash)?.is_none() {
        repo_table.insert(node_hash, encoded)?;
    }
    Ok(node_hash)
}

fn hash_value(value: &str) -> Hash {
    let blake3_hash = hash(value.as_bytes());
    *blake3_hash.as_bytes()
}

fn create_mst_key(entity: &str, attribute: &str) -> MstKey {
    MstKey {
        entity: entity.to_string(),
        attribute: attribute.to_string(),
    }
}

fn serialize_and_hash_mst_node(
    node: &MstNode,
) -> Result<(Vec<u8>, Hash), Box<dyn std::error::Error>> {
    let encoded = to_stdvec(&Object::MstNode(node.clone()))?;
    let blake3_hash = hash(&encoded);
    let array_hash: Hash = *blake3_hash.as_bytes();
    Ok((encoded, array_hash))
}

fn hex_string(buf: &[u8]) -> String {
    buf.iter().map(|b| format!("{:02x}", b)).collect::<String>()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let db = Database::create(&args.db_path)?;

    match &args.command {
        Commands::Write {
            entity,
            attribute,
            value,
        } => {
            let write_txn = db.begin_write()?;
            {
                let mut eav_table = write_txn.open_table(EAV_TABLE)?;
                eav_table.insert((entity.as_str(), attribute.as_str()), value.as_str())?;

                // Prepare for MST update
                let target_mst_key = create_mst_key(entity, attribute);
                let target_value_hash = hash_value(value);

                let mut repo_table = write_txn.open_table(REPO_TABLE)?;
                let mut refs_table = write_txn.open_table(REFS_TABLE)?;

                // Retrieve current MST root hash
                let current_mst_root_hash = refs_table
                    .get(MST_ROOT_REF_NAME)?
                    .map(|guard| *guard.value());

                // Update the MST and get the new root hash
                let new_mst_root_hash = update_mst(
                    &mut repo_table,
                    current_mst_root_hash,
                    target_mst_key,
                    target_value_hash,
                )?;

                // Store the new MST root hash
                refs_table.insert(MST_ROOT_REF_NAME, &new_mst_root_hash)?;

                // --- Existing logic for storing individual EAV objects and "recent" ref ---
                // Keeping this for now as per current code structure, though it's separate from MST.
                let record = Object::Eav(entity, attribute, value);
                let encoded = to_stdvec(&record)?;
                let hash = hash(&encoded);
                let hash_value = hash.as_bytes();

                if repo_table.get(hash_value)?.is_none() {
                    repo_table.insert(*hash_value, encoded)?;
                }
                refs_table.insert("recent", hash_value)?;
                // --- End of existing logic ---
            }
            write_txn.commit()?;
            println!(
                "Successfully wrote EAV triple ('{}', '{}', '{}') to database at: {:?}",
                entity, attribute, value, args.db_path
            );
        }
        Commands::Read { entity, attribute } => {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(EAV_TABLE)?;
            if let Some(read_value) = table.get(&(entity.as_str(), attribute.as_str()))? {
                println!(
                    "Read from DB: ('{}', '{}', '{}')",
                    entity,
                    attribute,
                    read_value.value()
                );
            } else {
                println!(
                    "EAV triple ('{}', '{}') not found in database.",
                    entity, attribute
                );
            }
        }
        Commands::List { table_name } => {
            let read_txn = db.begin_read()?;
            match table_name {
                Tables::Eav => {
                    let table = read_txn.open_table(EAV_TABLE)?;
                    println!("Listing all items in {}:", EAV_TABLE.name());
                    for result in table.iter()? {
                        let (key_guard, value_guard) = result?;
                        let (entity, attribute) = key_guard.value();
                        println!("('{}', '{}', '{}')", entity, attribute, value_guard.value());
                    }
                }
                Tables::Repo => {
                    let table = read_txn.open_table(REPO_TABLE)?;
                    println!("Listing all items in {}:", REPO_TABLE.name());
                    for result in table.iter()? {
                        let (key_guard, value_guard) = result?;
                        let key_hex = hex_string(&key_guard.value());
                        match from_bytes::<Object>(value_guard.value().as_slice()) {
                            Ok(obj) => println!("('{}', '{:?}')", key_hex, obj),
                            Err(e) => println!("Could not deserialize for {}: {}", key_hex, e),
                        }
                    }
                }
                Tables::Refs => {
                    let table = read_txn.open_table(REFS_TABLE)?;
                    println!("Listing all items in {}:", REFS_TABLE.name());
                    for result in table.iter()? {
                        let (key_guard, value_guard) = result?;
                        let ref_name = key_guard.value();
                        let val_hex = value_guard
                            .value()
                            .iter()
                            .map(|b| format!("{:02x}", b))
                            .collect::<String>();
                        println!("('{}', '{}')", ref_name, val_hex);
                    }
                }
            }
        }
        Commands::Commit => {
            println!("Not yet implemented");
        }
        Commands::Gc => {
            println!("Not yet implemented");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_serialization_deserialization() {
        let original_record = Object::Eav("entity1", "attribute1", "value1");

        let encoded = to_stdvec(&original_record).expect("Failed to serialize record");
        let decoded_record: Object = from_bytes(&encoded).expect("Failed to deserialize record");

        assert_eq!(original_record, decoded_record);
    }
}
