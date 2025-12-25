use blake3::hash;
use postcard::{from_bytes, to_stdvec};
use redb::{ReadableTable, TableDefinition, Table, ReadableDatabase};
use serde::{Deserialize, Serialize};
use std::fmt;
use termtree::Tree;

// --- Type Aliases ---
pub type Entity = str;
pub type Attribute = str;
pub type EavValue = str;
pub type RefName = str;
pub type Hash = [u8; 32];
pub type Blob = Vec<u8>; // Redefined as Vec<u8> for redb compatibility

// --- Table Definitions and Constants ---
pub const EAV_TABLE: TableDefinition<(&Entity, &Attribute), &EavValue> = TableDefinition::new("eav");
pub const REPO_TABLE: TableDefinition<Hash, Blob> = TableDefinition::new("repo");
pub const REFS_TABLE: TableDefinition<&RefName, &Hash> = TableDefinition::new("refs");
pub const MST_ROOT_REF_NAME: &str = "mst_root";

// --- Structs and Enums ---
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub enum Object<'a> {
    Eav(&'a str, &'a str, &'a str),
    RefList(Vec<Hash>),
    MstNode(MstNode), // Added MstNode variant
}

#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub struct MstNode {
    pub key: MstKey,
    pub left_child_hash: Option<Hash>,
    pub right_child_hash: Option<Hash>,
    pub value_hash: Option<Hash>,
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

pub fn hex_string(buf: &[u8]) -> String {
    buf.iter().map(|b| format!("{:02x}", b)).collect::<String>()
}

pub fn hash_value(value: &str) -> Hash {
    let blake3_hash = hash(value.as_bytes());
    *blake3_hash.as_bytes()
}

pub fn create_mst_key(entity: &str, attribute: &str) -> MstKey {
    MstKey {
        entity: entity.to_string(),
        attribute: attribute.to_string(),
    }
}

pub fn serialize_and_hash_mst_node(
    node: &MstNode,
) -> Result<(Vec<u8>, Hash), Box<dyn std::error::Error>> {
    let encoded = to_stdvec(&Object::MstNode(node.clone()))?;
    let blake3_hash = hash(&encoded);
    let array_hash: Hash = *blake3_hash.as_bytes();
    Ok((encoded, array_hash))
}

pub fn get_mst_node(
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

pub fn put_mst_node(
    repo_table: &mut Table<Hash, Vec<u8>>,
    node: &MstNode,
) -> Result<Hash, Box<dyn std::error::Error>> {
    let (encoded, node_hash) = serialize_and_hash_mst_node(node)?;
    if repo_table.get(&node_hash)?.is_none() {
        repo_table.insert(node_hash, encoded)?;
    }
    Ok(node_hash)
}

pub fn update_mst(
    repo_table: &mut Table<Hash, Vec<u8>>,
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

pub fn store_eav_object_and_update_recent_ref(
    repo_table: &mut Table<Hash, Blob>,
    refs_table: &mut Table<&RefName, &Hash>,
    entity: &str,
    attribute: &str,
    value: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let record = Object::Eav(entity, attribute, value);
    let encoded = to_stdvec(&record)?;
    let blake3_hash = hash(&encoded);
    let hash_value = *blake3_hash.as_bytes();

    if repo_table.get(&hash_value)?.is_none() {
        repo_table.insert(hash_value, encoded)?;
    }
    refs_table.insert("recent", &hash_value)?;
    Ok(())
}

pub struct Db<'db> {
    db: &'db redb::Database,
}

impl<'db> Db<'db> {
    pub fn new(db: &'db redb::Database) -> Self {
        Db { db }
    }

    pub fn write(
        &self,
        entity: &str,
        attribute: &str,
        value: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let write_txn = self.db.begin_write()?;
        {
            let mut eav_table = write_txn.open_table(EAV_TABLE)?;
            eav_table.insert((entity, attribute), value)?;

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

            // Use the helper function from repo module
            store_eav_object_and_update_recent_ref(
                &mut repo_table,
                &mut refs_table,
                entity,
                attribute,
                value,
            )?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn read(
        &self,
        entity: &str,
        attribute: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(EAV_TABLE)?;
        if let Some(read_value) = table.get(&(entity, attribute))? {
            Ok(Some(read_value.value().to_string()))
        } else {
            Ok(None)
        }
    }
}

pub fn build_mst_tree_recursive(
    repo_table: &impl ReadableTable<Hash, Vec<u8>>,
    node_hash: Option<Hash>,
) -> Result<Tree<String>, Box<dyn std::error::Error>> {
    if let Some(hash) = node_hash {
        let node = get_mst_node(repo_table, &hash)?.expect("Node should exist");
        let node_label = format!("Node ({}): {:?}", hex_string(&hash), node.key);

        let mut children_trees = Vec::new();

        if let Some(left_child_hash) = node.left_child_hash {
            children_trees.push(build_mst_tree_recursive(repo_table, Some(left_child_hash))?);
        }

        if let Some(right_child_hash) = node.right_child_hash {
            children_trees.push(build_mst_tree_recursive(repo_table, Some(right_child_hash))?);
        }

        if let Some(value_hash) = node.value_hash {
            children_trees.push(Tree::new(format!("Value: {}", hex_string(&value_hash))));
        }

        Ok(Tree::new(node_label).with_leaves(children_trees))
    } else {
        Ok(Tree::new("None".to_string()))
    }
}

pub fn print_mst_recursive(
    repo_table: &impl ReadableTable<Hash, Vec<u8>>,
    node_hash: Option<Hash>,
) -> Result<(), Box<dyn std::error::Error>> {
    let tree = build_mst_tree_recursive(repo_table, node_hash)?;
    println!("{}", tree);
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
