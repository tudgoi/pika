use std::borrow::{Borrow, Cow};
use std::cmp::Ordering;

use ptree::TreeItem;
use redb::{Key, ReadableTable, Table};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::db::Blob;

/// Errors that can occur when interacting with the Merkle Search Tree.
#[derive(Error, Debug)]
pub enum MstError {
    #[error("could not access Redb")]
    RedbError(#[from] redb::StorageError),

    #[error("could not serde using Postcard")]
    PostcardError(#[from] postcard::Error),

    #[error("could not find ref: {hash:?})")]
    RefNotFound { hash: Hash },
}

/// A 32-byte hash used to identify nodes in the MST.
pub type Hash = [u8; 32];

/// Converts a byte slice to a hex string.
pub fn hex_string(buf: &[u8]) -> String {
    buf.iter().map(|b| format!("{:02x}", b)).collect::<String>()
}

/// An item in an MST node.
///
/// It can either be a `Payload` containing a key-value pair, or a `Ref` pointing to a child node's hash.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MstItem<K: Key, V> {
    Payload(K, V),
    Ref(Hash),
}

/// A node in the Merkle Search Tree.
///
/// It contains a list of `MstItem`s, which can be payloads or references to other nodes.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MstNode<K: Key, V>(Vec<MstItem<K, V>>);

impl<
    K: Key + Serialize + for<'de> Deserialize<'de> + PartialEq + Clone,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
> MstNode<K, V>
where
    K: for<'a> Borrow<<K as redb::Value>::SelfType<'a>>,
{
    /// Loads a node from the repository table using its hash.
    ///
    /// # Arguments
    ///
    /// * `repo_table` - The table containing the MST nodes.
    /// * `hash` - The hash of the node to load.
    pub fn load<T: ReadableTable<Hash, Blob>>(
        repo_table: &T,
        hash: &Hash,
    ) -> Result<Self, MstError> {
        let node = Self::get_node_from_repo(repo_table, hash)?
            .ok_or(MstError::RefNotFound { hash: *hash })?;

        Ok(node)
    }

    /// Creates a new, empty MST node.
    pub fn new() -> Self {
        MstNode(Vec::new())
    }

    /// Calculates the level of a key based on its hash.
    ///
    /// The level is determined by the number of leading zeros in the hash, divided by a factor (3).
    /// This simulates a probabilistic skip-list or similar structure where higher levels are rarer.
    fn calc_level(key: &K) -> u32 {
        let key_bytes = K::as_bytes(key.borrow());
        let hash = blake3::hash(key_bytes.as_ref());
        let hash_bytes = hash.as_bytes();
        // Use the first 16 bytes for level calculation to ensure good distribution
        let val = u128::from_be_bytes(hash_bytes[0..16].try_into().unwrap());
        // TODO: Extract '3' into a named constant for clarity (e.g., LEVEL_DIVISOR).
        val.leading_zeros() / 3
    }

    /// Estimates the level of the current node based on the keys it contains.
    ///
    /// If the node has payload items, it returns the calculated level of the first one found.
    fn estimate_level(&self) -> Option<u32> {
        for item in &self.0 {
            if let MstItem::Payload(k, _) = item {
                return Some(Self::calc_level(k));
            }
        }
        None
    }

    /// Finds a value associated with a key in the MST.
    ///
    /// # Arguments
    ///
    /// * `repo_table` - The table containing the MST nodes.
    /// * `key` - The key to search for.
    ///
    /// Returns `Ok(Some(value))` if found, `Ok(None)` if not found, or an `MstError`.
    pub fn find<T: ReadableTable<Hash, Blob>>(
        &self,
        repo_table: &T,
        key: &K,
    ) -> Result<Option<V>, MstError> {
        let key_bytes_borrow = K::as_bytes(key.borrow());

        for (i, item) in self.0.iter().enumerate() {
            match item {
                MstItem::Payload(k, v) => {
                    let k_bytes = K::as_bytes(k.borrow());
                    match K::compare(key_bytes_borrow.as_ref(), k_bytes.as_ref()) {
                        Ordering::Equal => return Ok(Some(v.clone())),
                        Ordering::Less => {
                            // Key is smaller than current payload, check the preceding reference
                            if i > 0 {
                                if let MstItem::Ref(h) = &self.0[i - 1] {
                                    let child = Self::load(repo_table, h)?;
                                    return child.find(repo_table, key);
                                }
                            }
                            return Ok(None);
                        }
                        Ordering::Greater => continue,
                    }
                }
                MstItem::Ref(_) => continue,
            }
        }

        // Key is greater than all payloads, check the last reference if it exists
        if let Some(MstItem::Ref(h)) = self.0.last() {
            let child = Self::load(repo_table, h)?;
            return child.find(repo_table, key);
        }

        Ok(None)
    }

    /// Inserts or updates a key-value pair in the MST.
    ///
    /// # Arguments
    ///
    /// * `repo_table` - The table containing the MST nodes.
    /// * `key` - The key to insert or update.
    /// * `value` - The value to associate with the key.
    ///
    /// Returns the hash of the new root node of the modified tree.
    pub fn upsert(
        &mut self,
        repo_table: &mut Table<Hash, Blob>,
        key: K,
        value: V,
    ) -> Result<Hash, MstError> {
        // TODO: Avoid re-calculating calc_level(key) in recursive calls.
        // Pass req_level as an argument to a private internal helper function.
        let req_level = Self::calc_level(&key);
        let node_level = self.estimate_level().unwrap_or(req_level);

        if req_level > node_level {
            // New key belongs to a higher level: current node must be split
            let (left_hash, right_hash) = self.split(repo_table, &key)?;
            self.0.clear();
            self.0.push(MstItem::Ref(left_hash));
            self.0.push(MstItem::Payload(key, value));
            self.0.push(MstItem::Ref(right_hash));
        } else if req_level == node_level {
            self.insert_local(repo_table, key, value)?;
        } else {
            self.insert_into_child(repo_table, key, value)?;
        }

        Self::put_node_to_repo(repo_table, self)
    }

    /// Inserts a key-value pair directly into the current node (local insertion).
    ///
    /// This method handles finding the correct position, updating existing keys,
    /// or splitting a child reference if the insertion point falls on a reference.
    fn insert_local(
        &mut self,
        repo_table: &mut Table<Hash, Blob>,
        key: K,
        value: V,
    ) -> Result<(), MstError> {
        let mut insert_pos = 0;
        let mut split_target_idx = None;
        let mut found = false;
        let mut prev_was_ref = false;

        {
            let key_bytes_borrow = K::as_bytes(key.borrow());

            for (i, item) in self.0.iter_mut().enumerate() {
                let is_ref = matches!(item, MstItem::Ref(_));
                match item {
                    MstItem::Payload(k, v) => {
                        let k_ref: &K = k;
                        let k_bytes = K::as_bytes(k_ref.borrow());
                        match K::compare(key_bytes_borrow.as_ref(), k_bytes.as_ref()) {
                            Ordering::Equal => {
                                *v = value.clone();
                                found = true;
                                break;
                            }
                            Ordering::Less => {
                                insert_pos = i;
                                if i > 0 && prev_was_ref {
                                    split_target_idx = Some(i - 1);
                                }
                                break;
                            }
                            Ordering::Greater => {
                                insert_pos = i + 1;
                            }
                        }
                    }
                    MstItem::Ref(_) => {
                        if insert_pos == i {
                            insert_pos = i + 1;
                        }
                    }
                }
                prev_was_ref = is_ref;
            }
        }

        if found {
            return Ok(());
        }

        // If insertion position is at the end and the last item is a reference, split it
        if insert_pos == self.0.len() && !self.0.is_empty() {
            if let MstItem::Ref(_) = self.0.last().unwrap() {
                split_target_idx = Some(self.0.len() - 1);
            }
        }

        if let Some(idx) = split_target_idx {
            // Split the child reference and replace it with (left_ref, new_payload, right_ref)
            if let MstItem::Ref(h) = &self.0[idx] {
                let child = Self::load(repo_table, h)?;
                let (l_hash, r_hash) = child.split(repo_table, &key)?;

                self.0.splice(
                    idx..idx + 1,
                    vec![
                        MstItem::Ref(l_hash),
                        MstItem::Payload(key, value),
                        MstItem::Ref(r_hash),
                    ],
                );
            }
        } else {
            self.0.insert(insert_pos, MstItem::Payload(key, value));
        }

        Ok(())
    }

    /// Inserts a key-value pair into a child node.
    ///
    /// This method identifies the correct child reference, loads the child node,
    /// recursively calls upsert on it, and updates the reference in the current node.
    fn insert_into_child(
        &mut self,
        repo_table: &mut Table<Hash, Blob>,
        key: K,
        value: V,
    ) -> Result<(), MstError> {
        let mut child_idx = None;

        {
            let key_bytes_borrow = K::as_bytes(key.borrow());

            for (i, item) in self.0.iter().enumerate() {
                if let MstItem::Payload(k, _) = item {
                    let k_bytes = K::as_bytes(k.borrow());
                    if K::compare(key_bytes_borrow.as_ref(), k_bytes.as_ref()) == Ordering::Less {
                        // Key belongs before current payload, use preceding reference if available
                        if i > 0 && matches!(self.0[i - 1], MstItem::Ref(_)) {
                            child_idx = Some(i - 1);
                        } else {
                            child_idx = Some(i);
                        }
                        break;
                    }
                }
            }
        }

        if child_idx.is_none() {
            // Key is greater than all payloads, use last reference or append to the end
            if let Some(MstItem::Ref(_)) = self.0.last() {
                child_idx = Some(self.0.len() - 1);
            } else {
                child_idx = Some(self.0.len());
            }
        }

        let idx = child_idx.unwrap();

        let mut child_node = if idx < self.0.len() {
            if let MstItem::Ref(h) = &self.0[idx] {
                Self::load(repo_table, h)?
            } else {
                MstNode::new()
            }
        } else {
            MstNode::new()
        };

        let new_child_hash = child_node.upsert(repo_table, key, value)?;

        if idx < self.0.len() {
            if let MstItem::Ref(_) = self.0[idx] {
                self.0[idx] = MstItem::Ref(new_child_hash);
            } else {
                self.0.insert(idx, MstItem::Ref(new_child_hash));
            }
        } else {
            self.0.push(MstItem::Ref(new_child_hash));
        }

        Ok(())
    }

    /// Splits the current node into two nodes (left and right) based on a split key.
    ///
    /// Items less than the split key go to the left node, items greater go to the right node.
    /// If the split point falls on a child reference, that child is also split recursively.
    fn split(
        &self,
        repo_table: &mut Table<Hash, Blob>,
        split_key: &K,
    ) -> Result<(Hash, Hash), MstError> {
        let key_bytes = K::as_bytes(split_key.borrow());

        let mut left_node = MstNode::new();
        let mut right_node = MstNode::new();

        let mut split_index = self.0.len();

        for (i, item) in self.0.iter().enumerate() {
            if let MstItem::Payload(k, _) = item {
                let k_bytes = K::as_bytes(k.borrow());
                if K::compare(k_bytes.as_ref(), key_bytes.as_ref()) == Ordering::Greater {
                    split_index = i;
                    break;
                }
            }
        }

        let mut ref_to_split_idx = None;
        if split_index > 0 {
            if let MstItem::Ref(_) = self.0[split_index - 1] {
                ref_to_split_idx = Some(split_index - 1);
            }
        } else if split_index == 0 {
            if !self.0.is_empty() {
                if let MstItem::Ref(_) = self.0[0] {
                    ref_to_split_idx = Some(0);
                }
            }
        }

        let limit = ref_to_split_idx.unwrap_or(split_index);

        // Copy items before the split point to the left node
        // TODO: Avoid cloning items. Since `upsert` clears `self.0` after split,
        // we can consume `self` or use `std::mem::take` to move items.
        for i in 0..limit {
            left_node.0.push(self.0[i].clone());
        }

        if let Some(idx) = ref_to_split_idx {
            // Recursively split the child reference at the split point
            if let MstItem::Ref(h) = &self.0[idx] {
                let child = Self::load(repo_table, h)?;
                let (l_hash, r_hash) = child.split(repo_table, split_key)?;
                left_node.0.push(MstItem::Ref(l_hash));
                right_node.0.push(MstItem::Ref(r_hash));
            }
        }

        let start = if ref_to_split_idx.is_some() {
            ref_to_split_idx.unwrap() + 1
        } else {
            split_index
        };

        // Copy items after the split point to the right node
        for i in start..self.0.len() {
            right_node.0.push(self.0[i].clone());
        }

        let l_hash = Self::put_node_to_repo(repo_table, &left_node)?;
        let r_hash = Self::put_node_to_repo(repo_table, &right_node)?;

        Ok((l_hash, r_hash))
    }

    /// Retrieves a node from the repository table by its hash.
    ///
    /// Deserializes the node using `postcard`.
    fn get_node_from_repo<T: ReadableTable<Hash, Blob>>(
        repo_table: &T,
        node_hash: &Hash,
    ) -> Result<Option<MstNode<K, V>>, MstError> {
        if let Some(guard) = repo_table.get(node_hash)? {
            let value = guard.value();
            let node = postcard::from_bytes(value.as_slice())?;
            Ok(Some(node))
        } else {
            Ok(None)
        }
    }

    /// Serializes and saves a node to the repository table.
    ///
    /// Returns the hash of the serialized node. If the node already exists, it is not overwritten.
    fn put_node_to_repo(
        repo_table: &mut Table<Hash, Blob>,
        node: &MstNode<K, V>,
    ) -> Result<Hash, MstError> {
        let encoded = postcard::to_stdvec(node)?;
        let blake3_hash = blake3::hash(&encoded);
        let node_hash: Hash = *blake3_hash.as_bytes();

        if repo_table.get(&node_hash)?.is_none() {
            repo_table.insert(&node_hash, encoded)?;
        }
        Ok(node_hash)
    }
}

/// A wrapper struct for `MstItem` to implement `ptree::TreeItem` for visualization.
pub struct MstTreeItem<'a, K: Key + Clone, V: Clone, T: ReadableTable<Hash, Blob>> {
    pub item: MstItem<K, V>,
    pub repo_table: &'a T,
}

impl<'a, K: Key + Clone, V: Clone, T: ReadableTable<Hash, Blob>> Clone for MstTreeItem<'a, K, V, T> {
    fn clone(&self) -> Self {
        Self {
            item: self.item.clone(),
            repo_table: self.repo_table,
        }
    }
}

impl<
    'a,
    K: Key + Clone + Serialize + for<'de> Deserialize<'de> + Ord,
    V: Clone + Serialize + for<'de> Deserialize<'de>,
    T: ReadableTable<Hash, Blob>,
> TreeItem for MstTreeItem<'a, K, V, T>
where
    K: for<'b> Borrow<<K as redb::Value>::SelfType<'b>>,
{
    type Child = Self;

    fn write_self<W: std::io::Write>(
        &self,
        f: &mut W,
        _style: &ptree::Style,
    ) -> std::io::Result<()> {
        match &self.item {
            MstItem::Payload(key, _) => write!(f, "{:?}", key)?,
            MstItem::Ref(hash) => write!(f, "[{:}]", hex_string(hash))?,
        };

        Ok(())
    }

    fn children(&self) -> Cow<'_, [Self::Child]> {
        let mut children_vec = Vec::new();
        if let MstItem::Ref(hash) = self.item {
            if let Ok(Some(node)) = MstNode::get_node_from_repo(self.repo_table, &hash) {
                for item in node.0 {
                    children_vec.push(MstTreeItem {
                        item,
                        repo_table: self.repo_table,
                    });
                }
            }
        }

        children_vec.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::{Database, TableDefinition};
    use tempfile::tempdir;

    const TEST_REPO_TABLE: TableDefinition<Hash, Blob> = TableDefinition::new("test_repo");

    type TestKey = String;
    type TestValue = String;

    /// Helper function to create a temporary Redb database for testing.
    fn setup_test_db() -> (Database, tempfile::TempDir) {
        let dir = tempdir().expect("Failed to create temp dir");
        let db_path = dir.path().join("test.redb");
        let db = Database::create(db_path).expect("Failed to create database");
        (db, dir)
    }

    #[test]
    /// Tests inserting a new key-value pair into an empty MST.
    ///
    /// Verifies that the insertion returns a valid root hash and the value can be retrieved.
    fn test_mst_upsert_new_key() {
        let (db, _dir) = setup_test_db();
        {
            let write_txn = db.begin_write().unwrap();
            {
                let mut repo_table = write_txn.open_table(TEST_REPO_TABLE).unwrap();
                let mut mst = MstNode::<TestKey, TestValue>::new();

                let key = "test_key".to_string();
                let value = "test_value".to_string();

                let root_hash = mst
                    .upsert(&mut repo_table, key.clone(), value.clone())
                    .unwrap();
                assert!(!root_hash.is_empty());

                let found_value = mst.find(&repo_table, &key).unwrap();
                assert_eq!(found_value, Some(value));
            }
            write_txn.commit().unwrap();
        }
    }

    #[test]
    /// Tests updating the value of an existing key.
    ///
    /// Verifies that updating a key results in a new root hash and the new value is retrievable.
    fn test_mst_upsert_update_existing_key() {
        let (db, _dir) = setup_test_db();
        {
            let write_txn = db.begin_write().unwrap();
            {
                let mut repo_table = write_txn.open_table(TEST_REPO_TABLE).unwrap();
                let mut mst = MstNode::<TestKey, TestValue>::new();

                let key = "test_key".to_string();
                let initial_value = "initial_value".to_string();
                let updated_value = "updated_value".to_string();

                let initial_root_hash = mst
                    .upsert(&mut repo_table, key.clone(), initial_value)
                    .unwrap();
                let updated_root_hash = mst
                    .upsert(&mut repo_table, key.clone(), updated_value.clone())
                    .unwrap();

                assert_ne!(initial_root_hash, updated_root_hash);

                let found_value = mst.find(&repo_table, &key).unwrap();
                assert_eq!(found_value, Some(updated_value));
            }
            write_txn.commit().unwrap();
        }
    }

    #[test]
    /// Tests searching for a key that does not exist in the MST.
    ///
    /// Verifies that `find` returns `None` for non-existent keys.
    fn test_mst_find_non_existent_key() {
        let (db, _dir) = setup_test_db();
        {
            let write_txn = db.begin_write().unwrap();
            {
                let repo_table = write_txn.open_table(TEST_REPO_TABLE).unwrap();
                let mst = MstNode::<TestKey, TestValue>::new();

                let key = "non_existent_key".to_string();
                let found_value = mst.find(&repo_table, &key).unwrap();
                assert_eq!(found_value, None);
            }
            write_txn.commit().unwrap();
        }
    }

    #[test]
    /// Tests inserting a larger number of keys to trigger multi-level structure creation.
    ///
    /// Inserts 50 keys and verifies that all of them can be correctly retrieved.
    /// This implicitly tests node splitting and child node management.
    fn test_mst_structure_multilevel() {
        let (db, _dir) = setup_test_db();
        {
            let write_txn = db.begin_write().unwrap();
            {
                let mut repo_table = write_txn.open_table(TEST_REPO_TABLE).unwrap();
                let mut mst = MstNode::<TestKey, TestValue>::new();

                for i in 0..50 {
                    mst.upsert(&mut repo_table, format!("key_{}", i), format!("val_{}", i))
                        .unwrap();
                }

                for i in 0..50 {
                    let val = mst.find(&repo_table, &format!("key_{}", i)).unwrap();
                    assert_eq!(val, Some(format!("val_{}", i)));
                }
            }
            write_txn.commit().unwrap();
        }
    }
}
