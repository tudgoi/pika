use std::borrow::Borrow;
use std::cmp::Ordering;

use redb::{Key, ReadableTable, Table};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::db::Blob;

/// Errors that can occur when interacting with the Prolly Tree.
#[derive(Error, Debug)]
pub enum PtError {
    #[error("could not access Redb")]
    RedbError(#[from] redb::StorageError),

    #[error("could not serde using Postcard")]
    PostcardError(#[from] postcard::Error),

    #[error("could not find ref: {hash:?})")]
    RefNotFound { hash: Hash },
}

/// A 32-byte hash used to identify nodes in the PT.
pub type Hash = [u8; 32];

/// Converts a byte slice to a hex string.
pub fn hex_string(buf: &[u8]) -> String {
    buf.iter().map(|b| format!("{:02x}", b)).collect::<String>()
}

/// An item in a Prolly Tree node.
///
/// It can either be a `Payload` containing a key-value pair (in leaf nodes),
/// or a `Ref` pointing to a child node's hash (in internal nodes).
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum PtItem<K: Key, V> {
    /// A leaf item containing the key and value.
    Payload(K, V),
    /// A reference to a child node.
    /// `K` is the first key in the child node (used for navigation).
    /// `Hash` is the content-addressed hash of the child node.
    Ref(K, Hash),
}

impl<K: Key, V> PtItem<K, V> {
    pub fn key(&self) -> &K {
        match self {
            PtItem::Payload(k, _) => k,
            PtItem::Ref(k, _) => k,
        }
    }
}

/// A node in the Prolly Tree.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PtNode<K: Key, V>(Vec<PtItem<K, V>>);

// Parameters for the rolling hash (Buzhash)
// Using a small window and modulus for demonstration purposes to ensure splits occur.
// In production, these would be tuned for target block size (e.g., 4KB).
const WINDOW_SIZE: usize = 32;
// A small modulus ensures frequent splits for small datasets (good for testing/demo).
// For real 4KB blocks, use something like 1 << 12 (4096).
const CHUNK_MODULUS: u32 = 1 << 6; 

/// Simple cyclic polynomial rolling hash (Buzhash-like)
struct RollingHash {
    window: [u8; WINDOW_SIZE],
    pos: usize,
    sum: u32,
}

impl RollingHash {
    fn new() -> Self {
        Self {
            window: [0; WINDOW_SIZE],
            pos: 0,
            sum: 0,
        }
    }

    fn update(&mut self, b: u8) {
        let old = self.window[self.pos];
        self.window[self.pos] = b;
        self.pos = (self.pos + 1) % WINDOW_SIZE;
        
        // Simple rotation and addition (not a full Buzhash with table, but sufficient for distribution)
        self.sum = self.sum.rotate_left(1).wrapping_sub(old as u32).wrapping_add(b as u32);
    }

    fn is_boundary(&self) -> bool {
        self.sum % CHUNK_MODULUS == 0
    }
}

impl<
    K: Key + Serialize + for<'de> Deserialize<'de> + PartialEq + Clone + Ord,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
> PtNode<K, V>
where
    K: for<'a> Borrow<<K as redb::Value>::SelfType<'a>>,
{
    /// Loads a node from the repository table using its hash.
    pub fn load<T: ReadableTable<Hash, Blob>>(
        repo_table: &T,
        hash: &Hash,
    ) -> Result<Self, PtError> {
        let node = Self::get_node_from_repo(repo_table, hash)?
            .ok_or(PtError::RefNotFound { hash: *hash })?;

        Ok(node)
    }

    /// Creates a new, empty PT node.
    pub fn new() -> Self {
        PtNode(Vec::new())
    }

    /// Finds a value associated with a key in the PT.
    pub fn find<T: ReadableTable<Hash, Blob>>(
        &self,
        repo_table: &T,
        key: &K,
    ) -> Result<Option<V>, PtError> {
        let key_bytes_borrow = K::as_bytes(key.borrow());

        // Binary search to find the position
        let idx = self.0.partition_point(|item| {
            let item_key_bytes = K::as_bytes(item.key().borrow());
            K::compare(item_key_bytes.as_ref(), key_bytes_borrow.as_ref()) != Ordering::Greater
        });

        // For Payload (Leaf): exact match required.
        // For Ref (Internal): we need the child that covers the key range.
        
        // If empty, nothing to find.
        if self.0.is_empty() {
            return Ok(None);
        }

        // Check the item at idx-1 (since partition_point returns the first item > key)
        // Or if we are in a leaf, we might check idx-1 for equality if exact match.
        
        // Strategy: Iterate to check types.
        let is_leaf = matches!(self.0[0], PtItem::Payload(_, _));

        if is_leaf {
            // In a leaf, we look for exact match.
            // partition_point gave us the first item > key.
            // So we check idx - 1.
            if idx > 0 {
                if let PtItem::Payload(k, v) = &self.0[idx - 1] {
                     let k_bytes = K::as_bytes(k.borrow());
                     if K::compare(k_bytes.as_ref(), key_bytes_borrow.as_ref()) == Ordering::Equal {
                         return Ok(Some(v.clone()));
                     }
                }
            }
            // Check idx as well just in case (though partition_point semantics say idx is > key)
            // Wait, partition_point: "returns the index of the first element satisfying the predicate is false".
            // Predicate: item <= key.
            // False when: item > key.
            // So idx is the first item strictly greater than key.
            // So the candidate is indeed idx - 1.
            
            // However, if the list is [A, C] and we look for B.
            // A <= B (True). C <= B (False). idx = 1 (points to C).
            // Check idx-1 (A). A != B. Not found.
            
            // If list is [A, B] and we look for B.
            // A <= B (True). B <= B (True). idx = 2 (end).
            // Check idx-1 (B). B == B. Found.
            
            return Ok(None);
        } else {
            // Internal node.
            // We need the child where child.first_key <= key.
            // partition_point gave us the first child where child.first_key > key.
            // So we want child at idx - 1.
            if idx > 0 {
                if let PtItem::Ref(_, h) = &self.0[idx - 1] {
                    let child = Self::load(repo_table, h)?;
                    return child.find(repo_table, key);
                }
            }
            // If idx == 0, it means the key is smaller than the first key in this node.
            // In a valid B-tree/Prolly tree, the parent would have routed us to the correct sibling.
            // However, the first child in a node conceptually covers (-inf, key2).
            // Usually the first key in a node is just the lower bound of that node.
            // If we are here, we should check the first child?
            // Prolly tree logic: Keys are sorted globally.
            // If idx == 0, try the first child?
            // No, if key < self.0[0].key, then it can't be in this subtree if we enforce strict ranges.
            return Ok(None);
        }
    }

    /// Inserts or updates a key-value pair in the PT.
    /// Returns a list of `PtItem::Ref`s that should replace the reference to this node in the parent.
    /// This allows the node to split (returning multiple refs) or stay same (returning one ref).
    pub fn upsert(
        &self,
        repo_table: &mut Table<Hash, Blob>,
        key: K,
        value: V,
    ) -> Result<Vec<PtItem<K, V>>, PtError> {
        let is_leaf = self.0.is_empty() || matches!(self.0[0], PtItem::Payload(_, _));
        
        let mut new_items: Vec<PtItem<K, V>>;

        if is_leaf {
             // 1. Modify the items list
             new_items = self.0.clone();
             
             // Find insertion point
             // We use a scope to limit the borrow of key
             let idx = {
                 let key_bytes_borrow = K::as_bytes(key.borrow());
                 new_items.partition_point(|item| {
                    let item_key_bytes = K::as_bytes(item.key().borrow());
                    K::compare(item_key_bytes.as_ref(), key_bytes_borrow.as_ref()) == Ordering::Less
                 })
             };
             
             // Check if update or insert
             let mut is_update = false;
             if idx < new_items.len() {
                 let is_equal = {
                     let key_bytes_borrow = K::as_bytes(key.borrow());
                     let curr_key_bytes = K::as_bytes(new_items[idx].key().borrow());
                     K::compare(curr_key_bytes.as_ref(), key_bytes_borrow.as_ref()) == Ordering::Equal
                 };
                 
                 if is_equal {
                     new_items[idx] = PtItem::Payload(key.clone(), value.clone());
                     is_update = true;
                 }
             }
             
             if !is_update {
                 new_items.insert(idx, PtItem::Payload(key, value));
             }
        } else {
             // Internal node: find child and recurse
             
             let idx = {
                 let key_bytes_borrow = K::as_bytes(key.borrow());
                 self.0.partition_point(|item| {
                    let item_key_bytes = K::as_bytes(item.key().borrow());
                    K::compare(item_key_bytes.as_ref(), key_bytes_borrow.as_ref()) != Ordering::Greater
                 })
             };
             
             let child_idx = if idx > 0 { idx - 1 } else { 0 };
             
             // Defensive check if empty (shouldn't happen for valid internal node unless root was empty internal?)
             if self.0.is_empty() {
                 // Should have been treated as leaf
                 return Ok(vec![]);
             }

             if let PtItem::Ref(_, child_hash) = &self.0[child_idx] {
                 let child = Self::load(repo_table, child_hash)?;
                 let new_child_refs = child.upsert(repo_table, key, value)?;
                 
                 new_items = self.0.clone();
                 // Replace the single old ref with the new refs (which could be 1 or many)
                 new_items.splice(child_idx..child_idx+1, new_child_refs);
             } else {
                 return Err(PtError::RefNotFound { hash: [0;32] }); // Should not happen
             }
        }

        // 2. Re-chunk (Split) based on Rolling Hash
        Self::chunk_and_save(repo_table, new_items)
    }
    
    /// Processes a list of items, chunks them using the rolling hash,
    /// saves the chunks to the repo, and returns references to them.
    pub fn chunk_and_save(
        repo_table: &mut Table<Hash, Blob>,
        items: Vec<PtItem<K, V>>,
    ) -> Result<Vec<PtItem<K, V>>, PtError> {
        let mut result_refs = Vec::new();
        let mut current_chunk = Vec::new();
        let mut hasher = RollingHash::new();

        for item in items {
            // Update hasher with item content
            {
                let key_bytes = K::as_bytes(item.key().borrow());
                for b in key_bytes.as_ref() {
                    hasher.update(*b);
                }
            }

            current_chunk.push(item);

            if hasher.is_boundary() && !current_chunk.is_empty() {
                let node = PtNode(current_chunk);
                let hash = Self::save(repo_table, &node)?;
                // The key for the Ref is the first key in the chunk
                let first_key = node.0[0].key().clone();
                result_refs.push(PtItem::Ref(first_key, hash));
                current_chunk = Vec::new();
                // We do NOT reset the hasher. Prolly trees roll continuously.
            }
        }

        // Handle remaining items
        if !current_chunk.is_empty() {
            let node = PtNode(current_chunk);
            let hash = Self::save(repo_table, &node)?;
            let first_key = node.0[0].key().clone();
            result_refs.push(PtItem::Ref(first_key, hash));
        }

        Ok(result_refs)
    }

    fn get_node_from_repo<T: ReadableTable<Hash, Blob>>(
        repo_table: &T,
        node_hash: &Hash,
    ) -> Result<Option<PtNode<K, V>>, PtError> {
        if let Some(guard) = repo_table.get(node_hash)? {
            let value = guard.value();
            let node = postcard::from_bytes(value.as_slice())?;
            Ok(Some(node))
        } else {
            Ok(None)
        }
    }

    /// Serializes and saves the node to the repository. Returns the hash.
    pub fn save(
        repo_table: &mut Table<Hash, Blob>,
        node: &PtNode<K, V>,
    ) -> Result<Hash, PtError> {
        let encoded = postcard::to_stdvec(node)?;
        let blake3_hash = blake3::hash(&encoded);
        let node_hash: Hash = *blake3_hash.as_bytes();

        if repo_table.get(&node_hash)?.is_none() {
            repo_table.insert(&node_hash, encoded)?;
        }
        Ok(node_hash)
    }

    /// Calculates the height of the tree starting from this node.
    /// Used in tests.
    #[allow(dead_code)]
    pub fn height<T: ReadableTable<Hash, Blob>>(
        &self,
        repo_table: &T,
    ) -> Result<usize, PtError> {
        if self.0.is_empty() {
            return Ok(1);
        }
        if let PtItem::Ref(_, hash) = &self.0[0] {
            let child = Self::load(repo_table, hash)?;
            return Ok(1 + child.height(repo_table)?);
        }
        Ok(1)
    }
}

// --- Visualization ---

use ptree::TreeItem;
use std::borrow::Cow;

/// A wrapper struct for `PtItem` to implement `ptree::TreeItem` for visualization.
pub struct PtTreeItem<'a, K: Key + Clone, V: Clone, T: ReadableTable<Hash, Blob>> {
    pub item: PtItem<K, V>,
    pub repo_table: &'a T,
}

impl<'a, K: Key + Clone, V: Clone, T: ReadableTable<Hash, Blob>> Clone for PtTreeItem<'a, K, V, T> {
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
    T: ReadableTable<Hash, Blob>
> TreeItem for PtTreeItem<'a, K, V, T>
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
            PtItem::Payload(key, _) => write!(f, "{:?}", key)?,
            PtItem::Ref(first_key, hash) => write!(f, "[{:?}] @ {:}", first_key, hex_string(hash))?,
        };

        Ok(())
    }

    fn children(&self) -> Cow<'_, [Self::Child]> {
        let mut children_vec = Vec::new();
        if let PtItem::Ref(_, hash) = self.item {
            if let Ok(Some(node)) = PtNode::get_node_from_repo(self.repo_table, &hash) {
                for item in node.0 {
                    children_vec.push(PtTreeItem {
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

    const TEST_REPO_TABLE: TableDefinition<Hash, Blob> = TableDefinition::new("test_pt_repo");

    type TestKey = String;
    type TestValue = String;

    fn setup_test_db() -> (Database, tempfile::TempDir) {
        let dir = tempdir().expect("Failed to create temp dir");
        let db_path = dir.path().join("test_pt.redb");
        let db = Database::create(db_path).expect("Failed to create database");
        (db, dir)
    }

    #[test]
    fn test_pt_upsert_find() {
        let (db, _dir) = setup_test_db();
        let write_txn = db.begin_write().unwrap();
        {
            let mut repo_table = write_txn.open_table(TEST_REPO_TABLE).unwrap();
            
            // 1. Initial Insert
            let node = PtNode::<TestKey, TestValue>::new();
            // Upsert returns a list of Refs (children of a hypothetical root).
            // We need to manage the root manually for the test.
            let refs = node.upsert(&mut repo_table, "key1".to_string(), "val1".to_string()).unwrap();
            
            // Create a root node pointing to these refs
            let root = PtNode(refs);
            let root_hash = PtNode::save(&mut repo_table, &root).unwrap();

            // 2. Find
            let loaded_root = PtNode::<TestKey, TestValue>::load(&repo_table, &root_hash).unwrap();
            let val = loaded_root.find(&repo_table, &"key1".to_string()).unwrap();
            assert_eq!(val, Some("val1".to_string()));
        }
        write_txn.commit().unwrap();
    }
    
    #[test]
    fn test_pt_split() {
        let (db, _dir) = setup_test_db();
        let write_txn = db.begin_write().unwrap();
        {
            let mut repo_table = write_txn.open_table(TEST_REPO_TABLE).unwrap();
            
            let mut current_refs = PtNode::<TestKey, TestValue>::new()
                .upsert(&mut repo_table, "init".to_string(), "val".to_string())
                .unwrap();
                
            let mut root = PtNode(current_refs.clone());
            let mut root_hash = PtNode::save(&mut repo_table, &root).unwrap();

            // Insert enough items to force splits
            for i in 0..100 {
                let loaded_root = PtNode::<TestKey, TestValue>::load(&repo_table, &root_hash).unwrap();
                current_refs = loaded_root.upsert(
                    &mut repo_table, 
                    format!("key_{:03}", i), 
                    format!("val_{}", i)
                ).unwrap();
                
                // If current_refs has > 1 item, the root effectively split (height grew)
                // or we just have a new set of children for the root.
                // We wrap them in a new root.
                root = PtNode(current_refs.clone());
                root_hash = PtNode::save(&mut repo_table, &root).unwrap();
            }
            
            // Verify we can find them all
            let loaded_root = PtNode::<TestKey, TestValue>::load(&repo_table, &root_hash).unwrap();
            for i in 0..100 {
                let key = format!("key_{:03}", i);
                let val = loaded_root.find(&repo_table, &key).unwrap();
                assert_eq!(val, Some(format!("val_{}", i)));
            }
        }
    }

    #[test]
    fn test_height_growth() {
        let (db, _dir) = setup_test_db();
        let write_txn = db.begin_write().unwrap();
        {
            let mut repo_table = write_txn.open_table(TEST_REPO_TABLE).unwrap();
            let mut root_hash = PtNode::<TestKey, TestValue>::save(&mut repo_table, &PtNode::new()).unwrap();

            for i in 0..100 {
                let root = PtNode::<TestKey, TestValue>::load(&repo_table, &root_hash).unwrap();
                let mut current_refs = root.upsert(&mut repo_table, format!("k{:02}", i), format!("v{:02}", i)).unwrap();
                
                // Proper root management
                while current_refs.len() > 1 {
                    current_refs = PtNode::chunk_and_save(&mut repo_table, current_refs).unwrap();
                }
                
                if let Some(PtItem::Ref(_, hash)) = current_refs.first() {
                    root_hash = *hash;
                }

                let final_root = PtNode::<TestKey, TestValue>::load(&repo_table, &root_hash).unwrap();
                let current_height = final_root.height(&repo_table).unwrap();
                
                // For 100 items and CHUNK_MODULUS 64, height should be very small (2 or 3)
                assert!(current_height <= 3, "Height grew too much: {} at iteration {}", current_height, i);
            }
        }
    }
}
