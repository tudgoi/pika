use std::borrow::{Borrow, Cow};
use std::cmp::Ordering;

use ptree::TreeItem;
use redb::{Key, ReadableTable, Table};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::db::Blob;

#[derive(Error, Debug)]
pub enum MstError {
    #[error("could not access Redb")]
    RedbError(#[from] redb::StorageError),

    #[error("could not serde using Postcard")]
    PostcardError(#[from] postcard::Error),

    #[error("could not find ref: {hash:?})")]
    RefNotFound { hash: Hash },
}

pub type Hash = [u8; 32];

pub fn hex_string(buf: &[u8]) -> String {
    buf.iter().map(|b| format!("{:02x}", b)).collect::<String>()
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum MstItem<K: Key, V> {
    Payload(K, V),
    Ref(Hash),
}

impl<K: Key, V> MstItem<K, V> {
    pub fn get_key(&self) -> Option<&K> {
        match self {
            MstItem::Payload(k, _) => Some(k),
            MstItem::Ref(_) => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MstNode<K: Key, V>(Vec<MstItem<K, V>>);

impl<
    K: Key + Serialize + for<'de> Deserialize<'de> + PartialEq + Clone,
    V: Serialize + for<'de> Deserialize<'de> + Clone,
> MstNode<K, V>
where
    K: for<'a> Borrow<<K as redb::Value>::SelfType<'a>>,
{
    pub fn load<T: ReadableTable<Hash, Blob>>(
        repo_table: &T,
        root_hash: &Hash,
    ) -> Result<Self, MstError> {
        let node = Self::get_node_from_repo(repo_table, root_hash)?
            .ok_or(MstError::RefNotFound { hash: *root_hash })?;

        Ok(node)
    }

    pub fn new() -> Self {
        MstNode(Vec::new())
    }

    fn calc_level(key: &K) -> u32 {
        let key_bytes = K::as_bytes(key.borrow());
        let hash = blake3::hash(key_bytes.as_ref());
        let hash_bytes = hash.as_bytes();
        let val = u128::from_be_bytes(hash_bytes[0..16].try_into().unwrap());
        val.leading_zeros() / 3
    }

    fn estimate_level(&self) -> Option<u32> {
        for item in &self.0 {
            if let MstItem::Payload(k, _) = item {
                return Some(Self::calc_level(k));
            }
        }
        None
    }

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

        if let Some(MstItem::Ref(h)) = self.0.last() {
            let child = Self::load(repo_table, h)?;
            return child.find(repo_table, key);
        }

        Ok(None)
    }

    pub fn upsert(
        &mut self,
        repo_table: &mut Table<Hash, Blob>,
        key: K,
        value: V,
    ) -> Result<Hash, MstError> {
        let req_level = Self::calc_level(&key);
        let node_level = self.estimate_level().unwrap_or(req_level);

        if req_level > node_level {
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
                                    split_target_idx = Some(i-1);
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
        
        if insert_pos == self.0.len() && !self.0.is_empty() {
             if let MstItem::Ref(_) = self.0.last().unwrap() {
                 split_target_idx = Some(self.0.len() - 1);
             }
        }
        
        if let Some(idx) = split_target_idx {
             if let MstItem::Ref(h) = &self.0[idx] {
                 let child = Self::load(repo_table, h)?;
                 let (l_hash, r_hash) = child.split(repo_table, &key)?;
                 
                 self.0.splice(idx..idx+1, vec![
                     MstItem::Ref(l_hash),
                     MstItem::Payload(key, value),
                     MstItem::Ref(r_hash),
                 ]);
             }
        } else {
            self.0.insert(insert_pos, MstItem::Payload(key, value));
        }
        
        Ok(())
    }

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
                        if i > 0 && matches!(self.0[i-1], MstItem::Ref(_)) {
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
        
        for i in 0..limit {
            left_node.0.push(self.0[i].clone());
        }
        
        if let Some(idx) = ref_to_split_idx {
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
        
        for i in start..self.0.len() {
            right_node.0.push(self.0[i].clone());
        }
        
        let l_hash = Self::put_node_to_repo(repo_table, &left_node)?;
        let r_hash = Self::put_node_to_repo(repo_table, &right_node)?;
        
        Ok((l_hash, r_hash))
    }

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

#[derive(Clone)]
pub struct MstTreeItem<'a, K: Key + Clone, V: Clone> {
    pub item: MstItem<K, V>,
    pub repo_table: &'a Table<'a, Hash, Blob>,
}

impl<
    'a,
    K: Key + Clone + Serialize + for<'de> Deserialize<'de> + Ord,
    V: Clone + Serialize + for<'de> Deserialize<'de>,
> TreeItem for MstTreeItem<'a, K, V>
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

    fn setup_test_db() -> (Database, tempfile::TempDir) {
        let dir = tempdir().expect("Failed to create temp dir");
        let db_path = dir.path().join("test.redb");
        let db = Database::create(db_path).expect("Failed to create database");
        (db, dir)
    }

    #[test]
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
    fn test_mst_structure_multilevel() {
        let (db, _dir) = setup_test_db();
        {
            let write_txn = db.begin_write().unwrap();
            {
                let mut repo_table = write_txn.open_table(TEST_REPO_TABLE).unwrap();
                let mut mst = MstNode::<TestKey, TestValue>::new();
                
                for i in 0..50 {
                    mst.upsert(&mut repo_table, format!("key_{}", i), format!("val_{}", i)).unwrap();
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