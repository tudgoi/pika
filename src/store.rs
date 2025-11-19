use std::{collections::HashMap, path::Path};

use rusqlite::Connection;
use serde::Serialize;

pub struct Store {
    pub conn: Connection,
}

#[derive(thiserror::Error, Debug)]
pub enum StoreError {
    #[error("Rusqlite error: {0}")]
    RusqliteError(#[from] rusqlite::Error),
}

impl Store {
    pub fn open(db_path: &Path) -> Result<Self, StoreError> {
        let conn = Connection::open(db_path)?;

        Ok(Store { conn })
    }

    pub fn get_all_properties(
        &self,
        schema: &str,
        id: &str,
    ) -> Result<HashMap<String, HashMap<String, String>>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT property_schema_name, property_name, value FROM entity_property WHERE entity_schema_name = ?1 AND entity_id = ?2",
        )?;

        let rows = stmt.query_map(rusqlite::params![schema, id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;

        let mut properties: HashMap<String, HashMap<String, String>> = HashMap::new();
        for row_result in rows {
            let (property_schema_name, property_name, value) = row_result?;
            properties
                .entry(property_schema_name)
                .or_default()
                .insert(property_name, value);
        }

        Ok(properties)
    }

    pub fn get_properties(
        &self,
        entity_schema: &str,
        id: &str,
        schema: &str,
    ) -> Result<HashMap<String, String>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT property_name, value FROM entity_property WHERE entity_schema_name = ?1 AND entity_id = ?2 AND property_schema_name = ?3",
        )?;

        let rows = stmt.query_map(rusqlite::params![entity_schema, id, schema], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        let mut properties: HashMap<String, String> = HashMap::new();
        for row_result in rows {
            let (property_name, value) = row_result?;
            properties.insert(property_name, value.to_string());
        }

        Ok(properties)
    }

    pub fn put_properties(
        &mut self,
        entity_schema: &str,
        id: &str,
        property_schema: &str,
        properties: HashMap<String, String>,
    ) -> Result<(), StoreError> {
        let tx = self.conn.transaction()?;

        tx.execute(
            "DELETE FROM entity_property WHERE entity_schema_name = ?1 AND entity_id = ?2 AND property_schema_name = ?3",
            rusqlite::params![entity_schema, id, property_schema],
        )?;

        for (property_name, value) in properties {
            tx.execute(
                "INSERT INTO entity_property (entity_schema_name, entity_id, property_schema_name, property_name, value) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![entity_schema, id, property_schema, property_name, value],
            )?;
        }

        tx.commit()?;

        Ok(())
    }
    
    pub fn add_document(&mut self, source_id: i64, document: &Document) -> Result<String, StoreError> {
        use sha2::{Digest, Sha256};

        let id = format!("{:x}", Sha256::digest(&document.content));

        self.conn.execute(
            "INSERT OR IGNORE INTO document (id, source_id, retrieved_date, etag, title, content) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                id,
                source_id,
                document.retrieved,
                document.etag,
                document.title,
                document.content
            ],
        )?;

        Ok(id)
    }
    
    pub fn get_documents(&self, source_id: i64) -> Result<HashMap<String, Document>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, retrieved_date, etag, title, content FROM document WHERE source_id = ?1",
        )?;
        
        let rows = stmt.query_map(rusqlite::params![source_id], |row| {
            Ok((row.get::<_, String>(0)?, 
            Document {
                retrieved: row.get::<_, String>(1)?,
                etag: row.get::<_, Option<String>>(2)?,
                title: row.get::<_, String>(3)?,
                content: row.get::<_, String>(4)?,
            }))
        })?;
        
        let mut documents: HashMap<String, Document> = HashMap::new();
        for row_result in rows {
            let (id, document) = row_result?;
            documents.insert(id, document);
        }
        
        Ok(documents)
    }

    pub fn get_source_url(&self, id: i64) -> Result<String, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT url FROM source WHERE id = ?1",
        )?;
        
        let url = stmt.query_one(rusqlite::params![id], |row| {
            Ok(row.get::<_, String>(0)?)
        })?;
        
        Ok(url)
    }
    
    pub fn get_sources(&self) -> Result<HashMap<i64, SourceDocuments>, StoreError> {
        let mut stmt = self.conn.prepare(
            "SELECT id, url FROM source",
        )?;

        let rows = stmt.query_map(rusqlite::params![], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;

        let mut sources: HashMap<i64, SourceDocuments> = HashMap::new();
        for row_result in rows {
            let (id, url) = row_result?;
            let documents = self.get_documents(id)?;
            sources.insert(id, SourceDocuments {
                url,
                documents,
            });
        }

        Ok(sources)
    }
}

#[derive(Serialize)]
pub struct SourceDocuments {
    url: String,
    documents: HashMap<String, Document>,
}

#[derive(Serialize)]
pub struct Document {
    pub retrieved: String,
    pub etag: Option<String>,
    pub title: String,
    pub content: String,
}