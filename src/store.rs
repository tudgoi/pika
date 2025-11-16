use std::{collections::HashMap, path::Path};

use rusqlite::Connection;

pub struct Store {
    conn: Connection,
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
}
