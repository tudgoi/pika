use std::{collections::HashMap, path::Path, collections::HashSet};

use rusqlite::Connection;
use serde::Serialize;

use crate::schema::{Schema, SchemaProperty, Type};

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

    pub fn get_entity(&self, schema: &str, id: &str) -> Result<Entity, StoreError> {
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

        let mut properties: HashMap<String, HashMap<String, Value>> = HashMap::new();
        for row_result in rows {
            let (property_schema_name, property_name, value) = row_result?;
            properties
                .entry(property_schema_name)
                .or_default()
                .insert(property_name, Value::Name(value));
        }

        Ok(Entity {
            schema: schema.to_string(),
            id: id.to_string(),
            properties,
        })
    }
}

#[derive(Serialize)]
pub struct Entity {
    pub schema: String,
    pub id: String,
    pub properties: HashMap<String, HashMap<String, Value>>,
}

#[derive(Serialize)]
pub enum Value {
    Name(String),
}