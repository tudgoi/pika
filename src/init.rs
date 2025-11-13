use crate::schema::Schema;
use anyhow::{Context, Result};
use rusqlite::Connection;
use std::{fs, path::PathBuf};

const SCHEMA_SQL: &str = include_str!("schema.sql");

pub fn run(db_path: PathBuf, schema_path: PathBuf) -> Result<()> {
    let str = fs::read_to_string(&schema_path)
        .with_context(|| format!("could not read schema file: {:?}", schema_path))?;
    let schema: Schema = toml::from_str(&str)
        .with_context(|| format!("could not parse schema file: {:?}", schema_path))?;

    let conn = Connection::open(db_path)?;
    conn.execute_batch(SCHEMA_SQL)
        .with_context(|| format!("could not create tables"))?;

    conn.execute(
        "INSERT INTO schema (name, abstract) VALUES (?1, ?2)",
        ("test", schema.abstrct)
    ).with_context(|| format!("could not insert schema"))?;
    
    Ok(())
}
