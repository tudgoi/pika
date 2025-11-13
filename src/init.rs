use crate::schema::Schema;
use anyhow::{Context, Result};
use rusqlite::Connection;
use std::{fs, path::PathBuf};

const SCHEMA_SQL: &str = include_str!("schema.sql");

pub fn run(db_path: PathBuf, schema_path: PathBuf) -> Result<()> {
    let conn = Connection::open(db_path)?;
    conn.execute_batch(SCHEMA_SQL)
        .with_context(|| format!("could not create tables"))?;

    let files = schema_path
        .read_dir()
        .with_context(|| format!("could not read schema dir: {:?}", schema_path))?;
    for entry in files {
        let file =
            entry.with_context(|| format!("could not get file entry in {:?}", schema_path))?
            .path();
        let name = file.file_stem()
            .with_context(|| format!("could not extract file stem from {:?}", file))?
            .to_str()
            .with_context(|| format!("could not convert file stem to string {:?}", file))?
            .to_string();
        let str = fs::read_to_string(file)
            .with_context(|| format!("could not read schema file: {:?}", schema_path))?;
        let schema: Schema = toml::from_str(&str)
            .with_context(|| format!("could not parse schema file: {:?}", schema_path))?;

        conn.execute(
            "INSERT INTO schema (name, abstract) VALUES (?1, ?2)",
            (name, schema.abstrct),
        )
        .with_context(|| format!("could not insert schema"))?;
    }

    Ok(())
}
