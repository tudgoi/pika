use crate::{
    schema::{self, Schema},
    tomldir,
};
use anyhow::{Context, Result};
use rusqlite::{Connection, ToSql};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};
use topological_sort::TopologicalSort;

const SCHEMA_SQL: &str = include_str!("schema.sql");

pub fn run(db_path: &Path, schema_path: PathBuf) -> Result<()> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("could not open database: {:?}", db_path))?;

    // setup our tables
    conn.execute_batch(SCHEMA_SQL)
        .with_context(|| "could not create tables")?;

    let mut schemas = HashMap::new();
    let mut ts = TopologicalSort::<String>::new();
    for result in tomldir::parse::<Schema>(&schema_path)? {
        let (schema_name, schema) = result?;
        ts.insert(schema_name.clone());
        if let Some(extends) = &schema.extends {
            for parent in extends {
                ts.add_dependency(parent.clone(), schema_name.clone());
            }
        }
        schemas.insert(schema_name, schema);
    }

    // insert the given schema for the app
    for schema_name in ts {
        let schema = schemas.get(&schema_name).context("could not get schema")?;

        conn.execute(
            "INSERT INTO schema (name, abstract) VALUES (?1, ?2)",
            (&schema_name, schema.abstrct),
        )
        .with_context(|| format!("could not insert schema {}", schema_name))?;

        // insert properties
        if let Some(schema_properties) = &schema.properties {
            for (name, schema_property) in schema_properties {
                conn.execute(
                    "INSERT INTO schema_property VALUES(?1, ?2, ?3)",
                    (&schema_name, name, &schema_property.typ),
                )
                .with_context(|| {
                    format!(
                        "could not insert property:{} for schema:{}",
                        name, schema_name
                    )
                })?;
            }
        }

        // insert extends
        if let Some(schema_extends) = &schema.extends {
            for name in schema_extends {
                conn.execute(
                    "INSERT INTO schema_extend VALUES(?1, ?2)",
                    (&schema_name, name),
                )
                .with_context(|| {
                    format!(
                        "could not insert extends {} for schema {}",
                        name, schema_name
                    )
                })?;
            }
        }
    }

    Ok(())
}

impl ToSql for schema::Type {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            schema::Type::Name => Ok("name".into()),
        }
    }
}
