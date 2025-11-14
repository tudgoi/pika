use crate::schema::{self, Schema};
use anyhow::{Context, Result};
use rusqlite::{Connection, ToSql};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};
use topological_sort::TopologicalSort;

const SCHEMA_SQL: &str = include_str!("schema.sql");

pub fn run(db_path: &Path, schema_path: PathBuf) -> Result<()> {
    let conn = Connection::open(&db_path)
        .with_context(|| format!("could not open database: {:?}", db_path))?;

    // setup our tables
    conn.execute_batch(SCHEMA_SQL)
        .with_context(|| format!("could not create tables"))?;

    let schemas = parse_schemas(schema_path)?;
    let mut ts = TopologicalSort::<&str>::new();

    for (schema_name, schema) in &schemas {
        if let Some(extends) = &schema.extends {
            for parent in extends {
                ts.add_dependency(parent.as_str(), schema_name.as_str());
            }
        }
    }

    // insert the given schema for the app
    for schema_name in ts {
        let schema = schemas
            .get(schema_name)
            .context("could not get schema")?;

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

fn parse_schemas(schema_path: PathBuf) -> Result<HashMap<String, Schema>> {
    let mut schemas = HashMap::new();
    let files = schema_path
        .read_dir()
        .with_context(|| format!("could not read schema dir: {:?}", schema_path))?;
    for entry in files {
        let file = entry
            .with_context(|| format!("could not get file entry in {:?}", schema_path))?
            .path();
        let schema_name = file
            .file_stem()
            .with_context(|| format!("could not extract file stem from {:?}", file))?
            .to_str()
            .with_context(|| format!("could not convert file stem to string {:?}", file))?
            .to_string();
        let str = fs::read_to_string(&file)
            .with_context(|| format!("could not read schema file: {:?}", schema_path))?;
        let schema: Schema = toml::from_str(&str)
            .with_context(|| format!("could not parse schema file: {:?}", file))?;
        schemas.insert(schema_name, schema);
    }

    Ok(schemas)
}

impl ToSql for schema::Type {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            schema::Type::Name => Ok("name".into()),
        }
    }
}
