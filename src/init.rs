use crate::{
    parsedir,
    schema::{self, Schema},
};
use anyhow::{Context, Result};
use aykroyd::{Statement, rusqlite::Client};
use rusqlite::{Connection, ToSql};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};
use topological_sort::TopologicalSort;

const SCHEMA_SQL: &str = include_str!("schema.sql");

#[derive(Statement)]
#[aykroyd(text = "INSERT INTO schema (name, abstract) VALUES ($1, $2)")]
pub struct InsertSchemaStatement<'a> {
    #[aykroyd(param = "$1")]
    pub name: &'a str,
    #[aykroyd(param = "$2")]
    pub abstrct: bool, // Note: 'abstract' is a keyword, so I'll use 'abstrct'
}

#[derive(Statement)]
#[aykroyd(text = "INSERT INTO schema_property VALUES($1, $2, $3)")]
pub struct InsertSchemaPropertyStatement<'a> {
    #[aykroyd(param = "$1")]
    pub schema_name: &'a str,
    #[aykroyd(param = "$2")]
    pub property_name: &'a str,
    #[aykroyd(param = "$3")]
    pub property_type: &'a schema::Type,
}

#[derive(Statement)]
#[aykroyd(text = "INSERT INTO schema_extend VALUES($1, $2)")]
pub struct InsertSchemaExtendStatement<'a> {
    #[aykroyd(param = "$1")]
    pub schema_name: &'a str,
    #[aykroyd(param = "$2")]
    pub extends_name: &'a str,
}

pub fn run(db_path: &Path, schema_path: PathBuf) -> Result<()> {
    let connection = Connection::open(db_path)?;
    // setup our tables
    connection
        .execute_batch(SCHEMA_SQL)
        .with_context(|| "could not create tables")?;

    let mut db: Client = connection.into();

    let mut schemas = HashMap::new();
    let mut ts = TopologicalSort::<String>::new();
    for result in parsedir::parse(&schema_path, |s| toml::from_str(s))? {
        let (schema_name, schema): (String, Schema) = result?;
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

        db.execute(&InsertSchemaStatement {
            name: &schema_name,
            abstrct: schema.abstrct,
        })
        .with_context(|| format!("could not insert schema {}", schema_name))?;

        // insert properties
        if let Some(schema_properties) = &schema.properties {
            for (name, schema_property) in schema_properties {
                db.execute(&InsertSchemaPropertyStatement {
                    schema_name: &schema_name,
                    property_name: name,
                    property_type: &schema_property.typ,
                })
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
                db.execute(&InsertSchemaExtendStatement {
                    schema_name: &schema_name,
                    extends_name: name,
                })
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
