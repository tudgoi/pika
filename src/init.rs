use crate::schema::Schema;
use anyhow::{Context, Result};
use std::{fs, path::PathBuf};

pub fn run(schema_path: PathBuf) -> Result<()> {
    let str = fs::read_to_string(&schema_path)
        .with_context(|| format!("could not read schema file: {:?}", schema_path))?;
    let schema: Schema = toml::from_str(&str)
        .with_context(|| format!("could not parse schema file: {:?}", schema_path))?;

    Ok(())
}
