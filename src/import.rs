use std::{fs, path::{Path, PathBuf}};
use anyhow::{Context, Result};
use toml;
use crate::mapping;

pub fn run(_db_path: &Path, data_path: PathBuf, mapping_path: PathBuf) -> Result<()> {
    let files = mapping_path
        .read_dir()
        .with_context(|| format!("could not read mapping dir: {:?}", mapping_path))?;
    for entry in files {
        let file = entry
            .with_context(|| format!("could not get file entry in {:?}", mapping_path))?
            .path();
        let schema_name = file
            .file_stem()
            .with_context(|| format!("could not extract file stem from {:?}", file))?
            .to_str()
            .with_context(|| format!("could not convert file stem to string {:?}", file))?
            .to_string();
        let str = fs::read_to_string(&file)
            .with_context(|| format!("could not read mapping file: {:?}", file))?;
        let _mapping: mapping::Mapping = toml::from_str(&str)
            .with_context(|| format!("could not parse schema file: {:?}", file))?;
        
        // read files from dir
        parse_data(&schema_name, data_path.join(&schema_name));
    }
    Ok(())
}

fn parse_data(schema_name: &str, data_path: PathBuf) {
    println!("parsing {} from path: {:?}", schema_name, data_path);
}