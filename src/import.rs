use crate::{mapping::Mapping, parsedir};
use anyhow::Result;
use std::path::{Path, PathBuf};

pub fn run(_db_path: &Path, _data_path: PathBuf, mapping_path: PathBuf) -> Result<()> {
    for result in parsedir::parse(&mapping_path, |s| toml::from_str(s))? {
        let (schema_name, _mapping): (String, Mapping) = result?;
        println!("mapping for {}", schema_name);
    }

    Ok(())
}
