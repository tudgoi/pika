use std::path::{Path, PathBuf};
use anyhow::Result;

pub fn run(db_path: &Path, data_path: PathBuf, mapping_path: PathBuf) -> Result<()> {
    println!("Hello import");
    Ok(())
}