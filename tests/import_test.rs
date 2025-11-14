use std::path::PathBuf;

use pika::{import, init};
use tempdir::TempDir;
use anyhow::{Context, Result};

#[test]
fn test_sample_import() -> Result<()> {
    // Get the path to the test schema file.

    let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let schema_path = manifest_path.join("tests/schema");
    let mapping_path = manifest_path.join("tests/mapping");
    let data_path = manifest_path.join("tests/data");

    let tempdir = TempDir::new("pika-tests")
        .with_context(|| format!("could not create tempdir"))?;

    let db_path = tempdir.path().join("sample_import.db");

    // Call the run function.
    let result = init::run(&db_path, schema_path);
    result.expect("could not init db");

    let result = import::run(&db_path, data_path, mapping_path);
    result.expect("could not import data");

    Ok(())
}