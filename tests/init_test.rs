use anyhow::{Context, Result};
use pika::init;
use std::path::PathBuf;
use tempdir::TempDir;

#[test]
fn test_sample_schema() -> Result<()> {
    // Get the path to the test schema file.

    let mut schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    schema_path.push("tests/schema.toml");

    let db_path = TempDir::new("pika-tests")
        .with_context(|| format!("could not create tempdir"))?
        .path()
        .join("sample_schema.db");

    // Call the run function.
    let result = init::run(db_path, schema_path);

    // Assert that the function returns Ok.
    assert!(result.is_ok());

    Ok(())
}
