use anyhow::Result;
use pika::init;
use std::path::PathBuf;

#[test]
fn test_sample_schema() -> Result<()> {
    // Get the path to the test schema file.
    let mut schema_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    schema_path.push("tests/schema.toml");

    // Call the run function.
    let result = init::run(schema_path);

    // Assert that the function returns Ok.
    assert!(result.is_ok());

    Ok(())
}
