use std::path::PathBuf;

use anyhow::{Context, Result};
use pika::{import, init, store::Store};
use std::collections::HashMap;
use tempdir::TempDir;

#[test]
fn test_sample_data() -> Result<()> {
    // Get the path to the test schema file.

    let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let schema_path = manifest_path.join("tests/schema");
    let mapping_path = manifest_path.join("tests/mapping");
    let data_path = manifest_path.join("tests/data");

    let tempdir =
        TempDir::new("pika-tests").with_context(|| format!("could not create tempdir"))?;

    let db_path = tempdir.path().join("sample_import.db");

    init::run(&db_path, schema_path).expect("could not init db");
    import::run(&db_path, data_path, mapping_path).expect("could not import data");

    let store = Store::open(&db_path)?;
    let properties = store.get_properties("person", "pikachu", "thing")?;

    let mut expected_properties = HashMap::new();
    expected_properties.insert("name".to_string(), "Pikachu".to_string());

    assert_eq!(properties, expected_properties);

    Ok(())
}
