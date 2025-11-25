use std::path::PathBuf;

use anyhow::{Context, Result};
use aykroyd::rusqlite::Client;
use pika::{import, init, store::entity::PropertyForEntitySchemaQuery};
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

    let mut db = Client::open(&db_path)?;
    let properties = db.query(&PropertyForEntitySchemaQuery {
        schema: "person",
        id: "pikachu",
        property_schema: "thing",
    })?;
    for property in properties {
        assert_eq!(property.property_name, "name");
        assert_eq!(property.value, "Pikachu");
    }

    Ok(())
}
