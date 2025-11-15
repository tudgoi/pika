use crate::{mapper, parsedir};
use mapper::Mapper;
use anyhow::{Context, Result};
use jaq_json::Val;
use rusqlite::Connection;
use std::path::{Path, PathBuf};

pub fn run(db_path: &Path, data_path: PathBuf, mapping_path: PathBuf) -> Result<()> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("could not open database: {:?}", db_path))?;

    for result in parsedir::parse(&mapping_path, |s| toml::from_str(s))? {
        let (schema_name, mapping) = result?;

        let mapper = Mapper::new(mapping)
            .with_context(|| format!("could not create mapper for schema {}", schema_name))?;

        // iterate over data for each schema
        for result in parsedir::parse(&data_path.join(&schema_name), |s| jaq_json::toml::parse(s))? {
            let (id, data): (String, Val) = result?;
            conn.execute(
                "INSERT INTO entity (schema_name, id) VALUES (?1, ?2)",
                (&schema_name, &id),
            )
            .with_context(|| format!("could not insert schema {}", schema_name))?;

            for result in mapper.run(data) {
                let property = result
                    .with_context(|| format!("could not run mapper for schema {} and id {}", schema_name, id))?;
                println!("{} {} {} {} {}", &schema_name, &id, property.schema, property.name, property.value);
                conn.execute("
                    INSERT INTO entity_property
                        (entity_schema_name, entity_id, property_schema_name, property_name, value) VALUES
                        (?1, ?2, ?3, ?4, ?5)
                    ", (&schema_name, &id, &property.schema, &property.name, &property.value.to_string()))?;
            }
        }
    }

    Ok(())
}
