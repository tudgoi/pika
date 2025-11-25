use crate::{
    mapper, parsedir,
    store::entity::{InsertEntityStatement, PropertyForEntitySchemaInsert},
};
use anyhow::{Context, Result};
use aykroyd::rusqlite::Client;
use jaq_json::Val;
use mapper::Mapper;
use std::path::{Path, PathBuf};

pub fn run(db_path: &Path, data_path: PathBuf, mapping_path: PathBuf) -> Result<()> {
    let mut db = Client::open(db_path)?;

    for result in parsedir::parse(&mapping_path, |s| toml::from_str(s))? {
        let (schema_name, mapping) = result?;

        let mapper = Mapper::new(mapping)
            .with_context(|| format!("could not create mapper for schema {}", schema_name))?;

        // iterate over data for each schema
        for result in parsedir::parse(&data_path.join(&schema_name), |s| jaq_json::toml::parse(s))?
        {
            let (id, data): (String, Val) = result?;
            db.execute(&InsertEntityStatement {
                schema_name: &schema_name,
                id: &id,
            })
            .with_context(|| format!("could not insert schema {}", schema_name))?;

            for result in mapper.run(data) {
                let property = result.with_context(|| {
                    format!(
                        "could not run mapper for schema {} and id {}",
                        schema_name, id
                    )
                })?;
                let property_value = match &property.value {
                    Val::Str(s, _) => String::from_utf8(s.to_vec())
                        .context("Invalid UTF-8 string in property value")?,
                    _ => property.value.to_string(),
                };
                db.execute(&PropertyForEntitySchemaInsert {
                    schema: &schema_name,
                    id: &id,
                    property_schema: &property.schema,
                    name: &property.name,
                    value: &property_value,
                })?;
            }
        }
    }

    Ok(())
}
