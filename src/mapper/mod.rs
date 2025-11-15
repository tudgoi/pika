mod mapping;

use jaq_core::{
    Ctx, Filter,
    data,
    load::{Arena, File, Loader},
};
use jaq_json::Val;


use mapping::Mapping;

#[derive(thiserror::Error, Debug)]
pub enum MapperError {
    #[error("could not load jaq program: {0}")]
    JaqLoadError(String),
    #[error("could not compile jaq filter: {0}")]
    JaqCompileError(String),
    #[error("jaq filter execution error: {0}")]
    JaqRunError(String),
}

pub struct PropertyFilter {
    pub schema: String,
    pub name: String,
    pub filter: Filter<data::JustLut<Val>>,
}

pub struct Mapper {
    property_filters: Vec<PropertyFilter>,
}

impl Mapper {
    pub fn new(mapping: Mapping) -> Result<Self, MapperError> {
        let mut property_filters = Vec::new();
        let arena = Arena::default();

        for (schema_name, properties_map) in mapping.properties {
            for (property_name, filter_string) in properties_map {
                let program = File {
                    code: filter_string.as_str(),
                    path: (),
                };
                let loader = Loader::new([]); // Correctly placed inside the loop
                let modules = loader.load(&arena, program)
                    .map_err(|e| MapperError::JaqLoadError(format!("{:?}", e)))?;
                let filter = jaq_core::Compiler::default().compile(modules)
                    .map_err(|e| MapperError::JaqCompileError(format!("{:?}", e)))?;

                property_filters.push(PropertyFilter {
                    schema: schema_name.clone(),
                    name: property_name,
                    filter,
                });
            }
        }

        Ok(Self { property_filters })
    }

    pub fn run<'a>(&'a self, val: Val) -> impl Iterator<Item = Result<Property, MapperError>> + 'a {
        self.property_filters.iter().flat_map(move |pf| {
            let ctx = Ctx::<data::JustLut<Val>>::new(&pf.filter.lut, jaq_core::Vars::new([]));
            pf.filter.id.run((ctx, val.clone())).map(move |r| {
                r.map(|value| Property {
                    schema: pf.schema.clone(),
                    name: pf.name.clone(),
                    value,
                })
                .map_err(|e| MapperError::JaqRunError(format!("{:?}", e)))
            })
        })
    }
}

pub struct Property {
    pub schema: String,
    pub name: String,
    pub value: Val,
}