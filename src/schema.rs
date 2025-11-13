use std::collections::HashMap;

use serde::Deserialize;

#[derive(Deserialize)]
pub struct Schema {
    #[serde(rename = "abstract")]
    pub abstrct: bool,
    
    pub extends: Option<Vec<String>>,
    pub properties: Option<HashMap<String, SchemaProperty>>,
}

#[derive(Deserialize)]
pub struct SchemaProperty {
    #[serde(rename = "type")]
    pub typ: Type,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Name,
}
