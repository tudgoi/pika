use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Schema {
    #[serde(rename = "abstract")]
    pub abstrct: bool,
    
    pub extends: Option<Vec<String>>,
    pub properties: Option<HashMap<String, SchemaProperty>>,
}

#[derive(Deserialize, Serialize)]
pub struct SchemaProperty {
    #[serde(rename = "type")]
    pub typ: Type,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Name,
}
