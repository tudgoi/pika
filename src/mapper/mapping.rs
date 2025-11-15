use std::collections::HashMap;

use serde::Deserialize;

#[derive(Deserialize)]
pub struct Mapping {
    pub properties: HashMap<String, HashMap<String, String>>,
}
