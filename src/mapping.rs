use std::collections::HashMap;

use serde::Deserialize;

#[derive(Deserialize)]
pub struct Mapping {
    properties: HashMap<String, HashMap<String, String>>,
}
