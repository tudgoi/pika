use serde::Deserialize;

#[derive(Deserialize)]
pub struct Schema {
    #[serde(rename = "abstract")]
    pub abstrct: bool,
}
