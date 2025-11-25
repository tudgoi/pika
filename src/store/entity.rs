use aykroyd::{FromRow, Query, Statement};

#[derive(FromRow)]
pub struct PropertyRow {
    pub property_schema_name: String,
    pub property_name: String,
    pub value: String,
}

#[derive(Query)]
#[aykroyd(
    row(PropertyRow),
    text = "
    SELECT property_schema_name, property_name, value FROM entity_property WHERE entity_schema_name = $1 AND entity_id = $2
"
)]
pub struct PropertyForEntityQuery<'a> {
    #[aykroyd(param = "$1")]
    pub schema: &'a str,

    #[aykroyd(param = "$2")]
    pub id: &'a str,
}

#[derive(FromRow)]
pub struct PropertyForSchemaRow {
    pub property_name: String,
    pub value: String,
}

#[derive(Query)]
#[aykroyd(
    row(PropertyForSchemaRow),
    text = "
    SELECT property_name, value FROM entity_property WHERE entity_schema_name = $1 AND entity_id = $2 AND property_schema_name = $3
"
)]
pub struct PropertyForEntitySchemaQuery<'a> {
    #[aykroyd(param = "$1")]
    pub schema: &'a str,

    #[aykroyd(param = "$2")]
    pub id: &'a str,

    #[aykroyd(param = "$3")]
    pub property_schema: &'a str,
}

#[derive(Statement)]
#[aykroyd(text = "
    DELETE FROM entity_property WHERE entity_schema_name = $1 AND entity_id = $2 AND property_schema_name = $3
")]
pub struct PropertyForEntitySchemaDelete<'a> {
    #[aykroyd(param = "$1")]
    pub schema: &'a str,

    #[aykroyd(param = "$2")]
    pub id: &'a str,

    #[aykroyd(param = "$3")]
    pub property_schema: &'a str,
}

#[derive(Statement)]
#[aykroyd(text = "
    INSERT INTO entity_property (entity_schema_name, entity_id, property_schema_name, property_name, value) VALUES (?1, ?2, ?3, ?4, ?5)
")]
pub struct PropertyForEntitySchemaInsert<'a> {
    #[aykroyd(param = "$1")]
    pub schema: &'a str,

    #[aykroyd(param = "$2")]
    pub id: &'a str,

    #[aykroyd(param = "$3")]
    pub property_schema: &'a str,

    #[aykroyd(param = "$4")]
    pub name: &'a str,
    
    #[aykroyd(param = "$5")]
    pub value: &'a str,
}

#[derive(Statement)]
#[aykroyd(text = "INSERT INTO entity (schema_name, id) VALUES ($1, $2)")]
pub struct InsertEntityStatement<'a> {
    #[aykroyd(param = "$1")]
    pub schema_name: &'a str,
    #[aykroyd(param = "$2")]
    pub id: &'a str,
}
