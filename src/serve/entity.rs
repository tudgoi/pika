pub(crate) use anyhow::Result;
use axum::{extract, response::Html};
use std::{collections::HashMap, sync::Arc};

use crate::{
    serve::{AppError, AppState, template_new},
    store::entity::{PropertyForEntityQuery, PropertyForEntitySchemaDelete, PropertyForEntitySchemaInsert, PropertyForEntitySchemaQuery, PropertyRow, PropertyForSchemaRow},
};

#[axum::debug_handler]
pub async fn edit(
    extract::State(state): extract::State<Arc<AppState>>,
    extract::Path((schema, id)): extract::Path<(String, String)>,
) -> Result<Html<String>, AppError> {
    let properties_vec: Vec<PropertyRow> =
        state.db()?.query(&PropertyForEntityQuery { schema: &schema, id: &id })?;
    let mut properties: HashMap<String, HashMap<String, String>> = HashMap::new();
    for row in properties_vec {
        properties
            .entry(row.property_schema_name)
            .or_default()
            .insert(row.property_name, row.value);
    }

    let tera = template_new()?;
    let mut context = tera::Context::new();
    context.insert("schema", &schema);
    context.insert("id", &id);
    context.insert("properties", &properties);
    let body = tera.render("entity/edit.html", &context)?;

    Ok(Html(body))
}

#[axum::debug_handler]
pub async fn properties_edit_partial(
    extract::State(state): extract::State<Arc<AppState>>,
    extract::Path((schema, id, property_schema)): extract::Path<(String, String, String)>,
) -> Result<Html<String>, AppError> {
    let properties_vec: Vec<PropertyForSchemaRow> = state
        .db()?
        .query(&PropertyForEntitySchemaQuery {
            schema: &schema,
            id: &id,
            property_schema: &property_schema,
        })?;
    let mut properties: HashMap<String, String> = HashMap::new();
    for row in properties_vec {
        properties.insert(row.property_name, row.value);
    }

    let tera = template_new()?;
    let mut context = tera::Context::new();
    context.insert("schema", &schema);
    context.insert("id", &id);
    context.insert("property_schema", &property_schema);
    context.insert("properties", &properties);
    let body = tera.render("entity/properties_edit_partial.html", &context)?;

    Ok(Html(body))
}

#[axum::debug_handler]
pub async fn properties_view_partial(
    extract::State(state): extract::State<Arc<AppState>>,
    extract::Path((schema, id, property_schema)): extract::Path<(String, String, String)>,
) -> Result<Html<String>, AppError> {
    let properties_vec: Vec<PropertyForSchemaRow> = state.db()?.query(&PropertyForEntitySchemaQuery {
        schema: &schema,
        id: &id,
        property_schema: &property_schema,
    })?;
    let mut properties: HashMap<String, String> = HashMap::new();
    for row in properties_vec {
        properties.insert(row.property_name, row.value);
    }

    let tera = template_new()?;
    let mut context = tera::Context::new();
    context.insert("schema", &schema);
    context.insert("id", &id);
    context.insert("property_schema", &property_schema);
    context.insert("properties", &properties);
    let body = tera.render("entity/properties_view_partial.html", &context)?;

    Ok(Html(body))
}

pub async fn properties_save_partial(
    extract::State(state): extract::State<Arc<AppState>>,
    extract::Path((schema, id, property_schema)): extract::Path<(String, String, String)>,
    extract::Form(properties_form): extract::Form<HashMap<String, String>>,
) -> Result<Html<String>, AppError> {
    let mut db = state.db()?;
    let mut txn = db.transaction()?;
    txn.execute(&PropertyForEntitySchemaDelete { schema: &schema, id: &id, property_schema: &property_schema })?;
    for (name, value) in properties_form {
        txn.execute(&PropertyForEntitySchemaInsert { schema: &schema, id: &id, property_schema: &property_schema, name: &name, value: &value })?;
    }
    txn.commit()?;

    let properties_vec: Vec<PropertyForSchemaRow> = db.query(&PropertyForEntitySchemaQuery { schema: &schema, id: &id, property_schema: &property_schema })?;
    let mut properties: HashMap<String, String> = HashMap::new();
    for row in properties_vec {
        properties.insert(row.property_name, row.value);
    }

    let tera = template_new()?;
    let mut context = tera::Context::new();
    context.insert("schema", &schema);
    context.insert("id", &id);
    context.insert("property_schema", &property_schema);
    context.insert("properties", &properties);
    let body = tera.render("entity/properties_view_partial.html", &context)?;

    Ok(Html(body))
}
