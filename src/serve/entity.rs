pub(crate) use anyhow::Result;
use axum::{
    extract,
    response::Html,
};
use std::{collections::HashMap, sync::Arc};

use crate::{serve::{AppError, AppState, template_new}, store::Store};

#[axum::debug_handler]
pub async fn entity_edit(
    extract::State(state): extract::State<Arc<AppState>>,
    extract::Path((schema, id)): extract::Path<(String, String)>,
) -> Result<Html<String>, AppError> {
    let store = Store::open(&state.db_path)?;
    let properties = store.get_all_properties(&schema, &id)?;

    let tera = template_new()?;
    let mut context = tera::Context::new();
    context.insert("schema", &schema);
    context.insert("id", &id);
    context.insert("properties", &properties);
    let body = tera.render("entity/edit.html", &context)?;

    Ok(Html(body))
}

#[axum::debug_handler]
pub async fn properties_edit(
    extract::State(state): extract::State<Arc<AppState>>,
    extract::Path((schema, id, property_schema)): extract::Path<(String, String, String)>,
) -> Result<Html<String>, AppError> {
    let store = Store::open(&state.db_path)?;
    let properties = store.get_properties(&schema, &id, &property_schema)?;

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
pub async fn properties_view(
    extract::State(state): extract::State<Arc<AppState>>,
    extract::Path((schema, id, property_schema)): extract::Path<(String, String, String)>,
) -> Result<Html<String>, AppError> {
    let store = Store::open(&state.db_path)?;
    let properties = store.get_properties(&schema, &id, &property_schema)?;

    let tera = template_new()?;
    let mut context = tera::Context::new();
    context.insert("schema", &schema);
    context.insert("id", &id);
    context.insert("property_schema", &property_schema);
    context.insert("properties", &properties);
    let body = tera.render("entity/properties_view_partial.html", &context)?;

    Ok(Html(body))
}

#[axum::debug_handler]
pub async fn properties_save(
    extract::State(state): extract::State<Arc<AppState>>,
    extract::Path((schema, id, property_schema)): extract::Path<(String, String, String)>,
    extract::Form(properties): extract::Form<HashMap<String, String>>,
) -> Result<Html<String>, AppError> {
    let mut store = Store::open(&state.db_path)?;
    store.put_properties(&schema, &id, &property_schema, properties)?;
    let properties = store.get_properties(&schema, &id, &property_schema)?;

    let tera = template_new()?;
    let mut context = tera::Context::new();
    context.insert("schema", &schema);
    context.insert("id", &id);
    context.insert("property_schema", &property_schema);
    context.insert("properties", &properties);
    let body = tera.render("entity/properties_view_partial.html", &context)?;

    Ok(Html(body))
}
