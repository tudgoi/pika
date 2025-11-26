use std::sync::Arc;

use axum::{extract, response::Html};
use serde::Deserialize;

use crate::{serve::{AppError, AppState, template_new}, store::document::SearchDocuments};

#[axum::debug_handler]
pub async fn search_form() -> Result<Html<String>, AppError> {
    let tera = template_new()?;
    let context = tera::Context::new();
    let body = tera.render("document/search.html", &context)?;

    Ok(Html(body))
}

#[derive(Deserialize)]
pub struct Query {
    search: String,
}

#[axum::debug_handler]
pub async fn search(
    extract::State(state): extract::State<Arc<AppState>>,
    extract::Form(query): extract::Form<Query>,
) -> Result<Html<String>, AppError> {
    let documents = if query.search.trim().len() > 0 {
        state.db()?.query(&SearchDocuments(&query.search))?
    } else {
        Vec::new()
    };
    
    let tera = template_new()?;
    let mut context = tera::Context::new();
    context.insert("documents", &documents);
    let body = tera.render("document/search_result_partial.html", &context)?;

    Ok(Html(body))
}