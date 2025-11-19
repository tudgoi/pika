use std::sync::Arc;

use axum::{extract, response::Html};
use chrono::Local;

use crate::{
    serve::{AppError, AppState, template_new},
    store::{Document, Store},
};

#[axum::debug_handler]
pub async fn list(
    extract::State(state): extract::State<Arc<AppState>>,
) -> Result<Html<String>, AppError> {
    let store = Store::open(&state.db_path)?;
    let sources = store.get_sources()?;

    let tera = template_new()?;
    let mut context = tera::Context::new();
    context.insert("sources", &sources);
    let body = tera.render("source/list.html", &context)?;

    Ok(Html(body))
}

#[axum::debug_handler]
pub async fn fetch(
    extract::State(state): extract::State<Arc<AppState>>,
    extract::Path(id): extract::Path<i64>,
) -> Result<Html<String>, AppError> {
    let mut store = Store::open(&state.db_path)?;
    let url = store.get_source_url(id)?;

    let response = reqwest::get(url.clone()).await?;

    // Check if the request was successful (status code 2xx)
    let body = if response.status().is_success() {
        // Get the response body as text
        response.text().await?
    } else {
        return Err(AppError(anyhow::anyhow!(
            "Request failed with status: {}",
            response.status()
        )));
    };

    store.add_document(
        id,
        &Document {
            retrieved: Local::now().to_rfc3339(),
            etag: None,
            title: "".to_string(),
            content: body,
        },
    )?;

    let documents = store.get_documents(id)?;

    let tera = template_new()?;
    let mut context = tera::Context::new();
    context.insert("documents", &documents);
    let body = tera.render("source/list_partial.html", &context)?;

    Ok(Html(body))
}
