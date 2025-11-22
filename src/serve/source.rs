use std::sync::Arc;

use axum::{extract, response::Html};
use reqwest::header;

use crate::{
    chu,
    serve::{AppError, AppState, template_new},
    store::Store,
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
pub async fn crawl(
    extract::State(state): extract::State<Arc<AppState>>,
) -> Result<Html<String>, AppError> {
    let mut store = Store::open(&state.db_path)?;

    let urls = store.get_stale_sources()?;

    for (id, url) in urls {
        let response = reqwest::get(url.clone()).await?;

        let etag = if let Some(etag_value) = response.headers().get(header::ETAG) {
            Some(String::from(etag_value.to_str()?))
        } else {
            None
        };

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

        let document = chu::extract_tables(&body);
        let text = chu::tables_to_string(document.tables);

        store.add_document(id, etag.as_deref(), document.title.as_deref(), &text)?;
    }

    Ok(Html(String::from("")))
}
