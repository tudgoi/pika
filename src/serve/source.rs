use std::sync::Arc;

use axum::{extract, response::Html};
use chrono::Local;
use reqwest::header;
use sha2::{Digest, Sha256};

use crate::{
    chu,
    serve::{AppError, AppState, template_new},
    store::{
        document::AddDocumentStatement,
        source::{GetSourcesQuery, GetStaleSourcesQuery},
    },
};

#[axum::debug_handler]
pub async fn list(
    extract::State(state): extract::State<Arc<AppState>>,
) -> Result<Html<String>, AppError> {
    let sources = state.db()?.query(&GetSourcesQuery)?;

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
    let mut db = state.db()?;
    let rows = db.query(&GetStaleSourcesQuery)?;

    for row in rows {
        let (source_id, url) = (row.id, row.url);
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

        db.execute(&AddDocumentStatement {
            id: &format!("{:x}", Sha256::digest(body)),
            source_id,
            retrieved_date: &Local::now().to_rfc3339(),
            etag: etag.as_deref(),
            title: document.title.as_deref(),
            content: &text,
        })?;
    }

    Ok(Html(String::from("")))
}
