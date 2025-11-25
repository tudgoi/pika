use std::sync::Arc;

use axum::{extract, response::Html};
use chrono::Local;
use reqwest::header;
use sha2::{Digest, Sha256};
use tracing::{info, warn};
use anyhow::Context;

use crate::{
    chu,
    serve::{AppError, AppState, template_new},
    store::{
        document::AddDocumentStatement,
        source::{Sources, StaleSources, UpdateCrawlDate},
    },
};

#[axum::debug_handler]
pub async fn list(
    extract::State(state): extract::State<Arc<AppState>>,
) -> Result<Html<String>, AppError> {
    let sources = state.db()?.query(&Sources)?;

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
    let rows = db.query(&StaleSources)?;

    for row in rows {
        let (source_id, url) = (row.id, row.url);
        
        info!("Crawling source: {} - {}", source_id, url);

        let response = reqwest::get(url.clone()).await
            .with_context(|| format!("Failed to fetch URL: {}", url))?;

        let etag = if let Some(etag_value) = response.headers().get(header::ETAG) {
            Some(String::from(etag_value.to_str()
                .with_context(|| format!("Failed to convert ETag header to string for URL: {}", url))?))
        } else {
            None
        };

        // Check if the request was successful (status code 2xx)
        let body = if response.status().is_success() {
            // Get the response body as text
            response.text().await
                .with_context(|| format!("Failed to get response body as text for URL: {}", url))?
        } else {
            warn!("Request failed for {} with status: {}", url, response.status());
            continue; // Skip to the next source
        };

        let document = chu::extract_tables(&body);
        let text = chu::tables_to_string(document.tables);
        let now = &Local::now().to_rfc3339();
        
        let count = db.execute(&UpdateCrawlDate(source_id, now))
            .with_context(|| format!("Failed to update crawl date for source ID: {}", source_id))?;
        
        db.execute(&AddDocumentStatement {
            id: &format!("{:x}", Sha256::digest(body.as_bytes())), // body needs to be bytes for digest
            source_id,
            retrieved_date: now,
            etag: etag.as_deref(),
            title: document.title.as_deref(),
            content: &text,
        }).with_context(|| format!("Failed to add document for source ID: {}", source_id))?;
    }

    let sources = state.db()?.query(&Sources)?;

    let tera = template_new()?;
    let mut context = tera::Context::new();
    context.insert("sources", &sources);
    let body = tera.render("source/list_partial.html", &context)?;

    Ok(Html(body))
}
