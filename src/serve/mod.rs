pub mod entity;
pub mod source;
pub mod document;

use anyhow::{Context, Result};
use axum::{
    Router, extract, http::StatusCode, response::{Html, IntoResponse, Response}, routing::{get, post, put}
};
use aykroyd::rusqlite::Client;
use mime_guess::from_path;
use reqwest::header;
use rust_embed::Embed;
use tracing::info;
use std::{path::PathBuf, sync::Arc};
use tera::Tera;

#[derive(Embed)]
#[folder = "$CARGO_MANIFEST_DIR/templates/"]
struct Templates;

#[derive(Embed)]
#[folder = "$CARGO_MANIFEST_DIR/static/"]
struct StaticFiles;

pub struct AppState {
    pub db_path: PathBuf,
}

impl AppState {
    pub fn db(&self) -> Result<Client, AppError> {
        Ok(Client::open(&self.db_path)?)
    }
}

#[derive(Debug)]
pub struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {:?}", self.0),
        )
            .into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

#[tokio::main]
pub async fn run(db_path: PathBuf) -> Result<()> {
    let state = AppState { db_path };
    let app = Router::new()
        .route("/", get(index))
        .route("/entity/{schema}/{id}/edit", get(entity::edit))
        .route(
            "/entity/{schema}/{id}/{property_schema}",
            get(entity::properties_view_partial),
        )
        .route(
            "/entity/{schema}/{id}/{property_schema}",
            put(entity::properties_save_partial),
        )
        .route(
            "/entity/{entity_schema}/{id}/{schema}/edit",
            get(entity::properties_edit_partial),
        )
        .route("/source", get(source::index))
        .route("/source", post(source::add))
        .route("/source/add", get(source::add_form))
        .route("/source/list", get(source::list))
        .route("/source/crawl", post(source::crawl))
        .route("/document/search", get(document::search_form))
        .route("/document/search", post(document::search))
        .route("/static/{*path}", get(static_file))
        .with_state(Arc::new(state));
    let addr = format!("0.0.0.0:{}", 8080);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("could not listen on {}", addr))?;

    info!("Serving at http://{}/", addr);
    axum::serve(listener, app)
        .await
        .with_context(|| "could not start server")?;

    Ok(())
}

fn template_new() -> Result<Tera> {
    let mut templates: Vec<(String, String)> = Vec::new();
    // Iterate over the files in the embedded directory.
    for filename in Templates::iter() {
        if let Some(file) = Templates::get(&filename) {
            let bytes = file.data.as_ref();
            let str = String::from_utf8(bytes.to_vec())?;
            templates.push((String::from(filename), str));
        }
    }

    let mut tera = Tera::default();
    tera.add_raw_templates(templates)
        .with_context(|| format!("Error loading templates"))?;
    Ok(tera)
}

#[axum::debug_handler]
async fn index() -> Result<Html<String>, AppError> {
    let tera = template_new()?;
    let context = tera::Context::new();
    let body = tera.render("index.html", &context)?;

    Ok(Html(body))
}

#[axum::debug_handler]
async fn static_file(uri: extract::Path<String>) -> Response {
    let path = uri.as_str();
    if let Some(content) = StaticFiles::get(path) {
        let mime_type = from_path(path).first_or_octet_stream();
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, mime_type.as_ref())],
            content.data,
        )
            .into_response()
    } else {
        StatusCode::NOT_FOUND.into_response()
    }
}
