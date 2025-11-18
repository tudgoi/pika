mod entity;

use anyhow::{Context, Result};
use axum::{
    Router,
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, put},
};
use include_dir::{Dir, include_dir};
use std::{path::PathBuf, sync::Arc};
use tera::Tera;

static TEMPLATES_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/templates");

struct AppState {
    db_path: PathBuf,
}

struct AppError(anyhow::Error);

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
        .route("/", get(root))
        .route("/entity/{schema}/{id}/edit", get(entity::entity_edit))
        .route(
            "/entity/{schema}/{id}/{property_schema}",
            get(entity::properties_view),
        )
        .route(
            "/entity/{schema}/{id}/{property_schema}",
            put(entity::properties_save),
        )
        .route(
            "/entity/{entity_schema}/{id}/{schema}/edit",
            get(entity::properties_edit),
        )
        .with_state(Arc::new(state));
    let addr = format!("0.0.0.0:{}", 8080);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("could not listen on {}", addr))?;

    println!("Serving at http://{}/", addr);
    axum::serve(listener, app)
        .await
        .with_context(|| "could not start server")?;

    Ok(())
}

fn template_new() -> Result<Tera> {
    let mut tera = Tera::default();
    // Iterate over the files in the embedded directory.
    let glob = "**/*.html";
    for direntry in TEMPLATES_DIR.find(glob)? {
        if let Some(file) = direntry.as_file() {
            println!("Adding {:?}", file.path());
            let path = file
                .path()
                .to_str()
                .with_context(|| format!("Path is not valid UTF-8: {:?}", file.path()))?;
            let content = file
                .contents_utf8()
                .with_context(|| format!("Template file is not valid UTF-8: {}", path))?;

            // Add the template to Tera. We use the file's path as the template name.
            tera.add_raw_template(path, content)
                .with_context(|| format!("Error loading template {}", path))?;
        }
    }

    Ok(tera)
}

#[axum::debug_handler]
async fn root() -> Result<Html<String>, AppError> {
    let tera = template_new()?;
    let context = tera::Context::new();
    let body = tera.render("index.html", &context)?;

    Ok(Html(body))
}
