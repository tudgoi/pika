use anyhow::{Context, Result};
use axum::{
    Router, extract,
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::get,
};
use include_dir::{Dir, include_dir};
use std::{path::{PathBuf}, sync::Arc};
use tera::Tera;

use crate::store::Store;

static TEMPLATES_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/templates");

struct AppState {
    db_path: PathBuf,
}

struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
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
    let state = AppState{ db_path };
    let app = Router::new()
        .route("/", get(root))
        .route("/entities/{schema}/{id}", get(view))
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
    for file in TEMPLATES_DIR.files() {
        let path = file.path().to_str().expect("Path is not valid UTF-8");
        let content = file
            .contents_utf8()
            .expect("Template file is not valid UTF-8");

        // Add the template to Tera. We use the file's path as the template name.
        if let Err(e) = tera.add_raw_template(path, content) {
            eprintln!("Error loading template {}: {}", path, e);
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

#[axum::debug_handler]
async fn view(
    extract::State(state): extract::State<Arc<AppState>>,
    extract::Path((schema, id)): extract::Path<(String, String)>,
) -> Result<Html<String>, AppError> {
    let store = Store::open(&state.db_path)?;
    let entity = store.get_entity(&schema, &id)?;

    let tera = template_new()?;
    let mut context = tera::Context::new();
    context.insert("entity", &entity);
    let body = tera.render("view.html", &context)?;

    Ok(Html(body))
}
