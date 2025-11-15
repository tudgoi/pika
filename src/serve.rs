use std::path::Path;
use anyhow::{Context, Result, bail};
use axum::{Router, http::StatusCode, response::{Html, IntoResponse, Response}, routing::get};
use include_dir::{Dir, include_dir};
use tera::Tera;

static TEMPLATES_DIR: Dir = include_dir!("$CARGO_MANIFEST_DIR/templates");

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
pub async fn run(_db_path: &Path) -> Result<()> {
    let app = Router::new()
        .route("/", get(root));
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

#[axum::debug_handler]
async fn root() -> Result<Html<String>, AppError> {
    let mut tera = Tera::default();
    // Iterate over the files in the embedded directory.
    for file in TEMPLATES_DIR.files() {
        let path = file.path().to_str().expect("Path is not valid UTF-8");
        let content = file.contents_utf8().expect("Template file is not valid UTF-8");
        
        // Add the template to Tera. We use the file's path as the template name.
        if let Err(e) = tera.add_raw_template(path, content) {
            eprintln!("Error loading template {}: {}", path, e);
        }
    }
    let context = tera::Context::new();
    let body = tera.render("index.html", &context)?;

    Ok(Html(body))
}