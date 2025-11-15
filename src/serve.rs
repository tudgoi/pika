use std::path::Path;
use anyhow::{Context, Result, bail};
use axum::{Router, http::StatusCode, response::{IntoResponse, Response}, routing::get};

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
async fn root() -> Result<String, AppError> {
    Ok(String::from("Hello world!"))
}