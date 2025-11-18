use std::sync::Arc;

use axum::{extract, response::Html};

use crate::{serve::{AppError, AppState, template_new}, store::Store};


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