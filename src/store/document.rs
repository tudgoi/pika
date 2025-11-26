use aykroyd::{FromRow, Query, QueryOne, Statement};
use serde::Serialize;

#[derive(Statement)]
#[aykroyd(text = "
    INSERT OR IGNORE INTO document (source_id, hash, retrieved_date, etag, title, content) VALUES ($1, $2, $3, $4, $5, $6)
")]
pub struct AddDocument<'a> {
    pub source_id: i64,
    pub hash: &'a str,
    pub retrieved_date: &'a str,
    pub etag: Option<&'a str>,
    pub title: Option<&'a str>,
    pub content: &'a str,
}

#[derive(FromRow, Serialize)]
pub struct Content(pub String);

#[derive(QueryOne)]
#[aykroyd(
    row(Content),
    text = "
        SELECT content FROM document WHERE id = $1
")]
pub struct GetContent(pub i64);

#[derive(Query)]
#[aykroyd(
    row(SearchDocumentRow),
    text = "
        SELECT d.id, s.url, d.retrieved_date, d.title, snippet(i.fts_document, -1, '<b>', '</b>', '...', 16) AS snippet
        FROM fts_document($1) AS i
        LEFT JOIN document AS d ON d.id = i.rowid
        LEFT JOIN source AS s ON d.source_id = s.id
"
)]
pub struct SearchDocuments<'a>(pub &'a str);

#[derive(FromRow, Serialize)]
pub struct SearchDocumentRow {
    pub id: i64,
    pub url: String,
    pub retrieved_date: String,
    pub title: Option<String>,
    pub snippet: String,
}
