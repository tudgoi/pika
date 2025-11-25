use aykroyd::{FromRow, Query, Statement};


#[derive(Statement)]
#[aykroyd(text = "
    INSERT OR IGNORE INTO document (id, source_id, retrieved_date, etag, title, content) VALUES ($1, $2, $3, $4, $5, $6)
")]
pub struct AddDocumentStatement<'a> {
    #[aykroyd(param = "$1")]
    pub id: &'a str,
    #[aykroyd(param = "$2")]
    pub source_id: i64,
    #[aykroyd(param = "$3")]
    pub retrieved_date: &'a str,
    #[aykroyd(param = "$4")]
    pub etag: Option<&'a str>,
    #[aykroyd(param = "$5")]
    pub title: Option<&'a str>,
    #[aykroyd(param = "$6")]
    pub content: &'a str,
}

#[derive(FromRow)]
pub struct DocumentRow {
    pub id: String,
    pub retrieved_date: String,
    pub etag: Option<String>,
    pub title: Option<String>,
    pub content: String,
}

#[derive(Query)]
#[aykroyd(
    row(DocumentRow),
    text = "SELECT id, retrieved_date, etag, title, content FROM document WHERE source_id = $1"
)]
pub struct GetDocumentsQuery {
    #[aykroyd(param = "$1")]
    pub source_id: i64,
}
