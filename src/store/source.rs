use aykroyd::{FromRow, Query, Statement};
use serde::Serialize;

#[derive(FromRow)]
pub struct SourceUrlRow {
    pub url: String,
}

#[derive(Query)]
#[aykroyd(row(SourceUrlRow), text = "SELECT url FROM source WHERE id = $1")]
pub struct GetSourceUrlQuery {
    #[aykroyd(param = "$1")]
    pub id: i64,
}

#[derive(FromRow, Debug)]
pub struct StaleSourceRow {
    pub id: i64,
    pub url: String,
}

#[derive(Query)]
#[aykroyd(
    row(StaleSourceRow),
    text = "
        SELECT id, url FROM source WHERE (((crawl_date IS NULL) OR (unixepoch('now') - unixepoch(crawl_date)) > 12 * 60 * 60) OR force_crawl = TRUE)
    "
)]
pub struct StaleSources;

#[derive(FromRow, Serialize)]
pub struct SourceRow {
    pub id: i64,
    pub url: String,
    pub crawl_date: Option<String>,
    pub force_crawl: Option<bool>,
}

#[derive(Query)]
#[aykroyd(
    row(SourceRow),
    text = "
        SELECT id, url, crawl_date, force_crawl FROM source
    "
)]
pub struct Sources;

#[derive(Statement)]
#[aykroyd(text = "
    UPDATE source SET crawl_date = ?2 WHERE id = ?1
")]
pub struct UpdateCrawlDate<'a>(pub i64, pub &'a str);

#[derive(Statement)]
#[aykroyd(text = "
    INSERT INTO source (url) VALUES ($1)
")]
pub struct AddSource<'a>(pub &'a str);

#[derive(FromRow, Debug)]
pub struct SimpleSourceRow {
    pub id: i64,
    pub url: String,
    pub crawl_date: Option<String>,
}

#[derive(Query)]
#[aykroyd(row(SimpleSourceRow), text = "SELECT id, url, crawl_date FROM source WHERE id = $1")]
pub struct GetSourceByIdQuery {
    #[aykroyd(param = "$1")]
    pub id: i64,
}
