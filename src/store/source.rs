use aykroyd::{FromRow, Query};
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

#[derive(FromRow)]
pub struct StaleSourceRow {
    pub id: i64,
    pub url: String,
}

#[derive(Query)]
#[aykroyd(
    row(StaleSourceRow),
    text = "
        SELECT id, url FROM source WHERE (((crawl_date IS NOT NULL) OR (unixepoch('now') - unixepoch(crawl_date)) > 12 * 60 * 60) OR force_crawl = TRUE)
    "
)]
pub struct GetStaleSourcesQuery;

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
pub struct GetSourcesQuery;