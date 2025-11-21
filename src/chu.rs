use std::{
    collections::HashMap,
    io::{self, Read},
};

use anyhow::Result;
use scraper::{Html, Selector};

pub fn run() -> Result<()> {
    let mut buffer = String::new();
    io::stdin().read_to_string(&mut buffer)?;

    let all_tables = extract_tables(&buffer);
    let output = tables_to_string(all_tables);
    print!("{}", output);
    Ok(())
}

pub fn extract_tables(html: &str) -> Vec<Vec<HashMap<String, String>>> {
    let document = Html::parse_document(html);

    let table_selector = Selector::parse("table").unwrap();
    let tr_selector = Selector::parse("tr").unwrap();
    let td_selector = Selector::parse("td, th").unwrap(); // Select both td and th for cells

    let mut all_tables: Vec<Vec<HashMap<String, String>>> = Vec::new();
    for table_element in document.select(&table_selector) {
        let mut header_cells: Option<Vec<String>> = None;
        let mut current_table_processed_rows: Vec<HashMap<String, String>> = Vec::new();

        for row_element in table_element.select(&tr_selector) {
            let mut row_cells: Vec<String> = Vec::new();
            for cell_element in row_element.select(&td_selector) {
                row_cells.push(cell_element.text().collect::<String>().trim().to_string());
            }

            if row_cells.is_empty() {
                continue; // Skip empty rows
            }

            if header_cells.is_none() {
                header_cells = Some(row_cells);
            } else {
                let unwrapped_header = header_cells.as_ref().unwrap();
                let mut row_map: HashMap<String, String> = HashMap::new();
                for (index, cell_value) in row_cells.into_iter().enumerate() {
                    if index < unwrapped_header.len() {
                        row_map.insert(
                            unwrapped_header[index].clone(),
                            remove_redundant_spaces(&cell_value),
                        );
                    }
                }
                if !row_map.is_empty() {
                    current_table_processed_rows.push(row_map);
                }
            }
        }
        if !current_table_processed_rows.is_empty() {
            all_tables.push(current_table_processed_rows);
        }
    }

    all_tables
}

pub fn tables_to_string(tables: Vec<Vec<HashMap<String, String>>>) -> String {
    let mut text = String::new();
    for table in tables {
        for row in table {
            for (key, value) in row {
                text.push_str(&format!("{}: {}\n", key, value));
            }
            text.push_str("\n");
        }
        text.push_str("---\n");
    }
    
    text
}

fn remove_redundant_spaces(s: &str) -> String {
    s.split_whitespace().collect::<Vec<&str>>().join(" ")
}
