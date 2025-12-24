use clap::Parser;
use redb::{Database, TableDefinition, ReadableDatabase};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the redb database file
    #[arg(short, long)]
    db_path: PathBuf,
}

const TABLE: TableDefinition<&str, &str> = TableDefinition::new("entity");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let db = Database::create(&args.db_path)?;

    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(TABLE)?;
        table.insert("message", "hello world")?;
    }
    write_txn.commit()?;

    println!("Successfully wrote 'hello world' to database at: {:?}", args.db_path);

    // Optional: read it back to verify
    let read_txn = db.begin_read()?;
    let table = read_txn.open_table(TABLE)?;
    if let Some(msg) = table.get("message")? {
        println!("Read from DB: {}", msg.value());
    } else {
        println!("Could not read 'message' from DB.");
    }

    Ok(())
}
